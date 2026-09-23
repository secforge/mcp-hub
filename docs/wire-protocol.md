# mcp-hub wire protocol — implementer's spec

Audience: anyone building a second server implementation (or a second
client) against this protocol. This is the contract — where the current
Go source (`internal/wire`, `internal/hubconn`, `internal/wsserver`)
disagrees with it, that's marked explicitly as a **known gap**, and the
spec wins; don't replicate the gap on purpose.

## 0. The one thing to get right first: nothing here is "teams-only"

Every message type in this protocol — `messageAfter`, `reaction`, `edit`,
`delete`, `ack`, all their replies — is available on **any** connection,
plain or teams. There is no connection-kind flag on the wire and no
server-type check anywhere in the client that gates *sending* one of
these. The client will happily send a `messageAfter` or `reaction`
request over a connection to any server that accepts the initial
handshake.

What actually varies by server is **which requests get a real answer**:

- `mcp-hub-server`'s own relay (`internal/wsserver`) implements only
  `msg` (broadcast and directed). Every other inbound request type —
  `messageAfter`, `reaction`, `edit`, `delete`, `ack` — is silently
  dropped (`internal/wsserver/server.go`'s read loop discards anything
  whose `type` isn't `"msg"`). This is a *deployment's* limited feature
  set, not a protocol restriction.
- A teams relay (chat-relay) implements the rest because it has real
  history, write access, and a reason to track read position.

So: implement whatever subset of this spec makes sense for your server,
and **declare that subset** in `Joined.features` (§2.1a). That is the
capability mechanism — a client reads it instead of discovering your
limits by sending a request and watching for silence. A server that
declares nothing forces exactly that guessing, and silence is
indistinguishable from a slow answer, so a request may be dropped or
waited on pointlessly; `mcp-hub-server` does this today and a new
implementation should not copy it (see §5).

## 1. Handshake

Everything a client knows about itself travels in **request headers**,
never in the URL. A credential in a query string lands in access logs,
proxy logs and shell history; the fields below include two, so the
carrier is part of the contract rather than a preference.

A client sends **every** field it has on **every** connection. It does
not decide per-server which are relevant, and it must not infer anything
from the shape of the address it was given — that address is opaque to
it. A server ignores what it does not use.

**URL:** `{scheme}://{host}/{path}`, with the credential in the fragment
(see `Authorization` below).

- `scheme`: `ws`/`wss` only. (The client also accepts `http`/`https` on
  the *host* it's given and rewrites them to `ws`/`wss` — that's a
  client-side convenience, not part of the wire contract.)
- `path`: whatever the server issued. A client never parses it, and a
  server must not require it to. `mcp-hub-server` currently addresses a
  session by a UUID path segment matching
  `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`
  (case-insensitive) and rejects an invalid one with HTTP 400 *before*
  the upgrade.
- `Authorization: Bearer {credential}` — the part of the link after the
  `#`. A URL fragment is by definition never transmitted, so a client
  moving it into this header is what keeps the credential out of the
  server's own request line and every log along the way. A link with no
  fragment, or an empty one, is refused by the client before it dials.
- `Agent-Secret` (**required**): the secret that reclaims this client's
  identity. The client manages it — mints one on a first connect, stores
  it per target, and presents the same one every time. It is never
  chosen, held, or passed by whatever is driving the client. A server
  must refuse a connection without one: handing out an identity that has
  no secret behind it means it can never be reclaimed, so the next
  connect is a different participant to the server, to its peers, and to
  its own read position — a silent degradation nobody chooses.
- `Agent-Id` (optional): the peerId this client was last assigned here,
  sent only when it has one. It is a **request** to be given that
  identity back, authorized by `Agent-Secret` — never an assertion to be
  trusted alone, since an identity header honoured without the secret is
  impersonation of any peer whose id someone has seen. Three cases, and
  no others: verified → that identity, displacing any live connection
  holding it (see §4); present but unverifiable → **refuse the join**,
  before the upgrade, with one message for unknown/absent/wrong so a
  caller cannot enumerate a session's peers; absent → assign a fresh
  identity. A server must not fall back to searching its peers for a
  matching secret: that makes the id decorative and reintroduces a
  per-candidate key-derivation cost on the handshake path.
- `Agent-Name` (optional): free-text display name. `mcp-hub-server`
  sanitizes it (strips control characters, caps length at 64 runes) and
  echoes back the sanitized form in `Joined.name` — a client should treat
  whatever comes back as authoritative, not what it sent.
- `Agent-Age-Public-Key` (optional): a well-formed age recipient string —
  bech32, human-readable part `"age"`, 7–90 chars, valid checksum.
  Rejected with HTTP 400 before upgrade if malformed. Never parsed or
  used cryptographically server-side, purely redistributed to other
  peers. Deliberately **not** an identity credential: every peer can see
  it, so granting identity reuse on it would let anyone who saw it
  impersonate its owner.
- `Hub-Protocol-Version` (optional): the client's `ProtocolVersion`
  (currently `3`). A server may log a mismatch; it must not refuse the
  connection over it (see §6).
- `Hub-Create-Token` (optional): a capability for creating and claiming a
  session in this same handshake, for a server that refuses an unknown
  session rather than creating one on first connect.
- `Hub-Topic` (optional): a display name for a session being created.
  Meaningless on a join.

**Known gap:** `mcp-hub-server` does not yet read any of these headers or
require a secret — it still takes `v`, `name`, `agePublicKey` and
`reconnectSecret` as query parameters, credential included. The spec
wins; don't replicate that.

**After a successful upgrade**, the server sends exactly one `Joined`
message (§2.1) before anything else. The client reads exactly one
message and parses it as `Joined`; if that read errors or
`Joined.peerId` isn't a valid UUID, the client closes the connection and
reports a dial failure — no retry.

**Known gap:** the client's first read has **no read deadline** — if a
server upgrades the connection and then sends nothing at all, the client
hangs indefinitely rather than timing out. A conformant server should
send `Joined` promptly regardless; don't rely on the client's patience
here, and treat this as something the client should probably fix (it's
being tracked, not something the spec asks a server to work around).

**Known laxity, not a requirement to match:** the client's first-message
parser does not check that the frame's `type` field is literally
`"joined"` — it just unmarshals into the `Joined` shape and checks
`peerId`. A conformant server **must** still send `type:"joined"` as
specified; don't rely on the client's lack of a check.

## 2. Message types

Every message has a `type` string field identifying it. Unknown fields
are ignored (`encoding/json` default); a message with an unrecognized
`type` is silently dropped by the client. Both of these make additive
changes (new optional fields, new event kinds) safe without a version
bump — see §6.

### 2.1 `joined` (server → client, exactly once, first message)

| Field | Type | Required | Notes |
|---|---|---|---|
| `type` | `"joined"` | yes | |
| `peerId` | string (UUID) | yes | This connection's assigned identity — see §4. |
| `peerCount` | int | yes | How many *other* peers were already present. Informational only — see §3. |
| `serverVersion` | int | yes | This server's protocol version — see §6. |
| `name` | string | no | Echoed back, post-sanitization. |
| `agePublicKey` | string | no | Echoed back verbatim. |
| `canSend` | bool | no | Whether sending is currently permitted — a snapshot, not a guarantee (re-checked per send). |
| `conversationKind` | string | no | e.g. `"oneOnOne"`, `"group"`, `"meeting"`, `"channel"` — free text, not a closed enum. Send it only for a conversation mirrored from a real chat platform; omit it entirely for an ordinary hub session, so its presence alone distinguishes the two. |
| `topic` | string\|null | no | Display name/topic of what was joined, if applicable. |
| `features` | object | no | This server's declared capabilities — see §2.1a. |
| `behind` | int | no | How many messages this peer's own last-acked position trails the newest message in this conversation, computed once at connect. `0` states "caught up" as a fact, distinct from omitted (no such concept — including every mcp-hub-server, and a first-ever connection with no prior position to compare). See §2.6a. |
| `behindSince` | string (RFC 3339, explicit UTC offset) | no, but required alongside `behind` when `behind` is set to a positive value | The timestamp of this peer's last-acked position — what a `messageAfter{at:...}` seek is computed from when the gap is too large to walk. Omitted under the same conditions as `behind`. |

### 2.1a `features` — capability declaration

`joined.features` is how a server states what it can do, so a client never
has to infer a capability from runtime behaviour or from the shape of the
address it dialled. It is keyed by feature name, each value an object
carrying that feature's own parameters — `{}` when it has none:

```json
"features": {"messageAfter": {}, "ackReplies": {}, "attachments": {"maxRawBytes": 33554432}}
```

Three states, all distinguishable, and a client must not conflate them:

- a **named key present** — supported;
- a **key absent** while `features` itself is present — not supported;
- **`features` omitted entirely** — this server predates the mechanism and
  nothing is known either way. A client may then probe or refuse, but must
  not read absence as a "no".

A flag states what the server *does*. It is never an instruction to the
client, and a server must not send one to request client behaviour.

| Feature | Parameters | Means |
|---|---|---|
| `messageAfter` | — | Answers `messageAfter` requests (§2.6a). Without it, a client cannot catch up and must say so rather than wait. |
| `ackReplies` | — | Replies to a *standalone* `ack` (§2.7) with its own `ack` carrying `behind`. |
| `actionAcks` | — | Answers a write action with its own ack — `sendAck` for `msg`, and `reactionAck`/`editAck`/`deleteAck` for whichever of `reactions`/`edit`/`delete` are also declared. Which acks exist follows from those features; this one says only that waiting for an ack is worthwhile at all. |
| `rosterReadAt` | — | Reports per-peer read positions in the roster. |
| `mentions` | — | Accepts `mentions` on `msg`/`edit` and resolves them to platform-native @-mentions. |
| `attachments` | `maxRawBytes`, `maxFrameBytes` int; `imagesOnly` bool | Accepts attachments, within those limits. `imagesOnly` refuses anything but an image. |
| `reactions` | — | Accepts `reaction` requests. |
| `edit` | — | Accepts `edit` requests. |
| `delete` | — | Accepts `delete` requests. |
| `replyTo` | — | Accepts `replyTo` on `msg`/`edit` and renders a native threaded citation. |

Adding a feature needs no version bump — a client at protocol 3 or above
already knows to look. A feature name is removed in the same change that
removes the capability it names, never left describing something gone.

### 2.2 `msg` (both directions)

| Field | Type | Required | Direction | Notes |
|---|---|---|---|---|
| `type` | `"msg"` | yes | both | |
| `peerId` | string | client←server only | server | Sender's peerId. Absent on the client→server request (the connection *is* the sender). |
| `text` | string | yes | both | |
| `ts` | string | server→client | server | Timestamp, server-defined format (RFC3339 in `mcp-hub-server`'s case). |
| `to` | string | no | client→server | Set to request directed (private) delivery to one peerId instead of broadcast. |
| `private` | bool | no | server→client | Set by the server on a delivered directed message. |
| `historical` | bool | no | server→client | True if this is answering a `messageAfter` request (§2.6a) rather than live traffic. |
| `externalId` | string | no | server→client | Teams-only concept: this server's own id for the message, correlating it with an earlier `sendAck`. |
| `own` | bool | no | server→client | True if *this exact connection* sent it. A receiving client's own policy decision whether to treat this as wake-worthy — see §3 for what the reference client does. |
| `cursor` | string | no | server→client | This message's own opaque position — pass back as a `messageAfter` anchor (§2.6a). |
| `answers` | Anchor (see §2.6a) | no | server→client | Present only when this `msg` is the direct answer to a `messageAfter` request — the exact anchor that request was sent with, echoed back verbatim. |
| `ackCursor` | string | no | client→server | Piggybacked read receipt — see §2.7. |
| `attachments` | array of Attachment (see below) | no | both | Binary content, inline or by reference. |
| `format` | string | no | both | How `text` should be interpreted — see below. |
| `replyTo` | string | no | both | `externalId` of the message this one is a threaded reply/citation to — see below. |
| `replyPreview` | string | no | server→client | Server's own lossy abbreviation of the quoted message — see below. |
| `mentions` | array of `{name?, id}` | no | server→client | Who this message @-mentions, if anyone — see below. |
| `mentionedMe` | bool | no | server→client | Whether the receiving connection's own identity is among `mentions` — see below. |

`replyTo`, when present, is in the same id space as `externalId`/`sendAck`
— directly comparable against a message a client already holds, and
usable as the target of `reaction`/`edit`/`delete` on the quoted message,
not just a display reference. Server→client, it's set on a delivered
`msg`/`messageEdited` when that message is a reply (absent, not null/
empty, when it isn't — an edit never changes what a message replies to,
so a teams relay should carry the same value through on
`messageEdited` too, not just the original `msg`). Client→server, a
client may set it on an outgoing `msg`/`edit` to request a threaded
citation; a server that supports this should validate it strictly —
refuse the whole send outright (nothing sent, not a partial send without
the citation) for an id it doesn't hold, holds in a *different*
conversation, or that's malformed/empty, since resolving a citation can
surface that other message's own preview text and an unvalidated
cross-conversation reference is a disclosure risk, not merely a bad
request. `mcp-hub-server`'s own relay has no opinion on this field
either direction — accepted and relayed unmodified (`wsserver` threads
it through to `NewBroadcastMsg`/`NewDirectedMsg` on send, same as
`attachments`/`format`), never validated or interpreted.

One `replyTo` may mean two different underlying operations depending on
what kind of conversation it's in, and a client never needs to know
which: in a flat conversation (e.g. a chat) it's a citation naming the
quoted message; in a threaded one (e.g. a channel) it's a reply into
that message's thread — and if `replyTo` names a reply rather than a
thread root, the new message should land in that reply's thread, not be
refused for not being a root. Same field, same client-side contract
(refuse outright, per above, rather than degrade) either way — this is
purely a note for a server implementer translating `replyTo` into a
specific backend's API, not something a client needs to branch on.

`replyPreview`, when present, is the server's own lossy (formatting
flattened, possibly truncated) abbreviation of the quoted message's
text — a fallback for a `replyTo` that names a message outside a
client's own history, not the authoritative quoted text (use
`messageAfter`, §2.6a, around that cursor for that). Server→client only;
a client never sets
this on send — the server derives it once it has resolved `replyTo`.

`format`, when present, is `"text"` (the default if omitted — plain,
escaped verbatim) or `"html"` (bold/lists/code/quotes/tables/links,
sanitized server-side through the same allowlist used to render it:
scripts, event handlers, styles, iframes, and off-host images are
stripped). A server that validates this field should refuse an
unrecognized value outright (e.g. as an `error`, §2.5) rather than
silently downgrading it to `"text"` — a client should never guess a
value the target server wasn't confirmed to accept, since that failure
mode is much harder to notice than an outright refusal. `mcp-hub-server`
itself has no opinion on this field — it's accepted and relayed
unmodified (`wsserver` threads it through to `NewBroadcastMsg`/
`NewDirectedMsg` just like `attachments`), but never interpreted, since
its relay does no rendering of any kind. `edit` (§2.8) also carries
`format`, applying to the new `text` it sets.

`mentions`, when present, lists who a message @-mentions — each entry's
`id` is the sending platform's own directory id (opaque to this spec, not
a hub `peerId` — there is no wire-level way to resolve one into the
other), `name` is a display name if the server has one. Absent (nil, not
an empty array) when the message mentions no one. `mentionedMe` is true
when the *receiving connection's own identity* is among `mentions` — a
server computes this per connection (or, per chat-relay: per conversation,
when every connection on that conversation necessarily shares one
identity — either way, a client never needs to know its own directory id
to use this field). Both server→client only; `mcp-hub-server`'s own relay
never sets either. `messageEdited` (§2.8) carries the same two fields,
reflecting the edited text — an edit can change who's mentioned, unlike
`replyTo`/`replyPreview` which never change on edit.

`systemPeerId` (§2.1, `joined`) names a `peerId` a server itself
originates operator/system messages from on this session. Since every
`peerId` is necessarily server-assigned — no inbound client frame ever
supplies one — a client can treat a `msg`/`peerJoined`/`peerLeft` whose
`peerId` equals `systemPeerId` as reliably from the server's own operator
channel, not from a peer that typed the same claim into ordinary message
text. That guarantee covers the *sender's identity* only, not the
*content* of what they said: a client surfacing this to a model should
frame an operator message as outranking another **agent's** instructions
on this hub, while never outranking the model's own user, who isn't a
party to the hub session at all. `mcp-hub-server` never sets
`systemPeerId` — there is no operator concept on a plain hub session,
every peer is an ordinary participant.

An **Attachment** is one of two shapes sharing the same object, distinguished
by which fields are present — a client attaching its own content (client→server,
on `msg` or `edit`) always sends the inline shape; what comes back
server→client depends on the server:

- **Inline**: `{contentType, contentBytes, name?}` — `contentType` is
  open, not a closed enum: `image/png`/`jpeg`/`gif`/`webp` render inline
  on both `mcp-hub-server`'s relay and chat-relay's hub sessions (Teams
  sessions still refuse anything non-image outright — see below); any
  other type is accepted too on a hub session (not Teams), just without
  inline rendering — chat-relay stores it and serves it back only as an
  `application/octet-stream` download, `mcp-hub-server`'s relay just
  passes it through unmodified with no server-side handling at all.
  `contentBytes` is base64 of the raw file; `name`, when given, is the
  original filename (useful for anything that isn't an image — it
  becomes the download filename on a server that honors it). A sender
  should keep raw (pre-base64) size under a server-defined cap — 32MB is
  the figure both `mcp-hub-server`/its reference clients and chat-relay
  settled on (raised together from an earlier 8MB, coordinated live
  specifically so the two numbers can't drift apart the way an earlier
  1MB-vs-12MB split once did on chat-relay's own side) — and check that
  against the raw bytes, not the base64-inflated wire size.
  `mcp-hub-server`'s own relay only ever produces this shape.
- **Reference**: `{token, contentType, name?, kind?}`, no `contentBytes`
  — used by a server that doesn't want to inline bytes into every
  delivery (chat-relay does this). `contentType` here describes the
  server's *recoded* stored copy for an image, which may differ from
  whatever the original sender attached — every image is decoded and
  re-encoded before ever being served; a non-image is stored and served
  byte-identical, but still only as a download, never inline, and never
  under its own declared type in a way a client should trust for
  rendering. Fetch the actual bytes with an `attachment` request (§2.2a)
  using `token`. A client can tell the shapes apart by whether `token`
  is set and `contentBytes` is absent.

**Teams sessions specifically** (as opposed to a teams relay's
*hub* sessions, which follow the general rule above) still refuse any
non-image `attachments` entry outright — enforced independently of
whatever content-type list a server's hub sessions accept, so widening
that list elsewhere can never leak a binary attachment into a Teams
conversation. A file would need to be uploaded into the chat's SharePoint
folder, a different permission and a different design, not something
this field covers.

Like every other field in this table, an unrecognized `attachments` is
safely ignored by `encoding/json`-style decoders — a server or client
with no attachment support just never populates or reads it. `edit`
(§2.8) also carries `attachments`, always the inline shape even against a
reference-style server (which then recodes/stores/references it the same
as for a live `msg`): an *absent* `attachments` on an edit must be read
as "unchanged", never as "remove them" — there is deliberately no way to
express attachment removal in this version of the protocol. `reaction`/
`delete` carry no `attachments` — not applicable.

### 2.2a `attachment` (client → server) / `attachmentData` (server → client)

Fetches the actual bytes behind a reference-shape Attachment's `token`
(§2.2). Not meaningful against a server that only ever produces the
inline shape — `mcp-hub-server` silently ignores an `attachment` request
like any other unrecognized type, so a client waiting on a reply simply
times out; only send this for a `token` actually seen on a received
Attachment.

| Field | Type | Required | Direction | Notes |
|---|---|---|---|---|
| `type` | `"attachment"` / `"attachmentData"` | yes | both | |
| `token` | string | yes | both | Echoed back on the reply. |
| `name` | string | no | server→client | Original filename, if known. |
| `contentType` | string | yes | server→client | The recoded stored copy's type. |
| `contentBytes` | string | yes | server→client | base64 of the raw (recoded) file. |

A request that can't be satisfied comes back as an ordinary `error`
(§2.5) instead of `attachmentData`, with `code` one of `bad_attachment`
(malformed token), `not_found` (unknown token, or belongs to a different
session), or `unavailable` (recorded but no servable bytes, e.g. a recode
failure — never served raw as a fallback).

A plain broadcast (`to` omitted) is never echoed back to its own sender
by `mcp-hub-server`'s relay logic (broadcast excludes the sender) — if
your server does the same, don't expect the sender to see its own `msg`
come back; `sendAck` (§2.8) exists for exactly this gap on a teams relay that
*can't* deliver synchronously.

### 2.3 `peerJoined` / `peerLeft` (server → client)

```json
{"type": "peerJoined", "peerId": "...", "name": "...", "agePublicKey": "..."}
{"type": "peerLeft", "peerId": "..."}
```

`name`/`agePublicKey` are omitted if that peer didn't supply them.

### 2.4 `rosterComplete` (server → client)

```json
{"type": "rosterComplete"}
```

Terminates the initial roster burst — see §3.

### 2.5 `error` (server → client)

| Field | Type | Required |
|---|---|---|
| `type` | `"error"` | yes |
| `message` | string | yes |
| `code` | string | no |
| `retryable` | bool | no, meaningless without `code` |

`error` can arrive at any time, in response to any request the server
refuses. It never implies a close is coming — a server may refuse a
request and keep the connection open. Known codes in use today (not a
closed set — treat `code` as an open string):

- `send_refused` / `reaction_refused` / `edit_refused` / `delete_refused`
  — a policy gate on the teams relay refused the action (`retryable:false`
  typically — check the specific server's semantics, this spec doesn't
  mandate retryability per code).
- `invalid_credential` — relay auth failure (teams-specific, not part
  of the plain-session handshake in §1).
- `bad_ack` / `bad_ack_cursor` — ack-subsystem-specific: a standalone
  `ack` with no cursor, or a cursor that won't decode/parse.
  **Deliberately distinct from any generic `bad_request`/`bad_cursor`
  you might use for other malformed requests** — `error` carries no
  correlation id, so a client watching for ack-specific failures
  (to permanently stop sending receipts on a persistent bug — see §2.7)
  cannot tell a generic error apart from an ack one. If you emit a
  generic `bad_cursor`/`bad_request` for other request kinds too, use
  different codes for the ack case, not the same ones, or every client
  built against this spec will misattribute your unrelated errors to
  their ack subsystem.
- `bad_reply_to`, `retryable:false` — a `msg`/`edit`'s `replyTo` (§2.2)
  named a message the server won't cite: unknown, held in a different
  conversation, or malformed/empty. Nothing is sent when this fires — not
  a partial send with the citation silently dropped. A message the
  server once held but that's since been deleted upstream is a different
  failure (e.g. `send_failed`), not this code — `bad_reply_to` is
  specifically about validating the reference itself before attempting
  anything. **Why this is its own strict code, not generic validation**:
  a server implementing citations typically has to resolve `replyTo` to
  build the citation payload (fetching that message's own preview/sender
  to embed) — an unvalidated id would let a client pull another
  conversation's content into this one just by naming a message id it
  was never actually shown, a disclosure risk rather than a mere bad
  request, especially for a server-side identity present in many
  conversations at once. `message` (not `code`) is the place to
  distinguish *why* a given id was refused (e.g. "held in a different
  conversation" vs. "never seen, possibly predating this server's
  history") — a client can act on that difference (the latter likely
  means "too old to cite, fall back to quoting the text"), but `code`
  alone doesn't carry it.
- `bad_anchor`, `retryable:false` — a `messageAfter` (§2.6a) request's
  anchor doesn't decode to a stream position at all. See §2.6a for why
  this is the *only* error `messageAfter` can produce (no
  `unknown_message` — a position always has a successor or does not),
  and why it should be unreachable for a well-behaved client.

### 2.6 `history` (client → server) / `historyComplete` (server → client)

**Removed 2026-09-04.** `history`/`historyBegin`/`historyComplete` (and
the `Joined.latestCursor`/`historyAfter`/`historyLimitMax` capability
fields that advertised them) are no longer part of this spec — every
caller, LLM or not, should use `messageAfter` (§2.6a) instead. This
reverses an earlier compromise recorded in this section ("kept for any
other caller") that treated `history` as still-valid for a non-LLM
reader; the reference client owner decided the batch-paging shape itself
— not just the LLM-attention failure mode §2.6a was originally built to
fix — wasn't worth maintaining two parallel read paths for, and asked
that server-side support be removed too. A server or client still
speaking `history` after this point is on an old, incompatible protocol
version — see §6's `ProtocolVersion` bump for how that mismatch now
surfaces at connect time rather than as a silent failure the first time
a page is requested.

What `history` did, for context: it paged backward (`before`, exclusive,
reaching only older messages) or forward (`after`, exclusive, only if the
server advertised `Joined.historyAfter`) in a batch of up to `limit`
messages (each `historical: true`), terminated by a `historyComplete`
frame (optionally preceded by a `historyBegin` frame carrying the same
`count`/`oldest`/`newest`, so a downstream truncation that cuts the tail
of a burst — the overwhelmingly common case, confirmed live — didn't
silently swallow the completion signal too). `messageAfter` replaces all
of that with a single "next message" primitive that removes the *batch*
itself, which is what actually mattered: §2.6a's rationale section below
explains the failure mode (an LLM reader skimming past a message inside
a complete, correctly-delivered batch) this removal fixes, but nothing
about "a batch invites skimming" is specific to that one reader type
either — it's just more visible there.

The security note that used to live here — a server answering `history`
straight from its message store, filtered only by session/conversation,
can leak a directed (`to`-targeted) message to a peer who was never its
recipient, unless the store also records *who a directed message was
addressed to* and filters on that — still applies verbatim to any server
implementing `messageAfter`'s own backward-compatible read path, if it
has one. The fix is the same: store the addressee, filter reads on
"public, or I'm the sender, or I'm the recipient," and never persist a
*refused* send (target absent, policy refusal) for a later read to hand
out — a message the sender was told never went anywhere must not
reappear for anyone via `messageAfter` either.

### 2.6a `messageAfter` (client → server) / answer (server → client)

Reads exactly one message — the message at the first stream position
strictly after a given position. Designed, live, 2026-09-04, as the
minimal replacement for `history`'s batch-paging shape once a real
incident showed *why* a batch is unsafe for an LLM reader specifically —
see the rationale at the end of this section before implementing.

Request — exactly one of `cursor`/`at` is set, never both, and neither is
optional (there is no anchor-less form). Sending neither, or sending
both, is a malformed request: it doesn't decode to a position, so it
gets the same answer as any other undecodable anchor — `error{code:
"bad_anchor"}` — not a default like "the oldest message." A well-behaved
client should never construct this case; it's specified here only so two
independent implementations agree on the malformed case too.

| Field | Type | Required | Notes |
|---|---|---|---|
| `type` | `"messageAfter"` | yes | |
| `cursor` | string | exactly one of `cursor`/`at` | An opaque handle, verbatim from a `msg.cursor` this client already holds. Never parsed, compared, or constructed by the client — see "Why the anchor is a position, not a message identity" below. |
| `at` | string (RFC 3339, explicit UTC offset — a naive/zoneless timestamp must be refused, not assumed) | exactly one of `cursor`/`at` | A client-chosen instant. "The first message at or after this instant" — inclusive, unlike `cursor`'s exclusive "strictly after." |

Answer — exactly one of three, and *only* one of the three:

```json
{"type": "msg", ..., "historical": true, "answers": {"cursor": "..."}}
{"type": "noMoreMessages", "answers": {"cursor": "..."}}
{"type": "error", "code": "bad_anchor", "retryable": false}
```

- The `msg` form is the answer, formatted exactly like any other `msg`
  (§2.2) — same `attachments`/`format`/`replyTo`/`mentions` fields, same
  rules — with `historical: true` and `answers` added.
- `noMoreMessages` means "there is no message at a later position than
  the one given" — **not an error**, this is the normal, expected way a
  walk or a seek terminates. `answers` is set here too.
- `error{code: "bad_anchor"}` means the anchor string doesn't decode to a
  position at all — a corrupted/garbled `cursor`, or an `at` that fails
  to parse as an explicit-offset RFC 3339 instant. This is **unreachable
  for any client that only ever echoes a `cursor` it was actually given
  and sends a well-formed `at`** — there is no `unknown_message` code,
  and deliberately so: see below for why a position always has a
  successor or does not, with nothing for the server to fail to
  recognize.

`answers` is the same `Anchor` shape as the request (`{"cursor": "..."}`
or `{"at": "..."}`), echoing **the anchor exactly as sent** — not the
resolved message's own cursor (which is already present as `cursor` on
the `msg` itself and would tell a client nothing new). This is what lets
a client tell a pull's answer apart from unrelated live traffic even if a
downstream layer merges the two into one delivery, and what lets more
than one walk be correlated if a client ever has more than one in flight.

**Why the anchor is a position, not a message identity.** An earlier
design iterated through `cursor`/`id`/`{before,after,limit}` forms before
converging here; the reason worth carrying forward, not just the
conclusion: a message *identity* (an id) is sparse — it can be deleted,
malformed, or simply never have existed, and "the next message after
message M" is only answerable when M does — which is exactly where an
`unknown_message` error, a lookup, and a whole class of "is this id valid"
bugs come from. A *position* is total and dense: "the first message at a
position strictly after P" is answerable for **any** P, including one no
message currently occupies. `cursor` and `at` are both positions in this
sense (a cursor is the server's own exact composite position; a bare
timestamp is a coarser position with no tiebreak, which is what makes the
inclusive/exclusive difference between them free — see below), so there
is no "invalid id" case for the server to guard against, and — critically
for the opacity rule — no way for a client-fabricated `cursor` to name
"the wrong message," only "some position," because a `cursor` must never
be fabricated in the first place (see below).

**One ordering rule, not two.** The answer is always "the message at the
first position **strictly after** the given one" — for a `cursor` this is
an ordinary exclusive walk; for a bare `at` timestamp T, treat T as the
position `(T, <before any tiebreak>)`, so "strictly after" naturally
returns a message *at* T if one exists. One rule produces both the
exclusive-walk and the inclusive-seek behavior a client needs, with
nothing to remember about which anchor form gets which rule.

**Opacity, and why it's a *safety* property, not just a style
preference.** `cursor` is entirely the server's format and precision to
choose and change — a client must never parse, compare, derive, or
increment one, only ever hand back a value it was literally given (from
a `msg.cursor`). This isn't only about forward-compatibility: on a real
server backing this protocol, message rows are **not** contiguous within
one conversation (they share a sequence across every conversation on the
server), so a client doing `cursor + 1` on a numeric-looking handle
wouldn't merely skip a gap — it would name a message in a *different
conversation*. `at`, by contrast, is a coordinate the client legitimately
owns: it's meaningful without the server, and "the first message at or
after instant T" is well-defined for every T, so there's no equivalent
hazard in choosing one freely.

**Why this exists at all, and why `historyBegin`/`historyComplete`
weren't enough**: confirmed live, 2026-09-04 — a 50-message `history`
page was delivered completely and correctly (verified by reading the raw
bytes on the client's own reading surface, all present) and was *still*
misread: one message was skimmed inside the page, its position then
recorded as "seen" by a cursor that advances over a whole returned page,
and it would never be re-delivered. Nothing was lost in transit — the
existing truncation-detection machinery (§2.6, and the client-side
per-event markers described in the design doc) had nothing to detect,
because there was no cut. The failure was **attention**, not delivery: a
reader given a large batch of mostly-already-seen text does not reliably
read every line. `messageAfter` fixes this at the root by removing the
batch: a client that only ever asks for and receives one message at a
time has nothing to skim *inside*, because there is no "inside" — the
page is exactly the thing it asked for. This is why a well-behaved
client (see the design doc's `hub_catch_up`) deliberately never requests
more than one message per call even though this protocol places no such
limit on the wire itself.

**General rule for any future burst-shaped addition to this protocol**:
a real downstream truncation observed against this protocol's now-removed
`history` mechanism cut the *tail* of a multi-event delivery, not the
head — so a count/size/"read the full copy at X" marker belongs at the
**head** of a burst it describes, not only the tail, if one is ever added
back. This isn't specific to `history`: the reference client (`mcp-hub`)
applies the identical idea to *any* multi-event delivery over
`wait --follow`/`hub_receive`/`hub_wait`, prefixing every batch of more
than one event with an explicit "delivering N events below" header,
since ordinary live traffic arriving in a burst (several messages landing
before a listener catches up) has the exact same undetectable-truncation
exposure `history` used to, with no protocol-level count to fall back on
at all.

### 2.7 `ack` (both directions) — read receipts

This is the read-cursor / read-receipt mechanism. It answers "has the
model/client actually consumed this event, not just received it" — see
the design rationale in `docs/superpowers/specs/2026-08-21-mcp-hub-design.md`'s
"Read receipts" section if you want the full history of why this
exists.

**Piggybacked form** (preferred): any client→server request
(`msg`/`reaction`/`edit`/`delete`) may carry an `ackCursor`
field — "this is the cursor of the last event I've actually consumed."
Fire-and-forget: **no reply** to a piggybacked receipt, ever. A server
should just record the position.

**Standalone form**, when nothing else is about to go out anyway:

```json
{"type": "ack", "ackCursor": "<cursor>"}
```

Reply — success:

```json
{"type": "ack", "ackCursor": "<cursor you sent>", "ok": true}
```

Reply — stale (the given cursor is behind what you already hold):

```json
{"type": "ack", "ackCursor": "<the position you actually hold>", "ok": false}
```

`ok:false`'s `ackCursor` is **not an echo** — it's your truth, which
the client should adopt as its own bookkeeping rather than retry
(monotonicity means nothing is lost: you only reject a position older
than one you already have).

Malformed input never gets an `ack` reply — see §2.5's `bad_ack`/
`bad_ack_cursor`.

**Monotonicity is a server-side invariant you must enforce**: never let
a stored read-position move backward. A client is allowed to send a
receipt for a position it's already sent — tolerate repeats (a
heartbeat-suppression bug on a client's side is not a protocol
violation), but never *reject outright* a request merely for repeating
a known-good position; only reject one that's strictly older than what
you hold.

**Persistence**: store the position keyed to something that survives a
reconnect (a stable per-peer identity, not the live connection) — the
whole point is that a client that drops and resumes has still read what
it read.

**What "consumed" means is entirely up to the client — this server
cannot verify it, only record whatever position it's told.** A client
whose async delivery path (e.g. a background `wait`-style process)
writes an event to a local socket has no way to know whether whatever's
reading that socket actually processed the event, only that this process
wrote it onward — so an ack sent automatically at that point records
"reached this client," not "reached whatever consumes on the other end
of it," while the field's own name implies the latter. `mcp-hub-client`
therefore gates its standalone ack on a genuinely synchronous hand-over,
and — for the harder case where most delivery IS async by design —
offers `hub_confirm`, a tool the model calls itself once it has actually
seen a message, so the ack this server receives is model-issued by
construction. A server relying on the
ack cursor for anything stronger than "the client's process received
this" (e.g. `Joined.Behind`, §2.1/§2.6a) should keep that distinction in
mind: it's honest about delivery, not about a human or model actually
having read the content.

### 2.8 Reactions / edits / deletes (client → server) and their echoes

Requests:

```json
{"type": "reaction", "externalId": "...", "reaction": "👍", "action": "add", "ackCursor": "..."}
{"type": "edit", "externalId": "...", "text": "...", "ackCursor": "...", "attachments": [...], "format": "...", "replyTo": "..."}
{"type": "delete", "externalId": "...", "ackCursor": "..."}
```

`action` is `"add"` or `"remove"`. `reaction` is an open string — do not
validate against a closed set; whatever the underlying platform reports
is authoritative (real data has included things like `"Eyes"` and
`"Question mark"` alongside `"Like"`). `edit.attachments`, when present,
is always the inline Attachment shape (§2.2) even against a
reference-style server — see §2.2's note on absent-means-unchanged.

Acks (server → client, sent once the action is actually carried out —
**a refusal is an `error` event, §2.5, not one of these with `ok:
false`**; `ok` is expected true whenever one of these is sent at all):

```json
{"type": "reactionAck", "externalId": "...", "reaction": "...", "action": "...", "ok": true}
{"type": "editAck", "externalId": "...", "ok": true}
{"type": "deleteAck", "externalId": "...", "ok": true}
```

Fan-out to *other* connections (server → client, not something a
receiving client requested):

```json
{"type": "reactionChanged", "externalId": "...", "peerId": "...", "reaction": "...", "label": "...", "action": "add", "ts": "...", "own": false}
{"type": "messageEdited", "externalId": "...", "text": "...", "ts": "...", "own": false, "attachments": [...], "format": "...", "replyTo": "...", "replyPreview": "...", "mentions": [...], "mentionedMe": false}
{"type": "messageDeleted", "externalId": "...", "cursor": "...", "ts": "...", "own": false}
```

Notes:
- `messageEdited.attachments` is the edited message's *current* full set
  (inline or reference shape, per §2.2 — not a diff against what it had
  before). Absent here means the `edit` request itself carried no
  `attachments` and so left them unchanged — a receiving client should
  keep whatever attachments it already had for this `externalId`, not
  treat an absent field as "now has none."
- `reactionChanged.peerId` may be **absent** — if a removal is detected
  by diffing a message's reaction set and the remover isn't identifiable
  from that diff, still send the event (the removal is real information)
  rather than suppressing it for lack of full attribution.
- `messageDeleted` is deliberately its own event, not an edit to empty
  text — a client that conflated the two would render a blank message
  where the platform renders a tombstone. Include `cursor` so a client
  can place the tombstone in a transcript without having seen the
  original message.
- `own` on all three is true only when *this exact connection* performed
  the action.

### 2.9 `sendAck` (server → client)

```json
{"type": "sendAck", "externalId": "...", "ok": true}
```

Sent immediately on a send, **before** the canonical `msg` comes back
through whatever async fan-out path you use (which may be arbitrarily
delayed — a slow polling/reconciliation loop, for instance). This is
what lets a client distinguish "still in flight" from "silently failed"
during that gap. The canonical message still arrives exactly once,
later, as an ordinary `msg` with a real `cursor` — `sendAck` never
substitutes for it. `mcp-hub-server` never sends this (a plain
`hub_send` already completes synchronously with no such gap).

## 3. Ordering and atomicity

- **`joined` → N × `peerJoined` → `rosterComplete`**: the sentinel event
  (`rosterComplete`) is what the client actually waits for. **`peerCount`
  is purely informational** — the reference client never compares it
  against the number of `peerJoined` events actually received, and never
  blocks on it. Sending a `peerCount` that doesn't match reality changes
  nothing structurally on the client side, though obviously your own
  correctness (and human/model-facing text) depends on it being right.
- The whole join sequence must be computed **atomically** with respect
  to concurrent joins/leaves — a joining peer's roster snapshot must be
  exactly the set of peers present at that moment, with no gap where a
  peer from the snapshot could vanish (a phantom `peerJoined` with no
  matching `peerLeft` reachable) or a concurrent joiner could be missed
  entirely from everyone's view.
- Beyond the roster burst, this protocol makes no ordering guarantee
  across unrelated message kinds and no coalescing guarantee — don't
  assume, e.g., that a `sendAck` always arrives strictly before the
  canonical `msg` in wall-clock terms on the wire, only that it's
  *sent* first by a well-behaved server.

## 4. Identity: peerId and reconnectSecret

- `peerId` is a UUID the server assigns, either freshly generated or
  reclaimed — never chosen by the client.
- A client **should always send a `reconnectSecret`**, even on a brand
  new connection with nothing to resume (the reference client's own
  tool contract requires this, as a guardrail against a client ending
  up unable to resume its own identity later — the wire protocol itself
  doesn't require it, but treat it as a MUST for interop with that
  client).
- Presenting the **exact same** `reconnectSecret` on a later connect
  reassigns the same `peerId` as before. If the previous connection
  holding that secret is not currently live, this is an ordinary
  reconnect. If it *is* still connected, the new connection **supersedes**
  it — the old connection is force-closed (with close code 4004, §5) and
  the new one takes over the identity, rather than the new connection
  being handed a fresh, unrelated `peerId`. This was a deliberate
  correction from an earlier revision of this contract (which assigned a
  fresh id on live collision): that behavior silently defeated
  `reconnectSecret`'s entire purpose for exactly the case that needs it
  most — a fast reconnect racing the server's own detection that the old
  socket died, where "live" from the server's point of view can simply
  mean "hasn't noticed yet." Never collide two live connections onto one
  identity; always resolve to exactly one.
  No `peerLeft`/`peerJoined` is broadcast to other peers for a
  supersede — from their perspective this identity never left, it just
  changed which connection holds it.
- The secret is never distributed to any other peer — it's a private
  channel between one client and the server, unlike `agePublicKey`
  (which is broadcast) or `peerId` (which is public within the session).
  Do not use anything publicly visible (like `agePublicKey`) to grant
  identity reuse — that would let anyone who observed it impersonate
  the original holder.
- The mapping should survive a server restart and even the session
  becoming fully empty — a reconnect months later with the same secret
  should still reclaim the same `peerId`, unless you deliberately expire
  it (teams-specific link/session lifetimes are your own policy, not
  part of this identity contract).

## 5. Close codes

| Code | Meaning | Client behavior |
|---|---|---|
| 1000 (NormalClosure) | Deliberate, clean disconnect by either side | No note surfaced — this is the expected, unremarkable case. The reference client sends this with description `"client disconnect"` on every intentional close (e.g. `hub_disconnect`). |
| 1001 (GoingAway) | Also treated as an ordinary, expected close | Same as 1000 — no note. |
| 1006 (AbnormalClosure) | No close frame received at all — network death, crash, or (historically, now fixed client-side) a close frame written but not flushed before the underlying connection was torn down | Surfaced to the model/caller as "connection assumed dead" plus (if a read timeout, not a close frame, triggered it) how long since the last frame seen. |
| 4001 | Teams-specific: credential revoked | Client surfaces "(revoked — do not reconnect)". |
| 4002 | Teams-specific: credential/link expired | Client surfaces "(expired — do not reconnect)". |
| 4003 | Teams-specific: the underlying conversation became unavailable | Client surfaces "(conversation unavailable — do not reconnect)". |
| 4004 | A new connection presented the same `reconnectSecret` while this one was still live, and took over (superseded) rather than being assigned a fresh, unrelated peerId | Client surfaces "(connection closed, code N)" (no specific-case guidance yet — this is new) — **do not treat this as a signal to avoid reconnecting**, unlike 4001–4003: the identity is alive and well on the connection that superseded this one, this is simply not that connection anymore. |
| any other non-1000/1001 code | Generic | Client surfaces "(connection closed, code N)" — no specific guidance. |

4001–4003 are an existing convention for "this credential is dead, don't
retry" — if your server has an analogous concept (e.g. a superseded
link), you may mint a new code in the same private range (4000–4999);
the reference client will fall back to the generic "(connection closed,
code N)" note for anything it doesn't specifically recognize, which is
safe but less informative — worth coordinating a code and getting it
added client-side if the case is common enough to matter. 4004 itself is
exactly such a coordinated code: chosen to match chat-relay's own
existing convention for the identical case on its LINK sessions (the
newcomer takes the identity, the older connection is closed with 4004),
rather than picked independently.

`mcp-hub-server`'s relay sends 4004 when `hubsession.Session.Join`
supersedes a still-live identity (see §4 below) — the one core-protocol
case in this table, not a teams-only convention like 4001–4003.

A server should always prefer sending an `error` event (§2.5) before
closing, when the close reason is something the model could act on
(e.g. a dead credential) — the close code is a *secondary*, best-effort
signal for the case where the `error` event itself didn't make it
through in time, not a replacement for it.

**A code was proposed and deliberately dropped, worth recording so it
isn't re-proposed identically**: 4005 `behind`, for "this connection
couldn't keep up with live delivery and was closed rather than blocking
every other peer" (see §8's backpressure guidance below). Found live,
2026-09-04: a graceful close *is itself a write*, and the condition 4005
would signal is exactly "writes to this peer don't complete" — so it can
never actually be sent for the case it exists to describe. The correct
implementation instead aborts the raw transport (an ordinary close, no
code — typically surfacing as 1006 to the client) once a per-peer
outbound queue overflows or a write exceeds its deadline. A client needs
no special handling for this: it's already just an ordinary disconnect,
recovered the same way any other one is (reconnect, then `messageAfter`/
`hub_catch_up` from the last known position) — which is what makes the
abort acceptable rather than a loss.

## 6. Versioning

`ProtocolVersion` is currently `2`. The policy: bump it only for a
genuinely breaking change. New optional fields and new event kinds are
additive and don't need it — `encoding/json` ignores unknown fields on
decode, and an unrecognized `type` is silently dropped rather than
erroring. A server should follow the same policy: don't require a
specific `v` to accept a connection.

`1` → `2` (2026-09-04) is exactly such a breaking change: `history`/
`historyBegin`/`historyComplete` (§2.6, removed) had no replacement
fallback left once `messageAfter` (§2.6a) became the only read-catch-up
path — a client that only knows `history` can no longer be served by a
server that only knows `messageAfter`, and vice versa. That's the bar for
a version bump: not "does this add something new" (that's additive, no
bump) but "does an old peer's request now go unanswered where it used to
work."

If `Joined.serverVersion` differs from the client's own `ProtocolVersion`,
the reference client does **not** refuse the connection or alter its own
behavior beyond this — it's surfaced as an explicit note in
`hub_connect`'s/`teams_relay_connect`'s result text: "tell the user to
update mcp-hub-client" if the server is newer, "the server may need
updating" if the server is older. This is the mechanism an incompatible
peer is expected to be caught by — at connect time, as a clear
human-actionable instruction, rather than a silent hang or a confusing
per-request failure discovered only once a client tries to page. There is
no further protocol-level negotiation beyond this one-shot version
comparison.

## 7. Liveness / keepalive

- The server should ping periodically. The reference server pings every
  30s (`pingPeriod`).
- The client tears the connection down if it hears **nothing at all —
  neither a ping nor any data frame** — for 100s (`pongWait`). This
  margin is deliberately generous (>3x the ping period) specifically to
  absorb ordinary jitter (a GC pause, scheduler contention, an
  intermediary briefly holding a frame) without false-triggering; don't
  assume a tighter margin is safe.
- **Unsolicited PONGs do not reset the client's deadline.** The client's
  pong handler is a no-op (it does not even have one registered) — only
  receiving a **PING** (auto-answered) or any data frame resets the
  deadline. A server-side "keepalive" that only sends unsolicited PONGs
  (e.g. some .NET WebSocket options' default legacy keepalive mode)
  looks like activity to a naive prober but **does not** satisfy this
  client's liveness check. This cost real debugging time in practice —
  a teams relay implementer whose framework's keepalive was PONG-only chased
  the resulting drops through several wrong hypotheses (nginx timeouts,
  the client's own margin, an orphaned socket) before finding this.
  Send real, unsolicited-from-the-server PINGs if you want the client to
  consider your connection alive.
- The client itself sends no application-level keepalive of its own
  beyond the ack idle timer (§2.7), which is not a liveness signal — a
  server should not treat client silence beyond that as a health check
  result either way.

## §8. Server extensions outside this spec

Nothing stops a server from adding its own handshake-time mechanism on
top of §1, as long as it stays additive (an unrecognized header/query
param is simply ignored by any other server or client — this is exactly
the "unknown fields ignored" rule §6 already establishes for message
bodies, extended to the handshake). One such extension exists today,
documented here for reference, not as something every server needs:

**`X-Hub-Create-Token` (chat-relay).** A capability token, transport
`{prefix}.{secret}` (both base64url), sent as a request header on the
handshake (preferred — a query-string equivalent, `?create=...`, exists
as a fallback for a client that can't set headers, but ends up in
plaintext in a reverse proxy's access log, so header is strictly
better when available). Lets a client create and claim a brand-new
`sessionId` in the same handshake that joins it — meaningful only for a
server that refuses an unknown `sessionId` by design (chat-relay 404s
one rather than creating it, unlike `mcp-hub-server`'s create-on-first-
connect behavior). If `sessionId` already exists, the token is ignored
entirely and the join proceeds normally — never mutually exclusive with
`reconnectSecret`/`name`/`agePublicKey`. The reference client
(`hubconn.DialOptions.CreateToken`, surfaced as `hub_connect`'s
`createToken` parameter) sends it as the header form only; it never
generates or discovers a token on its own — the user supplies one,
issued out of band (chat-relay: `POST /api/conversations/hub/create-
tokens`, shown once, only its hash is later stored).

**Outbound `mentions` on `msg`/`edit` (chat-relay).** A client may set
`mentions` (an array of `Mention`, §2.2's table — same field a client
receives on an incoming `msg`/`messageEdited`, reused here for the
request direction) to request real platform-native @-mentions be
attached to an outgoing send or edit:

```json
{"type": "msg", "text": "... @Steffen Heil ...",
 "mentions": [{"name": "Steffen Heil", "text": "@Steffen Heil"}]}
```

Each entry sets **exactly one** of:

- `id` — the sending platform's own directory id for who to mention.
- `peerId` — a hub `peerId`; the server resolves it to that peer's own
  identity, so a client never needs to know its own directory id.
- `name` — a display name; refused if ambiguous, never guessed at.

`text`, if set, is the exact substring already present in the outgoing
`text` to turn into the mention — omitted, it defaults to `"@"` +
the resolved display name. A server implementing this refuses the
**whole** send/edit (`error{code: "bad_request"}`) rather than deliver
it without the requested mention, on any of: zero or more than one
identifier set on an entry, the named person not a current participant
of this conversation (never looked up in a wider directory), an
ambiguous `name`, or a `text` substring not actually present in the
message. Deliberate: a mention that silently becomes plain text tells a
sender somebody was notified when nobody was — refuse outright rather
than degrade quietly, the same posture this spec already takes for
`replyTo`.

A server that doesn't implement this simply ignores the field (additive,
per §6) — `mcp-hub-server`'s own relay is exactly this case: it neither
validates nor interprets `mentions` either direction, only threads it
through to other local peers unmodified, same as it already does for
`format`/`replyTo`. The reference client sends this as `hub_send`'s/
`hub_edit`'s `mentions` parameter, validating the exactly-one-identifier
rule client-side before ever writing to the wire (a clear MCP-level
error beats an async `bad_request` refusal for a mistake this cheap to
catch locally) — but does not otherwise interpret the field, and applies
the same security note as `history`/`messageAfter`'s own read paths: a
server resolving `peerId`/`name` must check the resolved identity is
actually a participant of *this* conversation, not merely known to the
server anywhere, to avoid an unauthenticated hub session enabling
directory enumeration or cross-conversation targeting.

## §9. Server-side fan-out / backpressure guidance (non-normative)

Not part of the wire format — nothing here is observable by a
well-behaved client — but worth recording as implementer guidance, since
it was found as a real production bug during this protocol's own
bring-up on a second implementation: a naive fan-out that sequentially
`await`s a socket write per peer means **one slow or stalled peer
delays delivery to every other peer** in the same conversation, because
the loop can't reach connection N+1 until connection N's write completes
(or a keepalive eventually kills it — often tens of seconds later). This
is head-of-line blocking across unrelated peers caused by one peer's own
problem.

Recommended shape: a per-peer bounded outbound queue; fan-out enqueues a
message and returns immediately, never awaiting a socket; one writer
task per connection drains its own queue in order (preserving per-peer
delivery order — do not spawn one task per message, which can reorder
under load); on queue overflow or a write exceeding a deadline, **abort
that one connection's raw transport** rather than blocking the fan-out
or silently dropping the message from the queue. See §5's note on why a
dedicated close code for this doesn't work (a close is itself a write)
and isn't needed (an ordinary abort is already a correctly-handled
disconnect on the client side).

## Appendix: reference client's own tuning constants

For context, not requirements — a second implementation is free to
choose its own values, though matching these avoids surprising a client
built against this same reference:

| Constant | Value | Meaning |
|---|---|---|
| `pingPeriod` (server) | 30s | How often `mcp-hub-server` pings. |
| `pongWait` (client) | 100s | Client's own inactivity deadline. |
| `pongWait` (server) | 100s | Server's inactivity deadline for the client. |
| `writeWait` | 10s | Deadline for writing a single control frame. |
| `ackIdleInterval` | 60s | How long the client waits, otherwise idle, before firing a standalone `ack`. |
| `AckWaitTimeout` | 5s | How long a teams-session tool call (`hub_react`/`hub_edit`/etc.) waits for its own ack before falling back to "sent, outcome pending." |
| `closeFlushGrace` | 200ms | How long the client waits after writing its own close frame before tearing down the connection, to dodge the write/close race described in §5. |
