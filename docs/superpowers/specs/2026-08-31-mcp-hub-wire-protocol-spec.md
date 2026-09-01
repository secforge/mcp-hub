# mcp-hub wire protocol — implementer's spec

Audience: anyone building a second server implementation (or a second
client) against this protocol. This is the contract — where the current
Go source (`internal/wire`, `internal/hubconn`, `internal/wsserver`)
disagrees with it, that's marked explicitly as a **known gap**, and the
spec wins; don't replicate the gap on purpose.

## 0. The one thing to get right first: nothing here is "bridge-only"

Every message type in this protocol — `history`, `reaction`, `edit`,
`delete`, `ack`, all their replies — is available on **any** connection,
plain or bridge. There is no `isBridge` flag on the wire and no
server-type check anywhere in the client that gates *sending* one of
these. The client will happily send a `history` or `reaction` request
over a connection to any server that accepts the initial handshake.

What actually varies by server is **which requests get a real answer**:

- `mcp-hub-server`'s own relay (`internal/wsserver`) implements only
  `msg` (broadcast and directed). Every other inbound request type —
  `history`, `reaction`, `edit`, `delete`, `ack` — is silently dropped
  (`internal/wsserver/server.go`'s read loop discards anything whose
  `type` isn't `"msg"`). This is a *deployment's* limited feature set,
  not a protocol restriction.
- A bridge server (chat-relay) implements the rest because it has real
  history, write access, and a reason to track read position.

So: implement whatever subset of this spec makes sense for your server,
and advertise capability via `Joined` fields (`historyAfter`,
`historyLimitMax`) where the spec defines one — there is no other
capability-negotiation mechanism. For everything without a capability
flag (reactions, edits, deletes, ack), a client has no way to know in
advance whether your server will act on it; it just sends the request
and waits to see whether an ack/error/no-response follows. Silence is a
valid answer only in the sense that `mcp-hub-server` today gives none at
all — a real implementation should not do that; see §5.

## 1. Handshake

**URL:** `{scheme}://{host}/{sessionId}?v={protocolVersion}&name={name}&agePublicKey={key}&reconnectSecret={secret}`

- `scheme`: `ws`/`wss` only. (The client also accepts `http`/`https` on
  the *host* it's given and rewrites them to `ws`/`wss` — that's a
  client-side convenience, not part of the wire contract.)
- `sessionId` (path segment, required): must match
  `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`
  (a standard UUID string, case-insensitive). `mcp-hub-server` rejects an
  invalid one with a plain HTTP 400 *before* the websocket upgrade.
- `v` (query, optional): the client's `ProtocolVersion`. Currently
  always `1` if sent. Absent is treated as version 1 — the permanent
  backward-compatible default, not a fallback that will later change
  meaning. A server may log a mismatch; it must not refuse the
  connection over it (see §6).
- `name` (query, optional): free-text display name. `mcp-hub-server`
  sanitizes it (strips control characters, caps length at 64 runes) and
  echoes back the sanitized form in `Joined.name` — a client should
  treat whatever comes back as authoritative, not what it sent.
- `agePublicKey` (query, optional): must be a well-formed age recipient
  string — bech32, human-readable part `"age"`, 7–90 chars total, valid
  checksum. `mcp-hub-server` rejects a malformed one with HTTP 400
  *before* upgrade. Never parsed or used cryptographically server-side —
  purely distributed to other peers as-is.
- `reconnectSecret` (query, optional at the wire level — see §4 for why
  the client always sends one anyway): any string, capped at 256 runes.
  Longer is rejected with HTTP 400 before upgrade.

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
| `latestCursor` | string\|null | no | Newest message cursor this server holds, if any. Bridge-capability field — omit/null if you have no history. |
| `historyAfter` | bool | no | Capability flag — true only if you implement forward paging (`history.after`, §2.6). Omit or false otherwise. |
| `historyLimitMax` | int | no | Your cap on a single `history` request's `limit`. Omit/zero if uncapped or you don't implement history. |
| `canSend` | bool | no | Whether sending is currently permitted — a snapshot, not a guarantee (re-checked per send). |
| `conversationKind` | string | no | e.g. `"oneOnOne"`, `"group"`, `"meeting"` — free text, not a closed enum. |
| `topic` | string\|null | no | Display name/topic of what was joined, if applicable. |

### 2.2 `msg` (both directions)

| Field | Type | Required | Direction | Notes |
|---|---|---|---|---|
| `type` | `"msg"` | yes | both | |
| `peerId` | string | client←server only | server | Sender's peerId. Absent on the client→server request (the connection *is* the sender). |
| `text` | string | yes | both | |
| `ts` | string | server→client | server | Timestamp, server-defined format (RFC3339 in `mcp-hub-server`'s case). |
| `to` | string | no | client→server | Set to request directed (private) delivery to one peerId instead of broadcast. |
| `private` | bool | no | server→client | Set by the server on a delivered directed message. |
| `historical` | bool | no | server→client | True if this is answering a `history` request rather than live traffic. |
| `externalId` | string | no | server→client | Bridge-only concept: this server's own id for the message, correlating it with an earlier `sendAck`. |
| `own` | bool | no | server→client | True if *this exact connection* sent it. A receiving client's own policy decision whether to treat this as wake-worthy — see §3 for what the reference client does. |
| `cursor` | string | no | server→client | This message's own opaque position — pass back as `history.before`/`history.after`. |
| `ackCursor` | string | no | client→server | Piggybacked read receipt — see §2.7. |

A plain broadcast (`to` omitted) is never echoed back to its own sender
by `mcp-hub-server`'s relay logic (broadcast excludes the sender) — if
your server does the same, don't expect the sender to see its own `msg`
come back; `sendAck` (§2.8) exists for exactly this gap on a bridge that
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
  — a bridge-side policy gate refused the action (`retryable:false`
  typically — check the specific server's semantics, this spec doesn't
  mandate retryability per code).
- `invalid_credential` — relay auth failure (bridge-specific, not part
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

### 2.6 `history` (client → server) / `historyComplete` (server → client)

Request:

| Field | Type | Required | Notes |
|---|---|---|---|
| `type` | `"history"` | yes | |
| `before` | string | no | Backward paging — see below. |
| `after` | string | no | Forward paging — see below. Only send this if the server's `Joined.historyAfter` was true. |
| `limit` | int | no | Server may cap lower than requested. |
| `ackCursor` | string | no | Piggybacked read receipt — §2.7. |

Send **at most one** of `before`/`after`. If a server receives both, it
should prefer `before` and should treat it as a client bug (not
something to silently paper over) — a well-behaved client never does
this.

- **`before`** (backward): omitted (with `after` also empty) means "the
  most recent `limit` messages." Otherwise the page ends strictly
  before the given cursor — exclusive, so paging further back means
  repeatedly passing the *oldest* cursor seen so far, never repeating a
  boundary message. **This can only reach older messages, never newer
  ones — it cannot be used to fill a reconnect gap.**
- **`after`** (forward): the page starts strictly after the given
  cursor — exclusive. This is what a reconnecting client should use to
  fetch exactly what arrived while it was disconnected, passing the
  last cursor it actually consumed (see §2.7's `lastConsumed` — this is
  the same value). Only meaningful if you advertised
  `Joined.historyAfter: true`.

Response: a burst of `msg` events (each `historical: true`), terminated
by:

```json
{"type": "historyComplete"}
```

— sent even for an empty result, so a client gets a positive "there is
no more" rather than inferring completion from a traffic gap.

### 2.7 `ack` (both directions) — read receipts

This is the read-cursor / read-receipt mechanism. It answers "has the
model/client actually consumed this event, not just received it" — see
the design rationale in `docs/superpowers/specs/2026-08-21-mcp-hub-design.md`'s
"Read receipts" section if you want the full history of why this
exists.

**Piggybacked form** (preferred): any client→server request
(`msg`/`reaction`/`edit`/`delete`/`history`) may carry an `ackCursor`
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

### 2.8 Reactions / edits / deletes (client → server) and their echoes

Requests:

```json
{"type": "reaction", "externalId": "...", "reaction": "👍", "action": "add", "ackCursor": "..."}
{"type": "edit", "externalId": "...", "text": "...", "ackCursor": "..."}
{"type": "delete", "externalId": "...", "ackCursor": "..."}
```

`action` is `"add"` or `"remove"`. `reaction` is an open string — do not
validate against a closed set; whatever the underlying platform reports
is authoritative (real data has included things like `"Eyes"` and
`"Question mark"` alongside `"Like"`).

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
{"type": "messageEdited", "externalId": "...", "text": "...", "ts": "...", "own": false}
{"type": "messageDeleted", "externalId": "...", "cursor": "...", "ts": "...", "own": false}
```

Notes:
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
  reassigns the same `peerId` as before, **if and only if** the previous
  connection holding that secret is not currently live. If it *is*
  still connected, a fresh `peerId` is assigned instead — never collide
  two live connections onto one identity.
- The secret is never distributed to any other peer — it's a private
  channel between one client and the server, unlike `agePublicKey`
  (which is broadcast) or `peerId` (which is public within the session).
  Do not use anything publicly visible (like `agePublicKey`) to grant
  identity reuse — that would let anyone who observed it impersonate
  the original holder.
- The mapping should survive a server restart and even the session
  becoming fully empty — a reconnect months later with the same secret
  should still reclaim the same `peerId`, unless you deliberately expire
  it (bridge-specific link/session lifetimes are your own policy, not
  part of this identity contract).

## 5. Close codes

| Code | Meaning | Client behavior |
|---|---|---|
| 1000 (NormalClosure) | Deliberate, clean disconnect by either side | No note surfaced — this is the expected, unremarkable case. The reference client sends this with description `"client disconnect"` on every intentional close (e.g. `hub_disconnect`). |
| 1001 (GoingAway) | Also treated as an ordinary, expected close | Same as 1000 — no note. |
| 1006 (AbnormalClosure) | No close frame received at all — network death, crash, or (historically, now fixed client-side) a close frame written but not flushed before the underlying connection was torn down | Surfaced to the model/caller as "connection assumed dead" plus (if a read timeout, not a close frame, triggered it) how long since the last frame seen. |
| 4001 | Bridge-specific: credential revoked | Client surfaces "(revoked — do not reconnect)". |
| 4002 | Bridge-specific: credential/link expired | Client surfaces "(expired — do not reconnect)". |
| 4003 | Bridge-specific: the underlying conversation became unavailable | Client surfaces "(conversation unavailable — do not reconnect)". |
| any other non-1000/1001 code | Generic | Client surfaces "(connection closed, code N)" — no specific guidance. |

4001–4003 are an existing convention for "this credential is dead, don't
retry" — if your server has an analogous concept (e.g. a superseded
link), you may mint a new code in the same private range (4000–4999);
the reference client will fall back to the generic "(connection closed,
code N)" note for anything it doesn't specifically recognize, which is
safe but less informative — worth coordinating a code and getting it
added client-side if the case is common enough to matter.

`mcp-hub-server`'s relay never sends 4001–4003 — those are entirely a
bridge-server convention.

A server should always prefer sending an `error` event (§2.5) before
closing, when the close reason is something the model could act on
(e.g. a dead credential) — the close code is a *secondary*, best-effort
signal for the case where the `error` event itself didn't make it
through in time, not a replacement for it.

## 6. Versioning

`ProtocolVersion` is currently `1`. The policy: bump it only for a
genuinely breaking change. New optional fields and new event kinds are
additive and don't need it — `encoding/json` ignores unknown fields on
decode, and an unrecognized `type` is silently dropped rather than
erroring. A server should follow the same policy: don't require a
specific `v` to accept a connection.

If `Joined.serverVersion` differs from the client's own `ProtocolVersion`,
the reference client does **not** refuse or alter behavior — it's purely
informational, surfaced in `hub_connect`'s result text as a hint to the
user ("consider updating mcp-hub-client" if the server is newer,
"the server may need updating" if the server is older). There is no
protocol-level negotiation.

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
  a bridge implementer whose framework's keepalive was PONG-only chased
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
| `AckWaitTimeout` | 5s | How long a bridge-session tool call (`hub_react`/`hub_edit`/etc.) waits for its own ack before falling back to "sent, outcome pending." |
| `closeFlushGrace` | 200ms | How long the client waits after writing its own close frame before tearing down the connection, to dodge the write/close race described in §5. |
