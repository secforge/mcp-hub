# mcp-hub: design spec

## Purpose

A minimal relay letting a Claude Code session join a shared text "session" with
other clients (most commonly another Claude Code session, but any websocket
client), exchange plain-text messages, and leave again — without either side
needing to be reachable/addressable itself. Primary use case: cross-Claude-instance
chat/coordination.

## Components

Two standalone Go binaries, no external dependencies beyond a websocket library
(`gorilla/websocket`) and an MCP server library:

- **mcp-hub-server** — the relay. Long-running process, listens on a TCP port,
  speaks the wire protocol below. Holds all session state in memory only.
- **mcp-hub-client** — an MCP server. Spawned by Claude Code over stdio when the
  MCP is loaded, exits when Claude Code exits. Holds at most one active hub
  connection per process. Exposes tools for connect/send/disconnect/receive, and
  a second CLI mode (`wait`) used as an external blocking command (see
  "Background delivery" below) — this is what actually makes message arrival
  visible to Claude without polling.

## Wire protocol (mcp-hub-server ↔ any client)

JSON lines over a websocket. Field names are camelCase.

**Joining is done via the URL path, not a message.** A client dials
`ws://host:port/<sessionId>` (e.g. `ws://localhost:8765/550e8400-e29b-41d4-a716-446655440000`);
the sessionId is the entire path (a single segment). This means any plain
websocket client — a browser, `wscat`, `curl --http1.1`, this project's own
client — can join a session with nothing more than that one URL, no
handshake message required. There used to be a separate `/ws` endpoint plus
an explicit `{"type":"join",...}` first message; that has been fully
replaced by this URL-based form.

```
GET /<sessionId>?v=<protocolVersion>&name=<untrusted>&agePublicKey=<age1...>&reconnectSecret=<opaque>
    (upgrade to websocket; v/name/agePublicKey/reconnectSecret all optional
    — see "Protocol versioning" and "Peer identity: name, age public key,
    and reconnect secret")
← {"type":"joined","peerId":"<uuid>","peerCount":<int>,"serverVersion":<int>,"name":"<sanitized>","agePublicKey":"<age1...>"}   // sent immediately
← {"type":"peerJoined","peerId":"<uuid>","name":"<sanitized>","agePublicKey":"<age1...>"}   // one per peer already in the session (see "Roster on join" below)
← {"type":"rosterComplete"}                    // marks the end of the initial roster catch-up above
← {"type":"error","message":"..."}

→ {"type":"msg","text":"..."}                                       // broadcast to everyone else
→ {"type":"msg","text":"...","to":"<uuid>"}                         // private: only to peer <uuid>
← {"type":"msg","peerId":"<uuid>","text":"...","ts":"<RFC3339>"}                       // broadcast delivery
← {"type":"msg","peerId":"<uuid>","text":"...","ts":"<RFC3339>","private":true}        // private delivery

← {"type":"peerJoined","peerId":"<uuid>","name":"<sanitized>","agePublicKey":"<age1...>"}   // another peer joined after you did
← {"type":"peerLeft","peerId":"<uuid>"}
```

`name` and `agePublicKey` are both `omitempty` on the wire — present only
when that peer actually supplied one.

Rules:

- `sessionId` (the URL path segment) must be a standard UUID string (e.g.
  `550e8400-e29b-41d4-a716-446655440000`, regex
  `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`).
  Anything else → plain HTTP `400 Bad Request`, rejected *before* any
  websocket upgrade is attempted — no connection is ever opened for a
  malformed sessionId.
- `peerId` is a random UUID assigned at join (or, if a `reconnectSecret` was
  given, possibly reused from that same secret's previous connection in
  this same still-alive session — see "Peer identity" below; note this is
  *not* driven by `agePublicKey`, deliberately). Stable for that
  connection's lifetime. It only lets recipients tell "this message came
  from the same peer as that earlier one" — an optional, untrusted
  human-readable `name` can additionally be given at connect time (see
  below).
- A session is keyed by `sessionId` and exists only in server memory. The first
  successful connection for a given `sessionId` creates the session; when the
  last connection in a session closes (cleanly or via read error), the session
  is torn down.
- Text messages only. Both ends may agree out-of-band to send HTML as the text
  payload; the hub never interprets or transforms message content.
- No authentication beyond the `sessionId` match. This is a known, accepted
  limitation for the current scope — anyone who knows (or guesses) a
  `sessionId` can join that session.
- A `msg` may set `to` (a peerId) to request *private* delivery to that one
  peer instead of a broadcast. The server delivers only to that peer and
  marks the delivered copy `"private":true`; other session members never see
  it. If `to` doesn't match anyone currently in the session (unknown,
  departed, or the sender's own peerId — sending privately to yourself is
  also rejected), the server replies `{"type":"error","message":"..."}` to
  the sender only; nothing is delivered or logged. This error reply does not
  close the connection — it's processed the same way any other server
  message is.

### Roster on join

A newly joined peer is told about every peer already in the session, using
the same `peerJoined` event they'd have seen had they been connected at the
time — one event per existing peer, sent right after `joined`, followed by a
dedicated `{"type":"rosterComplete"}` event that marks the end of that
catch-up. Existing peers are, as before, told about the *new* peer via a
single broadcast `peerJoined`.

The whole sequence — snapshotting the existing peers, running
`beforeVisible` (used by the server to send `joined` with an accurate
`peerCount`), registering the new peer, delivering its roster, sending
`rosterComplete`, and broadcasting its own `peerJoined` to everyone else —
happens in one call, `hubsession.Session.Join`, which holds the session lock
for the entire sequence. This was originally split across two calls
(`Register` then `AnnounceRoster`) with the lock released in between; that
allowed a peer included in the roster snapshot to `Leave` in the gap,
producing a phantom `peerJoined` for someone already gone with no way to
correct it. Collapsing it into one atomic, lock-held `Join` removes that
race entirely: since `Leave` also takes the session lock, it cannot
interleave with an in-progress `Join` — the roster a peer receives is always
exactly the set of peers still present at the moment `rosterComplete` is
sent. (`broadcastExceptLocked` is `broadcastExcept`'s lock-free body, used
by `Join` since it already holds the lock and can't re-lock a non-reentrant
mutex.)

Client-side (`hubconn.Conn`), `rosterComplete` is passed straight through as
a real event into the same buffer `wait`/`hub_receive` already drain — no
counting or synthesis needed. Rendered to the model as: `[hub: initial
roster complete — you now know everyone who was already in the session]`.
`Conn.RosterComplete() bool` is also available for an on-demand check.
`joined.peerCount` is still reported (informational — lets a client show
"N other peers" before the roster catch-up arrives) but the client no longer
needs it for correctness.

### Peer identity: name, age public key, and reconnect secret

A client may optionally supply, at connect time (as query params — see the
wire block above):

- `name`: a free-text display name. **Untrusted** — it's peer-controlled
  content, exactly like message text. The server sanitizes it
  (`internal/sanitize.Text`) before doing anything else with it: every
  control character (including newlines/tabs) is stripped and it's capped
  at `maxNameRunes` (64) runes, so it can never inject a fake extra line
  into the session log or smuggle control sequences into a delivered event.
  `joined.name` echoes back the sanitized value actually in use — a client
  that cares should read that back rather than assume its input survived
  unchanged. It's written into the log's `joined` line
  (`hublog.FormatJoinedEntry`) and included on every `peerJoined` event this
  peer generates (see the wire block above), which is how other peers and
  their models learn it — always still framed as untrusted, never as an
  instruction.
- `agePublicKey`: an [age](https://age-encryption.org) recipient string
  (`age1...`), for encryption only. The hub validates its *format* only —
  `internal/agekey.Valid` checks it's a syntactically well-formed bech32
  string with human-readable part `age` and a correct checksum — and never
  parses, decodes, or uses it for any cryptographic purpose. A
  syntactically invalid key is rejected with a plain HTTP `400` before the
  websocket upgrade, exactly like an invalid `sessionId`. Once accepted,
  it's distributed to every other peer exactly like `name` (echoed in
  `joined`, included on `peerJoined`), so they can encrypt messages to this
  peer with `age` themselves, entirely outside the hub's involvement — the
  hub remains a dumb relay for message content either way. **It plays no
  role in peerId reuse** — see below for why.
- `reconnectSecret`: an opaque, client-chosen string (any format — a UUID,
  a random token, whatever) used *purely* to reclaim a previous peerId on
  reconnect. Unlike `name`/`agePublicKey`, it is **never distributed to
  anyone** — not echoed in `joined`, not included in any `peerJoined` event,
  never logged. Capped at `maxReconnectSecretRunes` (256); an overlong one
  is rejected with HTTP `400` before upgrade, the same way an invalid
  `agePublicKey` is.

**Identity reuse.** `hubsession.Session` keeps a
`secretToPeerID map[string]string`, populated whenever a peer connects with
a non-empty `reconnectSecret`, and durably persisted (see below). On
`Join`, if the given secret maps to a peerID that isn't a currently-active
connection, that same peerID is reused instead of a fresh one — so a peer
that drops and reconnects with the same secret is recognized as the same
identity by anyone still watching the roster.

This is deliberately **not** keyed by `agePublicKey`, even though an
earlier version of this feature did exactly that. `agePublicKey` is
broadcast to every other peer in the session (that's the whole point — so
they can encrypt to it); keying reuse off it would mean anyone who merely
*observed* a peer's public key on the wire could reconnect presenting that
same key and be handed that peer's peerId — a straightforward impersonation
vector, since a public key by definition proves nothing about possessing
the matching private key, and the hub never asks for or verifies any proof
of that (no signing, no challenge/response — this iteration is explicitly
just "does the string match", which is why it has to be a value nobody else
ever sees). `reconnectSecret` fixes this by being a separate value that's
never distributed at all — `TestJoinNeverReusesPeerIDBasedOnAgePublicKeyAlone`
(`internal/hubsession`) and `TestObservingAgePublicKeyDoesNotAllowImpersonation`
(`internal/wsserver`) are regression tests for exactly this.

Two things bound the reuse itself:

- If the secret's previous peerID is *still* an active connection (e.g. two
  processes present the same secret into the same session concurrently),
  the new connection gets a fresh UUID instead of colliding with the live
  one — `resolvePeerIDLocked` checks `s.peers` for that ID before reusing
  it. Both the reuse and the collision-avoidance decision happen under
  `s.mu`, in the same locked section as the rest of `Join`, for the same
  atomicity reasons described above.
- Identity is scoped to "the same still-alive channel" — not to the secret
  indefinitely. See "Identity persistence across a server restart" below
  for exactly what that means now that the mapping survives more than the
  in-memory `Session` object.

### Identity persistence across a server restart

`secretToPeerID` used to live purely in the in-memory `Session`, so *any*
server restart silently reset every peer's identity, even a peer that still
held the exact `reconnectSecret` it always had — found from a real
production incident where this was surprising in practice. Fixed by
`internal/identitystore`: `Session` now durably persists (and, on
`newSession`, reloads) that mapping to a small per-session file, so it
survives a restart while still respecting the original "same still-alive
channel" scoping for an *intentional* teardown:

- **Restart** (the process dies and a new one starts): not a deliberate end
  of the channel, just an interruption — `newSession(id)` calls
  `identitystore.Load(id)`, so a fresh `Session` created for a `sessionId`
  that was already in use picks up whatever mapping was last persisted for
  it. A reconnecting peer presenting the same secret is recognized exactly
  as if the server had never restarted.
- **Intentional teardown** (the session's last peer leaves): still forgets
  the mapping, same as before restart-persistence existed —
  `Manager.Remove` (called exactly when a session becomes empty) now also
  calls `identitystore.Delete(id)`, removing the persisted file. A later
  connection with that same secret, even after this point, gets a fresh
  peerId — the channel deliberately ended, so its identities end with it.

The persisted file (`<sessionId>.secrets.json`, in the same
`MCP_HUB_LOG_DIR` the session log uses) never contains a `reconnectSecret`'s
own value — only `identitystore.HashSecret(reconnectSecret)` (SHA-256) as
the map key, matched by re-hashing an incoming secret the same way. This
means the file can't be replayed to impersonate anyone even if someone
reads it directly off disk; `Save` writes are also `0600` (owner-only) and
go through a write-to-temp-then-rename so a concurrent `Load` never
observes a partially-written file. A `Save`/`Load` failure is silently
discarded by the caller (`Session`), not fatal — restart-survival is a
convenience layered on top of the in-memory mapping the session already
works from correctly on its own, not something session correctness itself
depends on.

`sessionID` becomes part of that file's path, so `identitystore` validates
it as a well-formed UUID (`wire.IsValidID`) itself, independently of
`wsserver` already rejecting a malformed `sessionId` before a session is
ever created — defense in depth rather than trusting a single caller's
validation to hold forever. `path()` returns an error for anything else;
`Load`/`Delete` treat that the same as "nothing persisted"/"nothing to
delete" rather than propagating it, since neither has a meaningful error
path back to its caller. `TestPathTraversalSessionIDIsRejected`
(`internal/identitystore`) is the regression test.

`TestReconnectSecretSurvivesSimulatedServerRestart` and
`TestReconnectSecretDoesNotSurviveIntentionalTeardown` (`internal/hubsession`)
are the regression tests for the two restart/teardown halves.

### Protocol versioning

`wire.ProtocolVersion` (currently `1`) identifies the wire protocol's
schema. It only needs to be bumped for a genuinely *breaking* change —
purely additive changes (new optional fields, new event kinds) don't need
it, because both ends already tolerate those without any version check:
`encoding/json` silently ignores unknown fields in either direction, and
`hubconn.decodeEvent`'s `default` case silently drops any message with an
unrecognized `"type"`. Today's version-exchange feature and the
`rosterComplete` addition are themselves additive — the version stays `1`
now that this is in place; it exists for the next time a *breaking* change
is needed.

The client appends `?v=<wire.ProtocolVersion>` to the connect URL. The
server parses it (`wsserver`), defaulting to `1` if it's absent or
unparseable — **this is the permanent backward-compatible baseline, not a
temporary fallback**: any client that doesn't send a version at all
(including every plain non-`mcp-hub-client` websocket client) is assumed to
speak v1, forever. The parsed value is currently only logged
(`clientVersion`, reserved for future server-side compatibility decisions);
the server never rejects a connection over it.

The server always reports its own version back as `joined.serverVersion`.
`hubconn.Conn.ServerVersion()` exposes it; `mcptools.handleConnect` compares
it against this build's `wire.ProtocolVersion` and, on a mismatch, appends a
note to the `hub_connect` result telling Claude to prompt the user to update
`mcp-hub-client` (if the server is ahead) or that the server may need
updating (if this client is ahead).

## Transport / TLS

The server supports both `ws://` and `wss://`. TLS is optional and controlled by
whether cert/key paths are configured via CLI flags (`-tls-cert`, `-tls-key`):
if unset, the server listens plain (`ws://`); if set, it listens TLS (`wss://`).
This keeps local/dev usage simple while allowing the same binary to either
terminate TLS itself or run naked behind an nginx reverse proxy that terminates
TLS in front of it.

## Keepalive

The server pings each connection every 30s and requires a pong within 40s
(via a read deadline refreshed on every pong), closing the connection
otherwise. This detects peers that go silently unreachable (dropped by a NAT,
a sleeping laptop, a stalled process) without either side sending a clean
close — without it, such a connection would sit forever with neither side
noticing, since a blocked `ReadMessage()` only returns on data or a real
TCP-level error. Detection surfaces through the normal teardown path: the
server's read loop errors out, which fires `peerLeft` to the rest of the
session and tears the session down if that was the last peer.

The client (`hubconn.Conn`) mirrors this in the other direction. An earlier
version of this doc claimed the client didn't need its own liveness check
since gorilla/websocket's default ping handler already answers the
server's pings automatically — true, but incomplete: that only lets the
*server* detect a dead *client*. The client itself never called
`SetReadDeadline`, so `ws.ReadMessage()` blocked with no timeout; a silent
drop from the client's point of view (server process killed without a
clean close, a network partition — no TCP FIN/RST either) meant `readLoop`
never errored, `Conn.closed` never flipped true, and `Peek()`/`Drain()`
would report `connected: true` forever while anything sent into that dead
socket in the meantime was silently lost. Fixed by giving the client its
own read deadline (`pongWait`, same 40s value), reset both on every
successfully-read data message and via a custom `SetPingHandler` that
still answers the server's ping (as before) but *also* pushes the deadline
out — using the server's own periodic pings as the client's liveness
signal, rather than adding a second independent ping direction. If neither
a message nor a ping arrives within `pongWait`, `ReadMessage` times out,
`readLoop`'s existing error path fires exactly as it would for a real
socket error, and the connection is correctly marked disconnected.
`TestSilentDropIsDetectedViaReadDeadline` (`internal/hubconn`) is the
regression test.

Both `pongWait`/`writeWait` (client) and `pingPeriod`/`pongWait`/`writeWait`
(server) are package-level vars so tests can shorten them, but are
snapshotted into local variables synchronously — before the background
`readLoop`/`pingLoop` goroutine is spawned — rather than read from that
goroutine directly. Reading a mutable package var from an unsynchronized
background goroutine races (under `-race`) against a *later, unrelated*
test reassigning it for its own connection: Go's race detector flags the
absence of a happens-before edge, not actual timing overlap, and a plain
`go f()` spawn establishes that edge only up to the moment of the read
that already happened inside `Dial`/`serve`, not for every subsequent read
inside the goroutine.

## Logging (PoC)

One log file per session, named `<sessionId>.log`, created when the session is
created and appended to for its lifetime (left on disk after teardown — no
rotation/cleanup in this PoC). Every broadcast `msg` is appended as:

```
<RFC3339 timestamp> <peerId>
  <message text, original line breaks preserved, each line further
   word-wrapped only if still >100 chars, every output line indented 2 spaces>

```

and every successfully delivered *private* `msg` as:

```
<RFC3339 timestamp> <peerId> -> <targetPeerId>
  <message text, wrapped/indented identically to a broadcast entry>

```

A peer joining or leaving the session (including a ping/pong keepalive
timeout, which is treated as a leave) is appended as a single-line entry with
no body:

```
<RFC3339 timestamp> <peerId> joined
<RFC3339 timestamp> <peerId> (<name>) joined                       // sanitized name given
<RFC3339 timestamp> <peerId> agePublicKey=<age1...> joined         // agePublicKey given
<RFC3339 timestamp> <peerId> (<name>) joined (reconnectSecret set) // a secret was given, not yet reused
<RFC3339 timestamp> <peerId> (<name>) joined (reconnected)         // a secret matched a departed peer's

<RFC3339 timestamp> <peerId> left

```

`reconnectSecret`'s own value is never written to the log (nor anywhere
else — see "Peer identity" below) — only whether one was involved for this
join, and whether it actually matched a prior identity. Since peerIds are
otherwise always fresh random UUIDs, seeing the same peerId reappear in a
later `joined` line is itself indirect evidence of a `reconnected` reuse,
but `FormatJoinedEntry` makes it explicit rather than something that has to
be inferred by comparing entries.

(blank line separates all entries; a message's own line breaks are kept as
written — each source line is only further wrapped if it's still over 100
chars, and that wrapping breaks only on word boundaries, never mid-word — so
a blank line inside a message becomes an indented empty line, distinct from
the unindented blank line that separates entries). Private sends that were
rejected (unknown target) are not logged — only successfully delivered `msg`
traffic, plus
join/leave events.

## MCP client: tools

- **`hub_connect(host, sessionId?, name?, agePublicKey?, reconnectSecret?)`** — dials `host` + `/` + `sessionId` (e.g.
  `host="wss://relay.example.com:8765"` and `sessionId="550e8400-..."` dials
  `wss://relay.example.com:8765/550e8400-...`), which auto-joins as part of
  the websocket handshake, and on success spawns a background goroutine that
  reads `msg`/`peerJoined`/`peerLeft` events into an in-memory buffer for the
  lifetime of the connection. `host` is validated/normalized before dialing
  (`normalizeHost` in `internal/hubconn`): `http://`/`https://` are silently
  rewritten to `ws://`/`wss://` (an mcp-hub server is also reachable over
  plain HTTPS for humans browsing to it, so this mix-up is expected and
  harmless to auto-correct), but a `host` containing a path, query, or
  fragment is rejected outright with a clear error — guessing what the
  caller meant there would be unsafe, since `sessionId` is always appended as
  a path segment automatically and must never be included in `host` itself.
  Returns an error if a connection is already active (single connection per
  process) or if the handshake is rejected. `sessionId` is optional: if
  omitted, the client generates a fresh UUID itself and uses that to join
  (starting a brand new session, since no one else can yet know that id). On
  success, its result
  text tells Claude the exact wait command to run next (see "Background
  delivery" below) — it does not tell Claude to call `hub_receive()` as a next
  step, since the wait command's own race-fix (below) already covers anything
  buffered before the first wait starts. The result text also:
  - when `sessionId` was generated, prominently states the new `sessionId` and
    instructs Claude to share it with whoever else should join — without this,
    the session is unreachable to anyone else, since a `sessionId` is the only
    way to find it;
  - always includes a ready-to-copy invite string of the exact form
    `Connect to the hub at <host> with sessionId <sessionId>, then wait for
    messages.`, with an instruction telling Claude to propose it to the user
    so they can paste it as-is into another Claude Code session — this is
    included whether or not `sessionId` was generated, since a user may want
    to invite further peers into an existing session too;
  - always states plainly that connecting alone delivers nothing — receiving
    messages requires the returned wait command to actually be run (and
    re-run) in the background; skipping it silently means no message is ever
    noticed, so this is called out as a hard requirement, not a suggestion;
  - states how many peers were already in the session (`Conn.ExpectedPeerCount()`)
    and that a `rosterComplete` notification will follow once caught up (see
    "Roster on join"), or that there are none yet;
  - if the server's `serverVersion` doesn't match this build's
    `wire.ProtocolVersion`, appends a note telling Claude to prompt the user
    to update `mcp-hub-client` (server ahead) or that the server may need
    updating (client ahead) — see "Protocol versioning" above.

  `name`, `agePublicKey`, and `reconnectSecret` are all optional (see "Peer
  identity" above). `agePublicKey` is format-validated client-side
  (`agekey.Valid`) before even dialing, so a malformed key fails fast with
  a clear tool error instead of a round trip to the server (which validates
  it again anyway). If `name`/`agePublicKey` was given, the result text
  confirms what other peers can now see via `hub_peers()` — including the
  sanitized name, called out explicitly if it differs from what was passed
  in, so Claude never silently assumes an unsanitized value took effect. If
  `reconnectSecret` was given, the result confirms it's remembered (and
  never shared) for a future reconnect.
- **`hub_send(text, to?)`** — sends `{"type":"msg","text":...}` (broadcast) or,
  if `to` (a peerId) is given, `{"type":"msg","text":...,"to":...}` (private)
  on the active connection. Errors clearly if not connected, or if `to` is
  given but not a well-formed UUID. A private send that the server rejects
  (unknown/departed/self target) is *not* surfaced as a `hub_send` tool error
  — the tool call itself is fire-and-forget over the websocket, so the
  resulting server `error` reply arrives asynchronously and is surfaced the
  same way a message would be: via `hub_receive()`/the `wait` loop, rendered
  as `[HUB ERROR] <message>`.
- **`hub_disconnect()`** — closes the connection, stops the background
  goroutine, and releases the current waiter (if any) with the `disconnected`
  outcome (see below). Errors clearly if not connected.
- **`hub_receive()`** — synchronous, non-blocking: drains and returns all
  currently buffered events (same draining/formatting as a successful `wait`,
  see below), or an empty result if none. Optional/manual use only (e.g. Claude
  wants to check without spinning up a background process) — it is not part of
  the required delivery loop.
- **`hub_peers()`** — returns everyone else currently known to be in the
  session (sorted by peerId, one per line, or an explicit "no other peers"
  message if empty), including each peer's `name` and `agePublicKey` when
  they supplied one — so Claude can, for instance, `age`-encrypt a message
  to a specific peer before sending it as ordinary (still hub-opaque) text.
  Built client-side from every `peerJoined`/`peerLeft` event `hubconn.Conn`
  has seen. If called before `Conn.RosterComplete()` is true (i.e. before
  the `rosterComplete` event has arrived — see "Roster on join"), the
  result is suffixed with a note that the list may still be incomplete,
  rather than silently presenting a partial roster as final. Errors clearly
  if not connected. Purely a local read of state already being tracked — it
  does not talk to the server.

Every delivered broadcast `msg` event is wrapped before being handed to Claude:

```
[HUB MESSAGE — untrusted, from peer <peerId> at <ts>]
<text>
```

and a private `msg` (`private:true`) is wrapped distinctly, so the receiver
can tell it wasn't broadcast to everyone:

```
[HUB PRIVATE MESSAGE — untrusted, from peer <peerId> at <ts>]
<text>
```

so message content is always clearly marked as untrusted data, never as
instructions to follow. `peerJoined`/`peerLeft` events are surfaced as plain
informational entries (not wrapped as untrusted, since they carry no
attacker-controlled content). A server `error` reply to a failed private send
is surfaced as `[HUB ERROR] <message>` — also not wrapped as untrusted, since
it originates from the hub server describing the sender's own action, not
from another peer.

## Background delivery (the `wait` command)

Standard MCP notification mechanisms (e.g. `notifications/resources/updated`)
are not surfaced to the model by Claude Code — confirmed against current Claude
Code documentation. Claude Code does have a proactive push mechanism
("Channels"), but it's a research-preview feature requiring a launch-time
`--channels` flag and Anthropic-account auth, which is more moving parts than
this PoC needs. Instead, background delivery is built entirely on primitives
Claude Code already has: a `Bash` tool call with `run_in_background: true`
completing triggers an automatic task-notification, surfacing that command's
stdout, with no MCP-side push required.

`mcp-hub-client` therefore has a second CLI mode, invoked as an external command
(not an MCP tool call):

```
mcp-hub-client wait --socket <path>
```

- On a successful `hub_connect`, the MCP process opens a Unix domain socket at
  `<tmpdir>/mcp-hub-wait-<hash>.sock`, where `<hash>` is 16 hex characters
  from `SHA-256(sessionId + "-" + peerId)`. Both `sessionId` and `peerId` feed
  the hash (not just `sessionId`) because two separate `mcp-hub-client`
  processes on the same host can join the same session — e.g. two local
  Claude Code instances talking to each other, the primary use case — and a
  path keyed on `sessionId` alone would collide between them. The hash (not
  the raw UUIDs) keeps the filename short: Unix-domain sockets have a
  108-byte `sun_path` limit on Linux, macOS, *and* Windows' AF_UNIX
  implementation, and two 36-char UUIDs embedded directly would already
  consume ~91 bytes before the OS temp directory is even joined in — on
  Windows in particular, `%TEMP%` is often 40-50+ bytes on its own, so the
  combined path silently exceeded the limit and `net.Listen` failed. When
  that happened, `hub_connect` closed the connection it had just opened
  (since the wait socket is required), producing a join immediately followed
  by a leave in the same second — the actual failure mode this was fixed to
  avoid. This socket is unrelated to the hub websocket connection itself —
  it's purely local IPC between the MCP process and `wait` invocations of the
  same binary. It's torn down on `hub_disconnect()` (after releasing any
  waiter).
- `wait` connects to that socket and, as the very first thing, sends one mode
  byte: `0x00` (`ModeOnce`, the default) or `0x01` (`ModeFollow`, only with
  `--follow` — see below). It then blocks on a read, acts on what it
  receives, prints a short result to stdout, and always exits 0 (a wake-up is
  a normal outcome, never a process error):
  - **`message` available** — the server drains the buffer, formats the
    event(s) exactly as `hub_receive()` would (including the untrusted
    wrapper), sends them down the socket, and `wait` prints them followed by a
    trailing line instructing Claude to run the same `wait` command again in
    the background to keep receiving.
  - **`disconnected`** — sent when the hub connection ends (via
    `hub_disconnect()` or the read loop dying). `wait` prints `hub
    disconnected` with no re-run instruction.
  - **`superseded`** — the server allows only one registered waiter at a time.
    If a new `wait` connects while one is already registered, the server
    immediately sends `superseded` to the *old* one and closes it, then
    registers the new connection as the current waiter. `wait` prints an
    explicit "do NOT run this command again" message (with the `--follow`
    command as the suggested replacement) rather than the usual re-run
    trailer — `Waiter.supersededMessage()` — specifically so a model that
    otherwise follows `hub_connect`'s generic "run it again on completion"
    instruction too literally doesn't spawn a replacement for the loser,
    which would immediately supersede whatever legitimately still-active
    wait was already running, and so on: exactly one wait should ever be
    kept running at a time.
- **Race fix (initial registration)**: when a `wait` connection is accepted,
  the server checks immediately whether the buffer already has unread
  events. If so, it responds with `message` right away instead of
  registering the connection as a pending waiter. This closes the gap
  between "nothing was buffered a moment ago" and "the `wait` process
  actually started" (including the moment right after `hub_connect`
  returns).
- **Race fix (re-registration, `--follow`)**: a second, subtler version of
  the same problem existed in `deliver`'s re-registration for the *next*
  event after a `--follow` delivery — found from a real production incident
  where a message was sent and successfully buffered, but the receiving
  side's `wait --follow` process never printed it and just sat there,
  looking like a healthy listener indefinitely. `deliver`'s tail used to
  call `source.Peek()` *before* acquiring `w.mu`, then separately lock to
  set `w.current`. `Poke()` (triggered by `hubconn.Conn.OnActivity` when a
  new event lands) reads and clears `w.current` under that same lock — so
  a `Poke()` landing in the gap between the unlocked `Peek()` and the
  locked registration would find `w.current` still `nil` (from the
  *delivery in progress*, which hadn't re-registered yet), silently no-op
  (`Poke` does nothing when nothing is registered), and the event that had
  in fact already arrived would never be delivered: `deliver` would go on
  to register based on its now-stale `Peek()` result, and nothing would
  ever call `Poke()` again for that connection. Fixed by moving `Peek()`
  inside the same lock hold used to set `w.current` (in both `deliver`'s
  re-registration and `handleAccept`'s initial registration), so the two
  operations can no longer be interleaved: whichever of `Poke()` or
  registration acquires `w.mu` first, the other is guaranteed to observe
  accurate state once it's their turn — no window remains in which an
  event can land unnoticed. `TestFollowNeverLosesAnEventToRegistrationRace`
  is the regression test.

### `wait --follow`: an alternative for harnesses with per-line notifications

`mcp-hub-client wait --socket <path> --follow` sends `ModeFollow` instead of
`ModeOnce`. In this mode the server does *not* close the connection after one
delivery — it writes `<formatted events>\n\n` (no "run again" trailer, since
none is needed) and keeps the same connection registered as the current
waiter for the *next* event too, repeating indefinitely until the connection
is superseded by a new `wait` or the hub disconnects (both of which still end
the stream exactly as they would for a one-shot `wait`).

This exists because the default `ModeOnce` flow assumes a harness whose
background-task notification fires on process *completion* — which is what
Claude Code's own backgroundable shell tool does. For a harness with a tool
that notifies per *line* of new output from a long-running background
process instead (this project's own dev environment has `Monitor` for
exactly that), `--follow` is a better fit: one long-lived connection, no
per-message process-respawn, and a notification per delivered chunk.

`hub_connect`'s result text gives Claude both commands up front and states
the choice explicitly — see `mcptools.waitBlock` — rather than leaving
Claude to default to `ModeOnce` and rediscover `--follow` on its own: prefer
`--follow` backgrounded via a Monitor-style tool when the harness has one,
falling back to the `ModeOnce` run-it-again loop otherwise. It also says
explicitly: use the streaming tool *directly* on `--follow`, don't wrap it
in a hand-rolled shell loop or a manual `tee`/`grep` filter pipeline — a
harness with a real per-line notification tool was observed doing exactly
that anyway (`while true; do wait --socket ...; done | tee -a <logfile> |
grep -E "..."`), which works but is unnecessary in that case: it defeats
`--follow`'s one-connection design (the loop keeps respawning `wait`) and
`grep`'s per-line filtering can silently drop the body lines of a
multi-line message (only the `[HUB MESSAGE...]` header line matches the
filter pattern) from what's shown, even though `tee` preserves everything
in the log file untouched.

`hub_connect` also detects a harness that has *no* such tool at all and
adjusts instead of assuming Claude Code's own capabilities everywhere:
`clientName(ctx)` reads the connecting MCP client's self-reported
`clientInfo.name` from the standard MCP `initialize` handshake (exposed via
`server.ClientSessionFromContext`), and `looksLikeCodex` matches it
case-insensitively against `"codex"` (a loose substring match, not exact —
OpenAI's own docs show `clientInfo.name` varying by integration, e.g.
`"codex_vscode"`, and a false positive here is far cheaper than a false
negative). As of writing, Codex CLI has no shipped equivalent to a
`Monitor`-style per-line notification tool — it's an open, unshipped
proposal (https://github.com/openai/codex/issues/29922) — so for a
Codex-detected client, the generic "prefer `--follow` with a streaming
tool" sentence is replaced with an explicit warning that `--follow` will
not notify it and to use `ModeOnce`, re-run each time it completes,
instead.

Expected steady-state loop: `hub_connect` → run `wait --follow` (or
`ModeOnce`, re-run each time) in the background → harness notifies on each
delivery → Claude reads the message(s) directly from that command's stdout
and processes them. No additional tool call is required in the steady
state.

## Error handling

- Malformed/invalid `sessionId` in the URL path → plain HTTP `400`, no
  websocket connection ever opened, no session state touched.
- `hub_send` / `hub_disconnect` with no active connection → clear tool error,
  no crash.
- Server: a peer's socket drops without a clean close → treated as a leave once
  the read loop errors; fires `peerLeft`; session torn down if that was the last
  peer.
- Client: if the background read loop dies (server restart, network blip), the
  client treats this the same as an explicit `hub_disconnect()` — any
  registered `wait` waiter is released with `disconnected`, and a subsequent
  `hub_receive()`/`hub_connect()` reflects the disconnected state, so Claude
  finds out instead of silently hearing nothing further.

## Testing

- **Server**: unit tests for the session manager (join/leave/broadcast/teardown,
  bad sessionId rejection) using `httptest.Server` plus a real websocket client
  — no mocking of the socket itself.
- **Client**: unit tests for the buffer/waiter logic (including the
  race-fix and the single-waiter supersede behavior) against a fake hub
  connection (interface-based) and an in-process wait socket; one manual
  end-to-end smoke test (real server + real client tool calls + a real `wait`
  subprocess) since this is a PoC.

## Explicit non-goals (this iteration)

- No authentication beyond the sessionId match; no rate limiting; no message
  history/replay for late joiners.
- No multi-connection-per-client-process support (one hub connection at a time
  per mcp-hub-client instance).
- No binary/file transfer — text only.
- No persistence beyond the plain-text PoC log files.
