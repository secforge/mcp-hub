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
- Identity, once established for a `reconnectSecret`, persists
  indefinitely — see "Identity persistence beyond the in-memory Session"
  below.

### Identity persistence beyond the in-memory Session

`secretToPeerID` used to live purely in the in-memory `Session`, so *any*
server restart silently reset every peer's identity, even a peer that still
held the exact `reconnectSecret` it always had — found from a real
production incident where this was surprising in practice. Fixed by
`internal/identitystore`: `Session` now durably persists (and, on
`newSession`, reloads) that mapping to a small per-session file.

This survives more than just a restart. An earlier version of this design
also had `Manager.Remove` (called exactly when a session becomes empty)
delete the persisted mapping, on the reasoning that "the same still-alive
channel" scoping should mean an intentional teardown forgets identity too.
That was explicitly reversed: `reconnectSecret` identity is meant to work
even if the session was fully torn down (everyone left) and only later
reconstituted by a new connection reusing the same `sessionId` — so
`Manager.Remove` no longer touches the persisted file at all, only the
in-memory `Session`. There is currently no expiry or cleanup for these
files, matching this project's existing PoC-log precedent (also never
rotated/cleaned) — a mapping, once written, is recognized for as long as
the file exists.

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
`Load` treats that the same as "nothing persisted" rather than propagating
it, since it has no meaningful error path back to its caller.
`TestPathTraversalSessionIDIsRejected` (`internal/identitystore`) is the
regression test.

`TestReconnectSecretSurvivesSimulatedServerRestart` and
`TestReconnectSecretSurvivesIntentionalTeardown` (`internal/hubsession`)
are the regression tests for the restart and teardown halves respectively
— both now prove the mapping *does* survive, matching the reversal above.

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

- **`hub_connect(host, sessionId?, name?, agePublicKey?, reconnectSecret)`** — dials `host` + `/` + `sessionId` (e.g.
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
    messages.`, with an instruction telling Claude it *must* relay this to
    the user verbatim (not paraphrase, summarize, or omit it) before doing
    anything else, so they can paste it as-is into another Claude Code (or
    other AI) session — this is included whether or not `sessionId` was
    generated, since a user may want to invite further peers into an
    existing session too. This was strengthened from an earlier, softer
    "propose this to the user" phrasing after finding that a weaker
    instruction was too easy for a model to silently drop or compress away;
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

  `name` and `agePublicKey` are optional; `reconnectSecret` is declared
  `mcp.Required()` in the tool schema — Claude must always pass one, minting
  its own if the user hasn't given it one to reuse (see "Peer identity"
  above). This is enforced only at the MCP tool-schema level (`mcp-go`'s
  own input validation rejects a `tools/call` missing it before
  `handleConnect` ever runs), deliberately *not* on the hub server itself:
  the websocket handshake (`wsserver`) still accepts a connection with no
  `reconnectSecret` at all, since a non-MCP client (a browser, `wscat`,
  a hand-rolled script) has no such contract to honor and shouldn't be
  forced into one. `agePublicKey` is format-validated client-side
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
- **`hub_wait()`** — blocks until an event is buffered or the hub
  disconnects, then returns it: the direct MCP-tool equivalent of running
  the `wait` CLI binary, for a harness that can't background or persist a
  process at all (Codex — see "`wait --follow`" below for why
  `hub_connect` recommends this specifically to it, and by how much).
  Internally polls `Conn.Peek()` every `waitPollInterval` (100ms) rather
  than being event-driven like the CLI path — `hubconn.Conn.OnActivity`
  holds only a single callback, already claimed by the waiter socket for
  the CLI `wait` command, and polling this rarely is cheap enough not to
  warrant a multi-listener redesign just for this. Respects context
  cancellation: if the caller's `ctx` is cancelled (`mark3labs/mcp-go`
  wires MCP's `notifications/cancelled` into the per-request context — see
  below), `hub_wait` returns promptly instead of polling forever with
  nothing left listening for the result. Whether that actually happens
  depends on the *client* sending that notification when its own
  tool-call timeout elapses, which the MCP spec makes optional, not
  guaranteed — a client that silently abandons the call instead leaves the
  blocked goroutine polling until something else intervenes.

  That "something else" is deliberate: a new `hub_wait` call always
  supersedes one already in flight, mirroring `waiter.Waiter`'s single-
  registered-waiter design for the CLI wait socket, for the identical
  reason. Without this, two genuinely concurrent calls (most plausibly the
  abandoned-retry case above, but nothing prevents it otherwise) would
  independently poll the same buffer and race for whichever event arrives
  first via `Drain` — destructive, so only one caller ever sees it — and
  the loser would sit blocked waiting for a *different* event that might
  never come, with no indication anything was "stolen." `Hub` tracks the
  in-flight call's own `context.CancelFunc` (`waitCancel`, guarded by
  `waitMu`) plus a generation counter (`waitGen`) so a completing call only
  clears the field if it's still the current one, not a stale write from a
  call that's already been superseded. The superseded call's `innerCtx`
  (derived from the caller's own `ctx` via `context.WithCancel`) fires
  either way; `handleWait` distinguishes a real cancellation from being
  superseded by checking whether the *outer* `ctx.Err()` is non-nil —
  superseded returns a plain `"superseded by a newer hub_wait call"` text
  result, not an error, since nothing about the request itself failed.
  `TestHubWaitNewCallSupersedesInFlightOne` is the regression test.

  A successful delivery (real content, connection still up — not the
  "hub disconnected" or "superseded" outcomes) is prefixed with
  `waitAgainReminder`, a fixed reminder telling the model to call
  `hub_wait` again immediately, *before* the delivered content, not after
  it. This is deliberate placement, not just phrasing: `hub_wait` is a
  single blocking call with no follow-up chunk to fall back on, so if
  whatever's reading the result is cut off partway through — a client-side
  read timeout, a truncated display of a large result — a trailing
  reminder is exactly the part most likely to never be seen, while a
  leading one survives even a truncated read. The CLI `wait` binary's
  default ("once") mode carries the identical fix for the identical
  reason: `deliver()` in `internal/waiter/waiter.go` now writes "Run this
  command again to keep receiving: ..." *before* the formatted event
  content instead of after it. Neither change touches `wait --follow` or
  its `ModeFollow` delivery path — a `--follow` connection keeps receiving
  indefinitely over the same connection, so there's nothing to remind it
  to restart. Regression tests: `TestHubWaitReturnsImmediatelyWhenAlreadyBuffered`
  (asserts the reminder leads) and `TestWaitDeliversAlreadyBufferedEvent`
  (same, for the CLI path).
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

- **Disconnect detection is uniform across every tool, not just
  `hub_receive`/`hub_wait`.** `hubconn.Conn` tracks `closed` (flipped by the
  read loop once `ws.ReadMessage` errors — a server restart, a network
  drop, `kill -9` on the server, anything short of a clean local
  `hub_disconnect()`) and exposes it via `Conn.Connected()`. Before this,
  only `hub_receive`/`hub_wait` actually checked it (via `Peek`/`Drain`'s
  own `connected` return value); `hub_send` and `hub_peers` did not,
  so — the concrete bug this fixed — `hub_peers` kept confidently
  returning the last-known roster from before a silent drop, with nothing
  telling Claude it was stale, and `hub_send` would just attempt (and fail
  with a raw websocket error, not a clear "disconnected") a write into a
  socket already known to be dead. Every tool now either checks
  `Conn.Connected()` up front (`hub_send`, `hub_peers`) or, where it
  already naturally observes the connection's state as a side effect
  (`hub_receive`'s `Drain`, `hub_wait`'s `Peek`/`Drain` loop), reports
  `"hub disconnected"` uniformly.
  `hub_receive`/`hub_wait` still prefer surfacing any final buffered
  content over a bare disconnect note (e.g. a message that arrived right
  before the read loop errored out) — appending `"\n\nhub disconnected"`
  rather than discarding it, which `hub_receive` used to do unconditionally
  whenever `Drain` reported `connected=false`, silently dropping real
  content.

- **Teardown of a dead connection is automatic, not just reactive.**
  `Hub.conn`/`Hub.waiter` are guarded by `Hub.mu` and only ever mutated
  through `activeConn`/`setActiveConn`/`clearActiveConn`/`teardownIfCurrent`
  — needed because, beyond the per-call checks above, `handleConnect` now
  wires `conn.OnActivity` to do more than just `waiter.Waiter.Poke()`: it
  also checks `conn.Connected()` after every activity callback (which
  `readLoop` invokes once more, with `closed` already set, as the very
  last thing it does before returning) and, the moment it sees the
  connection has died, calls `Hub.teardownIfCurrent(conn)` right there from
  the `Conn`'s own background read goroutine — no tool call needs to
  happen first. `teardownIfCurrent` only clears `Hub.conn`/`Hub.waiter` if
  they still point at exactly the `Conn` that died (compare-and-clear under
  `Hub.mu`), so a notification about a connection that's already been
  superseded by a fresh `hub_connect`, or already cleared by an explicit
  `hub_disconnect`, can never wrongly tear down whatever replaced it. This
  means:
  - a blocked `hub_wait` call (or a registered CLI `wait --follow`, via the
    same `OnActivity` → `Poke` → `deliver` path) terminates immediately
    instead of polling a dead connection forever;
  - the wait socket itself stops accepting new connections and its file is
    removed within moments of the drop, instead of lingering indefinitely
    just to tell each new connection "hub disconnected";
  - a subsequent `hub_connect` almost always finds `Hub.conn` already `nil`
    and proceeds directly — the "already connected; call hub_disconnect
    first" error only fires for a connection that's genuinely still alive.
    `handleConnect` also runs the same `teardownIfCurrent` itself, on
    entry, as a fallback for the rare window where a caller gets there
    before the automatic teardown above has run.
  Regression tests: `TestPeersToolReportsDisconnectInsteadOfStaleRoster`,
  `TestSendToolReportsDisconnectInsteadOfAttemptingASend`,
  `TestConnectAfterSilentDisconnectDoesNotRequireExplicitDisconnect`,
  `TestDisconnectDetectedAutomaticallyWithoutAnyToolCall`.

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

## Persisted connection identity (`mcp-hub-client`)

`reconnectSecret` on `hub_connect` is now optional — `mcp-hub-client`
manages it itself via a new `internal/connstore` package (one JSON file
per machine, `os.UserConfigDir()/mcp-hub/connections.json`, keyed by
`project`+`host`+`sessionId` — see "Project-scoped identity" below for
why `project` was added after this first shipped), rather than requiring
the model to remember and repeat a secret across turns/sessions. Omitted
with no prior entry for that target: one is generated and stored after a
successful connect.
Omitted with a prior entry: it's reused automatically, reassigning the
same `peerId`. Passed explicitly: always wins, unconditionally — this is
the entire "reject the automatic default" mechanism, no separate flag or
tool. A new `hub_list_connections` tool lists stored entries (never the
secret itself). `hub_connect`'s own tool description gets a note, computed
once at `Register()` time (= process launch), when any stored entry is
still marked open from a connection that never got an explicit
`hub_disconnect` — the best available mechanism given MCP's total lack of
a server-initiated push into the model's context, not a guaranteed
notification. Full rationale and design: `docs/superpowers/specs/2026-09-01-mcp-hub-client-connection-store-design.md`.

`Hub.Shutdown()` (`cmd/mcp-hub-client/main.go`, called once after
`server.ServeStdio` returns, before any `os.Exit`) tears the active
connection down the same way `hub_disconnect` does, on both paths
`mcp-go`'s `ServeStdio` already handles for us — `SIGTERM`/`SIGINT` (it
installs a handler that cancels its context) and stdin EOF (the MCP
client closing the pipe). This is what keeps a `connstore` entry from
being left "still marked open" on an ordinary session end, and — since
closing the wait socket makes a backgrounded `wait --follow` CLI process
see EOF and exit on its own — is what stops that process from lingering
as an orphaned background job the harness has to warn about. Cannot run
on a hard `SIGKILL`, which no process can catch in any language.

### Project-scoped identity, and session superseding

Two related bugs, both surfaced live over the hub by chat-relay's author
debugging a real multi-agent collision (several agents on one machine all
showing as "Claude" and losing continuity across reconnects):

1. **`connstore` was one file per *machine*, not per project.** Every
   `mcp-hub-client` process on a box — regardless of which Claude Code
   project spawned it — shared one `connections.json`, so two unrelated
   agents connecting to the same `host`+`sessionId` (e.g. the same shared
   coordination session) would silently reuse each other's
   `reconnectSecret`. Fixed by adding `Project` to `connstore.Target`/
   `Entry` and the storage key (`internal/connstore/store.go`).
   `connstore.CurrentProject()` resolves it: an `MCP_HUB_PROJECT_DIR`
   override (also how tests get isolation) if set, else — preferably —
   the MCP client's own advertised **roots** (`roots/list`, via
   `server.WithRoots()` + `MCPServer.RequestRoots`, bounded by a 2s
   `rootsRequestTimeout` since `RequestRoots` has no built-in one and a
   client that claims support but never replies would otherwise hang a
   connect call), falling back to `os.Getwd()` if the client has no
   `ClientSession` in context, doesn't support roots, times out, or
   reports none. Roots is the actual spec mechanism for "what project(s)
   does the client have open"; `$PWD` is merely what this process
   happened to inherit at launch — usually but not necessarily the same
   thing, and the only option when roots isn't answered. `hub_connect`/
   `hub_list_connections` both resolve project the same way
   (`mcptools.projectForConnect`), so their views stay consistent; `hub_list_connections`'s
   output now labels each entry `[this project]` or
   `[other project: ...]`.

2. **A live identity collision assigned a fresh, unrelated peerId instead
   of taking over.** `hubsession.Session.resolvePeerIDLocked` used to
   fall back to a brand new UUID whenever a presented `reconnectSecret`
   matched a peerId that was *still connected* — silently defeating the
   entire point of `reconnectSecret` for exactly the case that needs it
   most: a fast reconnect after an abrupt drop racing the server's own
   detection that the old socket died (case 2 chat-relay's author
   diagnosed — the *interesting* one: reconnecting faster makes this
   *worse*, not better, since less time has passed for the server to
   notice the old connection is gone). Per explicit user direction
   ("this is PRECISELY the case where the server should kill the OLD
   connection... IDs are assigned by the server then"), this is now a
   **supersede**, not a collision-avoidance fresh-ID: `Session.Join`
   force-disconnects the still-live holder
   (`Peer.Close(hubsession.SupersededCloseCode, ...)`,
   `SupersededCloseCode = 4004`, coordinated with chat-relay's own
   identical convention for its LINK sessions rather than picked
   independently — see the wire protocol spec's close-codes section) and
   hands its peerId straight to the newcomer. No `peerLeft`/`peerJoined`
   is broadcast for this — from every other peer's perspective this
   identity never left, it just changed which connection holds it.
   `reused` is still `true` in this case (the peerId really was
   reclaimed, just by force).
   - **The stale-Leave race this creates, and its fix.** The superseded
     connection's own transport dies asynchronously and independently —
     its `serve` goroutine (websocket) or nothing at all (see below)
     eventually notices and calls `Session.Leave` on itself, with no idea
     it was ever superseded. By then `Join` has already deleted the old
     map entry and inserted a new `Peer` under the same id; a naive
     delete-by-id in `Leave` would silently evict the *new*, legitimately
     live peer and broadcast a false `peerLeft` for an identity that
     never actually left. Fixed by giving `Leave` an identity check
     (`s.peers[p.ID()] == p`, safe because every `Peer` implementation
     here is a pointer) — a stale `Leave` call becomes a safe no-op
     instead.
   - **Two `Peer.Close` implementations, two different fidelities.**
     `wsserver`'s `peer.Close` sends a real close frame
     (`WriteControl`+`FormatCloseMessage`) and closes the socket, which
     makes the old connection's blocked `ReadMessage` return an error and
     run its own normal teardown on its own goroutine — `Close` itself
     never touches `Session` state. `httpmcp`'s `httpPeer.Close` has no
     real transport to sever (it's an in-process event buffer, not a
     socket) — it delivers a synthetic `error` event instead, which
     surfaces through the peer's next `hub_receive`/`hub_wait` like any
     other server-sent error. Known, explicitly-flagged simplification:
     the owning `httpHub` (`internal/httpmcp/hub.go`) has no background
     loop watching for this the way `wsserver`'s peer does, so nothing
     clears `hub.p` — a subsequent `hub_send` against the now-superseded
     connection still nominally reaches the session under a peerId it no
     longer owns, until an explicit `hub_disconnect`. Full disconnect
     detection for that in-process path is a separate gap, not solved
     here.
   - **Test-suite fallout.** Several `mcptools` tests simulated "two
     independent peers in one session" via two separate `*Hub` instances
     in the same test process connecting to the same target with no
     explicit `reconnectSecret` — which, before this change, coincidentally
     worked (each got its own fresh peerId) but now correctly supersedes
     (same `Project`+`Host`+`SessionID` ⇒ same auto-managed secret ⇒ the
     second `Hub` kills the first's connection). Fixed via a small test
     helper, `connectAs(t, ctx, hub, connReq, project)`, that pins
     `MCP_HUB_PROJECT_DIR` for one connect call — simulating what two
     *actually* distinct real agents would have (see point 1 above)
     rather than two coincidentally-colliding ones.

### Wait-socket path: random, not derived from (sessionId, peerId)

A follow-on bug from the same root cause as the two above, found by asking
"is there a lock?" while explaining why a local MCP-harness stdio restart
tears down the live WebSocket: `internal/waiter.socketPath` used to hash
`(sessionID, peerID)` into the wait socket's filename — deterministic *by
design*, back when every `hub_connect` got a fresh peerId and the only
goal was keeping two different peers' sockets from colliding. Persisted
identity and supersede changed that premise: peerId is now routinely
*reused* across a reconnect, which means two different processes — or the
tail end of a dying one racing a restart — can now compute the exact
*same* socket path. `waiter.Listen` used to unconditionally
`os.Remove(path)` before binding, on the assumption the file was always
"a stale socket from a crashed prior run" — no longer a safe assumption
once the same path can belong to a socket that's still genuinely live.
Nothing arbitrates that race: no lock, no check, just delete-then-bind.

Fixed by making `socketPath()` generate a random 8-byte suffix
(`crypto/rand`, with a timestamp fallback only for the practically
unreachable case of the kernel RNG being unavailable) instead of hashing
the connection's identity — collision probability negligible, and now
structurally impossible for it to collide with anything *derived from*
peerId, since it isn't derived from anything about the connection at all.
`Listen` dropped its now-unused `sessionID`/`peerID` parameters entirely
(along with `teamsRelaySocketSessionID`, the placeholder constant that
existed only to give `teams_relay_connect` a fake "sessionId" for the old
hash — no longer needed) and the preemptive `os.Remove` before binding,
since a random path has nothing stale to remove. `sweepStaleSockets`
(actual crash cleanup — glob + connect-and-check liveness) is unaffected;
it never depended on the naming scheme.

## HTTP-MCP endpoint (`mcp-hub-server`)

`mcp-hub-server` also serves a Streamable-HTTP MCP endpoint at `/mcp`,
alongside its existing websocket relay at `/{sessionId}` — additive, no
changes to the websocket path, `mcp-hub-client`'s stdio mode, or its
`wait`/`wait --follow` unix-socket mechanism (below). It exposes
`hub_connect`/`hub_disconnect`/`hub_send`/`hub_receive`/`hub_wait`/`hub_peers`
— a deliberate subset matching only what the plain relay understands
server-side (no reactions/edit/delete/history, which are
chat-relay/Teams-bridge-specific). `hub_connect`'s result includes a
`watchToken`; `GET /watch?token=<token>&follow=1` streams that peer's
events live via `http.Flusher`, for `curl -N`-based async monitoring
(the same purpose as `wait --follow`, for a remote HTTP client with no
local process). Full rationale, rejected alternatives, and design details:
`docs/superpowers/specs/2026-08-28-http-mcp-endpoint-design.md`.

## Chat-relay bridge support (`teams_relay_connect`, `hub_history`)

A second connect path, for a server that bridges into a real chat platform
(the motivating case: a "chat-relay" project mirroring a bot account's
Microsoft Teams conversations) rather than being an `mcp-hub-server`.
Designed collaboratively over a live hub session with the Claude instance
building that server — the two clients negotiated the wire contract
directly, each grounded in their own actual code rather than assumption.
`hub_connect`'s own constraints (`sessionId` must be a UUID; `host` must be
bare scheme+authority, no path/query/fragment) made it unusable for a
server that issues opaque, arbitrarily-shaped links, so this is a wholly
separate tool rather than a relaxation of `hub_connect`'s rules.

- **`teams_relay_connect(link, name?, reconnectSecret)`** — `link` is one
  opaque string, unparsed and unvalidated beyond a single rule that never
  changes: split on the first `#`. Everything before it is dialed as an
  ordinary WebSocket URL (arbitrary path/query allowed, unlike
  `hub_connect`'s `host`); everything after it is sent as
  `Authorization: Bearer <secret>` on the handshake instead of ever
  appearing in the request itself. This is deliberate: a URL fragment is
  defined to never reach a server, so a secret placed there cannot leak
  into that server's access logs, a proxy in front of it, or ours — by
  construction, not by remembering to redact it afterward. `reconnectSecret`
  is sent as a second header, `Reconnect-Secret: <secret>` — never as a
  query parameter, for the identical log-exposure reason (this is *not*
  the same field/purpose as `hub_connect`'s `reconnectSecret`: it doesn't
  identify a peer — a link already fixes which conversation a connection
  belongs to — it authorizes *resuming* after a drop, since a bridge link
  may be single-use and the same spent link only works again if paired
  with the same `reconnectSecret` presented at the original connect,
  within whatever window the bridge grants). `name` is optional and sent
  as a third header, `Agent-Name: <name>`, if given — a bridge server is
  not obligated to make it visible on the other side of the bridge (in the
  motivating case, it's audit-only: every client on a link posts under the
  bridge's own bot identity in Teams, so `name` never appears in the
  conversation itself).

  Implemented as `hubconn.DialRelay` (`internal/hubconn/relay.go`),
  distinct from `Dial` only in how it reaches an open `*websocket.Conn` —
  both funnel into a shared `finishHandshake` that reads the initial
  `joined` message, validates it, and starts the same background read
  loop, buffering, keepalive, and disconnect detection either path uses.
  A relay connection is an ordinary `Conn` after that point, including
  reusing `hub_send`/`hub_receive`/`hub_wait`/`hub_peers` unmodified — this
  is why the bridge's peer ids must stay UUID-shaped (the design
  discussion settled on the bridge deriving synthetic per-link
  HMAC-derived UUIDs for real participant identities, rather than this
  client relaxing `decodeEvent`'s UUID validation to accommodate them).

  Two protocol differences a bridge session must handle, agreed during
  design:
  - **A directed `hub_send` (`to`) must be refused with an `error` event,
    never silently delivered as a broadcast** — a bridge with no
    peer-to-peer delivery (everything goes into one shared conversation)
    would otherwise disclose a message its sender believed was private.
    Confirmed as already correct with no client changes needed: the read
    loop only ever marks a connection closed on an actual
    `ws.ReadMessage()` failure, never on a decoded `error` event, so a
    mid-connection refusal (e.g. a policy-refused send) is buffered and
    rendered like any other event without disturbing the connection.
  - **Close codes 4001 (revoked), 4002 (expired), and 4003 (conversation
    unavailable)** signal a dead credential rather than ordinary network
    trouble — a client that reconnect-loops against one of these would
    loop forever against something no retry can fix. The *primary* signal
    is a server sending a final `error` event (with `Code`/`Retryable`,
    see below) before it deliberately closes, since reconnection here is
    entirely model-driven — a raw WebSocket close code is invisible to
    whatever is actually deciding whether to retry, since nothing in this
    client branches on one automatically. The close codes are a
    *secondary*, best-effort signal for the case where that event was
    missed: `hubconn.Conn` tracks the close frame's code
    (`websocket.CloseError`) and `Conn.DisconnectNote()` maps the three
    agreed codes to a suffix folded directly into the disconnect text
    every call site already produces (`disconnectedText` in
    `internal/mcptools/tools.go`; `Waiter.disconnectedMessage` in
    `internal/waiter/waiter.go`, via a `disconnectNoter` duck-typed
    interface so `waiter.Source` stays generic) — e.g. `"hub disconnected
    (revoked — do not reconnect)"` instead of a bare `"hub disconnected"`.
    Empty (a no-op) for every ordinary drop, including every
    `mcp-hub-server` disconnect.

  `wire.Error` gained two additive fields for this: `Code` (a stable
  machine-readable reason, e.g. `"revoked"`, `"invalid_credential"`,
  `"conversation_unavailable"`, `"unavailable"`) and `Retryable` (whether
  retrying could succeed — `"unavailable"` is the one transient case in
  the agreed starting set). Deliberately, per the bridge side's own
  security reasoning: an *unverified* credential and a *revoked* one both
  answer `invalid_credential` — a bridge server that distinguished them
  would let anyone probing link ids learn which ones were ever real,
  turning the handshake into an enumeration oracle for a bearer
  capability. Only a secret that verifies earns a precise reason.
  `hubconn`'s `FormatEvent` renders a coded error as `[HUB ERROR —
  code=<code>, retryable=<bool>] <message>` instead of the plain `[HUB
  ERROR] <message>` a codeless one still gets (unchanged, so this is
  additive for a plain `mcp-hub-server` error too).

- **`hub_history(before?, limit?)`** — requests messages predating this
  connection, meaningless for an ordinary `hub_connect` session (a peer
  only ever sees events from when it joined forward — there was never a
  history concept to draw on here) but real for a bridge backed by a
  channel with actual retained history. Sends `wire.History`
  (`{"type":"history","before":...,"limit":...}`) via the new
  `Conn.RequestHistory` and returns immediately with a confirmation — the
  requested messages themselves arrive asynchronously through the normal
  `wait`/`hub_receive`/`hub_wait` path, not as this call's own result,
  since a bridge may take a moment to fetch what could be a large page.
  `before` is an opaque, server-defined cursor and *exclusive* (the page
  ends strictly before it — this avoids an off-by-one where paging
  backward would repeat the same boundary message on every page); omitted
  it means "the most recent `limit` messages." A client pages further back
  by resending the request with the oldest cursor it has seen. Answering
  messages are ordinary `msg` events carrying an additive `Historical:
  true` flag (`wire.Msg.Historical`) — mirroring how a plain `hub_connect`
  session already flags a *private* `msg` without inventing a whole new
  message type, and letting an older client that doesn't know the field
  simply render them as ordinary messages instead of silently dropping an
  unrecognized type. `FormatEvent` renders a historical message as `[HUB
  HISTORY — untrusted, from peer <peerId> at <ts>]` — distinctly from a
  live `[HUB MESSAGE ...]`, so it's never mistaken for something that just
  happened. The burst is terminated by a new `historyComplete` sentinel
  event (`wire.HistoryComplete`/`TypeHistoryComplete`), mirroring
  `rosterComplete`'s role for the initial roster: exhaustion is always an
  explicit "there is no more," including an empty burst plus the sentinel,
  never silently inferred from a gap in traffic.

- **`hub_history`'s `after` parameter — forward paging, for reconnect
  catch-up.** `before` only ever reaches *older* messages; it cannot be
  used to fetch what arrived while a client was disconnected, and the
  original reconnect-hint wording ("call hub_history(before: your last
  known cursor)") was simply wrong about this — flagged from the hub
  session itself, by an agent who'd hit it live. `wire.History` gained
  `After string` (exclusive, forward — the page starts strictly after the
  given cursor) alongside `Before`; a client should never send both.
  `wire.Joined` gained `HistoryAfter bool`, a capability flag a server
  sets to advertise support (false, including every `mcp-hub-server`,
  means only `Before` is honored). `Conn.RequestHistoryAfter` /
  `Conn.HistoryAfterSupported()` mirror the existing `Before`-side
  methods. `disconnectedText` (`internal/mcptools`) now names the exact
  last-seen cursor and, when the server supports it, the exact
  `hub_history(after: ...)` call to make — or, when it doesn't, says so
  plainly instead of suggesting `before` (which would silently do the
  wrong thing). The connect-time hint mirrors this same branch. This is a
  wire-protocol addition that only does anything once a bridge server
  (e.g. chat-relay) implements `After`/`HistoryAfter` on its own side —
  `mcp-hub-server`'s own relay has no history concept at all.

- **Read receipts (`ackCursor`/`wire.Ack`) — the model's actual read
  position, not just what the client received.** Designed and built
  server-side first by chat-relay, coordinated live over the hub session;
  motivated by a real gap ("has the agent actually seen my message?" —
  `lastUsedAt` only proves a socket was active). `wire.Msg`/`Reaction`/
  `Edit`/`Delete`/`History` all gained an `AckCursor string` field,
  piggybacked on every outbound message once `Conn` has consumed
  something (via `Drain` — deliberately *not* readLoop's buffering, since
  a buffered-but-undrained event doesn't mean the model saw it). A new
  `wire.Ack` (`{"type":"ack","ackCursor":...}`) is the standalone form,
  fired by a new `Conn.ackLoop` background goroutine after `ackIdleInterval`
  (60s) of otherwise-idle connection, but only if the consumed position
  moved since the last receipt actually sent — an idle ack repeating an
  already-known position would turn a read receipt into a heartbeat.
  `Conn.lastConsumed`/`lastAckSent`/`ackDisabled` track this; none of it
  is exposed as a tool — it's fully automatic.

  The server's reply protocol has two distinct shapes for two distinct
  failures: `wire.Ack{OK: false}` for a stale-but-valid receipt (the
  server reports the position it actually holds; the client adopts it
  rather than retrying, since monotonicity means nothing was lost), and a
  plain `wire.Error{Code: "bad_ack"/"bad_ack_cursor"}` for a malformed
  receipt (a client bug, not transient — resending would fail
  identically, so the client permanently disables further receipts on
  that connection rather than repeat the mistake). These two codes are
  deliberately ack-subsystem-specific, not the generic `bad_cursor`/
  `bad_request` a bridge server may also use for unrelated requests (a
  malformed reaction, a history request naming both `before` and
  `after`) — error events carry no correlation id, so reacting to the
  generic codes would have let one unrelated malformed request silently
  and permanently disable read receipts for the rest of the session
  (caught and fixed during the same live coordination, before shipping).
  Neither shape is ever surfaced to the model: `Conn.handleAckPlumbingLocked`
  intercepts both before they'd otherwise reach the buffer or (for the
  error path) risk
  being stolen by an unrelated pending `claimNextAck`. mcp-hub-server
  ignores `ackCursor` entirely (unrecognized field, silently dropped by
  `encoding/json`) — this only does anything once a bridge server (e.g.
  chat-relay) implements it server-side.

- **`sendAck` — confirms a send reached its destination, immediately.**
  Found necessary by a real incident during the joint design/testing
  session with chat-relay, not designed up front: chat-relay's server
  originally gave no synchronous confirmation for `hub_send` at all,
  reasoning that the sent message would come back through the normal
  delivery pipe like any other — true in production, but its dev server
  ran in a slow polling mode with a multi-hour reconciliation sweep, so
  the sender got no ack, no error, and no echo for hours. Two identical
  test sends were made in that gap because there was no way to tell
  "still in flight" from "silently failed," and no way to know without
  retrying whether a retry would create a duplicate — which it did,
  landing two real messages in a real conversation. `sendAck`
  (`wire.SendAck`/`TypeSendAck`, `{"type":"sendAck","externalId":...,
  "ok":true}`) fixes this at the protocol level: sent right after the
  bridge server's own send actually succeeds (or fails), correlating via
  `externalId` (the bridge's own id for the sent message) with the
  canonical `msg` that still arrives later, exactly once, through the
  normal path — deliberately not a `msg` itself, so nothing is
  double-counted and no cursor needs to be fabricated. Decoded into
  `Event.ExternalID`/`Event.ActionOK`, rendered distinctly by `FormatEvent`
  (`"send acknowledged"` vs `"send NOT acknowledged"`, both naming the
  `externalId`) so a model reading it can't confuse "this specific send
  succeeded" with "here is the message in the conversation." Additive and
  optional per this project's own versioning convention — an older client
  that doesn't know the type just doesn't get the early signal and falls
  back to inferring from the later canonical message, same as before this
  existed. `mcp-hub-server` never sends this, since a plain `hub_send`
  already completes synchronously and has no such gap to cover.

- **`msg.externalId`/`msg.own`, and own-message wake suppression.** Two
  more additive `wire.Msg` fields, from the same joint session, discovered
  in the opposite order from `sendAck`: chat-relay's user first asked for
  the *server* to hold a sender's own message back and only flush it once
  a reply arrived, framed as "the wake should carry information, not just
  an echo of what the sender already knows it sent." That was then
  overruled by the same user for a better reason: *which* events should
  wake a caller is policy, and policy differs per consumer of the same
  stream (an agent, a UI, a human-driven client) — a server holding
  per-connection buffer state to enforce one fixed policy for everyone
  couldn't express that, and the buffer dying on disconnect meant the
  policy wasn't even reliable. The fix moved bookkeeping to where the
  decision actually happens: the server tags, the client decides.
  - `ExternalID` on `msg` is the same id `sendAck` returns for the send
    that produced it — before this, a client could tell "my send
    succeeded" and, separately, "a message just arrived," but had no real
    way to know they were the *same* message short of matching text and
    timing, which isn't a real answer once more than one send is in
    flight.
  - `Own` is `true` when *this exact connection* — not "the bridge's bot
    identity" — sent the message. A message sent by a different
    connection to the same bridge account, or through the bridge's own
    agent API, arrives without the flag: it's genuine new information to
    this connection even though it also originates from the bot.
  - **The actual suppression lives entirely client-side**, in
    `hubconn.Conn.Peek()` (`internal/hubconn/conn.go`): "wake-worthy" now
    excludes a `msg` event with `Own` true — the sender already knows it
    sent that message, so waking on nothing but its own echo is a wake
    with no new information, and risks an agent seeing its own message
    and answering itself. Such an event stays buffered, not dropped,
    until either a genuinely new (non-own) event arrives — at which point
    `Drain()` returns everything buffered together, in order, so the
    caller sees `[my own message, their reply]` as one delivery — or
    `maxPendingOwnMessages` (10) own messages have piled up with nothing
    else, the overflow valve for "usually shouldn't happen" scenarios
    (e.g. a burst of sends with no reply between them), so a connection
    can never go silently unbounded on suppressed events. Every other
    event kind, including `sendAck` itself, is always wake-worthy — the
    ack exists specifically to be an immediate signal and this doesn't
    touch it.
  - This is the one place a change had to reach two independent
    consumers of the same `Peek()`/`Drain()` contract, and only one of
    them was obvious: `hub_wait`'s polling loop (`internal/mcptools/
    tools.go`) already gates on `Peek()`, so it inherited the new
    behavior for free. The CLI `wait` socket did not — `waiter.Waiter.
    Poke()` (`internal/waiter/waiter.go`) used to drain and deliver
    unconditionally whenever called, and `OnActivity` (wired in
    `handleConnect`/`handleTeamsRelayConnect`) calls `Poke()` on *every*
    buffered event, suppressed or not. Left unfixed, an own-message echo
    would have been correctly held back from `hub_wait` while
    simultaneously being delivered instantly to anyone running the CLI
    `wait` binary against the same connection — the same underlying
    buffer, two different wake behaviors depending on which door a caller
    used. `Poke()` now checks `Peek()` itself, under the same lock that
    reads/clears `w.current`, before consuming the registered waiter —
    if the source isn't wake-worthy yet, it leaves the registration in
    place for a later `Poke()` call to try again, exactly the discipline
    `deliver()`'s own re-registration tail already used for the
    registration-race fix (see above), for the identical reason: a callback
    invoked far more often than it should actually act must recheck fresh
    state under one lock rather than assume its last observation still
    holds.
  - Regression tests: `TestPeekSuppressesOwnMessagesUntilANonOwnEventArrives`,
    `TestPeekWakesOnOwnMessageOverflowEvenWithNoReply`,
    `TestPeekTreatsNonMsgEventsAsAlwaysWakeWorthy`,
    `TestPeekReportsDisconnectedEvenWithOnlySuppressedOwnMessages`
    (`internal/hubconn`); `TestPokeDoesNotDeliverWhileSourceReportsNotWakeWorthy`
    (`internal/waiter`); `TestHubWaitDoesNotWakeOnOwnMessageAloneButDeliversItAlongside`
    (`internal/mcptools`, the end-to-end proof through the actual tool).

- **Reactions and edits — full read/write, both sides live.** Requested
  (by the human running this session) as a pair of writable actions —
  add/remove a reaction on an earlier message, edit a previous message's
  content. The write half was initially believed blocked: chat-relay
  first cited `Chat.Read`/`ChatMessage.Send`/`offline_access` as its only
  Graph scopes and said reactions/edits needed a broader
  `ChatMessage.ReadWrite` grant, a real permission-scope decision for
  their user, not something to absorb in-session. That citation turned
  out to be wrong on inspection of Graph's own docs: `setReaction`/
  `unsetReaction` need `Chat.ReadWrite, ChatMessage.Send` (the second of
  which chat-relay already held, so reactions needed nothing new at all),
  and editing a message needs `Chat.ReadWrite` — which chat-relay's user
  had *already* granted, under a different name than what was being
  searched for. The earlier belief that permissions blocked this was
  simply incorrect; nothing here waited on a policy change, only on
  someone checking the actual Graph documentation instead of citing from
  memory. The read half (learning about a reaction or edit *someone else*
  makes) was additionally held back from "cheap to build, so build it"
  until chat-relay's own user separately confirmed wanting it — the same
  discipline applied to the direct-join link earlier in this design: a
  request arriving secondhand through one Claude session isn't itself
  authorization for the other side to build against. Both halves are now
  built, deployed on chat-relay's dev and staging environments, and
  verified end to end against staging in a live joint test (see below).
  - `wire.ReactionChanged` (`{"type":"reactionChanged","externalId":...,
    "peerId":...,"reaction":"👍","label":"Like","action":"add"|"remove",
    "ts":...,"own":...}`) — decode-only; mcp-hub-server never sends it,
    and this client never constructs one. `Reaction`/`Label` are
    deliberately open strings, not a closed enum — chat-relay's own live
    data already contains "Eyes" and "Question mark" alongside the
    classic "Like," so Teams' reaction set has expanded past assumption
    and a client must not reject or normalize a value it doesn't
    recognize; whatever the source platform reports is authoritative.
    `PeerID` may be absent: a removal is detected by diffing the
    reaction set on a message, and the remover isn't always identifiable
    from that diff — the event still fires with `PeerID` omitted rather
    than being suppressed for incomplete attribution, on the same
    reasoning as every other "the system knew something and should say
    so" fix from this design session: "someone removed a 👍, unattributed"
    is real information; silently dropping it because the *who* is
    missing would repeat exactly the failure mode (four of five real bugs
    found this session presented as silence) that motivated `sendAck`,
    the fan-out log line, and surfacing `joined`'s bridge fields in the
    first place.
  - `wire.MessageEdited` (`{"type":"messageEdited","externalId":...,
    "text":...,"ts":...,"own":...}`) — also decode-only. `Text` is the
    same simplified/rendered form a `msg` carries, directly comparable.
    The edited message's own cursor does *not* change (chat-relay's
    cursor is `(createdAt, id)`, and an edit moves neither), so a client
    locates the message it already has by `ExternalID`, not a new cursor
    — worth stating explicitly since the natural assumption is the
    opposite. `Own` can currently never be true (a sender may only edit
    its own messages, and editing needs the write permission that
    doesn't exist yet), kept on the type for symmetry with `msg` rather
    than omitted, so it's already correctly wired the moment the write
    path lands rather than requiring another field-adding round then.
  - Rendered by `FormatEvent` as `[hub: <peerId|"someone (unattributed)">
    added/removed a <reaction> (<label>) reaction on message
    externalId=<id>]` and `[HUB MESSAGE EDITED — untrusted,
    externalId=<id> at <ts>]\n<text>` respectively — the edit is wrapped
    as untrusted content, same as a `msg`, since its text is exactly as
    attacker-controlled (editable by whoever sent the original).
  - **Own-action wake suppression extends to both**, not just `msg`:
    `hubconn.Conn`'s `isSuppressibleOwnEvent` (used by `Peek`, see
    "own-message wake suppression" above) now covers `msg`,
    `reactionChanged`, and `messageEdited` uniformly — a reaction you
    added yourself is exactly as much a no-op echo as a message you sent
    yourself, and the same `maxPendingOwnMessages` overflow valve and
    together-with-the-next-real-event delivery apply. `messageEdited`'s
    `Own` can't be true yet in practice, but the logic already handles it
    correctly for when it can.
  - Regression tests: `TestReactionChangedRoundTrip`,
    `TestReactionChangedOmitsPeerIDWhenUnattributed`,
    `TestMessageEditedRoundTrip` (`internal/wire`);
    `TestFormatEventReactionChangedAdd`,
    `TestFormatEventReactionChangedRemoveUnattributed`,
    `TestFormatEventReactionChangedOwnIsMarked`,
    `TestFormatEventMessageEditedIsWrappedAsUntrusted`,
    `TestPeekSuppressesOwnReactionChangedAndMessageEdited`,
    `TestPeekTreatsNonOwnReactionChangedAsWakeWorthy` (`internal/hubconn`).

- **The write side: `hub_react`/`hub_edit`, `wire.Reaction`/`wire.Edit`,
  `wire.ReactionAck`/`wire.EditAck`.** `Conn.React(externalID, reaction,
  action)` and `Conn.EditMessage(externalID, text)` send
  `{"type":"reaction",...}`/`{"type":"edit",...}` requests; not meaningful
  for `mcp-hub-server`, which silently ignores any message type it
  doesn't recognize (confirmed from its own read loop — an unrecognized
  `Type` just `continue`s the loop), so sending either against a plain
  `hub_connect` session is a harmless no-op rather than an error. Two new
  MCP tools, `hub_react(externalId, reaction, action)` and
  `hub_edit(externalId, text)`, expose these the same way `hub_history`
  does: the call itself only confirms the request was sent, with the
  actual outcome arriving asynchronously via `wait`/`hub_receive`/
  `hub_wait` as a `reactionAck`/`editAck` (success) or an ordinary `error`
  event (refusal — chat-relay routes reactions/edits through the same
  tenant-lock gate as a send, so a federated conversation refuses all
  three for the same reason). `reaction` accepts an open string exactly
  as `wire.ReactionChanged.Reaction` does — chat-relay also accepts the
  field under the name `reactionType` (mirroring Graph's own request-body
  naming) if a different client would rather send that instead; this
  client always sends `reaction`.

  `Event.ActionOK` (renamed from the narrower `SendOK` once it needed to
  cover three ack kinds, not one) and `Event.ExternalID` are now shared
  across `sendAck`/`reactionAck`/`editAck` — a deliberate small refactor
  rather than three near-duplicate boolean fields, since all three answer
  the identical question ("did the action this connection asked for
  succeed") for the identical reason `sendAck` was built: without an
  explicit ack, a client can't tell "still in flight" from "silently
  failed," which is exactly the ambiguity that caused two duplicate real
  messages earlier in this design's own history. A refusal is expected to
  arrive as an `error` event, not one of these acks with `OK: false` — the
  field exists on the wire type for symmetry and in case a server ever
  has a concrete reason to send a negative ack explicitly, but no server
  currently does.

  **Verified end to end in a live joint test against chat-relay's
  staging** (deliberately staging, not dev: dev's `INGEST_MODE=poll`
  four-hour sweep means the read-side events — `messageEdited`,
  `reactionChanged` — would never arrive within any sensible wait, the
  same ambiguity that caused the `sendAck` incident; staging's live
  Graph subscription delivers them within a second or two): send →
  `sendAck` → canonical `msg`; edit → `editAck` → `messageEdited` with
  `own: true` genuinely set (previously always false, since editing
  needed the write path this test is exercising); react add → `reactionAck`
  → `reactionChanged` with `peerId` correctly present; react remove →
  `reactionAck` → `reactionChanged`, `peerId` still present (chat-relay's
  stored row retains who reacted even though Graph's own diff-based
  detection can't always say). The test also confirmed, live and by
  design rather than by accident, that own-message wake suppression (see
  above) correctly withheld the canonical own `msg` from `Peek()` until
  drained directly — the first time that logic ran against a real send
  rather than a synthetic one.

  **One genuine finding from that test, on chat-relay's side, not this
  client's:** `reactionChanged` for this connection's own reaction —
  both the add and the remove — arrived with `peerId` correctly set to
  this connection's own peer id, but **without `own: true`**, despite
  `wire.ReactionChanged.Own` existing specifically to mark that case (see
  above; the design already anticipated this might not work, since
  reaction attribution goes through the reactor's identity rather than
  through which connection acted, unlike the send path). Reported back
  to chat-relay for a fix; this client's own handling (rendering `own`
  when present, suppressing the wake on it) needed no change, since it
  was chat-relay's server that omitted the field, not this client
  mis-decoding it. **Fix confirmed working** in a follow-up joint test
  minutes later (reacting to the already-edited message from the prior
  round, so no new message was needed) — both add and remove now arrive
  correctly marked `own: true`. Root cause on chat-relay's side, for the
  record: the field was declared on their event record and never
  populated at all — always absent regardless of who reacted, which is
  why their own tests (which check `DidSend`, "did you post this
  message," not "did you put this reaction on it") couldn't have caught
  it structurally, not just by chance.

- **Making `hub_send`/`hub_react`/`hub_edit` report their own real outcome
  synchronously, instead of a bare "sent" that doesn't mean anything on a
  bridge session.** Raised as a direct question after the write side
  landed: `handleSend` returned `"sent"` the moment the local websocket
  write succeeded, saying nothing about whether chat-relay's server (or
  Graph) actually accepted the message — the real answer arrived later,
  asynchronously, as a `sendAck` or an `error` event, and nothing told
  the model it needed to separately check for one. The fix isn't simply
  "block on `Peek`/`Drain` until an ack shows up," though: `hub_wait`, the
  CLI wait socket, and now three tool handlers would all be polling the
  same shared, destructive, non-selective buffer — exactly the shape of
  bug already found and fixed once this session (the `waiter` registration
  race, where two independent pollers on one buffer could steal an event
  meant for the other). A second independent poller reading `Peek`/`Drain`
  would reintroduce it.

  Instead, `hubconn.Conn` gained a proper claim mechanism that intercepts
  matching events *before* they ever reach the general buffer, so nothing
  else is competing for them:
  - `Conn.claimNextAck(ackKind string) (result <-chan Event, cancel func())`
    registers interest in the next event of exactly `ackKind`
    (`"sendAck"`/`"reactionAck"`/`"editAck"`), or the next generic
    `"error"` event, whichever arrives first. `readLoop` checks every
    incoming event against `pendingAcks` (a `map[string]*ackClaim`,
    keyed by ack kind) under the same lock it uses to append to the
    buffer — `tryDivertToClaimLocked` — and if it matches, delivers the
    event straight to the claim's channel and `continue`s the read loop
    without ever touching `c.buffer` or firing `OnActivity` for it. A
    non-matching event (a live `msg`, `peerJoined`, an ack of a
    *different* kind, anything else) is completely unaffected and flows
    through exactly as before, claim active or not.
  - The generic-`"error"`-diversion part is a deliberate, acknowledged
    protocol limitation, not something this method can fully close:
    chat-relay's refusals (the tenant lock, an edit Graph won't permit)
    carry no per-request id linking them back to the specific action that
    caused them, so an error arriving while a claim is pending is
    *assumed* to be that claim's outcome. With more than one claim active
    at once (e.g. a concurrent `hub_send` and `hub_react`), an error
    could in principle be attributed to the wrong one — accepted as a
    known edge case rather than solved, since the protocol itself doesn't
    distinguish them either.
  - `Conn.SendAwaitingAck(text, to)` / `Conn.ReactAwaitingAck(externalID,
    reaction, action)` / `Conn.EditMessageAwaitingAck(externalID, text)`
    wrap claim-then-write-then-wait, but only *do* the wait for a bridge
    connection (`Conn.IsBridge()`, true only when constructed via
    `DialRelay`) — a plain `mcp-hub-server` connection never emits any
    ack at all, so waiting on one would just be a fixed latency tax on
    every single send for zero benefit. `IsBridge` is set once during
    `finishHandshake` (a new parameter, `false` from `Dial`, `true` from
    `DialRelay`) and read only after construction, so — like
    `peerID`/`name`/etc. — it needs no locking of its own. Each method
    returns `(Event{}, false, nil)` on a plain connection (report success
    exactly as always) or on a real timeout (`AckWaitTimeout`, a
    package var default 5s — generous relative to how fast an ack has
    actually been observed to arrive against a real bridge server, well
    under a second) — the write may still succeed or fail later, reported
    the normal way via `wait`/`hub_receive`/`hub_wait`, unchanged from
    before this existed. It returns `(event, true, nil)` the moment a
    real outcome arrives, which `handleSend`/`handleReact`/`handleEdit`
    now render directly with `FormatEvent` as their own tool result —
    success *or* failure, synchronously, which is what closes the actual
    gap: a refused send now reads as the real refusal reason immediately,
    not as a misleading `"sent"`.
  - Regression tests: `TestSendAwaitingAckOnPlainConnDoesNotWait`,
    `TestSendAwaitingAckReturnsSendAckOnBridge`,
    `TestSendAwaitingAckReturnsErrorEventOnRefusal`,
    `TestSendAwaitingAckTimesOutWithoutStealingLaterEvents`,
    `TestClaimNextAckDoesNotStealUnrelatedEvents` (`internal/hubconn`);
    `TestHubReactReportsAckDirectly`, `TestHubEditReportsAckDirectly`,
    `TestHubSendReportsAckDirectlyOnBridgeSession`,
    `TestHubSendDoesNotWaitOnAPlainHubConnectSession` (`internal/mcptools`,
    the last one specifically proving the `IsBridge` gate keeps a normal
    session's `hub_send` exactly as fast as it always was).

- **A real bug caught while adding `hub_delete`, on this client's side:
  `wire.Msg` never actually had a `Cursor` field.** chat-relay had been
  including a `cursor` on every message all along (mentioned as far back
  as the original history design: "each with `historical: true` and a
  cursor"), and this client silently dropped it every time —
  `encoding/json` ignores an unrecognized field by default, so decoding
  never errored, it just quietly lost the one piece of data
  `hub_history`'s own paging story depends on. Concretely: `hub_history`'s
  own description promises "page further back by passing the oldest
  cursor seen so far," but there was no way to *see* a cursor at all —
  every historical (and live) message rendered with no cursor in it.
  Found only because `wire.MessageDeleted`'s spec explicitly called out a
  `cursor` field and prompted a direct check of whether `Msg` already had
  one. Fixed: `wire.Msg.Cursor` (additive, `omitempty`), threaded through
  `decodeEvent` into `Event.Cursor`, and rendered by `FormatEvent` on
  every `msg` that carries one (`... cursor=<value>]`) — including live
  messages, not just historical ones, since a client resuming after a
  drop may want to remember its own last-seen cursor from a live message
  just as much as from a `hub_history` page. Regression tests:
  `TestMsgCursorRoundTrip`, `TestMsgOmitsCursorWhenUnset` (`internal/wire`);
  `TestFormatEventMsgIncludesCursorWhenPresent`,
  `TestFormatEventMsgOmitsCursorWhenAbsent` (`internal/hubconn`).

- **`hub_delete`/`wire.Delete`/`wire.DeleteAck`/`wire.MessageDeleted`** —
  added alongside the `Cursor` fix, following the exact same shape as
  `hub_edit`/`hub_react`: `Conn.DeleteMessage(externalID)` sends
  `{"type":"delete","externalId":...}`; `Conn.DeleteMessageAwaitingAck`
  claims its own `"deleteAck"`/`"error"` outcome the same way
  `SendAwaitingAck`/`ReactAwaitingAck`/`EditMessageAwaitingAck` do, and
  `handleDelete` reports it directly, falling back to an async
  confirmation on timeout — no new plumbing needed, the claim mechanism
  and the `IsBridge` gate were already general. Deliberately a distinct
  request from `Edit` with empty text, not a special case of it: the
  underlying platform payload for a deletion and an edit-to-nothing look
  identical, but a client that conflated them would render an empty
  message where the platform renders a tombstone. `wire.MessageDeleted`
  (the read-side "someone deleted a message" notification) carries
  `ExternalID`, `Cursor` (the deleted message's own position, so a client
  can place the tombstone in a rendered transcript without having seen
  the original message first), `TS`, and `Own` — `own` wake-suppression
  (see above) was extended to cover `"messageDeleted"` alongside `"msg"`/
  `"reactionChanged"`/`"messageEdited"`, on the same reasoning: your own
  deletion isn't news to yourself either. Regression tests:
  `TestDeleteRequestRoundTrip`, `TestDeleteAckRoundTrip`,
  `TestMessageDeletedRoundTrip` (`internal/wire`);
  `TestFormatEventMessageDeleted`, `TestFormatEventMessageDeletedOwnIsMarked`,
  `TestFormatEventDeleteAckOK`, `TestFormatEventDeleteAckNotOK`,
  `TestDeleteMessageAwaitingAckReturnsDeleteAckOnBridge`,
  `TestDeleteMessageOnPlainConnDoesNotWait` (`internal/hubconn`);
  `TestHubDeleteReportsAckDirectly`,
  `TestHubDeleteSendsDeleteRequestAndFallsBackOnTimeout`,
  `TestHubDeleteErrorsWhenNotConnected` (`internal/mcptools`).

- **Bridge-only fields on `joined`**: `LatestCursor` (`*string`, nil if the
  conversation has no messages yet), `HistoryLimitMax` (`int`), `CanSend`
  (`bool`), `ConversationKind` (`string`, e.g. `"oneOnOne"`/`"group"`/
  `"meeting"`), `Topic` (`*string`). All zero-valued for every
  `mcp-hub-server` connection, since the server never sets them, and never
  referenced by `handleConnect`'s own result text — only
  `teams_relay_connect` reads them, via matching `Conn` accessors
  (`LatestCursor()`, `HistoryLimitMax()`, `CanSend()`,
  `ConversationKind()`, `Topic()`). This was a deliberate second pass, not
  part of the original design: the first cut only confirmed unknown
  `joined` fields decode without error (true, and necessary for
  forward-compatibility — a bridge server can ship these before a client
  supports them) but stopped there, which missed the actual point.
  "Decodes without crashing" only matters if the data then reaches the
  thing meant to act on it — here, the model reading `teams_relay_connect`'s
  result text, not just a Go struct nobody reads. So `handleTeamsRelayConnect`
  surfaces all five directly: the conversation kind/topic and message
  count context up front, `CanSend` explicitly flagged as a connect-time
  snapshot that can go stale (a chat can gain an outside participant
  mid-session, which is exactly when a cached "yes" would be wrong — so a
  later `hub_send` refusal even after `CanSend: true` is not a
  contradiction), and `LatestCursor` framed as what a model should compare
  against its own memory of an earlier cursor to decide whether to call
  `hub_history` — deliberately not something this client tracks or
  compares automatically, since "what a model remembers from a prior
  session" isn't state this code has any access to.

## Image attachments

Wire shape agreed live over the hub with chat-relay's author, matching its
own extension: `wire.Msg` gained `attachments []Attachment`, each
`{contentType, contentBytes}` — `contentBytes` is base64, `contentType`
one of `image/png`, `image/jpeg`, `image/gif`, `image/webp` (chosen to
mirror Microsoft Graph's own attachment shape, since chat-relay's server
side sits in front of Graph). Rules, all inherited from that design
conversation rather than decided independently here:

- The 8MB cap (`wire.MaxAttachmentRawBytes`) is checked against **raw**
  bytes, before base64 inflation (~33%) and JSON envelope overhead — a
  client shouldn't need to reason about the wire-size number, only the
  file it's attaching.
- Images only, msg-only (no attachment support on `hub_edit` yet) — an
  absent `attachments` field on an edit must be read as "unchanged", never
  "remove them"; there's deliberately no way to express removal.
- Unknown/absent fields stay silently ignored by both ends, same as every
  other additive field this session (`ackCursor` etc.) — a server or
  client that doesn't understand attachments just never populates or reads
  them, nothing breaks either way.

Two send paths, deliberately different shapes for different trust
contexts:

- **`mcp-hub-client`'s `hub_send`** takes `imagePath` — a *local*
  filesystem path, read and base64-encoded by the client process itself
  (`wire.ReadAttachmentFile`), so the model never has to inline base64
  into a tool call just to send a picture (token cost). This only makes
  sense because mcp-hub-client runs as a local stdio process next to
  whatever files the model can already reference.
- **The built-in HTTP-MCP endpoint's `hub_send`** takes `imageData` +
  `imageContentType` instead (`wire.NewAttachmentFromData`) — a remote
  caller has no local filesystem relationship to mcp-hub-server's host, so
  an `imagePath`-style parameter there would mean reading arbitrary files
  off the *server's* disk based on an untrusted remote parameter, an
  arbitrary-file-read vector. Deliberately not built that way.

Receiving also splits along the same client/remote-endpoint line, again for
token-cost and trust-boundary reasons rather than protocol constraints —
`hubconn.Event` carries `Attachments []wire.Attachment` through either way
(added in `decodeEvent`'s `wire.TypeMsg` case), needing a new
`Conn.DrainEvents()` (raw events) alongside the existing `Conn.Drain()`
(pre-formatted text only) — `Drain()` now just calls `DrainEvents()` and
formats the result, so callers that only ever wanted text keep working
unchanged.

- **The built-in HTTP-MCP endpoint's `hub_receive`/`hub_wait`** build a
  `*mcp.CallToolResult` with the formatted text as one `TextContent` block
  plus one `mcp.ImageContent` block per attachment — the remote caller has
  no local file it could be pointed at instead, so inlining base64 in the
  tool result is the only option; `ContentBytes` is already base64 in
  exactly the form `ImageContent.Data` expects, so no re-encoding happens.
- **`mcp-hub-client`'s `hub_receive`/`hub_wait`** do the opposite,
  deliberately reversed from the first design pass: an attachment is
  base64-decoded and written to a local temp file
  (`Hub.saveReceivedImages`, under `os.TempDir()/mcp-hub-images-*`), and
  the result text gets an annotation naming the path instead of an inline
  image block — `[image attached ... saved to <path> ... — read the file
  to view it]`. mcp-hub-client runs right next to the model, so handing
  back a path it can read with its own file tool (which Claude Code's
  `Read`, for one, already renders as an image) costs far fewer tokens per
  event than re-embedding base64 in every `hub_wait` result across a
  long-running loop. The directory is per-connection, created lazily on
  first use, and removed entirely (`Hub.clearAttachDir`) wherever the
  connection tears down — explicit `hub_disconnect`, automatic
  dead-connection detection (`teardownIfCurrent`), and process
  `Shutdown()` all funnel through the same two chokepoints
  (`clearActiveConn`/`teardownIfCurrent`), so this needed no new teardown
  path, just a hook into the existing ones.

`mcp-hub-server`'s own relay (`wsserver`) now threads `Attachments` through
`wire.NewBroadcastMsg`/`NewDirectedMsg` rather than dropping them on
re-encode — plain hub-to-hub sessions can exchange images too, not just a
chat-relay bridge. This also meant giving the websocket connection an
actual `SetReadLimit` (16MB) for the first time — previously undocumented
as a real gap (see Explicit non-goals below), but an attachment-sized
message made an unbounded read frame a much more practical
resource-exhaustion vector than plain chat text ever was, so it was fixed
alongside this rather than left for later.

### Reference-form attachments (fetch by token)

Live over the hub, chat-relay's author reported a real divergence from
the first-pass design above: their server does **not** inline
`contentBytes` on a delivered `msg`/`messageEdited`. Instead it delivers a
reference — `{"token":"att-3142","contentType":"image/webp","name":"shot.png","kind":"image"}`,
no `contentBytes` — and the bytes are fetched on demand over the same
socket:

```
→ {"type":"attachment","token":"att-3142"}
← {"type":"attachmentData","token":"att-3142","name":"shot.png","contentType":"image/webp","contentBytes":"<base64>"}
```

with an ordinary `error` event (`code` one of `bad_attachment`,
`not_found`, `unavailable`) on refusal. Their reasoning: no peer pays for
an image none of them may want, one send doesn't fan out N copies, and a
live message stays identical to the same message replayed from history —
but the harder constraint is that a Teams-relayed image never exists as
inline bytes in a `msg` at all; it's fetched from Graph, recoded, and
stored, so token-fetch is the only mechanism that works for both the hub
case and the Teams-bridge case. `contentType` on the reference (and on
the fetched reply) is the *recoded* copy's type, not whatever the
original sender attached — every image is decoded and re-encoded before
being served, to a peer exactly as to a browser; a recode failure means
`unavailable`, never served raw.

This only affects the wire's *receive* shape — send is unchanged
(`contentBytes` inline, same as before). Added to support it:

- `wire.Attachment` gained `Token`/`Name`/`Kind` fields alongside the
  existing `ContentType`/`ContentBytes`, plus `IsReference()` (true when
  `Token` is set and `ContentBytes` isn't) — one struct, two shapes,
  distinguished by which fields a given server actually populates. A
  client attaching its own content only ever sends the inline shape,
  regardless of which shape it later receives.
- New wire types `wire.AttachmentRequest`/`wire.AttachmentData` (`"attachment"`/`"attachmentData"`).
- `hubconn.Conn.RequestAttachment(token string) (Event, bool, error)` —
  same `claimNextAck`-based synchronous-wait shape as
  `SendAwaitingAck`/`EditMessageAwaitingAck`, keyed on the `"attachmentData"`
  event kind, generic `"error"` diverted the same way. Against
  mcp-hub-server itself (which never emits a `Token` and silently drops
  an unrecognized `"attachment"` request type) this always times out —
  expected, since a caller should only ever call it for a `Token` actually
  seen on an `Attachment.IsReference()==true` entry, which mcp-hub-server
  never produces.
- `mcptools.Hub.resolveAttachment` is the single choke point that decides
  whether to decode inline bytes directly or fetch-by-token first, called
  from `saveReceivedImages` (now `saveReceivedImages(conn, events)` —
  needs the `Conn` to issue the fetch) before the existing save-to-local-
  file logic runs unchanged either way. httpmcp's inline `ImageContent`
  receive path was **not** updated to resolve references — the built-in
  HTTP-MCP endpoint has never been tested against a reference-style
  server, and doing this without a real one to test against would be
  speculative; flagged as a known gap, not an oversight.

`mcp-hub-server`'s stricter `SetReadLimit` (16MB) versus chat-relay's own
12MB wire cap were both already large enough to comfortably fit the
agreed 8MB raw `wire.MaxAttachmentRawBytes` figure either way — worth
stating the raw number in a shared spec rather than either implementation's
derived limit, per chat-relay's author's own note.

### Edit-with-attachments (client-side; awaiting chat-relay coordination)

`wire.Edit` gained an `Attachments []Attachment` field (always the inline
form, even against a reference-style server — that server is responsible
for recoding/storing and delivering it back by reference on
`MessageEdited`, exactly as it does for a live `Msg`), and
`wire.MessageEdited` gained the matching `Attachments` field for the
receive side, mirroring `Msg` in both directions. `hub_edit` gained an
`imagePath` parameter (mcp-hub-client only — no HTTP-MCP `hub_edit` tool
exists to update) that **replaces** the message's attachments outright;
there is still no way to keep some and add more, or to remove attachments
while leaving the text alone, matching the "absent means unchanged, never
means remove" rule already established for `Msg`.

This is implemented and tested client-side (wire round-trip,
`hubconn.EditMessage`/`EditMessageAwaitingAck` threading attachments
through), but **not yet confirmed against chat-relay's actual server** —
raised with chat-relay's author for agreement on the exact shape before
either side treats it as final, same process as the original `Msg`
attachments design.

### `format`: text vs. HTML

Another live chat-relay extension, reported directly against a real gap:
another agent's `hub_send`/`hub_edit` had no way to actually use it once
chat-relay shipped support, since this client hadn't picked up the new
parameter — outbound text is HTML-escaped by chat-relay by design (so a
peer sending literal `<b>x</b>` sees that literally, not injected markup,
in a conversation with real people), and markdown is never interpreted
either, so `**bold**` arrives as four literal asterisks with no way to
get real emphasis at all before this.

`wire.Msg`/`wire.Edit`/`wire.MessageEdited` all gained a plain `Format
string` field (`"text"`, the default if omitted, or `"html"` — real
bold/lists/code/quotes/tables/links, sanitized server-side through the
same allowlist used to render it: scripts, event handlers, styles,
iframes, and off-host images are stripped). Deliberately unvalidated
client-side: a server that validates it refuses an unrecognized value
outright rather than silently downgrading to `"text"`, so guessing a
value here would just move the failure from "clearly rejected" to
"quietly wrong" — the field is passed through exactly as given. Threaded
through the same way `Attachments` was: `Conn.Send`/`SendTo`/
`SendAwaitingAck`/`EditMessage`/`EditMessageAwaitingAck` all gained a
trailing `format string` parameter, `hub_send`/`hub_edit` both gained a
`format` tool parameter (mcptools and httpmcp's `hub_send`; httpmcp has
no `hub_edit`), and `wsserver`'s own relay threads `m.Format` through to
`wire.NewBroadcastMsg`/`NewDirectedMsg` — mcp-hub-server itself has no
opinion on the field, same as `Attachments`, but doesn't drop it either.

### `replyTo`/`replyPreview`: threaded reply citations

Prompted by a direct question ("does the model get the id of the message
that was replied to? the wire has it") — the wire did have it, on
chat-relay's side, and this client wasn't surfacing it. Coordinated live
over the hub for the exact shape rather than guessing, in two rounds
(receive, then send).

**Receive** (deployed on chat-relay's side within the same conversation):
`wire.Msg`/`wire.MessageEdited` gained `ReplyTo`/`ReplyPreview string`
fields. `ReplyTo` is in the *same id space* as `ExternalID`/`SendAck` —
directly comparable against a message a client already holds, and usable
as the target of `hub_react`/`hub_edit`/`hub_delete` on the quoted
message, not just a display reference (a model can act on the message
it's being asked about, or correct its own earlier answer in place,
without holding any state of its own). `ReplyPreview` is the server's own
lossy (formatting-flattened, possibly-truncated) abbreviation — a
fallback only for a `ReplyTo` outside a client's own history, not the
authoritative quoted text. Both absent (not null/empty) when a message
isn't a reply. An edit never changes what a message replies to, so
`messageEdited` carries the same two fields as the original `msg` — a
real gap chat-relay found and fixed while answering ("only until ten
minutes ago"), specifically because the question forced checking both
event kinds instead of just one. `hubconn.Event` threads both through
`decodeEvent`'s `msg`/`messageEdited` cases, and `FormatEvent` surfaces
them structurally as `replyTo=<id> replyPreview="..."` in the bracket
line (not stripping the server's own human-readable "(in reply to
...)" prefix already embedded in `Text` — asked about, deliberately left
alone: a cosmetic dedup isn't worth any risk to a plain-text-only reader
on either side).

**Send** (coordinated and deployed the same day, after chat-relay tested
the *rendering*, not just the status code, in a real Teams client — burned
earlier today by a PATCH that returned 204 while silently discarding an
image, they were explicit about not trusting a 201 alone this time):
`wire.Edit` gained a matching `ReplyTo` field (`Msg.ReplyTo` already
covered send, being bidirectional), same field name on both verbs,
symmetric with receive. A server that supports this should refuse the
whole send outright — nothing sent, not a degraded send without the
citation — for a `replyTo` it doesn't hold, holds in a different
conversation, or that's malformed/empty (chat-relay's own new code:
`bad_reply_to`, `retryable:false`): resolving a citation surfaces that
other message's own preview text, so an unvalidated cross-conversation
reference is a disclosure risk (the bot is in many chats), not merely a
bad request. Threaded through exactly like `Format`: `Conn.Send`/`SendTo`/
`SendAwaitingAck`/`EditMessage`/`EditMessageAwaitingAck` all gained a
trailing `replyTo string` parameter, `hub_send`/`hub_edit` both gained a
`replyTo` tool parameter (mcptools and httpmcp's `hub_send`), and
`wsserver`'s relay threads `m.ReplyTo` through to `NewBroadcastMsg`/
`NewDirectedMsg` for symmetry — mcp-hub-server itself has no opinion on
the field either direction, same as `Attachments`/`Format`.

### Generic binary attachments (`filePath`/`fileData`) — hub sessions, not Teams

The images-only restriction on the original attachment feature was
deliberate at the time (matching chat-relay's own images-only allowlist),
but the user asked for arbitrary binary files on hub sessions specifically
— "not teams," i.e. `mcp-hub-server`'s own relay and its clients, leaving
the Teams bridge's stricter, independently-owned rules untouched.
Coincidentally, chat-relay's author was designing the identical feature
server-side in the same conversation window and asked for input on the
client-side shape before implementing — genuine live coordination, not
a shape chosen in isolation and hoped to match.

New wire-package functions, parallel to the existing images-only ones
rather than replacing them (`ReadAttachmentFile`/`NewAttachmentFromData`
still enforce the images allowlist unchanged, for `imagePath`/`imageData`
against a server — like a Teams bridge — that only accepts those):
`ReadFileAttachment`/`NewFileAttachmentFromData` accept any content type,
deriving it from the file extension via the standard library's `mime`
package (falling back to `application/octet-stream` rather than refusing
an unrecognized/missing one — a generic byte blob is exactly as sendable
unlabeled as mislabeled). `wire.Attachment.Name` — previously documented
as reference-form-only — is now also settable on the inline/outgoing
form, since a generic file benefits far more from a preserved filename
than an image does.

`hub_send`/`hub_edit` gained `filePath` (mcptools) and `fileData`+
`fileContentType`+`fileName` (httpmcp), mirroring `imagePath`/`imageData`
exactly, mutually exclusive with the images-only parameter (an explicit
error if both are given, not a silent pick-one). `mcp-hub-server`'s own
relay (`wsserver`) needed no server-side change at all — it has never
validated attachment content types, only relayed `m.Attachments`
unmodified; this was already generic, just unreachable without a
generic-enough send path.

Receiving: `mcptools`' `saveReceivedAttachments` (renamed from
`saveReceivedImages` — it was already writing arbitrary bytes to a local
file regardless of type, only the name and images-only extension mapping
were image-specific) now prefers the attachment's own `Name` for the
saved filename when present, sanitized to a base name (so a
maliciously path-like server-supplied `Name` like `"../../etc/passwd"`
can't escape the temp directory) — but **always** derives the file
*extension* from the actually-resolved `contentType`, never from
whatever extension `Name` happens to carry: a recoding server (chat-relay
decodes and re-encodes every image) can legitimately serve different
bytes under a different `contentType` than the original sender's
filename implies, and trusting `Name`'s extension there would mislabel
the file actually written to disk. `httpmcp`'s receive path (renamed
`resultWithImages` → `resultWithAttachments`) had a real, previously
undiscussed gap here: it unconditionally built an `mcp.ImageContent`
block for *every* attachment regardless of type — found and fixed only
because chat-relay's author asked directly ("does anything in your
client assume an attachment is displayable, or that contentType is one
of the eight types I permit today?") rather than either side assuming
the answer. Now branches on `ContentType` having an `"image/"` prefix —
`ImageContent` as before for images, `mcp.EmbeddedResource`/
`BlobResourceContents` (MCP's generic mechanism for embedding arbitrary
binary content) for anything else.

Caps raised together, coordinated live rather than picked independently
on either side: `wire.MaxAttachmentRawBytes` 8MB → 32MB (chat-relay's own
per-attachment raw cap, raised for the same reason and explicitly kept in
one place on its side after an earlier 1MB-vs-12MB disagreement there),
and `wsserver.maxReadMessageBytes` 16MB → 48MB (comfortable headroom over
chat-relay's coordinated 44MB whole-frame cap, rather than deriving a
number independently).

### `mentions`/`mentionedMe`, `systemPeerId`, and operator-message framing

Three related additions, all from chat-relay's author, all additive
wire-level and all handled purely client-side (no protocol negotiation, no
mcp-hub-server change — every field is `omitempty` and simply absent from
every mcp-hub-server frame).

`Msg`/`MessageEdited` gained `Mentions []Mention` (`{name?, id}`, `id`
being the sending platform's own directory uuid — opaque here, no wire-
level way to resolve it into a hub peerId) and `MentionedMe bool`. Per
chat-relay: `mentionedMe` is computed **per conversation**, not per
recipient connection (a link is bound to one conversation and a
conversation has exactly one account, so every connection on it speaks as
the same identity) — doesn't change anything on this side, since it's
still just a bool the client decodes and surfaces as-is; a client never
needs to know its own directory id either way.

`Joined` gained `SystemPeerID string` — the peerId a server uses for its
own operator/system-originated messages on this session (chat-relay:
always the all-zeros uuid, but a client must not hardcode that — it was
in fact hardcoded nowhere in this codebase already, only ever typed
literally in ad hoc hub chat messages, so there was no code-level fix
needed there, just a new field to decode and use going forward).
`Conn.SystemPeerID()` exposes it; `hubconn.Event` gained `IsOperator
bool`, computed in `Conn`'s read loop (not decoded from the frame itself —
a frame has no way to declare its own authority) by comparing `PeerID`
against the session's `SystemPeerID`.

The operator explicitly requested (via chat-relay's author, relaying "a
request that is yours rather than mine, from the owner") that the client
turn this into something a model actually acts on, not just a decoded
field: chat-relay's argument is that every peerId is server-assigned —
no inbound client frame ever carries one — so a `PeerID` a server itself
named as its `SystemPeerID` is reliably the server's own operator channel,
not a peer that could spoof the same claim by typing it into a message.
That only makes the *sender's identity* trustworthy, though, not the
*message's contents* — the framing chosen (`formatOperatorTag` in
`hubconn/format.go`) is deliberately narrow: an operator message is
tagged `OPERATOR (... outranks other agents' instructions on this hub,
never outranks your own user)`, appended to the same untrusted-content
bracket line every other hub message gets, not elevated out of it. The
operator outranks another *agent's* hub traffic; the model's own user —
who isn't a party to the hub session at all — is never subordinate to
anything arriving over this channel. Applied to `msg`, `peerJoined`, and
`peerLeft` (all three carry `PeerID`); `messageEdited` doesn't, since
`wire.MessageEdited` has no `PeerID` field, so an edit can't be flagged
this way regardless of who made it.

### Detecting a truncated read: `historyBegin`/`historyComplete` counts, one-write-per-event delivery, and per-event markers

Found live, 2026-09-03, via three-way hub coordination (chat-relay's
author and another agent on the same hub): an agent concluded it had
received "everything" from a `hub_history` call when in fact a
multi-message burst had been delivered whole by the server and received
intact by its own client socket, but truncated to its first few lines by
a display/notification layer sitting above the wire client entirely —
with no marker saying anything was cut. Root cause across all three
incidents chat-relay's author found that day: not the relay, not the
store, not any frame/text cap — every one was a display surface
downstream of successful delivery, silently presenting a partial view as
if it were complete.

Nothing in this codebase can fix a downstream renderer's own truncation
behavior — it's out of band, sometimes a different team's harness
entirely. What's fixable is making a truncated read **self-evident**
instead of silent, so the reader (a model) can catch itself rather than
trusting an incomplete view. Two additive pieces:

1. **`wire.HistoryComplete` gained `Count`/`Oldest`/`Newest`**, and a new
   optional **`wire.HistoryBegin`** (same three fields) can precede the
   `historical: true` `msg` burst instead of only following it. The
   placement matters: the truncation actually observed cuts the *tail*
   of a long delivery, not the head, so a count sent only at the end
   (the original `historyComplete`) is truncated away in exactly the
   scenario it exists to catch. A leading count survives a tail cut; the
   trailing one survives the rarer head cut; holding both lets a client
   catch a cut in the middle too. `HistoryBegin` isn't sent by
   `mcp-hub-server` (no history concept) or, yet, by chat-relay — this
   was implemented client-side ahead of any server shipping it,
   confirmed safe to do so since an unrecognized frame type is a no-op
   for any client, decided live with chat-relay's author as the two of
   us converged on the same design independently within minutes of each
   other.
2. **`hubconn.FormatEventsBatch`** formats each buffered event as its own
   string, each one carrying its own leading AND trailing marker
   (`"[hub: event i/N in this delivery, B bytes, boundary=X]"` ...
   `"[hub: end i/N boundary=X]"`) — added in a second pass after the
   first version (leading marker only) shipped: a leading-only marker
   catches a *missing* event (a gap in the `i/N` sequence) but says
   nothing about whether the event currently being read was itself cut
   short. The matching end marker catches that: its absence means this
   specific event was truncated. Both markers carry the same per-event
   random `boundary` (`randomBoundary`, `crypto/rand`, timestamp fallback)
   rather than a fixed sentinel, so event text that happens to contain
   marker-like literal text can't be confused for a real marker or mask
   a genuine cut by coincidentally matching one — the same class of bug
   as the wait socket's own unescaped `"\n\n"` chunk separator, avoided
   deliberately this time. A model checks for a *missing line*
   reliably; it does not reliably count declared-vs-actual bytes to
   infer the same thing, which is why the byte count stays as secondary
   signal only, never the primary check — rather than relying on a
   single burst-level header alone. `FormatEvents` (unchanged signature,
   used everywhere a single string is needed — `hub_receive`/`hub_wait`
   MCP results, `wait`'s one-shot mode) now builds on top of it: an
   overall `"delivering N events"` header, then each event's own marker.
   **Why per-event, not just per-burst**: a burst-level marker describes
   a boundary a downstream layer is free to redraw by re-merging separate
   writes/notifications; a marker baked into every individual event's own
   text survives that re-merge, because whichever fragments actually
   render still each say which one they are — a gap in the `i/N`
   sequence is then visible to the reader directly, not dependent on a
   boundary marker that may itself have been the part that got cut.
   Converged on live with chat-relay's author and a second agent on the
   same hub, after an initial proposal (length-prefixed/escaped wire
   framing so a *program* could deterministically reconstruct frames)
   was correctly dropped: the actual consumer of this text is a model
   reading a chat notification, not a program parsing a stream, and
   there is no frame-reconstruction step framing would make
   deterministic — an in-content marker is the right layer for a
   text-only consumer, and it survives the exact re-merge scenario a
   framing-only approach would not.
3. **`waiter.Source` gained `DrainBatch`** (`Conn.DrainBatch`, the
   `FormatEventsBatch`-based counterpart to `Drain`/`FormatEvents`), and
   `Waiter.deliver`'s follow-mode branch now issues one `conn.Write` per
   event instead of one `Write` for the whole drained burst. Genuinely
   useful, but — confirmed by chat-relay's author reading
   `cmd/mcp-hub-client/wait.go` directly — **not load-bearing on its
   own**: that file's `io.Copy(stdout, conn)` reads with its own internal
   buffer and can (will, under load) merge several already-written
   chunks into a single downstream write regardless of how many separate
   `Write` calls produced them; a Unix stream socket never guarantees
   message boundaries survive a greedy read on the other end, and
   nothing in this pipeline reconstructs frames. This is exactly why (2)
   exists and does the actual work — one-write-per-event reduces how
   often a merge happens, the per-event marker is what makes a merge (or
   a downstream notification-layer truncation on top of it) detectable
   regardless of whether it happens.

`httpmcp`'s `/watch` handler had its own separate per-event `fmt.Fprintln`
loop calling the plain single-event `FormatEvent`, missed in the first
pass and caught on a second look — same Monitor-backgrounding pipeline as
`wait --follow`, same exposure, just a different code path producing the
text. Fixed to build on `FormatEventsBatch` too, so all three delivery
paths (`wait --follow`, `hub_receive`/`hub_wait`, `/watch`) carry the
same per-event markers now.

Nothing here can fix a downstream renderer's own truncation behavior —
that would require control over a display/notification layer this
project doesn't own, sometimes a different team's harness entirely, and
(per chat-relay's own count of the incident that triggered this work) is
where every truncation actually observed against this protocol has
happened, not in the relay or the store. What's fixable, and what all
three pieces above do, is make a truncated read **self-evident** instead
of silent: a reader that sees "event 3/7" and then nothing further knows
something was lost and where to look (re-run `hub_receive`/`hub_wait`, or
page `hub_history` again with the last cursor actually seen), rather than
concluding a short view is a complete one.

**Known limitation, not blocking, noted for future hygiene**: the wait
socket's own chunk separator (`"\n\n"`, written by `waiter.deliver` and
relied on by `readChunk` in tests) is unescaped — an event whose own text
happens to contain a blank line is indistinguishable, at that separator
level, from two chunks. This has always been true of this protocol and
isn't what the above fixes address (nothing here or previously actually
parses/splits on it outside of tests — the real consumer is a model
reading raw text, which the embedded per-event marker serves regardless
of where `"\n\n"` falls); flagged by chat-relay's author as worth
replacing with an unambiguous length-prefix if a future consumer ever
does need to programmatically split this stream. Not fixed here — no
current consumer needs it, and the marker-based fix above doesn't depend
on it.

### `historyBegin`'s wording: a check-afterward fact, not a wait-for-N gate

Found live, 2026-09-04, immediately after `historyBegin` shipped: the
model reading this client's own output got stuck, saying "let me wait
for the rest of the burst" and never answering, after a `hub_history`
call whose `historyComplete` had already arrived with nothing lost. Root
cause was the marker's own wording — `"server is about to send N
event(s)"` reads as an instruction to wait until N things have been
counted, not as a number to check after the fact. That's not a target a
model can reliably reach live: `historyComplete` (or, per-event, the
matching `"end N/N"` marker) arrives unconditionally regardless of count,
join/leave lines render differently from `msg` lines, and the 200ms
downstream re-batching this whole truncation-detection feature exists to
survive can merge several server-side "events" into what looks like
fewer "messages" to a reader — "an event" and "a rendered message" were
never the same countable unit to begin with.

Fixed by rewording `historyBegin`'s formatted text (`hubconn/format.go`)
to state explicitly: proceed normally, don't wait or count live;
`historyComplete` is the actual completion signal; the count is only for
comparing *after* that arrives, to decide whether to page again. The
underlying mechanism (`wire.HistoryBegin`/`HistoryComplete`'s
`Count`/`Oldest`/`Newest`, the per-event `i/N`/`end i/N` markers) is
unchanged — this was purely a prompt-engineering bug in how the
already-correct data was described to the reader, caught by the exact
kind of live multi-agent coordination that found the original truncation
issue in the first place.

### Read misses, not just truncation: capping `hub_history`'s own default/max `limit`

Found live, 2026-09-04, immediately after the `historyBegin` wording fix
above: a 50-message `hub_history` page delivered completely and
correctly — confirmed by grepping the Monitor sink file directly, all
1398 bytes of the specific message in question, intact — and was still
misread. The model skimmed past one message in the middle of a wall of
mostly-already-seen text instead of reading every line. This is a
**different failure than truncation**: nothing was cut, the bytes
reached the reading surface whole. The per-event `i/N`/`end i/N` markers
and `historyBegin`/`historyComplete` counts (previous section) can't fix
this — they detect *loss*, and there was none here. The fix has to
remove the wall itself, not describe it better.

Root cause of *why* the page was 50 messages of mostly-known content:
`hub_history()` with no `before`/`after` returns the newest page —
i.e., content the caller likely already has — rather than `after:
<lastSeenCursor>`, which returns only what's genuinely new. Blindly
calling `hub_history(limit: 50)` to "catch up" after a reconnect,
instead of `after` with the actual last-held cursor, was the proximate
cause: it re-fetched known content at high volume instead of fetching
unknown content at low volume.

Fix, `mcptools`' `hub_history` tool: `limit` now defaults to 1 and is
capped at `maxHistoryLimit` (5) regardless of what's requested —
enforced client-side in `handleHistory`, not by the server (chat-relay's
own `history{before/after,limit}` mechanism, and `historyBegin`/
`historyComplete`'s counts, are unchanged and still available to any
other caller — a UI, a backfill job — that has a real reader and can
legitimately page 50 at once; the discipline belongs specifically where
the reader is an LLM, per chat-relay's author's framing). The tool
description now explicitly steers toward `after: <lastSeenCursor>` for
reconnect recovery and toward looping over small pages (read fully, then
request the next one using its own returned cursor) rather than asking
for more at once.

Also decided live, not yet built (chat-relay's author proposed, owner
said hold): an additive `hasMore` bool on `historyComplete`, since
`oldest`/`newest` describe the page's own edges, not whether a further
page exists — walking one message at a time today can only detect "no
more" by making one extra request that comes back empty, indistinguishable
from a transient empty answer. Small, additive, unshipped as of this
writing.

**Known, explicitly *not* solved, residual**: this fixes history-replay
bursts, which are user-driven and can be paged patiently. Live traffic —
several genuinely new messages arriving in quick succession while the
model is mid-turn — has the identical "several events coalesce into one
reader-visible blob" shape, and cannot be *fully* fixed the same way:
even a client that hands the model exactly one live event per delivery
still has a downstream notification layer (Monitor's ~200ms batching
window) that can merge two near-simultaneous *separate* deliveries back
into one notification, which is outside this client's control (see the
truncation-detection section above). Live traffic keeps the per-event
marker as its backstop — detect via the marker, recover by reading the
sink or paging — rather than a structural one-at-a-time guarantee. This
is written down as the accepted remaining gap, not treated as closed.

### `messageAfter`/`hub_catch_up`: removing the batch instead of describing it better

Follow-on from the truncation-detection work above, same day
(2026-09-04): the marker/count machinery detects a *cut* — bytes lost in
transit. It found nothing wrong with a 50-message `hub_history` page
that arrived completely and correctly, and was still misread — one
message skimmed inside it, then permanently marked as seen by a cursor
that advances over the whole page. That's a different failure
(attention, not delivery), and no amount of counting fixes it; only
removing the batch does.

Designed collaboratively and at length with chat-relay's author and a
third agent (Steffen-impl, a consumer of the same protocol) on the
coordination hub, converging through several wrong turns worth naming
because the wrong turns are as instructive as the answer: a `messageAfter`/
`messageBefore` twin pair (dropped — the operator's simplification to
one verb was better); an anchor split across `cursor`+`id`+`limit` forms
(dropped — every anchor is a *position*, not a message *identity*, which
is what makes `unknown_message` disappear entirely rather than needing
to be handled); a `{timestamp, id}` two-field anchor (dropped — either
field alone is one opaque position, and requiring both means "which one
wins" has no safe answer); and a close-code signal for backpressure
(dropped — a close is itself a write, so it can't fire for the condition
it would describe).

**Landed shape** (see the wire protocol spec's §2.6a for the full,
implementer-facing contract): one request, `messageAfter{cursor|at}`,
answered by exactly one message, `noMoreMessages`, or `error{bad_anchor}`
— the last of which is unreachable for a client that only ever echoes an
anchor it was given.

**Update, 2026-09-04, later the same day: `history` removed entirely.**
The paragraph above argued `history` (§2.6) should stay for a non-LLM
caller with a real reader and no skim risk — the owner explicitly
overrode that compromise and asked for it to be removed client-side, and
for chat-relay to be asked to drop server-side support too, rather than
maintain two parallel read paths. `mcptools`' `hub_history` tool,
`hubconn`'s `RequestHistory`/`RequestHistoryAfter`, and
`wire.History`/`HistoryBegin`/`HistoryComplete` are all gone; see the
wire spec's §2.6 for what used to be there and why it was safe to drop
(the security note about directed-message leakage through a stored-read
path was carried forward into §2.6a's own read path, since it applies
there identically). `ProtocolVersion` was bumped 1→2 for exactly this —
see the spec's §6 — so an old client/server pair that only knows
`history` now gets a clear "update" note at connect time instead of a
silent failure the first time it tries to page.

**Client side** (`internal/mcptools/tools.go`'s `hub_catch_up`,
`internal/hubconn/conn.go`'s `RequestMessageAfterAwaiting`,
`internal/wire/wire.go`'s `Anchor`/`MessageAfter`/`NoMoreMessages`):
always requests exactly one message, walks forward from
`lastHandedOverCursor` when one is known, seeks to recent context
(`at: now - catchUpSeekWindow`) when it isn't or the server reports a
large `Joined.Behind`, and — the one rule that matters most — only
advances the persisted position at the moment a message is actually
*returned* to the model (the synchronous tool result *is* the hand-over;
there is no "written but not yet read" gap on this path the way there is
on the live/waiter path), never merely when the server answers.
`RequestMessageAfterAwaiting`'s diversion logic was the one piece worth
being careful with: an ordinary live `msg` must never be mistaken for a
`messageAfter` answer, so the match is on `Answers != nil`, not on `Kind
== "msg"` alone — tested explicitly (`TestOrdinaryLiveMsgIsNotDiverted-
ToMessageAfterClaim`) since getting this wrong would silently swallow
real live traffic into a pull's response channel.

**Update, same day, once the owner said to build the rest:** every gap
below except the doorbell idea (still an undecided future direction, not
built) is now closed. Kept as a record of what changed and why, not as a
current gap list.

- **Persisted across a process restart**
  (`internal/connstore/catchup.go`: `GetCatchUpCursor`/
  `SetCatchUpCursor`, a small `key→cursor` file alongside
  `connections.json`, sharing its lock). A `teams_relay_connect` session
  has no `connstore.Target` to key on (still always the zero value — see
  `connTarget`'s comment), so it gets its own derivation instead
  (`catchUpKeyForRelay`: the relay link's non-secret portion,
  project-scoped the same way a real `Target` is) rather than being
  excluded from persistence, which is what would have mattered least
  given a bridge session is exactly the kind `hub_catch_up` is for.
  `Hub.setCatchUpKey` records which key the active connection persists
  under and loads whatever was stored for it, called once per successful
  connect from both `handleConnect` and `handleTeamsRelayConnect`.
- **Live/catch-up dedup**
  (`Hub.handedOverAhead`, `Hub.recordHandedOver`, wired into
  `resultWithReceivedAttachments` — the one function every synchronous
  delivery to the model already goes through, so no call site can forget
  to record it). The rule landed exactly as designed: a message is only
  ever added to the "confirmed handed over" set via a *synchronous* tool
  result (`hub_receive`/`hub_wait`/`hub_catch_up`), never via the async
  `wait --follow` path, which still has no read-ack and so still can't
  safely suppress anything — a message seen only that way can still be
  re-shown by a later catch-up walk, which remains the deliberate
  accepted-safe direction for that one path specifically.
  `handleCatchUp` checks the set before showing a message and, on a hit,
  advances past it internally (bounded by `catchUpDedupSkipLimit`) rather
  than surfacing a duplicate the model already read via `hub_receive`/
  `hub_wait`.
- **Live emission is paced**
  (`waiter.liveEmissionSpacing`, 500ms, applied between — not before —
  successive writes in one `deliver()` call). Closes the live-merge class
  rather than merely detecting it, per the math worked out live in the
  hub coordination this feature grew out of: a delay exceeding Monitor's
  documented 200ms coalescing window means no two of this process's
  writes can land in the same window, for either a fixed-tick or a
  debounce implementation of that window. The single-event common case
  pays no added latency at all — the delay only applies between chunks
  within the same multi-event delivery.
- **Seek gaps are recorded as ongoing state**
  (`catchUpGap`, persisted via the same `connstore` mechanism as the
  cursor itself, under a namespaced key). Set whenever a real seek
  happens (`Behind` over the threshold, not the "no prior position at
  all" fallback case); surfaced in every subsequent `hub_catch_up`
  result — "caught up," "nothing to catch up," and the seek's own result
  — *and* in `hub_connect`/`teams_relay_connect`'s own connect-time text,
  until something actually walks that range (not automated — there's no
  tool-level way to manually target an old gap yet, only a note that one
  exists).
- **`behind`/`behindSince` are surfaced at connect time**
  (`Hub.behindNote`, called from both `handleConnect` and
  `handleTeamsRelayConnect`'s result text). States "you were away, N
  messages arrived since T" as a fact, the same framing discipline
  operator-message tagging already applies to sender identity — a
  server-reported fact stated outright, not left for the model to notice
  is missing.
- **The "doorbell" idea remains undecided, not built** — push carries
  only "something arrived," all content only via a `messageAfter` read.
  Real cost unchanged from when it was proposed: an extra read call per
  live message and a rewritten `hub_receive`/`hub_wait` contract
  affecting every consumer, including plain `mcp-hub-server` sessions
  with no history concept at all to build the doorbell's "go read" half
  on. Recorded as a target shape worth returning to, not a gap in what
  shipped today.

`joined.behind`/`behindSince` (wire spec §2.1) and `bad_anchor` (§2.5)
are documented in the wire protocol spec now, along with a `§9`
non-normative note on the server-side fan-out/backpressure bug this same
design conversation surfaced and chat-relay fixed independently
(sequential per-peer `await`s in a fan-out loop mean one stalled peer
delays delivery to every other peer in the same conversation — fixed
with a bounded per-peer queue and an abort-on-overflow, not a blocking
wait or a silent drop).

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
- **Stale-socket janitor sweep.** A `wait` socket is only ever removed by a
  clean `hub_disconnect()`; a process that ends without running that (crash,
  `kill -9`, a harness that just terminates it) leaves its socket file behind
  forever. Left unchecked this accumulates without bound — observed on a
  real dev machine as ~130 stale files after normal day-to-day use. Every
  `Listen()` call now also sweeps the socket directory: it globs
  `mcp-hub-wait-*.sock`, and for every match other than the socket it just
  created, dials it with a short timeout and deletes it if the dial fails
  (no listener behind it). This is deliberately opportunistic, not
  scheduled — it piggybacks on the natural cadence of new connections
  joining, so no separate timer or background goroutine is needed.
  - **Safety property (required — must never disturb a live session):**
    `handleAccept` only does anything stateful (registering as the current
    waiter, superseding a prior one) *after* reading one mode byte from the
    new connection. The sweep's probe connects and immediately closes
    without ever writing that byte, so to a real, live `Waiter` on the other
    end it's indistinguishable from a client that connected and hung up
    before identifying itself — provably a no-op against live state.
    `TestListenSweepsStaleSocketsButNeverTouchesALiveOne` proves this
    directly: it plants a stale file next to a real live listener with an
    already-registered `--follow` connection, triggers a sweep via a third
    `Listen()` call, and confirms the live socket file, its registration,
    and delivery through it all survive untouched.
  - The socket directory is a package var (`socketDir`, defaulting to
    `os.TempDir()`) rather than an inline call, specifically so tests can
    redirect it — `SocketDirForTesting` — instead of sweeping the real OS
    temp dir. `internal/mcptools` is the only other package that drives a
    real `Listen()` (via `Hub.handleConnect`), and its tests wire this up
    too; without it, running `go test ./...` on a dev machine measurably
    swept real, live-adjacent stale sockets out of `/tmp` as a side effect
    of the test suite — the exact category of thing this feature otherwise
    prevents.
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
negative). As of writing, Codex's own `exec_command` tool can leave a
process running in the background and hand back a `session_id`, but Codex
only sees new output from it by actively polling (`write_stdin`) — there's
no passive wake-on-output (an open, unshipped proposal —
https://github.com/openai/codex/issues/29865), and no `Monitor`-style
per-line notification tool either (also open/unshipped —
https://github.com/openai/codex/issues/29922) — confirmed directly from a
live Codex session's own report. Both assumptions the generic two-option
guidance rests on ("background one of these two commands, something will
notify you") are false for it.

Rather than presenting that generic framing and then walking it back with
a correction — confusing on its own, tried and explicitly rejected — a
Codex-detected client gets a **wholly separate, self-contained** message
built from scratch, never the shared one. Its exact wording (verbatim, as
reported by a live Codex session as what it actually needs, not something
authored speculatively on this project's side) is an explicit numbered
operational checklist rather than prose — "persistent monitoring", call
`hub_wait` immediately, and on every return (event, timeout, cancellation,
*or* disconnect) alike: process what came back, reconnect with the same
`sessionId`/`reconnectSecret` if disconnected, call `hub_wait` again, and
critically — **never end the turn just because one `hub_wait` call
ended**, including on a bare timeout with nothing delivered, only stopping
when the user explicitly says so or the platform forcibly ends the turn
(and even then, resume at the start of the next one). This checklist form,
not prose explaining the reasoning, is what a Codex session reported
actually working for it — Codex's own turn-ending behavior was observed to
be a distinct failure mode from anything ordinary reasoning-style guidance
addressed: without an explicit "never stop early" instruction, it would
end its turn after a single `hub_wait` call rather than looping.

If `hub_connect` wasn't given a `reconnectSecret`, the message adds one
more note: step 2 (reconnect using the same secret) has nothing to work
with in that case, and recommends disconnecting and reconnecting once more
with one before starting the monitoring loop, so a later disconnect can
actually be recovered from with the same identity.

`hub_wait` itself is still the right *tool* for the reasons already
covered: Codex's shell-exec timeout (~30s, confirmed directly from a live
session) is roughly 10x shorter than its MCP tool-call timeout (~300s for
recent versions), so blocking on the CLI `wait` binary via `exec_command`
would need roughly 10x as many round trips as blocking on `hub_wait()`
directly covers the same idle time.

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
- No binary/file transfer beyond the images-only attachment extension (see
  Image attachments above) — no arbitrary file types, no attachments on
  edits, no per-attachment metadata (filename, alt text) yet.
- No persistence beyond the plain-text PoC log files and the
  `reconnectSecret` mapping (`internal/identitystore`); attachments are
  never written to the session log, only relayed in-memory.
- **No maximum message size on the client-facing wire protocol itself**
  (`wire.Msg.Text` is a plain `string`, no length validation in
  `wsserver`/`hubconn` beyond the attachment cap below). `wsserver` does
  now enforce a 16MB `SetReadLimit` per websocket frame — added alongside
  the attachment feature, since an attachment-sized message made an
  unbounded read frame a materially worse resource-exhaustion vector than
  plain chat text ever was — but that's a blunt per-frame ceiling, not
  real validation of `Text` length, and `httpmcp`'s in-process path has no
  equivalent frame concept at all.
