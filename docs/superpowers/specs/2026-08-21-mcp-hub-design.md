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
`host`+`sessionId`), rather than requiring the model to remember and
repeat a secret across turns/sessions. Omitted with no prior entry for
that target: one is generated and stored after a successful connect.
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
- No binary/file transfer — text only.
- No persistence beyond the plain-text PoC log files and the
  `reconnectSecret` mapping (`internal/identitystore`).
- **No maximum message size.** Neither the websocket layer (no
  `SetReadLimit` on either end — gorilla/websocket's default is
  unlimited) nor the wire protocol (`wire.Msg.Text` is a plain `string`,
  no length validation anywhere in `wsserver`/`hubconn`) enforces a cap.
  A message of any size is fully buffered in memory by every connected
  peer and written in full to the session log, with no truncation —
  a real resource-exhaustion vector against a malicious or buggy peer,
  explicitly left unfixed for now (asked directly, declined) rather than
  an oversight.
