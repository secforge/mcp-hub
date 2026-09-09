# HTTP-MCP endpoint on mcp-hub-server

## Motivation

Today the only way to use mcp-hub's tools is `mcp-hub-client`, a stdio MCP
server that dials out to `mcp-hub-server` over a websocket like any other
peer. That means every Claude Code instance that wants hub access must spawn
a local `mcp-hub-client` process, configured per-machine.

The goal here is to let any MCP-HTTP-capable client — in particular, one
whose MCP servers are configured centrally rather than per-machine — talk to
the hub directly over `https://mcp-hub.secforge.de/`, with no local process
and no separate websocket hop. Since `mcp-hub-server` already holds all
session/peer state in-process (`internal/hubsession`), an MCP-HTTP peer can
interact with that state directly — call `Session.Broadcast`/`DeliverTo`
straight from a tool handler — instead of round-tripping through the wire
protocol's JSON encoding to itself.

This is purely additive: the existing `/{sessionId}` websocket endpoint,
`mcp-hub-client`'s stdio mode, and its `wait`/`wait --follow` unix-socket
mechanism are all unmodified.

## Scope

The built-in MCP tool set covers only what `hubsession`/`wsserver` already
understand server-side: connect, send, receive, wait, peers. Reactions,
edits, deletes, and history are chat-relay/teams-specific concepts
that only exist between `mcp-hub-client` and a teams relay's own
implementation (e.g. chat-relay) — `mcp-hub-server`'s relay has no
server-side concept of any of them today, and adding one is out of scope
here.

## Rejected approach: session ID in the connection URL

An earlier version of this design mounted the MCP endpoint at
`/{sessionId}/mcp`, auto-joining on connect exactly like the websocket
endpoint does today. Rejected: an MCP client's server URL is normally part
of its static configuration (e.g. `.mcp.json`) — the model cannot rewrite it
mid-conversation to switch or create sessions. A fixed pre-known URL also
provides no way to create a *new* session. Both needs are why explicit
`hub_connect`/`hub_disconnect` tools exist in `mcp-hub-client` today, and why
this design keeps them rather than encoding session identity in the URL.

## Architecture

**Routing** (same `*http.Server`/mux `mcp-hub-server` already runs):
- `/{sessionId}` — today's websocket endpoint, unmodified.
- `/mcp` — new, single, static Streamable-HTTP MCP endpoint
  (`server.NewStreamableHTTPServer`). Every MCP client connects here
  regardless of which hub session (if any) it ends up joining.
- `/watch` — new, plain (non-MCP) `http.Flusher`-based streaming endpoint,
  `?token=...`. See "Watching" below.

**Per-MCP-session state.** `mcptools.Hub` (the stdio binary's single
package-level instance) has no equivalent notion of multiple concurrent
clients, because stdio is one process per client. The HTTP binary needs one
independent piece of state per MCP client session. A new type, tentatively
`httpHub`, holds a `sync.Map[mcpSessionID]*httpHub` at the top level;
`hub_connect`, in the general case, wraps a nilable `*httpPeer` the way
`mcptools.Hub` wraps a nilable `*hubconn.Conn` today. Each tool handler
resolves its `*httpHub` via `server.ClientSessionFromContext(ctx).SessionID()`
before doing anything else. Registered via
`server.WithHooks(hooks)`:
- `OnRegisterSession`: create and store an empty `*httpHub` for this MCP
  session. (No hub session join happens yet — that's `hub_connect`'s job.)
- `OnUnregisterSession`: if this MCP session's `*httpHub` currently holds a
  joined `*httpPeer`, `Leave()` it (tearing down the hub session via
  `Manager.Remove` if it was the last peer, exactly like `wsserver.serve`'s
  existing deferred cleanup) and delete the map entry.

**Tools registered on the `/mcp` server:**
- `hub_connect(sessionId?, name?, agePublicKey?, reconnectSecret?)` — omit
  `sessionId` to create a fresh session (a new UUID, generated here and
  returned in the result); otherwise join the given one. Constructs an
  `httpPeer`, calls `session.Join(...)` (the same `hubsession.Session` method
  `wsserver` already uses), stores it on this MCP session's `*httpHub`.
  Returns the assigned `peerId`, the `sessionId` (so a freshly created one
  can be shared with whoever else should join), and a **watch token** (see
  below).
- `hub_disconnect()` — `Leave()`, clear the `*httpHub`'s peer, same teardown
  as `OnUnregisterSession` above (also covers a client that explicitly wants
  to leave before its MCP session itself ends).
- `hub_send(text, to?)` — errors if not connected; otherwise calls
  `session.Broadcast(peer, wire.NewBroadcastMsg(...))` or
  `session.DeliverTo(peer, to, wire.NewDirectedMsg(...))` directly. No wire
  JSON round-trip to itself.
- `hub_receive()` — drains `httpPeer`'s buffered events, non-blocking.
- `hub_wait(timeoutSeconds?)` — blocks until an event arrives or the timeout
  elapses; same single-in-flight-supersedes-the-old-one semantics
  `mcp-hub-client`'s `hub_wait` already has.
- `hub_peers()` — needs one small addition to `hubsession.Session`: a
  `Peers() []Peer` read accessor (doesn't exist today; `wsserver` has never
  needed to enumerate a session's peers itself).

**`httpPeer`:** implements `hubsession.Peer` (`ID`/`Name`/`AgePublicKey`
trivially). `Deliver(event any)` appends to a small mutex-protected buffer
and wakes anything blocked in `hub_wait` or reading `/watch` — a lighter,
in-process sibling of `hubconn.Conn`'s buffer/`Peek`/`Drain`, minus
websocket/reconnect machinery, since events here are native Go values, never
JSON-decoded.

**Watching (async notification for Claude specifically).** A blocking
`hub_wait` tool call ties up a conversation turn. Claude's existing pattern
for this — `mcp-hub-client wait --follow` backgrounded and watched via the
Monitor tool — depends on a local subprocess, which doesn't exist for a
direct HTTP-MCP client. The equivalent here: `hub_connect`'s result includes
an opaque, per-connection watch token (minted at join time, mapped 1:1 to
that call's `httpPeer`, unrelated to any hub session ID or peer ID).
`GET /watch?token=...&follow=1` streams that exact peer's buffered/incoming
events as plain text (reusing `hubconn.FormatEvent`'s rendering), flushing
after each one via `http.Flusher`. Claude backgrounds `curl -N` against it
and watches the output with Monitor, the same shape as `wait --follow`
today. No new peer is created and no join/leave events are generated — this
is a read-only tap on the live peer's own inbox, not a second identity.
Invalid/expired/already-disconnected token: 404.

**Auth.** Unchanged from today: the websocket endpoint has none (anyone who
can reach the port and knows a session UUID can join), so `/mcp` and
`/watch` match that — no bearer token added by this change.

## Testing

- `hubsession`: new test(s) for `Session.Peers()`.
- New package (tentatively `internal/httpmcp` or directly under
  `internal/wsserver`) covering: `hub_connect` with/without `sessionId`
  (including the returned `sessionId`/watch token), two concurrent MCP
  sessions joining the same hub session and seeing each other via
  `hub_peers`/broadcast, `hub_wait` timeout and delivery, `hub_disconnect`
  and `OnUnregisterSession` both correctly leaving, `/watch` streaming
  (including `?follow=1` vs single-shot) and its token validation, and that
  the existing `/{sessionId}` websocket path is completely unaffected.
- Full `go build ./... && go vet ./... && go test -race -count=1|2 ./...`
  before considering this done, as with every other change this session.

## Docs

`README.adoc` and this project's design doc both need a new section
describing `mcp-hub-server`'s HTTP-MCP mode (routes, tool set, watch-token
flow) alongside the existing websocket-relay description, once implemented.
