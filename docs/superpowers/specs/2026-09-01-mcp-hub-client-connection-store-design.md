# mcp-hub-client: persisted connection identity

## Motivation

Today `reconnectSecret` must be supplied by the model on every `hub_connect`,
and the model itself is responsible for remembering it across turns/sessions
to reclaim the same `peerId` later. This is fragile (the model can forget or
misremember it) and puts a security-relevant secret into the model's own
conversation context for no real benefit — the client, not the model, is the
natural place to hold it.

Separately, there's no way for the model to discover what hub sessions it
was connected to before, or whether a prior process ended mid-connection
(crash, or simply the Claude Code session ending without an explicit
`hub_disconnect`).

Scope: `mcp-hub-client` (the stdio client) only. The HTTP-MCP endpoint
(`internal/httpmcp`, on `mcp-hub-server`) has a different lifecycle — no
single long-lived local process to hold a file's worth of state the way a
stdio client does — and needs its own design if this is wanted there later.

## Storage

New package `internal/connstore`. One JSON file at
`os.UserConfigDir()/mcp-hub/connections.json`, holding a map keyed by
`host + "\x00" + sessionId`:

```go
type Entry struct {
    Host            string
    SessionID       string
    PeerID          string
    Name            string
    ReconnectSecret string
    LastConnectedAt time.Time
    Connected       bool
}
```

- `Connected` is set `true` on a successful `hub_connect`, and set `false`
  only by an explicit `hub_disconnect`. Nothing currently disconnects
  automatically when a process ends, so a leftover `Connected: true` entry
  after a restart is exactly the signal for "this was open when the
  process last ended" — crash or an ordinary Claude Code session ending
  mid-conversation look the same on disk, and that's fine: both mean "you
  didn't get a chance to clean this up."
- Reads/writes are read-modify-write, written atomically via a temp file
  + rename. Concurrent `mcp-hub-client` processes (e.g. two Claude Code
  sessions in different projects) racing a write to this single shared
  file is an accepted PoC-grade limitation — no file locking — consistent
  with this project's existing precedent elsewhere (e.g. `hublog`'s
  session logs). Not addressed here; flagged, not silently accepted.
- The store is process-wide, not project-scoped — `mcp-hub-client` has no
  project awareness at all (confirmed: it reads no CWD, no project env
  vars), so this is deliberately one shared file across every invocation
  on the machine, keyed by the actual connection target, not by project.

## `hub_connect` behavior change

`reconnectSecret` becomes optional:

- **Omitted, stored entry exists** for `(host, sessionId)` → reuse the
  stored secret automatically. The result text tells the model this
  happened and names the `peerId` it got back, so the model can sanity
  check it matches the stored one.
- **Omitted, no stored entry** → auto-generate one (`uuid.NewString()`,
  matching existing patterns elsewhere in this codebase), store it after
  the connect actually succeeds (not before — a failed connect shouldn't
  leave a phantom entry).
- **Explicitly passed by the model** → always wins, exactly as today.
  This *is* the reject/override mechanism: no separate flag or "forget"
  tool. A model that wants a fresh identity for a target it has a stored
  entry for just passes a new `reconnectSecret` itself.
- On success (either path), the store is updated: `PeerID`, `Name`,
  `LastConnectedAt`, `Connected: true`.
- On `hub_disconnect`, the matching entry (if any) is updated:
  `Connected: false`.

## New tool: `hub_list_connections`

Read-only. Lists every stored entry — host, sessionId, peerId, name,
last-connected time, whether still marked open. No side effects, no
`reconnectSecret` in the output (it's meant to stay out of the model's
context by default — this tool would defeat that if it echoed secrets
back).

## "Notify at startup"

Baked into `hub_connect`'s tool description text, computed once when
`Register()` runs — which happens exactly once per process launch, i.e.
"startup." If any stored entries have `Connected: true` at that moment,
the description gets an appended note listing them (host, sessionId,
last-connected time) and pointing at `hub_list_connections` for the full
list.

**Disclosed limitation, not hidden:** this is not a proactive push — MCP
gives a server no mechanism for that at all (the same reason `wait`/
`hub_wait`/Monitor exist for live events). It's visible whenever the
model actually looks at `hub_connect`'s description, which may not be
immediately: Claude Code's Tool Search defers full tool schemas out of
context by default, so a model that never inspects the tool won't see
the note until it does. This is the best available mechanism given MCP's
constraints, not a complete guarantee the model notices.

## Testing

- `internal/connstore`: load/save round trip, atomic write (temp file +
  rename), missing-file-is-empty-store, concurrent-safe-enough (no
  corruption from a single process's sequential read-modify-write cycle
  — not a concurrency stress test, given the accepted limitation above).
- `internal/mcptools`: `handleConnect` auto-generates and stores a secret
  when omitted with no prior entry; reuses a stored one when omitted with
  a prior entry; an explicitly-passed secret is used as-is and still
  updates the store; `handleDisconnect` marks the entry `Connected:
  false`; `hub_list_connections` lists what's stored without leaking
  `ReconnectSecret`; `Register()`'s `hub_connect` description includes
  the startup note only when a `Connected: true` entry exists.
- Full `go build ./... && go vet ./... && go test -race -count=1|2 ./...`
  before considering this done, as with every other change this session.

## Docs

`README.adoc` and the main design spec get a new section describing this
— the optional `reconnectSecret`, `hub_list_connections`, and the
startup-note mechanism and its disclosed limitation.
