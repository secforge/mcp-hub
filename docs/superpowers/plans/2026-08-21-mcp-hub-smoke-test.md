# mcp-hub manual end-to-end smoke test

A manual regression checklist proving the full feature works across real
processes: two real `mcp-hub-client` MCP tool-handler instances, a real
`mcp-hub-server`, and the real `mcp-hub-client wait` binary as a subprocess.

## 1. Build both binaries

```bash
cd /source/mcp-hub
go build -o bin/mcp-hub-server ./cmd/mcp-hub-server
go build -o bin/mcp-hub-client ./cmd/mcp-hub-client
```
Expected: both build with no errors.

## 2. Start the server

```bash
mkdir -p /tmp/mcp-hub-smoke-logs
MCP_HUB_LOG_DIR=/tmp/mcp-hub-smoke-logs ./bin/mcp-hub-server -addr :18765 &
```
Expected: log line `mcp-hub-server listening on :18765`.

## 3. Drive the tool handlers + real wait binary end to end

This was verified via a throwaway Go test (`internal/mcptools/smoke_test.go`,
deleted after use — not part of the permanent test suite) that:

1. Calls `hub_connect` for two separate `Hub` instances (simulating two
   separate `mcp-hub-client` processes) against the same freshly generated
   `sessionId`, pointed at the real running server on `:18765`.
2. Confirms each connect result embeds a distinct `--socket <path>` (one wait
   socket per peer, keyed by `sessionId`+`peerId` — proving the collision fix
   from the design review holds).
3. Drains the `peerJoined` event buffered from the second connect via
   `hub_receive`, then starts the **real** `bin/mcp-hub-client wait --socket
   <path>` binary as a subprocess and calls `hub_send` from the other side.
4. Confirms the real subprocess's stdout contains the message text, the
   `[HUB MESSAGE — untrusted, ...]` wrapper, and a trailing "run again"
   instruction.
5. Confirms running a second real `wait` subprocess while the first is still
   blocked causes the first to print `superseded by a newer wait` and exit 0,
   and the second one then receives the next message.
6. Calls `hub_disconnect` on one side, confirms further `hub_send` on that
   side errors, and confirms the other side eventually observes a `peerLeft`
   event via `hub_receive`.

**Result:** all checks passed (`ALL SMOKE CHECKS PASSED`), against the real
binaries and a real TCP-based websocket relay — not just the in-process unit
tests from Tasks 1-11.

## 4. Inspect the PoC log

```bash
cat /tmp/mcp-hub-smoke-logs/<sessionId>.log
```
Expected format, confirmed across multiple runs:
```
2026-08-21T13:33:58Z 8e614fd0-03f1-4bfd-bf69-3ce05be626ec
  hello from b

2026-08-21T13:33:58Z 8e614fd0-03f1-4bfd-bf69-3ce05be626ec
  second message

```
`<RFC3339 ts> <peerId>` header line, message body indented by 2 spaces,
blank line separating entries. `peerJoined`/`peerLeft` events are correctly
absent from the log (only `msg` traffic is logged, per spec).

## 5. Stop the server and clean up

```bash
kill %1
rm -rf /tmp/mcp-hub-smoke-logs
rm -f /tmp/mcp-hub-wait-*.sock
```

## Notes for re-running this check in the future

- The throwaway driver lived at `internal/mcptools/smoke_test.go` and used
  `../../bin/mcp-hub-client` as the binary path (relative to that package
  directory) to invoke the real `wait` subcommand — `os.Executable()` cannot
  be used for this inside `go test`, since it resolves to the test binary,
  not `mcp-hub-client`. Recreate a similar file if re-verifying this
  end-to-end path after a significant change to the wait/waiter protocol.
- A background security review during implementation caught two real issues
  worth remembering if this area is touched again: (1) `sessionId`/`peerId`
  must be validated as UUIDs at every point they cross a trust boundary
  (`wire.IsValidID`) — a `peerId` in particular is fully server-controlled
  from the client's perspective and was being used unvalidated both in a
  filesystem path (the wait socket) and rendered directly to the model in
  `peerJoined`/`peerLeft` events; (2) the HTTP server needs
  `ReadHeaderTimeout`/`IdleTimeout` set (not `ReadTimeout`/`WriteTimeout`,
  which would kill long-lived websocket connections) to avoid a Slowloris-style
  resource exhaustion issue.
