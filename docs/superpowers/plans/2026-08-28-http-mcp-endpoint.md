# HTTP-MCP Endpoint on mcp-hub-server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Streamable-HTTP MCP endpoint to `mcp-hub-server` so any HTTP-MCP client can use the hub directly — no local process, no websocket hop to itself — while leaving the existing `/{sessionId}` websocket relay, `mcp-hub-client`'s stdio mode, and its `wait`/`wait --follow` unix-socket mechanism completely unmodified.

**Architecture:** A new `internal/httpmcp` package implements `hubsession.Peer` directly (`httpPeer`) and registers `hub_connect`/`hub_disconnect`/`hub_send`/`hub_receive`/`hub_wait`/`hub_peers` tools on a `server.MCPServer`, mounted via `server.NewStreamableHTTPServer` at a single static `/mcp` path — session identity is a tool parameter, not part of the URL, because MCP client configuration is static. Per-MCP-session state is resolved via `server.ClientSessionFromContext(ctx).SessionID()`. A companion `/watch?token=...` endpoint (plain HTTP, `http.Flusher`-based) taps a connected peer's own event buffer for `curl -N`-based async monitoring — the same shape Claude already uses for `wait --follow` via the Monitor tool, but with no local process required.

**Tech Stack:** Go, `github.com/mark3labs/mcp-go@v0.58.0` (`server.NewStreamableHTTPServer`, `server.WithHooks`, `server.ClientSessionFromContext`), `github.com/google/uuid`, existing `internal/hubsession`, `internal/wire`, `internal/hubconn` (event formatting), `internal/sanitize`, `internal/agekey`.

**Spec:** `docs/superpowers/specs/2026-08-28-http-mcp-endpoint-design.md`

## Global Constraints

- Purely additive: `internal/waiter`, `mcp-hub-client`'s stdio mode, and the existing `/{sessionId}` websocket handler in `internal/wsserver/server.go` must not change behavior.
- Scope is the base tool set only: `hub_connect`, `hub_disconnect`, `hub_send`, `hub_receive`, `hub_wait`, `hub_peers`. No reactions/edit/delete/history — `mcp-hub-server`'s relay has no server-side concept of any of those.
- No new auth mechanism — matches the websocket endpoint's existing (lack of) auth.
- `go build ./... && go vet ./... && go test -race -count=1 ./...` must pass after every task.

---

### Task 1: Shared infra — `Session.Peers()`, `Handler.Manager()`, exported sanitize limits

**Files:**
- Modify: `internal/hubsession/session.go`
- Modify: `internal/hubsession/session_test.go`
- Modify: `internal/wsserver/server.go`
- Modify: `internal/wsserver/server_test.go`

**Interfaces:**
- Produces: `hubsession.Session.Peers() []Peer`, `wsserver.Handler.Manager() *hubsession.Manager`, `wsserver.MaxNameRunes` (int, =64), `wsserver.MaxReconnectSecretRunes` (int, =256).

- [ ] **Step 1: Write the failing test for `Session.Peers()`**

Add to `internal/hubsession/session_test.go`:

```go
func TestPeersReturnsAllCurrentMembers(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "Alice", "", "", nil)
	b := joinFake(s, "Bob", "", "", nil)

	peers := s.Peers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d: %+v", len(peers), peers)
	}
	seen := map[string]bool{}
	for _, p := range peers {
		seen[p.ID()] = true
	}
	if !seen[a.ID()] || !seen[b.ID()] {
		t.Fatalf("expected to see both a and b, got %+v", peers)
	}
}

func TestPeersReflectsLeave(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)
	s.Leave(a)

	if peers := s.Peers(); len(peers) != 0 {
		t.Fatalf("expected no peers after the only one left, got %+v", peers)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/hubsession/... -run 'TestPeersReturnsAllCurrentMembers|TestPeersReflectsLeave' -v`
Expected: FAIL with "s.Peers undefined"

- [ ] **Step 3: Implement `Session.Peers()`**

In `internal/hubsession/session.go`, add after `Leave`:

```go
// Peers returns a snapshot of everyone currently in the session, in no
// particular order.
func (s *Session) Peers() []Peer {
	s.mu.Lock()
	defer s.mu.Unlock()
	peers := make([]Peer, 0, len(s.peers))
	for _, p := range s.peers {
		peers = append(peers, p)
	}
	return peers
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/hubsession/... -v`
Expected: PASS

- [ ] **Step 5: Write the failing test for `Handler.Manager()`**

Add to `internal/wsserver/server_test.go`:

```go
func TestManagerReturnsTheSessionManager(t *testing.T) {
	h := NewHandler()
	if h.Manager() == nil {
		t.Fatal("expected Manager() to return a non-nil *hubsession.Manager")
	}
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `go test ./internal/wsserver/... -run TestManagerReturnsTheSessionManager -v`
Expected: FAIL with "h.Manager undefined"

- [ ] **Step 7: Implement `Handler.Manager()` and export the sanitize limits**

In `internal/wsserver/server.go`, change:

```go
// maxNameRunes bounds a peer's untrusted display name — long enough for any
// reasonable name, short enough to keep it from bloating logs/events.
const maxNameRunes = 64

// maxReconnectSecretRunes bounds a reconnectSecret — generous for any
// reasonable client-generated token, but bounded so a client can't bloat
// Session.secretToPeerID with arbitrarily large values.
const maxReconnectSecretRunes = 256
```

to:

```go
// MaxNameRunes bounds a peer's untrusted display name — long enough for any
// reasonable name, short enough to keep it from bloating logs/events.
// Exported so other in-process peer implementations (see internal/httpmcp)
// apply the exact same limit instead of a second, driftable copy of it.
const MaxNameRunes = 64

// MaxReconnectSecretRunes bounds a reconnectSecret — generous for any
// reasonable client-generated token, but bounded so a client can't bloat
// Session.secretToPeerID with arbitrarily large values. Exported for the
// same reason as MaxNameRunes.
const MaxReconnectSecretRunes = 256
```

Then update the two existing use sites in the same file (`handleUpgrade`) from `maxNameRunes`/`maxReconnectSecretRunes` to `MaxNameRunes`/`MaxReconnectSecretRunes`.

Add after `NewHandler`:

```go
// Manager exposes the session manager so other in-process protocol
// handlers (see internal/httpmcp) can join/interact with the exact same
// hubsession.Session objects this websocket handler uses, instead of a
// second, disconnected set of sessions.
func (h *Handler) Manager() *hubsession.Manager {
	return h.manager
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `go test ./internal/wsserver/... -v`
Expected: PASS

- [ ] **Step 9: Full build/vet/test**

Run: `go build ./... && go vet ./... && go test -race -count=1 ./...`
Expected: all pass

- [ ] **Step 10: Commit**

```bash
git add internal/hubsession/session.go internal/hubsession/session_test.go internal/wsserver/server.go internal/wsserver/server_test.go
git commit -m "feat: expose Session.Peers, Handler.Manager, and shared sanitize limits"
```

---

### Task 2: `httpPeer` — in-process `hubsession.Peer` with buffering and blocking wait

**Files:**
- Create: `internal/httpmcp/peer.go`
- Create: `internal/httpmcp/peer_test.go`
- Modify: `internal/hubconn/conn.go` (export `DecodeEvent`)
- Modify: `internal/hubconn/conn_test.go`

**Interfaces:**
- Consumes: `hubsession.Peer` interface (`ID() string`, `Deliver(event any)`, `Name() string`, `AgePublicKey() string`) from `internal/hubsession/session.go` (Task 1 context, unchanged). `hubconn.Event`, `hubconn.FormatEvents([]Event) string` (existing, `internal/hubconn/format.go`).
- Produces: `hubconn.DecodeEvent(raw []byte) (Event, bool)`. `httpmcp.newHTTPPeer(id, name, agePublicKey string) *httpPeer`, `(*httpPeer) ID/Name/AgePublicKey() string`, `(*httpPeer) Deliver(event any)`, `(*httpPeer) Drain() []hubconn.Event`, `(*httpPeer) Wait(ctx context.Context) ([]hubconn.Event, error)`, `(*httpPeer) watchToken string` field.

- [ ] **Step 1: Write the failing test for `hubconn.DecodeEvent`**

Add to `internal/hubconn/conn_test.go`:

```go
func TestDecodeEventExportedWrapperMatchesInternalDecode(t *testing.T) {
	raw, err := json.Marshal(wire.NewPeerJoined("550e8400-e29b-41d4-a716-446655440000", "Alice", ""))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ev, ok := DecodeEvent(raw)
	if !ok {
		t.Fatal("expected DecodeEvent to succeed")
	}
	if ev.Kind != "peerJoined" || ev.PeerID != "550e8400-e29b-41d4-a716-446655440000" || ev.Name != "Alice" {
		t.Fatalf("unexpected decoded event: %+v", ev)
	}
}
```

Add `"encoding/json"` to the test file's imports if not already present (it is — `decodeEvent` tests elsewhere in this package already use it; if the import is missing, add it).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/hubconn/... -run TestDecodeEventExportedWrapperMatchesInternalDecode -v`
Expected: FAIL with "undefined: DecodeEvent"

- [ ] **Step 3: Export `DecodeEvent`**

In `internal/hubconn/conn.go`, immediately before `func decodeEvent(raw []byte) (Event, bool) {`, add:

```go
// DecodeEvent decodes a single raw wire-protocol JSON event into an Event —
// exported so other in-process code that constructs the same wire.* values
// directly (see internal/httpmcp) can reuse this decoding/normalization
// logic instead of duplicating it, by marshaling the wire value and
// decoding it right back rather than hand-rolling a second conversion path.
func DecodeEvent(raw []byte) (Event, bool) {
	return decodeEvent(raw)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/hubconn/... -v`
Expected: PASS

- [ ] **Step 5: Write the failing tests for `httpPeer`**

Create `internal/httpmcp/peer_test.go`:

```go
package httpmcp

import (
	"context"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/wire"
)

func TestNewHTTPPeerHasIdentityAndWatchToken(t *testing.T) {
	p := newHTTPPeer("550e8400-e29b-41d4-a716-446655440000", "Alice", "")
	if p.ID() != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("unexpected ID: %s", p.ID())
	}
	if p.Name() != "Alice" {
		t.Fatalf("unexpected Name: %s", p.Name())
	}
	if p.watchToken == "" {
		t.Fatal("expected a non-empty watch token")
	}
}

func TestDeliverThenDrainReturnsFormattableEvent(t *testing.T) {
	p := newHTTPPeer("550e8400-e29b-41d4-a716-446655440000", "", "")
	p.Deliver(wire.NewBroadcastMsg("6ba7b810-9dad-11d1-80b4-00c04fd430c8", "hi there", "2026-08-28T10:00:00Z"))

	events := p.Drain()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(events), events)
	}
	if events[0].Kind != "msg" || events[0].Text != "hi there" {
		t.Fatalf("unexpected event: %+v", events[0])
	}

	// Drain empties the buffer.
	if events := p.Drain(); len(events) != 0 {
		t.Fatalf("expected Drain to be empty after a prior Drain, got %+v", events)
	}
}

func TestWaitBlocksUntilDeliverThenReturns(t *testing.T) {
	p := newHTTPPeer("550e8400-e29b-41d4-a716-446655440000", "", "")

	done := make(chan []error, 0)
	_ = done
	resultCh := make(chan struct {
		events []struct{ Kind string }
		err    error
	}, 1)
	_ = resultCh

	type waitResult struct {
		n   int
		err error
	}
	results := make(chan waitResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		events, err := p.Wait(ctx)
		results <- waitResult{n: len(events), err: err}
	}()

	time.Sleep(20 * time.Millisecond) // give Wait time to block first
	p.Deliver(wire.NewBroadcastMsg("6ba7b810-9dad-11d1-80b4-00c04fd430c8", "hi", "2026-08-28T10:00:00Z"))

	select {
	case r := <-results:
		if r.err != nil {
			t.Fatalf("unexpected error: %v", r.err)
		}
		if r.n != 1 {
			t.Fatalf("expected 1 event, got %d", r.n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait never returned after Deliver")
	}
}

func TestWaitReturnsErrorOnContextTimeout(t *testing.T) {
	p := newHTTPPeer("550e8400-e29b-41d4-a716-446655440000", "", "")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := p.Wait(ctx)
	if err == nil {
		t.Fatal("expected an error when the context times out with nothing delivered")
	}
}

func TestWaitReturnsImmediatelyIfAlreadyBuffered(t *testing.T) {
	p := newHTTPPeer("550e8400-e29b-41d4-a716-446655440000", "", "")
	p.Deliver(wire.NewBroadcastMsg("6ba7b810-9dad-11d1-80b4-00c04fd430c8", "hi", "2026-08-28T10:00:00Z"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	events, err := p.Wait(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 already-buffered event, got %d", len(events))
	}
}
```

(Clean up the two unused scratch variables/types accidentally left in step 5's draft — the final file must not contain `done`, `resultCh`, or their `_ =` lines; only `TestWaitBlocksUntilDeliverThenReturns`'s `results`/`waitResult` are used. Write the file without those two dead lines.)

- [ ] **Step 6: Run tests to verify they fail**

Run: `go test ./internal/httpmcp/... -v`
Expected: FAIL — package doesn't build yet (`newHTTPPeer` undefined)

- [ ] **Step 7: Implement `httpPeer`**

Create `internal/httpmcp/peer.go`:

```go
// Package httpmcp implements a Streamable-HTTP MCP endpoint for
// mcp-hub-server, backed directly by internal/hubsession — an in-process
// alternative to mcp-hub-client dialing out over a websocket to itself.
package httpmcp

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/google/uuid"

	"github.com/secforge/mcp-hub/internal/hubconn"
)

// httpPeer implements hubsession.Peer directly, buffering delivered events
// for hub_receive/hub_wait/the /watch endpoint to drain — a lighter,
// in-process sibling of hubconn.Conn's own buffer/Peek/Drain, minus
// websocket/reconnect machinery, since events here are native Go values
// (from Session.Broadcast/DeliverTo/Join) rather than JSON read off a
// socket.
type httpPeer struct {
	id           string
	name         string
	agePublicKey string

	// watchToken identifies this exact peer to the /watch endpoint —
	// unrelated to peerId or sessionId, minted once at connect time and
	// known only to whoever received it from hub_connect's result.
	watchToken string

	mu     sync.Mutex
	buffer []hubconn.Event
	// woken is closed (and immediately replaced) every time Deliver adds to
	// buffer, waking anything blocked in Wait — the same
	// close-and-replace-a-channel pattern used to broadcast "something
	// changed" to any number of waiters without a lost-wakeup race.
	woken chan struct{}
}

func newHTTPPeer(id, name, agePublicKey string) *httpPeer {
	return &httpPeer{
		id:           id,
		name:         name,
		agePublicKey: agePublicKey,
		watchToken:   uuid.NewString(),
		woken:        make(chan struct{}),
	}
}

func (p *httpPeer) ID() string           { return p.id }
func (p *httpPeer) Name() string         { return p.name }
func (p *httpPeer) AgePublicKey() string { return p.agePublicKey }

// Deliver implements hubsession.Peer. event is always one of the wire.*
// values Session.Join/Leave/Broadcast/DeliverTo already construct for the
// websocket path (wire.PeerEvent, wire.RosterComplete, wire.Msg,
// wire.Error) — marshaling it back to JSON and decoding that via
// hubconn.DecodeEvent reuses that package's already-tested
// decode/normalize logic instead of a second, hand-rolled conversion.
func (p *httpPeer) Deliver(event any) {
	raw, err := json.Marshal(event)
	if err != nil {
		return
	}
	ev, ok := hubconn.DecodeEvent(raw)
	if !ok {
		return
	}
	p.mu.Lock()
	p.buffer = append(p.buffer, ev)
	close(p.woken)
	p.woken = make(chan struct{})
	p.mu.Unlock()
}

// Drain returns and clears everything buffered so far, without blocking.
func (p *httpPeer) Drain() []hubconn.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	events := p.buffer
	p.buffer = nil
	return events
}

// Wait blocks until at least one event is buffered or ctx is done, then
// drains and returns everything buffered. Returns a non-nil error (ctx's
// own error) on cancellation/timeout with nothing delivered.
func (p *httpPeer) Wait(ctx context.Context) ([]hubconn.Event, error) {
	for {
		p.mu.Lock()
		if len(p.buffer) > 0 {
			events := p.buffer
			p.buffer = nil
			p.mu.Unlock()
			return events, nil
		}
		woken := p.woken
		p.mu.Unlock()

		select {
		case <-woken:
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `go test ./internal/httpmcp/... -race -v`
Expected: PASS

- [ ] **Step 9: Full build/vet/test**

Run: `go build ./... && go vet ./... && go test -race -count=1 ./...`
Expected: all pass

- [ ] **Step 10: Commit**

```bash
git add internal/hubconn/conn.go internal/hubconn/conn_test.go internal/httpmcp/peer.go internal/httpmcp/peer_test.go
git commit -m "feat: add httpPeer, an in-process hubsession.Peer for the HTTP-MCP endpoint"
```

---

### Task 3: `httpHub` per-MCP-session state and connect/disconnect lifecycle

**Files:**
- Create: `internal/httpmcp/hub.go`
- Create: `internal/httpmcp/hub_test.go`

**Interfaces:**
- Consumes: `httpPeer`/`newHTTPPeer` (Task 2, same package). `hubsession.Manager.GetOrCreate(id string) *hubsession.Session`, `hubsession.Manager.Remove(id string)`, `hubsession.Session.Join(reconnectSecret string, makePeer func(string) hubsession.Peer, beforeVisible func(int)) (hubsession.Peer, bool)`, `hubsession.Session.Leave(p hubsession.Peer) bool` (existing, `internal/hubsession/session.go`). `wire.IsValidID(s string) bool` (existing, `internal/wire/wire.go`). `sanitize.Text(s string, maxRunes int) string` (existing, `internal/sanitize/sanitize.go`). `agekey.Valid(s string) bool` (existing, `internal/agekey/agekey.go`). `wsserver.MaxNameRunes`, `wsserver.MaxReconnectSecretRunes` (Task 1).
- Produces: `httpmcp.NewServer(manager *hubsession.Manager) *Server`, `(*Server) connect(mcpSessionID, sessionID, name, agePublicKey, reconnectSecret string) (peerID, joinedSessionID, watchToken string, err error)`, `(*Server) disconnect(mcpSessionID string)`, `(*Server) hubFor(mcpSessionID string) *httpHub` (returns the existing or a freshly created empty one — never nil), `(*httpHub) peer() *httpPeer` (nil if not connected), `(*httpHub) session() *hubsession.Session`.

- [ ] **Step 1: Write the failing tests**

Create `internal/httpmcp/hub_test.go`:

```go
package httpmcp

import (
	"testing"

	"github.com/secforge/mcp-hub/internal/hubsession"
)

func TestConnectWithoutSessionIDCreatesOne(t *testing.T) {
	s := NewServer(hubsession.NewManager())

	peerID, sessionID, watchToken, err := s.connect("mcp-session-1", "", "Alice", "", "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if peerID == "" || sessionID == "" || watchToken == "" {
		t.Fatalf("expected non-empty peerID/sessionID/watchToken, got %q/%q/%q", peerID, sessionID, watchToken)
	}
}

func TestConnectWithExplicitSessionIDJoinsIt(t *testing.T) {
	s := NewServer(hubsession.NewManager())

	_, sessionID, _, err := s.connect("mcp-session-1", "", "Alice", "", "")
	if err != nil {
		t.Fatalf("connect (creator): %v", err)
	}

	peerID2, sessionID2, _, err := s.connect("mcp-session-2", sessionID, "Bob", "", "")
	if err != nil {
		t.Fatalf("connect (joiner): %v", err)
	}
	if sessionID2 != sessionID {
		t.Fatalf("expected joiner to join the same session %q, got %q", sessionID, sessionID2)
	}

	hub := s.hubFor("mcp-session-1")
	peers := hub.session().Peers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers in the shared session, got %d", len(peers))
	}
	found := false
	for _, p := range peers {
		if p.ID() == peerID2 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected the shared session's roster to include the joiner")
	}
}

func TestConnectRejectsInvalidSessionID(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	if _, _, _, err := s.connect("mcp-session-1", "not-a-valid-uuid", "", "", ""); err == nil {
		t.Fatal("expected an error for an invalid sessionId")
	}
}

func TestConnectRejectsInvalidAgePublicKey(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	if _, _, _, err := s.connect("mcp-session-1", "", "", "not-a-valid-key", ""); err == nil {
		t.Fatal("expected an error for an invalid agePublicKey")
	}
}

func TestConnectTwiceOnSameMCPSessionErrors(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	if _, _, _, err := s.connect("mcp-session-1", "", "", "", ""); err != nil {
		t.Fatalf("first connect: %v", err)
	}
	if _, _, _, err := s.connect("mcp-session-1", "", "", "", ""); err == nil {
		t.Fatal("expected an error connecting twice without disconnecting first")
	}
}

func TestDisconnectClearsPeerAndRemovesEmptySession(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	_, sessionID, _, err := s.connect("mcp-session-1", "", "", "", "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	s.disconnect("mcp-session-1")

	if hub := s.hubFor("mcp-session-1"); hub.peer() != nil {
		t.Fatal("expected peer to be cleared after disconnect")
	}
	// Reconnecting to the same sessionId should find it empty (a fresh
	// session), proving the old one was torn down rather than left with a
	// phantom member.
	if _, _, _, err := s.connect("mcp-session-1", sessionID, "", "", ""); err != nil {
		t.Fatalf("reconnect after disconnect: %v", err)
	}
	if peers := s.hubFor("mcp-session-1").session().Peers(); len(peers) != 1 {
		t.Fatalf("expected exactly the reconnecting peer, got %d peers", len(peers))
	}
}

func TestDisconnectWithoutConnectIsANoop(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	s.disconnect("mcp-session-1") // must not panic
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/httpmcp/... -run 'TestConnect|TestDisconnect' -v`
Expected: FAIL — `NewServer` undefined

- [ ] **Step 3: Implement `httpHub` and `Server`**

Create `internal/httpmcp/hub.go`:

```go
package httpmcp

import (
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/secforge/mcp-hub/internal/agekey"
	"github.com/secforge/mcp-hub/internal/hubsession"
	"github.com/secforge/mcp-hub/internal/sanitize"
	"github.com/secforge/mcp-hub/internal/wire"
	"github.com/secforge/mcp-hub/internal/wsserver"
)

// httpHub is the per-MCP-session state: at most one hub session membership
// at a time, mirroring how mcp-hub-client's single-process Hub wraps a
// nilable *hubconn.Conn — here scoped to one MCP client session instead of
// one OS process, since one mcp-hub-server process serves many concurrent
// MCP sessions.
type httpHub struct {
	mu   sync.Mutex
	p    *httpPeer
	sess *hubsession.Session
}

func (h *httpHub) peer() *httpPeer {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.p
}

func (h *httpHub) session() *hubsession.Session {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sess
}

// Server holds all per-MCP-session httpHubs and the watch-token index used
// by the /watch endpoint, and implements hub_connect/hub_disconnect's
// underlying logic (the MCP tool handlers themselves are added in a later
// task and just call these methods).
type Server struct {
	manager *hubsession.Manager

	hubs sync.Map // mcpSessionID string -> *httpHub

	tokens sync.Map // watchToken string -> *httpPeer
}

// NewServer wires a Server to manager — pass wsserver's own
// (*wsserver.Handler).Manager() so HTTP-MCP peers join the exact same
// hubsession.Session objects the websocket endpoint uses, not a second,
// disconnected set of sessions.
func NewServer(manager *hubsession.Manager) *Server {
	return &Server{manager: manager}
}

// hubFor returns mcpSessionID's httpHub, creating an empty one on first
// use. Never nil.
func (s *Server) hubFor(mcpSessionID string) *httpHub {
	v, _ := s.hubs.LoadOrStore(mcpSessionID, &httpHub{})
	return v.(*httpHub)
}

// connect implements hub_connect: joins sessionID (or a freshly generated
// one, if empty) as a new peer, on behalf of mcpSessionID. Errors if
// mcpSessionID is already connected (call disconnect first), or if
// sessionID/agePublicKey are malformed.
func (s *Server) connect(mcpSessionID, sessionID, name, agePublicKey, reconnectSecret string) (peerID, joinedSessionID, watchToken string, err error) {
	hub := s.hubFor(mcpSessionID)

	hub.mu.Lock()
	alreadyConnected := hub.p != nil
	hub.mu.Unlock()
	if alreadyConnected {
		return "", "", "", fmt.Errorf("already connected — call hub_disconnect first")
	}

	if sessionID == "" {
		sessionID = uuid.NewString()
	} else if !wire.IsValidID(sessionID) {
		return "", "", "", fmt.Errorf("sessionId is not a valid session id")
	}
	if agePublicKey != "" && !agekey.Valid(agePublicKey) {
		return "", "", "", fmt.Errorf("agePublicKey is not a validly formatted age public key")
	}
	name = sanitize.Text(name, wsserver.MaxNameRunes)
	if len(reconnectSecret) > wsserver.MaxReconnectSecretRunes {
		return "", "", "", fmt.Errorf("reconnectSecret too long")
	}

	hubSession := s.manager.GetOrCreate(sessionID)
	var peer *httpPeer
	hubSession.Join(reconnectSecret, func(id string) hubsession.Peer {
		peer = newHTTPPeer(id, name, agePublicKey)
		return peer
	}, nil)

	s.tokens.Store(peer.watchToken, peer)

	hub.mu.Lock()
	hub.p = peer
	hub.sess = hubSession
	hub.mu.Unlock()

	return peer.ID(), sessionID, peer.watchToken, nil
}

// disconnect implements hub_disconnect, and is also what OnUnregisterSession
// calls (a later task) when an MCP session ends without an explicit
// hub_disconnect. A no-op if mcpSessionID was never connected.
func (s *Server) disconnect(mcpSessionID string) {
	hub := s.hubFor(mcpSessionID)

	hub.mu.Lock()
	peer, hubSession := hub.p, hub.sess
	hub.p, hub.sess = nil, nil
	hub.mu.Unlock()

	if peer == nil {
		return
	}
	s.tokens.Delete(peer.watchToken)
	if hubSession.Leave(peer) {
		s.manager.Remove(hubSession.ID())
	}
}
```

This introduces one new requirement on `hubsession.Session`: an `ID() string` accessor (needed so `disconnect` can call `Manager.Remove` with the right id without the caller having to track it separately). Add it now:

In `internal/hubsession/session.go`, add after `Peers`:

```go
// ID returns this session's id, as passed to Manager.GetOrCreate.
func (s *Session) ID() string {
	return s.id
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/httpmcp/... ./internal/hubsession/... -race -v`
Expected: PASS

- [ ] **Step 5: Full build/vet/test**

Run: `go build ./... && go vet ./... && go test -race -count=1 ./...`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add internal/httpmcp/hub.go internal/httpmcp/hub_test.go internal/hubsession/session.go
git commit -m "feat: add httpHub connect/disconnect lifecycle for the HTTP-MCP endpoint"
```

---

### Task 4: MCP tool handlers — `hub_connect`, `hub_disconnect`, `hub_send`, `hub_receive`, `hub_wait`, `hub_peers`

**Files:**
- Create: `internal/httpmcp/tools.go`
- Create: `internal/httpmcp/tools_test.go`

**Interfaces:**
- Consumes: `Server.connect`/`disconnect`/`hubFor` (Task 3). `httpPeer.Drain()`/`Wait(ctx)` (Task 2). `hubsession.Session.Broadcast(from hubsession.Peer, event any)`, `DeliverTo(from hubsession.Peer, targetID string, event any) error`, `Peers() []hubsession.Peer` (existing). `wire.NewBroadcastMsg`/`NewDirectedMsg(peerID, text, ts string) wire.Msg` (existing). `hubconn.FormatEvents([]hubconn.Event) string` (existing). `server.ClientSessionFromContext(ctx) server.ClientSession` (mcp-go).
- Produces: `(*Server) Register(mcpServer *server.MCPServer)` — registers all six tools.

- [ ] **Step 1: Write the failing tests**

Create `internal/httpmcp/tools_test.go`:

```go
package httpmcp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/hubsession"
)

// fakeSession is a minimal server.ClientSession for tests that need to put
// a specific MCP session id on the context without a real transport.
type fakeSession struct {
	id string
}

func (f *fakeSession) SessionID() string                                  { return f.id }
func (f *fakeSession) NotificationChannel() chan<- mcp.JSONRPCNotification { return make(chan mcp.JSONRPCNotification, 1) }
func (f *fakeSession) Initialize()                                        {}
func (f *fakeSession) Initialized() bool                                  { return true }

func newTestServer(t *testing.T) (*Server, *server.MCPServer) {
	t.Helper()
	s := NewServer(hubsession.NewManager())
	mcpServer := server.NewMCPServer("test", "0.0.0", server.WithToolCapabilities(false))
	s.Register(mcpServer)
	return s, mcpServer
}

func ctxFor(mcpServer *server.MCPServer, mcpSessionID string) context.Context {
	return mcpServer.WithContext(context.Background(), &fakeSession{id: mcpSessionID})
}

// callTool invokes one of Server's tool handler methods directly (they are
// ordinary func(ctx, mcp.CallToolRequest) (*mcp.CallToolResult, error)
// values) — no need to round-trip through JSON-RPC/HandleMessage for a
// unit test.
func callTool(t *testing.T, ctx context.Context, s *Server, handler func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error), args map[string]any) string {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = args
	result, err := handler(ctx, req)
	if err != nil {
		t.Fatalf("handler returned an error (not a tool-error result): %v", err)
	}
	if len(result.Content) != 1 {
		t.Fatalf("expected exactly 1 content block, got %d: %+v", len(result.Content), result.Content)
	}
	tc, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("expected text content, got %T", result.Content[0])
	}
	return tc.Text
}

func TestConnectSendReceiveRoundTrip(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctxA := ctxFor(mcpServer, "mcp-a")
	ctxB := ctxFor(mcpServer, "mcp-b")

	connectA := callTool(t, ctxA, s, s.handleConnect, map[string]any{"name": "Alice"})
	if !strings.Contains(connectA, "sessionId=") {
		t.Fatalf("expected connect result to include a sessionId, got %q", connectA)
	}
	sessionID := s.hubFor("mcp-a").session().ID()

	callTool(t, ctxB, s, s.handleConnect, map[string]any{"sessionId": sessionID, "name": "Bob"})

	callTool(t, ctxA, s, s.handleSend, map[string]any{"text": "hello from Alice"})

	// Give Deliver's goroutine-free synchronous call a moment to land (it's
	// synchronous, but hub_receive below still shouldn't need to retry).
	time.Sleep(10 * time.Millisecond)
	received := callTool(t, ctxB, s, s.handleReceive, map[string]any{})
	if !strings.Contains(received, "hello from Alice") {
		t.Fatalf("expected Bob to receive Alice's message, got %q", received)
	}

	// Sender never gets its own broadcast back.
	selfReceived := callTool(t, ctxA, s, s.handleReceive, map[string]any{})
	if strings.Contains(selfReceived, "hello from Alice") {
		t.Fatalf("sender should not receive its own broadcast, got %q", selfReceived)
	}
}

func TestHubPeersListsOthersNotSelf(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctxA := ctxFor(mcpServer, "mcp-a")
	ctxB := ctxFor(mcpServer, "mcp-b")

	callTool(t, ctxA, s, s.handleConnect, map[string]any{"name": "Alice"})
	sessionID := s.hubFor("mcp-a").session().ID()
	callTool(t, ctxB, s, s.handleConnect, map[string]any{"sessionId": sessionID, "name": "Bob"})

	peers := callTool(t, ctxA, s, s.handlePeers, map[string]any{})
	if !strings.Contains(peers, "Bob") {
		t.Fatalf("expected Alice's hub_peers to mention Bob, got %q", peers)
	}
	if strings.Contains(peers, "Alice") {
		t.Fatalf("expected hub_peers to exclude the caller itself, got %q", peers)
	}
}

func TestHubWaitReturnsDeliveredEvent(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctxA := ctxFor(mcpServer, "mcp-a")
	ctxB := ctxFor(mcpServer, "mcp-b")

	callTool(t, ctxA, s, s.handleConnect, map[string]any{})
	sessionID := s.hubFor("mcp-a").session().ID()
	callTool(t, ctxB, s, s.handleConnect, map[string]any{"sessionId": sessionID})

	go func() {
		time.Sleep(30 * time.Millisecond)
		callTool(t, ctxB, s, s.handleSend, map[string]any{"text": "async hello"})
	}()

	waited := callTool(t, ctxA, s, s.handleWait, map[string]any{"timeoutSeconds": float64(2)})
	if !strings.Contains(waited, "async hello") {
		t.Fatalf("expected hub_wait to return the delivered message, got %q", waited)
	}
}

func TestHubWaitTimesOutWithNoEvents(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctxA := ctxFor(mcpServer, "mcp-a")
	callTool(t, ctxA, s, s.handleConnect, map[string]any{})

	waited := callTool(t, ctxA, s, s.handleWait, map[string]any{"timeoutSeconds": float64(0.05)})
	if !strings.Contains(waited, "no new events") {
		t.Fatalf("expected a no-new-events result, got %q", waited)
	}
}

func TestToolsErrorWhenNotConnected(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctx := ctxFor(mcpServer, "mcp-a")

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"text": "hi"}
	result, err := s.handleSend(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected hub_send to report an error result when not connected")
	}
}

func TestDisconnectThenReconnectWorks(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctx := ctxFor(mcpServer, "mcp-a")

	callTool(t, ctx, s, s.handleConnect, map[string]any{})
	callTool(t, ctx, s, s.handleDisconnect, map[string]any{})
	callTool(t, ctx, s, s.handleConnect, map[string]any{}) // must not error "already connected"
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/httpmcp/... -v`
Expected: FAIL — `s.Register`/`s.handleConnect` etc. undefined

- [ ] **Step 3: Implement the tool handlers**

Create `internal/httpmcp/tools.go`:

```go
package httpmcp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/wire"
)

// mcpSessionID resolves the calling MCP client session's id from ctx, as
// set by mcp-go for every tool call. Every handler below calls this first.
func mcpSessionID(ctx context.Context) (string, error) {
	session := server.ClientSessionFromContext(ctx)
	if session == nil {
		return "", fmt.Errorf("no active MCP session")
	}
	return session.SessionID(), nil
}

// Register adds hub_connect, hub_disconnect, hub_send, hub_receive,
// hub_wait, and hub_peers to mcpServer.
func (s *Server) Register(mcpServer *server.MCPServer) {
	mcpServer.AddTool(
		mcp.NewTool("hub_connect",
			mcp.WithDescription("Join a hub session over this HTTP-MCP connection. Omit sessionId "+
				"to create a brand new session (its id is returned, to share with whoever else "+
				"should join); pass an existing one to join it. The result also includes a "+
				"watchToken — pass it to GET /watch?token=<token>&follow=1 (e.g. via curl -N) to "+
				"observe events asynchronously instead of blocking on hub_wait."),
			mcp.WithString("sessionId", mcp.Description("An existing session id to join; omit to create a new session")),
			mcp.WithString("name", mcp.Description("Optional display name shown to other peers")),
			mcp.WithString("agePublicKey", mcp.Description("Optional age recipient public key, shared with other peers")),
			mcp.WithString("reconnectSecret", mcp.Description("Optional secret that reclaims this same peer identity on a future reconnect to the same session")),
		),
		s.handleConnect,
	)
	mcpServer.AddTool(
		mcp.NewTool("hub_disconnect",
			mcp.WithDescription("Leave the current hub session, if connected")),
		s.handleDisconnect,
	)
	mcpServer.AddTool(
		mcp.NewTool("hub_send",
			mcp.WithDescription("Send a text message to the current hub session"),
			mcp.WithString("text", mcp.Required(), mcp.Description("Message text")),
			mcp.WithString("to", mcp.Description("Optional peerId to send this privately to a single peer instead of broadcasting to everyone in the session")),
		),
		s.handleSend,
	)
	mcpServer.AddTool(
		mcp.NewTool("hub_receive",
			mcp.WithDescription("Drain any events that have arrived since the last hub_receive/hub_wait, without blocking")),
		s.handleReceive,
	)
	mcpServer.AddTool(
		mcp.NewTool("hub_wait",
			mcp.WithDescription("Block until at least one new event arrives, or timeoutSeconds elapses"),
			mcp.WithNumber("timeoutSeconds", mcp.Description("How long to wait before giving up; default 30")),
		),
		s.handleWait,
	)
	mcpServer.AddTool(
		mcp.NewTool("hub_peers",
			mcp.WithDescription("List everyone else currently in the hub session")),
		s.handlePeers,
	)
}

func (s *Server) handleConnect(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := mcpSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	peerID, sessionID, watchToken, err := s.connect(
		id,
		req.GetString("sessionId", ""),
		req.GetString("name", ""),
		req.GetString("agePublicKey", ""),
		req.GetString("reconnectSecret", ""),
	)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf(
		"connected — peerId=%s sessionId=%s watchToken=%s\n"+
			"Use watchToken with GET /watch?token=%s&follow=1 (e.g. curl -N) to observe events "+
			"asynchronously instead of blocking on hub_wait.",
		peerID, sessionID, watchToken, watchToken,
	)), nil
}

func (s *Server) handleDisconnect(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := mcpSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	s.disconnect(id)
	return mcp.NewToolResultText("disconnected"), nil
}

func (s *Server) handleSend(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := mcpSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	hub := s.hubFor(id)
	peer, hubSession := hub.peer(), hub.session()
	if peer == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	text, err := req.RequireString("text")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	ts := time.Now().UTC().Format(time.RFC3339)
	if to := req.GetString("to", ""); to != "" {
		if err := hubSession.DeliverTo(peer, to, wire.NewDirectedMsg(peer.ID(), text, ts)); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText("sent"), nil
	}
	hubSession.Broadcast(peer, wire.NewBroadcastMsg(peer.ID(), text, ts))
	return mcp.NewToolResultText("sent"), nil
}

func (s *Server) handleReceive(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := mcpSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	peer := s.hubFor(id).peer()
	if peer == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	events := peer.Drain()
	if len(events) == 0 {
		return mcp.NewToolResultText("no new events"), nil
	}
	return mcp.NewToolResultText(hubconn.FormatEvents(events)), nil
}

func (s *Server) handleWait(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := mcpSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	peer := s.hubFor(id).peer()
	if peer == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	timeoutSeconds := req.GetFloat("timeoutSeconds", 30)
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds*float64(time.Second)))
	defer cancel()
	events, err := peer.Wait(waitCtx)
	if err != nil {
		return mcp.NewToolResultText("no new events"), nil
	}
	return mcp.NewToolResultText(hubconn.FormatEvents(events)), nil
}

func (s *Server) handlePeers(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := mcpSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	hub := s.hubFor(id)
	peer, hubSession := hub.peer(), hub.session()
	if peer == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	var b strings.Builder
	for _, p := range hubSession.Peers() {
		if p.ID() == peer.ID() {
			continue
		}
		fmt.Fprintf(&b, "peer %s", p.ID())
		if p.Name() != "" {
			fmt.Fprintf(&b, " (%q)", p.Name())
		}
		if p.AgePublicKey() != "" {
			fmt.Fprintf(&b, " agePublicKey=%s", p.AgePublicKey())
		}
		b.WriteString("\n")
	}
	if b.Len() == 0 {
		return mcp.NewToolResultText("no other peers"), nil
	}
	return mcp.NewToolResultText(b.String()), nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/httpmcp/... -race -v`
Expected: PASS

- [ ] **Step 5: Full build/vet/test**

Run: `go build ./... && go vet ./... && go test -race -count=1 ./...`
Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add internal/httpmcp/tools.go internal/httpmcp/tools_test.go
git commit -m "feat: add hub_connect/disconnect/send/receive/wait/peers MCP tools for HTTP-MCP"
```

---

### Task 5: Session cleanup hook + `/watch` streaming endpoint

**Files:**
- Create: `internal/httpmcp/watch.go`
- Create: `internal/httpmcp/watch_test.go`
- Modify: `internal/httpmcp/hub.go` (add `Hooks()`)
- Modify: `internal/httpmcp/hub_test.go`

**Interfaces:**
- Consumes: `Server.disconnect` (Task 3), `httpPeer.Wait(ctx)` (Task 2), `hubconn.FormatEvent(hubconn.Event) string` (existing), `server.Hooks`/`AddOnUnregisterSession` (mcp-go).
- Produces: `(*Server) Hooks() *server.Hooks`, `(*Server) WatchHandler() http.HandlerFunc`.

- [ ] **Step 1: Write the failing test for the unregister hook**

Add to `internal/httpmcp/hub_test.go`:

```go
func TestHooksUnregisterSessionDisconnectsThePeer(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	_, _, _, err := s.connect("mcp-session-1", "", "", "", "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	hooks := s.Hooks()
	for _, hook := range hooks.OnUnregisterSession {
		hook(context.Background(), &fakeSession{id: "mcp-session-1"})
	}

	if s.hubFor("mcp-session-1").peer() != nil {
		t.Fatal("expected the peer to be cleared once the MCP session unregisters")
	}
}
```

Add `"context"` to this file's imports. `fakeSession` is defined in `tools_test.go` (same package) — no need to redefine it.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/httpmcp/... -run TestHooksUnregisterSessionDisconnectsThePeer -v`
Expected: FAIL — `s.Hooks` undefined

- [ ] **Step 3: Implement `Hooks()`**

In `internal/httpmcp/hub.go`, add the import `"github.com/mark3labs/mcp-go/server"` and, after `disconnect`:

```go
// Hooks returns the server.Hooks that must be installed on the MCPServer
// wrapping this Server (via server.WithHooks) so a client disconnecting —
// without ever calling hub_disconnect itself — still leaves its hub
// session cleanly instead of leaking a phantom peer forever.
func (s *Server) Hooks() *server.Hooks {
	hooks := &server.Hooks{}
	hooks.AddOnUnregisterSession(func(ctx context.Context, session server.ClientSession) {
		s.disconnect(session.SessionID())
	})
	return hooks
}
```

Add `"context"` to `hub.go`'s imports.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/httpmcp/... -v`
Expected: PASS

- [ ] **Step 5: Write the failing tests for `/watch`**

Create `internal/httpmcp/watch_test.go`:

```go
package httpmcp

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/hubsession"
)

func TestWatchUnknownTokenReturns404(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	srv := httptest.NewServer(s.WatchHandler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "?token=does-not-exist")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestWatchSingleShotReturnsOnePendingEventThenCloses(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	_, sessionID, watchToken, err := s.connect("mcp-a", "", "", "", "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, _, _, err := s.connect("mcp-b", sessionID, "", "", ""); err != nil {
		t.Fatalf("connect (b): %v", err)
	}
	callTool(t, ctxForSession("mcp-b"), s, s.handleSend, map[string]any{"text": "hi from b"})

	srv := httptest.NewServer(s.WatchHandler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "?token=" + watchToken)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body := make([]byte, 4096)
	n, _ := resp.Body.Read(body)
	if !strings.Contains(string(body[:n]), "hi from b") {
		t.Fatalf("expected the watch stream to contain the pending message, got %q", string(body[:n]))
	}
}

func TestWatchFollowStreamsLiveEvents(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	_, sessionID, watchToken, err := s.connect("mcp-a", "", "", "", "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, _, _, err := s.connect("mcp-b", sessionID, "", "", ""); err != nil {
		t.Fatalf("connect (b): %v", err)
	}

	srv := httptest.NewServer(s.WatchHandler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"?token="+watchToken+"&follow=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	go func() {
		time.Sleep(30 * time.Millisecond)
		callTool(t, ctxForSession("mcp-b"), s, s.handleSend, map[string]any{"text": "live event"})
	}()

	scanner := bufio.NewScanner(resp.Body)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && scanner.Scan() {
		if strings.Contains(scanner.Text(), "live event") {
			return
		}
	}
	t.Fatal("expected to see the live event on the follow stream")
}
```

`ctxForSession` is a small helper this file needs that `tools_test.go`'s `ctxFor` can't provide directly (that one needs an `*server.MCPServer`, which these watch tests don't otherwise construct). Add to `internal/httpmcp/tools_test.go` (same package as `ctxFor`, so both live together):

```go
// ctxForSession builds a context carrying mcpSessionID as the active MCP
// client session, using a throwaway MCPServer purely for its WithContext
// helper (server.ClientSessionFromContext's storage key is unexported, so
// this is the only way to construct such a context from outside the
// mcp-go/server package).
func ctxForSession(mcpSessionID string) context.Context {
	throwaway := server.NewMCPServer("test", "0.0.0")
	return throwaway.WithContext(context.Background(), &fakeSession{id: mcpSessionID})
}
```

(`"github.com/mark3labs/mcp-go/server"` is already imported in `tools_test.go`.)

- [ ] **Step 6: Run tests to verify they fail**

Run: `go test ./internal/httpmcp/... -v`
Expected: FAIL — `s.WatchHandler` undefined

- [ ] **Step 7: Implement `/watch`**

Create `internal/httpmcp/watch.go`:

```go
package httpmcp

import (
	"fmt"
	"net/http"

	"github.com/secforge/mcp-hub/internal/hubconn"
)

// WatchHandler serves GET /watch?token=<watchToken>[&follow=1] — a plain
// (non-MCP) http.Flusher-based text stream of one connected peer's own
// events, read-only: it neither joins a new peer nor consumes anything
// from the hub-protocol side. It exists because a blocking hub_wait tool
// call ties up a conversation turn; this lets a client (in practice,
// Claude backgrounding `curl -N` and watching it via the Monitor tool) get
// notified asynchronously instead, the same shape mcp-hub-client's own
// `wait --follow` already provides for its local unix-socket mechanism.
//
// follow=1 keeps streaming until the request's context is canceled (the
// client disconnects) or the peer is torn down (hub_disconnect, or its MCP
// session ending); omitted, it returns after the first batch of events and
// closes.
func (s *Server) WatchHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Query().Get("token")
		v, ok := s.tokens.Load(token)
		if !ok {
			http.Error(w, "unknown or expired watch token", http.StatusNotFound)
			return
		}
		peer := v.(*httpPeer)

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}
		follow := r.URL.Query().Get("follow") == "1"

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		ctx := r.Context()
		for {
			events, err := peer.Wait(ctx)
			if err != nil {
				return
			}
			for _, ev := range events {
				fmt.Fprintln(w, hubconn.FormatEvent(ev))
				fmt.Fprintln(w)
			}
			flusher.Flush()
			if !follow {
				return
			}
		}
	}
}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `go test ./internal/httpmcp/... -race -v`
Expected: PASS

- [ ] **Step 9: Full build/vet/test**

Run: `go build ./... && go vet ./... && go test -race -count=1 ./...`
Expected: all pass

- [ ] **Step 10: Commit**

```bash
git add internal/httpmcp/hub.go internal/httpmcp/hub_test.go internal/httpmcp/watch.go internal/httpmcp/watch_test.go internal/httpmcp/tools_test.go
git commit -m "feat: add /watch streaming endpoint and MCP session cleanup hook"
```

---

### Task 6: Wire into `cmd/mcp-hub-server`

**Files:**
- Modify: `cmd/mcp-hub-server/main.go`
- Create: `cmd/mcp-hub-server/main_test.go` (if it doesn't already cover this; check first — if a `main_test.go` already exists here, extend it instead of creating a second one)

**Interfaces:**
- Consumes: `wsserver.NewHandler() *wsserver.Handler`, `(*wsserver.Handler) Manager() *hubsession.Manager` (Task 1), `httpmcp.NewServer(manager) *Server`, `(*Server) Register(mcpServer *server.MCPServer)`, `(*Server) Hooks() *server.Hooks`, `(*Server) WatchHandler() http.HandlerFunc` (Tasks 3-5), `server.NewMCPServer`, `server.NewStreamableHTTPServer`, `server.WithHooks` (mcp-go).

- [ ] **Step 1: Check for an existing test file**

Run: `ls cmd/mcp-hub-server/*_test.go`

If one exists, read it before writing Step 2's test so the new test follows its existing conventions (helper names, port-0 pattern, etc.) instead of introducing a second, inconsistent style.

- [ ] **Step 2: Write the failing integration test**

Add (to the existing test file, or a new `cmd/mcp-hub-server/main_test.go` if none exists) — this exercises the actual mux wiring end to end, standing in for `main()` since `main()` itself isn't unit-testable:

```go
package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBuildMuxServesWebsocketMCPAndWatchRoutes(t *testing.T) {
	mux := buildMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// /watch with no token: proves the route exists and reaches httpmcp's
	// handler, not a 404 from the mux itself having no such route at all.
	resp, err := http.Get(srv.URL + "/watch")
	if err != nil {
		t.Fatalf("GET /watch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected /watch with no token to 404, got %d", resp.StatusCode)
	}

	// /mcp: proves the route exists and reaches the StreamableHTTPServer
	// (a GET with no session is expected to be rejected by mcp-go itself,
	// not 404 from the mux having no such route).
	resp2, err := http.Get(srv.URL + "/mcp")
	if err != nil {
		t.Fatalf("GET /mcp: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode == http.StatusNotFound {
		t.Fatal("expected /mcp to be routed to the StreamableHTTPServer, got 404")
	}

	// /{sessionId}: proves the pre-existing websocket route is still
	// reachable and unaffected — a plain GET (no upgrade) to a
	// syntactically valid sessionId still reaches wsserver's own handler,
	// which will reject it for lacking a websocket upgrade, not 404 the
	// route itself.
	resp3, err := http.Get(srv.URL + "/550e8400-e29b-41d4-a716-446655440000")
	if err != nil {
		t.Fatalf("GET /{sessionId}: %v", err)
	}
	resp3.Body.Close()
	if resp3.StatusCode == http.StatusNotFound {
		t.Fatal("expected the websocket sessionId route to still be reachable, got 404")
	}
}
```

Do not import `"strings"` in this file — nothing in the test above uses it.

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./cmd/mcp-hub-server/... -v`
Expected: FAIL — `buildMux` undefined

- [ ] **Step 4: Refactor `main.go` to expose `buildMux` and wire in the new routes**

Replace `cmd/mcp-hub-server/main.go` entirely with:

```go
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/httpmcp"
	"github.com/secforge/mcp-hub/internal/wsserver"
)

// buildMux wires the existing websocket relay together with the new
// HTTP-MCP endpoint and its companion watch stream, all sharing one
// hubsession.Manager (via wsHandler.Manager()) so a websocket peer and an
// HTTP-MCP peer can join the very same hub session. /mcp and /watch are
// exact, static routes; everything else (in particular "/{sessionId}")
// falls through to wsHandler's own internal mux, unmodified.
func buildMux() *http.ServeMux {
	wsHandler := wsserver.NewHandler()

	mcpSrv := httpmcp.NewServer(wsHandler.Manager())
	mcpServer := server.NewMCPServer("mcp-hub-server", "0.1.0",
		server.WithToolCapabilities(false),
		server.WithHooks(mcpSrv.Hooks()),
	)
	mcpSrv.Register(mcpServer)
	streamable := server.NewStreamableHTTPServer(mcpServer)

	mux := http.NewServeMux()
	mux.Handle("/mcp", streamable)
	mux.HandleFunc("/watch", mcpSrv.WatchHandler())
	mux.Handle("/", wsHandler)
	return mux
}

func main() {
	addr := flag.String("addr", ":8765", "listen address")
	certFile := flag.String("tls-cert", "", "TLS certificate file (optional)")
	keyFile := flag.String("tls-key", "", "TLS key file (optional)")
	flag.Parse()

	if (*certFile == "") != (*keyFile == "") {
		log.Fatal("-tls-cert and -tls-key must both be set, or both left empty")
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           buildMux(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	if useTLS(*certFile, *keyFile) {
		log.Printf("mcp-hub-server listening on %s (tls)", *addr)
		log.Fatal(srv.ListenAndServeTLS(*certFile, *keyFile))
	} else {
		log.Printf("mcp-hub-server listening on %s", *addr)
		log.Fatal(srv.ListenAndServe())
	}
}
```

Check whether `useTLS` is defined in `main.go` itself or a sibling file in `cmd/mcp-hub-server/` (run `grep -rn "func useTLS" cmd/mcp-hub-server/`) — if it's in a separate file, leave that file untouched; the replacement above assumes `useTLS` stays available exactly as it is today either way.

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./cmd/mcp-hub-server/... -race -v`
Expected: PASS

- [ ] **Step 6: Full build/vet/test**

Run: `go build ./... && go vet ./... && go test -race -count=2 ./...`
Expected: all pass

- [ ] **Step 7: Rebuild binaries and smoke-test manually**

```bash
go build -o bin/mcp-hub-client ./cmd/mcp-hub-client
go build -o bin/mcp-hub-server ./cmd/mcp-hub-server
./bin/mcp-hub-server -addr :18765 &
sleep 0.5
curl -s -X POST localhost:18765/mcp -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke-test","version":"0"}}}'
kill %1
```

Expected: a JSON-RPC response containing `"serverInfo":{"name":"mcp-hub-server"...` (or equivalent success), not a connection error or 404.

- [ ] **Step 8: Commit**

```bash
git add cmd/mcp-hub-server/main.go cmd/mcp-hub-server/main_test.go
git commit -m "feat: mount the HTTP-MCP endpoint and /watch alongside the websocket relay"
```

---

### Task 7: Docs

**Files:**
- Modify: `README.adoc`
- Modify: `docs/superpowers/specs/2026-08-21-mcp-hub-design.md`

**Interfaces:**
- Consumes: nothing new — this task only documents Tasks 1-6's already-implemented behavior.

- [ ] **Step 1: Read both files' existing structure**

Run: `grep -n "^==\|^===" README.adoc docs/superpowers/specs/2026-08-21-mcp-hub-design.md`

to find the right place to insert a new section (alongside the existing description of `mcp-hub-server`/the websocket relay) rather than appending at the end out of context.

- [ ] **Step 2: Add a section to `README.adoc`**

Insert a new section near the existing description of `mcp-hub-server`, covering:
- `mcp-hub-server` now also serves a Streamable-HTTP MCP endpoint at `/mcp`, in addition to its existing websocket relay at `/{sessionId}` — no configuration changes to existing deployments required.
- Tool set: `hub_connect` (optionally creating a new session), `hub_disconnect`, `hub_send`, `hub_receive`, `hub_wait`, `hub_peers` — a deliberate subset of `mcp-hub-client`'s tools; reactions/edit/delete/history remain `mcp-hub-client`-only (bridge-specific, not something the plain relay understands).
- Async notification: `hub_connect`'s result includes a `watchToken`; `GET /watch?token=<token>&follow=1` (e.g. `curl -N`) streams that peer's events live, for clients (in particular, Claude Code backgrounding the curl call and watching it via Monitor) that want to avoid tying up a turn in a blocking `hub_wait`.
- No new auth — matches the existing websocket endpoint.

Write this as plain AsciiDoc prose/list matching the surrounding style already in the file (check a nearby section's formatting before writing — e.g. whether the existing tool list uses a description list, a table, or plain bullets — and match it).

- [ ] **Step 3: Add the same information to the design doc**

Add a corresponding section to `docs/superpowers/specs/2026-08-21-mcp-hub-design.md`, cross-referencing `docs/superpowers/specs/2026-08-28-http-mcp-endpoint-design.md` for the full rationale rather than duplicating it at length.

- [ ] **Step 4: Full build/vet/test (docs-only change, but keep the habit)**

Run: `go build ./... && go vet ./... && go test -race -count=1 ./...`
Expected: all pass (no code changed, this just confirms nothing else drifted)

- [ ] **Step 5: Commit**

```bash
git add README.adoc docs/superpowers/specs/2026-08-21-mcp-hub-design.md
git commit -m "docs: document mcp-hub-server's HTTP-MCP endpoint"
```
