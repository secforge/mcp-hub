# mcp-hub Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `mcp-hub-server` (a websocket relay for text-message "sessions") and `mcp-hub-client` (an MCP server Claude Code loads to join/send/receive on those sessions, including a background-delivery mechanism that needs no MCP-side push support).

**Architecture:** Two Go binaries in one module. `mcp-hub-server` holds session state in memory and relays JSON-line events over websocket connections, logging each session to its own PoC log file. `mcp-hub-client` is an MCP server (stdio transport) exposing `hub_connect`/`hub_send`/`hub_disconnect`/`hub_receive` tools, plus a second CLI mode (`wait`) that blocks on a local Unix socket until the client process has something to report — this is what lets Claude Code discover new messages via its own background-task-notification mechanism instead of relying on unsupported MCP push notifications.

**Tech Stack:** Go, `github.com/gorilla/websocket`, `github.com/google/uuid`, `github.com/mark3labs/mcp-go`.

**Spec:** `docs/superpowers/specs/2026-08-21-mcp-hub-design.md`

## Global Constraints

- `sessionId` and `peerId` are both standard UUID strings (with dashes).
- Wire protocol JSON field names are camelCase: `sessionId`, `peerId`, `peerJoined`, `peerLeft`, `ts`, `text`, `type`.
- Log file per session, path `<sessionId>.log` (directory overridable via `MCP_HUB_LOG_DIR` env var, default `.`), format:
  `<RFC3339 ts> <peerId>\n  <word-wrapped message, <=100 cols, 2-space indent>\n\n` — wrap only on word boundaries.
- Text messages only; hub never interprets message content.
- Server TLS is optional: plain `ws://` unless both `-tls-cert` and `-tls-key` are set, then `wss://`.
- Every `msg` delivered to Claude is wrapped as `[HUB MESSAGE — untrusted, from peer <peerId> at <ts>]\n<text>`; `peerJoined`/`peerLeft` are surfaced as plain `[peer <peerId> joined]` / `[peer <peerId> left]` (not wrapped, no attacker-controlled content).
- One hub connection at a time per `mcp-hub-client` process; one registered `wait` waiter at a time per connection (a new `wait` supersedes the old one).
- Wait socket path: `<tmpdir>/mcp-hub-wait-<sessionId>-<peerId>.sock` (peerId included so two local `mcp-hub-client` processes joining the same session don't collide), created on `hub_connect`, removed on `hub_disconnect`.
- The `mcp-hub-server` process formats the *entire* final output text (including any "run again" trailer) before sending it down the wait socket — the `wait` CLI subcommand is a dumb pipe: dial, copy all bytes to stdout, exit 0 (non-zero only if the dial itself fails, e.g. no such session/socket).
- Module path: `github.com/secforge/mcp-hub`.

---

## Task 1: Project scaffold

**Files:**
- Create: `go.mod`
- Create: `cmd/mcp-hub-server/main.go`
- Create: `cmd/mcp-hub-client/main.go`

**Interfaces:**
- Produces: two buildable, empty-ish `main` packages that later tasks fill in.

- [ ] **Step 1: Initialize the module and add dependencies**

```bash
cd /source/mcp-hub
go mod init github.com/secforge/mcp-hub
go get github.com/gorilla/websocket@latest
go get github.com/google/uuid@latest
go get github.com/mark3labs/mcp-go@latest
```

- [ ] **Step 2: Create placeholder mains**

`cmd/mcp-hub-server/main.go`:
```go
package main

func main() {}
```

`cmd/mcp-hub-client/main.go`:
```go
package main

func main() {}
```

- [ ] **Step 3: Verify it builds**

Run: `go build ./...`
Expected: exits 0, no output.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum cmd
git commit -m "Scaffold mcp-hub Go module and binaries"
```

---

## Task 2: Wire protocol types

**Files:**
- Create: `internal/wire/wire.go`
- Test: `internal/wire/wire_test.go`

**Interfaces:**
- Produces: `wire.Type` constants (`TypeJoin`, `TypeJoined`, `TypeError`, `TypeMsg`, `TypePeerJoined`, `TypePeerLeft`); types `Join{Type,SessionID}`, `Joined{Type,PeerID}`, `Error{Type,Message}`, `Msg{Type,PeerID,Text,TS}`, `PeerEvent{Type,PeerID}`; constructors `NewJoin(sessionID string) Join`, `NewJoined(peerID string) Joined`, `NewError(message string) Error`, `NewOutgoingMsg(text string) Msg`, `NewBroadcastMsg(peerID, text, ts string) Msg`, `NewPeerJoined(peerID string) PeerEvent`, `NewPeerLeft(peerID string) PeerEvent`; `DecodeType(raw []byte) (Type, error)`.

- [ ] **Step 1: Write the failing tests**

`internal/wire/wire_test.go`:
```go
package wire

import (
	"encoding/json"
	"testing"
)

func TestJoinRoundTrip(t *testing.T) {
	j := NewJoin("550e8400-e29b-41d4-a716-446655440000")
	raw, err := json.Marshal(j)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(raw); got != `{"type":"join","sessionId":"550e8400-e29b-41d4-a716-446655440000"}` {
		t.Fatalf("unexpected json: %s", got)
	}
	typ, err := DecodeType(raw)
	if err != nil {
		t.Fatalf("decode type: %v", err)
	}
	if typ != TypeJoin {
		t.Fatalf("got type %q, want %q", typ, TypeJoin)
	}
}

func TestBroadcastMsgFields(t *testing.T) {
	m := NewBroadcastMsg("peer-1", "hello", "2026-08-21T10:00:00Z")
	raw, _ := json.Marshal(m)
	var decoded Msg
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Type != TypeMsg || decoded.PeerID != "peer-1" || decoded.Text != "hello" || decoded.TS != "2026-08-21T10:00:00Z" {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
}

func TestOutgoingMsgHasNoPeerOrTS(t *testing.T) {
	m := NewOutgoingMsg("hi")
	raw, _ := json.Marshal(m)
	if got := string(raw); got != `{"type":"msg","text":"hi"}` {
		t.Fatalf("outgoing msg should omit empty peerId/ts, got: %s", got)
	}
}

func TestDecodeTypeError(t *testing.T) {
	if _, err := DecodeType([]byte("not json")); err == nil {
		t.Fatal("expected error decoding invalid json")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/wire/...`
Expected: FAIL — package `wire` (and its types) don't exist yet.

- [ ] **Step 3: Implement**

`internal/wire/wire.go`:
```go
package wire

import "encoding/json"

type Type string

const (
	TypeJoin       Type = "join"
	TypeJoined     Type = "joined"
	TypeError      Type = "error"
	TypeMsg        Type = "msg"
	TypePeerJoined Type = "peerJoined"
	TypePeerLeft   Type = "peerLeft"
)

type envelope struct {
	Type Type `json:"type"`
}

// DecodeType reads just the "type" field from a wire message.
func DecodeType(raw []byte) (Type, error) {
	var e envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return "", err
	}
	return e.Type, nil
}

type Join struct {
	Type      Type   `json:"type"`
	SessionID string `json:"sessionId"`
}

func NewJoin(sessionID string) Join {
	return Join{Type: TypeJoin, SessionID: sessionID}
}

type Joined struct {
	Type   Type   `json:"type"`
	PeerID string `json:"peerId"`
}

func NewJoined(peerID string) Joined {
	return Joined{Type: TypeJoined, PeerID: peerID}
}

type Error struct {
	Type    Type   `json:"type"`
	Message string `json:"message"`
}

func NewError(message string) Error {
	return Error{Type: TypeError, Message: message}
}

type Msg struct {
	Type   Type   `json:"type"`
	PeerID string `json:"peerId,omitempty"`
	Text   string `json:"text"`
	TS     string `json:"ts,omitempty"`
}

// NewOutgoingMsg is what a client sends to the server.
func NewOutgoingMsg(text string) Msg {
	return Msg{Type: TypeMsg, Text: text}
}

// NewBroadcastMsg is what the server sends to other session members.
func NewBroadcastMsg(peerID, text, ts string) Msg {
	return Msg{Type: TypeMsg, PeerID: peerID, Text: text, TS: ts}
}

type PeerEvent struct {
	Type   Type   `json:"type"`
	PeerID string `json:"peerId"`
}

func NewPeerJoined(peerID string) PeerEvent {
	return PeerEvent{Type: TypePeerJoined, PeerID: peerID}
}

func NewPeerLeft(peerID string) PeerEvent {
	return PeerEvent{Type: TypePeerLeft, PeerID: peerID}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/wire/...`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/wire
git commit -m "Add wire protocol message types"
```

---

## Task 3: Log formatting (word-wrap)

**Files:**
- Create: `internal/hublog/wrap.go`
- Test: `internal/hublog/wrap_test.go`

**Interfaces:**
- Produces: `hublog.FormatEntry(ts, peerID, text string) string`.

- [ ] **Step 1: Write the failing tests**

`internal/hublog/wrap_test.go`:
```go
package hublog

import "testing"

func TestFormatEntryBasic(t *testing.T) {
	got := FormatEntry("2026-08-21T10:00:00Z", "peer-1", "hello world")
	want := "2026-08-21T10:00:00Z peer-1\n  hello world\n\n"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatEntryWrapsOnWordBoundary(t *testing.T) {
	word := "abcdefghij" // 10 chars
	text := ""
	for i := 0; i < 12; i++ { // 12*10 + 11 spaces = 131 chars, must wrap
		if i > 0 {
			text += " "
		}
		text += word
	}
	got := FormatEntry("ts", "p1", text)
	lines := splitLines(got)
	for _, l := range lines {
		if len(l) > 102 { // 100 + 2-space indent
			t.Fatalf("line too long (%d): %q", len(l), l)
		}
	}
}

func TestFormatEntryNeverSplitsAWord(t *testing.T) {
	longWord := ""
	for i := 0; i < 150; i++ {
		longWord += "x"
	}
	got := FormatEntry("ts", "p1", "short "+longWord)
	if !contains(got, longWord) {
		t.Fatalf("long word was split across lines: %q", got)
	}
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i, c := range s {
		if c == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	return lines
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/hublog/...`
Expected: FAIL — `FormatEntry` doesn't exist.

- [ ] **Step 3: Implement**

`internal/hublog/wrap.go`:
```go
package hublog

import "strings"

const wrapWidth = 100

// FormatEntry renders one log entry: "<ts> <peerId>" then the message text
// word-wrapped at wrapWidth columns, each line indented by two spaces, then a
// blank line.
func FormatEntry(ts, peerID, text string) string {
	var b strings.Builder
	b.WriteString(ts)
	b.WriteByte(' ')
	b.WriteString(peerID)
	b.WriteByte('\n')
	for _, line := range wrapText(text, wrapWidth) {
		b.WriteString("  ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.String()
}

func wrapText(text string, width int) []string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return []string{""}
	}
	lines := make([]string, 0, len(words))
	cur := words[0]
	for _, w := range words[1:] {
		if len(cur)+1+len(w) > width {
			lines = append(lines, cur)
			cur = w
			continue
		}
		cur += " " + w
	}
	lines = append(lines, cur)
	return lines
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/hublog/...`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/hublog
git commit -m "Add PoC log entry word-wrap formatting"
```

---

## Task 4: Per-session log file writer

**Files:**
- Create: `internal/hublog/log.go`
- Test: `internal/hublog/log_test.go`

**Interfaces:**
- Consumes: `FormatEntry` from Task 3.
- Produces: `hublog.OpenSessionLog(sessionID string) (*SessionLog, error)`, `(*SessionLog).Append(peerID, text, ts string) error`, `(*SessionLog).Close() error`.

- [ ] **Step 1: Write the failing test**

`internal/hublog/log_test.go`:
```go
package hublog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenSessionLogAppendsFormattedEntries(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	l, err := OpenSessionLog(sessionID)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := l.Append("peer-1", "hello", "2026-08-21T10:00:00Z"); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, sessionID+".log"))
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	want := FormatEntry("2026-08-21T10:00:00Z", "peer-1", "hello")
	if string(data) != want {
		t.Fatalf("got %q, want %q", data, want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/hublog/... -run TestOpenSessionLog`
Expected: FAIL — `OpenSessionLog` doesn't exist.

- [ ] **Step 3: Implement**

`internal/hublog/log.go`:
```go
package hublog

import (
	"os"
	"path/filepath"
	"sync"
)

type SessionLog struct {
	mu   sync.Mutex
	file *os.File
}

// OpenSessionLog opens (creating if needed) the append-only log file for a
// session, under MCP_HUB_LOG_DIR (default ".").
func OpenSessionLog(sessionID string) (*SessionLog, error) {
	path := filepath.Join(logDir(), sessionID+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &SessionLog{file: f}, nil
}

func logDir() string {
	if d := os.Getenv("MCP_HUB_LOG_DIR"); d != "" {
		return d
	}
	return "."
}

func (l *SessionLog) Append(peerID, text, ts string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := l.file.WriteString(FormatEntry(ts, peerID, text))
	return err
}

func (l *SessionLog) Close() error {
	return l.file.Close()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/hublog/...`
Expected: PASS (4 tests total in package).

- [ ] **Step 5: Commit**

```bash
git add internal/hublog
git commit -m "Add per-session PoC log file writer"
```

---

## Task 5: Session manager

**Files:**
- Create: `internal/hubsession/session.go`
- Test: `internal/hubsession/session_test.go`

**Interfaces:**
- Consumes: `wire.NewPeerJoined`, `wire.NewPeerLeft` from Task 2.
- Produces: `hubsession.Peer` interface (`ID() string`, `Deliver(event any)`); `hubsession.Session` with `Join(p Peer)`, `Leave(p Peer) (empty bool)`, `Broadcast(from Peer, event any)`; `hubsession.Manager` with `NewManager() *Manager`, `(*Manager).GetOrCreate(id string) *Session`, `(*Manager).Remove(id string)`.

- [ ] **Step 1: Write the failing tests**

`internal/hubsession/session_test.go`:
```go
package hubsession

import (
	"testing"

	"github.com/secforge/mcp-hub/internal/wire"
)

type fakePeer struct {
	id       string
	received []any
}

func (f *fakePeer) ID() string          { return f.id }
func (f *fakePeer) Deliver(event any)   { f.received = append(f.received, event) }

func TestJoinBroadcastsPeerJoinedToOthersNotSelf(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	b := &fakePeer{id: "b"}

	s.Join(a)
	s.Join(b)

	if len(a.received) != 1 {
		t.Fatalf("a should have received b's join event, got %d events", len(a.received))
	}
	if ev, ok := a.received[0].(wire.PeerEvent); !ok || ev.PeerID != "b" {
		t.Fatalf("unexpected event for a: %+v", a.received[0])
	}
	if len(b.received) != 0 {
		t.Fatalf("b should not receive its own join event, got %d", len(b.received))
	}
}

func TestBroadcastExcludesSender(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	b := &fakePeer{id: "b"}
	s.Join(a)
	s.Join(b)
	a.received = nil
	b.received = nil

	s.Broadcast(a, wire.NewBroadcastMsg("a", "hi", "ts"))

	if len(a.received) != 0 {
		t.Fatalf("sender should not receive its own broadcast, got %d", len(a.received))
	}
	if len(b.received) != 1 {
		t.Fatalf("b should have received the broadcast, got %d", len(b.received))
	}
}

func TestLeaveReportsEmptyWhenLastPeerLeaves(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	b := &fakePeer{id: "b"}
	s.Join(a)
	s.Join(b)

	if empty := s.Leave(a); empty {
		t.Fatal("session should not be empty after only one of two peers leaves")
	}
	if empty := s.Leave(b); !empty {
		t.Fatal("session should be empty after the last peer leaves")
	}
}

func TestManagerGetOrCreateReturnsSameSession(t *testing.T) {
	m := NewManager()
	s1 := m.GetOrCreate("session-1")
	s2 := m.GetOrCreate("session-1")
	if s1 != s2 {
		t.Fatal("GetOrCreate should return the same session for the same id")
	}
	m.Remove("session-1")
	s3 := m.GetOrCreate("session-1")
	if s3 == s1 {
		t.Fatal("GetOrCreate should create a fresh session after Remove")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/hubsession/...`
Expected: FAIL — package doesn't exist yet.

- [ ] **Step 3: Implement**

`internal/hubsession/session.go`:
```go
package hubsession

import (
	"sync"

	"github.com/secforge/mcp-hub/internal/wire"
)

// Peer is anything that can receive broadcast events for a session.
type Peer interface {
	ID() string
	Deliver(event any)
}

type Session struct {
	id    string
	mu    sync.Mutex
	peers map[string]Peer
}

func newSession(id string) *Session {
	return &Session{id: id, peers: make(map[string]Peer)}
}

func (s *Session) Join(p Peer) {
	s.mu.Lock()
	s.peers[p.ID()] = p
	s.mu.Unlock()
	s.broadcastExcept(p.ID(), wire.NewPeerJoined(p.ID()))
}

// Leave removes p from the session and reports whether the session is now
// empty. The caller is responsible for tearing the session down via
// Manager.Remove when empty is true.
func (s *Session) Leave(p Peer) (empty bool) {
	s.mu.Lock()
	delete(s.peers, p.ID())
	empty = len(s.peers) == 0
	s.mu.Unlock()
	s.broadcastExcept(p.ID(), wire.NewPeerLeft(p.ID()))
	return empty
}

func (s *Session) Broadcast(from Peer, event any) {
	s.broadcastExcept(from.ID(), event)
}

func (s *Session) broadcastExcept(exceptID string, event any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, p := range s.peers {
		if id == exceptID {
			continue
		}
		p.Deliver(event)
	}
}

type Manager struct {
	mu       sync.Mutex
	sessions map[string]*Session
}

func NewManager() *Manager {
	return &Manager{sessions: make(map[string]*Session)}
}

func (m *Manager) GetOrCreate(id string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[id]; ok {
		return s
	}
	s := newSession(id)
	m.sessions[id] = s
	return s
}

func (m *Manager) Remove(id string) {
	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/hubsession/...`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/hubsession
git commit -m "Add in-memory session manager"
```

---

## Task 6: Websocket server handler

**Files:**
- Create: `internal/wsserver/server.go`
- Test: `internal/wsserver/server_test.go`

**Interfaces:**
- Consumes: `wire.*` (Task 2), `hubsession.NewManager/Peer` (Task 5), `hublog.OpenSessionLog` (Task 4).
- Produces: `wsserver.NewHandler() *Handler` where `*Handler` implements `http.Handler`.

- [ ] **Step 1: Write the failing integration test**

`internal/wsserver/server_test.go`:
```go
package wsserver

import (
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return c
}

func readTyped(t *testing.T, c *websocket.Conn) (wire.Type, []byte) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, raw, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	typ, err := wire.DecodeType(raw)
	if err != nil {
		t.Fatalf("decode type: %v", err)
	}
	return typ, raw
}

func TestJoinRelayAndTeardown(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a := dial(t, url)
	defer a.Close()
	a.WriteJSON(wire.NewJoin(sessionID))
	if typ, _ := readTyped(t, a); typ != wire.TypeJoined {
		t.Fatalf("a: expected joined, got %s", typ)
	}

	b := dial(t, url)
	b.WriteJSON(wire.NewJoin(sessionID))
	if typ, _ := readTyped(t, b); typ != wire.TypeJoined {
		t.Fatalf("b: expected joined, got %s", typ)
	}
	if typ, _ := readTyped(t, a); typ != wire.TypePeerJoined {
		t.Fatalf("a: expected peerJoined for b, got %s", typ)
	}

	b.WriteJSON(wire.NewOutgoingMsg("hello from b"))
	typ, raw := readTyped(t, a)
	if typ != wire.TypeMsg {
		t.Fatalf("a: expected msg, got %s", typ)
	}
	var m wire.Msg
	decodeJSON(t, raw, &m)
	if m.Text != "hello from b" || m.PeerID == "" || m.TS == "" {
		t.Fatalf("unexpected msg: %+v", m)
	}

	b.Close()
	if typ, _ := readTyped(t, a); typ != wire.TypePeerLeft {
		t.Fatalf("a: expected peerLeft for b, got %s", typ)
	}

	data, err := os.ReadFile(dir + "/" + sessionID + ".log")
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if !strings.Contains(string(data), "hello from b") {
		t.Fatalf("log missing message: %s", data)
	}
}

func TestInvalidSessionIDRejected(t *testing.T) {
	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	c := dial(t, url)
	defer c.Close()
	c.WriteJSON(wire.NewJoin("not-a-uuid"))
	typ, _ := readTyped(t, c)
	if typ != wire.TypeError {
		t.Fatalf("expected error, got %s", typ)
	}
}

func decodeJSON(t *testing.T, raw []byte, v any) {
	t.Helper()
	if err := jsonUnmarshal(raw, v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
}
```

`internal/wsserver/testutil_test.go`:
```go
package wsserver

import "encoding/json"

func jsonUnmarshal(raw []byte, v any) error {
	return json.Unmarshal(raw, v)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/wsserver/...`
Expected: FAIL — `NewHandler` doesn't exist.

- [ ] **Step 3: Implement**

`internal/wsserver/server.go`:
```go
package wsserver

import (
	"encoding/json"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/hublog"
	"github.com/secforge/mcp-hub/internal/hubsession"
	"github.com/secforge/mcp-hub/internal/wire"
)

var sessionIDPattern = regexp.MustCompile(
	`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`,
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type Handler struct {
	manager *hubsession.Manager
}

func NewHandler() *Handler {
	return &Handler{manager: hubsession.NewManager()}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	h.serve(conn)
}

func (h *Handler) serve(conn *websocket.Conn) {
	defer conn.Close()

	_, raw, err := conn.ReadMessage()
	if err != nil {
		return
	}
	var join wire.Join
	if err := json.Unmarshal(raw, &join); err != nil ||
		join.Type != wire.TypeJoin || !sessionIDPattern.MatchString(join.SessionID) {
		conn.WriteJSON(wire.NewError("invalid sessionId"))
		return
	}

	peerID := uuid.NewString()
	p := &peer{id: peerID, conn: conn}

	logger, logErr := hublog.OpenSessionLog(join.SessionID)
	if logErr == nil {
		defer logger.Close()
	}

	session := h.manager.GetOrCreate(join.SessionID)
	if err := conn.WriteJSON(wire.NewJoined(peerID)); err != nil {
		return
	}
	session.Join(p)
	defer func() {
		if session.Leave(p) {
			h.manager.Remove(join.SessionID)
		}
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var m wire.Msg
		if err := json.Unmarshal(raw, &m); err != nil || m.Type != wire.TypeMsg {
			continue
		}
		ts := time.Now().UTC().Format(time.RFC3339)
		if logger != nil {
			logger.Append(peerID, m.Text, ts)
		}
		session.Broadcast(p, wire.NewBroadcastMsg(peerID, m.Text, ts))
	}
}

type peer struct {
	id   string
	mu   sync.Mutex
	conn *websocket.Conn
}

func (p *peer) ID() string { return p.id }

func (p *peer) Deliver(event any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = p.conn.WriteJSON(event)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/wsserver/...`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/wsserver
git commit -m "Add websocket relay handler"
```

---

## Task 7: mcp-hub-server main (CLI + optional TLS)

**Files:**
- Create: `cmd/mcp-hub-server/tls.go`
- Test: `cmd/mcp-hub-server/tls_test.go`
- Modify: `cmd/mcp-hub-server/main.go`

**Interfaces:**
- Consumes: `wsserver.NewHandler` (Task 6).
- Produces: `useTLS(certFile, keyFile string) bool` (small pure helper, unit tested); `main.go` wires flags `-addr` (default `:8765`), `-tls-cert`, `-tls-key`.

- [ ] **Step 1: Write the failing test**

`cmd/mcp-hub-server/tls_test.go`:
```go
package main

import "testing"

func TestUseTLSRequiresBothCertAndKey(t *testing.T) {
	cases := []struct {
		cert, key string
		want      bool
	}{
		{"", "", false},
		{"cert.pem", "", false},
		{"", "key.pem", false},
		{"cert.pem", "key.pem", true},
	}
	for _, c := range cases {
		if got := useTLS(c.cert, c.key); got != c.want {
			t.Errorf("useTLS(%q, %q) = %v, want %v", c.cert, c.key, got, c.want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/mcp-hub-server/...`
Expected: FAIL — `useTLS` doesn't exist.

- [ ] **Step 3: Implement**

`cmd/mcp-hub-server/tls.go`:
```go
package main

func useTLS(certFile, keyFile string) bool {
	return certFile != "" && keyFile != ""
}
```

`cmd/mcp-hub-server/main.go`:
```go
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/secforge/mcp-hub/internal/wsserver"
)

func main() {
	addr := flag.String("addr", ":8765", "listen address")
	certFile := flag.String("tls-cert", "", "TLS certificate file (optional)")
	keyFile := flag.String("tls-key", "", "TLS key file (optional)")
	flag.Parse()

	handler := wsserver.NewHandler()
	mux := http.NewServeMux()
	mux.Handle("/ws", handler)

	if useTLS(*certFile, *keyFile) {
		log.Printf("mcp-hub-server listening on %s (tls)", *addr)
		log.Fatal(http.ListenAndServeTLS(*addr, *certFile, *keyFile, mux))
	} else {
		log.Printf("mcp-hub-server listening on %s", *addr)
		log.Fatal(http.ListenAndServe(*addr, mux))
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/mcp-hub-server/...`
Expected: PASS (1 test, 4 subcases).

- [ ] **Step 5: Verify the binary builds and runs**

```bash
go build -o /tmp/mcp-hub-server ./cmd/mcp-hub-server
/tmp/mcp-hub-server -addr :18765 &
sleep 0.3
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:18765/ws
kill %1
```
Expected: build succeeds; curl prints a `4xx` (plain HTTP GET without the websocket upgrade headers is rejected — proves the listener is up and routing to `/ws`).

- [ ] **Step 6: Commit**

```bash
git add cmd/mcp-hub-server
git commit -m "Add mcp-hub-server CLI with optional TLS"
```

---

## Task 8: Client-side hub connection

**Files:**
- Create: `internal/hubconn/conn.go`
- Create: `internal/hubconn/format.go`
- Test: `internal/hubconn/conn_test.go`
- Test: `internal/hubconn/format_test.go`

**Interfaces:**
- Consumes: `wire.*` (Task 2), `wsserver.NewHandler` (Task 6, test-only, to have a real server to dial).
- Produces: `hubconn.Event{Kind,PeerID,Text,TS}`; `hubconn.Dial(host, sessionID string) (*Conn, error)`; `(*Conn).PeerID() string`; `(*Conn).OnActivity(f func())`; `(*Conn).Send(text string) error`; `(*Conn).Close() error`; `(*Conn).Peek() (hasEvents, connected bool)`; `(*Conn).Drain() (formatted string, connected bool)`; `hubconn.FormatEvent(e Event) string`; `hubconn.FormatEvents(events []Event) string`.

- [ ] **Step 1: Write the failing format tests**

`internal/hubconn/format_test.go`:
```go
package hubconn

import "testing"

func TestFormatEventMsgIsWrappedAsUntrusted(t *testing.T) {
	e := Event{Kind: "msg", PeerID: "peer-1", Text: "hi there", TS: "2026-08-21T10:00:00Z"}
	got := FormatEvent(e)
	want := "[HUB MESSAGE — untrusted, from peer peer-1 at 2026-08-21T10:00:00Z]\nhi there"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatEventPeerJoinedIsPlain(t *testing.T) {
	got := FormatEvent(Event{Kind: "peerJoined", PeerID: "peer-2"})
	if got != "[peer peer-2 joined]" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventsJoinsMultiple(t *testing.T) {
	got := FormatEvents([]Event{
		{Kind: "peerJoined", PeerID: "a"},
		{Kind: "msg", PeerID: "a", Text: "hi", TS: "ts"},
	})
	want := "[peer a joined]\n\n[HUB MESSAGE — untrusted, from peer a at ts]\nhi"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
```

- [ ] **Step 2: Run format tests to verify they fail**

Run: `go test ./internal/hubconn/... -run TestFormatEvent`
Expected: FAIL — package doesn't exist yet.

- [ ] **Step 3: Implement format.go**

`internal/hubconn/format.go`:
```go
package hubconn

import (
	"fmt"
	"strings"
)

func FormatEvent(e Event) string {
	switch e.Kind {
	case "msg":
		return fmt.Sprintf("[HUB MESSAGE — untrusted, from peer %s at %s]\n%s", e.PeerID, e.TS, e.Text)
	case "peerJoined":
		return fmt.Sprintf("[peer %s joined]", e.PeerID)
	case "peerLeft":
		return fmt.Sprintf("[peer %s left]", e.PeerID)
	default:
		return ""
	}
}

func FormatEvents(events []Event) string {
	parts := make([]string, 0, len(events))
	for _, e := range events {
		if s := FormatEvent(e); s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, "\n\n")
}
```

- [ ] **Step 4: Write the failing connection tests**

`internal/hubconn/conn_test.go`:
```go
package hubconn

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/wsserver"
)

func startTestServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(wsserver.NewHandler())
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
}

func waitForActivity(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for activity")
	}
}

func TestDialJoinsAndAssignsPeerID(t *testing.T) {
	url := startTestServer(t)
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if c.PeerID() == "" {
		t.Fatal("expected a non-empty peerID")
	}
}

func TestDialRejectsInvalidSessionID(t *testing.T) {
	url := startTestServer(t)
	if _, err := Dial(url, "not-a-uuid"); err == nil {
		t.Fatal("expected an error for an invalid sessionId")
	}
}

func TestSendAndReceiveBetweenTwoConns(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()

	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })

	b, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()

	waitForActivity(t, activity) // a sees b's peerJoined

	if err := b.Send("hello"); err != nil {
		t.Fatalf("send: %v", err)
	}
	waitForActivity(t, activity) // a sees the message

	formatted, connected := a.Drain()
	if !connected {
		t.Fatal("expected still connected")
	}
	if !strings.Contains(formatted, "hello") {
		t.Fatalf("expected drained events to include the message, got: %s", formatted)
	}
}

func TestPeekAndDrainReflectDisconnect(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })

	b, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	waitForActivity(t, activity) // a sees b's peerJoined
	b.Close()
	// server closing the connection propagates as a peerLeft to a, then
	// (separately) a's own socket must be closed by the test to observe its
	// own disconnect:
	a.Close()

	if _, connected := a.Peek(); connected {
		t.Fatal("expected Peek to report disconnected after Close")
	}
}
```

- [ ] **Step 5: Run connection tests to verify they fail**

Run: `go test ./internal/hubconn/...`
Expected: FAIL — `Dial`/`Conn`/`Event` don't exist.

- [ ] **Step 6: Implement conn.go**

`internal/hubconn/conn.go`:
```go
package hubconn

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

type Event struct {
	Kind   string
	PeerID string
	Text   string
	TS     string
}

type Conn struct {
	ws     *websocket.Conn
	peerID string

	mu         sync.Mutex
	buffer     []Event
	closed     bool
	onActivity func()
}

// Dial connects to host, joins sessionID, and starts a background read loop.
func Dial(host, sessionID string) (*Conn, error) {
	ws, _, err := websocket.DefaultDialer.Dial(host, nil)
	if err != nil {
		return nil, err
	}
	if err := ws.WriteJSON(wire.NewJoin(sessionID)); err != nil {
		ws.Close()
		return nil, err
	}
	_, raw, err := ws.ReadMessage()
	if err != nil {
		ws.Close()
		return nil, err
	}
	typ, err := wire.DecodeType(raw)
	if err != nil {
		ws.Close()
		return nil, err
	}
	if typ == wire.TypeError {
		var e wire.Error
		json.Unmarshal(raw, &e)
		ws.Close()
		return nil, fmt.Errorf("join rejected: %s", e.Message)
	}
	var joined wire.Joined
	if err := json.Unmarshal(raw, &joined); err != nil {
		ws.Close()
		return nil, err
	}

	c := &Conn{ws: ws, peerID: joined.PeerID}
	go c.readLoop()
	return c, nil
}

func (c *Conn) PeerID() string { return c.peerID }

// OnActivity registers a callback invoked (from the background read
// goroutine) after every new buffered event and on disconnect.
func (c *Conn) OnActivity(f func()) {
	c.mu.Lock()
	c.onActivity = f
	c.mu.Unlock()
}

func (c *Conn) readLoop() {
	for {
		_, raw, err := c.ws.ReadMessage()
		if err != nil {
			c.mu.Lock()
			c.closed = true
			f := c.onActivity
			c.mu.Unlock()
			if f != nil {
				f()
			}
			return
		}
		ev, ok := decodeEvent(raw)
		if !ok {
			continue
		}
		c.mu.Lock()
		c.buffer = append(c.buffer, ev)
		f := c.onActivity
		c.mu.Unlock()
		if f != nil {
			f()
		}
	}
}

func decodeEvent(raw []byte) (Event, bool) {
	typ, err := wire.DecodeType(raw)
	if err != nil {
		return Event{}, false
	}
	switch typ {
	case wire.TypeMsg:
		var m wire.Msg
		if err := json.Unmarshal(raw, &m); err != nil {
			return Event{}, false
		}
		return Event{Kind: "msg", PeerID: m.PeerID, Text: m.Text, TS: m.TS}, true
	case wire.TypePeerJoined:
		var p wire.PeerEvent
		if err := json.Unmarshal(raw, &p); err != nil {
			return Event{}, false
		}
		return Event{Kind: "peerJoined", PeerID: p.PeerID}, true
	case wire.TypePeerLeft:
		var p wire.PeerEvent
		if err := json.Unmarshal(raw, &p); err != nil {
			return Event{}, false
		}
		return Event{Kind: "peerLeft", PeerID: p.PeerID}, true
	default:
		return Event{}, false
	}
}

func (c *Conn) Send(text string) error {
	return c.ws.WriteJSON(wire.NewOutgoingMsg(text))
}

func (c *Conn) Close() error {
	return c.ws.Close()
}

// Peek reports whether unread events are buffered, and whether the
// connection is still open. Non-destructive.
func (c *Conn) Peek() (hasEvents, connected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.buffer) > 0, !c.closed
}

// Drain clears and returns the buffered events formatted for delivery, and
// whether the connection is still open.
func (c *Conn) Drain() (formatted string, connected bool) {
	c.mu.Lock()
	events := c.buffer
	c.buffer = nil
	connected = !c.closed
	c.mu.Unlock()
	return FormatEvents(events), connected
}
```

- [ ] **Step 7: Run all hubconn tests to verify they pass**

Run: `go test ./internal/hubconn/...`
Expected: PASS (7 tests).

- [ ] **Step 8: Commit**

```bash
git add internal/hubconn
git commit -m "Add client-side hub connection and message formatting"
```

---

## Task 9: Wait-socket waiter manager

**Files:**
- Create: `internal/waiter/waiter.go`
- Test: `internal/waiter/waiter_test.go`

**Interfaces:**
- Consumes: nothing outside the package except the `Source` interface it defines (Task 8's `*hubconn.Conn` satisfies it via `Peek`/`Drain`, but the test uses a fake).
- Produces: `waiter.Source` interface (`Peek() (hasEvents, connected bool)`, `Drain() (formatted string, connected bool)`); `waiter.Listen(sessionID, peerID string, source Source) (*Waiter, error)`; `(*Waiter).WaitCommand() string`; `(*Waiter).Poke()`; `(*Waiter).Close() error`.

- [ ] **Step 1: Write the failing tests**

`internal/waiter/waiter_test.go`:
```go
package waiter

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type fakeSource struct {
	mu        sync.Mutex
	hasEvents bool
	connected bool
	formatted string
}

func (f *fakeSource) Peek() (bool, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hasEvents, f.connected
}

func (f *fakeSource) Drain() (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	formatted := f.formatted
	f.hasEvents = false
	f.formatted = ""
	return formatted, f.connected
}

func (f *fakeSource) push(text string) {
	f.mu.Lock()
	f.hasEvents = true
	f.formatted = text
	f.mu.Unlock()
}

func dialAndRead(t *testing.T, path string) string {
	t.Helper()
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(data)
}

func TestWaitDeliversAlreadyBufferedEvent(t *testing.T) {
	src := &fakeSource{connected: true}
	src.push("hello")
	w, err := Listen("session-a", "peer-a", src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	got := dialAndRead(t, w.socketPath)
	if got != "hello\n\nRun this command again to keep receiving:\n"+w.WaitCommand()+"\n" {
		t.Fatalf("unexpected output: %q", got)
	}
}

func TestPokeDeliversToRegisteredWaiter(t *testing.T) {
	src := &fakeSource{connected: true}
	w, err := Listen("session-b", "peer-b", src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	done := make(chan string, 1)
	go func() { done <- dialAndRead(t, w.socketPath) }()
	time.Sleep(100 * time.Millisecond) // let it register as the current waiter

	src.push("new message")
	w.Poke()

	select {
	case got := <-done:
		if got == "" {
			t.Fatal("expected non-empty output")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}
}

func TestNewWaiterSupersedesOld(t *testing.T) {
	src := &fakeSource{connected: true}
	w, err := Listen("session-c", "peer-c", src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	oldDone := make(chan string, 1)
	go func() { oldDone <- dialAndRead(t, w.socketPath) }()
	time.Sleep(100 * time.Millisecond)

	newDone := make(chan string, 1)
	go func() { newDone <- dialAndRead(t, w.socketPath) }()

	select {
	case got := <-oldDone:
		if got != "superseded by a newer wait\n" {
			t.Fatalf("expected the old waiter to be superseded, got: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the old waiter to be superseded")
	}

	src.push("hi")
	w.Poke()
	select {
	case got := <-newDone:
		if got == "" || got == "superseded by a newer wait\n" {
			t.Fatalf("expected the new waiter to receive the message, got: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the new waiter's delivery")
	}
}

func TestDisconnectedSourceReportedImmediately(t *testing.T) {
	src := &fakeSource{connected: false}
	w, err := Listen("session-d", "peer-d", src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	got := dialAndRead(t, w.socketPath)
	if got != "hub disconnected\n" {
		t.Fatalf("got %q", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/waiter/...`
Expected: FAIL — package doesn't exist.

- [ ] **Step 3: Implement**

`internal/waiter/waiter.go`:
```go
package waiter

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
)

// Source is the buffered-event side of a hub connection (satisfied by
// *hubconn.Conn).
type Source interface {
	// Peek reports whether unread events are buffered, and whether the
	// underlying connection is still open. Non-destructive.
	Peek() (hasEvents, connected bool)
	// Drain clears and returns the buffered events formatted for delivery,
	// and whether the connection is still open.
	Drain() (formatted string, connected bool)
}

type Waiter struct {
	source     Source
	ln         net.Listener
	socketPath string

	mu      sync.Mutex
	current net.Conn
}

// Listen opens the wait socket for this (sessionID, peerID) connection and
// starts accepting connections. peerID is included in the path (not just
// sessionID) so two separate mcp-hub-client processes joining the same
// session on the same host don't collide on the same socket path. Only one
// connection to this socket may be pending at a time; a new connection
// supersedes any previously registered one.
func Listen(sessionID, peerID string, source Source) (*Waiter, error) {
	path := filepath.Join(os.TempDir(), "mcp-hub-wait-"+sessionID+"-"+peerID+".sock")
	_ = os.Remove(path) // stale socket from a crashed prior run
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	w := &Waiter{source: source, ln: ln, socketPath: path}
	go w.acceptLoop()
	return w, nil
}

// WaitCommand is the exact command Claude should run in the background to
// receive the next event.
func (w *Waiter) WaitCommand() string {
	exe, err := os.Executable()
	if err != nil {
		exe = "mcp-hub-client"
	}
	return fmt.Sprintf("%s wait --socket %s", exe, w.socketPath)
}

func (w *Waiter) acceptLoop() {
	for {
		conn, err := w.ln.Accept()
		if err != nil {
			return // listener closed
		}
		w.handleAccept(conn)
	}
}

func (w *Waiter) handleAccept(conn net.Conn) {
	if hasEvents, connected := w.source.Peek(); hasEvents || !connected {
		w.deliver(conn)
		return
	}
	w.mu.Lock()
	old := w.current
	w.current = conn
	w.mu.Unlock()
	if old != nil {
		writeAndClose(old, "superseded by a newer wait\n")
	}
}

// Poke delivers to the currently registered waiter, if any, using whatever
// the source currently has buffered.
func (w *Waiter) Poke() {
	w.mu.Lock()
	conn := w.current
	w.current = nil
	w.mu.Unlock()
	if conn == nil {
		return
	}
	w.deliver(conn)
}

func (w *Waiter) deliver(conn net.Conn) {
	formatted, connected := w.source.Drain()
	if !connected {
		writeAndClose(conn, "hub disconnected\n")
		return
	}
	writeAndClose(conn, formatted+"\n\nRun this command again to keep receiving:\n"+w.WaitCommand()+"\n")
}

func (w *Waiter) Close() error {
	w.mu.Lock()
	if w.current != nil {
		writeAndClose(w.current, "hub disconnected\n")
		w.current = nil
	}
	w.mu.Unlock()
	err := w.ln.Close()
	_ = os.Remove(w.socketPath)
	return err
}

func writeAndClose(conn net.Conn, msg string) {
	_, _ = conn.Write([]byte(msg))
	_ = conn.Close()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/waiter/...`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/waiter
git commit -m "Add single-waiter wait-socket manager"
```

---

## Task 10: MCP tool handlers

**Files:**
- Create: `internal/mcptools/tools.go`
- Test: `internal/mcptools/tools_test.go`

**Interfaces:**
- Consumes: `hubconn.Dial/Conn` (Task 8, satisfies `waiter.Source`), `waiter.Listen` (Task 9), `wsserver.NewHandler` (Task 6, test-only).
- Produces: `mcptools.NewHub() *Hub`; `(*Hub).Register(s *server.MCPServer)`.

- [ ] **Step 1: Write the failing tests**

`internal/mcptools/tools_test.go`:
```go
package mcptools

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/wsserver"
)

func startTestServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(wsserver.NewHandler())
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
}

func TestConnectSendReceiveDisconnect(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"host": url, "sessionId": sessionID}
	res, err := hubA.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}

	hubB := NewHub()
	res, err = hubB.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "hello from b"}
	if res, err := hubB.handleSend(ctx, sendReq); err != nil || res.IsError {
		t.Fatalf("send failed: err=%v result=%+v", err, res)
	}

	recvReq := mcp.CallToolRequest{}
	var got string
	deadlinePoll(t, func() bool {
		res, err := hubA.handleReceive(ctx, recvReq)
		if err != nil {
			t.Fatalf("receive failed: %v", err)
		}
		got = textOf(res)
		return strings.Contains(got, "hello from b")
	})

	if !strings.Contains(got, "[HUB MESSAGE") {
		t.Fatalf("expected untrusted wrapper, got: %s", got)
	}

	discReq := mcp.CallToolRequest{}
	if res, err := hubA.handleDisconnect(ctx, discReq); err != nil || res.IsError {
		t.Fatalf("disconnect failed: err=%v result=%+v", err, res)
	}
	if res, err := hubA.handleSend(ctx, sendReq); err != nil || !res.IsError {
		t.Fatal("expected send after disconnect to error")
	}

	hubB.handleDisconnect(ctx, discReq)
}

func TestConnectTwiceErrors(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()
	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"host": url, "sessionId": sessionID}

	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("first connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	res, err := hub.handleConnect(ctx, connReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected second connect to report an error")
	}
}

func deadlinePoll(t *testing.T, check func() bool) {
	t.Helper()
	for i := 0; i < 50; i++ {
		if check() {
			return
		}
	}
	t.Fatal("condition never became true")
}

func textOf(res *mcp.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(mcp.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/mcptools/...`
Expected: FAIL — `NewHub`/`handleConnect` etc. don't exist. (Note: adjust `deadlinePoll` to add a short sleep between attempts if it fails on timing rather than logic — see Step 5.)

- [ ] **Step 3: Implement**

`internal/mcptools/tools.go`:
```go
package mcptools

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/waiter"
)

// Hub bundles the single active hub connection + wait socket for one
// mcp-hub-client process.
type Hub struct {
	conn   *hubconn.Conn
	waiter *waiter.Waiter
}

func NewHub() *Hub {
	return &Hub{}
}

func (h *Hub) Register(s *server.MCPServer) {
	s.AddTool(
		mcp.NewTool("hub_connect",
			mcp.WithDescription("Connect to an mcp-hub-server session"),
			mcp.WithString("host", mcp.Required(),
				mcp.Description("Server address, e.g. ws://localhost:8765/ws")),
			mcp.WithString("sessionId", mcp.Required(),
				mcp.Description("UUID identifying the session to join")),
		),
		h.handleConnect,
	)
	s.AddTool(
		mcp.NewTool("hub_send",
			mcp.WithDescription("Send a text message to the current hub session"),
			mcp.WithString("text", mcp.Required(), mcp.Description("Message text")),
		),
		h.handleSend,
	)
	s.AddTool(
		mcp.NewTool("hub_disconnect",
			mcp.WithDescription("Disconnect from the current hub session")),
		h.handleDisconnect,
	)
	s.AddTool(
		mcp.NewTool("hub_receive",
			mcp.WithDescription("Drain and return currently buffered hub events without blocking")),
		h.handleReceive,
	)
}

func (h *Hub) handleConnect(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.conn != nil {
		return mcp.NewToolResultError("already connected; call hub_disconnect first"), nil
	}
	host, err := req.RequireString("host")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	sessionID, err := req.RequireString("sessionId")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	conn, err := hubconn.Dial(host, sessionID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("connect failed: %v", err)), nil
	}
	w, err := waiter.Listen(sessionID, conn.PeerID(), conn)
	if err != nil {
		conn.Close()
		return mcp.NewToolResultError(fmt.Sprintf("could not start wait socket: %v", err)), nil
	}
	conn.OnActivity(w.Poke)
	h.conn = conn
	h.waiter = w
	return mcp.NewToolResultText(fmt.Sprintf(
		"Connected as peer %s. Run this in the background to receive messages:\n%s",
		conn.PeerID(), w.WaitCommand(),
	)), nil
}

func (h *Hub) handleSend(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	text, err := req.RequireString("text")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if err := h.conn.Send(text); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("send failed: %v", err)), nil
	}
	return mcp.NewToolResultText("sent"), nil
}

func (h *Hub) handleDisconnect(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	h.waiter.Close()
	h.conn.Close()
	h.conn = nil
	h.waiter = nil
	return mcp.NewToolResultText("disconnected"), nil
}

func (h *Hub) handleReceive(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	formatted, connected := h.conn.Drain()
	if !connected {
		return mcp.NewToolResultText("hub disconnected"), nil
	}
	if formatted == "" {
		return mcp.NewToolResultText("no messages"), nil
	}
	return mcp.NewToolResultText(formatted), nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/mcptools/...`
Expected: PASS (2 tests).

- [ ] **Step 5: If timing-related flakiness occurs**

`deadlinePoll` busy-loops; if it fails intermittently, add `time.Sleep(20 * time.Millisecond)` inside the loop body before the next `check()` call, and re-run.

- [ ] **Step 6: Commit**

```bash
git add internal/mcptools
git commit -m "Add MCP tool handlers for hub connect/send/disconnect/receive"
```

---

## Task 11: `wait` CLI subcommand and mcp-hub-client main

**Files:**
- Create: `cmd/mcp-hub-client/wait.go`
- Test: `cmd/mcp-hub-client/wait_test.go`
- Modify: `cmd/mcp-hub-client/main.go`

**Interfaces:**
- Consumes: `mcptools.NewHub/Register` (Task 10).
- Produces: `runWait(socketPath string, stdout, stderr io.Writer) int`; `main.go` dispatches `wait --socket <path>` to `runWait`, otherwise serves MCP over stdio.

- [ ] **Step 1: Write the failing test**

`cmd/mcp-hub-client/wait_test.go`:
```go
package main

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRunWaitCopiesSocketBytesToStdout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		conn.Write([]byte("hello\n"))
		conn.Close()
	}()

	var stdout, stderr bytes.Buffer
	code := runWait(path, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d (stderr: %s)", code, stderr.String())
	}
	if stdout.String() != "hello\n" {
		t.Fatalf("got %q", stdout.String())
	}
}

func TestRunWaitReturnsErrorOnMissingSocket(t *testing.T) {
	path := filepath.Join(os.TempDir(), "does-not-exist-"+time.Now().Format("150405")+".sock")
	var stdout, stderr bytes.Buffer
	code := runWait(path, &stdout, &stderr)
	if code == 0 {
		t.Fatal("expected non-zero exit code for a missing socket")
	}
	if stderr.Len() == 0 {
		t.Fatal("expected an error message on stderr")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/mcp-hub-client/...`
Expected: FAIL — `runWait` doesn't exist.

- [ ] **Step 3: Implement**

`cmd/mcp-hub-client/wait.go`:
```go
package main

import (
	"fmt"
	"io"
	"net"
)

func runWait(socketPath string, stdout, stderr io.Writer) int {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		fmt.Fprintf(stderr, "could not connect to %s: %v\n", socketPath, err)
		return 1
	}
	defer conn.Close()
	if _, err := io.Copy(stdout, conn); err != nil {
		fmt.Fprintf(stderr, "read error: %v\n", err)
		return 1
	}
	return 0
}
```

`cmd/mcp-hub-client/main.go`:
```go
package main

import (
	"flag"
	"os"

	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/mcptools"
)

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "wait" {
		fs := flag.NewFlagSet("wait", flag.ExitOnError)
		socket := fs.String("socket", "", "path to the wait unix socket")
		fs.Parse(os.Args[2:])
		os.Exit(runWait(*socket, os.Stdout, os.Stderr))
	}

	s := server.NewMCPServer("mcp-hub-client", "0.1.0", server.WithToolCapabilities(false))
	mcptools.NewHub().Register(s)
	if err := server.ServeStdio(s); err != nil {
		os.Exit(1)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/mcp-hub-client/...`
Expected: PASS (2 tests).

- [ ] **Step 5: Verify the full module builds and all tests pass**

```bash
go build ./...
go vet ./...
go test ./...
```
Expected: all succeed with no errors.

- [ ] **Step 6: Commit**

```bash
git add cmd/mcp-hub-client
git commit -m "Add wait CLI subcommand and mcp-hub-client MCP entry point"
```

---

## Task 12: End-to-end manual smoke test

**Files:**
- Create: `docs/superpowers/plans/2026-08-21-mcp-hub-smoke-test.md` (a short manual test procedure, not automated — this documents the acceptance check for the whole feature)

**Interfaces:**
- Consumes: both built binaries from all prior tasks.

- [ ] **Step 1: Build both binaries**

```bash
cd /source/mcp-hub
go build -o bin/mcp-hub-server ./cmd/mcp-hub-server
go build -o bin/mcp-hub-client ./cmd/mcp-hub-client
```
Expected: both build with no errors.

- [ ] **Step 2: Start the server**

```bash
MCP_HUB_LOG_DIR=/tmp/mcp-hub-smoke-logs mkdir -p /tmp/mcp-hub-smoke-logs
MCP_HUB_LOG_DIR=/tmp/mcp-hub-smoke-logs ./bin/mcp-hub-server -addr :18765 &
```
Expected: log line `mcp-hub-server listening on :18765`.

- [ ] **Step 3: Drive mcp-hub-client's MCP tools directly via stdio JSON-RPC to prove connect → send → receive → disconnect end to end**

Write a small throwaway Go program (not part of the module — delete after use) or use an existing MCP stdio test client if available; at minimum, confirm the following manually:

```bash
SESSION_ID=$(uuidgen)
echo "session: $SESSION_ID"
```

Using two separate `./bin/mcp-hub-client` MCP stdio sessions (e.g. via a local MCP inspector tool, or Claude Code itself configured with this MCP), call `hub_connect` with `host=ws://localhost:18765/ws` and `sessionId=$SESSION_ID` on both. Confirm:
- Each `hub_connect` result includes a distinct `wait --socket ...` command referencing `$SESSION_ID`.
- Calling `hub_send` on one and `hub_receive` on the other returns the message wrapped as `[HUB MESSAGE — untrusted, ...]`.
- Running the `wait` command (e.g. `./bin/mcp-hub-client wait --socket /tmp/mcp-hub-wait-$SESSION_ID.sock`) while the other side has nothing buffered blocks; sending a message from the other side causes it to print the message and exit 0 immediately.
- Running two `wait` commands back to back (without the first exiting) causes the first to print `superseded by a newer wait` and exit 0.
- Calling `hub_disconnect` on one side causes the other's next `wait`/`hub_receive` to reflect a `peerLeft` event, and removes that side's own wait socket file (confirm with `ls /tmp/mcp-hub-wait-$SESSION_ID.sock`, which should then report "No such file or directory" for the disconnected side's socket — note each `mcp-hub-client` process has its own socket, so with two clients you'll see two distinct socket paths, one per process, even though both share the same `$SESSION_ID`).

- [ ] **Step 4: Inspect the PoC log**

```bash
cat /tmp/mcp-hub-smoke-logs/$SESSION_ID.log
```
Expected: entries in the documented format (`<ts> <peerId>` header, 2-space-indented wrapped text, blank line separators), containing the message(s) sent during Step 3.

- [ ] **Step 5: Stop the server and clean up**

```bash
kill %1
rm -rf /tmp/mcp-hub-smoke-logs
```

- [ ] **Step 6: Write up the smoke test as a short doc and commit**

Write `docs/superpowers/plans/2026-08-21-mcp-hub-smoke-test.md` capturing the exact commands and expected outputs from Steps 1-4 above (as a durable manual regression checklist), then:

```bash
git add docs/superpowers/plans/2026-08-21-mcp-hub-smoke-test.md bin
git commit -m "Add manual end-to-end smoke test procedure"
```

(If `bin/` was added, add `/bin/` to `.gitignore` first instead of committing built binaries — check `.gitignore` already excludes it from Task 1's scaffold; if not, fix `.gitignore` and re-stage.)
