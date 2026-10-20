// Package agenttest is a fake agent-connection server for tests: it speaks
// the agent framing (see wire/agent.go), answers "open" with a wrapped
// "joined", and records every frame a client sends per conversation.
package agenttest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

// Frame is one inner frame a client sent, with the conversation the
// wrapper named and the one stamped inside it.
type Frame struct {
	Wrapper string
	Inner   string
	Type    wire.Type
	Raw     json.RawMessage
}

// Server is the fake. Its methods are safe from any goroutine.
type Server struct {
	t   *testing.T
	srv *httptest.Server

	mu       sync.Mutex
	ws       *websocket.Conn
	writeMu  sync.Mutex
	grants   map[string]wire.Assignment
	order    []string
	open     map[string]bool
	frames   map[string][]Frame
	controls []json.RawMessage
	headers  []http.Header
	dials    int
	// Features is what each conversation's "joined" declares; nil
	// declares nothing.
	Features map[string]json.RawMessage
	// AgentFeatures is what "agentJoined" declares.
	AgentFeatures map[string]json.RawMessage
	// OnSearch answers a "search": the frame it returns is sent back
	// unwrapped, and nil sends nothing.
	OnSearch func(wire.Search) any
	// encryption is what each encrypted conversation's "joined" declares.
	encryption map[string]*wire.Encryption
	// opens counts "open" frames per conversation, over all sockets.
	opens map[string]int
	// refuse answers "open" for a conversation with this error code.
	refuse map[string]string
	// RefuseDial makes the next dials fail with this HTTP status.
	RefuseDial int
	changed    chan struct{}
}

// New starts a server holding grants, in that order.
func New(t *testing.T, grants ...wire.Assignment) *Server {
	t.Helper()
	s := &Server{
		t:          t,
		grants:     map[string]wire.Assignment{},
		open:       map[string]bool{},
		frames:     map[string][]Frame{},
		changed:    make(chan struct{}, 1),
		encryption: map[string]*wire.Encryption{},
		opens:      map[string]int{},
		refuse:     map[string]string{},
	}
	for _, g := range grants {
		s.grants[g.Conv] = g
		s.order = append(s.order, g.Conv)
	}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(func() {
		s.Drop()
		s.srv.Close()
	})
	return s
}

// Link is the agent link to dial.
func (s *Server) Link() string {
	return "ws" + strings.TrimPrefix(s.srv.URL, "http") + "/agent/join#agent-token"
}

// AgentID is the id the handshake names.
const AgentID = "a9e1c2d0-0000-4000-8000-000000000001"

// PeerFor is the peerId a conversation's "joined" carries.
func PeerFor(conv string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("agenttest:"+conv)).String()
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.headers = append(s.headers, r.Header.Clone())
	refuse := s.RefuseDial
	s.mu.Unlock()
	if refuse != 0 {
		http.Error(w, "refused", refuse)
		return
	}
	if v, _ := strconv.Atoi(r.Header.Get("Hub-Protocol-Version")); v < 5 {
		http.Error(w, "this is an agent link; it needs protocol v5", http.StatusBadRequest)
		return
	}
	up := websocket.Upgrader{}
	ws, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.mu.Lock()
	if s.ws != nil {
		s.ws.Close()
	}
	s.ws = ws
	s.dials++
	s.open = map[string]bool{}
	agentFeatures := s.AgentFeatures
	items := make([]wire.Assignment, 0, len(s.order))
	for _, c := range s.order {
		items = append(items, s.grants[c])
	}
	s.mu.Unlock()
	s.write(ws, wire.AgentJoined{Type: wire.TypeAgentJoined, AgentID: AgentID, Name: "test agent",
		ServerVersion: wire.ProtocolVersion, Features: agentFeatures})
	s.write(ws, wire.Assignments{Type: wire.TypeAssignments, Items: items})
	for {
		_, raw, err := ws.ReadMessage()
		if err != nil {
			return
		}
		s.handle(ws, raw)
	}
}

func (s *Server) handle(ws *websocket.Conn, raw []byte) {
	switch wire.FrameType(raw) {
	case wire.TypeOpen:
		var f wire.OpenConv
		_ = json.Unmarshal(raw, &f)
		s.mu.Lock()
		g, ok := s.grants[f.Conv]
		if ok {
			s.open[f.Conv] = true
		}
		s.opens[f.Conv]++
		features := s.Features
		enc := s.encryption[f.Conv]
		refused := s.refuse[f.Conv]
		if refused != "" {
			delete(s.open, f.Conv)
		}
		s.mu.Unlock()
		if refused != "" {
			s.Conv(f.Conv, map[string]any{"type": "error", "code": refused,
				"message": "refused by the test", "retryable": true})
			s.notify()
			return
		}
		if !ok {
			s.Conv(f.Conv, map[string]any{"type": "error", "code": "not_assigned",
				"message": "not assigned", "retryable": false})
			s.notify()
			return
		}
		s.Conv(f.Conv, wire.Joined{Type: wire.TypeJoined, PeerID: PeerFor(f.Conv),
			ServerVersion: wire.ProtocolVersion, Name: "test agent", ConversationKind: g.Kind,
			Topic: &g.Title, CanSend: g.Writable(), Access: g.Access, HistoryFrom: g.HistoryFrom,
			Features: features, Encryption: enc})
	case wire.TypeSearch:
		var q wire.Search
		_ = json.Unmarshal(raw, &q)
		s.mu.Lock()
		s.controls = append(s.controls, raw)
		answer := s.OnSearch
		s.mu.Unlock()
		if answer != nil {
			if reply := answer(q); reply != nil {
				s.write(ws, reply)
			}
		}
	case wire.TypeClose:
		var f wire.CloseConv
		_ = json.Unmarshal(raw, &f)
		s.mu.Lock()
		delete(s.open, f.Conv)
		s.controls = append(s.controls, raw)
		s.mu.Unlock()
	case wire.TypeConv:
		var f wire.ConvFrame
		_ = json.Unmarshal(raw, &f)
		var inner struct {
			Type wire.Type `json:"type"`
			Conv string    `json:"conv"`
		}
		_ = json.Unmarshal(f.Frame, &inner)
		s.mu.Lock()
		s.frames[f.Conv] = append(s.frames[f.Conv], Frame{Wrapper: f.Conv, Inner: inner.Conv,
			Type: inner.Type, Raw: f.Frame})
		s.mu.Unlock()
	default:
		s.mu.Lock()
		s.controls = append(s.controls, raw)
		s.mu.Unlock()
	}
	s.notify()
}

func (s *Server) notify() {
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

func (s *Server) write(ws *websocket.Conn, v any) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = ws.WriteJSON(v)
}

func (s *Server) current() *websocket.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ws
}

// Send writes an unwrapped frame on the agent socket.
func (s *Server) Send(v any) {
	if ws := s.current(); ws != nil {
		s.write(ws, v)
	}
}

// Conv writes frame wrapped for conv.
func (s *Server) Conv(conv string, frame any) {
	inner, err := json.Marshal(frame)
	if err != nil {
		s.t.Errorf("agenttest: %v", err)
		return
	}
	s.Send(wire.ConvFrame{Type: wire.TypeConv, Conv: conv, Frame: inner})
}

// Msg delivers a message from peer into conv.
func (s *Server) Msg(conv, peer, text, cursor string) {
	m := wire.NewBroadcastMsg(peer, text, time.Now().UTC().Format(time.RFC3339Nano), nil, "", "", nil)
	m.Cursor = cursor
	m.ExternalID = "ext-" + cursor
	s.Conv(conv, m)
}

// Assign adds a grant and tells the client.
func (s *Server) Assign(g wire.Assignment) {
	s.mu.Lock()
	if _, ok := s.grants[g.Conv]; !ok {
		s.order = append(s.order, g.Conv)
	}
	s.grants[g.Conv] = g
	s.mu.Unlock()
	s.Send(wire.AssignmentChange{Type: wire.TypeAssigned, Item: g})
}

// Change replaces a grant, closes its session for reopening, and tells
// the client — in the order the spec gives.
func (s *Server) Change(g wire.Assignment) {
	s.mu.Lock()
	s.grants[g.Conv] = g
	delete(s.open, g.Conv)
	s.mu.Unlock()
	s.Send(wire.AssignmentChange{Type: wire.TypeChanged, Item: g})
	s.Send(wire.Closed{Type: wire.TypeClosed, Conv: g.Conv, Code: 1000, Reason: "access changed", Reopen: true})
}

// Revoke withdraws a grant.
func (s *Server) Revoke(conv, reason string) {
	s.mu.Lock()
	delete(s.grants, conv)
	for i, c := range s.order {
		if c == conv {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	delete(s.open, conv)
	s.mu.Unlock()
	s.Send(wire.Revoked{Type: wire.TypeRevoked, Conv: conv, Reason: reason})
}

// Drop ends the agent socket without a close frame.
func (s *Server) Drop() {
	s.mu.Lock()
	ws := s.ws
	s.ws = nil
	s.mu.Unlock()
	if ws != nil {
		ws.Close()
	}
}

// CloseAgent ends the agent socket with a close code.
func (s *Server) CloseAgent(code int, reason string) {
	ws := s.current()
	if ws == nil {
		return
	}
	s.writeMu.Lock()
	_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason),
		time.Now().Add(time.Second))
	s.writeMu.Unlock()
	s.Drop()
}

// CloseConv ends conv's session with a close code, keeping its grant.
func (s *Server) CloseConv(conv string, code int, reason string, reopen bool) {
	s.mu.Lock()
	delete(s.open, conv)
	s.mu.Unlock()
	s.Send(wire.Closed{Type: wire.TypeClosed, Conv: conv, Code: code, Reason: reason, Reopen: reopen})
}

// RefuseOpen answers every "open" for conv with code until it is called
// again with "".
func (s *Server) RefuseOpen(conv, code string) {
	s.mu.Lock()
	s.refuse[conv] = code
	s.mu.Unlock()
}

// Encrypt makes conv an encrypted session from its next "joined" on.
func (s *Server) Encrypt(conv string, enc *wire.Encryption) {
	s.mu.Lock()
	s.encryption[conv] = enc
	s.mu.Unlock()
}

// Opens counts the "open" frames conv has received, over every socket.
func (s *Server) Opens(conv string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opens[conv]
}

// Frames returns what the client sent inside conv.
func (s *Server) Frames(conv string) []Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Frame(nil), s.frames[conv]...)
}

// AllFrames returns every conversation's frames.
func (s *Server) AllFrames() map[string][]Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string][]Frame{}
	for k, v := range s.frames {
		out[k] = append([]Frame(nil), v...)
	}
	return out
}

// Controls returns the unwrapped frames the client sent besides "open".
func (s *Server) Controls() []json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]json.RawMessage(nil), s.controls...)
}

// Headers returns the request headers of every dial.
func (s *Server) Headers() []http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]http.Header(nil), s.headers...)
}

// Dials counts the upgrades so far.
func (s *Server) Dials() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dials
}

// IsOpen reports whether conv is open on the current socket.
func (s *Server) IsOpen(conv string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.open[conv]
}

// OpenCount counts the conversations open on the current socket.
func (s *Server) OpenCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.open)
}

// WaitFor polls cond until it holds or the timeout passes.
func (s *Server) WaitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-s.changed:
		case <-time.After(10 * time.Millisecond):
		}
	}
}
