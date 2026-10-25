package hubconn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

// Agent is an agent connection: one socket carrying every conversation the
// server has shared with this agent. Each opened conversation is an
// ordinary *Conn running over an agentConv, which reads only the frames
// addressed to its own conversation and writes only frames stamped with
// it — so the routing is decided in one place, here, and nothing above
// can pick a conversation for a frame.
type Agent struct {
	ws        *websocket.Conn
	pongWait  time.Duration
	writeWait time.Duration
	joined    wire.AgentJoined
	initial   []wire.Assignment

	writeMu sync.Mutex

	mu        sync.Mutex
	convs     map[string]*agentConv
	closed    bool
	closeCode int
	closeText string
	fatalText string
	// reconnectAfter is a "serverStopping" frame's estimate, kept for the
	// drop that follows it.
	reconnectAfter int
	done           chan struct{}
	// searches holds the requests awaiting an answer by id: searches and
	// conversation starts, each answered by its own id.
	searches  map[string]chan searchAnswer
	searchSeq int
	// startAccounts is the accounts this agent may start conversations
	// in, as the server last stated them.
	startAccounts []wire.StartAccount

	// Agent events are queued and handed to one handler in order, on a
	// goroutine of their own: the read loop must never wait on whatever
	// the handler does, or one slow reaction stalls every conversation.
	evMu      sync.Mutex
	evCond    *sync.Cond
	evQueue   []AgentEvent
	evHandler func(AgentEvent)
}

// AgentEvent is something the server said about the agent's set of
// conversations, or about the agent connection itself.
type AgentEvent struct {
	// Kind is the frame type: "assigned", "changed", "revoked", "closed",
	// "activity", "assignments", "error", "serverStopping" — or "down"
	// when the agent socket itself ended, which is always the last event.
	Kind   string
	Item   wire.Assignment
	Items  []wire.Assignment
	Conv   string
	Reason string
	Code   int
	Reopen bool
	Behind int
	// ErrCode is an "error" frame's code, such as bad_conv: the server
	// answers a malformed handle unwrapped, so it is never mistaken for a
	// real conversation.
	ErrCode string
}

// errAgentDown ends every open conversation when the agent socket does.
// Deliberately not a close code: a conversation's own close codes say
// what happened to that conversation, and the socket carrying all of them
// going away says nothing about any one of them.
var errAgentDown = errors.New("the agent connection carrying this conversation ended")

// errConvClosed is a write to a conversation whose session has ended.
var errConvClosed = errors.New("this conversation's session on the agent connection has ended")

// startAgent takes over ws after its first frame turned out to be
// "agentJoined", and reads the "assignments" frame that follows it.
func startAgent(ws *websocket.Conn, raw []byte, snapPongWait, snapWriteWait time.Duration) (*Agent, error) {
	var joined wire.AgentJoined
	if err := json.Unmarshal(raw, &joined); err != nil {
		ws.Close()
		return nil, err
	}
	if joined.AgentID == "" {
		ws.Close()
		return nil, fmt.Errorf("the agent handshake named no agent")
	}
	first, err := readFirstFrame(ws, snapPongWait)
	if err != nil {
		return nil, fmt.Errorf("no assignments from the server after the agent handshake: %w", err)
	}
	if wire.FrameType(first) != wire.TypeAssignments {
		ws.Close()
		return nil, fmt.Errorf("the agent handshake was followed by %q, not by its assignments",
			wire.FrameType(first))
	}
	var list wire.Assignments
	if err := json.Unmarshal(first, &list); err != nil {
		ws.Close()
		return nil, fmt.Errorf("unreadable assignments: %w", err)
	}
	a := &Agent{
		ws:        ws,
		pongWait:  snapPongWait,
		writeWait: snapWriteWait,
		joined:    joined,
		initial:   list.Items,
		convs:     map[string]*agentConv{},
		done:      make(chan struct{}),
		searches:  map[string]chan searchAnswer{},
	}
	if raw, ok := joined.Features["startConversation"]; ok {
		var f wire.StartConversationFeature
		if json.Unmarshal(raw, &f) == nil {
			a.startAccounts = f.Accounts
		}
	}
	a.evCond = sync.NewCond(&a.evMu)
	ws.SetReadDeadline(time.Now().Add(snapPongWait))
	ws.SetPingHandler(func(appData string) error {
		ws.SetReadDeadline(time.Now().Add(snapPongWait))
		// A conversation has no pings of its own, so the agent socket's
		// are what keeps each one's keepalive deadline moving.
		a.mu.Lock()
		convs := make([]*agentConv, 0, len(a.convs))
		for _, v := range a.convs {
			convs = append(convs, v)
		}
		a.mu.Unlock()
		for _, v := range convs {
			v.ping(appData)
		}
		return ws.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(snapWriteWait))
	})
	go a.readLoop()
	return a, nil
}

// AgentID is the agent's identity on the server.
func (a *Agent) AgentID() string { return string(a.joined.AgentID) }

// Name is the agent's display name as the server holds it.
func (a *Agent) Name() string { return a.joined.Name }

// ServerVersion is the server's protocol version.
func (a *Agent) ServerVersion() int { return a.joined.ServerVersion }

// Assignments is the list of conversations the server sent at connect.
// Later changes arrive as events.
func (a *Agent) Assignments() []wire.Assignment {
	return append([]wire.Assignment(nil), a.initial...)
}

// OnEvent installs the handler for agent events and starts handing them
// over, including any that arrived before it was installed. Called once.
func (a *Agent) OnEvent(f func(AgentEvent)) {
	a.evMu.Lock()
	if a.evHandler != nil {
		a.evMu.Unlock()
		return
	}
	a.evHandler = f
	a.evMu.Unlock()
	go a.dispatch()
}

func (a *Agent) dispatch() {
	for {
		a.evMu.Lock()
		for len(a.evQueue) == 0 {
			a.evCond.Wait()
		}
		ev := a.evQueue[0]
		a.evQueue = a.evQueue[1:]
		f := a.evHandler
		a.evMu.Unlock()
		f(ev)
		if ev.Kind == "down" {
			return
		}
	}
}

func (a *Agent) emit(ev AgentEvent) {
	a.evMu.Lock()
	a.evQueue = append(a.evQueue, ev)
	a.evMu.Unlock()
	a.evCond.Signal()
}

func (a *Agent) readLoop() {
	for {
		_, raw, err := a.ws.ReadMessage()
		if err != nil {
			a.fail(err)
			return
		}
		a.ws.SetReadDeadline(time.Now().Add(a.pongWait))
		switch wire.FrameType(raw) {
		case wire.TypeConv:
			var f wire.ConvFrame
			if err := json.Unmarshal(raw, &f); err != nil || f.Conv == "" {
				debugf("agent: unreadable conv frame dropped: %s", raw)
				continue
			}
			a.route(f.Conv, f.Frame)
		case wire.TypeAssigned, wire.TypeChanged:
			var f wire.AssignmentChange
			if err := json.Unmarshal(raw, &f); err != nil || f.Item.Conv == "" {
				debugf("agent: unreadable %s dropped: %s", wire.FrameType(raw), raw)
				continue
			}
			a.emit(AgentEvent{Kind: string(f.Type), Item: f.Item, Conv: f.Item.Conv})
		case wire.TypeAssignments:
			var f wire.Assignments
			if err := json.Unmarshal(raw, &f); err != nil {
				continue
			}
			a.emit(AgentEvent{Kind: "assignments", Items: f.Items})
		case wire.TypeRevoked:
			var f wire.Revoked
			if err := json.Unmarshal(raw, &f); err != nil || f.Conv == "" {
				continue
			}
			// The grant is gone, so its session is too, with the code a
			// revoked link's socket closes with: the conversation's own
			// "do not reconnect" logic then applies unchanged.
			a.end(f.Conv, &websocket.CloseError{Code: 4001, Text: f.Reason})
			a.emit(AgentEvent{Kind: "revoked", Conv: f.Conv, Reason: f.Reason})
		case wire.TypeClosed:
			var f wire.Closed
			if err := json.Unmarshal(raw, &f); err != nil || f.Conv == "" {
				continue
			}
			code := f.Code
			if code == 0 {
				code = websocket.CloseNormalClosure
			}
			a.end(f.Conv, &websocket.CloseError{Code: code, Text: f.Reason})
			a.emit(AgentEvent{Kind: "closed", Conv: f.Conv, Code: f.Code, Reason: f.Reason, Reopen: f.Reopen})
		case wire.TypeActivity:
			var f wire.Activity
			if err := json.Unmarshal(raw, &f); err != nil || f.Conv == "" {
				continue
			}
			a.emit(AgentEvent{Kind: "activity", Conv: f.Conv, Behind: f.Behind})
		case wire.TypeSearchResults:
			var f wire.SearchResults
			if err := json.Unmarshal(raw, &f); err != nil {
				debugf("agent: unreadable searchResults dropped: %s", raw)
				continue
			}
			a.answerSearch(f.ID, searchAnswer{results: f})
		case wire.TypeConversationStarted:
			var f wire.ConversationStarted
			if err := json.Unmarshal(raw, &f); err != nil || f.Conv == "" {
				debugf("agent: unreadable conversationStarted dropped: %s", raw)
				continue
			}
			a.answerSearch(f.ID, searchAnswer{started: f})
		case wire.TypeGroupStarted:
			var f wire.GroupStarted
			if err := json.Unmarshal(raw, &f); err != nil || f.Conv == "" {
				debugf("agent: unreadable groupStarted dropped: %s", raw)
				continue
			}
			a.answerSearch(f.ID, searchAnswer{group: f})
		case wire.TypeStartAccounts:
			var f wire.StartAccounts
			if err := json.Unmarshal(raw, &f); err != nil {
				debugf("agent: unreadable startAccounts dropped: %s", raw)
				continue
			}
			a.mu.Lock()
			a.startAccounts = f.Items
			a.mu.Unlock()
			a.emit(AgentEvent{Kind: "startAccounts"})
		case wire.TypeError:
			var e wire.Error
			if json.Unmarshal(raw, &e) == nil && e.ID != "" && a.answerSearch(e.ID, searchAnswer{
				err: &SearchError{Code: e.Code, Message: e.Message, Retryable: e.Retryable, Conv: e.Conv}}) {
				continue
			}
			ev, ok := decodeEvent(raw)
			if !ok {
				continue
			}
			if ev.Code != "" && !ev.Retryable && fatalErrorCodes[ev.Code] {
				a.mu.Lock()
				a.fatalText = fmt.Sprintf("%s (%s)", ev.Text, ev.Code)
				a.mu.Unlock()
			}
			a.emit(AgentEvent{Kind: "error", Reason: ev.Text, ErrCode: ev.Code})
		case wire.TypeServerStopping:
			ev, ok := decodeEvent(raw)
			if !ok {
				continue
			}
			a.mu.Lock()
			a.reconnectAfter = ev.ReconnectAfter
			a.mu.Unlock()
			a.emit(AgentEvent{Kind: "serverStopping"})
		default:
			// An unwrapped frame of a type this client does not know is
			// dropped, as any unknown frame is.
			debugf("agent: unknown frame dropped: %s", raw)
		}
	}
}

// route hands a conversation's frame to its session. A frame for a
// conversation that is not open here is dropped: delivering it anywhere
// else is exactly the misrouting this design exists to rule out.
func (a *Agent) route(conv string, frame []byte) {
	a.mu.Lock()
	v := a.convs[conv]
	a.mu.Unlock()
	if v == nil {
		debugf("agent: frame for conversation %q, which is not open here, dropped", conv)
		return
	}
	v.deliver(frame)
}

// end closes one conversation's session with err, if it is open.
func (a *Agent) end(conv string, err error) {
	a.mu.Lock()
	v := a.convs[conv]
	delete(a.convs, conv)
	a.mu.Unlock()
	if v != nil {
		v.finish(err, false)
	}
}

func (a *Agent) fail(err error) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	var ce *websocket.CloseError
	if errors.As(err, &ce) {
		a.closeCode, a.closeText = ce.Code, ce.Text
	}
	convs := a.convs
	a.convs = map[string]*agentConv{}
	a.mu.Unlock()
	for _, v := range convs {
		v.finish(errAgentDown, false)
	}
	close(a.done)
	a.emit(AgentEvent{Kind: "down", Code: a.closeCode, Reason: a.closeText})
}

// Open starts one conversation's session and returns it as an ordinary
// connection. key is the conversation's encryption key, nil for none.
func (a *Agent) Open(conv string, key []byte) (*Conn, error) {
	if conv == "" {
		return nil, fmt.Errorf("no conversation named")
	}
	v := &agentConv{
		agent: a,
		conv:  conv,
		ready: make(chan struct{}, 1),
		done:  make(chan struct{}),
		kick:  make(chan struct{}, 1),
	}
	a.mu.Lock()
	switch {
	case a.closed:
		a.mu.Unlock()
		return nil, errAgentDown
	case a.convs[conv] != nil:
		a.mu.Unlock()
		return nil, fmt.Errorf("conversation %q is already open on this agent connection", conv)
	}
	a.convs[conv] = v
	a.mu.Unlock()
	if err := a.writeFrame(wire.OpenConv{Type: wire.TypeOpen, Conv: conv}); err != nil {
		a.forget(v)
		return nil, err
	}
	raw, err := readFirstFrameWithin(v, convOpenWait)
	if err != nil {
		v.Close()
		return nil, err
	}
	c, err := startConn(v, raw, a.pongWait, a.writeWait, confirmReminderInterval, key)
	if err != nil {
		v.Close()
		return nil, err
	}
	return c, nil
}

// convOpenWait is how long an opened conversation's "joined" may take.
// Longer than a socket's own handshake wait: an agent opens many
// conversations at once on one socket, and a server answering them one
// after another can take seconds each — a Teams conversation's open is
// calls to Microsoft's API — so the last in line wait for all the others.
var convOpenWait = 30 * time.Second

// forget removes v, if it is still the session registered for its
// conversation.
func (a *Agent) forget(v *agentConv) {
	a.mu.Lock()
	if a.convs[v.conv] == v {
		delete(a.convs, v.conv)
	}
	a.mu.Unlock()
}

func (a *Agent) writeFrame(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return a.writeRaw(raw)
}

func (a *Agent) writeRaw(raw []byte) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	_ = a.ws.SetWriteDeadline(time.Now().Add(a.writeWait))
	return a.ws.WriteMessage(websocket.TextMessage, raw)
}

// Connected reports whether the agent socket is still open.
func (a *Agent) Connected() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return !a.closed
}

// Done is closed when the agent socket ends.
func (a *Agent) Done() <-chan struct{} { return a.done }

// PermanentFailure reports that reconnecting the agent cannot succeed, and
// why, by the same rules a single conversation's connection uses.
func (a *Agent) PermanentFailure() (string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if note, ok := relayCloseNotes[a.closeCode]; ok {
		return strings.TrimSpace(strings.Trim(note, " ()")), true
	}
	if a.fatalText != "" {
		return a.fatalText, true
	}
	return "", false
}

// DisconnectNote says why the agent socket ended, where more than "it is
// gone" is known.
func (a *Agent) DisconnectNote() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if note, ok := relayCloseNotes[a.closeCode]; ok {
		return note
	}
	if a.closeCode != 0 && a.closeCode != websocket.CloseNormalClosure && a.closeCode != websocket.CloseGoingAway {
		return fmt.Sprintf(" (connection closed, code %d)", a.closeCode)
	}
	return ""
}

// SuggestedReconnectDelay is the server's announced restart estimate with
// jitter, or zero when none was announced.
func (a *Agent) SuggestedReconnectDelay() time.Duration {
	a.mu.Lock()
	secs := a.reconnectAfter
	a.mu.Unlock()
	if secs <= 0 {
		return 0
	}
	base := time.Duration(secs) * time.Second
	return base + time.Duration(rand.Float64()*reconnectJitter*float64(base))
}

// Close ends the agent socket and every conversation on it.
func (a *Agent) Close() error {
	a.writeMu.Lock()
	writeErr := a.ws.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client disconnect"),
		time.Now().Add(a.writeWait))
	a.writeMu.Unlock()
	if writeErr == nil {
		time.Sleep(closeFlushGrace)
	}
	closeErr := a.ws.Close()
	a.fail(&websocket.CloseError{Code: websocket.CloseNormalClosure, Text: "client disconnect"})
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

// agentConv is one conversation's transport on an agent socket. It reads
// only frames the agent routed to its conversation, and stamps that
// conversation on every frame it writes.
//
// Routed frames wait in a queue of their own that never blocks the agent
// socket: one conversation not reading must not stop the others' frames,
// nor the pings that keep every conversation's deadline moving.
type agentConv struct {
	agent *Agent
	conv  string
	// ready signals that the queue has frames.
	ready chan struct{}
	done  chan struct{}
	// kick wakes a blocked read when its deadline moves.
	kick chan struct{}

	mu          sync.Mutex
	deadline    time.Time
	pingHandler func(string) error
	err         error
	closeSent   bool
	queue       [][]byte
	queuedBytes int
}

// maxConvBacklog bounds what one conversation may hold unread in its
// queue. Past it the conversation is ended as abandoned for its backlog,
// and is opened again rather than holding more.
const maxConvBacklog = 64 << 20

// errConvBacklog ends a conversation that stopped reading.
var errConvBacklog = errors.New("the conversation stopped reading while more than 64 MB arrived for it")

func (v *agentConv) deliver(frame []byte) {
	v.mu.Lock()
	if v.err != nil {
		v.mu.Unlock()
		return
	}
	if v.queuedBytes+len(frame) > maxConvBacklog {
		v.mu.Unlock()
		v.agent.forget(v)
		v.finish(errConvBacklog, true)
		return
	}
	v.queue = append(v.queue, frame)
	v.queuedBytes += len(frame)
	v.mu.Unlock()
	select {
	case v.ready <- struct{}{}:
	default:
	}
}

// pop takes the oldest queued frame, if there is one.
func (v *agentConv) pop() ([]byte, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.queue) == 0 {
		return nil, false
	}
	frame := v.queue[0]
	v.queue[0] = nil
	v.queue = v.queue[1:]
	v.queuedBytes -= len(frame)
	return frame, true
}

// finish ends the session with err as what reads return from now on.
// sendClose tells the server too, which is only wanted when this side
// is the one ending it.
func (v *agentConv) finish(err error, sendClose bool) {
	v.mu.Lock()
	if v.err != nil {
		v.mu.Unlock()
		return
	}
	v.err = err
	send := sendClose && !v.closeSent
	v.closeSent = v.closeSent || sendClose
	v.mu.Unlock()
	close(v.done)
	if send {
		_ = v.agent.writeFrame(wire.CloseConv{Type: wire.TypeClose, Conv: v.conv})
	}
}

func (v *agentConv) ping(appData string) {
	v.mu.Lock()
	h := v.pingHandler
	v.mu.Unlock()
	if h != nil {
		_ = h(appData)
	}
}

// agentConvTimeout is what a read returns when its deadline passes, the
// same kind of error a websocket's own read deadline produces.
type agentConvTimeout struct{}

func (agentConvTimeout) Error() string   { return "i/o timeout" }
func (agentConvTimeout) Timeout() bool   { return true }
func (agentConvTimeout) Temporary() bool { return true }

var _ net.Error = agentConvTimeout{}

func (v *agentConv) ReadMessage() (int, []byte, error) {
	for {
		// Frames that arrived before the session ended are still read in
		// order before the ending is.
		if raw, ok := v.pop(); ok {
			return websocket.TextMessage, raw, nil
		}
		v.mu.Lock()
		deadline := v.deadline
		v.mu.Unlock()
		var timeout <-chan time.Time
		var timer *time.Timer
		if !deadline.IsZero() {
			wait := time.Until(deadline)
			if wait <= 0 {
				return 0, nil, agentConvTimeout{}
			}
			timer = time.NewTimer(wait)
			timeout = timer.C
		}
		raw, err, again := v.await(timeout)
		if timer != nil {
			timer.Stop()
		}
		if !again {
			if err != nil {
				return 0, nil, err
			}
			return websocket.TextMessage, raw, nil
		}
	}
}

// await waits for one frame, the session's end, the deadline, or the
// deadline moving — the last of which asks the caller to wait again.
func (v *agentConv) await(timeout <-chan time.Time) (raw []byte, err error, again bool) {
	select {
	case <-v.ready:
		return nil, nil, true
	case <-v.done:
		if raw, ok := v.pop(); ok {
			return raw, nil, false
		}
		v.mu.Lock()
		err := v.err
		v.mu.Unlock()
		return nil, err, false
	case <-timeout:
		return nil, agentConvTimeout{}, false
	case <-v.kick:
		return nil, nil, true
	}
}

func (v *agentConv) WriteJSON(x any) error {
	select {
	case <-v.done:
		return errConvClosed
	default:
	}
	inner, err := json.Marshal(x)
	if err != nil {
		return err
	}
	raw, err := wire.WrapConv(v.conv, inner)
	if err != nil {
		return err
	}
	return v.agent.writeRaw(raw)
}

// WriteControl carries a close as the conversation's "close" frame. Pings
// and pongs belong to the agent socket, not to a conversation.
func (v *agentConv) WriteControl(messageType int, _ []byte, _ time.Time) error {
	if messageType != websocket.CloseMessage {
		return nil
	}
	select {
	case <-v.done:
		return errConvClosed
	default:
	}
	v.mu.Lock()
	already := v.closeSent
	v.closeSent = true
	v.mu.Unlock()
	if already {
		return nil
	}
	return v.agent.writeFrame(wire.CloseConv{Type: wire.TypeClose, Conv: v.conv})
}

func (v *agentConv) SetReadDeadline(t time.Time) error {
	v.mu.Lock()
	v.deadline = t
	v.mu.Unlock()
	select {
	case v.kick <- struct{}{}:
	default:
	}
	return nil
}

func (v *agentConv) SetWriteDeadline(time.Time) error { return nil }

func (v *agentConv) SetReadLimit(int64) {}

func (v *agentConv) SetPingHandler(h func(string) error) {
	v.mu.Lock()
	v.pingHandler = h
	v.mu.Unlock()
}

// Close ends this conversation's session here and tells the server to stop
// delivering it; the grant stays.
func (v *agentConv) Close() error {
	v.agent.forget(v)
	v.finish(&websocket.CloseError{Code: websocket.CloseNormalClosure, Text: "client disconnect"}, true)
	return nil
}

// searchWait bounds how long a search waits for its answer.
var searchWait = 30 * time.Second

type searchAnswer struct {
	results wire.SearchResults
	started wire.ConversationStarted
	group   wire.GroupStarted
	err     error
}

// SearchError is the server's refusal of a search.
type SearchError struct {
	Code      string
	Message   string
	Retryable bool
	// Conv is the conversation a refused start is about, where one exists.
	Conv string
}

func (e *SearchError) Error() string {
	if e.Message == "" {
		return e.Code
	}
	return fmt.Sprintf("%s (%s)", e.Message, e.Code)
}

// SearchLimit is the most results one search returns, and whether the
// server offers search at all.
func (a *Agent) SearchLimit() (int, bool) {
	raw, ok := a.joined.Features["search"]
	if !ok {
		return 0, false
	}
	var f wire.SearchFeature
	if err := json.Unmarshal(raw, &f); err != nil || f.MaxLimit <= 0 {
		return 20, true
	}
	return f.MaxLimit, true
}

// Search sends q and waits for its answer. The id is set here, so an
// answer is only ever taken by the search that asked for it.
func (a *Agent) Search(ctx context.Context, q wire.Search) (wire.SearchResults, error) {
	if _, ok := a.SearchLimit(); !ok {
		return wire.SearchResults{}, fmt.Errorf("this server does not offer search across conversations")
	}
	ch := make(chan searchAnswer, 1)
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return wire.SearchResults{}, errAgentDown
	}
	a.searchSeq++
	q.ID = fmt.Sprintf("search-%d", a.searchSeq)
	a.searches[q.ID] = ch
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.searches, q.ID)
		a.mu.Unlock()
	}()
	q.Type = wire.TypeSearch
	if err := a.writeFrame(q); err != nil {
		return wire.SearchResults{}, err
	}
	timer := time.NewTimer(searchWait)
	defer timer.Stop()
	select {
	case ans := <-ch:
		return ans.results, ans.err
	case <-a.done:
		return wire.SearchResults{}, errAgentDown
	case <-ctx.Done():
		return wire.SearchResults{}, ctx.Err()
	case <-timer.C:
		return wire.SearchResults{}, fmt.Errorf("the server did not answer the search within %s", searchWait)
	}
}

// answerSearch hands an answer to the search waiting under id, reporting
// whether one was.
func (a *Agent) answerSearch(id string, ans searchAnswer) bool {
	a.mu.Lock()
	ch := a.searches[id]
	delete(a.searches, id)
	a.mu.Unlock()
	if ch == nil {
		return false
	}
	ch <- ans
	return true
}

// startWait bounds how long a conversation start waits for its answer:
// the server creates the chat in the platform first.
var startWait = 60 * time.Second

// StartAccounts is the accounts this agent may start conversations in.
func (a *Agent) StartAccounts() []wire.StartAccount {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]wire.StartAccount(nil), a.startAccounts...)
}

// StartConversation asks the server to open a one-to-one conversation
// with contact in account's name and waits for its answer. A refusal comes
// back as a *SearchError carrying the server's code.
func (a *Agent) StartConversation(ctx context.Context, account, contact string) (wire.ConversationStarted, error) {
	ch := make(chan searchAnswer, 1)
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return wire.ConversationStarted{}, errAgentDown
	}
	a.searchSeq++
	id := fmt.Sprintf("start-%d", a.searchSeq)
	a.searches[id] = ch
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.searches, id)
		a.mu.Unlock()
	}()
	if err := a.writeFrame(wire.StartConversation{Type: wire.TypeStartConversation, ID: id,
		Account: account, Contact: contact}); err != nil {
		return wire.ConversationStarted{}, err
	}
	timer := time.NewTimer(startWait)
	defer timer.Stop()
	select {
	case ans := <-ch:
		return ans.started, ans.err
	case <-a.done:
		return wire.ConversationStarted{}, errAgentDown
	case <-ctx.Done():
		return wire.ConversationStarted{}, ctx.Err()
	case <-timer.C:
		return wire.ConversationStarted{}, fmt.Errorf("the server did not answer within %s — the "+
			"conversation may or may not have been started; hub_list_connections shows whether it "+
			"is shared with this agent now", startWait)
	}
}

// StartGroup asks the server to create a new group chat with contacts in
// account's name and waits for its answer. id is the caller's: repeating
// it gives the chat already created for it rather than a second one. A
// refusal comes back as a *SearchError carrying the server's code.
func (a *Agent) StartGroup(ctx context.Context, id, account string, contacts []string, topic string) (wire.GroupStarted, error) {
	ch := make(chan searchAnswer, 1)
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return wire.GroupStarted{}, errAgentDown
	}
	if _, busy := a.searches[id]; busy {
		a.mu.Unlock()
		return wire.GroupStarted{}, fmt.Errorf("a start with request id %q is still waiting for its answer", id)
	}
	a.searches[id] = ch
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		if a.searches[id] == ch {
			delete(a.searches, id)
		}
		a.mu.Unlock()
	}()
	if err := a.writeFrame(wire.StartGroup{Type: wire.TypeStartGroup, ID: id, Account: account,
		Contacts: contacts, Topic: topic}); err != nil {
		return wire.GroupStarted{}, err
	}
	timer := time.NewTimer(startWait)
	defer timer.Stop()
	select {
	case ans := <-ch:
		return ans.group, ans.err
	case <-a.done:
		return wire.GroupStarted{}, errAgentDown
	case <-ctx.Done():
		return wire.GroupStarted{}, ctx.Err()
	case <-timer.C:
		return wire.GroupStarted{}, errStartUnanswered
	}
}

// errStartUnanswered is a start the server did not answer in time: it
// may or may not have happened.
var errStartUnanswered = fmt.Errorf("the server did not answer within %s", startWait)

// IsStartUnanswered reports whether err is a start left without an answer.
func IsStartUnanswered(err error) bool { return errors.Is(err, errStartUnanswered) }

// IsAgentDown reports whether err is the agent connection going down.
func IsAgentDown(err error) bool { return errors.Is(err, errAgentDown) }
