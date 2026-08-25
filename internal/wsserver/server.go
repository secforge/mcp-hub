package wsserver

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/hublog"
	"github.com/secforge/mcp-hub/internal/hubsession"
	"github.com/secforge/mcp-hub/internal/wire"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// Ping/pong keepalive periods. Vars (not consts) so tests can shorten them.
var (
	pingPeriod = 30 * time.Second
	pongWait   = 40 * time.Second
	writeWait  = 10 * time.Second
)

type Handler struct {
	manager *hubsession.Manager
	mux     *http.ServeMux
}

// NewHandler returns an http.Handler that upgrades to a websocket at
// "/{sessionId}" (any single-segment path whose value is a valid UUID) and
// immediately joins that session — auto-join, no separate join message. An
// invalid sessionId is rejected with a plain HTTP 400 before any websocket
// upgrade is attempted.
func NewHandler() *Handler {
	h := &Handler{manager: hubsession.NewManager()}
	mux := http.NewServeMux()
	mux.HandleFunc("/{sessionId}", h.handleUpgrade)
	h.mux = mux
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) handleUpgrade(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")
	if !wire.IsValidID(sessionID) {
		http.Error(w, "invalid sessionId", http.StatusBadRequest)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	h.serve(conn, sessionID)
}

func (h *Handler) serve(conn *websocket.Conn, sessionID string) {
	defer conn.Close()

	peerID := uuid.NewString()
	p := &peer{id: peerID, conn: conn, done: make(chan struct{})}
	defer close(p.done)

	logger, logErr := hublog.OpenSessionLog(sessionID)
	if logErr == nil {
		defer logger.Close()
	}

	session := h.manager.GetOrCreate(sessionID)
	if err := conn.WriteJSON(wire.NewJoined(peerID)); err != nil {
		return
	}
	if logger != nil {
		logger.AppendJoined(peerID, time.Now().UTC().Format(time.RFC3339))
	}
	session.Join(p)
	defer func() {
		if logger != nil {
			logger.AppendLeft(peerID, time.Now().UTC().Format(time.RFC3339))
		}
		if session.Leave(p) {
			h.manager.Remove(sessionID)
		}
	}()

	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	go p.pingLoop()

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
		if m.To == "" {
			if logger != nil {
				logger.Append(peerID, m.Text, ts)
			}
			session.Broadcast(p, wire.NewBroadcastMsg(peerID, m.Text, ts))
			continue
		}

		if err := session.DeliverTo(p, m.To, wire.NewDirectedMsg(peerID, m.Text, ts)); err != nil {
			conn.WriteJSON(wire.NewError(err.Error()))
			continue
		}
		if logger != nil {
			logger.AppendDirected(peerID, m.To, m.Text, ts)
		}
	}
}

type peer struct {
	id   string
	mu   sync.Mutex
	conn *websocket.Conn
	done chan struct{}
}

func (p *peer) ID() string { return p.id }

func (p *peer) Deliver(event any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = p.conn.WriteJSON(event)
}

// pingLoop periodically pings the connection until either a write fails
// (the connection is dead) or done is closed (serve returned normally). A
// missing pong is detected via the read deadline set in serve, which causes
// the blocking ReadMessage call there to fail and return.
func (p *peer) pingLoop() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// WriteControl may be called concurrently with the other Conn
			// write methods per the gorilla/websocket concurrency contract.
			if err := p.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				return
			}
		case <-p.done:
			return
		}
	}
}
