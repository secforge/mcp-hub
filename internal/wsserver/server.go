package wsserver

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/agekey"
	"github.com/secforge/mcp-hub/internal/hublog"
	"github.com/secforge/mcp-hub/internal/hubsession"
	"github.com/secforge/mcp-hub/internal/sanitize"
	"github.com/secforge/mcp-hub/internal/wire"
)

// maxNameRunes bounds a peer's untrusted display name — long enough for any
// reasonable name, short enough to keep it from bloating logs/events.
const maxNameRunes = 64

// maxReconnectSecretRunes bounds a reconnectSecret — generous for any
// reasonable client-generated token, but bounded so a client can't bloat
// Session.secretToPeerID with arbitrarily large values.
const maxReconnectSecretRunes = 256

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
	// clientVersion is currently only logged (reserved for future
	// server-side compatibility decisions); a missing or unparseable "v" is
	// treated as version 1 — the permanent backward-compatible default for
	// any client (including plain websocket clients) that doesn't send one
	// at all.
	clientVersion := 1
	if v := r.URL.Query().Get("v"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			clientVersion = parsed
		}
	}
	if clientVersion != wire.ProtocolVersion {
		log.Printf("client for session %s connected with protocol version %d (server is %d)",
			sessionID, clientVersion, wire.ProtocolVersion)
	}

	name := sanitize.Text(r.URL.Query().Get("name"), maxNameRunes)
	agePublicKey := r.URL.Query().Get("agePublicKey")
	if agePublicKey != "" && !agekey.Valid(agePublicKey) {
		http.Error(w, "invalid agePublicKey", http.StatusBadRequest)
		return
	}
	reconnectSecret := r.URL.Query().Get("reconnectSecret")
	if len(reconnectSecret) > maxReconnectSecretRunes {
		http.Error(w, "reconnectSecret too long", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	h.serve(conn, sessionID, name, agePublicKey, reconnectSecret)
}

func (h *Handler) serve(conn *websocket.Conn, sessionID, name, agePublicKey, reconnectSecret string) {
	defer conn.Close()

	// Snapshot once, synchronously, before pingLoop is spawned — tests
	// reassign these package vars, and an unsynchronized background
	// goroutine reading them directly has no happens-before edge against a
	// later test's write, which the race detector (correctly) flags even
	// when this goroutine has logically already finished by then. Capturing
	// here, before `go p.pingLoop()`, uses the goroutine-creation
	// happens-before edge instead.
	snapPingPeriod, snapPongWait, snapWriteWait := pingPeriod, pongWait, writeWait

	var p *peer
	var peerID string
	done := make(chan struct{})
	defer close(done)

	logger, logErr := hublog.OpenSessionLog(sessionID)
	if logErr == nil {
		defer logger.Close()
	}

	session := h.manager.GetOrCreate(sessionID)
	var writeErr error
	_, reused := session.Join(reconnectSecret,
		func(id string) hubsession.Peer {
			peerID = id
			p = &peer{id: id, conn: conn, done: done, name: name, agePublicKey: agePublicKey, pingPeriod: snapPingPeriod, writeWait: snapWriteWait}
			return p
		},
		func(existingCount int) {
			writeErr = conn.WriteJSON(wire.NewJoined(peerID, existingCount, name, agePublicKey))
		},
	)
	if writeErr != nil {
		return
	}
	if logger != nil {
		logger.AppendJoined(peerID, name, agePublicKey, reused, reconnectSecret != "", time.Now().UTC().Format(time.RFC3339))
	}
	defer func() {
		if logger != nil {
			logger.AppendLeft(peerID, time.Now().UTC().Format(time.RFC3339))
		}
		if session.Leave(p) {
			h.manager.Remove(sessionID)
		}
	}()

	conn.SetReadDeadline(time.Now().Add(snapPongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(snapPongWait))
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
	id           string
	mu           sync.Mutex
	conn         *websocket.Conn
	done         chan struct{}
	name         string
	agePublicKey string
	pingPeriod   time.Duration
	writeWait    time.Duration
}

func (p *peer) ID() string           { return p.id }
func (p *peer) Name() string         { return p.name }
func (p *peer) AgePublicKey() string { return p.agePublicKey }

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
	ticker := time.NewTicker(p.pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// WriteControl may be called concurrently with the other Conn
			// write methods per the gorilla/websocket concurrency contract.
			if err := p.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(p.writeWait)); err != nil {
				return
			}
		case <-p.done:
			return
		}
	}
}
