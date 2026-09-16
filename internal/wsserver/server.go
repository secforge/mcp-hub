package wsserver

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/agekey"
	"github.com/secforge/mcp-hub/internal/hublog"
	"github.com/secforge/mcp-hub/internal/hubsession"
	"github.com/secforge/mcp-hub/internal/sanitize"
	"github.com/secforge/mcp-hub/internal/wire"
)

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

// maxReadMessageBytes bounds a single incoming websocket frame — plain text
// chatter is nowhere near this, but an attachment's base64 payload (see
// wire.Attachment; clients cap raw bytes at wire.MaxAttachmentRawBytes,
// currently 32MB, base64 inflates that by ~33%) plus JSON envelope
// overhead needs real headroom above that raw figure. Without this,
// gorilla/websocket has no default cap of its own, so an unbounded frame
// from a malicious client would be read entirely into memory. 48MB —
// matches chat-relay's own coordinated whole-frame cap (44MB) with a
// little extra headroom, rather than deriving it independently; the two
// caps were raised together for the same reason (arbitrary binary
// attachments, not just images) and there's no benefit to them disagreeing.
const maxReadMessageBytes = 48 * 1024 * 1024

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// Ping/pong keepalive periods. Vars (not consts) so tests can shorten them.
// pongWait is deliberately well over 3x pingPeriod — see hubconn.pongWait's
// doc comment for why a tight margin here false-triggers on ordinary
// jitter (a delayed ping reads as a dead client) rather than only on an
// actually-dead connection.
var (
	pingPeriod = 30 * time.Second
	pongWait   = 100 * time.Second
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

// Manager exposes the session manager so other in-process protocol
// handlers (see internal/httpmcp) can join/interact with the exact same
// hubsession.Session objects this websocket handler uses, instead of a
// second, disconnected set of sessions.
func (h *Handler) Manager() *hubsession.Manager {
	return h.manager
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

	name := sanitize.Text(r.URL.Query().Get("name"), MaxNameRunes)
	agePublicKey := r.URL.Query().Get("agePublicKey")
	if agePublicKey != "" && !agekey.Valid(agePublicKey) {
		http.Error(w, "invalid agePublicKey", http.StatusBadRequest)
		return
	}
	reconnectSecret := r.URL.Query().Get("reconnectSecret")
	// Runes, as the constant says. len() counts BYTES, so a secret in
	// German or CJK was refused at roughly a third of the documented
	// limit — a limit that means something different depending on the
	// language it is written in.
	if utf8.RuneCountInString(reconnectSecret) > MaxReconnectSecretRunes {
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
	_, reused := session.Join(reconnectSecret,
		func(id string) hubsession.Peer {
			peerID = id
			p = &peer{id: id, conn: conn, done: done, name: name, agePublicKey: agePublicKey,
				pingPeriod: snapPingPeriod, writeWait: snapWriteWait,
				out:      make(chan any, outboundQueue),
				closing:  make(chan closeRequest, 1),
				finished: make(chan struct{}),
			}
			go p.writeLoop()
			return p
		},
		func() {
			// QUEUED like everything else. Written here, this was a second
			// writer on a socket that already had one, which is both a
			// race and — two frames interleaved — a protocol violation.
			// Ordering still holds: the roster Join delivers next goes
			// into the same queue, behind this.
			p.Deliver(wire.NewJoined(peerID, name, agePublicKey))
		},
	)
	if logger != nil {
		logger.AppendJoined(peerID, name, agePublicKey, reused, reconnectSecret != "", time.Now().UTC().Format(time.RFC3339))
	}
	defer func() {
		// Stops the writer goroutine; safe to call more than once.
		p.closeOut()
		if logger != nil {
			logger.AppendLeft(peerID, time.Now().UTC().Format(time.RFC3339))
		}
		if session.Leave(p) {
			h.manager.Remove(sessionID)
		}
	}()

	conn.SetReadLimit(maxReadMessageBytes)
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
			session.Broadcast(p, wire.NewBroadcastMsg(peerID, m.Text, ts, m.Attachments, m.Format, m.ReplyTo, m.Mentions))
			continue
		}

		if err := session.DeliverTo(p, m.To, wire.NewDirectedMsg(peerID, m.Text, ts, m.Attachments, m.Format, m.ReplyTo, m.Mentions)); err != nil {
			p.Deliver(wire.NewError(err.Error()))
			continue
		}
		if logger != nil {
			logger.AppendDirected(peerID, m.To, m.Text, ts)
		}
	}
}

// outboundQueue bounds what one peer may have waiting to be written.
//
// A peer that has stopped reading is the case this exists for: TCP stops
// accepting, the write blocks, and before this every other peer's
// delivery queued up behind it — while the SESSION LOCK was held, so the
// whole conversation stopped for one dead socket. The queue turns "every
// peer waits for the slowest" into "the slowest peer loses its own
// messages", which is the trade a chat wants.
//
// Deep enough to absorb an ordinary burst, shallow enough that a peer
// which has genuinely stopped is disconnected rather than accumulating a
// backlog nobody will ever read — and what a dropped peer misses is
// recoverable: the server holds the messages, and catch-up walks them.
const outboundQueue = 64

// closeRequest is the END OF THE QUEUE, not an action taken beside it.
//
// Closing the socket directly let a close overtake frames already queued
// for that peer — so a peer could learn the connection had ended and
// never learn why, because the error frame explaining it was still
// waiting. Queued, it is written after them, by the same goroutine, in
// order. It is also what removes the send-on-closed-channel panic: with
// the close being an item, nothing ever closes the channel items are sent
// on.
type closeRequest struct {
	code   int
	reason string
}

type peer struct {
	id           string
	conn         *websocket.Conn
	done         chan struct{}
	name         string
	agePublicKey string
	pingPeriod   time.Duration
	writeWait    time.Duration
	// out carries everything this peer is sent to the single goroutine
	// that writes it — events, the joined frame, errors. NEVER CLOSED: a
	// closed channel is what turned a delivery racing a teardown into a
	// panic, and the close travels through the queue instead (see
	// closeRequest).
	out chan any
	// closing carries the one close request, and is separate from out so
	// that a peer whose queue is full can still be told to go away — a
	// full queue is exactly when that has to work.
	closing chan closeRequest
	// finished is closed by writeLoop when it has written everything it
	// is going to and shut the socket. Teardown waits on THIS, not on a
	// flush: a barrier enqueued after a close is refused and returns at
	// once, which reads as "nothing left to wait for" while the close is
	// still unwritten.
	finished  chan struct{}
	closeOnce sync.Once
}

func (p *peer) ID() string           { return p.id }
func (p *peer) Name() string         { return p.name }
func (p *peer) AgePublicKey() string { return p.agePublicKey }

// Deliver hands an event to this peer's writer goroutine and returns
// immediately. It never touches the network, so a caller holding a
// session lock cannot be stalled by a socket.
//
// A full queue means this peer has stopped consuming: it is disconnected
// rather than served, because the alternative is an unbounded buffer for
// a reader that is not reading. Nothing is lost that cannot be recovered
// — the server still holds the messages and catch-up walks them — and a
// peer told nothing would sit there looking alive.
func (p *peer) Deliver(event any) {
	select {
	case p.out <- event:
	default:
		// A full queue means this peer has stopped reading. Blocking is
		// the bug this queue exists to remove and dropping is silent, so
		// it is closed: a close is the one signal a peer that has stopped
		// reading cannot miss.
		p.requestClose(websocket.CloseTryAgainLater, "outbound queue full")
	}
}

// requestClose queues this peer's close, once. Everything else — a full
// queue, a failed write, the session superseding this identity, serve
// returning — goes through here, so there is exactly one path that ends a
// connection and it is ordered against everything already queued.
func (p *peer) requestClose(code int, reason string) {
	p.closeOnce.Do(func() {
		p.closing <- closeRequest{code: code, reason: reason}
	})
}

// closeOut ends this peer's outbound side and waits for the writer to
// have finished — not for a flush, and not merely for the request to be
// queued. The socket is shut by the writer itself, which is what makes
// the blocked ReadMessage in serve return and run the ordinary teardown.
func (p *peer) closeOut() {
	p.requestClose(websocket.CloseNormalClosure, "")
	<-p.finished
}

// writeLoop is the ONLY place this peer's socket is written for events.
// One goroutine, so writes cannot interleave, and a deadline on each so a
// wedged connection ends instead of holding this goroutine forever.
func (p *peer) writeLoop() {
	defer close(p.finished)
	for {
		select {
		case event := <-p.out:
			if !p.write(event) {
				p.shutdown(websocket.CloseAbnormalClosure, "write failed")
				return
			}
		case cr := <-p.closing:
			// Everything already queued goes out FIRST: the close is the
			// end of this peer's stream, not a jump to the front of it.
			for {
				select {
				case event := <-p.out:
					if !p.write(event) {
						p.shutdown(websocket.CloseAbnormalClosure, "write failed")
						return
					}
					continue
				default:
				}
				break
			}
			p.shutdown(cr.code, cr.reason)
			return
		}
	}
}

// write is the ONLY place an application frame reaches this socket, which
// is what makes ordering a property of the code rather than of timing. A
// write mutex would have made concurrent writes safe and still let them
// interleave in the wrong order — two interleaved frames are a protocol
// violation, not just a race — so there is no mutex here and no second
// writer for one to guard.
func (p *peer) write(event any) bool {
	_ = p.conn.SetWriteDeadline(time.Now().Add(p.writeWait))
	return p.conn.WriteJSON(event) == nil
}

// shutdown sends the close frame and shuts the socket. Called only from
// writeLoop, on its way out.
func (p *peer) shutdown(code int, reason string) {
	_ = p.conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(code, reason), time.Now().Add(p.writeWait))
	_ = p.conn.Close()
}

// Close implements hubsession.Peer — see its doc comment on why this must
// not (and does not) touch Session state itself. Sends a close frame
// (best-effort; a write failure here just means the connection was
// already gone) and closes the underlying socket, which makes the
// blocked ReadMessage call in serve's loop return an error and run its
// own normal teardown (session.Leave, log append) on its own goroutine.
func (p *peer) Close(code int, reason string) {
	// Queued, not written here, and deliberately NOT waited for: this is
	// called from Join with the session lock held, and waiting for a
	// socket under that lock is the stall this whole queue exists to
	// prevent. serve's own teardown waits.
	p.requestClose(code, reason)
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
