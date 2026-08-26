package hubconn

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/agekey"
	"github.com/secforge/mcp-hub/internal/wire"
)

type Event struct {
	Kind         string
	PeerID       string
	Text         string
	TS           string
	Private      bool
	Name         string
	AgePublicKey string
}

// PeerInfo is what's known about one other peer in the session.
type PeerInfo struct {
	ID           string
	Name         string
	AgePublicKey string
}

type Conn struct {
	ws            *websocket.Conn
	peerID        string
	name          string
	agePublicKey  string
	serverVersion int
	expectedPeers int

	mu              sync.Mutex
	buffer          []Event
	closed          bool
	onActivity      func()
	peers           map[string]PeerInfo
	rosterAnnounced bool
}

// DialOptions carries the optional, untrusted-to-everyone-else identity a
// client presents on connect.
type DialOptions struct {
	// Name is a free-text display name shown alongside logs and reported to
	// other peers. The server sanitizes it (control characters stripped,
	// length capped) before relaying it or writing it to the log — treat
	// whatever comes back in Conn.Name() as the authoritative value.
	Name string
	// AgePublicKey is an age (https://age-encryption.org) recipient string.
	// It is validated for correct bech32 format (agekey.Valid) — both here
	// and again server-side — but never parsed, decoded, or used
	// cryptographically by the hub in any way; it is only distributed to
	// other peers so they can encrypt to this one, entirely outside the
	// hub's involvement. It does NOT affect peerId reuse — see
	// ReconnectSecret — because it's broadcast to every other peer in the
	// session, so keying identity off it would let anyone who saw it
	// impersonate that peer on reconnect.
	AgePublicKey string
	// ReconnectSecret, if given, is never distributed to anyone — only this
	// client and the server ever see it. If a peer previously connected to
	// this same still-alive session with this exact secret, it is
	// reassigned that same peerId (see hubsession.Session.Join), as long as
	// that previous connection isn't still active; otherwise it's simply
	// remembered for a future reconnect. Any string works — a UUID, a
	// random token, whatever the caller wants to remember and present again
	// later.
	ReconnectSecret string
}

// Dial connects to host+"/"+sessionID (e.g. "ws://localhost:8765" joining
// session "550e8400-..." dials "ws://localhost:8765/550e8400-..."), which
// auto-joins the session as part of the websocket handshake — no separate
// join message is sent. host is validated/normalized first (see
// normalizeHost) so a common mistake like using https:// or including a
// path fails with a clear message instead of an opaque dial error. Starts a
// background read loop on success.
func Dial(host, sessionID string, opts DialOptions) (*Conn, error) {
	base, err := normalizeHost(host)
	if err != nil {
		return nil, err
	}
	if opts.AgePublicKey != "" && !agekey.Valid(opts.AgePublicKey) {
		return nil, fmt.Errorf("agePublicKey is not a validly formatted age public key")
	}
	target := base + "/" + sessionID + "?v=" + strconv.Itoa(wire.ProtocolVersion)
	if opts.Name != "" {
		target += "&name=" + url.QueryEscape(opts.Name)
	}
	if opts.AgePublicKey != "" {
		target += "&agePublicKey=" + url.QueryEscape(opts.AgePublicKey)
	}
	if opts.ReconnectSecret != "" {
		target += "&reconnectSecret=" + url.QueryEscape(opts.ReconnectSecret)
	}
	ws, _, err := websocket.DefaultDialer.Dial(target, nil)
	if err != nil {
		return nil, err
	}
	_, raw, err := ws.ReadMessage()
	if err != nil {
		ws.Close()
		return nil, err
	}
	var joined wire.Joined
	if err := json.Unmarshal(raw, &joined); err != nil {
		ws.Close()
		return nil, err
	}
	if !wire.IsValidID(joined.PeerID) {
		ws.Close()
		return nil, fmt.Errorf("server returned malformed peerId %q", joined.PeerID)
	}

	c := &Conn{
		ws:            ws,
		peerID:        joined.PeerID,
		name:          joined.Name,
		agePublicKey:  joined.AgePublicKey,
		serverVersion: joined.ServerVersion,
		expectedPeers: joined.PeerCount,
		peers:         make(map[string]PeerInfo),
	}
	go c.readLoop()
	return c, nil
}

func (c *Conn) PeerID() string { return c.peerID }

// Name is this connection's own display name, after server-side
// sanitization — empty if none was supplied.
func (c *Conn) Name() string { return c.name }

// AgePublicKey is this connection's own age public key, echoed back by the
// server — empty if none was supplied.
func (c *Conn) AgePublicKey() string { return c.agePublicKey }

// ServerVersion is the wire.ProtocolVersion the server reported in "joined".
// Compare against wire.ProtocolVersion to tell if this client is behind.
func (c *Conn) ServerVersion() int { return c.serverVersion }

// ExpectedPeerCount is how many peers were already in the session at join
// time, as reported by the server's "joined" message — i.e. how many
// peerJoined events make up the initial roster catch-up.
func (c *Conn) ExpectedPeerCount() int { return c.expectedPeers }

// RosterComplete reports whether the server's "rosterComplete" event — sent
// once it has finished delivering this peer's initial roster — has been
// seen yet. Once true, that event has also been buffered (see Drain/Peek) —
// this method is for an on-demand check; the buffered event is the actual
// notification.
func (c *Conn) RosterComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rosterAnnounced
}

// Peers returns everyone else currently known to be in the session, sorted
// by peerId for stable output. Built entirely from peerJoined/peerLeft
// events seen so far — since a newly joined peer is told the full existing
// roster on join (see the server's "roster on join" behavior), this is
// complete from shortly after Dial returns, not just for peers who joined
// after this connection did.
func (c *Conn) Peers() []PeerInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]PeerInfo, 0, len(c.peers))
	for _, info := range c.peers {
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

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
		switch ev.Kind {
		case "peerJoined":
			c.peers[ev.PeerID] = PeerInfo{ID: ev.PeerID, Name: ev.Name, AgePublicKey: ev.AgePublicKey}
		case "peerLeft":
			delete(c.peers, ev.PeerID)
		case "rosterComplete":
			c.rosterAnnounced = true
		}
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
		if err := json.Unmarshal(raw, &m); err != nil || !wire.IsValidID(m.PeerID) {
			return Event{}, false
		}
		return Event{Kind: "msg", PeerID: m.PeerID, Text: m.Text, TS: m.TS, Private: m.Private}, true
	case wire.TypeError:
		var e wire.Error
		if err := json.Unmarshal(raw, &e); err != nil {
			return Event{}, false
		}
		return Event{Kind: "error", Text: e.Message}, true
	case wire.TypePeerJoined:
		var p wire.PeerEvent
		if err := json.Unmarshal(raw, &p); err != nil || !wire.IsValidID(p.PeerID) {
			return Event{}, false
		}
		return Event{Kind: "peerJoined", PeerID: p.PeerID, Name: p.Name, AgePublicKey: p.AgePublicKey}, true
	case wire.TypePeerLeft:
		var p wire.PeerEvent
		if err := json.Unmarshal(raw, &p); err != nil || !wire.IsValidID(p.PeerID) {
			return Event{}, false
		}
		return Event{Kind: "peerLeft", PeerID: p.PeerID}, true
	case wire.TypeRosterComplete:
		return Event{Kind: "rosterComplete"}, true
	default:
		return Event{}, false
	}
}

func (c *Conn) Send(text string) error {
	return c.ws.WriteJSON(wire.NewOutgoingMsg(text))
}

// SendTo sends text privately to a single peer, identified by peerId. The
// server processes this asynchronously: a delivery failure (e.g. an unknown
// or departed peer) does not surface as a returned error here, but as a
// buffered "error" event picked up by a later Peek/Drain.
func (c *Conn) SendTo(text, peerID string) error {
	return c.ws.WriteJSON(wire.NewOutgoingDirectedMsg(text, peerID))
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
