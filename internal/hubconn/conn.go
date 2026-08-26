package hubconn

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

type Event struct {
	Kind    string
	PeerID  string
	Text    string
	TS      string
	Private bool
}

type Conn struct {
	ws            *websocket.Conn
	peerID        string
	serverVersion int
	expectedPeers int

	mu              sync.Mutex
	buffer          []Event
	closed          bool
	onActivity      func()
	peers           map[string]struct{}
	rosterSeen      int
	rosterAnnounced bool
}

// Dial connects to host+"/"+sessionID (e.g. "ws://localhost:8765" joining
// session "550e8400-..." dials "ws://localhost:8765/550e8400-..."), which
// auto-joins the session as part of the websocket handshake — no separate
// join message is sent. host is validated/normalized first (see
// normalizeHost) so a common mistake like using https:// or including a
// path fails with a clear message instead of an opaque dial error. Starts a
// background read loop on success.
func Dial(host, sessionID string) (*Conn, error) {
	base, err := normalizeHost(host)
	if err != nil {
		return nil, err
	}
	target := base + "/" + sessionID + "?v=" + strconv.Itoa(wire.ProtocolVersion)
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
		serverVersion: joined.ServerVersion,
		expectedPeers: joined.PeerCount,
		peers:         make(map[string]struct{}),
	}
	if c.expectedPeers == 0 {
		// Nothing to catch up on - the roster is complete right away, so
		// synthesize the notification immediately rather than waiting for
		// an event that will never arrive to trigger it.
		c.rosterAnnounced = true
		c.buffer = append(c.buffer, Event{Kind: "rosterComplete"})
	}
	go c.readLoop()
	return c, nil
}

func (c *Conn) PeerID() string { return c.peerID }

// ServerVersion is the wire.ProtocolVersion the server reported in "joined".
// Compare against wire.ProtocolVersion to tell if this client is behind.
func (c *Conn) ServerVersion() int { return c.serverVersion }

// ExpectedPeerCount is how many peers were already in the session at join
// time, as reported by the server's "joined" message — i.e. how many
// peerJoined events make up the initial roster catch-up.
func (c *Conn) ExpectedPeerCount() int { return c.expectedPeers }

// RosterComplete reports whether this connection has now seen every
// peerJoined event the server promised (via "joined"'s peerCount) for the
// roster that existed at join time. Once true, a "rosterComplete" event has
// also been buffered (see Drain/Peek) — this method is for an on-demand
// check; the buffered event is the actual notification.
func (c *Conn) RosterComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rosterAnnounced
}

// Peers returns the peerIds of everyone else currently known to be in the
// session, sorted for stable output. Built entirely from peerJoined/peerLeft
// events seen so far — since a newly joined peer is told the full existing
// roster on join (see the server's "roster on join" behavior), this is
// complete from shortly after Dial returns, not just for peers who joined
// after this connection did.
func (c *Conn) Peers() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.peers))
	for id := range c.peers {
		out = append(out, id)
	}
	sort.Strings(out)
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
			c.peers[ev.PeerID] = struct{}{}
			c.rosterSeen++
			if !c.rosterAnnounced && c.rosterSeen >= c.expectedPeers {
				c.rosterAnnounced = true
				c.buffer = append(c.buffer, Event{Kind: "rosterComplete"})
			}
		case "peerLeft":
			delete(c.peers, ev.PeerID)
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
		return Event{Kind: "peerJoined", PeerID: p.PeerID}, true
	case wire.TypePeerLeft:
		var p wire.PeerEvent
		if err := json.Unmarshal(raw, &p); err != nil || !wire.IsValidID(p.PeerID) {
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
