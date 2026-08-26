package hubsession

import (
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/secforge/mcp-hub/internal/wire"
)

// Peer is anything that can receive broadcast events for a session.
type Peer interface {
	ID() string
	Deliver(event any)
	// Name and AgePublicKey are this peer's own sanitized/validated,
	// already-immutable values from when it connected — used to populate
	// the peerJoined events other peers (and this peer's own roster
	// catch-up) receive about it.
	Name() string
	AgePublicKey() string
}

type Session struct {
	id    string
	mu    sync.Mutex
	peers map[string]Peer
	// pubkeyToPeerID remembers which peerID an age public key was last
	// assigned, for the lifetime of this Session (i.e. only as long as the
	// channel stays alive — a peer that reconnects after everyone else has
	// left, tearing the session down, gets a fresh identity like anyone
	// else, since a brand new Session has no memory of the old one).
	pubkeyToPeerID map[string]string
}

func newSession(id string) *Session {
	return &Session{id: id, peers: make(map[string]Peer)}
}

// Join resolves the peerID to use (reusing the ID a previous, now-departed
// connection with the same agePublicKey used, if any — see
// pubkeyToPeerID — otherwise a fresh UUID), constructs the peer via
// makePeer, registers it, delivers it the current roster (one peerJoined
// per existing peer, terminated by a RosterComplete), and announces its own
// join to everyone else — all while holding the session lock for the
// entire sequence. That atomicity is what makes this safe: no other peer
// can Join or Leave in the middle of it, so the roster the new peer
// receives is exactly the set of peers still present when RosterComplete is
// sent, with no gap in which a peer from the snapshot could vanish (a
// phantom peerJoined with no way to correct it) or a concurrent joiner
// could be missed.
//
// makePeer runs first (so beforeVisible, wsserver's own code, and this
// method can all refer to the resolved peerID), then beforeVisible, if
// non-nil, runs under the same lock before the peer becomes visible to
// anyone else — e.g. to write a "joined" confirmation with an
// existing-peer count that's guaranteed consistent with the roster that
// follows.
func (s *Session) Join(agePublicKey string, makePeer func(peerID string) Peer, beforeVisible func(existingCount int)) Peer {
	s.mu.Lock()
	defer s.mu.Unlock()

	peerID := s.resolvePeerIDLocked(agePublicKey)
	existing := make([]Peer, 0, len(s.peers))
	for _, ep := range s.peers {
		existing = append(existing, ep)
	}
	p := makePeer(peerID)
	if beforeVisible != nil {
		beforeVisible(len(existing))
	}
	s.peers[peerID] = p
	if agePublicKey != "" {
		if s.pubkeyToPeerID == nil {
			s.pubkeyToPeerID = make(map[string]string)
		}
		s.pubkeyToPeerID[agePublicKey] = peerID
	}

	for _, ep := range existing {
		p.Deliver(wire.NewPeerJoined(ep.ID(), ep.Name(), ep.AgePublicKey()))
	}
	p.Deliver(wire.NewRosterComplete())
	s.broadcastExceptLocked(peerID, wire.NewPeerJoined(peerID, p.Name(), p.AgePublicKey()))
	return p
}

// resolvePeerIDLocked returns the peerID a joining connection should use.
// Called with s.mu already held.
func (s *Session) resolvePeerIDLocked(agePublicKey string) string {
	if agePublicKey != "" {
		if id, ok := s.pubkeyToPeerID[agePublicKey]; ok {
			if _, stillConnected := s.peers[id]; !stillConnected {
				return id
			}
		}
	}
	return uuid.NewString()
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

// DeliverTo sends event only to the peer identified by targetID. It errors
// (without delivering anything) if targetID is the sender itself, or if
// targetID is not currently a member of the session.
func (s *Session) DeliverTo(from Peer, targetID string, event any) error {
	if targetID == from.ID() {
		return fmt.Errorf("cannot send a private message to yourself")
	}
	s.mu.Lock()
	target, ok := s.peers[targetID]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("peer %q is not in this session", targetID)
	}
	target.Deliver(event)
	return nil
}

func (s *Session) broadcastExcept(exceptID string, event any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.broadcastExceptLocked(exceptID, event)
}

// broadcastExceptLocked is broadcastExcept for callers that already hold
// s.mu (namely Join, which can't re-lock a non-reentrant mutex).
func (s *Session) broadcastExceptLocked(exceptID string, event any) {
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
