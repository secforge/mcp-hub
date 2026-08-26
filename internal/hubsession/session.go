package hubsession

import (
	"fmt"
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

// Join registers p, delivers it the current roster (one peerJoined per
// existing peer, terminated by a RosterComplete), and announces p's own join
// to everyone else — all while holding the session lock for the entire
// sequence. That atomicity is what makes this safe: no other peer can Join
// or Leave in the middle of it, so the roster p receives is exactly the set
// of peers still present when RosterComplete is sent, with no gap in which
// a peer from the snapshot could vanish (a phantom peerJoined with no way to
// correct it) or a concurrent joiner could be missed.
//
// beforeVisible, if non-nil, runs under the same lock before p becomes
// visible to anyone else — e.g. to write a "joined" confirmation with an
// existing-peer count that's guaranteed consistent with the roster that
// follows.
func (s *Session) Join(p Peer, beforeVisible func(existingCount int)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existingIDs := make([]string, 0, len(s.peers))
	for id := range s.peers {
		existingIDs = append(existingIDs, id)
	}
	if beforeVisible != nil {
		beforeVisible(len(existingIDs))
	}
	s.peers[p.ID()] = p
	for _, id := range existingIDs {
		p.Deliver(wire.NewPeerJoined(id))
	}
	p.Deliver(wire.NewRosterComplete())
	s.broadcastExceptLocked(p.ID(), wire.NewPeerJoined(p.ID()))
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
