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

// Join adds p to the session, tells everyone else p joined, and tells p
// about every peer already present (as the same peerJoined event they'd
// have seen if they'd been connected at the time) so a new member always
// knows the full current roster.
func (s *Session) Join(p Peer) {
	s.mu.Lock()
	existing := make([]string, 0, len(s.peers))
	for id := range s.peers {
		existing = append(existing, id)
	}
	s.peers[p.ID()] = p
	s.mu.Unlock()

	for _, id := range existing {
		p.Deliver(wire.NewPeerJoined(id))
	}
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
