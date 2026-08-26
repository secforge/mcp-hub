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

// Register adds p to the session and returns the peerIds of everyone
// already present at that moment. The session lock is held for the whole
// call, including beforeVisible — so beforeVisible (e.g. writing a "joined"
// confirmation with an accurate existing-peer count) is guaranteed to run
// before p becomes visible to any other peer's Broadcast/DeliverTo/Register.
// That guarantees two things: p can never receive anything else before
// whatever beforeVisible sends it, and any count beforeVisible reports can
// never drift from the roster AnnounceRoster goes on to actually deliver
// (both come from the same snapshot, under the same lock).
func (s *Session) Register(p Peer, beforeVisible func(existingCount int)) (existingIDs []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existingIDs = make([]string, 0, len(s.peers))
	for id := range s.peers {
		existingIDs = append(existingIDs, id)
	}
	beforeVisible(len(existingIDs))
	s.peers[p.ID()] = p
	return existingIDs
}

// AnnounceRoster tells p about each peer in existingIDs (as if it just
// joined) and broadcasts p's own join to everyone else. Call once, after
// Register.
func (s *Session) AnnounceRoster(p Peer, existingIDs []string) {
	for _, id := range existingIDs {
		p.Deliver(wire.NewPeerJoined(id))
	}
	s.broadcastExcept(p.ID(), wire.NewPeerJoined(p.ID()))
}

// Join registers p and immediately announces the roster — a convenience for
// callers that don't need to inject anything between the two steps (mainly
// tests; wsserver uses Register/AnnounceRoster directly so it can send an
// accurate peerCount in the "joined" confirmation first).
func (s *Session) Join(p Peer) {
	existingIDs := s.Register(p, func(int) {})
	s.AnnounceRoster(p, existingIDs)
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
