package hubsession

import (
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"

	"github.com/secforge/mcp-hub/internal/identitystore"
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
	// Close forcibly disconnects this peer — used by Join when a new
	// connection presents a reconnectSecret that maps to this peer's
	// identity while it's still live (see Join, SupersededCloseCode): the
	// newcomer takes over the identity, and this connection needs to
	// actually go away rather than being silently forgotten server-side
	// while its own transport still thinks it's connected. code/reason
	// are the close-code/message to use where the underlying transport
	// has such a concept (e.g. a websocket close frame); an
	// implementation with none (e.g. an in-process HTTP-MCP peer) may
	// synthesize equivalent behavior however fits. Close must not itself
	// touch Session state — cleanup happens through this peer's own
	// normal disconnect path noticing its transport is gone (see Leave's
	// identity check for why that's safe even though it now runs after
	// this peerID has already been reassigned).
	Close(code int, reason string)
}

// SupersededCloseCode is the websocket close code used when a new
// connection reclaims a peerId whose previous connection was still live
// (see Join) — chosen to match chat-relay's own convention for the
// identical case on its LINK sessions (see the wire protocol spec's
// close-codes section: 4000-4999 is the private range other servers are
// invited to mint from, and this value was coordinated rather than picked
// independently).
const SupersededCloseCode = 4004

type Session struct {
	id    string
	mu    sync.Mutex
	peers map[string]Peer
	// secretToPeerID remembers which peerID a reconnectSecret was last
	// assigned, for the lifetime of this Session (i.e. as long as the
	// channel stays alive across individual peer disconnects) *and* across
	// a server restart — see identitystore. Only an intentional teardown
	// (the last peer leaving, via Manager.Remove) forgets it. Keyed by
	// identitystore.HashSecret(reconnectSecret), never the secret's own
	// value, both here and on disk — see that package.
	//
	// Deliberately NOT keyed by agePublicKey: that value is broadcast to
	// every other peer in the session (see Join below), so anyone who saw
	// it could replay it to steal a peer's identity on reconnect. A
	// reconnectSecret is never distributed to anyone — only the connecting
	// client and the server ever see it — so only whoever actually holds it
	// can reclaim the identity it maps to.
	secretToPeerID map[string]string
}

func newSession(id string) *Session {
	return &Session{id: id, peers: make(map[string]Peer), secretToPeerID: identitystore.Load(id)}
}

// Join resolves the peerID to use (reusing the ID a previous, now-departed
// connection that presented the same reconnectSecret, if any — see
// secretToPeerID — otherwise a fresh UUID), constructs the peer via
// makePeer, registers it, and states the new membership to everyone
// present as ONE message (see wire.Roster) — all while holding the
// session lock for the entire sequence. That atomicity
// is what makes this safe: no other peer can Join or Leave in the middle
// of it, so the roster the new peer receives is exactly the set of peers
// present at that instant, with no gap in which a peer from the snapshot
// could vanish (a phantom peerJoined with no way to correct it) or a
// concurrent joiner could be missed.
//
// One message rather than a burst with a terminator because the end of a
// burst is not something a client can establish for itself: where a
// server's roster delivery is not serialised ahead of live traffic, a
// newcomer's join is indistinguishable from a roster entry. Stating the
// list removes the question.
//
// makePeer runs first (so beforeVisible, wsserver's own code, and this
// method can all refer to the resolved peerID), then beforeVisible, if
// non-nil, runs under the same lock before the peer becomes visible to
// anyone else — e.g. to write the "joined" confirmation that precedes the
// roster, so the two cannot be interleaved with a membership change.
// Join's second return value, reused, reports whether peerID was reclaimed
// from a matching reconnectSecret (true) or freshly generated (false) — the
// caller (wsserver) surfaces this in the session log so a reconnect with a
// valid secret is visible there without having to infer it from a repeated
// peerId across entries. reused is also true in the supersede case (see
// resolvePeerIDLocked) — the peerID itself really was reclaimed, just by
// force rather than because the old holder had already gone.
func (s *Session) Join(reconnectSecret string, makePeer func(peerID string) Peer, beforeVisible func()) (p Peer, reused bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	peerID, reused, supersede := s.resolvePeerIDLocked(reconnectSecret)
	if supersede != nil {
		// The identity this reconnectSecret maps to is still attached to a
		// live connection — take over rather than handing the newcomer a
		// fresh, unrelated peerID (see resolvePeerIDLocked's doc comment
		// for why: a fresh ID here silently defeats the entire point of
		// reconnectSecret for exactly the caller who most needs it, a fast
		// reconnect after an abrupt drop). Remove it from the roster now,
		// before existing is snapshotted below, so the roster this Join
		// delivers/announces is already consistent with the takeover
		// having happened. Its own connection is closed with
		// SupersededCloseCode — its transport will notice independently
		// and run its own normal teardown (Leave), which Leave's identity
		// check makes a safe no-op once this peerID has been reassigned
		// below. No peerLeft/peerJoined is broadcast for this — from
		// every other peer's perspective this identity never left, it
		// just changed which connection holds it.
		delete(s.peers, peerID)
		supersede.Close(SupersededCloseCode, "superseded by a new connection with the same identity")
	}
	p = makePeer(peerID)
	if beforeVisible != nil {
		beforeVisible()
	}
	s.peers[peerID] = p
	if reconnectSecret != "" {
		if s.secretToPeerID == nil {
			s.secretToPeerID = make(map[string]string)
		}
		s.secretToPeerID[identitystore.HashSecret(reconnectSecret)] = peerID
		// Best-effort: a persistence failure doesn't affect this Join's
		// correctness, only whether the mapping happens to survive a
		// future restart.
		_ = identitystore.Save(s.id, s.secretToPeerID)
	}

	// The membership changed, so everyone gets the new membership — the
	// newcomer included, and by the same statement rather than a
	// different one. A supersede changed which connection holds an
	// identity, not who is present, so it re-sends nothing to anyone
	// else.
	roster := wire.NewRoster(s.membersLocked())
	p.Deliver(roster)
	if supersede == nil {
		s.broadcastExceptLocked(peerID, roster)
	}
	return p, reused
}

// membersLocked is the session's membership as the wire states it —
// everyone present, including whoever is about to receive it. Called with
// s.mu already held.
func (s *Session) membersLocked() []wire.RosterMember {
	members := make([]wire.RosterMember, 0, len(s.peers))
	for _, p := range s.peers {
		members = append(members, wire.RosterMember{
			PeerID: p.ID(), Name: p.Name(), AgePublicKey: p.AgePublicKey()})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].PeerID < members[j].PeerID })
	return members
}

// broadcastRoster states the membership to everyone still present, and is
// how a departure is announced: not "X left" but "here is who is here",
// recomputed at send time. A client that missed the last one is repaired
// by this one, and a peer whose identity was reclaimed while its old
// connection was still tearing down cannot be announced as gone, because
// the list is what is true now rather than a delta about what changed.
func (s *Session) broadcastRoster() {
	s.mu.Lock()
	roster := wire.NewRoster(s.membersLocked())
	s.mu.Unlock()
	s.broadcastExcept("", roster)
}

// resolvePeerIDLocked returns the peerID a joining connection should use,
// whether it was reclaimed from a matching reconnectSecret (reused), and —
// only when the secret matches an identity that's still attached to a
// live connection — that connection's Peer, for Join to supersede
// (forcibly disconnect and hand its identity to the newcomer) rather than
// silently falling back to an unrelated fresh ID. Called with s.mu
// already held.
func (s *Session) resolvePeerIDLocked(reconnectSecret string) (peerID string, reused bool, supersede Peer) {
	if reconnectSecret != "" {
		if id, ok := s.secretToPeerID[identitystore.HashSecret(reconnectSecret)]; ok {
			if existing, stillConnected := s.peers[id]; stillConnected {
				return id, true, existing
			}
			return id, true, nil
		}
	}
	return uuid.NewString(), false, nil
}

// Leave removes p from the session and reports whether the session is now
// empty. The caller is responsible for tearing the session down via
// Manager.Remove when empty is true.
// Leave removes p from the session, but only if p is still exactly the
// peer currently registered under its own ID — this identity check (not
// just a key match) matters because of Join's supersede path: a
// superseded connection's own transport notices its socket died
// independently and asynchronously, and calls Leave on itself with no
// idea it was ever superseded. By the time that runs, Join has already
// deleted the old entry and inserted a new Peer under the same ID; a
// plain delete-by-ID here would silently evict that new, legitimately
// live peer and broadcast a false peerLeft for an identity that, from
// every other peer's perspective, never actually left. When the check
// fails (p is stale), this is a safe no-op — not an error, since Leave is
// meant to always be safely callable from a connection's own teardown
// path regardless of what happened to its identity in the meantime.
func (s *Session) Leave(p Peer) (empty bool) {
	s.mu.Lock()
	current, ok := s.peers[p.ID()]
	stale := ok && current != p
	if ok && !stale {
		delete(s.peers, p.ID())
	}
	empty = len(s.peers) == 0
	s.mu.Unlock()
	if stale {
		return empty
	}
	s.broadcastRoster()
	return empty
}

// isEmpty reports whether nobody is in this session right now. Used by
// Manager.Remove to re-check at the moment of deletion rather than act on
// an answer that was true when Leave returned it.
func (s *Session) isEmpty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.peers) == 0
}

// Peers returns a snapshot of everyone currently in the session, in no
// particular order.
func (s *Session) Peers() []Peer {
	s.mu.Lock()
	defer s.mu.Unlock()
	peers := make([]Peer, 0, len(s.peers))
	for _, p := range s.peers {
		peers = append(peers, p)
	}
	return peers
}

// ID returns this session's id, as passed to Manager.GetOrCreate.
func (s *Session) ID() string {
	return s.id
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

// Remove tears the in-memory Session down — called when its last peer
// leaves. Its persisted reconnectSecret mapping (identitystore) is
// deliberately left alone: a peer that reconnects later, even long after
// everyone left and the session was fully torn down, still gets the same
// peerID back for the same secret. There's currently no expiry or cleanup
// for these files, matching this project's existing PoC-log precedent
// (also never rotated/cleaned) — see identitystore's own doc comment.
func (m *Manager) Remove(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok {
		return
	}
	// Re-checked HERE, under the manager's own lock, rather than trusted
	// from Leave's answer. Leave reports "empty" and the caller then asks
	// for removal; a peer joining between those two calls is admitted to
	// this Session and would then be evicted with it. The next join
	// builds a SECOND Session under the same id, and two peers correctly
	// joined to the same session sit in different objects — invisible to
	// each other, with no error anywhere to say so.
	//
	// The interleaving needs a drop and an immediate redial, which is
	// exactly the fast reconnect a stored secret exists to make work. So
	// the failure is reserved for the path most likely to be taken.
	if !s.isEmpty() {
		return
	}
	delete(m.sessions, id)
}
