package hubsession

import (
	"sync"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/wire"
)

type fakePeer struct {
	id           string
	name         string
	agePublicKey string
	received     []any
}

func (f *fakePeer) ID() string           { return f.id }
func (f *fakePeer) Name() string         { return f.name }
func (f *fakePeer) AgePublicKey() string { return f.agePublicKey }
func (f *fakePeer) Deliver(event any)    { f.received = append(f.received, event) }

// joinFake joins a fakePeer (with the given name/agePublicKey) into s using
// reconnectSecret to resolve its peerID, and returns it, with .id set to
// whatever peerID Join actually assigned (a fresh UUID, unless
// reconnectSecret matches a still-alive-session but currently-disconnected
// earlier peer's — see the identity-reuse tests). agePublicKey is
// deliberately independent of peerID resolution — see those same tests.
func joinFake(s *Session, name, agePublicKey, reconnectSecret string, beforeVisible func(int)) *fakePeer {
	var fp *fakePeer
	s.Join(reconnectSecret, func(id string) Peer {
		fp = &fakePeer{id: id, name: name, agePublicKey: agePublicKey}
		return fp
	}, beforeVisible)
	return fp
}

func TestJoinNeverTellsAPeerAboutItself(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)
	b := joinFake(s, "", "", "", nil)

	// a: rosterComplete (immediately, empty roster of its own), then told
	// about b's later join.
	if len(a.received) != 2 {
		t.Fatalf("a should have received its own rosterComplete plus b's join event, got %d events: %+v", len(a.received), a.received)
	}
	if _, ok := a.received[0].(wire.RosterComplete); !ok {
		t.Fatalf("expected a's first event to be rosterComplete, got %+v", a.received[0])
	}
	if ev, ok := a.received[1].(wire.PeerEvent); !ok || ev.PeerID != b.ID() {
		t.Fatalf("unexpected second event for a: %+v", a.received[1])
	}
	if len(b.received) != 2 {
		t.Fatalf("b should be told about the one existing peer (a) then rosterComplete, got %d events: %+v", len(b.received), b.received)
	}
	if ev, ok := b.received[0].(wire.PeerEvent); !ok || ev.PeerID != a.ID() {
		t.Fatalf("unexpected first event for b: %+v", b.received[0])
	}
	if _, ok := b.received[1].(wire.RosterComplete); !ok {
		t.Fatalf("expected b's second event to be rosterComplete, got %+v", b.received[1])
	}
}

func TestJoinNotifiesNewPeerAboutExistingPeers(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)
	b := joinFake(s, "", "", "", nil)
	c := joinFake(s, "", "", "", nil)

	if len(c.received) != 3 {
		t.Fatalf("c should be told about both existing peers plus rosterComplete, got %d events: %+v", len(c.received), c.received)
	}
	seen := map[string]bool{}
	for _, ev := range c.received[:2] {
		pe, ok := ev.(wire.PeerEvent)
		if !ok || pe.Type != wire.TypePeerJoined {
			t.Fatalf("expected a peerJoined event, got %+v", ev)
		}
		seen[pe.PeerID] = true
	}
	if !seen[a.ID()] || !seen[b.ID()] {
		t.Fatalf("expected to be told about both a and b, got %+v", c.received)
	}
	if _, ok := c.received[2].(wire.RosterComplete); !ok {
		t.Fatalf("expected c's last event to be rosterComplete, got %+v", c.received[2])
	}
}

func TestJoinIncludesNameAndAgePublicKeyInPeerEvents(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	pubkeyA := "age1scdm7mae5t68c9ch0sqfzlusqyflpgxlrgk3zwl44zwl9vvq2guqtdv4fk"
	a := joinFake(s, "Alice", pubkeyA, "", nil)
	b := joinFake(s, "", "", "", nil)

	// b's roster catch-up must include a's name/pubkey.
	if len(b.received) < 1 {
		t.Fatalf("expected b to receive at least one event, got %+v", b.received)
	}
	pe, ok := b.received[0].(wire.PeerEvent)
	if !ok || pe.PeerID != a.ID() || pe.Name != "Alice" || pe.AgePublicKey != pubkeyA {
		t.Fatalf("expected b's roster entry for a to carry name/pubkey, got %+v", b.received[0])
	}

	// a is told about b's join (broadcast), which must carry b's (empty)
	// name/pubkey fields consistently, i.e. no crash/mixup.
	if len(a.received) != 2 {
		t.Fatalf("expected a to have 2 events (own rosterComplete, then b's join), got %+v", a.received)
	}
	joinedB, ok := a.received[1].(wire.PeerEvent)
	if !ok || joinedB.PeerID != b.ID() || joinedB.Name != "" || joinedB.AgePublicKey != "" {
		t.Fatalf("unexpected event for a about b: %+v", a.received[1])
	}
}

func TestJoinReportsExistingCountViaBeforeVisibleCallback(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	joinFake(s, "", "", "", nil)

	var reportedCount int
	joinFake(s, "", "", "", func(count int) { reportedCount = count })

	if reportedCount != 1 {
		t.Fatalf("expected beforeVisible to report 1 existing peer, got %d", reportedCount)
	}
}

func TestJoinReusesPeerIDForSameReconnectSecretAfterLeaving(t *testing.T) {
	t.Setenv("MCP_HUB_LOG_DIR", t.TempDir()) // isolate persisted secretToPeerID from other tests
	m := NewManager()
	s := m.GetOrCreate("session-1")
	secret := "super-secret-token"

	first := joinFake(s, "Alice", "", secret, nil)
	firstID := first.ID()
	s.Leave(first)

	second := joinFake(s, "Alice", "", secret, nil)
	if second.ID() != firstID {
		t.Fatalf("expected reconnecting with the same reconnectSecret to reuse peerID %q, got %q", firstID, second.ID())
	}
}

func TestJoinAssignsFreshPeerIDWhenSameReconnectSecretStillConnected(t *testing.T) {
	t.Setenv("MCP_HUB_LOG_DIR", t.TempDir()) // isolate persisted secretToPeerID from other tests
	m := NewManager()
	s := m.GetOrCreate("session-1")
	secret := "super-secret-token"

	first := joinFake(s, "Alice", "", secret, nil)
	// first never leaves - a second connection presenting the same secret
	// concurrently must not collide with it.
	second := joinFake(s, "Alice", "", secret, nil)

	if second.ID() == first.ID() {
		t.Fatal("expected a fresh peerID when the previous holder of this reconnectSecret is still connected")
	}
}

func TestJoinReportsReusedAccurately(t *testing.T) {
	t.Setenv("MCP_HUB_LOG_DIR", t.TempDir()) // isolate persisted secretToPeerID from other tests
	m := NewManager()
	s := m.GetOrCreate("session-1")
	secret := "super-secret-token"

	var firstID string
	_, reused := s.Join(secret, func(id string) Peer { firstID = id; return &fakePeer{id: id} }, nil)
	if reused {
		t.Fatal("first join with a never-before-seen secret must report reused=false")
	}
	s.Leave(&fakePeer{id: firstID})

	var secondID string
	p2, reused := s.Join(secret, func(id string) Peer { secondID = id; return &fakePeer{id: id} }, nil)
	if !reused {
		t.Fatal("reconnecting with a matching secret to a now-departed peer must report reused=true")
	}
	if secondID != firstID {
		t.Fatalf("expected reused peerID %q, got %q", firstID, secondID)
	}
	_ = p2

	// same secret, but its holder is still connected: fresh ID, reused=false.
	_, reused = s.Join(secret, func(id string) Peer { return &fakePeer{id: id} }, nil)
	if reused {
		t.Fatal("expected reused=false when the secret's previous holder is still connected")
	}
}

// TestReconnectSecretSurvivesSimulatedServerRestart proves the fix for the
// gap the user found: reconnectSecret -> peerID mappings used to live only
// in the in-memory Session, so any server restart silently reset every
// peer's identity even if it presented the exact secret it always had. A
// restart is simulated here by discarding the in-memory Session/Manager
// entirely and constructing brand new ones — nothing in-process survives
// that except whatever identitystore itself persisted to disk.
func TestReconnectSecretSurvivesSimulatedServerRestart(t *testing.T) {
	t.Setenv("MCP_HUB_LOG_DIR", t.TempDir()) // isolate persisted secretToPeerID from other tests
	sessionID := "session-restart-test"
	secret := "super-secret-token"

	m1 := NewManager()
	s1 := m1.GetOrCreate(sessionID)
	first := joinFake(s1, "Alice", "", secret, nil)
	firstID := first.ID()
	s1.Leave(first)
	// m1/s1 are now abandoned entirely, standing in for the old process's
	// in-memory state being wiped by a restart — nothing below references
	// them again.

	m2 := NewManager()
	s2 := m2.GetOrCreate(sessionID)
	second := joinFake(s2, "Alice", "", secret, nil)
	if second.ID() != firstID {
		t.Fatalf("expected the reconnectSecret to survive the simulated restart and reuse peerID %q, got %q", firstID, second.ID())
	}
}

// TestReconnectSecretDoesNotSurviveIntentionalTeardown proves the other
// half of the design: unlike a restart, the last peer leaving (Remove) is
// treated as a deliberate end of the channel, and does forget the mapping
// — even across a subsequent simulated restart, since the persisted file
// was deleted, not just the in-memory state.
func TestReconnectSecretDoesNotSurviveIntentionalTeardown(t *testing.T) {
	t.Setenv("MCP_HUB_LOG_DIR", t.TempDir()) // isolate persisted secretToPeerID from other tests
	sessionID := "session-teardown-test"
	secret := "super-secret-token"

	m1 := NewManager()
	s1 := m1.GetOrCreate(sessionID)
	first := joinFake(s1, "Alice", "", secret, nil)
	firstID := first.ID()
	if empty := s1.Leave(first); !empty {
		t.Fatal("expected the session to be empty after its only peer leaves")
	}
	m1.Remove(sessionID) // the real trigger for this, in wsserver, is exactly "session became empty"

	m2 := NewManager()
	s2 := m2.GetOrCreate(sessionID)
	second := joinFake(s2, "Alice", "", secret, nil)
	if second.ID() == firstID {
		t.Fatal("expected a fresh peerID after an intentional teardown (Remove), even with the same reconnectSecret")
	}
}

func TestJoinAssignsFreshPeerIDWhenNoReconnectSecretGiven(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")

	first := joinFake(s, "", "", "", nil)
	s.Leave(first)
	second := joinFake(s, "", "", "", nil)

	if second.ID() == first.ID() {
		t.Fatal("connections without a reconnectSecret must never be treated as the same identity")
	}
}

// TestJoinNeverReusesPeerIDBasedOnAgePublicKeyAlone is a security
// regression test: agePublicKey is broadcast to every other peer in the
// session (see TestJoinIncludesNameAndAgePublicKeyInPeerEvents), so if it
// alone granted identity reuse, anyone who observed a peer's key could
// reconnect presenting that same key and be handed that peer's peerId —
// impersonation. Only reconnectSecret (never distributed to anyone) may
// grant reuse.
func TestJoinNeverReusesPeerIDBasedOnAgePublicKeyAlone(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	pubkey := "age1scdm7mae5t68c9ch0sqfzlusqyflpgxlrgk3zwl44zwl9vvq2guqtdv4fk"

	first := joinFake(s, "Alice", pubkey, "", nil)
	firstID := first.ID()
	s.Leave(first)

	// an "impersonator" who merely observed first's public agePublicKey
	// (no secret) must not be able to reclaim first's peerId.
	impersonator := joinFake(s, "Alice", pubkey, "", nil)
	if impersonator.ID() == firstID {
		t.Fatal("agePublicKey alone must never grant peerId reuse — it's public, so this would allow impersonation")
	}
}

// blockingPeer's Deliver signals started, then blocks on unblock — used to
// hold Join's internal lock open long enough to prove a concurrent Leave
// (for a peer already included in the roster snapshot) cannot proceed until
// Join has fully finished delivering that snapshot.
type blockingPeer struct {
	id         string
	started    chan struct{}
	startedOne sync.Once
	unblock    chan struct{}
	received   []any
}

func (b *blockingPeer) ID() string           { return b.id }
func (b *blockingPeer) Name() string         { return "" }
func (b *blockingPeer) AgePublicKey() string { return "" }
func (b *blockingPeer) Deliver(event any) {
	b.received = append(b.received, event)
	b.startedOne.Do(func() { close(b.started) })
	<-b.unblock
}

func TestJoinIsAtomicAgainstConcurrentLeave(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)

	c := &blockingPeer{started: make(chan struct{}), unblock: make(chan struct{})}
	joinDone := make(chan struct{})
	go func() {
		s.Join("", func(id string) Peer { c.id = id; return c }, nil) // delivering peerJoined("a") to c blocks inside c.Deliver
		close(joinDone)
	}()

	select {
	case <-c.started:
	case <-time.After(2 * time.Second):
		t.Fatal("Join never started delivering to c")
	}

	leaveDone := make(chan struct{})
	go func() {
		s.Leave(a) // a was in c's roster snapshot — must wait for Join to finish
		close(leaveDone)
	}()

	select {
	case <-leaveDone:
		t.Fatal("Leave(a) completed while Join(c) still held the session lock mid-delivery — not atomic")
	case <-time.After(100 * time.Millisecond):
		// expected: still blocked, since Join(c) hasn't released the lock yet
	}

	close(c.unblock) // let Join(c) finish delivering and release the lock

	select {
	case <-joinDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Join(c) never completed")
	}
	select {
	case <-leaveDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Leave(a) never completed after Join(c) released the lock")
	}

	// c's roster snapshot correctly included a (a was still present at
	// snapshot time) — a's later departure is a's own concern, not
	// something that should have erased it from c's already-delivered
	// roster event. c then separately gets told about a's departure too,
	// since c is a member of the session by then.
	if len(c.received) != 3 {
		t.Fatalf("expected c to have received 1 roster event, rosterComplete, then a's departure, got %d: %+v", len(c.received), c.received)
	}
	if pe, ok := c.received[0].(wire.PeerEvent); !ok || pe.PeerID != a.ID() || pe.Type != wire.TypePeerJoined {
		t.Fatalf("unexpected first event for c: %+v", c.received[0])
	}
	if _, ok := c.received[1].(wire.RosterComplete); !ok {
		t.Fatalf("expected c's second event to be rosterComplete, got %+v", c.received[1])
	}
	if pe, ok := c.received[2].(wire.PeerEvent); !ok || pe.PeerID != a.ID() || pe.Type != wire.TypePeerLeft {
		t.Fatalf("expected c's third event to be a's departure, got %+v", c.received[2])
	}
}

func TestBroadcastExcludesSender(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)
	b := joinFake(s, "", "", "", nil)
	a.received = nil
	b.received = nil

	s.Broadcast(a, wire.NewBroadcastMsg(a.ID(), "hi", "ts"))

	if len(a.received) != 0 {
		t.Fatalf("sender should not receive its own broadcast, got %d", len(a.received))
	}
	if len(b.received) != 1 {
		t.Fatalf("b should have received the broadcast, got %d", len(b.received))
	}
}

func TestDeliverToSendsOnlyToTarget(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)
	b := joinFake(s, "", "", "", nil)
	c := joinFake(s, "", "", "", nil)
	a.received, b.received, c.received = nil, nil, nil

	if err := s.DeliverTo(a, b.ID(), wire.NewDirectedMsg(a.ID(), "psst", "ts")); err != nil {
		t.Fatalf("DeliverTo: %v", err)
	}

	if len(b.received) != 1 {
		t.Fatalf("b should have received the directed message, got %d", len(b.received))
	}
	if len(a.received) != 0 {
		t.Fatalf("sender should never receive its own message, got %d", len(a.received))
	}
	if len(c.received) != 0 {
		t.Fatalf("c should not receive a message directed at b, got %d", len(c.received))
	}
}

func TestDeliverToUnknownPeerErrors(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)

	err := s.DeliverTo(a, "does-not-exist", wire.NewDirectedMsg(a.ID(), "hi", "ts"))
	if err == nil {
		t.Fatal("expected an error targeting a peer that isn't in the session")
	}
}

func TestDeliverToSelfErrors(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)
	a.received = nil

	err := s.DeliverTo(a, a.ID(), wire.NewDirectedMsg(a.ID(), "hi", "ts"))
	if err == nil {
		t.Fatal("expected an error targeting yourself")
	}
	if len(a.received) != 0 {
		t.Fatalf("sender should not receive anything when targeting itself, got %d", len(a.received))
	}
}

func TestLeaveReportsEmptyWhenLastPeerLeaves(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)
	b := joinFake(s, "", "", "", nil)

	if empty := s.Leave(a); empty {
		t.Fatal("session should not be empty after only one of two peers leaves")
	}
	if empty := s.Leave(b); !empty {
		t.Fatal("session should be empty after the last peer leaves")
	}
}

func TestManagerGetOrCreateReturnsSameSession(t *testing.T) {
	m := NewManager()
	s1 := m.GetOrCreate("session-1")
	s2 := m.GetOrCreate("session-1")
	if s1 != s2 {
		t.Fatal("GetOrCreate should return the same session for the same id")
	}
	m.Remove("session-1")
	s3 := m.GetOrCreate("session-1")
	if s3 == s1 {
		t.Fatal("GetOrCreate should create a fresh session after Remove")
	}
}
