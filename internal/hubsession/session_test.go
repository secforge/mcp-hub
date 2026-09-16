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
	closed       bool
	closeCode    int
	closeReason  string
}

func (f *fakePeer) ID() string           { return f.id }
func (f *fakePeer) Name() string         { return f.name }
func (f *fakePeer) AgePublicKey() string { return f.agePublicKey }
func (f *fakePeer) Deliver(event any)    { f.received = append(f.received, event) }
func (f *fakePeer) Close(code int, reason string) {
	f.closed = true
	f.closeCode = code
	f.closeReason = reason
}

// joinFake joins a fakePeer (with the given name/agePublicKey) into s using
// reconnectSecret to resolve its peerID, and returns it, with .id set to
// whatever peerID Join actually assigned (a fresh UUID, unless
// reconnectSecret matches a still-alive-session but currently-disconnected
// earlier peer's — see the identity-reuse tests). agePublicKey is
// deliberately independent of peerID resolution — see those same tests.
func joinFake(s *Session, name, agePublicKey, reconnectSecret string, beforeVisible func()) *fakePeer {
	var fp *fakePeer
	s.Join(reconnectSecret, func(id string) Peer {
		fp = &fakePeer{id: id, name: name, agePublicKey: agePublicKey}
		return fp
	}, beforeVisible)
	return fp
}

// The roster states the session's membership, so it INCLUDES the peer
// receiving it — a list that silently omitted the reader would be a view
// each client has to mentally correct, and the correction is what used
// to be got wrong.
func TestTheRosterStatesTheWholeMembershipIncludingTheReceiver(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)
	b := joinFake(s, "", "", "", nil)

	// a: a roster naming only itself, then the roster again once b joins.
	if len(a.received) != 2 {
		t.Fatalf("a should have received its own roster plus the one b's join produced, got %d events: %+v", len(a.received), a.received)
	}
	r, ok := a.received[0].(wire.Roster)
	if !ok || len(r.Members) != 1 || r.Members[0].PeerID != a.ID() {
		t.Fatalf("expected a's first event to be a roster naming just itself, got %+v", a.received[0])
	}
	r2, ok := a.received[1].(wire.Roster)
	if !ok || len(r2.Members) != 2 {
		t.Fatalf("unexpected second event for a: %+v", a.received[1])
	}
	if len(b.received) != 1 {
		t.Fatalf("b should get the whole membership in ONE roster message, got %d events: %+v", len(b.received), b.received)
	}
	r, ok = b.received[0].(wire.Roster)
	if !ok || len(r.Members) != 2 {
		t.Fatalf("unexpected roster for b: %+v", b.received[0])
	}
}

func TestJoinNotifiesNewPeerAboutExistingPeers(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)
	b := joinFake(s, "", "", "", nil)
	c := joinFake(s, "", "", "", nil)

	if len(c.received) != 1 {
		t.Fatalf("c should get the whole membership in ONE roster message, got %d events: %+v", len(c.received), c.received)
	}
	r, ok := c.received[0].(wire.Roster)
	if !ok {
		t.Fatalf("expected a roster event, got %+v", c.received[0])
	}
	seen := map[string]bool{}
	for _, rp := range r.Members {
		seen[rp.PeerID] = true
	}
	if len(r.Members) != 3 || !seen[a.ID()] || !seen[b.ID()] || !seen[c.ID()] {
		t.Fatalf("expected the roster to name a, b and c, got %+v", r)
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
	r, ok := b.received[0].(wire.Roster)
	if !ok {
		t.Fatalf("expected b's first event to be the roster, got %+v", b.received[0])
	}
	var aEntry wire.RosterMember
	for _, m := range r.Members {
		if m.PeerID == a.ID() {
			aEntry = m
		}
	}
	if aEntry.Name != "Alice" || aEntry.AgePublicKey != pubkeyA {
		t.Fatalf("expected b's roster entry for a to carry name/pubkey, got %+v", r.Members)
	}

	// a is re-sent the whole membership when b joins, which must carry
	// b's (empty) name/pubkey fields consistently, i.e. no crash/mixup.
	if len(a.received) != 2 {
		t.Fatalf("expected a to have 2 events (own roster, then the roster b's join produced), got %+v", a.received)
	}
	after, ok := a.received[1].(wire.Roster)
	if !ok || len(after.Members) != 2 {
		t.Fatalf("unexpected event for a about b: %+v", a.received[1])
	}
	var bEntry wire.RosterMember
	for _, m := range after.Members {
		if m.PeerID == b.ID() {
			bEntry = m
		}
	}
	if bEntry.PeerID != b.ID() || bEntry.Name != "" || bEntry.AgePublicKey != "" {
		t.Fatalf("unexpected entry for b: %+v", after.Members)
	}
}

// beforeVisible runs under the roster lock, before the newcomer is
// visible to anyone — which is what lets the "joined" confirmation and
// the roster leave together and makes a membership change unable to
// interleave between them.
func TestJoinRunsBeforeVisibleWhileStillHoldingTheRosterLock(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	first := joinFake(s, "", "", "", nil)

	var sawMembership int
	joinFake(s, "", "", "", func() { sawMembership = len(s.peers) })

	// The newcomer is not in the map yet, so what beforeVisible can see is
	// exactly the membership the roster is about to be built from.
	if sawMembership != 1 {
		t.Fatalf("expected beforeVisible to run before the newcomer was visible, saw %d peers", sawMembership)
	}
	_ = first
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

func TestJoinSupersedesWhenSameReconnectSecretStillConnected(t *testing.T) {
	t.Setenv("MCP_HUB_LOG_DIR", t.TempDir()) // isolate persisted secretToPeerID from other tests
	m := NewManager()
	s := m.GetOrCreate("session-1")
	secret := "super-secret-token"

	first := joinFake(s, "Alice", "", secret, nil)
	// first never leaves - a second connection presenting the same secret
	// must take over its identity (supersede), not collide with a fresh one.
	second := joinFake(s, "Alice", "", secret, nil)

	if second.ID() != first.ID() {
		t.Fatalf("expected the second connection to reclaim the same peerID %q by superseding, got %q",
			first.ID(), second.ID())
	}
	if !first.closed {
		t.Fatal("expected the first (superseded) connection to have been closed")
	}
	if first.closeCode != SupersededCloseCode {
		t.Fatalf("expected close code %d, got %d", SupersededCloseCode, first.closeCode)
	}
}

// TestJoinSupersedeDoesNotBroadcastLeaveOrJoin proves the identity is
// invisible to other peers across a supersede: from a third peer's
// perspective, the reconnecting identity never left, so it should see
// neither a peerLeft nor a second peerJoined for it.
func TestJoinSupersedeDoesNotBroadcastLeaveOrJoin(t *testing.T) {
	t.Setenv("MCP_HUB_LOG_DIR", t.TempDir())
	m := NewManager()
	s := m.GetOrCreate("session-1")
	secret := "super-secret-token"

	first := joinFake(s, "Alice", "", secret, nil)
	bystander := joinFake(s, "Bob", "", "", nil)
	bystander.received = nil

	joinFake(s, "Alice", "", secret, nil)

	// A supersede changes which connection holds an identity, not who is
	// present, so nobody else is told anything at all.
	if len(bystander.received) != 0 {
		t.Fatalf("expected no roster re-send for a superseded identity, got %+v", bystander.received)
	}
	_ = first
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

	// same secret, and its holder is still connected: supersedes, reused=true.
	var thirdID string
	_, reused = s.Join(secret, func(id string) Peer { thirdID = id; return &fakePeer{id: id} }, nil)
	if !reused {
		t.Fatal("expected reused=true when superseding the secret's still-connected previous holder")
	}
	if thirdID != firstID {
		t.Fatalf("expected the superseding join to reclaim peerID %q, got %q", firstID, thirdID)
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
	t.Setenv("MCP_HUB_LOG_DIR", t.TempDir())            // isolate persisted secretToPeerID from other tests
	sessionID := "6ba7b810-9dad-11d1-80b4-00c04fd430c8" // must be a valid UUID - identitystore validates it
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

// TestReconnectSecretSurvivesIntentionalTeardown proves the mapping
// survives even the session becoming fully empty and being torn down
// (Manager.Remove) — not just a server restart. There's currently no
// expiry/cleanup for persisted mappings at all (matching this project's
// existing PoC-log precedent), so a peer reconnecting with the same secret
// is recognized as the same identity no matter how long ago everyone else
// left.
func TestReconnectSecretSurvivesIntentionalTeardown(t *testing.T) {
	t.Setenv("MCP_HUB_LOG_DIR", t.TempDir())            // isolate persisted secretToPeerID from other tests
	sessionID := "6ba7b810-9dad-11d1-80b4-00c04fd430c8" // must be a valid UUID - identitystore validates it
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
	if second.ID() != firstID {
		t.Fatalf("expected the reconnectSecret to survive an intentional teardown (Remove) and reuse peerID %q, got %q", firstID, second.ID())
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
func (b *blockingPeer) Close(code int, reason string) {}

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
	if len(c.received) != 2 {
		t.Fatalf("expected c to have received its roster then a's departure, got %d: %+v", len(c.received), c.received)
	}
	first, ok := c.received[0].(wire.Roster)
	if !ok {
		t.Fatalf("unexpected first event for c: %+v", c.received[0])
	}
	sawA := false
	for _, m := range first.Members {
		if m.PeerID == a.ID() {
			sawA = true
		}
	}
	if !sawA {
		t.Fatalf("expected c's roster snapshot to still include a, got %+v", first)
	}
	left, ok := c.received[1].(wire.Roster)
	if !ok {
		t.Fatalf("expected c's second event to be the roster a's departure produced, got %+v", c.received[1])
	}
	for _, m := range left.Members {
		if m.PeerID == a.ID() {
			t.Fatalf("expected a to be gone from the re-sent roster, got %+v", left)
		}
	}
}

func TestBroadcastExcludesSender(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)
	b := joinFake(s, "", "", "", nil)
	a.received = nil
	b.received = nil

	s.Broadcast(a, wire.NewBroadcastMsg(a.ID(), "hi", "ts", nil, "", "", nil))

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

	if err := s.DeliverTo(a, b.ID(), wire.NewDirectedMsg(a.ID(), "psst", "ts", nil, "", "", nil)); err != nil {
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

	err := s.DeliverTo(a, "does-not-exist", wire.NewDirectedMsg(a.ID(), "hi", "ts", nil, "", "", nil))
	if err == nil {
		t.Fatal("expected an error targeting a peer that isn't in the session")
	}
}

func TestDeliverToSelfErrors(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)
	a.received = nil

	err := s.DeliverTo(a, a.ID(), wire.NewDirectedMsg(a.ID(), "hi", "ts", nil, "", "", nil))
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

func TestPeersReturnsAllCurrentMembers(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "Alice", "", "", nil)
	b := joinFake(s, "Bob", "", "", nil)

	peers := s.Peers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d: %+v", len(peers), peers)
	}
	seen := map[string]bool{}
	for _, p := range peers {
		seen[p.ID()] = true
	}
	if !seen[a.ID()] || !seen[b.ID()] {
		t.Fatalf("expected to see both a and b, got %+v", peers)
	}
}

func TestPeersReflectsLeave(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := joinFake(s, "", "", "", nil)
	s.Leave(a)

	if peers := s.Peers(); len(peers) != 0 {
		t.Fatalf("expected no peers after the only one left, got %+v", peers)
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

// Leave says "empty" and the caller then asks for removal. A peer joining
// between those two calls was admitted to the session that was then
// evicted, and the next join built a SECOND session under the same id —
// two peers correctly joined to one session, in different objects,
// invisible to each other, with nothing anywhere reporting it.
func TestRemoveDoesNotEvictASessionSomeoneJustJoined(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("s1")

	first := joinFake(s, "first", "", "", nil)
	empty := s.Leave(first)
	if !empty {
		t.Fatal("expected the session to report empty after its only peer left")
	}

	// The interleaving: someone joins before the caller acts on "empty".
	// A drop and an immediate redial is exactly this, which is the fast
	// reconnect a stored secret exists to make work.
	joinFake(s, "second", "", "", nil)

	m.Remove("s1")

	if got := m.GetOrCreate("s1"); got != s {
		t.Fatal("expected the session holding a live peer to survive Remove — " +
			"a second object under the same id splits the conversation in two")
	}
}

// And the ordinary case still cleans up: genuinely empty, genuinely gone.
func TestRemoveStillDropsAnEmptySession(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("s2")
	p := joinFake(s, "only", "", "", nil)
	s.Leave(p)
	m.Remove("s2")
	if got := m.GetOrCreate("s2"); got == s {
		t.Fatal("expected an empty session to be removed")
	}
}
