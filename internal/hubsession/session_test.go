package hubsession

import (
	"sync"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/wire"
)

type fakePeer struct {
	id       string
	received []any
}

func (f *fakePeer) ID() string        { return f.id }
func (f *fakePeer) Deliver(event any) { f.received = append(f.received, event) }

func TestJoinNeverTellsAPeerAboutItself(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	b := &fakePeer{id: "b"}

	s.Join(a, nil)
	s.Join(b, nil)

	// a: rosterComplete (immediately, empty roster of its own), then told
	// about b's later join.
	if len(a.received) != 2 {
		t.Fatalf("a should have received its own rosterComplete plus b's join event, got %d events: %+v", len(a.received), a.received)
	}
	if _, ok := a.received[0].(wire.RosterComplete); !ok {
		t.Fatalf("expected a's first event to be rosterComplete, got %+v", a.received[0])
	}
	if ev, ok := a.received[1].(wire.PeerEvent); !ok || ev.PeerID != "b" {
		t.Fatalf("unexpected second event for a: %+v", a.received[1])
	}
	if len(b.received) != 2 {
		t.Fatalf("b should be told about the one existing peer (a) then rosterComplete, got %d events: %+v", len(b.received), b.received)
	}
	if ev, ok := b.received[0].(wire.PeerEvent); !ok || ev.PeerID != "a" {
		t.Fatalf("unexpected first event for b: %+v", b.received[0])
	}
	if _, ok := b.received[1].(wire.RosterComplete); !ok {
		t.Fatalf("expected b's second event to be rosterComplete, got %+v", b.received[1])
	}
}

func TestJoinNotifiesNewPeerAboutExistingPeers(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	b := &fakePeer{id: "b"}
	c := &fakePeer{id: "c"}

	s.Join(a, nil)
	s.Join(b, nil)
	c.received = nil
	s.Join(c, nil)

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
	if !seen["a"] || !seen["b"] {
		t.Fatalf("expected to be told about both a and b, got %+v", c.received)
	}
	if _, ok := c.received[2].(wire.RosterComplete); !ok {
		t.Fatalf("expected c's last event to be rosterComplete, got %+v", c.received[2])
	}
}

func TestJoinReportsExistingCountViaBeforeVisibleCallback(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	s.Join(a, nil)

	var reportedCount int
	c := &fakePeer{id: "c"}
	s.Join(c, func(count int) { reportedCount = count })

	if reportedCount != 1 {
		t.Fatalf("expected beforeVisible to report 1 existing peer, got %d", reportedCount)
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

func (b *blockingPeer) ID() string { return b.id }
func (b *blockingPeer) Deliver(event any) {
	b.received = append(b.received, event)
	b.startedOne.Do(func() { close(b.started) })
	<-b.unblock
}

func TestJoinIsAtomicAgainstConcurrentLeave(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	s.Join(a, nil)

	c := &blockingPeer{id: "c", started: make(chan struct{}), unblock: make(chan struct{})}
	joinDone := make(chan struct{})
	go func() {
		s.Join(c, nil) // delivering peerJoined("a") to c blocks inside c.Deliver
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
	if pe, ok := c.received[0].(wire.PeerEvent); !ok || pe.PeerID != "a" || pe.Type != wire.TypePeerJoined {
		t.Fatalf("unexpected first event for c: %+v", c.received[0])
	}
	if _, ok := c.received[1].(wire.RosterComplete); !ok {
		t.Fatalf("expected c's second event to be rosterComplete, got %+v", c.received[1])
	}
	if pe, ok := c.received[2].(wire.PeerEvent); !ok || pe.PeerID != "a" || pe.Type != wire.TypePeerLeft {
		t.Fatalf("expected c's third event to be a's departure, got %+v", c.received[2])
	}
}

func TestBroadcastExcludesSender(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	b := &fakePeer{id: "b"}
	s.Join(a, nil)
	s.Join(b, nil)
	a.received = nil
	b.received = nil

	s.Broadcast(a, wire.NewBroadcastMsg("a", "hi", "ts"))

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
	a := &fakePeer{id: "a"}
	b := &fakePeer{id: "b"}
	c := &fakePeer{id: "c"}
	s.Join(a, nil)
	s.Join(b, nil)
	s.Join(c, nil)
	a.received, b.received, c.received = nil, nil, nil

	if err := s.DeliverTo(a, "b", wire.NewDirectedMsg("a", "psst", "ts")); err != nil {
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
	a := &fakePeer{id: "a"}
	s.Join(a, nil)

	err := s.DeliverTo(a, "does-not-exist", wire.NewDirectedMsg("a", "hi", "ts"))
	if err == nil {
		t.Fatal("expected an error targeting a peer that isn't in the session")
	}
}

func TestDeliverToSelfErrors(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	s.Join(a, nil)
	a.received = nil

	err := s.DeliverTo(a, "a", wire.NewDirectedMsg("a", "hi", "ts"))
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
	a := &fakePeer{id: "a"}
	b := &fakePeer{id: "b"}
	s.Join(a, nil)
	s.Join(b, nil)

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
