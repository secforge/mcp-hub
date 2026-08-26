package hubsession

import (
	"testing"

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

	s.Join(a)
	s.Join(b)

	if len(a.received) != 1 {
		t.Fatalf("a should have received b's join event, got %d events", len(a.received))
	}
	if ev, ok := a.received[0].(wire.PeerEvent); !ok || ev.PeerID != "b" {
		t.Fatalf("unexpected event for a: %+v", a.received[0])
	}
	if len(b.received) != 1 {
		t.Fatalf("b should be told about the one existing peer (a), got %d events: %+v", len(b.received), b.received)
	}
	if ev, ok := b.received[0].(wire.PeerEvent); !ok || ev.PeerID != "a" {
		t.Fatalf("unexpected event for b: %+v", b.received[0])
	}
}

func TestJoinNotifiesNewPeerAboutExistingPeers(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	b := &fakePeer{id: "b"}
	c := &fakePeer{id: "c"}

	s.Join(a)
	s.Join(b)
	c.received = nil
	s.Join(c)

	if len(c.received) != 2 {
		t.Fatalf("c should be told about both existing peers, got %d events: %+v", len(c.received), c.received)
	}
	seen := map[string]bool{}
	for _, ev := range c.received {
		pe, ok := ev.(wire.PeerEvent)
		if !ok || pe.Type != wire.TypePeerJoined {
			t.Fatalf("expected a peerJoined event, got %+v", ev)
		}
		seen[pe.PeerID] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("expected to be told about both a and b, got %+v", c.received)
	}
}

func TestRegisterReportsExistingCountBeforeVisible(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	b := &fakePeer{id: "b"}
	s.Join(a)

	var reportedCount int
	c := &fakePeer{id: "c"}
	existingIDs := s.Register(c, func(count int) { reportedCount = count })

	if reportedCount != 1 {
		t.Fatalf("expected beforeVisible to report 1 existing peer, got %d", reportedCount)
	}
	if len(existingIDs) != 1 || existingIDs[0] != "a" {
		t.Fatalf("expected existingIDs [a], got %v", existingIDs)
	}

	// b joining now must not see c yet - AnnounceRoster hasn't run, so c
	// isn't part of the roster b gets told about... but c IS already
	// visible to broadcasts/DeliverTo, since Register alone makes it so.
	s.Join(b)
	sawC := false
	for _, ev := range b.received {
		if pe, ok := ev.(wire.PeerEvent); ok && pe.PeerID == "c" {
			sawC = true
		}
	}
	if !sawC {
		t.Fatal("expected b to be told about c too, since c was registered (visible) before b joined")
	}
}

func TestAnnounceRosterTellsNewPeerAndBroadcastsItsJoin(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	s.Join(a)
	a.received = nil

	c := &fakePeer{id: "c"}
	existingIDs := s.Register(c, func(int) {})
	s.AnnounceRoster(c, existingIDs)

	if len(c.received) != 1 {
		t.Fatalf("expected c to be told about a, got %d events: %+v", len(c.received), c.received)
	}
	if pe, ok := c.received[0].(wire.PeerEvent); !ok || pe.PeerID != "a" {
		t.Fatalf("unexpected event for c: %+v", c.received[0])
	}
	if len(a.received) != 1 {
		t.Fatalf("expected a to be told about c's join, got %d events: %+v", len(a.received), a.received)
	}
	if pe, ok := a.received[0].(wire.PeerEvent); !ok || pe.PeerID != "c" {
		t.Fatalf("unexpected event for a: %+v", a.received[0])
	}
}

func TestBroadcastExcludesSender(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	b := &fakePeer{id: "b"}
	s.Join(a)
	s.Join(b)
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
	s.Join(a)
	s.Join(b)
	s.Join(c)
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
	s.Join(a)

	err := s.DeliverTo(a, "does-not-exist", wire.NewDirectedMsg("a", "hi", "ts"))
	if err == nil {
		t.Fatal("expected an error targeting a peer that isn't in the session")
	}
}

func TestDeliverToSelfErrors(t *testing.T) {
	m := NewManager()
	s := m.GetOrCreate("session-1")
	a := &fakePeer{id: "a"}
	s.Join(a)

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
	s.Join(a)
	s.Join(b)

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
