package hubconn

import (
	"strings"
	"testing"
	"time"
)

// Member lists seconds apart go out as one, saying who came and went in
// order — including a peer that joined and left inside the window.
func TestMemberListsInsideTheWindowAreMergedWithWhatChanged(t *testing.T) {
	defer SetRosterMergeWindowForTesting(time.Hour)()
	defer SetRosterLeaveHoldForTesting(time.Hour)()
	a := PeerInfo{ID: "a", Name: "alpha"}
	x := PeerInfo{ID: "x", Name: "xavier"}
	y := PeerInfo{ID: "y", Name: "yvonne"}
	c := &Conn{}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{a}})
	if len(c.buffer) != 1 {
		t.Fatalf("the first list was not delivered at once: %d buffered", len(c.buffer))
	}
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{a, x}})
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{a, y}})
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{a, y}})
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{y}})
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{a, y}})
	if len(c.buffer) != 1 {
		t.Fatalf("a change inside the window went out on its own: %d buffered", len(c.buffer))
	}
	if !c.flushRosterLocked() || len(c.buffer) != 2 {
		t.Fatal("the held list was not delivered")
	}
	got := FormatEvent(c.buffer[1])
	for _, want := range []string{"membership changed", "xavier (x): joined", "yvonne (y): joined", "Now 3 here"} {
		if !strings.Contains(got, want) {
			t.Fatalf("lacks %q: %s", want, got)
		}
	}
	if strings.Contains(got, "alpha (a):") || strings.Contains(got, "left") {
		t.Fatalf("a leave inside its hold was said: %s", got)
	}
	if c.flushRosterLocked() {
		t.Fatal("a second flush delivered something")
	}
}

// After a restart, peers that were here before and come back are not
// news; a new peer is, and so is one that does not come back in time.
func TestPeersComingBackAfterARestartAreNotNews(t *testing.T) {
	defer SetRosterMergeWindowForTesting(time.Hour)()
	a := PeerInfo{ID: "a", Name: "alpha"}
	x := PeerInfo{ID: "x", Name: "xavier"}
	y := PeerInfo{ID: "y", Name: "yvonne"}
	z := PeerInfo{ID: "z", Name: "zora"}
	c := &Conn{activity: make(chan struct{}, 1)}
	c.mu.Lock()
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{a}})
	c.mu.Unlock()
	c.ExpectBack([]PeerInfo{a, x, y}, 100*time.Millisecond)

	c.mu.Lock()
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{a, x}})
	if c.rosterHeld != nil {
		t.Fatal("a peer coming back after the restart was held as a change")
	}
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{a, x, z}})
	if !c.flushRosterLocked() {
		t.Fatal("a new peer was not reported")
	}
	if got := FormatEvent(c.buffer[len(c.buffer)-1]); !strings.Contains(got, "zora (z): joined") || strings.Contains(got, "xavier (x): joined") {
		t.Fatalf("change: %s", got)
	}
	n := len(c.buffer)
	c.mu.Unlock()

	time.Sleep(250 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.buffer) != n+1 {
		t.Fatalf("a peer that stayed away was not reported: %d buffered", len(c.buffer))
	}
	if got := FormatEvent(c.buffer[n]); !strings.Contains(got, "yvonne (y): left, and did not come back") {
		t.Fatalf("missing peer: %s", got)
	}
}

// A peer that leaves and comes back within the hold was never news; one
// that stays away is said once the hold has passed.
func TestALeaveIsSaidOnlyIfThePeerStaysAway(t *testing.T) {
	defer SetRosterMergeWindowForTesting(time.Hour)()
	defer SetRosterLeaveHoldForTesting(80 * time.Millisecond)()
	a := PeerInfo{ID: "a", Name: "alpha"}
	b := PeerInfo{ID: "b", Name: "bravo"}
	c := &Conn{activity: make(chan struct{}, 1)}
	c.mu.Lock()
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{a, b}})
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{b}})
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{a, b}})
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{a}})
	n := len(c.buffer)
	c.mu.Unlock()
	time.Sleep(250 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.buffer) != n+1 {
		t.Fatalf("%d deliveries after the hold, want 1", len(c.buffer)-n)
	}
	got := FormatEvent(c.buffer[n])
	if !strings.Contains(got, "bravo (b): left") || strings.Contains(got, "alpha (a):") {
		t.Fatalf("change: %s", got)
	}
}

// While a leave is held, the list in a notice still counts that peer: the
// notice has not said it left.
func TestAPeerWhoseLeaveIsHeldIsStillListed(t *testing.T) {
	defer SetRosterMergeWindowForTesting(time.Hour)()
	defer SetRosterLeaveHoldForTesting(time.Hour)()
	a := PeerInfo{ID: "a", Name: "alpha"}
	b := PeerInfo{ID: "b", Name: "bravo"}
	c := &Conn{activity: make(chan struct{}, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{a}})
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{}})
	c.holdRosterLocked(Event{Kind: "roster", RosterPeers: []PeerInfo{b}})
	if !c.flushRosterLocked() {
		t.Fatal("the join was not delivered")
	}
	got := FormatEvent(c.buffer[len(c.buffer)-1])
	if !strings.Contains(got, "bravo (b): joined") || !strings.Contains(got, "Now 2 here") || !strings.Contains(got, "alpha (a)") {
		t.Fatalf("notice: %s", got)
	}
}
