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
	for _, want := range []string{"membership changed", "xavier (x): joined → left", "yvonne (y): joined",
		"alpha (a): left → reconnected", "Now 2 here"} {
		if !strings.Contains(got, want) {
			t.Fatalf("lacks %q: %s", want, got)
		}
	}
	if c.flushRosterLocked() {
		t.Fatal("a second flush delivered something")
	}
}
