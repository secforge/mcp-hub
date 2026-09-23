package httpmcp

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/hubsession"
	"github.com/secforge/mcp-hub/internal/wire"
)

func TestOverflowDoesNotDeadlockSession(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	_, id, _, err := s.connect("reader", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = s.connect("sender", id, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	h := s.hubFor("sender")
	sess, sender := h.session(), h.peer()
	done := make(chan struct{})
	go func() {
		for i := 0; i <= maxBufferedEvents; i++ {
			sess.Broadcast(sender, wire.NewBroadcastMsg(sender.ID(), "test", "", nil, "", "", nil))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("broadcast deadlocked on undrained HTTP peer")
	}
}

func TestDisconnectWakesWait(t *testing.T) {
	skipKnownIssue(t, "HTTP disconnect leaves waits and watch streams blocked")
	s := NewServer(hubsession.NewManager())
	_, _, _, err := s.connect("reader", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	p := s.hubFor("reader").peer()
	p.Drain()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.disconnect("reader")
	done := make(chan struct{})
	go func() { p.Wait(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("disconnected peer leaves waiter blocked")
	}
}

func TestRemovedSessionCannotAcceptInvisibleJoin(t *testing.T) {
	m := hubsession.NewManager()
	s := NewServer(m)
	_, id, _, err := s.connect("first", "", "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	// A concurrent connect can hold this pointer before its Join runs.
	pending := m.GetOrCreate(id)
	s.disconnect("first")
	pending.Join("", func(id string) hubsession.Peer { return newHTTPPeer(id, "", "") }, nil)
	if m.GetOrCreate(id) != pending {
		t.Fatal("pending join entered detached session; subsequent peers use a different session")
	}
}

func TestSupersededHTTPPeerCannotSendAsReplacement(t *testing.T) {
	skipKnownIssue(t, "superseded HTTP peers can still send as the replacement identity")
	s := NewServer(hubsession.NewManager())
	_, id, _, err := s.connect("old", "", "", "", "same-secret")
	if err != nil {
		t.Fatal(err)
	}
	old := s.hubFor("old").peer()
	_, _, _, err = s.connect("observer", id, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	observer := s.hubFor("observer").peer()
	_, _, _, err = s.connect("replacement", id, "", "", "same-secret")
	if err != nil {
		t.Fatal(err)
	}
	observer.Drain()
	h := s.hubFor("old")
	if h.peer() == nil {
		return
	}
	h.session().Broadcast(old, wire.NewBroadcastMsg(old.ID(), "from superseded peer", "", nil, "", "", nil))
	for _, ev := range observer.Drain() {
		if ev.Text == "from superseded peer" {
			t.Fatal("superseded HTTP connection can still broadcast under reclaimed identity")
		}
	}
}

// skipKnownIssue skips a reproduction of a defect recorded in
// docs/known-issues.md that is still present. Set MCP_HUB_KNOWN_ISSUES=1 to
// run it; it fails until the defect is fixed, and then the skip goes.
func skipKnownIssue(t *testing.T, issue string) {
	t.Helper()
	if os.Getenv("MCP_HUB_KNOWN_ISSUES") == "" {
		t.Skipf("known issue, still present: %s (docs/known-issues.md); MCP_HUB_KNOWN_ISSUES=1 runs it", issue)
	}
}
