package httpmcp

import (
	"context"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/hubsession"
	"github.com/secforge/mcp-hub/internal/wire"
)

func TestNewHTTPPeerHasIdentityAndWatchToken(t *testing.T) {
	p := newHTTPPeer("550e8400-e29b-41d4-a716-446655440000", "Alice", "")
	if p.ID() != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("unexpected ID: %s", p.ID())
	}
	if p.Name() != "Alice" {
		t.Fatalf("unexpected Name: %s", p.Name())
	}
	if p.watchToken == "" {
		t.Fatal("expected a non-empty watch token")
	}
}

func TestDeliverThenDrainReturnsFormattableEvent(t *testing.T) {
	p := newHTTPPeer("550e8400-e29b-41d4-a716-446655440000", "", "")
	p.Deliver(wire.NewBroadcastMsg("6ba7b810-9dad-11d1-80b4-00c04fd430c8", "hi there", "2026-08-28T10:00:00Z", nil, "", "", nil))

	events := p.Drain()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(events), events)
	}
	if events[0].Kind != "msg" || events[0].Text != "hi there" {
		t.Fatalf("unexpected event: %+v", events[0])
	}

	// Drain empties the buffer.
	if events := p.Drain(); len(events) != 0 {
		t.Fatalf("expected Drain to be empty after a prior Drain, got %+v", events)
	}
}

func TestWaitBlocksUntilDeliverThenReturns(t *testing.T) {
	p := newHTTPPeer("550e8400-e29b-41d4-a716-446655440000", "", "")

	type waitResult struct {
		n   int
		err error
	}
	results := make(chan waitResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		events, err := p.Wait(ctx)
		results <- waitResult{n: len(events), err: err}
	}()

	time.Sleep(20 * time.Millisecond) // give Wait time to block first
	p.Deliver(wire.NewBroadcastMsg("6ba7b810-9dad-11d1-80b4-00c04fd430c8", "hi", "2026-08-28T10:00:00Z", nil, "", "", nil))

	select {
	case r := <-results:
		if r.err != nil {
			t.Fatalf("unexpected error: %v", r.err)
		}
		if r.n != 1 {
			t.Fatalf("expected 1 event, got %d", r.n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait never returned after Deliver")
	}
}

func TestWaitReturnsErrorOnContextTimeout(t *testing.T) {
	p := newHTTPPeer("550e8400-e29b-41d4-a716-446655440000", "", "")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := p.Wait(ctx)
	if err == nil {
		t.Fatal("expected an error when the context times out with nothing delivered")
	}
}

func TestWaitReturnsImmediatelyIfAlreadyBuffered(t *testing.T) {
	p := newHTTPPeer("550e8400-e29b-41d4-a716-446655440000", "", "")
	p.Deliver(wire.NewBroadcastMsg("6ba7b810-9dad-11d1-80b4-00c04fd430c8", "hi", "2026-08-28T10:00:00Z", nil, "", "", nil))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	events, err := p.Wait(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 already-buffered event, got %d", len(events))
	}
}

// A peer nothing is draining is given up, not trimmed. Dropping the
// oldest would leave a live connection whose stream has a hole in it and
// every later message arriving looking normal; ending it makes the state
// unambiguous, and everything is still on the server.
func TestAnUndrainedPeerIsAbandonedRatherThanTrimmed(t *testing.T) {
	p := newHTTPPeer("p1", "test", "")
	// Counted through a channel, not a plain int: giving up the
	// connection now runs on its own goroutine, because doing it inline
	// deadlocked the session that was calling Deliver.
	ends := make(chan struct{}, 8)
	p.onAbandon = func() { ends <- struct{}{} }
	ended := func() int { return len(ends) }

	for i := 0; i < maxBufferedEvents; i++ {
		p.Deliver(wire.NewBroadcastMsg("550e8400-e29b-41d4-a716-446655440000", "hello", "2026-09-16T00:00:00Z", nil, "", "", nil))
	}
	if p.Abandoned() || ended() != 0 {
		t.Fatalf("expected a peer within the bound to be left alone, abandoned=%v ended=%d",
			p.Abandoned(), ended())
	}

	p.Deliver(wire.NewBroadcastMsg("550e8400-e29b-41d4-a716-446655440000", "one too many", "2026-09-16T00:00:00Z", nil, "", "", nil))
	if !p.Abandoned() {
		t.Fatal("expected the peer to be abandoned past the bound")
	}
	select {
	case <-ends:
	case <-time.After(2 * time.Second):
		t.Fatal("expected the connection to be ended once the bound was passed")
	}
	// And exactly once: every later Deliver still sees an over-full
	// buffer, and firing again would repeat a teardown already done.
	p.Deliver(wire.NewBroadcastMsg("550e8400-e29b-41d4-a716-446655440000", "and another", "2026-09-16T00:00:00Z", nil, "", "", nil))
	time.Sleep(50 * time.Millisecond)
	if n := ended(); n != 0 {
		t.Fatalf("expected the connection to be ended exactly once, got %d more", n)
	}

	// Nothing was thrown away on the way: what it held is still there to
	// be read, which is what makes "reconnect and catch up" honest.
	if got := len(p.Drain()); got != maxBufferedEvents+2 {
		t.Fatalf("expected everything buffered to survive, got %d", got)
	}
}

// TestAnAbandonedPeerDoesNotDeadlockItsSession is Codex's finding,
// 2026-09-19, reproduced here against the real wiring: Session broadcasts
// with its own mutex held, so a peer that gives up mid-broadcast and
// reaches back into Session.Leave takes a non-reentrant lock its caller
// already holds. The session — every peer in it, not just the one that
// overflowed — stopped there.
//
// The existing overflow test could not see it: its onAbandon only counted,
// so nothing ever re-entered the session.
func TestAnAbandonedPeerDoesNotDeadlockItsSession(t *testing.T) {
	sess := hubsession.NewManager().GetOrCreate("deadlock-session")

	var undrained *httpPeer
	sess.Join("", func(id string) hubsession.Peer {
		undrained = newHTTPPeer(id, "nobody is reading me", "")
		// Exactly what httpmcp's connect path wires up.
		undrained.onAbandon = func() { sess.Leave(undrained) }
		return undrained
	}, nil)

	var reader hubsession.Peer
	sess.Join("", func(id string) hubsession.Peer {
		reader = newHTTPPeer(id, "reader", "")
		return reader
	}, nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i <= maxBufferedEvents+1; i++ {
			sess.Broadcast(reader, wire.NewBroadcastMsg(
				"550e8400-e29b-41d4-a716-446655440000", "flood", "2026-09-19T00:00:00Z", nil, "", "", nil))
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the session deadlocked: a peer abandoned mid-broadcast re-entered Session.Leave " +
			"while the broadcast still held the session lock")
	}
	if !undrained.Abandoned() {
		t.Fatal("expected the undrained peer to have been given up")
	}
}
