package httpmcp

import (
	"context"
	"testing"
	"time"

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
