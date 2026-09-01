// Package httpmcp implements a Streamable-HTTP MCP endpoint for
// mcp-hub-server, backed directly by internal/hubsession — an in-process
// alternative to mcp-hub-client dialing out over a websocket to itself.
package httpmcp

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/google/uuid"

	"github.com/secforge/mcp-hub/internal/hubconn"
)

// httpPeer implements hubsession.Peer directly, buffering delivered events
// for hub_receive/hub_wait/the /watch endpoint to drain — a lighter,
// in-process sibling of hubconn.Conn's own buffer/Peek/Drain, minus
// websocket/reconnect machinery, since events here are native Go values
// (from Session.Broadcast/DeliverTo/Join) rather than JSON read off a
// socket.
type httpPeer struct {
	id           string
	name         string
	agePublicKey string

	// watchToken identifies this exact peer to the /watch endpoint —
	// unrelated to peerId or sessionId, minted once at connect time and
	// known only to whoever received it from hub_connect's result.
	watchToken string

	mu     sync.Mutex
	buffer []hubconn.Event
	// woken is closed (and immediately replaced) every time Deliver adds to
	// buffer, waking anything blocked in Wait — the same
	// close-and-replace-a-channel pattern used to broadcast "something
	// changed" to any number of waiters without a lost-wakeup race.
	woken chan struct{}
}

func newHTTPPeer(id, name, agePublicKey string) *httpPeer {
	return &httpPeer{
		id:           id,
		name:         name,
		agePublicKey: agePublicKey,
		watchToken:   uuid.NewString(),
		woken:        make(chan struct{}),
	}
}

func (p *httpPeer) ID() string           { return p.id }
func (p *httpPeer) Name() string         { return p.name }
func (p *httpPeer) AgePublicKey() string { return p.agePublicKey }

// Deliver implements hubsession.Peer. event is always one of the wire.*
// values Session.Join/Leave/Broadcast/DeliverTo already construct for the
// websocket path (wire.PeerEvent, wire.RosterComplete, wire.Msg,
// wire.Error) — marshaling it back to JSON and decoding that via
// hubconn.DecodeEvent reuses that package's already-tested
// decode/normalize logic instead of a second, hand-rolled conversion.
func (p *httpPeer) Deliver(event any) {
	raw, err := json.Marshal(event)
	if err != nil {
		return
	}
	ev, ok := hubconn.DecodeEvent(raw)
	if !ok {
		return
	}
	p.mu.Lock()
	p.buffer = append(p.buffer, ev)
	close(p.woken)
	p.woken = make(chan struct{})
	p.mu.Unlock()
}

// Drain returns and clears everything buffered so far, without blocking.
func (p *httpPeer) Drain() []hubconn.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	events := p.buffer
	p.buffer = nil
	return events
}

// Wait blocks until at least one event is buffered or ctx is done, then
// drains and returns everything buffered. Returns a non-nil error (ctx's
// own error) on cancellation/timeout with nothing delivered.
func (p *httpPeer) Wait(ctx context.Context) ([]hubconn.Event, error) {
	for {
		p.mu.Lock()
		if len(p.buffer) > 0 {
			events := p.buffer
			p.buffer = nil
			p.mu.Unlock()
			return events, nil
		}
		woken := p.woken
		p.mu.Unlock()

		select {
		case <-woken:
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
