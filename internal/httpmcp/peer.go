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
	"github.com/secforge/mcp-hub/internal/wire"
)

// maxBufferedEvents bounds what one HTTP-MCP peer may hold undrained.
//
// Generous — a reader that is working never approaches it — and finite,
// because the alternative is one abandoned MCP session holding every
// message of a busy conversation until the process ends.
const maxBufferedEvents = 2000

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
	// dropped counts events discarded because the buffer was full, so the
	// reader is TOLD rather than handed a silently incomplete stream. A
	// gap nobody mentions is the one thing this project refuses to
	// produce; the count turns it into a stated one, recoverable with
	// hub_catch_up.
	dropped int
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
	// Bounded, because this buffer belongs to a reader that may never
	// come back: an MCP session that stopped calling hub_receive leaves
	// this growing for the life of the process, holding every message of
	// a busy conversation. Past the bound the OLDEST are dropped — what
	// is dropped is still on the server and reachable by catch-up, while
	// the newest is what a returning reader actually needs.
	if len(p.buffer) >= maxBufferedEvents {
		drop := len(p.buffer) - maxBufferedEvents + 1
		p.buffer = append([]hubconn.Event(nil), p.buffer[drop:]...)
		p.dropped += drop
	}
	p.buffer = append(p.buffer, ev)
	close(p.woken)
	p.woken = make(chan struct{})
	p.mu.Unlock()
}

// TakeDropped reports how many events this peer discarded since the last
// call, and clears the count. Callers surface it to the reader: a
// truncated stream that says so is recoverable, one that does not is the
// failure this whole codebase exists to prevent.
func (p *httpPeer) TakeDropped() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := p.dropped
	p.dropped = 0
	return n
}

// Close implements hubsession.Peer — see its doc comment. httpPeer has no
// real transport to sever (it's an in-process buffer, not a socket), so
// this delivers a synthetic error event instead, which surfaces through
// the next hub_receive/hub_wait exactly like any other server-sent
// error. Known simplification: the httpHub that owns this peer (see
// hub.go) has no background loop watching for this the way wsserver's
// peer does, so nothing here clears hub.p — a subsequent hub_send against
// this now-superseded connection will still nominally reach the session
// under a peerID it no longer actually owns, until an explicit
// hub_disconnect. Full disconnect-detection for this in-process path is
// a separate gap, not solved here.
func (p *httpPeer) Close(code int, reason string) {
	p.Deliver(wire.NewError(reason))
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
