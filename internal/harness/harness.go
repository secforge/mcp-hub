// Package harness pushes hub events into the model harness that launched
// this MCP server, so a message arriving on the hub reaches the model
// without the model having asked.
//
// Until now the only way in was the wait socket: the model ran `wait
// --follow` and something on its side turned those writes into
// notifications. That works, and it stays — other clients depend on it —
// but it requires the reader to have started a follower and to keep it
// alive, and a follower that expires or was never armed makes an arriving
// message indistinguishable from no message. Pushing removes that
// requirement for the one process that can always be reached: our own
// parent.
//
// PARENT ONLY. A deliverer addresses the harness that spawned this
// process and nothing else. For Claude that is enforced by the credential
// rather than by this code: the child token lives only in the environment
// the session handed us, is written to no file, and so cannot be picked up
// by another process running as the same user — unlike the hub's own peer
// token, which any such process can read from the key file. This package
// never takes a target from a hub message, a peer, or a tool argument,
// because a transport that can be told where to deliver is a transport
// that can be told to deliver somewhere else.
package harness

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/secforge/harness-transport/deliver"
)

// deliverTimeout bounds one push. A harness that has not taken a message
// in this long is not going to, and the alternative to giving up is
// holding the connection's event loop while it does not.
const deliverTimeout = 30 * time.Second

// Pusher is the hub's use of a deliver.Deliverer: one target, opened once,
// safe to call when there is no harness at all.
type Pusher struct {
	mu sync.Mutex
	d  deliver.Deliverer
	// adopted records that a thread id has been latched from an MCP
	// request, so the Codex path is not asked to re-latch on every call.
	adopted bool
}

// Open returns a Pusher for whichever harness launched this process. It
// never fails: "no harness launched me" is a state to report to a model,
// not an error to retry, so it is reported through Available.
func Open() *Pusher {
	// The sender name is rendered in the harness's own wrapper, and that
	// wrapper describes what arrives as coming from another Claude session
	// working on the user's behalf. That is true of the session-to-session
	// traffic the channel was built for and false of an arbitrary hub
	// peer, so this field — whose job is exactly sender identity — says
	// what the thing actually is rather than restating a name the model
	// already has.
	//
	// It is deliberately over-broad: set once at Open, it labels
	// everything pushed, including the notices this client itself authors.
	// Those carry a "[hub: …]" prefix inside the payload, which a static
	// envelope field cannot, so the two compose — the envelope states the
	// default and the worst case, the prefix marks the exceptions. An
	// inaccurate static name would be worse than an over-broad one.
	return &Pusher{d: deliver.Open(deliver.WithSenderName("mcp-hub relay (external peer)"))}
}

// PushMode reports whether this process should deliver hub events by
// pushing them into its parent harness rather than by waiting to be
// asked. It is keyed on the harness having handed us a messaging socket,
// because that is the same fact that makes pushing possible at all.
//
// Where this is true the pull machinery is not merely redundant, it is
// harmful: a follower and a push are two consumers of one event buffer,
// and whichever drains first hides the event from the other.
func PushMode() bool {
	return os.Getenv(deliver.EnvClaudeSocket) != ""
}

// ClearEnvForTesting removes the inherited harness messaging environment
// and returns a function restoring it. A test binary inherits the
// launching session's socket and token, which makes it indistinguishable
// from the MCP server that ought to be pushing there — see the mcptools
// TestMain for what that cost once.
func ClearEnvForTesting() func() {
	sock, sockOK := os.LookupEnv(deliver.EnvClaudeSocket)
	tok, tokOK := os.LookupEnv(deliver.EnvClaudeToken)
	os.Unsetenv(deliver.EnvClaudeSocket)
	os.Unsetenv(deliver.EnvClaudeToken)
	return func() {
		if sockOK {
			os.Setenv(deliver.EnvClaudeSocket, sock)
		}
		if tokOK {
			os.Setenv(deliver.EnvClaudeToken, tok)
		}
	}
}

// Available reports whether the harness can be reached, with a sentence
// fit to show a model explaining a negative.
func (p *Pusher) Available() (bool, string) {
	if p == nil || p.d == nil {
		return false, "no harness delivery configured in this process"
	}
	return p.d.Available()
}

// Adopt latches the target from an inbound MCP request's _meta. A no-op
// where the target comes from the environment (Claude), and the only way
// to learn it where it does not (Codex), which is why it is called on
// every request rather than at startup: a server that has not yet been
// called has not yet been told which thread it belongs to.
func (p *Pusher) Adopt(meta map[string]any) error {
	if p == nil || p.d == nil || len(meta) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// Every request is passed through, never short-circuited after the
	// first. Latching here looked like a harmless optimisation and was
	// not: the library's own job is to notice when a SECOND thread starts
	// talking to one MCP process and fail closed, and a caller that stops
	// calling after the first success guarantees it never sees the second.
	// A latch that hides the mismatch check is worse than no latch.
	err := p.d.Adopt(meta)
	if err == nil {
		p.adopted = true
	}
	return err
}

// Push delivers one event. The returned observation is what was actually
// OBSERVED, not what was hoped: ObservedNothing means the bytes were
// written and nothing confirmed arrival, which is the most a Claude
// session can honestly report. A caller must not treat it as a read, and
// must not treat its absence as a loss — the cursor is the contract, and
// anything not delivered here is still on the server.
func (p *Pusher) Push(cursor, body string, more bool) (deliver.Receipt, error) {
	if p == nil || p.d == nil {
		return deliver.Receipt{}, fmt.Errorf("no harness delivery configured in this process")
	}
	p.mu.Lock()
	d := p.d
	p.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), deliverTimeout)
	defer cancel()
	return d.Deliver(ctx, deliver.Delivery{Cursor: cursor, Body: body, More: more})
}

// MaxIntactBytes is the largest body the harness will deliver whole. It is
// a guaranteed floor rather than the point at which delivery starts
// failing, and the distinction is the whole reason it is not used to size
// anything: a floor of N licenses sending N and licenses nothing about
// N+1. The delivery budget stays well below it by its own reasoning.
func (p *Pusher) MaxIntactBytes() (int, error) {
	if p == nil || p.d == nil {
		return 0, fmt.Errorf("no harness delivery configured in this process")
	}
	p.mu.Lock()
	d := p.d
	p.mu.Unlock()
	return d.MaxIntactBytes()
}

// Close releases the connection held, if any.
func (p *Pusher) Close() error {
	if p == nil || p.d == nil {
		return nil
	}
	return p.d.Close()
}
