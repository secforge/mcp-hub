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
	"strings"
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
	// d is opened on first use, not at construction, because the name it
	// is opened with is part of what the model sees and may only be
	// learnable from an inbound request. Opening eagerly would fix the
	// attribution before the one source that could improve it has spoken.
	d deliver.Deliverer
	// learned is a server name taken from request metadata, if the harness
	// supplies one. Empty when it does not.
	learned string
	// adopted records that a thread id has been latched from an MCP
	// request, so the Codex path is not asked to re-latch on every call.
	adopted bool
}

// Open returns a Pusher for whichever harness launched this process. It
// never fails: "no harness launched me" is a state to report to a model,
// not an error to retry, so it is reported through Available.
func Open() *Pusher {
	// The sender name is rendered in the harness's own wrapper, and is
	// what the model sees attributing a delivered message. It is
	// "mcp:<server>" so a reader can tell at a glance which MCP server
	// spoke and that an MCP server is what spoke — the wrapper otherwise
	// describes an arbitrary hub peer as another Claude session working on
	// the user's behalf, which is true of the session-to-session traffic
	// that channel was built for and false of our cargo.
	//
	// The name is short on purpose: the field is truncated in display, so
	// a long one loses its distinguishing tail, which is the half that
	// says WHICH server. The trust framing it replaces is not lost — every
	// relayed message carries "untrusted" as the first word of its own
	// header, in the payload, where truncation cannot reach.
	//
	// The name is taken from the harness when it offers one (see
	// learnServerName) and configured only when it does not. The
	// environment carries no alias — verified against a live process, it
	// passes the messaging socket, token, session id and project dir and
	// nothing else — and two entries can run the same binary with
	// identical arguments, so without either source they are
	// indistinguishable.
	return &Pusher{}
}

// deliverer returns the underlying Deliverer, opening it on first use with
// the best name known by then. Caller must hold p.mu.
func (p *Pusher) deliverer() deliver.Deliverer {
	if p.d == nil {
		p.d = deliver.Open(deliver.WithSenderName(senderName(p.learned)))
	}
	return p.d
}

// learnServerName looks for a server name the harness may have stamped
// into an inbound request's metadata, so the attribution names the right
// entry without anyone having to configure it.
//
// Speculative by necessity and safe by construction: if the name is
// available at all, request metadata is where it would be, so matching is
// by shape rather than by a key known to exist, and anything unrecognised
// leaves the name alone. The value is a display string, never an identity
// or an authorization, so a wrong guess mislabels a line and nothing more.
func learnServerName(meta map[string]any) string {
	for k, v := range meta {
		s, ok := v.(string)
		if !ok || s == "" || len(s) > 64 {
			continue
		}
		key := strings.ToLower(k)
		if strings.Contains(key, "server") && strings.Contains(key, "name") {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

// EnvServerName names this MCP server as the model's own config registered
// it — "mcp-hub2" for the second entry, say. Set it in the server's env
// block; without it every entry running this binary is indistinguishable
// in the attribution line.
const EnvServerName = "MCP_HUB_SERVER_NAME"

// senderName builds the attribution shown to the model. An explicitly
// configured name wins: it is the operator stating what this entry is
// called, which outranks anything inferred. A name learned from the
// harness comes next, and the product name last, so the field always says
// something rather than nothing.
func senderName(learned string) string {
	name := safeServerName(os.Getenv(EnvServerName))
	if name == "" {
		name = safeServerName(learned)
	}
	if name == "" {
		name = "mcp-hub"
	}
	return "mcp:" + name
}

// maxServerName keeps "mcp:" + the name inside the 64 characters the
// receiving harness allows before it truncates and appends an ellipsis.
const maxServerName = 64 - len("mcp:")

// safeServerName reduces a configured or learned name to what will survive
// display unchanged.
//
// The receiving harness sanitises this field itself: it strips quotes and
// angle brackets rather than escaping them, removes invisibles, trims, and
// truncates past 64 characters. All of that is silent and happens only in
// the display, so a name set by hand can arrive as something else with
// nothing anywhere reporting the difference — the attribution would then
// be wrong in exactly the situation it exists for, telling a reader which
// of several identical servers spoke.
//
// Normalising here makes the transformation ours and visible in one place,
// rather than someone else's and invisible. Anything outside a plain
// identifier is dropped instead of substituted: a name that loses a
// character is obviously wrong to whoever configured it, while one whose
// characters were quietly replaced looks deliberate.
func safeServerName(raw string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(raw) {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		}
		if b.Len() >= maxServerName {
			break
		}
	}
	return b.String()
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
	if p == nil {
		return false, "no harness delivery configured in this process"
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.deliverer().Available()
}

// Adopt latches the target from an inbound MCP request's _meta. A no-op
// where the target comes from the environment (Claude), and the only way
// to learn it where it does not (Codex), which is why it is called on
// every request rather than at startup: a server that has not yet been
// called has not yet been told which thread it belongs to.
func (p *Pusher) Adopt(meta map[string]any) error {
	if p == nil || len(meta) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.learned == "" && p.d == nil {
		// Only before the deliverer exists: its name is fixed at open, so
		// learning one afterwards would record a name that is not the one
		// being sent under, which is worse than the default.
		p.learned = learnServerName(meta)
	}
	// Every request is passed through, never short-circuited after the
	// first. Latching here looked like a harmless optimisation and was
	// not: the library's own job is to notice when a SECOND thread starts
	// talking to one MCP process and fail closed, and a caller that stops
	// calling after the first success guarantees it never sees the second.
	// A latch that hides the mismatch check is worse than no latch.
	err := p.deliverer().Adopt(meta)
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
	if p == nil {
		return deliver.Receipt{}, fmt.Errorf("no harness delivery configured in this process")
	}
	p.mu.Lock()
	d := p.deliverer()
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
	if p == nil {
		return 0, fmt.Errorf("no harness delivery configured in this process")
	}
	p.mu.Lock()
	d := p.deliverer()
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
