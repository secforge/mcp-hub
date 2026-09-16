package harness

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/secforge/harness-transport/udsmsg"
)

// Inbox is the return path: an address the model can send to, so a reply
// to a hub message uses the harness's own SendMessage rather than a
// separate tool.
//
// It exists because the two directions were asymmetric. A pushed hub
// message arrives as a cross-session message whose from= the model is
// told to copy as its `to`; without an inbox there is nothing at that
// address, so the model has to notice it is talking to a hub and switch
// to hub_send. With one, replying works the way replying already works.
//
// PARENT ONLY, and the enforcement is deliberately two-layered, because
// the two layers prove different things:
//
//   - RequireAuth proves the sender could read this machine's key file.
//     That is a gate, not an identity: measured by harness-transport on
//     2026-09-16, a model replying from its own session presents the PEER
//     token rather than the child token, so the token alone says "some
//     session here", not "my parent".
//   - The pid from the kernel is the identity. peerCred reports it and
//     the peer's start time, neither of which the sender can choose, and
//     it is compared against the pid of the session that spawned this
//     process. That is the same asymmetry as the push side, where the
//     child token authenticates and the socket names the target.
//
// An unidentified peer is refused rather than accepted with a warning: a
// platform that cannot report credentials cannot support parent-only, and
// pretending otherwise would make the guarantee weakest exactly where it
// is least visible.
type Inbox struct {
	mu     sync.Mutex
	srv    *udsmsg.Server
	parent int32
	// onMessage receives text a verified parent sent. Nil until Start.
	onMessage func(text string)
}

// EnvClaudeSocketName is the harness session's inbox path, which also
// names the pid a reply must come from.
const EnvClaudeSocketName = "CLAUDE_CODE_MESSAGING_SOCKET"

// frameText pulls the prompt out of a user frame.
func frameText(f *udsmsg.Frame) string {
	if f == nil || f.Message == nil {
		return ""
	}
	return strings.TrimSpace(f.Message.Content)
}

// OpenInbox binds a return-path inbox for this process's own parent and
// returns it stopped; call Start to serve. It fails rather than degrading
// when the parent cannot be identified — an inbox nobody can be
// authenticated against is not a smaller version of this feature.
func OpenInbox() (*Inbox, error) {
	sock := os.Getenv(EnvClaudeSocketName)
	if sock == "" {
		return nil, fmt.Errorf("no harness messaging socket in the environment, so there is no " +
			"parent session to accept replies from")
	}
	pid, ok := udsmsg.PIDFromSocketName(baseName(sock))
	if !ok {
		return nil, fmt.Errorf("could not read a pid from the harness socket name %q, so a reply "+
			"could not be checked against the session that spawned this process", sock)
	}
	// The Inbox exists before the server because the handler is fixed at
	// bind time and has to be able to reach it — there is no window in
	// which the socket is accepting with no handler behind it.
	in := &Inbox{parent: int32(pid)}
	srv, err := udsmsg.Listen(udsmsg.Config{
		Handler: udsmsg.Handler{OnUser: in.handleUser},
		// Auth is required and the key is published so a reply from the
		// model's own session can present a token at all. Neither decides
		// who may send — see the type comment.
		RequireAuth: true,
		PublishKey:  true,
		// A reply carries a from= address and a sender that waits on
		// delivery status; answering it is what a session does.
		AutoStatus: true,
		// Never: a peer whose credentials the kernel will not report
		// cannot be compared to our parent, so it cannot be accepted.
		AllowUnidentifiedPeers: false,
	})
	if err != nil {
		return nil, fmt.Errorf("could not bind a return-path inbox: %w", err)
	}
	in.srv = srv
	return in, nil
}

// Address is the string to advertise as this delivery's reply address,
// in the "uds:<path>" form the harness hands the model as from=.
func (i *Inbox) Address() string {
	if i == nil || i.srv == nil {
		return ""
	}
	return i.srv.Addr()
}

// Start serves the inbox, calling onMessage for each message a verified
// parent sends. Returns immediately; the server runs until Close.
func (i *Inbox) Start(ctx context.Context, onMessage func(text string)) {
	i.mu.Lock()
	i.onMessage = onMessage
	i.mu.Unlock()
	i.srv.Start(ctx)
}

// handleUser accepts a message only from the process that spawned this
// one. Everything else is dropped silently on the wire but not silently
// in the design: see the type comment for why the token cannot make this
// decision.
func (i *Inbox) handleUser(_ context.Context, p *udsmsg.Peer, f *udsmsg.Frame) {
	if p == nil || !p.Identified || p.PID != i.parent {
		return
	}
	i.mu.Lock()
	fn := i.onMessage
	i.mu.Unlock()
	if fn == nil {
		return
	}
	if text := frameText(f); text != "" {
		fn(text)
	}
}

// Close stops serving and removes the socket.
func (i *Inbox) Close() error {
	if i == nil || i.srv == nil {
		return nil
	}
	return i.srv.Close()
}

// baseName is filepath.Base without importing path/filepath for one call
// on a path the kernel gave us.
func baseName(p string) string {
	if idx := strings.LastIndexByte(p, '/'); idx >= 0 {
		return p[idx+1:]
	}
	return p
}
