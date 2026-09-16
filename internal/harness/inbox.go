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
	// published is the pid of the registry entry to remove on close, or
	// zero. An entry outliving its process is a listing that points at a
	// socket nobody is serving.
	published int
	// diagnostic holds the last thing worth telling a reader about the
	// wire: a held-message status with its cause, or whether an arriving
	// user frame asserted a permission mode. Read once and cleared.
	//
	// It exists because three sessions spent an hour inferring the gate's
	// cause from absences, while the answer was in a field on a frame
	// this process already receives. A status carries `cause` naming the
	// branch that held it; a user frame either carries from_mode or does
	// not. Both are facts rather than deductions.
	diagnostic string
}

// EnvClaudeSocketName is the harness session's inbox path, which also
// names the pid a reply must come from.
const EnvClaudeSocketName = "CLAUDE_CODE_MESSAGING_SOCKET"

// frameText pulls the model's own words out of a user frame.
//
// The harness wraps every SendMessage in a <cross-session-message …>
// envelope, so the frame's content is that wrapper around the text, not
// the text. Relaying it verbatim put the whole envelope on the hub —
// opening tag, attributes, hop chain and all — which is how this was
// found: the escape that rewrites a nested closing tag fired, and the
// mangled "<\" it left behind was the proof the wrapper was still there.
//
// Unwrap reports ok=false when the content does not round-trip byte for
// byte, and then the content is used as-is: a body that cannot be
// extracted safely is better relayed whole than silently truncated at
// whatever the parse thought was the end.
func frameText(f *udsmsg.Frame) string {
	if f == nil || f.Message == nil {
		return ""
	}
	content := f.Message.Content
	if _, body, ok := udsmsg.Unwrap(content); ok {
		content = body
	}
	return strings.TrimSpace(content)
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
		Handler: udsmsg.Handler{
			OnUser: in.handleUser,
			// The receiver builds this and sends it back here: it is the
			// only place the hold's own cause is stated.
			OnPeerMessageStatus: in.handleStatus,
		},
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
		// Derive the permission posture from the session that spawned
		// this process, and bind anyway when it cannot be derived.
		//
		// Parity is what decides whether a reply is delivered or held for
		// the user's approval: a frame asserting a mode equal to the
		// receiver's is accepted, a different one is held, and one
		// asserting nothing is held only by a bypass receiver. An inbox
		// that asserted nothing was therefore held by construction, which
		// is what made every reply cost an approval round-trip.
		//
		// DERIVED rather than REQUIRED because detection reads
		// /proc/<pid>/cmdline, which darwin and windows do not have.
		// Requiring it would refuse to bind on three of the five
		// platforms this ships on — losing the return path entirely to
		// avoid a hold that on those platforms is merely noise. Failing
		// to detect costs a hold; failing to bind costs the feature.
		//
		// The posture is never a value this code chooses: the field names
		// a SOURCE and cannot name a mode, so nothing here can claim to
		// be something it is not. Asserting one outright is possible only
		// through Server.AssertModeUnverified, which logs that it was
		// used.
		ModeSource: inboxModeSource(),
	})
	if err != nil {
		return nil, fmt.Errorf("could not bind a return-path inbox: %w", err)
	}
	in.srv = srv
	return in, nil
}

// Publish writes a registry entry so the inbox is addressable by name in
// the harness's own session list, and returns whether it was written.
//
// The entry is honest about what it is: kind and entrypoint "mcp" rather
// than "interactive", named for the parent session AND this server, so a
// reader sees which conversation it belongs to and which MCP it is.
// Nothing claims to be a conversation. That distinction is the whole
// reason this is acceptable — the objection was never to having an entry,
// it was to appearing as a session this process is not.
//
// It also removes one cost of the return path. A reply to an UNREGISTERED
// inbox was held for the user's approval; an entry may be what the
// harness uses to tell a known target from an unknown one. Measured, not
// assumed: see the note this reports back to the caller.
func (i *Inbox) Publish(mcpName string) error {
	if i == nil || i.srv == nil {
		return fmt.Errorf("no inbox to publish")
	}
	e, err := udsmsg.NewMCPEntry(i.srv.Path(), udsmsg.ParentSessionName(), mcpName)
	if err != nil {
		return fmt.Errorf("could not build a registry entry: %w", err)
	}
	if err := udsmsg.PublishSession(e); err != nil {
		return fmt.Errorf("could not publish the registry entry: %w", err)
	}
	i.mu.Lock()
	i.published = e.PID
	i.mu.Unlock()
	return nil
}

// inboxModeSource is the posture source every inbox binds with, named so
// a test can assert the choice rather than re-reading the literal.
func inboxModeSource() udsmsg.ModeSource { return udsmsg.ModeSourceDerived }

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
	// Whether the sender asserted a permission mode is the other half of
	// the gate question, and it is in the bytes rather than in anyone's
	// reading of the docs.
	// The frame field and the ENVELOPE are different lanes, and the gated
	// one is the envelope: the receiver parses <cross-session-message …>
	// out of the content and reads from-mode from there. The frame's own
	// fromMode belongs to host-injection on local stdin.
	//
	// The envelope only counts if it round-trips byte for byte — the
	// receiver re-renders what it parsed and compares, and any mismatch
	// silently discards the attribution, mode included. So "no mode
	// asserted" and "mode discarded by a failed parse" are different
	// facts that look identical downstream, and only here can they be
	// told apart.
	if f != nil {
		frameMode := "none"
		if f.FromMode != "" {
			frameMode = string(f.FromMode)
		}
		envelope := "no envelope parsed — attribution would be discarded"
		if f.Message != nil {
			if cs, _, ok := udsmsg.Unwrap(f.Message.Content); ok {
				mode := string(cs.Mode)
				if mode == "" {
					mode = "none"
				}
				envelope = "envelope parsed, from-mode=" + mode
			} else if f.Message.Content != "" &&
				strings.Contains(f.Message.Content, "cross-session-message") {
				envelope = "envelope PRESENT but did not round-trip — attribution discarded, " +
					"which is indistinguishable downstream from no mode being asserted"
			}
		}
		i.mu.Lock()
		i.diagnostic = "last reply: frame from_mode=" + frameMode + "; " + envelope
		i.mu.Unlock()
	}
	if text := frameText(f); text != "" {
		fn(text)
	}
}

// handleStatus records a delivery status verbatim. Held and refused ones
// carry the reason; a plain "delivered" is not worth reporting.
func (i *Inbox) handleStatus(_ context.Context, _ *udsmsg.Peer, f *udsmsg.Frame) {
	if f == nil || len(f.Raw) == 0 {
		return
	}
	raw := string(f.Raw)
	if !strings.Contains(raw, "held") && !strings.Contains(raw, "refused") &&
		!strings.Contains(raw, "cause") {
		return
	}
	i.mu.Lock()
	i.diagnostic = "delivery status from the harness: " + raw
	i.mu.Unlock()
}

// TakeDiagnostic returns the last wire fact worth reporting and clears
// it. Read once: it describes something that happened, not a state.
func (i *Inbox) TakeDiagnostic() string {
	if i == nil {
		return ""
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	d := i.diagnostic
	i.diagnostic = ""
	return d
}

// Close stops serving and removes the socket.
func (i *Inbox) Close() error {
	if i == nil || i.srv == nil {
		return nil
	}
	i.mu.Lock()
	pid := i.published
	i.published = 0
	i.mu.Unlock()
	if pid != 0 {
		// Before closing the socket, so there is no window where the
		// listing names an address that has already stopped answering.
		_ = udsmsg.UnpublishSession(pid)
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
