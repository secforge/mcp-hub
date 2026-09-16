package waiter

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Source is the buffered-event side of a hub connection (satisfied by
// *hubconn.Conn).
type Source interface {
	// Peek reports whether unread events are buffered, and whether the
	// underlying connection is still open. Non-destructive.
	Peek() (hasEvents, connected bool)
	// Drain clears and returns the buffered events formatted for delivery,
	// and whether the connection is still open.
	Drain() (formatted string, connected bool)
	// DrainBatch is Drain, but keeps each buffered event as its own
	// formatted string rather than one joined blob — used by follow-mode
	// delivery so a multi-event burst goes out as one network write per
	// event instead of a single write a downstream layer could truncate
	// as one unit with no signal anything was cut. See
	// hubconn.FormatEventsBatch's doc comment for the full reasoning
	// (confirmed live, 2026-09-03: the actual truncation observed against
	// this protocol happens downstream of this client, in a
	// notification/display layer this codebase doesn't own — this can't
	// prevent that layer from re-merging separate writes, but removing
	// the batching this code itself used to do is still strictly better
	// than not doing it, and every event's own per-event marker — see
	// FormatEventsBatch — survives even a re-merge).
	DrainBatch() (chunks []string, connected bool)
}

// Mode selector bytes a `wait` client sends as the first byte after
// connecting, choosing how the server treats that connection.
const (
	ModeOnce   = 0x00 // deliver one event, then close (the default `wait` behavior)
	ModeFollow = 0x01 // deliver events repeatedly over the same connection (`wait --follow`)
)

type registeredWaiter struct {
	conn   net.Conn
	follow bool
}

// attached is one connection this waiter carries, and the per-connection
// facts that used to be the whole waiter's.
//
// ONE SOCKET FOR THE PROCESS is not an optimisation. A pull harness runs
// one `wait` process; it cannot run one per connection, so a second
// socket is a connection nothing is listening to — which is exactly the
// silence this component exists to prevent.
//
// The cost is that "the connection ended" and "the connection is coming
// back" stop being facts about the channel and become facts about one
// conversation ON the channel. They are therefore delivered as labelled
// MESSAGES rather than as the socket closing: the follower survives any
// one connection ending, and closing is reserved for the process itself
// going away.
type attached struct {
	name string
	src  Source
	// expecting is set while a planned reconnect is in flight for THIS
	// connection. It changes what a dead source means: normally the
	// connection ending is the end of that conversation and the follower
	// is told so, but during an announced restart the connection is
	// coming back and the reader should still be attached when it does.
	expecting bool
	// held records that the reader has already been told this connection
	// is holding, so a burst of pokes during the outage does not repeat
	// it.
	held bool
}

type Waiter struct {
	ln         net.Listener
	socketPath string

	mu sync.Mutex
	// sources is every connection this socket carries, and order keeps
	// delivery stable so two runs of the same traffic read the same.
	sources map[string]*attached
	order   []string
	current *registeredWaiter
	// closed is set by Close under w.mu. A delivery in flight has
	// unregistered its reader, so Close cannot see it to say goodbye —
	// this is what lets the delivery discover, when it comes back to
	// re-register, that there is no longer a Waiter to register with.
	closed bool
}

// ExpectReconnect tells this waiter that one connection is about to die
// on purpose and will be replaced. The reader is kept attached across the
// gap rather than told that conversation ended.
func (w *Waiter) ExpectReconnect(name string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	if a := w.sources[name]; a != nil {
		a.expecting, a.held = true, false
	}
	w.mu.Unlock()
}

// SetSource attaches a connection under name, or swaps in the one that
// replaced it, ending any hold. The socket path never changes, so a
// reader that survived a gap keeps receiving without knowing anything
// happened.
func (w *Waiter) SetSource(name string, s Source) {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.sources == nil {
		w.sources = map[string]*attached{}
	}
	if a := w.sources[name]; a != nil {
		a.src, a.expecting, a.held = s, false, false
	} else {
		w.sources[name] = &attached{name: name, src: s}
		w.order = append(w.order, name)
	}
	w.mu.Unlock()
	w.Poke()
}

// Detach removes one connection from this waiter and tells whoever is
// reading, by name. The socket stays open: another conversation may still
// be running on it, and even if none is, a reader released here could not
// be reattached when the next connect happens.
func (w *Waiter) Detach(name, why string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	a := w.sources[name]
	delete(w.sources, name)
	for i, n := range w.order {
		if n == name {
			w.order = append(w.order[:i], w.order[i+1:]...)
			break
		}
	}
	remaining := len(w.order)
	w.mu.Unlock()
	if a == nil {
		return
	}
	msg := fmt.Sprintf("[hub: %q is no longer on this channel — %s. Nothing further will arrive "+
		"for it here.", name, why)
	if remaining == 0 {
		msg += " No connection is left on this channel; it stays open, and a new hub_connect " +
			"attaches to it without you restarting anything."
	} else {
		msg += fmt.Sprintf(" %d other connection(s) are still delivering here.", remaining)
	}
	w.Announce(msg + "]")
}

// Attached lists the connections this waiter carries.
func (w *Waiter) Attached() []string {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.order...)
}

// sourcesSnapshot copies the attachment list for iteration outside the
// lock — deliver writes to a socket, which must never happen while
// holding a lock a Poke needs.
func (w *Waiter) sourcesSnapshot() []*attached {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]*attached, 0, len(w.order))
	for _, n := range w.order {
		if a := w.sources[n]; a != nil {
			out = append(out, a)
		}
	}
	return out
}

// Following reports whether a follow-mode reader is registered right
// now. Used to decide whether a caller can be told to WAIT for a
// notification or has to be told to check back itself — telling someone
// to wait for a message that nothing will send is worse than telling
// them to poll.
func (w *Waiter) Following() bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.current != nil && w.current.follow
}

// Announce pushes one line to whoever is currently following, outside
// the ordinary event flow. Used to say something about the CHANNEL
// rather than about the conversation — the one case being a reconnect,
// after which this channel looks exactly as it did before while the
// session may have missed everything that arrived in the gap.
//
// A no-op when nobody is following: this is a courtesy to a live reader,
// not a record, and anything that must not be lost belongs in the
// catch-up position instead.
func (w *Waiter) Announce(msg string) {
	if w == nil {
		return
	}
	w.mu.Lock()
	rw := w.current
	w.mu.Unlock()
	if rw == nil || !rw.follow {
		return
	}
	if _, err := rw.conn.Write([]byte(msg + "\n\n")); err != nil {
		rw.conn.Close()
		w.mu.Lock()
		if w.current == rw {
			w.current = nil
		}
		w.mu.Unlock()
	}
}

// holdingFor reports whether a dead source should be waited out rather
// than reported as the end, and whether the reader still needs to be told
// that is what is happening — asked per connection, since one holding
// says nothing about the others.
func (w *Waiter) holdingFor(name string) (holding, announce bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	a := w.sources[name]
	if a == nil || !a.expecting {
		return false, false
	}
	if a.held {
		return true, false
	}
	a.held = true
	return true, true
}

// socketDir is where wait sockets live — a package var (not a plain
// os.TempDir() call inline) so tests can point it at an isolated temp dir
// instead of sweeping/polluting the real OS temp dir. Both socketPath and
// sweepStaleSockets must use this same var; letting them diverge (e.g. one
// hardcoding os.TempDir() while the other reads an env var) would silently
// break the sweep in production, where MCP_HUB_LOG_DIR — a *different*,
// unrelated directory used by hublog/identitystore — is commonly set.
var socketDir = os.TempDir()

// liveEmissionSpacing is the minimum delay between successive writes
// within one deliver() call to a follow-mode connection — see deliver's
// use of it. Set to comfortably exceed 2x Monitor's own documented
// ~200ms stdout-batching window: per the math worked out live,
// 2026-09-04 (a delay exceeding a coalescing window means no two of this
// process's writes can share one, for either a fixed-tick or a debounce
// implementation of that window), this makes pairwise merging of two
// deliveries from this waiter structurally impossible rather than merely
// less likely — PROVIDED that 200ms figure is accurate and stable, which
// is the harness's own documented constant, not something independently
// measured here. Only applied between multiple chunks in the same
// delivery; a single event (the ordinary case) is written immediately,
// with no added latency at all. Var so tests can shrink it.
var liveEmissionSpacing = 500 * time.Millisecond

// SocketDirForTesting overrides the directory wait sockets are created in
// (and swept from), returning a restore function. For use by *other*
// packages' tests that exercise a real Listen() call indirectly (e.g.
// mcptools' Hub.handleConnect) — without this, such a test would sweep the
// real OS temp dir, which on a dev machine is where other live processes'
// real wait sockets actually live. internal/waiter's own tests set
// socketDir directly (same package, no need for this).
func SocketDirForTesting(dir string) (restore func()) {
	orig := socketDir
	socketDir = dir
	return func() { socketDir = orig }
}

// Listen opens the wait socket for this connection and starts accepting
// connections. The path is random per call (see socketPath) — no longer
// derived from (sessionID, peerID) — specifically so two different
// processes never
// compute the same path: peerID is now routinely *reused* across a
// reconnect (persisted reconnectSecret, and a server-side supersede on a
// still-live collision — see hubsession.SupersededCloseCode), so a
// deterministic path would let a second, still-alive process's Listen
// unlink and rebind the first's socket file out from under it, mid-race,
// with nothing to arbitrate between them. Only one connection to this
// socket may be registered at a time; a new connection always supersedes
// any previously registered one, whether it was in "once" or "follow" mode.
func Listen() (*Waiter, error) {
	path := socketPath()
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	w := &Waiter{ln: ln, socketPath: path, sources: map[string]*attached{}}
	go w.acceptLoop()
	sweepStaleSockets(path)
	return w, nil
}

// sweepStaleSockets removes every mcp-hub-wait-*.sock file in the same
// directory that has no listener behind it — self-healing cleanup for
// sockets left behind by a process that ended without running Close
// (crash, kill -9, a harness just terminating it — none of which run a
// defer). Never touches excludePath (this Waiter's own, just-created
// socket) or anything with a live listener.
//
// Staleness is determined the only way that's actually reliable for a
// Unix domain socket: briefly connecting to it. This is provably harmless
// to a real, still-active Waiter on the other end — handleAccept requires
// reading one mode byte before it does anything stateful (registering as
// current, superseding a prior connection), and this probe closes without
// ever writing one, so at worst it's indistinguishable from a client that
// connected and immediately hung up before identifying itself.
func sweepStaleSockets(excludePath string) {
	matches, err := filepath.Glob(filepath.Join(socketDir, "mcp-hub-wait-*.sock"))
	if err != nil {
		return
	}
	for _, p := range matches {
		if p == excludePath {
			continue
		}
		if isStaleSocket(p) {
			_ = os.Remove(p)
		}
	}
}

// isStaleSocket reports whether path has no listener behind it, by
// attempting (and immediately abandoning) a connection — see
// sweepStaleSockets for why this is safe against a live listener.
func isStaleSocket(path string) bool {
	conn, err := net.DialTimeout("unix", path, 200*time.Millisecond)
	if err == nil {
		conn.Close()
		return false
	}
	// Only a refusal proves absence. Any other failure — a timeout above
	// all — says this dial did not complete, which is not the same fact
	// and on a loaded machine is routinely produced by a listener that is
	// perfectly alive and merely slow to accept.
	//
	// The difference is not academic: deleting the socket does not stop
	// the process behind it, it makes that process unreachable forever
	// while its follower still believes it is connected. This directory is
	// shared by every session on the machine, and every Listen sweeps it,
	// so treating "no answer within 200ms" as "dead" lets any session
	// silence any other. Absence of a reply is not evidence of absence.
	var se syscall.Errno
	if errors.As(err, &se) {
		// ECONNREFUSED: the path exists and nothing is listening.
		// ENOENT: there is no such socket at all. Both are absence.
		return se == syscall.ECONNREFUSED || se == syscall.ENOENT
	}
	return errors.Is(err, fs.ErrNotExist)
}

// socketPath generates a short, random wait-socket filename — deliberately
// not derived from (sessionID, peerID) (see Listen's doc comment on why
// determinism there was a real cross-process collision hazard once peerID
// became routinely reusable). Unix-domain sockets have a 108-byte sun_path
// limit on Linux, macOS, and Windows' AF_UNIX implementation, so this stays
// well under it with real headroom even on Windows (%TEMP% is often
// 40-50+ bytes) — an 8-byte random suffix, same length as the truncated
// hash this replaces, keeps collision probability negligible for this
// short-lived, single-host use.
func socketPath() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand failing at all is exceptionally rare (kernel RNG
		// unavailable) — fall back to a timestamp rather than a fixed
		// name, so a failure here still can't reintroduce the collision
		// hazard this function exists to avoid.
		binary.BigEndian.PutUint64(buf[:], uint64(time.Now().UnixNano()))
	}
	return filepath.Join(socketDir, fmt.Sprintf("mcp-hub-wait-%x.sock", buf))
}

// WaitCommand is the exact command Claude should run in the background to
// receive the next event (the default, "once" mode).
func (w *Waiter) WaitCommand() string {
	if w == nil {
		return ""
	}
	return w.waitCommand("")
}

// WaitFollowCommand is the same command in "follow" mode: it stays running
// and prints each event as it arrives instead of exiting after one, which
// fits a harness with a way to stream a long-running background process's
// output (e.g. a "Monitor"-style tool) better than the once-mode
// run-it-again loop WaitCommand is meant for.
func (w *Waiter) WaitFollowCommand() string {
	if w == nil {
		return ""
	}
	return w.waitCommand(" --follow")
}

func (w *Waiter) waitCommand(flags string) string {
	exe, err := os.Executable()
	if err != nil {
		exe = "mcp-hub-client"
	}
	return fmt.Sprintf("%s wait --socket %s%s", exe, w.socketPath, flags)
}

func (w *Waiter) acceptLoop() {
	for {
		conn, err := w.ln.Accept()
		if err != nil {
			return // listener closed
		}
		w.handleAccept(conn)
	}
}

func (w *Waiter) handleAccept(conn net.Conn) {
	mode := make([]byte, 1)
	if n, err := conn.Read(mode); err != nil || n != 1 {
		conn.Close()
		return
	}
	rw := &registeredWaiter{conn: conn, follow: mode[0] == ModeFollow}

	// The source's Peek() and the w.current registration below are checked
	// under one uninterrupted lock hold — see the comment on deliver's tail
	// for why the two must never be split, on pain of a permanently
	// undelivered event.
	w.mu.Lock()
	if w.pendingLocked() {
		w.mu.Unlock()
		w.deliver(rw)
		return
	}
	old := w.current
	w.current = rw
	w.mu.Unlock()
	if old != nil {
		writeAndClose(old.conn, w.supersededMessage())
	}
}

// Poke delivers to the currently registered waiter, if any, using whatever
// the source currently has buffered — but only if the source actually
// considers itself wake-worthy right now (see hubconn.Conn.Peek: a source
// may buffer something without yet wanting to wake anyone on it, e.g. an
// own-send echo held back pending a real reply). Checking Peek() here,
// under the same lock that reads/clears w.current, is required for the
// same reason deliver's re-registration tail does it: onActivity fires on
// every single event from the connection's read loop, suppressed or not,
// so Poke is called far more often than it should actually act — without
// this check it would fire on an event the source doesn't consider
// wake-worthy yet, prematurely draining and losing the "wait for
// something that matters" property entirely. When it declines to act,
// the registration is left in place untouched, to be woken by a later
// Poke call once the source's own state changes.
func (w *Waiter) Poke() {
	if w == nil {
		return
	}
	w.mu.Lock()
	if !w.pendingLocked() {
		w.mu.Unlock()
		return
	}
	rw := w.current
	w.current = nil
	w.mu.Unlock()
	if rw == nil {
		return
	}
	w.deliver(rw)
}

func (w *Waiter) deliver(rw *registeredWaiter) {
	if !rw.follow {
		// One-shot: whatever any connection has, labelled, then close.
		// "Disconnected" is reported only when there is nothing attached
		// at all — with connections still live, one of them ending is a
		// labelled line in the payload, not the end of the channel.
		var parts []string
		for _, a := range w.sourcesSnapshot() {
			formatted, connected := a.src.Drain()
			if formatted != "" {
				parts = append(parts, label(a.name)+formatted)
			}
			if !connected {
				if holding, _ := w.holdingFor(a.name); !holding {
					parts = append(parts, endedMessage(a))
					w.Detach(a.name, "its connection ended")
				}
			}
		}
		if len(parts) == 0 {
			writeAndClose(rw.conn, w.disconnectedMessage())
			return
		}
		// The reminder to re-run leads, rather than trails, the delivered
		// content — this is a single one-shot response with nothing to
		// fall back on, so if whatever's reading it gets cut off partway
		// through (e.g. a caller with its own read timeout), a trailing
		// reminder is exactly the part most likely to be lost. Leading
		// with it means the instruction survives even a truncated read.
		writeAndClose(rw.conn, "Run this command again to keep receiving:\n"+w.WaitCommand()+
			"\n\n"+strings.Join(parts, "\n\n")+"\n")
		return
	}

	// Follow mode: one network write per event (DrainBatch), not one
	// write for the whole burst (Drain) — see Source.DrainBatch's doc
	// comment. A multi-event burst that used to leave here as a single
	// write, ripe for a downstream layer to truncate as one unit, now
	// leaves as N separate writes, each individually complete and
	// self-identifying (FormatEventsBatch's "i/N" marker).
	//
	// Every write is labelled with the connection it came from. With one
	// channel carrying several conversations, an unlabelled line is a
	// message the reader cannot place, answer, or confirm — and confirming
	// it against the wrong connection is the one mistake that silently
	// skips messages.
	wrote := 0
	for _, a := range w.sourcesSnapshot() {
		chunks, connected := a.src.DrainBatch()
		for _, c := range chunks {
			// liveEmissionSpacing between successive writes in the same
			// delivery — not before the first, which stays immediate (the
			// common single-event case pays no latency at all). See its
			// own doc comment for why this closes, rather than merely
			// reduces, the downstream-coalescing merge class.
			if wrote > 0 {
				time.Sleep(liveEmissionSpacing)
			}
			if _, err := rw.conn.Write([]byte(label(a.name) + c + "\n\n")); err != nil {
				rw.conn.Close()
				return
			}
			wrote++
		}
		if connected {
			continue
		}
		// A dead source: either it is coming back, in which case the
		// reader is told once and stays, or that conversation has ended,
		// in which case the reader is told which one and stays anyway.
		// Neither closes the channel — the other conversations on it are
		// still live, and a reader released here could not be reattached
		// when the next connect happens.
		holding, announce := w.holdingFor(a.name)
		switch {
		case holding && announce:
			if _, err := rw.conn.Write([]byte(label(a.name) + w.holdingMessage() + "\n\n")); err != nil {
				rw.conn.Close()
				return
			}
			wrote++
		case !holding:
			if _, err := rw.conn.Write([]byte(endedMessage(a) + "\n\n")); err != nil {
				rw.conn.Close()
				return
			}
			wrote++
			w.Detach(a.name, "its connection ended")
		}
	}

	// Re-register for the next event — checking for pending work and
	// setting w.current in the same uninterrupted lock hold that Poke()
	// uses to read/clear w.current, not as two separate steps. Splitting
	// them leaves a gap in which a concurrent Poke() — triggered by an
	// event landing in exactly that window — finds w.current still nil
	// from the previous delivery, silently no-ops, and then this call
	// registers anyway based on a check taken before that event existed.
	// The result: an event that genuinely arrived sits in the buffer with
	// nothing left to ever poke it again.
	w.mu.Lock()
	if w.closed {
		// The Waiter was closed while this delivery was writing. Tell the
		// reader rather than re-registering it on something that will
		// never poke it again: its socket file is already gone, so silence
		// here is permanent and looks exactly like an idle connection.
		w.mu.Unlock()
		writeAndClose(rw.conn, w.disconnectedMessage())
		return
	}
	if w.current != nil {
		// A genuinely new connection claimed the slot while we were
		// writing; it rightfully wins (see handleAccept) — we lose ours.
		w.mu.Unlock()
		writeAndClose(rw.conn, w.supersededMessage())
		return
	}
	if w.pendingLocked() {
		w.mu.Unlock()
		w.deliver(rw)
		return
	}
	w.current = rw
	w.mu.Unlock()
}

// label prefixes a delivered chunk with the connection it belongs to.
// Every line on a shared channel carries one: a message a reader cannot
// place is a message it cannot answer or confirm.
func label(name string) string {
	return "[connection: " + name + "]\n"
}

// pendingLocked reports whether ANY attached connection has something a
// reader should be woken for: buffered events, or an ending that has not
// been reported yet. Caller holds w.mu.
//
// "Any", not "all": a reader waiting on eight conversations is waiting
// for whichever speaks first, and a quiet connection must not be able to
// hold back a busy one.
func (w *Waiter) pendingLocked() bool {
	for _, n := range w.order {
		a := w.sources[n]
		if a == nil {
			continue
		}
		hasEvents, connected := a.src.Peek()
		if hasEvents {
			return true
		}
		if connected {
			continue
		}
		// A dead source is news exactly ONCE. While it is held for an
		// announced restart and the reader has already been told, it
		// stays attached and stays disconnected — so counting it as
		// pending would wake a delivery that has nothing to write, which
		// would find it pending again, forever. (It did: one stack
		// overflow, caught by the suite.) A source whose ending has been
		// reported is detached instead, so it cannot reach here at all.
		if !(a.expecting && a.held) {
			return true
		}
	}
	return false
}

func (w *Waiter) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	// Set before releasing the lock, and checked by deliver before it
	// re-registers. A delivery in flight has UNREGISTERED its reader —
	// Poke clears w.current before writing — so a Close landing in that
	// window sees nobody to say goodbye to, tears the listener down, and
	// then deliver re-registers that reader on a dead Waiter. It is never
	// delivered to, never told the hub disconnected, and its socket path
	// is already gone: a follower that waits forever looking alive, which
	// is the exact state the hold mechanism exists to prevent.
	w.closed = true
	if w.current != nil {
		writeAndClose(w.current.conn, w.disconnectedMessage())
		w.current = nil
	}
	w.mu.Unlock()
	err := w.ln.Close()
	_ = os.Remove(w.socketPath)
	return err
}

// disconnectNoter is implemented by a Source that can explain why the
// connection died beyond an ordinary drop (currently *hubconn.Conn, for a
// teams relay's close code signaling a dead credential — see
// hubconn.Conn.DisconnectNote). Checked via an interface, not a direct
// hubconn dependency, since Source is meant to stay generic.
type disconnectNoter interface {
	DisconnectNote() string
}

// disconnectedMessage is the "hub disconnected" text sent to a waiter,
// with whatever extra note the source can offer appended.
func (w *Waiter) disconnectedMessage() string {
	return "hub disconnected\n"
}

// endedMessage reports that ONE connection ended, by name, with whatever
// the source can say about why. A labelled line rather than the socket
// closing: the other conversations on this channel have not ended, and a
// reader told "disconnected" would have no way to tell which of those two
// things happened.
func endedMessage(a *attached) string {
	note := ""
	if n, ok := a.src.(disconnectNoter); ok {
		note = n.DisconnectNote()
	}
	return fmt.Sprintf("[hub: %q disconnected%s. Nothing further arrives for it here until it is "+
		"reconnected; other connections on this channel are unaffected.]", a.name, note)
}

// holdingMessage tells the waiting side that the connection ended on
// purpose and this channel is staying open across it. Said once per
// outage, and deliberately says nothing needs doing: a follower that
// treats this as a disconnect and exits recreates the exact problem the
// hold exists to prevent.
func (w *Waiter) holdingMessage() string {
	return "[hub: the server is restarting on purpose — this follower is being held open " +
		"across it and will keep delivering once the client reconnects. Nothing to do, and do " +
		"NOT restart this process: doing so would replace a follower that is already waiting]"
}

// supersededMessage tells the losing wait's process not to restart itself —
// without this, a model that follows hub_connect's generic "run it again
// every time it completes" instruction too literally could spawn a
// replacement for the one that just lost, which immediately supersedes
// whatever legitimately still-active wait was already running, and so on
// indefinitely: only one wait should ever be kept running at a time.
func (w *Waiter) supersededMessage() string {
	return "superseded by a newer wait — do NOT run this command again. " +
		"Keep exactly ONE wait running at a time; the newer one is already " +
		"active and receiving for you. Prefer --follow for that one going " +
		"forward:\n" + w.WaitFollowCommand() + "\n"
}

func writeAndClose(conn net.Conn, msg string) {
	_, _ = conn.Write([]byte(msg))
	_ = conn.Close()
}
