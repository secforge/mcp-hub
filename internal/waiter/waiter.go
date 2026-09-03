package waiter

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
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

type Waiter struct {
	source     Source
	ln         net.Listener
	socketPath string

	mu      sync.Mutex
	current *registeredWaiter
}

// socketDir is where wait sockets live — a package var (not a plain
// os.TempDir() call inline) so tests can point it at an isolated temp dir
// instead of sweeping/polluting the real OS temp dir. Both socketPath and
// sweepStaleSockets must use this same var; letting them diverge (e.g. one
// hardcoding os.TempDir() while the other reads an env var) would silently
// break the sweep in production, where MCP_HUB_LOG_DIR — a *different*,
// unrelated directory used by hublog/identitystore — is commonly set.
var socketDir = os.TempDir()

// SocketDirForTesting overrides the directory wait sockets are created in
// (and swept from), returning a restore function. For use by *other*
// packages' tests that exercise a real Listen() call indirectly (e.g.
// mcptools.Hub.handleConnect) — without this, such a test would sweep the
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
func Listen(source Source) (*Waiter, error) {
	path := socketPath()
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	w := &Waiter{source: source, ln: ln, socketPath: path}
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
	if err != nil {
		return true
	}
	conn.Close()
	return false
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
	return w.waitCommand("")
}

// WaitFollowCommand is the same command in "follow" mode: it stays running
// and prints each event as it arrives instead of exiting after one, which
// fits a harness with a way to stream a long-running background process's
// output (e.g. a "Monitor"-style tool) better than the once-mode
// run-it-again loop WaitCommand is meant for.
func (w *Waiter) WaitFollowCommand() string {
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
	if hasEvents, connected := w.source.Peek(); hasEvents || !connected {
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
	w.mu.Lock()
	if hasEvents, connected := w.source.Peek(); !hasEvents && connected {
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
		formatted, connected := w.source.Drain()
		if !connected {
			writeAndClose(rw.conn, w.disconnectedMessage())
			return
		}
		// The reminder to re-run leads, rather than trails, the delivered
		// content — this is a single one-shot response with nothing to
		// fall back on, so if whatever's reading it gets cut off partway
		// through (e.g. a caller with its own read timeout), a trailing
		// reminder is exactly the part most likely to be lost. Leading
		// with it means the instruction survives even a truncated read.
		writeAndClose(rw.conn, "Run this command again to keep receiving:\n"+w.WaitCommand()+"\n\n"+formatted+"\n")
		return
	}

	// Follow mode: one network write per event (DrainBatch), not one
	// write for the whole burst (Drain) — see Source.DrainBatch's doc
	// comment. A multi-event burst that used to leave here as a single
	// write, ripe for a downstream layer to truncate as one unit, now
	// leaves as N separate writes, each individually complete and
	// self-identifying (FormatEventsBatch's "i/N" marker).
	chunks, connected := w.source.DrainBatch()
	if !connected {
		writeAndClose(rw.conn, w.disconnectedMessage())
		return
	}
	for _, c := range chunks {
		if _, err := rw.conn.Write([]byte(c + "\n\n")); err != nil {
			rw.conn.Close()
			return
		}
	}

	// Re-register for the next event — checking w.source.Peek() and setting
	// w.current in the same uninterrupted lock hold that Poke() uses to
	// read/clear w.current, not as two separate steps. Splitting them (an
	// unlocked Peek() first, then a separate lock to register) leaves a gap
	// in which a concurrent Poke() — triggered by an event landing in
	// exactly that window — finds w.current still nil from the *previous*
	// delivery, silently no-ops (Poke does nothing when nothing is
	// registered), and then this call goes on to register anyway, based on
	// a Peek() taken *before* that event existed. The result: an event that
	// genuinely arrived sits in the buffer with nothing left to ever poke
	// it again — the connection stays registered, technically alive,
	// forever waiting for a notification that already happened and was
	// missed. Checking Peek() fresh inside the same lock Poke() uses
	// closes the gap: whichever of the two acquires w.mu first, the other
	// is guaranteed to observe accurate state once it's their turn.
	w.mu.Lock()
	if w.current != nil {
		// A genuinely new connection claimed the slot while we were
		// writing; it rightfully wins (see handleAccept) — we lose ours.
		w.mu.Unlock()
		writeAndClose(rw.conn, w.supersededMessage())
		return
	}
	if hasEvents, connected := w.source.Peek(); hasEvents || !connected {
		w.mu.Unlock()
		w.deliver(rw)
		return
	}
	w.current = rw
	w.mu.Unlock()
}

func (w *Waiter) Close() error {
	w.mu.Lock()
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
// bridge server's close code signaling a dead credential — see
// hubconn.Conn.DisconnectNote). Checked via an interface, not a direct
// hubconn dependency, since Source is meant to stay generic.
type disconnectNoter interface {
	DisconnectNote() string
}

// disconnectedMessage is the "hub disconnected" text sent to a waiter,
// with whatever extra note the source can offer appended.
func (w *Waiter) disconnectedMessage() string {
	note := ""
	if n, ok := w.source.(disconnectNoter); ok {
		note = n.DisconnectNote()
	}
	return "hub disconnected" + note + "\n"
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
