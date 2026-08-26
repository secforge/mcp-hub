package waiter

import (
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
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

// Listen opens the wait socket for this (sessionID, peerID) connection and
// starts accepting connections. peerID is included in the path (not just
// sessionID) so two separate mcp-hub-client processes joining the same
// session on the same host don't collide on the same socket path. Only one
// connection to this socket may be registered at a time; a new connection
// always supersedes any previously registered one, whether it was in "once"
// or "follow" mode.
func Listen(sessionID, peerID string, source Source) (*Waiter, error) {
	path := socketPath(sessionID, peerID)
	_ = os.Remove(path) // stale socket from a crashed prior run
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	w := &Waiter{source: source, ln: ln, socketPath: path}
	go w.acceptLoop()
	return w, nil
}

// socketPath derives a short, fixed-length wait-socket filename from
// (sessionID, peerID) instead of embedding both raw UUIDs. Unix-domain
// sockets have a 108-byte sun_path limit on Linux, macOS, and Windows'
// AF_UNIX implementation; two 36-char UUIDs plus the surrounding literal
// text alone already total ~91 bytes, leaving no headroom for the OS temp
// directory once it's joined in (on Windows in particular, %TEMP% is often
// 40-50+ bytes) — net.Listen then fails, and the caller closes the
// connection it just opened, producing an immediate join-then-leave. A
// truncated SHA-256 hash keeps collision risk negligible for this
// short-lived, single-host use while staying well under the limit.
func socketPath(sessionID, peerID string) string {
	sum := sha256.Sum256([]byte(sessionID + "-" + peerID))
	return filepath.Join(os.TempDir(), fmt.Sprintf("mcp-hub-wait-%x.sock", sum[:8]))
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

	if hasEvents, connected := w.source.Peek(); hasEvents || !connected {
		w.deliver(rw)
		return
	}
	w.mu.Lock()
	old := w.current
	w.current = rw
	w.mu.Unlock()
	if old != nil {
		writeAndClose(old.conn, "superseded by a newer wait\n")
	}
}

// Poke delivers to the currently registered waiter, if any, using whatever
// the source currently has buffered.
func (w *Waiter) Poke() {
	w.mu.Lock()
	rw := w.current
	w.current = nil
	w.mu.Unlock()
	if rw == nil {
		return
	}
	w.deliver(rw)
}

func (w *Waiter) deliver(rw *registeredWaiter) {
	formatted, connected := w.source.Drain()
	if !connected {
		writeAndClose(rw.conn, "hub disconnected\n")
		return
	}
	if !rw.follow {
		writeAndClose(rw.conn, formatted+"\n\nRun this command again to keep receiving:\n"+w.WaitCommand()+"\n")
		return
	}

	if _, err := rw.conn.Write([]byte(formatted + "\n\n")); err != nil {
		rw.conn.Close()
		return
	}

	// Re-register for the next event. If one is already pending (raced while
	// we were writing), deliver it immediately instead of waiting for Poke.
	if hasEvents, connected := w.source.Peek(); hasEvents || !connected {
		w.deliver(rw)
		return
	}
	w.mu.Lock()
	if w.current != nil {
		// A genuinely new connection claimed the slot while we were
		// writing; it rightfully wins (see handleAccept) — we lose ours.
		w.mu.Unlock()
		writeAndClose(rw.conn, "superseded by a newer wait\n")
		return
	}
	w.current = rw
	w.mu.Unlock()
}

func (w *Waiter) Close() error {
	w.mu.Lock()
	if w.current != nil {
		writeAndClose(w.current.conn, "hub disconnected\n")
		w.current = nil
	}
	w.mu.Unlock()
	err := w.ln.Close()
	_ = os.Remove(w.socketPath)
	return err
}

func writeAndClose(conn net.Conn, msg string) {
	_, _ = conn.Write([]byte(msg))
	_ = conn.Close()
}
