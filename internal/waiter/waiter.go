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

type Waiter struct {
	source     Source
	ln         net.Listener
	socketPath string

	mu      sync.Mutex
	current net.Conn
}

// Listen opens the wait socket for this (sessionID, peerID) connection and
// starts accepting connections. peerID is included in the path (not just
// sessionID) so two separate mcp-hub-client processes joining the same
// session on the same host don't collide on the same socket path. Only one
// connection to this socket may be pending at a time; a new connection
// supersedes any previously registered one.
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
// receive the next event.
func (w *Waiter) WaitCommand() string {
	exe, err := os.Executable()
	if err != nil {
		exe = "mcp-hub-client"
	}
	return fmt.Sprintf("%s wait --socket %s", exe, w.socketPath)
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
	if hasEvents, connected := w.source.Peek(); hasEvents || !connected {
		w.deliver(conn)
		return
	}
	w.mu.Lock()
	old := w.current
	w.current = conn
	w.mu.Unlock()
	if old != nil {
		writeAndClose(old, "superseded by a newer wait\n")
	}
}

// Poke delivers to the currently registered waiter, if any, using whatever
// the source currently has buffered.
func (w *Waiter) Poke() {
	w.mu.Lock()
	conn := w.current
	w.current = nil
	w.mu.Unlock()
	if conn == nil {
		return
	}
	w.deliver(conn)
}

func (w *Waiter) deliver(conn net.Conn) {
	formatted, connected := w.source.Drain()
	if !connected {
		writeAndClose(conn, "hub disconnected\n")
		return
	}
	writeAndClose(conn, formatted+"\n\nRun this command again to keep receiving:\n"+w.WaitCommand()+"\n")
}

func (w *Waiter) Close() error {
	w.mu.Lock()
	if w.current != nil {
		writeAndClose(w.current, "hub disconnected\n")
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
