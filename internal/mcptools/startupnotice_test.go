package mcptools

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/connstore"
)

// The gap this closes: when the MCP process ends, every connection ends
// with it and the thing that would report that dies in the same instant.
// Measured live — four connections ended at 15:29 and the model learned
// of it half an hour later, from its user.
func TestAPreviousRunsConnectionsAreReportedOnce(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	project := connstore.CurrentProject()
	target := connstore.Target{Link: "wss://example/550e8400-e29b-41d4-a716-446655440000", Project: project}
	// A pid that is certainly not running: 0 is never a process, and
	// MaxInt is not one either — use a clearly dead one rather than
	// picking a number and hoping.
	dead := findDeadPID(t)
	if err := connstore.Upsert(target, connstore.Entry{
		PeerID: "550e8400-e29b-41d4-a716-446655440000", LocalName: "relay",
		HolderPID: dead, LastConnectedAt: time.Now().UTC(), Connected: true,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	h := &Hub{}
	h.reportAbandonedConnections()
	note := h.takeAutoReconnectNote()
	if !strings.Contains(note, "relay") {
		t.Fatalf("expected the notice to name the connection, got: %s", note)
	}
	if !strings.Contains(note, "NOT restored") {
		t.Fatalf("expected it to say they were not restored, got: %s", note)
	}
	// Said once. A notice repeated on every call is one a reader learns
	// to skip, and this one matters exactly once.
	if again := h.takeAutoReconnectNote(); again != "" {
		t.Fatalf("expected nothing further queued, got: %s", again)
	}
}

// A connection given up ON PURPOSE is not reported: that was a decision,
// and repeating it back is nagging about something already done.
func TestADeliberateDisconnectIsNotReported(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	project := connstore.CurrentProject()
	target := connstore.Target{Link: "wss://example/550e8400-e29b-41d4-a716-446655440001", Project: project}
	if err := connstore.Upsert(target, connstore.Entry{
		PeerID: "550e8400-e29b-41d4-a716-446655440001", LocalName: "ops",
		HolderPID: findDeadPID(t), LastConnectedAt: time.Now().UTC(), Connected: true,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := connstore.MarkLeftOnPurpose(target); err != nil {
		t.Fatalf("MarkLeftOnPurpose: %v", err)
	}

	h := &Hub{}
	h.reportAbandonedConnections()
	if note := h.takeAutoReconnectNote(); note != "" {
		t.Fatalf("expected silence about a deliberate disconnect, got: %s", note)
	}
}

// A holder that is still ALIVE is reported too, because one client holds
// every connection now: another process holding one is the process this
// one replaced, not a colleague. A /mcp restart can start the new process
// before the old has finished exiting, and a notice that fired or not
// depending on that race would be worse than none.
func TestALingeringHolderIsStillReported(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	project := connstore.CurrentProject()
	target := connstore.Target{Link: "wss://example/550e8400-e29b-41d4-a716-446655440002", Project: project}
	// A pid that IS alive and is not us: the test's own parent will do.
	alive := os.Getppid()
	if !processAlive(alive) {
		t.Skip("no live parent process to stand in for the one being replaced")
	}
	if err := connstore.Upsert(target, connstore.Entry{
		PeerID: "550e8400-e29b-41d4-a716-446655440002", LocalName: "held",
		HolderPID: alive, LastConnectedAt: time.Now().UTC(), Connected: true,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	h := &Hub{}
	h.reportAbandonedConnections()
	note := h.takeAutoReconnectNote()
	if !strings.Contains(note, "held") {
		t.Fatalf("expected the lingering connection to be named, got: %s", note)
	}
	if !strings.Contains(note, "not finished exiting") {
		t.Fatalf("expected it to say the old process is still around, got: %s", note)
	}
}

// This process's own connections are never reported as a previous run's.
func TestOurOwnConnectionsAreNotReported(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	project := connstore.CurrentProject()
	target := connstore.Target{Link: "wss://example/550e8400-e29b-41d4-a716-446655440003", Project: project}
	if err := connstore.Upsert(target, connstore.Entry{
		PeerID: "550e8400-e29b-41d4-a716-446655440003", LocalName: "mine",
		HolderPID: os.Getpid(), LastConnectedAt: time.Now().UTC(), Connected: true,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	h := &Hub{}
	h.reportAbandonedConnections()
	if note := h.takeAutoReconnectNote(); note != "" {
		t.Fatalf("expected silence about our own connection, got: %s", note)
	}
}

// findDeadPID returns a pid nothing is running under, so "abandoned"
// means abandoned rather than "whatever that number happens to be".
func findDeadPID(t *testing.T) int {
	t.Helper()
	for pid := 1 << 22; pid > 1<<20; pid-- {
		if !processAlive(pid) {
			return pid
		}
	}
	t.Fatal("could not find a pid with no process behind it")
	return 0
}
