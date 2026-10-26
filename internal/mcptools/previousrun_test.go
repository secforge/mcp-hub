package mcptools

import (
	"strings"
	"testing"
)

// A session whose harness shows no pushes never sees the startup notice,
// so the restart is said where it does look: the failing call.
func TestAConnectionThePreviousRunHeldIsNamedInTheError(t *testing.T) {
	h := NewHub()
	h.previousRun = []string{"debugger", "relay"}
	_, err := h.session("debugger")
	if err == nil || !strings.Contains(err.Error(), "RESTARTED") || !strings.Contains(err.Error(), "debugger, relay") {
		t.Fatalf("error: %v", err)
	}
	h.mu.Lock()
	h.restoredLocked("debugger")
	h.restoredLocked("relay")
	h.mu.Unlock()
	if _, err := h.session("typo"); err == nil || strings.Contains(err.Error(), "RESTARTED") {
		t.Fatalf("after everything was restored: %v", err)
	}
}
