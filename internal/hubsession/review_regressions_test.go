package hubsession

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCorruptIdentityStoreIsNotOverwrittenOnJoin(t *testing.T) {
	skipKnownIssue(t, "an unreadable identity file is silently overwritten")
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)
	id := "550e8400-e29b-41d4-a716-446655440000"
	path := filepath.Join(dir, id+".secrets.json")
	original := []byte(`{"previous-entry":`)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	s := newSession(id)
	joinFake(s, "new peer", "", "new-secret", nil)
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(original) {
		t.Fatal("joining silently overwrote unreadable persisted identities")
	}
}

// skipKnownIssue skips a reproduction of a defect recorded in
// docs/known-issues.md that is still present. Set MCP_HUB_KNOWN_ISSUES=1 to
// run it; it fails until the defect is fixed, and then the skip goes.
func skipKnownIssue(t *testing.T, issue string) {
	t.Helper()
	if os.Getenv("MCP_HUB_KNOWN_ISSUES") == "" {
		t.Skipf("known issue, still present: %s (docs/known-issues.md); MCP_HUB_KNOWN_ISSUES=1 runs it", issue)
	}
}
