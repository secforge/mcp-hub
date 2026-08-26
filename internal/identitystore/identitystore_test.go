package identitystore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveThenLoadRoundTrips(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	mapping := map[string]string{
		HashSecret("secret-a"): "peer-a",
		HashSecret("secret-b"): "peer-b",
	}
	if err := Save(sessionID, mapping); err != nil {
		t.Fatalf("save: %v", err)
	}

	got := Load(sessionID)
	if got[HashSecret("secret-a")] != "peer-a" || got[HashSecret("secret-b")] != "peer-b" {
		t.Fatalf("got %+v", got)
	}
}

func TestLoadReturnsEmptyMapWhenNothingPersisted(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	got := Load("never-seen-session")
	if len(got) != 0 {
		t.Fatalf("expected an empty map, got %+v", got)
	}
}

func TestSaveNeverWritesTheRawSecretToDisk(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	const rawSecret = "super-secret-do-not-persist-me"
	if err := Save(sessionID, map[string]string{HashSecret(rawSecret): "peer-a"}); err != nil {
		t.Fatalf("save: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, sessionID+".secrets.json"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if strings.Contains(string(data), rawSecret) {
		t.Fatalf("the raw secret must never appear in the persisted file, got: %s", data)
	}
}

func TestDeleteRemovesThePersistedFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	if err := Save(sessionID, map[string]string{HashSecret("x"): "peer-a"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	Delete(sessionID)

	if got := Load(sessionID); len(got) != 0 {
		t.Fatalf("expected nothing after delete, got %+v", got)
	}
	if _, err := os.Stat(filepath.Join(dir, sessionID+".secrets.json")); !os.IsNotExist(err) {
		t.Fatalf("expected the file to be gone, stat err: %v", err)
	}
}

func TestDeleteOfNonexistentSessionIsHarmless(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)
	Delete("never-existed") // must not panic
}

// TestPathTraversalSessionIDIsRejected is a security regression test:
// sessionID becomes part of a filesystem path, so an unvalidated caller
// passing something like "../../etc/cron.d/x" must never be able to read,
// write, or delete outside dir(). wsserver already rejects any non-UUID
// sessionId before a session is ever created, but this package validates
// independently rather than trusting that as its only line of defense.
func TestPathTraversalSessionIDIsRejected(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	malicious := []string{
		"../../../../etc/cron.d/evil",
		"../escape",
		"..",
		"/etc/passwd",
		"a/b",
		"a\\b",
		"not-a-uuid",
		"",
	}
	for _, id := range malicious {
		if got := Load(id); len(got) != 0 {
			t.Errorf("Load(%q): expected an empty map, got %+v", id, got)
		}
		if err := Save(id, map[string]string{"h": "peer-a"}); err == nil {
			t.Errorf("Save(%q): expected an error, got nil", id)
		}
		Delete(id) // must not panic or touch anything outside dir
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no files written for any malicious sessionID, got %+v", entries)
	}
}

func TestHashSecretIsDeterministicAndDistinct(t *testing.T) {
	if HashSecret("a") != HashSecret("a") {
		t.Fatal("expected the same secret to hash the same way every time")
	}
	if HashSecret("a") == HashSecret("b") {
		t.Fatal("expected distinct secrets to hash differently")
	}
}
