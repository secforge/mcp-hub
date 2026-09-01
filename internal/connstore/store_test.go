package connstore

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUpsertThenGetRoundTrips(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Host: "wss://mcp-hub.secforge.de", SessionID: "550e8400-e29b-41d4-a716-446655440000"}
	entry := Entry{
		Host: target.Host, SessionID: target.SessionID,
		PeerID: "peer-1", Name: "Alice", ReconnectSecret: "s3cr3t",
		LastConnectedAt: time.Now().UTC().Truncate(time.Second), Connected: true,
	}
	if err := Upsert(entry); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, ok := Get(target)
	if !ok {
		t.Fatal("expected an entry to be found")
	}
	if got.PeerID != "peer-1" || got.ReconnectSecret != "s3cr3t" || !got.Connected {
		t.Fatalf("got %+v", got)
	}
}

func TestGetReturnsFalseWhenNothingStored(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	_, ok := Get(Target{Host: "wss://never-seen", SessionID: "550e8400-e29b-41d4-a716-446655440000"})
	if ok {
		t.Fatal("expected no entry to be found")
	}
}

func TestUpsertOverwritesExistingEntryForSameTarget(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Host: "wss://mcp-hub.secforge.de", SessionID: "550e8400-e29b-41d4-a716-446655440000"}
	if err := Upsert(Entry{Host: target.Host, SessionID: target.SessionID, PeerID: "peer-1"}); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if err := Upsert(Entry{Host: target.Host, SessionID: target.SessionID, PeerID: "peer-2"}); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	got, ok := Get(target)
	if !ok || got.PeerID != "peer-2" {
		t.Fatalf("expected the second upsert to overwrite the first, got %+v (ok=%v)", got, ok)
	}
}

func TestMarkDisconnectedClearsConnectedFlag(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Host: "wss://mcp-hub.secforge.de", SessionID: "550e8400-e29b-41d4-a716-446655440000"}
	if err := Upsert(Entry{Host: target.Host, SessionID: target.SessionID, PeerID: "peer-1", Connected: true}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	if err := MarkDisconnected(target); err != nil {
		t.Fatalf("markDisconnected: %v", err)
	}

	got, ok := Get(target)
	if !ok {
		t.Fatal("expected the entry to still exist")
	}
	if got.Connected {
		t.Fatal("expected Connected to be false after MarkDisconnected")
	}
}

func TestMarkDisconnectedOnUnknownTargetIsANoop(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	if err := MarkDisconnected(Target{Host: "wss://never-seen", SessionID: "550e8400-e29b-41d4-a716-446655440000"}); err != nil {
		t.Fatalf("expected no error for an unknown target, got %v", err)
	}
}

func TestListReturnsAllEntries(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	if err := Upsert(Entry{Host: "wss://a", SessionID: "550e8400-e29b-41d4-a716-446655440000", PeerID: "peer-a"}); err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	if err := Upsert(Entry{Host: "wss://b", SessionID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", PeerID: "peer-b"}); err != nil {
		t.Fatalf("upsert b: %v", err)
	}

	entries, err := List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d: %+v", len(entries), entries)
	}
}

func TestListReturnsEmptyWhenNothingPersisted(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	entries, err := List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no entries, got %+v", entries)
	}
}

func TestGetReturnsFalseWhenStoreFileIsMissing(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_CONNSTORE_DIR", dir)

	// Never call Upsert — the directory/file genuinely doesn't exist yet.
	if _, err := os.Stat(filepath.Join(dir, "connections.json")); !os.IsNotExist(err) {
		t.Fatalf("expected no file to exist yet, stat err: %v", err)
	}
	if _, ok := Get(Target{Host: "wss://x", SessionID: "550e8400-e29b-41d4-a716-446655440000"}); ok {
		t.Fatal("expected no entry when the store file doesn't exist")
	}
}

func TestUpsertWritesAtomicallyViaTempFileAndRename(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_CONNSTORE_DIR", dir)

	if err := Upsert(Entry{Host: "wss://a", SessionID: "550e8400-e29b-41d4-a716-446655440000", PeerID: "peer-a"}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("expected no leftover .tmp file after a successful Upsert, found %q", e.Name())
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "connections.json")); err != nil {
		t.Fatalf("expected connections.json to exist: %v", err)
	}
}
