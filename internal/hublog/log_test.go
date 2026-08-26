package hublog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenSessionLogAppendsFormattedEntries(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	l, err := OpenSessionLog(sessionID)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := l.Append("peer-1", "hello", "2026-08-21T10:00:00Z"); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, sessionID+".log"))
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	want := FormatEntry("2026-08-21T10:00:00Z", "peer-1", "hello")
	if string(data) != want {
		t.Fatalf("got %q, want %q", data, want)
	}
}

func TestAppendDirectedWritesTargetedEntry(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	l, err := OpenSessionLog(sessionID)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := l.AppendDirected("peer-1", "peer-2", "hush", "2026-08-21T10:00:00Z"); err != nil {
		t.Fatalf("append directed: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, sessionID+".log"))
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	want := FormatDirectedEntry("2026-08-21T10:00:00Z", "peer-1", "peer-2", "hush")
	if string(data) != want {
		t.Fatalf("got %q, want %q", data, want)
	}
}

func TestAppendJoinedAndLeftWriteExpectedEntries(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	l, err := OpenSessionLog(sessionID)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := l.AppendJoined("peer-1", "", "", false, false, "2026-08-21T10:00:00Z"); err != nil {
		t.Fatalf("append joined: %v", err)
	}
	if err := l.AppendLeft("peer-1", "2026-08-21T10:05:00Z"); err != nil {
		t.Fatalf("append left: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, sessionID+".log"))
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	want := FormatJoinedEntry("2026-08-21T10:00:00Z", "peer-1", "", "", false, false) + FormatLeftEntry("2026-08-21T10:05:00Z", "peer-1")
	if string(data) != want {
		t.Fatalf("got %q, want %q", data, want)
	}
}
