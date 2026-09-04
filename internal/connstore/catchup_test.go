package connstore

import (
	"sync"
	"testing"
)

func TestGetCatchUpCursorMissingKeyReturnsFalse(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	if _, ok := GetCatchUpCursor("never-set"); ok {
		t.Fatal("expected ok=false for a key that was never stored")
	}
}

func TestSetThenGetCatchUpCursorRoundTrips(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	if err := SetCatchUpCursor("key-1", "cursor-1"); err != nil {
		t.Fatalf("SetCatchUpCursor: %v", err)
	}
	got, ok := GetCatchUpCursor("key-1")
	if !ok || got != "cursor-1" {
		t.Fatalf("got (%q, %v), want (\"cursor-1\", true)", got, ok)
	}
}

func TestSetCatchUpCursorOverwritesPreviousValue(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	if err := SetCatchUpCursor("key-1", "cursor-1"); err != nil {
		t.Fatalf("SetCatchUpCursor: %v", err)
	}
	if err := SetCatchUpCursor("key-1", "cursor-2"); err != nil {
		t.Fatalf("SetCatchUpCursor: %v", err)
	}
	got, ok := GetCatchUpCursor("key-1")
	if !ok || got != "cursor-2" {
		t.Fatalf("got (%q, %v), want (\"cursor-2\", true)", got, ok)
	}
}

func TestCatchUpCursorKeysAreIndependent(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	if err := SetCatchUpCursor("key-a", "cursor-a"); err != nil {
		t.Fatalf("SetCatchUpCursor: %v", err)
	}
	if err := SetCatchUpCursor("key-b", "cursor-b"); err != nil {
		t.Fatalf("SetCatchUpCursor: %v", err)
	}
	if got, ok := GetCatchUpCursor("key-a"); !ok || got != "cursor-a" {
		t.Fatalf("key-a: got (%q, %v)", got, ok)
	}
	if got, ok := GetCatchUpCursor("key-b"); !ok || got != "cursor-b" {
		t.Fatalf("key-b: got (%q, %v)", got, ok)
	}
}

func TestEmptyKeyIsANoOpNotAnError(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	if err := SetCatchUpCursor("", "cursor-1"); err != nil {
		t.Fatalf("SetCatchUpCursor with empty key: %v", err)
	}
	if _, ok := GetCatchUpCursor(""); ok {
		t.Fatal("expected ok=false for an empty key")
	}
}

// TestConcurrentSetCatchUpCursorDoesNotLoseUpdates mirrors
// TestConcurrentUpsertsUnderLockDoNotLoseUpdates: many goroutines each
// SetCatchUpCursor a distinct key concurrently, and every one must
// survive — the shared withLock serialization this file reuses from
// connections.json must hold for catchup.json too.
func TestConcurrentSetCatchUpCursorDoesNotLoseUpdates(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
			if err := SetCatchUpCursor(key, key+"-cursor"); err != nil {
				t.Errorf("SetCatchUpCursor(%s): %v", key, err)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		key := "key-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		if got, ok := GetCatchUpCursor(key); !ok || got != key+"-cursor" {
			t.Fatalf("%s: got (%q, %v)", key, got, ok)
		}
	}
}
