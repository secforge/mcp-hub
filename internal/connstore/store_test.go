package connstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestConcurrentUpsertsUnderLockDoNotLoseUpdates proves withLock closes the
// race a bare load-then-save has: many goroutines (standing in for many
// mcp-hub-client processes) each Upsert a distinct entry concurrently, and
// every single one must survive — none silently clobbered by another's stale
// in-memory snapshot winning the save race.
func TestConcurrentUpsertsUnderLockDoNotLoseUpdates(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			target := Target{Link: fmt.Sprintf("wss://mcp-hub.secforge.de/hub/join-%02d", i)}
			err := Upsert(target, Entry{PeerID: fmt.Sprintf("peer-%d", i)})
			if err != nil {
				t.Errorf("upsert %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	entries, err := List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != n {
		t.Fatalf("expected all %d concurrent upserts to survive, got %d entries", n, len(entries))
	}
}

func TestCurrentProjectUsesOverrideEnvVarWhenSet(t *testing.T) {
	t.Setenv("MCP_HUB_PROJECT_DIR", "/some/project/dir")
	if got := CurrentProject(); got != "/some/project/dir" {
		t.Fatalf("expected the override to win, got %q", got)
	}
}

func TestCurrentProjectFallsBackToWorkingDirectory(t *testing.T) {
	t.Setenv("MCP_HUB_PROJECT_DIR", "")
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	if got := CurrentProject(); got != wd {
		t.Fatalf("expected the working directory %q, got %q", wd, got)
	}
}

func TestDifferentProjectsWithTheSameLinkAreDistinctEntries(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Link: "wss://mcp-hub.secforge.de/hub/join"}
	targetA := target
	targetA.Project = "/projects/a"
	targetB := target
	targetB.Project = "/projects/b"

	if err := Upsert(targetA, Entry{ReconnectSecret: "secret-a"}); err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	if err := Upsert(targetB, Entry{ReconnectSecret: "secret-b"}); err != nil {
		t.Fatalf("upsert b: %v", err)
	}

	gotA, ok := Get(targetA)
	if !ok || gotA.ReconnectSecret != "secret-a" {
		t.Fatalf("expected project a's own entry, got %+v (ok=%v)", gotA, ok)
	}
	gotB, ok := Get(targetB)
	if !ok || gotB.ReconnectSecret != "secret-b" {
		t.Fatalf("expected project b's own entry, got %+v (ok=%v)", gotB, ok)
	}

	entries, err := List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 distinct entries for the same link under different projects, got %d: %+v",
			len(entries), entries)
	}
}

func TestSameProjectAndLinkOverwritesNotDuplicates(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Link: "wss://mcp-hub.secforge.de/hub/join", Project: "/projects/a"}
	if err := Upsert(target, Entry{PeerID: "peer-1"}); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if err := Upsert(target, Entry{PeerID: "peer-2"}); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	got, ok := Get(target)
	if !ok || got.PeerID != "peer-2" {
		t.Fatalf("expected the second upsert to overwrite the first within the same project, got %+v (ok=%v)", got, ok)
	}
	entries, err := List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 entry, got %d: %+v", len(entries), entries)
	}
}

func TestUpsertThenGetRoundTrips(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Link: "wss://mcp-hub.secforge.de/hub/join"}
	entry := Entry{
		PeerID: "peer-1", Name: "Alice", ReconnectSecret: "s3cr3t",
		LastConnectedAt: time.Now().UTC().Truncate(time.Second), Connected: true,
	}
	if err := Upsert(target, entry); err != nil {
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

// TestUpsertPreservesExistingCatchUpState guards the hazard Upsert's own doc
// comment names: CatchUp lives inside Entry, so a naive full-replace Upsert —
// which is exactly what a reconnect's identity-only Entry looks like — would
// silently wipe the persisted read position on every reconnect.
func TestUpsertPreservesExistingCatchUpState(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Link: "wss://mcp-hub.secforge.de/hub/join"}
	if err := Upsert(target, Entry{PeerID: "peer-1"}); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if err := SetCatchUp(target, CatchUpState{Cursor: "cursor-1"}); err != nil {
		t.Fatalf("SetCatchUp: %v", err)
	}

	// A reconnect's Upsert never carries CatchUp itself.
	if err := Upsert(target, Entry{PeerID: "peer-2", Connected: true}); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	cs, ok := GetCatchUp(target)
	if !ok || cs.Cursor != "cursor-1" {
		t.Fatalf("expected the catch-up cursor to survive a reconnect's Upsert, got %+v (ok=%v)", cs, ok)
	}
	got, ok := Get(target)
	if !ok || got.PeerID != "peer-2" || !got.Connected {
		t.Fatalf("expected the identity fields to still update, got %+v (ok=%v)", got, ok)
	}
}

func TestGetReturnsFalseWhenNothingStored(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	_, ok := Get(Target{Link: "wss://never-seen/hub/join"})
	if ok {
		t.Fatal("expected no entry to be found")
	}
}

func TestUpsertOverwritesExistingEntryForSameTarget(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Link: "wss://mcp-hub.secforge.de/hub/join"}
	if err := Upsert(target, Entry{PeerID: "peer-1"}); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if err := Upsert(target, Entry{PeerID: "peer-2"}); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	got, ok := Get(target)
	if !ok || got.PeerID != "peer-2" {
		t.Fatalf("expected the second upsert to overwrite the first, got %+v (ok=%v)", got, ok)
	}
}

func TestMarkDisconnectedClearsConnectedFlag(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Link: "wss://mcp-hub.secforge.de/hub/join"}
	if err := Upsert(target, Entry{PeerID: "peer-1", Connected: true}); err != nil {
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

	if err := MarkDisconnected(Target{Link: "wss://never-seen/hub/join"}); err != nil {
		t.Fatalf("expected no error for an unknown target, got %v", err)
	}
}

func TestListReturnsAllEntries(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	if err := Upsert(Target{Link: "wss://a/hub/join"}, Entry{PeerID: "peer-a"}); err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	if err := Upsert(Target{Link: "wss://b/teams/join"}, Entry{PeerID: "peer-b"}); err != nil {
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
	if _, err := os.Stat(filepath.Join(dir, "state.json")); !os.IsNotExist(err) {
		t.Fatalf("expected no file to exist yet, stat err: %v", err)
	}
	if _, ok := Get(Target{Link: "wss://x/hub/join"}); ok {
		t.Fatal("expected no entry when the store file doesn't exist")
	}
}

func TestUpsertWritesAtomicallyViaTempFileAndRename(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_CONNSTORE_DIR", dir)

	if err := Upsert(Target{Link: "wss://a/hub/join"}, Entry{PeerID: "peer-a"}); err != nil {
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
	if _, err := os.Stat(filepath.Join(dir, "state.json")); err != nil {
		t.Fatalf("expected state.json to exist: %v", err)
	}
}

// TestProjectIsTheOutermostKeyOnDisk proves the on-disk shape: every entry
// for one project is reachable as one contiguous block rather than scattered
// across every link a machine has ever seen, and each connection is keyed by
// its own link directly inside that block — one flat map per project, with no
// further nesting and no per-kind section.
func TestProjectIsTheOutermostKeyOnDisk(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	hubTarget := Target{Link: "wss://a/hub/join", Project: "/proj/x"}
	if err := Upsert(hubTarget, Entry{PeerID: "peer-1"}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	teamsTarget := Target{Link: "wss://b/teams/join", Project: "/proj/x"}
	if err := SetCatchUp(teamsTarget, CatchUpState{Cursor: "teams-cursor"}); err != nil {
		t.Fatalf("SetCatchUp: %v", err)
	}

	data, err := os.ReadFile(path())
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	var raw map[string]map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	byLink, ok := raw["/proj/x"]
	if !ok {
		t.Fatalf("expected the project keyed at the top level, got: %s", data)
	}

	// Both connections live side by side in the SAME per-project map, each
	// under its own link verbatim.
	if _, ok := byLink[hubTarget.Link]; !ok {
		t.Fatalf("expected the link %q as a key, got: %s", hubTarget.Link, data)
	}
	if _, ok := byLink[teamsTarget.Link]; !ok {
		t.Fatalf("expected the link %q as a key, got: %s", teamsTarget.Link, data)
	}
}

// TestEntryTopicRoundTrips proves the conversation's own display name
// (distinct from the peer's own Name) persists via Upsert.
func TestEntryTopicRoundTrips(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Link: "wss://a/hub/join"}
	if err := Upsert(target, Entry{PeerID: "peer-1", Topic: "Q3 Planning"}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, ok := Get(target)
	if !ok || got.Topic != "Q3 Planning" {
		t.Fatalf("got %+v (ok=%v)", got, ok)
	}
}

// TestSetTopicPreservesCatchUpAndViceVersa proves the two single-field
// setters merge rather than clobber each other — the same hazard Upsert's own
// doc comment covers.
func TestSetTopicPreservesCatchUpAndViceVersa(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Link: "wss://relay.example/teams/join", Project: "/proj"}
	if err := SetCatchUp(target, CatchUpState{Cursor: "cursor-1"}); err != nil {
		t.Fatalf("SetCatchUp: %v", err)
	}
	if err := SetTopic(target, "Design Review"); err != nil {
		t.Fatalf("SetTopic: %v", err)
	}

	cs, ok := GetCatchUp(target)
	if !ok || cs.Cursor != "cursor-1" {
		t.Fatalf("expected the catch-up cursor to survive SetTopic, got %+v (ok=%v)", cs, ok)
	}

	s := load()
	e := s[target.Project][target.Link]
	if e.Topic != "Design Review" {
		t.Fatalf("expected the topic to survive too, got %+v", e)
	}
}

func TestSetCatchUpGapAndAheadRoundTrip(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Link: "wss://mcp-hub.secforge.de/hub/join"}
	cs := CatchUpState{
		Cursor: "cursor-1",
		Gap:    &GapState{From: "2026-09-01T00:00:00Z", To: "2026-09-01T01:00:00Z"},
		Ahead:  []string{"cursor-2", "cursor-3"},
	}
	if err := SetCatchUp(target, cs); err != nil {
		t.Fatalf("SetCatchUp: %v", err)
	}

	got, ok := GetCatchUp(target)
	if !ok {
		t.Fatal("expected catch-up state to be found")
	}
	if got.Cursor != "cursor-1" || got.Gap == nil || got.Gap.From != "2026-09-01T00:00:00Z" || len(got.Ahead) != 2 {
		t.Fatalf("got %+v", got)
	}
}

// TestCatchUpForDifferentLinksIsIndependent proves one connection's read
// position never leaks into another's.
func TestCatchUpForDifferentLinksIsIndependent(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	one := Target{Link: "wss://chat-relay.example/teams/join", Project: "/proj"}
	two := Target{Link: "wss://chat-relay.example/hub/join", Project: "/proj"}
	if err := SetCatchUp(one, CatchUpState{Cursor: "cursor-one"}); err != nil {
		t.Fatalf("SetCatchUp one: %v", err)
	}

	if _, ok := GetCatchUp(two); ok {
		t.Fatal("expected no catch-up state for a link nothing was stored under")
	}
	cs, ok := GetCatchUp(one)
	if !ok || cs.Cursor != "cursor-one" {
		t.Fatalf("got %+v (ok=%v)", cs, ok)
	}
}
