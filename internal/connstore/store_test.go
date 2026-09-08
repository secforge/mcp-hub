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

// TestConcurrentUpsertsUnderLockDoNotLoseUpdates proves the fix for the
// documented race a bare load-then-save has: many goroutines (standing in
// for many mcp-hub-client processes) each Upsert a distinct entry
// concurrently, and every single one must survive — none silently
// clobbered by another's stale in-memory snapshot winning the save race.
// Without withLock's serialization, this reliably loses updates.
func TestConcurrentUpsertsUnderLockDoNotLoseUpdates(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			target := Target{Host: "wss://mcp-hub.secforge.de", SessionID: fmt.Sprintf("550e8400-e29b-41d4-a716-4466554400%02d", i)}
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

func TestDifferentProjectsWithSameHostAndSessionAreDistinctEntries(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Host: "wss://mcp-hub.secforge.de", SessionID: "550e8400-e29b-41d4-a716-446655440000"}
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
		t.Fatalf("expected 2 distinct entries for the same host+sessionId but different projects, got %d: %+v",
			len(entries), entries)
	}
}

func TestSameProjectHostAndSessionOverwritesNotDuplicates(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Host: "wss://mcp-hub.secforge.de", SessionID: "550e8400-e29b-41d4-a716-446655440000", Project: "/projects/a"}
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

	target := Target{Host: "wss://mcp-hub.secforge.de", SessionID: "550e8400-e29b-41d4-a716-446655440000"}
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

// TestUpsertPreservesExistingCatchUpState is the regression test for the
// hazard the 2026-09-08 rewrite's Upsert doc comment calls out: folding
// CatchUp into Entry means a naive full-replace Upsert (as a reconnect's
// identity-only Entry would trigger) would silently wipe the persisted
// read position on every reconnect. Upsert must preserve it instead.
func TestUpsertPreservesExistingCatchUpState(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Host: "wss://mcp-hub.secforge.de", SessionID: "550e8400-e29b-41d4-a716-446655440000"}
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

	_, ok := Get(Target{Host: "wss://never-seen", SessionID: "550e8400-e29b-41d4-a716-446655440000"})
	if ok {
		t.Fatal("expected no entry to be found")
	}
}

func TestUpsertOverwritesExistingEntryForSameTarget(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Host: "wss://mcp-hub.secforge.de", SessionID: "550e8400-e29b-41d4-a716-446655440000"}
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

	target := Target{Host: "wss://mcp-hub.secforge.de", SessionID: "550e8400-e29b-41d4-a716-446655440000"}
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

	if err := MarkDisconnected(Target{Host: "wss://never-seen", SessionID: "550e8400-e29b-41d4-a716-446655440000"}); err != nil {
		t.Fatalf("expected no error for an unknown target, got %v", err)
	}
}

func TestListReturnsAllEntries(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	if err := Upsert(Target{Host: "wss://a", SessionID: "550e8400-e29b-41d4-a716-446655440000"}, Entry{PeerID: "peer-a"}); err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	if err := Upsert(Target{Host: "wss://b", SessionID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8"}, Entry{PeerID: "peer-b"}); err != nil {
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
	if _, ok := Get(Target{Host: "wss://x", SessionID: "550e8400-e29b-41d4-a716-446655440000"}); ok {
		t.Fatal("expected no entry when the store file doesn't exist")
	}
}

func TestUpsertWritesAtomicallyViaTempFileAndRename(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_CONNSTORE_DIR", dir)

	if err := Upsert(Target{Host: "wss://a", SessionID: "550e8400-e29b-41d4-a716-446655440000"}, Entry{PeerID: "peer-a"}); err != nil {
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

// TestProjectIsTheOutermostKeyOnDisk proves the 2026-09-08 restructuring
// the user asked for directly: "shouldn't the main key be the project,
// because every project has its own connections?" — every entry for one
// project must be reachable as one contiguous block, not scattered
// across every host/sessionId/link a machine has ever seen. Also proves
// the follow-up merge ("merge hub and teams lists"): both connection
// kinds live in the SAME per-project map, distinguished only by their
// "hub "/"teams " key prefix — not two separate top-level sections.
func TestProjectIsTheOutermostKeyOnDisk(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Host: "wss://a", SessionID: "550e8400-e29b-41d4-a716-446655440000", Project: "/proj/x"}
	if err := Upsert(target, Entry{PeerID: "peer-1"}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	teamsID := TeamsID{LinkTarget: "wss://b", Project: "/proj/x"}
	if err := SetTeamsCatchUp(teamsID, CatchUpState{Cursor: "teams-cursor"}); err != nil {
		t.Fatalf("SetTeamsCatchUp: %v", err)
	}

	data, err := os.ReadFile(path())
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	var raw map[string]map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	byKey, ok := raw["/proj/x"]
	if !ok {
		t.Fatalf("expected the project keyed at the top level, got: %s", data)
	}

	// Host+SessionID combine into one space-joined key line prefixed
	// "hub ", and the teams entry lives right alongside it in the SAME
	// map, prefixed "teams " — not a further nesting level, not a
	// separate top-level section.
	wantHubKey := "hub wss://a 550e8400-e29b-41d4-a716-446655440000"
	if _, ok := byKey[wantHubKey]; !ok {
		t.Fatalf("expected the combined key %q, got: %s", wantHubKey, data)
	}
	wantTeamsKey := "teams wss://b"
	if _, ok := byKey[wantTeamsKey]; !ok {
		t.Fatalf("expected the combined key %q, got: %s", wantTeamsKey, data)
	}
}

// TestHubEntryTopicRoundTripsAndSurvivesReconnect proves the
// conversation's own display name (distinct from the peer's own Name)
// persists via Upsert and, like CatchUp, survives a later Upsert that
// doesn't carry a Topic of its own — built 2026-09-08 at the project
// owner's own request ("if the name of the conversation is known to the
// mcp, it should write that too").
func TestHubEntryTopicRoundTripsAndSurvivesReconnect(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Host: "wss://a", SessionID: "550e8400-e29b-41d4-a716-446655440000"}
	if err := Upsert(target, Entry{PeerID: "peer-1", Topic: "Q3 Planning"}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, ok := Get(target)
	if !ok || got.Topic != "Q3 Planning" {
		t.Fatalf("got %+v (ok=%v)", got, ok)
	}
}

// TestSetTeamsTopicPreservesCatchUpAndViceVersa proves the two setters
// on a teams entry (SetTeamsTopic, SetTeamsCatchUp) merge rather than
// clobber each other — the same hazard Upsert's own doc comment covers
// on the hub side.
func TestSetTeamsTopicPreservesCatchUpAndViceVersa(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	teamsID := TeamsID{LinkTarget: "wss://relay.example/join?c=abc", Project: "/proj"}
	if err := SetTeamsCatchUp(teamsID, CatchUpState{Cursor: "cursor-1"}); err != nil {
		t.Fatalf("SetTeamsCatchUp: %v", err)
	}
	if err := SetTeamsTopic(teamsID, "Design Review"); err != nil {
		t.Fatalf("SetTeamsTopic: %v", err)
	}

	cs, ok := GetTeamsCatchUp(teamsID)
	if !ok || cs.Cursor != "cursor-1" {
		t.Fatalf("expected the catch-up cursor to survive SetTeamsTopic, got %+v (ok=%v)", cs, ok)
	}

	s := load()
	e := s[teamsID.Project][teamsKey(teamsID.LinkTarget)]
	if e.Topic != "Design Review" {
		t.Fatalf("expected the topic to survive too, got %+v", e)
	}
}

func TestSetCatchUpGapAndAheadRoundTrip(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	target := Target{Host: "wss://mcp-hub.secforge.de", SessionID: "550e8400-e29b-41d4-a716-446655440000"}
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

// TestTeamsCatchUpIsIndependentOfHubCatchUp proves a bridge session's
// catch-up state lives under its own identity (TeamsID), never colliding
// with a hub_connect Target that happens to share the same-looking
// strings.
func TestTeamsCatchUpIsIndependentOfHubCatchUp(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	teamsID := TeamsID{LinkTarget: "wss://chat-relay.example/relay/join?c=abc", Project: "/proj"}
	if err := SetTeamsCatchUp(teamsID, CatchUpState{Cursor: "teams-cursor"}); err != nil {
		t.Fatalf("SetTeamsCatchUp: %v", err)
	}

	hubTarget := Target{Host: "wss://chat-relay.example/relay/join?c=abc", SessionID: "", Project: "/proj"}
	if _, ok := GetCatchUp(hubTarget); ok {
		t.Fatal("expected no hub catch-up entry to exist just because a teams one shares similar strings")
	}
	cs, ok := GetTeamsCatchUp(teamsID)
	if !ok || cs.Cursor != "teams-cursor" {
		t.Fatalf("got %+v (ok=%v)", cs, ok)
	}
}

// TestCatchUpIDDispatchesToTheRightBackingStore proves the CatchUpID
// union type mcptools.Hub keeps a single field of correctly dispatches
// Get/Set to the hub or teams backing store depending on which is set.
func TestCatchUpIDDispatchesToTheRightBackingStore(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	hubID := HubCatchUpID(Target{Host: "wss://a", SessionID: "550e8400-e29b-41d4-a716-446655440000"})
	if err := hubID.Set(CatchUpState{Cursor: "hub-cursor"}); err != nil {
		t.Fatalf("hubID.Set: %v", err)
	}
	teamsID := TeamsCatchUpID(TeamsID{LinkTarget: "wss://b"})
	if err := teamsID.Set(CatchUpState{Cursor: "teams-cursor"}); err != nil {
		t.Fatalf("teamsID.Set: %v", err)
	}

	if cs, ok := hubID.Get(); !ok || cs.Cursor != "hub-cursor" {
		t.Fatalf("got %+v (ok=%v)", cs, ok)
	}
	if cs, ok := teamsID.Get(); !ok || cs.Cursor != "teams-cursor" {
		t.Fatalf("got %+v (ok=%v)", cs, ok)
	}

	var zero CatchUpID
	if zero.Valid() {
		t.Fatal("expected the zero CatchUpID to be invalid")
	}
	if _, ok := zero.Get(); ok {
		t.Fatal("expected the zero CatchUpID's Get to report false")
	}
	if err := zero.Set(CatchUpState{Cursor: "x"}); err != nil {
		t.Fatalf("expected the zero CatchUpID's Set to be a silent no-op, got err: %v", err)
	}
}
