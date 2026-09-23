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

	entries, err := ListForProject("")
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
	want, err := filepath.EvalSymlinks(wd)
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}
	if got := CurrentProject(); got != want {
		t.Fatalf("expected the working directory %q, got %q", want, got)
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

	gotA, ok, _ := Get(targetA)
	if !ok || gotA.ReconnectSecret != "secret-a" {
		t.Fatalf("expected project a's own entry, got %+v (ok=%v)", gotA, ok)
	}
	gotB, ok, _ := Get(targetB)
	if !ok || gotB.ReconnectSecret != "secret-b" {
		t.Fatalf("expected project b's own entry, got %+v (ok=%v)", gotB, ok)
	}

	// Each project sees ITS OWN entry and only that one. Asserted per
	// scope rather than by listing both at once, because there is no
	// cross-project read to list them with — that is the property under
	// test, and a test that needed one would be asserting the isolation
	// through a hole in it.
	for _, tc := range []struct {
		project, secret string
	}{
		{"/projects/a", "secret-a"},
		{"/projects/b", "secret-b"},
	} {
		entries, err := ListForProject(tc.project)
		if err != nil {
			t.Fatalf("list %s: %v", tc.project, err)
		}
		if len(entries) != 1 {
			t.Fatalf("%s sees %d entries, want exactly its own: %+v", tc.project, len(entries), entries)
		}
		if entries[0].Entry.ReconnectSecret != tc.secret {
			t.Fatalf("%s sees %q, want its own %q — the same link under another project is another "+
				"entry, and neither scope may see the other's",
				tc.project, entries[0].Entry.ReconnectSecret, tc.secret)
		}
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

	got, ok, _ := Get(target)
	if !ok || got.PeerID != "peer-2" {
		t.Fatalf("expected the second upsert to overwrite the first within the same project, got %+v (ok=%v)", got, ok)
	}
	entries, err := ListForProject("/projects/a")
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

	got, ok, _ := Get(target)
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

	cs, ok, _ := GetCatchUp(target)
	if !ok || cs.Cursor != "cursor-1" {
		t.Fatalf("expected the catch-up cursor to survive a reconnect's Upsert, got %+v (ok=%v)", cs, ok)
	}
	got, ok, _ := Get(target)
	if !ok || got.PeerID != "peer-2" || !got.Connected {
		t.Fatalf("expected the identity fields to still update, got %+v (ok=%v)", got, ok)
	}
}

func TestGetReturnsFalseWhenNothingStored(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	_, ok, _ := Get(Target{Link: "wss://never-seen/hub/join"})
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

	got, ok, _ := Get(target)
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

	got, ok, _ := Get(target)
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

	entries, err := ListForProject("")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d: %+v", len(entries), entries)
	}
}

func TestListReturnsEmptyWhenNothingPersisted(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	entries, err := ListForProject("")
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
	if _, ok, _ := Get(Target{Link: "wss://x/hub/join"}); ok {
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
	got, ok, _ := Get(target)
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

	cs, ok, _ := GetCatchUp(target)
	if !ok || cs.Cursor != "cursor-1" {
		t.Fatalf("expected the catch-up cursor to survive SetTopic, got %+v (ok=%v)", cs, ok)
	}

	s, err := load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
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
		Gap:    &GapState{FromAt: "2026-09-01T00:00:00Z", To: "2026-09-01T01:00:00Z"},
		Ahead:  []string{"cursor-2", "cursor-3"},
	}
	if err := SetCatchUp(target, cs); err != nil {
		t.Fatalf("SetCatchUp: %v", err)
	}

	got, ok, _ := GetCatchUp(target)
	if !ok {
		t.Fatal("expected catch-up state to be found")
	}
	if got.Cursor != "cursor-1" || got.Gap == nil || got.Gap.FromAt != "2026-09-01T00:00:00Z" || len(got.Ahead) != 2 {
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

	if _, ok, _ := GetCatchUp(two); ok {
		t.Fatal("expected no catch-up state for a link nothing was stored under")
	}
	cs, ok, _ := GetCatchUp(one)
	if !ok || cs.Cursor != "cursor-one" {
		t.Fatalf("got %+v (ok=%v)", cs, ok)
	}
}

// A gap recorded by an older build carries its start in one field that
// held either a cursor or a timestamp. Dropping those records on upgrade
// would lose the one thing they exist to keep: a range nothing has
// walked to, which nobody would otherwise know to go looking for.
func TestALegacyGapRecordIsSortedIntoTheRightField(t *testing.T) {
	t.Setenv("MCP_HUB_LOG_DIR", t.TempDir())
	t.Setenv("MCP_HUB_PROJECT_DIR", t.TempDir())

	for _, tc := range []struct {
		name       string
		legacy     string
		wantCursor string
		wantAt     string
	}{
		{"a timestamp stays a timestamp", "2026-09-01T00:00:00Z", "", "2026-09-01T00:00:00Z"},
		{"anything else is a cursor", "639251841733942000.45797", "639251841733942000.45797", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := GapState{LegacyFrom: tc.legacy, To: "2026-09-01T01:00:00Z"}
			g.normalise()
			if g.FromCursor != tc.wantCursor || g.FromAt != tc.wantAt {
				t.Fatalf("got cursor=%q at=%q, want cursor=%q at=%q",
					g.FromCursor, g.FromAt, tc.wantCursor, tc.wantAt)
			}
			if g.LegacyFrom != "" {
				t.Fatalf("expected the legacy field to be cleared once sorted, got %q", g.LegacyFrom)
			}
			if !g.Started() {
				t.Fatal("expected the migrated gap to still count as a recorded gap")
			}
		})
	}
}

// A store that cannot be parsed must not be replaced by the next write.
// Quarantining it from a READ left no file at all, so the following
// write saw "nothing stored", created a fresh store, and every other
// identity and reading position stopped being active state — silently,
// with connect reporting success.
func TestAnUnreadableStoreIsNotReplacedByTheNextWrite(t *testing.T) {
	// MCP_HUB_CONNSTORE_DIR is what moves the file this test WRITES OVER.
	// Setting anything else points path() at the real store, and this
	// test destroys whatever it finds there.
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	t.Setenv("MCP_HUB_PROJECT_DIR", t.TempDir())

	keep := Target{Link: "wss://example.test/hub/join#keep", Project: "p"}
	if err := SetReconnectSecret(keep, "the-secret-that-must-survive"); err != nil {
		t.Fatalf("seeding the store: %v", err)
	}

	if err := os.WriteFile(path(), []byte("{not json at all"), 0o600); err != nil {
		t.Fatalf("corrupting the store: %v", err)
	}

	if _, _, err := Get(keep); err == nil {
		t.Fatal("expected a read of an unreadable store to report the failure, not an empty answer")
	}
	// The file is still there to be recovered from.
	if _, err := os.Stat(path()); err != nil {
		t.Fatalf("expected the unreadable store to be left in place, got: %v", err)
	}

	other := Target{Link: "wss://example.test/hub/join#other", Project: "p"}
	if err := SetReconnectSecret(other, "a-new-secret"); err == nil {
		t.Fatal("expected a write against an unreadable store to be refused, not to replace it")
	}

	// The unreadable bytes are still the ones on disk — nothing replaced
	// them, which is what leaves a repair possible at all.
	raw, err := os.ReadFile(path())
	if err != nil {
		t.Fatalf("reading the store: %v", err)
	}
	if string(raw) != "{not json at all" {
		t.Fatalf("the unreadable store was modified: %s", raw)
	}
	// And moving it aside is available, but only as a deliberate act.
	aside, err := MoveAside()
	if err != nil {
		t.Fatalf("MoveAside: %v", err)
	}
	if _, err := os.Stat(aside); err != nil {
		t.Fatalf("expected the old bytes to be kept at %s: %v", aside, err)
	}
	if err := SetReconnectSecret(other, "a-new-secret"); err != nil {
		t.Fatalf("expected writes to work once the store was moved aside: %v", err)
	}
}

// TestMain refuses to run this package's tests against the REAL store.
//
// These tests write to path(), and one of them deliberately corrupts what
// it finds there. The isolation is one environment variable, and setting
// a plausible-looking wrong one (MCP_HUB_LOG_DIR, say) points path() at
// the user's own file with no warning at all — which is how this package
// destroyed a real store holding every identity and reading position on
// this machine. A default that must be overridden correctly by every
// test, forever, is not isolation; this is.
func TestMain(m *testing.M) {
	if os.Getenv("MCP_HUB_CONNSTORE_DIR") == "" {
		tmp, err := os.MkdirTemp("", "connstore-tests-")
		if err != nil {
			fmt.Fprintf(os.Stderr, "cannot create a temp store dir: %v\n", err)
			os.Exit(1)
		}
		os.Setenv("MCP_HUB_CONNSTORE_DIR", tmp)
		defer os.RemoveAll(tmp)
	}
	os.Exit(m.Run())
}

// The mode contract in this package's doc comment is enforced, not
// merely requested. Both halves were stated and neither was true:
// os.MkdirAll leaves an existing directory's mode alone, and
// os.WriteFile's perm applies only when it creates the file — so a
// state.json.tmp left behind by a save that died between the write and
// the rename was reopened at its old mode and the rename handed that mode
// to state.json. Found by an external reviewer.
func TestTheModeContractIsEnforcedNotRequested(t *testing.T) {
	sub := filepath.Join(t.TempDir(), "store")
	// A directory that already exists, loosely: MkdirAll will not touch it.
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Setenv("MCP_HUB_CONNSTORE_DIR", sub)

	// A leftover temp file from a crashed save, world-readable.
	tmp := filepath.Join(sub, filepath.Base(path())+".tmp")
	if err := os.WriteFile(tmp, []byte("{}"), 0o644); err != nil {
		t.Fatalf("seed tmp: %v", err)
	}

	if err := save(state{}); err != nil {
		t.Fatalf("save: %v", err)
	}

	di, err := os.Stat(sub)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if got := di.Mode().Perm(); got != 0o700 {
		t.Errorf("state directory is %04o, want 0700 — a pre-existing loose directory stays loose "+
			"for the life of the install while the package doc claims otherwise", got)
	}

	fi, err := os.Stat(path())
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Errorf("state file is %04o, want 0600 — it inherited the mode of a temp file left behind "+
			"by an earlier crash, and it holds every reconnect secret this client has", got)
	}
}

// A READ FAILURE IS NOT "NOTHING STORED". An unreadable store must
// return an error rather than ok=false, or it looks exactly like a
// first-ever connection — and the caller then starts from no position,
// which re-walks a backlog at best and skips one wherever something else
// moves the position.
func TestGetCatchUpDistinguishesAnUnreadableStoreFromAnEmptyOne(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_CONNSTORE_DIR", dir)
	target := Target{Link: "wss://example.test/hub/join#s", Project: "/proj"}

	// Nothing stored: no error, and no state.
	cs, ok, err := GetCatchUp(target)
	if err != nil || ok || cs.Cursor != "" {
		t.Fatalf("an empty store reported cs=%+v ok=%v err=%v; want no state and no error",
			cs, ok, err)
	}

	// Something stored: no error, and the state.
	if err := SetCatchUp(target, CatchUpState{Cursor: "cursor-1"}); err != nil {
		t.Fatalf("SetCatchUp: %v", err)
	}
	if cs, ok, err = GetCatchUp(target); err != nil || !ok || cs.Cursor != "cursor-1" {
		t.Fatalf("stored state came back cs=%+v ok=%v err=%v", cs, ok, err)
	}

	// Unreadable: an ERROR, not an empty answer. A caller that cannot
	// tell these apart resumes from the beginning of a conversation it
	// has already read.
	if err := os.WriteFile(path(), []byte("{ this is not json"), 0o600); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	cs, ok, err = GetCatchUp(target)
	if err == nil {
		t.Fatalf("an unreadable store returned cs=%+v ok=%v and NO error — indistinguishable "+
			"from a first connection, which is the one thing it must not look like", cs, ok)
	}
	if ok || cs.Cursor != "" {
		t.Errorf("a failed read also returned state: cs=%+v ok=%v", cs, ok)
	}
}

// A TRAILING SEPARATOR IS THE SAME PROJECT. "/proj/" and "/proj" name one
// directory; filed as two keys, a session started with one spelling finds
// nothing the other stored, mints a new identity and strands the old one.
func TestProjectSpellingsOfOneDirectoryShareOneScope(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	link := "wss://example.test/hub/join#s"

	if err := Upsert(Target{Link: link, Project: "/proj"}, Entry{PeerID: "peer-1"}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	for _, spelling := range []string{"/proj/", "/proj//", "/proj/."} {
		got, ok, err := Get(Target{Link: link, Project: spelling})
		if err != nil || !ok || got.PeerID != "peer-1" {
			t.Errorf("Project %q: got %+v ok=%v err=%v; want the identity stored under /proj",
				spelling, got, ok, err)
		}
		listed, err := ListForProject(spelling)
		if err != nil || len(listed) != 1 || listed[0].Target.Project != "/proj" {
			t.Errorf("ListForProject(%q) = %+v, %v; want the one entry under /proj", spelling, listed, err)
		}
	}

	t.Setenv("MCP_HUB_PROJECT_DIR", "/proj/")
	if got := ProjectOverride(); got != "/proj" {
		t.Errorf("ProjectOverride() = %q with MCP_HUB_PROJECT_DIR=/proj/; want /proj", got)
	}
	if got := NormalizeProject(""); got != "" {
		t.Errorf(`NormalizeProject("") = %q; want "" — "." would be a scope of its own`, got)
	}
}

// A SYMLINK, A RELATIVE PATH AND A DIRECTORY THAT DOES NOT EXIST are
// canonicalised too. The last matters as much as the others: a scope can
// name a directory that was moved or never created, and one spelled
// through a symlink must still land where its real spelling would.
func TestNormalizeProjectCanonicalisesExistingAndMissingPaths(t *testing.T) {
	real, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}
	if err := os.Mkdir(filepath.Join(real, "proj"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	link := filepath.Join(real, "link")
	if err := os.Symlink(filepath.Join(real, "proj"), link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	t.Chdir(real)

	for _, tc := range []struct{ in, want string }{
		{filepath.Join(real, "proj"), filepath.Join(real, "proj")},
		{link, filepath.Join(real, "proj")},
		{link + "/", filepath.Join(real, "proj")},
		{"proj", filepath.Join(real, "proj")},
		{"./link/", filepath.Join(real, "proj")},
		{filepath.Join(link, "missing", "deeper"), filepath.Join(real, "proj", "missing", "deeper")},
		{filepath.Join(real, "missing") + "/", filepath.Join(real, "missing")},
		{"/does-not-exist/at/all/", "/does-not-exist/at/all"},
	} {
		if got := NormalizeProject(tc.in); got != tc.want {
			t.Errorf("NormalizeProject(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
