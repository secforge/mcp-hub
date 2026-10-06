package connstore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// Several client versions share one store. A write changes the one entry
// it is for and nothing else: other entries keep every field, including
// ones this version does not know and the single-gap form an older
// client still uses. Its own entry keeps unknown fields too, and its
// single gap is rewritten as gaps.
func TestAWriteLeavesWhatItDoesNotOwnAlone(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_CONNSTORE_DIR", dir)
	const project = "/proj"
	const mine = "wss://example.test/hub/join#mine"
	const theirs = "wss://example.test/hub/join#theirs"
	const otherProject = "/other"
	seed := `{
  "/proj": {
    "wss://example.test/hub/join#mine": {
      "peerId": "p-mine",
      "futureField": {"kept": true},
      "catchUp": {
        "cursor": "c-mine",
        "gap": {"fromCursor": "g-mine", "to": "2026-09-01T00:00:00Z"},
        "futureCatchUp": 7
      }
    },
    "wss://example.test/hub/join#theirs": {
      "peerId": "p-theirs",
      "futureField": "x",
      "catchUp": {"gap": {"fromAt": "2026-08-01T00:00:00Z", "to": "2026-08-02T00:00:00Z"}}
    }
  },
  "/other": {"wss://example.test/hub/join#mine": {"peerId": "p-other", "catchUp": {"gap": {"fromCursor": "g-other"}}}}
}`
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(seed), 0o600); err != nil {
		t.Fatal(err)
	}
	before := readStore(t, dir)

	// Reading its own entry, this version sees the single gap as its first range.
	cs, ok, err := GetCatchUp(Target{Link: mine, Project: project})
	if err != nil || !ok || len(cs.Gaps) != 1 || cs.Gaps[0].FromCursor != "g-mine" {
		t.Fatalf("own single gap not read as a range: %+v ok=%v err=%v", cs, ok, err)
	}

	if err := UpdateCatchUp(Target{Link: mine, Project: project}, func(cs *CatchUpState) {
		cs.Gaps = append(cs.Gaps, GapState{FromAt: "2026-09-02T00:00:00Z", To: "2026-09-03T00:00:00Z"})
	}); err != nil {
		t.Fatal(err)
	}
	after := readStore(t, dir)

	for _, key := range [][2]string{{project, theirs}, {otherProject, mine}} {
		if b, a := string(before[key[0]][key[1]]), string(after[key[0]][key[1]]); a != b {
			t.Errorf("entry %v was changed by a write to another entry:\nbefore %s\nafter  %s", key, b, a)
		}
	}

	var own map[string]json.RawMessage
	json.Unmarshal(after[project][mine], &own)
	if string(own["futureField"]) != `{"kept":true}` || string(own["peerId"]) != `"p-mine"` {
		t.Errorf("own entry lost a field: %s", after[project][mine])
	}
	var catchUp map[string]json.RawMessage
	json.Unmarshal(own["catchUp"], &catchUp)
	if string(catchUp["futureCatchUp"]) != "7" || string(catchUp["cursor"]) != `"c-mine"` {
		t.Errorf("own catchUp lost a field: %s", own["catchUp"])
	}
	if _, has := catchUp["gap"]; has {
		t.Errorf("own single gap was not rewritten: %s", own["catchUp"])
	}
	var gaps []GapState
	json.Unmarshal(catchUp["gaps"], &gaps)
	if len(gaps) != 2 || gaps[0].FromCursor != "g-mine" || gaps[1].FromAt != "2026-09-02T00:00:00Z" {
		t.Errorf("own gaps are not the old range followed by the new one: %s", catchUp["gaps"])
	}
}

func readStore(t *testing.T, dir string) map[string]map[string]json.RawMessage {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var s map[string]map[string]json.RawMessage
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	for _, byLink := range s {
		for link, raw := range byLink {
			var v any
			json.Unmarshal(raw, &v)
			byLink[link], _ = json.Marshal(v)
		}
	}
	return s
}
