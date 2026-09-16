// Package citations holds one test: that a test named in a comment exists.
//
// It is here because a comment in format.go stated a real risk, named the
// test that mitigated it, and the test did not exist — written, deleted
// when it would not compile, and the citation left behind. That is worse
// than saying nothing: a reader who checks the reasoning finds it sound
// and stops looking, so the citation was doing the work of a guard while
// being only a sentence.
//
// Nothing else in a Go toolchain checks a cross-reference. A rename breaks
// every caller and leaves every mention of the old name in prose intact.
package citations

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	// A test identifier as it appears in prose or in a definition.
	mentioned = regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]{3,}\b`)
	defined   = regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]+)\(`)
	// A comment that attributes the test to another project. Kept to an
	// explicit list rather than a vague "upstream", so adding an exemption
	// is a deliberate act and a reader can see whose guard it is.
	external = regexp.MustCompile(`harness-transport|chat-relay|the library's own`)
)

type citation struct{ name, file string }

// scan returns every test cited in a comment under root that is not
// declared anywhere under it.
func scan(root string) (dangling []citation, citedCount int, err error) {
	have := map[string]bool{}
	var cited []citation

	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		if strings.Contains(path, "/vendor/") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(src)
		for _, m := range defined.FindAllStringSubmatch(text, -1) {
			have[m[1]] = true
		}
		// Only comments: a mention inside code is a real reference the
		// compiler already checks. Examined as contiguous BLOCKS rather
		// than single lines, because an attribution and the name it
		// qualifies routinely sit on different lines of the same
		// paragraph — checking line by line would demand that prose wrap
		// to suit the checker.
		for _, block := range commentBlocks(text) {
			// A citation to a test in ANOTHER project is legitimate and
			// cannot be checked here — but it has to say so, because
			// "pinned by TestX" reads identically whether TestX is in this
			// tree or somebody else's, and the difference decides when a
			// break reaches you: your suite immediately, or theirs at a
			// dependency bump. Naming the owner is the whole exemption.
			if external.MatchString(block) {
				continue
			}
			for _, name := range mentioned.FindAllString(block, -1) {
				rel, _ := filepath.Rel(root, path)
				cited = append(cited, citation{name, rel})
			}
		}
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	for _, c := range cited {
		if !have[c.name] {
			dangling = append(dangling, c)
		}
	}
	return dangling, len(cited), nil
}

func TestEveryTestNamedInACommentExists(t *testing.T) {
	dangling, cited, err := scan(repoRoot(t))
	if err != nil {
		t.Fatalf("walking the tree: %v", err)
	}
	if cited == 0 {
		t.Fatal("found no test citations at all — the scan is not looking at the right files")
	}
	for _, c := range dangling {
		t.Errorf("%s cites %s, which is defined nowhere. Either write it or stop claiming it: "+
			"a comment naming an absent guard reads as a guarantee somebody is holding.",
			c.file, c.name)
	}
}

// The check has to be able to FAIL, or it is a citation of itself —
// exactly the thing it exists to catch, one level up. Names are assembled
// at runtime rather than written as literals, because a literal in this
// file would itself be a citation and the scanner would be right to
// report it.
func TestTheScannerActuallyDetectsADanglingCitation(t *testing.T) {
	dir := t.TempDir()
	missing := "Test" + "ThisOneWasNeverWritten"
	real := "Test" + "ThisOneExists"

	src := "package fixture\n\n// " + missing + " covers the case below.\n" +
		"// " + real + " covers another.\nfunc f() {}\n"
	if err := os.WriteFile(filepath.Join(dir, "fixture.go"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	test := "package fixture\n\nfunc " + real + "() {}\n"
	if err := os.WriteFile(filepath.Join(dir, "fixture_test.go"), []byte(test), 0o600); err != nil {
		t.Fatal(err)
	}

	dangling, cited, err := scan(dir)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if cited != 2 {
		t.Fatalf("saw %d citations, want 2 — a commented name must count as a citation", cited)
	}
	if len(dangling) != 1 || dangling[0].name != missing {
		t.Fatalf("dangling = %+v, want exactly %s — a name declared only in a COMMENT must not "+
			"count as defined", dangling, missing)
	}
}

// commentBlocks returns each run of consecutive comment lines as one
// string.
func commentBlocks(text string) []string {
	var blocks []string
	var cur []string
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			cur = append(cur, trimmed)
			continue
		}
		if len(cur) > 0 {
			blocks = append(blocks, strings.Join(cur, " "))
			cur = nil
		}
	}
	if len(cur) > 0 {
		blocks = append(blocks, strings.Join(cur, " "))
	}
	return blocks
}

// repoRoot walks up from the test's own directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod found above the test directory")
		}
		dir = parent
	}
}
