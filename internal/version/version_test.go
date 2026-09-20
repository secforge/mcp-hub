package version

import (
	"os"
	"strings"
	"testing"
	"time"
)

// A development build numbers itself from the last release plus when the
// BINARY was written: 2.4.1.20260916170302. Per-build rather than
// per-commit, because two builds from one commit are two binaries — and a
// staleness check comparing identical strings across a genuine rebuild is
// what prompted this.
func TestDevelopmentBuildsAreNumberedAndUnique(t *testing.T) {
	if IsRelease() {
		t.Skip("this build carries a release tag")
	}
	got := Short()
	prefix := strings.TrimPrefix(LastRelease, "v") + "."
	if !strings.HasPrefix(got, prefix) {
		t.Fatalf("expected a dev version starting %q, got %q", prefix, got)
	}
	stamp := strings.TrimPrefix(got, prefix)
	if i := strings.IndexAny(stamp, "+"); i >= 0 {
		stamp = stamp[:i]
	}
	if len(stamp) != len("20060102150405") {
		t.Fatalf("expected a yyyymmddhhmmss stamp, got %q from %q", stamp, got)
	}
	if _, err := time.Parse("20060102150405", stamp); err != nil {
		t.Fatalf("stamp %q is not a timestamp: %v", stamp, err)
	}
}

// Four parts, and self-update compares them: a dev build sorts above the
// release it was built from and below the next one. Stated here so the
// coupling between the two packages is visible from both sides.
func TestADevVersionHasFourParts(t *testing.T) {
	if IsRelease() {
		t.Skip("this build carries a release tag")
	}
	if n := strings.Count(strings.SplitN(Short(), "+", 2)[0], "."); n != 3 {
		t.Fatalf("expected four dot-separated parts in a dev version, got %q", Short())
	}
}

// TestARunningBuildKeepsItsOwnVersion covers an external reviewer's
// finding of 2026-09-20: Short() stat'd the executable on every call, so
// touching that file changed what the running process said it was, and
// replacing the installed binary made an old process report the
// replacement's timestamp.
//
// It matters more than it looks: this value is sent on every handshake
// and compared against a server's verified release, so a version that
// moves underneath a process makes both of those describe something
// other than the code that is running.
func TestARunningBuildKeepsItsOwnVersion(t *testing.T) {
	if IsRelease() {
		t.Skip("a release build takes its version from the injected tag, not from the file")
	}
	first := Short()
	if first == "" {
		t.Fatal("Short() must never be empty")
	}

	// Move the executable's timestamp an hour into the past. Nothing
	// about the running code changed.
	exe, err := os.Executable()
	if err != nil {
		t.Skipf("cannot find this test binary: %v", err)
	}
	fi, err := os.Stat(exe)
	if err != nil {
		t.Skipf("cannot stat this test binary: %v", err)
	}
	shifted := fi.ModTime().Add(-time.Hour)
	if err := os.Chtimes(exe, shifted, shifted); err != nil {
		t.Skipf("cannot change this test binary's timestamp: %v", err)
	}
	t.Cleanup(func() { _ = os.Chtimes(exe, fi.ModTime(), fi.ModTime()) })

	if again := Short(); again != first {
		t.Fatalf("the running build changed its own version from %q to %q because a file on "+
			"disk was touched", first, again)
	}
}
