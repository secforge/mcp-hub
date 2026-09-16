package version

import (
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
