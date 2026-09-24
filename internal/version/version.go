// Package version answers "which build is this?" — and answers it
// honestly, which is the harder half.
//
// A hand-maintained constant drifts the moment someone tags a release and
// forgets to bump it, and a version derived from local git is no better:
// releases cut with `gh release create` tag the remote, so a local
// `git describe` can sit several releases behind while looking
// authoritative. Both failure modes produce a binary that states a version
// it isn't, which is worse than one that admits it doesn't know.
//
// So there are two sources here and they are not interchangeable:
//
//   - Release is injected at link time, and ONLY by a release build. It is
//     the one moment a human genuinely chooses the number.
//   - Everything else comes from debug.BuildInfo, which the toolchain fills
//     in for any build inside a repository. Nothing to remember, nothing to
//     bump, and it cannot be made to lie about the commit it came from.
//
// A build with no injected Release does not guess one. It reports the
// revision it was built from and whether the tree was modified, which is
// enough to identify it exactly and never enough to mistake it for a
// release.
package version

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
)

// Release is the release tag this binary was built as — set only by a
// release build, via
//
//	-ldflags "-X github.com/secforge/mcp-hub/internal/version.Release=v2.0.2"
//
// Empty in every other build, including a plain `go build ./...`, and
// deliberately not defaulted: an empty Release means "this is not a
// release", which is a true and useful thing to say.
var Release string

// releaseMu guards Release and modified against the ONE writer that
// exists after start-up: SetReleaseForTest. The ldflags value is written
// by the linker before any goroutine runs, so production needs no lock —
// but this package is read from the connect path now (Agent-Version, and
// the comparison against a server-verified release), and a test changing
// the version while another test's connect reads it is a genuine data
// race, reported by -race and found by an external reviewer's run,
// 2026-09-19.
var releaseMu sync.RWMutex

func release() (string, bool) {
	releaseMu.RLock()
	defer releaseMu.RUnlock()
	return Release, modified
}

// LastRelease names the most recent published release, and exists so a
// development build can say what it is a development build OF. Updated by
// scripts/release.sh when a release is cut; a stale value costs a
// misleading prefix on dev builds only, never on a release, which gets
// its number from Release above.
const LastRelease = "v3.1.13"

// devStamp is when the running binary's file was written, as
// yyyymmddhhmmss, or "" when that cannot be read.
//
// The BINARY's timestamp rather than the commit's: two dev builds from
// one commit are two different binaries, and the reason to number them
// at all is to tell those apart. vcs.time would give both the same
// string — which is exactly the case that defeated a staleness check
// this morning, where a rebuilt client and the one it replaced reported
// identical versions.
func devStamp() string { return devStampValue }

// READ ONCE, at process start, and never again. It used to stat the
// executable on every call, which made a running process's own identity
// a function of a file on disk: touching that file changed what the
// process said it was, and replacing the installed binary made an old
// process report the replacement's timestamp. A version that moves
// underneath a running process is worse than a coarse one — it is now
// sent on every handshake and compared against a server's verified
// release, so it has to describe THIS process for as long as it runs.
// Found by an external reviewer, 2026-09-20.
//
// It also defeated the check it fed. The mtime-versus-process-start
// signal exists to catch "installed but not loaded" — and while the
// installed file's mtime also decided the version the RUNNING process
// reported, replacing the binary moved both halves together and the
// staleness became invisible, which is the one case that check was
// added for. chat-relay's author made that point the same day.
//
// A release build never reaches here (Release is injected), so this
// bounds a development build only.
// Captured at PROCESS START, not at first use. First-use caching still
// depended on what the file looked like whenever something first asked —
// so a binary replaced before the first call was read as this process's
// own version — which an external reviewer noted is a narrower fix than
// it appears, since a stable wrong value looks like a measurement and a
// lazy first caller makes the window the whole session.
//
// What this does NOT remove, stated because the earlier wording here
// overclaimed it: the file can still be replaced between exec and this
// init, so the value is the file's as of start-up rather than the
// running code's. Only an embedded build stamp (injected like Release)
// makes "what is running" and "what is on disk" independent facts —
// which is what the installed-but-not-loaded check needs in order to
// compare them at all. Not done here because it changes the release
// process for a value only development builds use.
var devStampValue = readDevStamp()

func readDevStamp() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	fi, err := os.Stat(exe)
	if err != nil {
		return ""
	}
	return fi.ModTime().UTC().Format("20060102150405")
}

// info is read once — BuildInfo is fixed at link time and cannot change
// while the process runs.
var revision, buildTime string
var modified bool

func init() {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.time":
			buildTime = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
}

// SetReleaseForTest makes this build report a release version and returns
// a function restoring what it reported before. Needed because a test
// binary carries no VCS stamp and no injected tag, so it reports "unknown"
// — which is correct, and makes every code path that only runs for a
// real release untestable without a seam.
func SetReleaseForTest(release string) func() {
	releaseMu.Lock()
	prevRelease, prevModified := Release, modified
	Release, modified = release, false
	releaseMu.Unlock()
	return func() {
		releaseMu.Lock()
		Release, modified = prevRelease, prevModified
		releaseMu.Unlock()
	}
}

// Revision is the commit this binary was built from, or "" when it was
// built outside a repository (e.g. from a module cache).
func Revision() string { return revision }

// Modified reports whether the tree had uncommitted changes at build time.
// A release build must never have this set — a binary that cannot be
// reproduced from a commit is not a release, whatever it is labelled.
func Modified() bool {
	_, m := release()
	return m
}

// IsRelease reports whether this build carries an injected release tag and
// was built from a clean tree. Both halves matter: a labelled build from a
// dirty tree is a development build wearing a release's name.
func IsRelease() bool {
	r, m := release()
	return r != "" && !m
}

// Short is the version as one token, for a header value or a log line:
// the release tag when there is one, otherwise the short revision. Never
// empty — "unknown" when a build carries no VCS information at all, which
// is still more useful than a blank field.
func Short() string {
	// READ ONCE, under the lock, and used throughout: two reads could
	// straddle a change and describe neither state.
	rel, mod := release()
	switch {
	case rel != "" && !mod:
		return rel
	case rel != "":
		return rel + "+modified"
	}
	// A development build is numbered from the last release plus the
	// moment this binary was built: 2.4.1.20260916170302. It sorts after
	// that release, reads as "newer than 2.4.1 and not itself a
	// release", and is unique per build rather than per commit.
	//
	// FOUR parts, which self-update compares as a dev suffix on the
	// release this was built from:
	//
	//	1.4.1 < 1.4.1.20260916170302 < 1.4.1.20260916170303 < 1.4.2
	//
	// So a dev build is offered the next real release, and no published
	// build ever replaces something newer than itself.
	if stamp := devStamp(); stamp != "" {
		v := strings.TrimPrefix(LastRelease, "v") + "." + stamp
		if revision == "" {
			return v
		}
		if mod {
			return v + "+" + shortRevision() + "+modified"
		}
		return v + "+" + shortRevision()
	}
	switch {
	case revision != "" && mod:
		return shortRevision() + "+modified"
	case revision != "":
		return shortRevision()
	}
	return "unknown"
}

// String is the human-facing form, for --version and anything a person
// reads while working out why two builds behave differently.
func String() string {
	var b strings.Builder
	b.WriteString("mcp-hub-client ")
	b.WriteString(Short())
	if revision != "" {
		fmt.Fprintf(&b, "\nbuilt from %s", revision)
		if buildTime != "" {
			fmt.Fprintf(&b, " at %s", buildTime)
		}
	}
	rel, mod := release()
	switch {
	case rel != "" && !mod:
	case rel != "":
		b.WriteString("\nNOTE: labelled " + rel + " but built from a modified tree — not a release build")
	case revision == "":
		b.WriteString("\nNOTE: no build information; this binary cannot say which commit it came from")
	default:
		b.WriteString("\nNOTE: development build — no release tag was injected at link time")
	}
	if mod {
		b.WriteString("\nNOTE: the tree had uncommitted changes when this was built")
	}
	return b.String()
}

func shortRevision() string {
	if len(revision) > 12 {
		return revision[:12]
	}
	return revision
}
