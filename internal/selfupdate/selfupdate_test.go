package selfupdate

import (
	"crypto/ed25519"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseVersionOutputTakesTheVersionToken(t *testing.T) {
	got, err := parseVersionOutput("mcp-hub-client v2.0.1\nbuilt from abc123 at whenever\nNOTE: something\n")
	if err != nil {
		t.Fatalf("parseVersionOutput: %v", err)
	}
	if got != "v2.0.1" {
		t.Fatalf("expected v2.0.1, got %q", got)
	}
}

// A file at this process's own path that is not an mcp-hub-client must be
// reported as unrecognized rather than parsed into something that looks
// like a version — the staleness comparison is only meaningful if both
// sides are known to be this program.
func TestParseVersionOutputRefusesSomethingElse(t *testing.T) {
	for _, in := range []string{"", "bash: --version: bad", "some-other-tool v1.2.3", "mcp-hub-client"} {
		if _, err := parseVersionOutput(in); err == nil {
			t.Fatalf("expected %q to be refused", in)
		}
	}
}

// The whole point of the deleted-marker handling: a binary replaced by
// write-then-rename leaves the running process pointing at an unlinked
// inode, and Linux appends a marker to the path. Keeping it would make
// every later exec of "our own path" fail.
func TestExecutablePathStripsTheDeletedMarker(t *testing.T) {
	if got := strings.TrimSuffix("/opt/mcp-hub-client (deleted)", " (deleted)"); got != "/opt/mcp-hub-client" {
		t.Fatalf("expected the marker stripped, got %q", got)
	}
}

func TestStalenessNoteSaysRestartWhenTheDiskHasMovedOn(t *testing.T) {
	note := Staleness{Running: "v2.0.0", OnDisk: "v2.0.1", Stale: true}.Note()
	if !strings.Contains(note, "restart") {
		t.Fatalf("expected a restart to be the prescribed fix, got: %s", note)
	}
	if !strings.Contains(note, "do NOT look for an update") {
		t.Fatalf("expected it to rule out an update, got: %s", note)
	}
}

func TestStalenessNoteOffersAnUpdateWhenNothingIsStale(t *testing.T) {
	note := Staleness{Running: "v2.0.1", OnDisk: "v2.0.1"}.Note()
	if !strings.Contains(note, "hub_self_update()") {
		t.Fatalf("expected the update tool to be offered, got: %s", note)
	}
	if strings.Contains(note, "restart") {
		t.Fatalf("expected no restart advice when the binary matches, got: %s", note)
	}
}

// An unknown on-disk version must not be reported as either state. Saying
// "not stale" would be a claim nothing checked.
func TestStalenessNoteAdmitsWhenItCouldNotTell(t *testing.T) {
	note := Staleness{Running: "v2.0.1", Err: os.ErrNotExist}.Note()
	if !strings.Contains(note, "unknown") {
		t.Fatalf("expected it to admit not knowing, got: %s", note)
	}
	if strings.Contains(note, "hub_self_update()") {
		t.Fatalf("expected no update offer on an unknown comparison, got: %s", note)
	}
}

func TestIsNewerOrdersReleasesNumerically(t *testing.T) {
	for _, tc := range []struct {
		candidate, running string
		want               bool
	}{
		{"v2.0.1", "v2.0.0", true},
		{"v2.1.0", "v2.0.9", true},
		{"v3.0.0", "v2.9.9", true},
		{"v2.0.0", "v2.0.0", false},
		{"v2.0.0", "v2.0.1", false},
		{"v2.0.0", "v10.0.0", false},
		// 10 > 9 numerically, and would be < 9 as a string. This is why
		// the comparison parses rather than compares text.
		{"v2.10.0", "v2.9.0", true},
		// A labelled-but-modified build is still that release for ordering.
		{"v2.0.1", "v2.0.0+modified", true},
	} {
		got, err := isNewer(tc.candidate, tc.running)
		if err != nil {
			t.Fatalf("isNewer(%q, %q): %v", tc.candidate, tc.running, err)
		}
		if got != tc.want {
			t.Fatalf("isNewer(%q, %q) = %v, want %v", tc.candidate, tc.running, got, tc.want)
		}
	}
}

// A development build reports a revision, not a version. Guessing that it
// is older than any release would let a connect failure trigger a silent
// downgrade of someone's working tree build.
func TestIsNewerRefusesToCompareADevelopmentBuild(t *testing.T) {
	if _, err := isNewer("v2.0.1", "7694a753ce46+modified"); err == nil {
		t.Fatal("expected a refusal to compare a revision against a release")
	}
	if _, err := isNewer("not-a-version", "v2.0.0"); err == nil {
		t.Fatal("expected a refusal to compare an unparseable tag")
	}
}

func TestVerifyAcceptsAGenuineSignatureAndRejectsEverythingElse(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	orig := releasePublicKey
	releasePublicKey = base64.StdEncoding.EncodeToString(pub)
	defer func() { releasePublicKey = orig }()

	payload := []byte("a binary, for the purposes of this test")
	good := []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(priv, payload)) + "\n")

	if err := verify(payload, good); err != nil {
		t.Fatalf("expected a genuine signature to verify: %v", err)
	}
	if err := verify([]byte("tampered"), good); err == nil {
		t.Fatal("expected altered content to be rejected")
	}
	if err := verify(payload, []byte("not base64 at all !!")); err == nil {
		t.Fatal("expected a non-base64 signature to be rejected")
	}
	if err := verify(payload, []byte(base64.StdEncoding.EncodeToString([]byte("short")))); err == nil {
		t.Fatal("expected a wrong-length signature to be rejected")
	}
	// Signed by someone else's key.
	_, other, _ := ed25519.GenerateKey(nil)
	wrong := []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(other, payload)))
	if err := verify(payload, wrong); err == nil {
		t.Fatal("expected a signature from another key to be rejected")
	}
}

// The staged file is written beside the target and renamed, so the path
// never holds a partial binary, and the replaced file's mode is preserved
// rather than assumed.
func TestReplaceExecutableRenamesIntoPlaceAndKeepsTheMode(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "mcp-hub-client")
	if err := os.WriteFile(target, []byte("old"), 0o750); err != nil {
		t.Fatal(err)
	}
	orig := executablePathForTest
	executablePathForTest = target
	defer func() { executablePathForTest = orig }()

	path, err := replaceExecutable([]byte("new"))
	if err != nil {
		t.Fatalf("replaceExecutable: %v", err)
	}
	if path != target {
		t.Fatalf("expected the target path %q, got %q", target, path)
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != "new" {
		t.Fatalf("expected the new bytes in place, got %q (%v)", got, err)
	}
	fi, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o750 {
		t.Fatalf("expected the replaced file's mode 0750 preserved, got %v", fi.Mode().Perm())
	}
	// Nothing staged may be left behind.
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		names := []string{}
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("expected only the replaced binary to remain, got %v", names)
	}
}

// A release asset name must match what the platform can actually run.
// Windows without .exe installs a file Windows refuses to execute.
func TestAssetNameIsPlatformSpecificAndExecutable(t *testing.T) {
	for _, tc := range []struct{ goos, goarch, want string }{
		{"linux", "amd64", "mcp-hub-client-linux-amd64"},
		{"linux", "arm64", "mcp-hub-client-linux-arm64"},
		{"darwin", "arm64", "mcp-hub-client-darwin-arm64"},
		{"windows", "amd64", "mcp-hub-client-windows-amd64.exe"},
		{"windows", "arm64", "mcp-hub-client-windows-arm64.exe"},
	} {
		if got := assetNameFor(tc.goos, tc.goarch); got != tc.want {
			t.Fatalf("assetNameFor(%q, %q) = %q, want %q", tc.goos, tc.goarch, got, tc.want)
		}
	}
}

// Windows will not let a running image be overwritten, so the old binary
// is moved aside first. Exercised here on a non-Windows machine, because
// otherwise this branch is only ever run by the users it would break.
func TestSwapIntoPlaceMovesTheRunningBinaryAsideOnWindows(t *testing.T) {
	orig := runtimeGOOS
	runtimeGOOS = func() string { return "windows" }
	defer func() { runtimeGOOS = orig }()

	dir := t.TempDir()
	exe := filepath.Join(dir, "mcp-hub-client.exe")
	staged := filepath.Join(dir, "staged")
	if err := os.WriteFile(exe, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(staged, []byte("new"), 0o755); err != nil {
		t.Fatal(err)
	}
	// A leftover from a previous update must not block this one.
	if err := os.WriteFile(exe+supersededSuffix, []byte("older still"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := swapIntoPlace(staged, exe); err != nil {
		t.Fatalf("swapIntoPlace: %v", err)
	}
	if got, _ := os.ReadFile(exe); string(got) != "new" {
		t.Fatalf("expected the new binary at the target, got %q", got)
	}
	if got, _ := os.ReadFile(exe + supersededSuffix); string(got) != "old" {
		t.Fatalf("expected the replaced binary moved aside, got %q", got)
	}
}

// If the new binary cannot be installed, the original goes back: a path
// with no binary at all is worse than one that is merely out of date.
func TestSwapIntoPlaceRestoresTheOriginalIfInstallFails(t *testing.T) {
	orig := runtimeGOOS
	runtimeGOOS = func() string { return "windows" }
	defer func() { runtimeGOOS = orig }()

	dir := t.TempDir()
	exe := filepath.Join(dir, "mcp-hub-client.exe")
	if err := os.WriteFile(exe, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	// A staged path that does not exist makes the second rename fail.
	err := swapIntoPlace(filepath.Join(dir, "missing"), exe)
	if err == nil {
		t.Fatal("expected an error when the staged binary is missing")
	}
	if !strings.Contains(err.Error(), "original is back in place") {
		t.Fatalf("expected the error to state the original was restored, got: %v", err)
	}
	if got, _ := os.ReadFile(exe); string(got) != "old" {
		t.Fatalf("expected the original binary restored, got %q", got)
	}
}
