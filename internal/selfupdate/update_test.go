package selfupdate

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeReleases stands in for GitHub so the path that downloads and
// installs an executable can be exercised without the live internet — the
// last code path that should only ever be tested by running it for real.
type fakeReleases struct {
	t       *testing.T
	tag     string
	files   map[string][]byte // asset name -> content
	srv     *httptest.Server
	fetched []string
}

func newFakeReleases(t *testing.T, tag string, files map[string][]byte) *fakeReleases {
	f := &fakeReleases{t: t, tag: tag, files: files}
	mux := http.NewServeMux()
	mux.HandleFunc("/assets/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/assets/")
		body, ok := f.files[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		f.fetched = append(f.fetched, name)
		w.Write(body)
	})
	mux.HandleFunc("/latest", func(w http.ResponseWriter, r *http.Request) {
		type asset struct {
			Name string `json:"name"`
			URL  string `json:"browser_download_url"`
		}
		out := struct {
			TagName string  `json:"tag_name"`
			Assets  []asset `json:"assets"`
		}{TagName: f.tag}
		for name := range f.files {
			out.Assets = append(out.Assets, asset{Name: name, URL: f.srv.URL + "/assets/" + name})
		}
		json.NewEncoder(w).Encode(out)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)

	orig := releasesAPI
	releasesAPI = f.srv.URL + "/latest"
	t.Cleanup(func() { releasesAPI = orig })
	return f
}

// withKey installs a throwaway signing key for the duration of a test and
// returns a signer for it.
func withKey(t *testing.T) func([]byte) []byte {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	orig := releasePublicKey
	releasePublicKey = base64.StdEncoding.EncodeToString(pub)
	t.Cleanup(func() { releasePublicKey = orig })
	return func(b []byte) []byte {
		return []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(priv, b)) + "\n")
	}
}

// stagedExecutable points the updater at a scratch file standing in for
// this process's own binary.
func stagedExecutable(t *testing.T, content string) string {
	dir := t.TempDir()
	exe := filepath.Join(dir, AssetName())
	if err := os.WriteFile(exe, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	orig := executablePathForTest
	executablePathForTest = exe
	t.Cleanup(func() { executablePathForTest = orig })
	return exe
}

// releaseFiles builds the asset set a real release publishes: the
// binary, its own detached signature (still published, for anyone
// verifying a download by hand), and the signed manifest this client
// actually reads.
func releaseFiles(t *testing.T, sign func([]byte) []byte, tag string, binary []byte) map[string][]byte {
	t.Helper()
	m := Manifest{
		Schema:  ManifestSchema,
		Version: tag,
		Commit:  "0123456789abcdef0123456789abcdef01234567",
		Binaries: []ManifestBinary{
			{Filename: AssetName(), SHA256: Digest(binary)},
		},
	}
	raw, err := MarshalManifest(m)
	if err != nil {
		t.Fatalf("marshalling the manifest: %v", err)
	}
	return map[string][]byte{
		AssetName():                   binary,
		AssetName() + signatureSuffix: sign(binary),
		ManifestName:                  raw,
		ManifestSigName:               sign(raw),
	}
}

func TestApplyInstallsAVerifiedNewerRelease(t *testing.T) {
	sign := withKey(t)
	exe := stagedExecutable(t, "the old binary")
	newBinary := []byte("the new binary")
	f := newFakeReleases(t, "v9.9.9", releaseFiles(t, sign, "v9.9.9", newBinary))

	res, err := Apply(context.Background(), "v2.0.1")
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !res.Replaced || res.Latest != "v9.9.9" || res.Path != exe {
		t.Fatalf("expected the binary replaced from v9.9.9 at %s, got %+v", exe, res)
	}
	if got, _ := os.ReadFile(exe); string(got) != string(newBinary) {
		t.Fatalf("expected the downloaded binary in place, got %q", got)
	}
	// The manifest, its signature, and the binary. NOT the binary's own
	// .sig: that is still published for anyone verifying by hand, and
	// this client verifies through the manifest instead.
	if len(f.fetched) != 3 {
		t.Fatalf("expected the manifest, its signature and the asset to be fetched, got %v", f.fetched)
	}
	for _, name := range f.fetched {
		if name == AssetName()+signatureSuffix {
			t.Fatalf("the per-binary signature should not be downloaded any more, got %v", f.fetched)
		}
	}
	if res.Commit == "" {
		t.Fatal("expected the signed manifest's commit to be reported")
	}
}

// The check that matters most: a binary whose signature does not verify
// must never reach the executable path, not even briefly.
func TestApplyRefusesAnUnverifiableBinaryAndLeavesTheOriginal(t *testing.T) {
	sign := withKey(t)
	exe := stagedExecutable(t, "the old binary")
	_, wrongKey, _ := ed25519.GenerateKey(nil)
	newBinary := []byte("a binary from somewhere else")
	files := releaseFiles(t, sign, "v9.9.9", newBinary)
	// The manifest is signed by a key this build does not trust, which
	// is the whole claim: without a verified manifest nothing says what
	// this release is, so nothing is installed.
	forged := []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(wrongKey, files[ManifestName])))
	files[ManifestSigName] = forged
	newFakeReleases(t, "v9.9.9", files)

	res, err := Apply(context.Background(), "v2.0.1")
	if err == nil {
		t.Fatal("expected a manifest signed by another key to be refused")
	}
	if !strings.Contains(err.Error(), "failed signature verification") {
		t.Fatalf("expected the error to name the failed verification, got: %v", err)
	}
	if res.Replaced {
		t.Fatal("expected Replaced=false on a verification failure")
	}
	if got, _ := os.ReadFile(exe); string(got) != "the old binary" {
		t.Fatalf("expected the original binary untouched, got %q", got)
	}
	// And nothing half-written left lying beside it.
	entries, _ := os.ReadDir(filepath.Dir(exe))
	if len(entries) != 1 {
		t.Fatalf("expected only the original to remain, got %d entries", len(entries))
	}
}

// A release with no signed manifest is refused rather than installed on
// the strength of its tag — the state every release published before the
// manifest existed is in, v3.1.3 included.
func TestApplyRefusesAReleaseWithNoManifest(t *testing.T) {
	sign := withKey(t)
	exe := stagedExecutable(t, "the old binary")
	body := []byte("signed, but unaccounted for")
	newFakeReleases(t, "v9.9.9", map[string][]byte{
		AssetName():                   body,
		AssetName() + signatureSuffix: sign(body),
	})

	if _, err := Apply(context.Background(), "v2.0.1"); err == nil {
		t.Fatal("expected a release with no signed manifest to be refused")
	} else if !strings.Contains(err.Error(), ManifestName) {
		t.Fatalf("expected the reason to name the missing manifest, got: %v", err)
	}
	if got, _ := os.ReadFile(exe); string(got) != "the old binary" {
		t.Fatalf("expected the original untouched, got %q", got)
	}
}

func TestApplyDoesNothingWhenAlreadyCurrent(t *testing.T) {
	sign := withKey(t)
	stagedExecutable(t, "the old binary")
	body := []byte("same release")
	newFakeReleases(t, "v2.0.1", releaseFiles(t, sign, "v2.0.1", body))

	res, err := Apply(context.Background(), "v2.0.1")
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.Replaced {
		t.Fatal("expected nothing installed when the running version is the latest")
	}
}

// GitHub's "latest" is the most recently PUBLISHED release, which is not
// necessarily the highest version — a patch cut on an old branch would
// otherwise be installed as an "update".
func TestApplyRefusesToDowngrade(t *testing.T) {
	sign := withKey(t)
	exe := stagedExecutable(t, "the old binary")
	body := []byte("an older release")
	newFakeReleases(t, "v1.9.0", releaseFiles(t, sign, "v1.9.0", body))

	res, err := Apply(context.Background(), "v2.0.1")
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.Replaced {
		t.Fatal("expected no downgrade to an older published release")
	}
	if got, _ := os.ReadFile(exe); string(got) != "the old binary" {
		t.Fatalf("expected the original untouched, got %q", got)
	}
}

func TestApplyReportsAReleaseWithNothingForThisPlatform(t *testing.T) {
	sign := withKey(t)
	stagedExecutable(t, "the old binary")
	other := []byte("a binary for some other platform")
	files := releaseFiles(t, sign, "v9.9.9", other)
	// Published under a name this platform will never ask for, and the
	// manifest accounts for that name rather than this one.
	delete(files, AssetName())
	delete(files, AssetName()+signatureSuffix)
	files["mcp-hub-client-plan9-mips"] = other
	files["mcp-hub-client-plan9-mips"+signatureSuffix] = sign(other)
	m := Manifest{
		Schema: ManifestSchema, Version: "v9.9.9",
		Commit:   "0123456789abcdef0123456789abcdef01234567",
		Binaries: []ManifestBinary{{Filename: "mcp-hub-client-plan9-mips", SHA256: Digest(other)}},
	}
	raw, err := MarshalManifest(m)
	if err != nil {
		t.Fatalf("marshalling the manifest: %v", err)
	}
	files[ManifestName] = raw
	files[ManifestSigName] = sign(raw)
	newFakeReleases(t, "v9.9.9", files)

	_, err = Apply(context.Background(), "v2.0.1")
	if err == nil {
		t.Fatal("expected a release with no asset for this platform to be reported")
	}
	if !strings.Contains(err.Error(), AssetName()) {
		t.Fatalf("expected the error to name the asset it looked for, got: %v", err)
	}
}

// A build with no embedded key cannot verify anything, so it must refuse
// rather than install on trust.
func TestApplyRefusesWhenTheBuildHasNoPublicKey(t *testing.T) {
	orig := releasePublicKey
	releasePublicKey = ""
	defer func() { releasePublicKey = orig }()

	if _, err := Apply(context.Background(), "v2.0.1"); err == nil {
		t.Fatal("expected a keyless build to refuse to update")
	} else if !strings.Contains(err.Error(), "no release public key") {
		t.Fatalf("expected the reason to be the missing key, got: %v", err)
	}
}

// A truncated download is caught by the limit rather than by the
// signature, so the reason reported is the real one.
func TestDownloadRefusesSomethingLargerThanTheLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, strings.Repeat("x", 100))
	}))
	defer srv.Close()

	if _, err := download(context.Background(), srv.URL, 10); err == nil {
		t.Fatal("expected a body over the limit to be refused")
	} else if !strings.Contains(err.Error(), "larger than") {
		t.Fatalf("expected the size to be the stated reason, got: %v", err)
	}
}

// A pre-release is behind the release of the same numbers — semver's own
// rule, and the one case where dropping the suffix inverts the answer.
// Without it a running v1.2.3-rc1 compared EQUAL to the published v1.2.3,
// so hub_self_update told anyone on an rc there was no update, forever.
func TestAPreReleaseIsBehindItsOwnRelease(t *testing.T) {
	for _, tc := range []struct {
		candidate, running string
		want               bool
	}{
		{"v1.2.3", "v1.2.3-rc1", true},
		{"v1.2.3", "v1.2.3", false},
		{"v1.2.3-rc2", "v1.2.3-rc1", false}, // same numbers, both pre: not ordered here
		{"v1.2.3-rc1", "v1.2.3", false},     // never offer a downgrade to an rc
		{"v1.2.4", "v1.2.3-rc1", true},
		{"v1.2.3+modified", "v1.2.3", false}, // build metadata is not a version change
	} {
		got, err := isNewer(tc.candidate, tc.running)
		if err != nil {
			t.Fatalf("isNewer(%q, %q): %v", tc.candidate, tc.running, err)
		}
		if got != tc.want {
			t.Errorf("isNewer(%q, %q) = %v, want %v", tc.candidate, tc.running, got, tc.want)
		}
	}
}

// The ordering a development build has to obey, stated by the owner:
//
//	1.4.1 < 1.4.1.20260916170302 < 1.4.1.20260916170303 < 1.4.2
//
// A dev build stays ahead of the release it came from and behind the next
// one, so it is offered real updates without a published build ever
// replacing something newer.
func TestADevBuildSortsAboveItsReleaseAndBelowTheNext(t *testing.T) {
	for _, tc := range []struct {
		candidate, running string
		want               bool
	}{
		{"v1.4.1.20260916170302", "v1.4.1", true},  // dev is newer than its release
		{"v1.4.1", "v1.4.1.20260916170302", false}, // and that release is not newer than it
		{"v1.4.1.20260916170303", "v1.4.1.20260916170302", true},
		{"v1.4.1.20260916170302", "v1.4.1.20260916170303", false},
		{"v1.4.2", "v1.4.1.20260916170302", true},  // the next release still wins
		{"v1.4.1.20260916170302", "v1.4.2", false}, // and is not undercut by a dev build
	} {
		got, err := isNewer(tc.candidate, tc.running)
		if err != nil {
			t.Fatalf("isNewer(%q, %q): %v", tc.candidate, tc.running, err)
		}
		if got != tc.want {
			t.Errorf("isNewer(%q, %q) = %v, want %v", tc.candidate, tc.running, got, tc.want)
		}
	}
}

// Three parts and four are both accepted — every released build has
// exactly three — and the missing fourth is -1, so a release sorts
// strictly below any dev build of it rather than tying with .0.
func TestAMissingFourthPartIsAcceptedAndSortsLowest(t *testing.T) {
	three, _, err := parseSemver("v1.4.1")
	if err != nil {
		t.Fatalf("three-part version rejected: %v", err)
	}
	four, _, err := parseSemver("v1.4.1.0")
	if err != nil {
		t.Fatalf("four-part version rejected: %v", err)
	}
	if three[3] != -1 {
		t.Fatalf("expected an absent fourth part to be -1, got %v", three)
	}
	if !(three[3] < four[3]) {
		t.Fatalf("expected 1.4.1 to sort below 1.4.1.0, got %v and %v", three, four)
	}
}

// Numerically, never as text — a 14-digit timestamp against a one-digit
// patch orders backwards as a string.
func TestPartsCompareNumericallyNotAsStrings(t *testing.T) {
	got, err := isNewer("v1.4.2", "v1.4.1.20260916170302")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Fatal("expected 1.4.2 to be newer than 1.4.1.20260916170302")
	}
}

// An absent fourth part is -1 rather than 0, so nothing can tie. Zero
// would make v1.4.1 and v1.4.1.0 compare EQUAL, and equal is the one
// answer that stops an update while saying nothing: isNewer returns false
// and Apply reports nothing available.
func TestAReleaseSortsBelowEvenADotZeroDevBuild(t *testing.T) {
	got, err := isNewer("v1.4.1.0", "v1.4.1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Fatal("expected 1.4.1.0 to be newer than 1.4.1 — a tie hides the update")
	}
	back, err := isNewer("v1.4.1", "v1.4.1.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if back {
		t.Fatal("expected 1.4.1 NOT to be newer than 1.4.1.0")
	}
}

// TestApplyRefusesAReplayedOldReleaseWearingANewTag is the whole point
// of the manifest, and the defect docs/known-issues.md recorded from
// 2026-09-15 until now.
//
// Every binary was signed, so the bytes were accounted for; the VERSION
// was not, because it came from the git tag, which is GitHub metadata
// and signed by nothing. Whoever could shape that response could serve
// tag v99.0.0 beside a genuine, genuinely signed old binary and its
// genuine signature: every signature check passed, the comparison passed
// because the tag said 99, and a known-buggy build installed over a good
// one. TLS to api.github.com was the only thing standing in the way,
// which is a real control and a different one from the ordering the code
// claimed to rely on.
func TestApplyRefusesAReplayedOldReleaseWearingANewTag(t *testing.T) {
	sign := withKey(t)
	exe := stagedExecutable(t, "the good binary")

	// A genuine old release: really signed, really published, really
	// v1.0.0 — served under a tag claiming to be far newer.
	old := []byte("a known-buggy old binary")
	files := releaseFiles(t, sign, "v1.0.0", old)
	newFakeReleases(t, "v99.0.0", files)

	res, err := Apply(context.Background(), "v2.0.1")
	if err == nil {
		t.Fatal("expected a tag and a signed version that disagree to stop the install")
	}
	if !strings.Contains(err.Error(), "disagree") {
		t.Fatalf("expected the error to name the disagreement, got: %v", err)
	}
	if res.Replaced {
		t.Fatal("expected nothing installed")
	}
	if got, _ := os.ReadFile(exe); string(got) != "the good binary" {
		t.Fatalf("expected the running binary untouched, got %q", got)
	}
}

// And the same attack with the tag left honest: an old release replayed
// as itself is refused by the ordering, now computed from the SIGNED
// version rather than from the tag.
func TestApplyRefusesAnOlderSignedVersion(t *testing.T) {
	sign := withKey(t)
	exe := stagedExecutable(t, "the good binary")
	old := []byte("a known-buggy old binary")
	newFakeReleases(t, "v1.0.0", releaseFiles(t, sign, "v1.0.0", old))

	res, err := Apply(context.Background(), "v2.0.1")
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.Replaced {
		t.Fatal("expected no downgrade to an older signed release")
	}
	if got, _ := os.ReadFile(exe); string(got) != "the good binary" {
		t.Fatalf("expected the running binary untouched, got %q", got)
	}
}

// A manifest that verifies but does not account for the file being
// installed leaves the binary unchecked. Swapping the bytes behind an
// accounted-for name is the same hole from the other side.
func TestApplyRefusesABinaryTheManifestDoesNotMatch(t *testing.T) {
	sign := withKey(t)
	exe := stagedExecutable(t, "the good binary")
	announced := []byte("the binary that was released")
	files := releaseFiles(t, sign, "v9.9.9", announced)
	// Manifest untouched and validly signed; the served bytes are not
	// the ones it is signed for.
	files[AssetName()] = []byte("something else entirely")
	newFakeReleases(t, "v9.9.9", files)

	res, err := Apply(context.Background(), "v2.0.1")
	if err == nil {
		t.Fatal("expected bytes that do not match the signed digest to be refused")
	}
	if !strings.Contains(err.Error(), "NOT installed") {
		t.Fatalf("expected the error to say plainly that nothing was installed, got: %v", err)
	}
	if res.Replaced {
		t.Fatal("expected Replaced=false")
	}
	if got, _ := os.ReadFile(exe); string(got) != "the good binary" {
		t.Fatalf("expected the running binary untouched, got %q", got)
	}
}

// A manifest with no entry for this platform's binary is not a reason to
// install it unchecked.
func TestApplyRefusesABinaryTheManifestDoesNotMention(t *testing.T) {
	sign := withKey(t)
	stagedExecutable(t, "the good binary")
	body := []byte("unaccounted for")
	files := releaseFiles(t, sign, "v9.9.9", body)
	m := Manifest{
		Schema: ManifestSchema, Version: "v9.9.9",
		Commit:   "0123456789abcdef0123456789abcdef01234567",
		Binaries: []ManifestBinary{{Filename: "mcp-hub-client-plan9-mips", SHA256: Digest(body)}},
	}
	raw, err := MarshalManifest(m)
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	files[ManifestName] = raw
	files[ManifestSigName] = sign(raw)
	newFakeReleases(t, "v9.9.9", files)

	if _, err := Apply(context.Background(), "v2.0.1"); err == nil {
		t.Fatal("expected a binary the manifest does not account for to be refused")
	} else if !strings.Contains(err.Error(), "does not account for") {
		t.Fatalf("expected the error to say the manifest does not account for it, got: %v", err)
	}
}
