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

func TestApplyInstallsAVerifiedNewerRelease(t *testing.T) {
	sign := withKey(t)
	exe := stagedExecutable(t, "the old binary")
	newBinary := []byte("the new binary")
	f := newFakeReleases(t, "v9.9.9", map[string][]byte{
		AssetName():                   newBinary,
		AssetName() + signatureSuffix: sign(newBinary),
	})

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
	if len(f.fetched) != 2 {
		t.Fatalf("expected the asset and its signature to be fetched, got %v", f.fetched)
	}
}

// The check that matters most: a binary whose signature does not verify
// must never reach the executable path, not even briefly.
func TestApplyRefusesAnUnverifiableBinaryAndLeavesTheOriginal(t *testing.T) {
	withKey(t)
	exe := stagedExecutable(t, "the old binary")
	_, wrongKey, _ := ed25519.GenerateKey(nil)
	newBinary := []byte("a binary from somewhere else")
	forged := []byte(base64.StdEncoding.EncodeToString(ed25519.Sign(wrongKey, newBinary)))
	newFakeReleases(t, "v9.9.9", map[string][]byte{
		AssetName():                   newBinary,
		AssetName() + signatureSuffix: forged,
	})

	res, err := Apply(context.Background(), "v2.0.1")
	if err == nil {
		t.Fatal("expected a binary signed by another key to be refused")
	}
	if !strings.Contains(err.Error(), "NOT installed") {
		t.Fatalf("expected the error to state plainly that nothing was installed, got: %v", err)
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

// A release with no signature beside the asset is refused rather than
// installed on trust — the state every release before signing existed is
// in, including the one live right now.
func TestApplyRefusesAReleaseWithNoSignature(t *testing.T) {
	withKey(t)
	exe := stagedExecutable(t, "the old binary")
	newFakeReleases(t, "v9.9.9", map[string][]byte{AssetName(): []byte("unsigned")})

	if _, err := Apply(context.Background(), "v2.0.1"); err == nil {
		t.Fatal("expected an unsigned release to be refused")
	} else if !strings.Contains(err.Error(), "cannot be verified") {
		t.Fatalf("expected the reason to be the missing signature, got: %v", err)
	}
	if got, _ := os.ReadFile(exe); string(got) != "the old binary" {
		t.Fatalf("expected the original untouched, got %q", got)
	}
}

func TestApplyDoesNothingWhenAlreadyCurrent(t *testing.T) {
	sign := withKey(t)
	stagedExecutable(t, "the old binary")
	body := []byte("same release")
	newFakeReleases(t, "v2.0.1", map[string][]byte{
		AssetName():                   body,
		AssetName() + signatureSuffix: sign(body),
	})

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
	newFakeReleases(t, "v1.9.0", map[string][]byte{
		AssetName():                   body,
		AssetName() + signatureSuffix: sign(body),
	})

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
	newFakeReleases(t, "v9.9.9", map[string][]byte{
		"mcp-hub-client-plan9-mips":                   other,
		"mcp-hub-client-plan9-mips" + signatureSuffix: sign(other),
	})

	_, err := Apply(context.Background(), "v2.0.1")
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
