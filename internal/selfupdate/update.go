package selfupdate

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// releasePublicKey verifies that a downloaded binary was signed by whoever
// holds this project's release key. It is compiled in on purpose: a key
// fetched at update time could be replaced by whatever served the binary,
// which would make the signature decorative.
//
// The consequence to know about is key rotation. A binary only trusts this
// one key, so a new key can only reach a client through a build signed by
// the old one — rotation has to ship before it is needed, never after.
//
// Empty disables updating entirely, with a message saying so. That is the
// correct behaviour for a build made before any key existed: refusing to
// update is recoverable, replacing a binary on an unverifiable signature
// is not.
var releasePublicKey = "poNqohWesbaT8bSnCehwC1ENaDL9decYbP4/djqglPU="

// releasesAPI is the only endpoint consulted. Pinned to this repository
// rather than derived from anything on disk, so nothing a caller controls
// can redirect where an executable comes from. A var only so tests can
// point it at a local server: the code path that fetches and installs a
// binary is the last one that should be exercised solely against the live
// internet.
var releasesAPI = "https://api.github.com/repos/secforge/mcp-hub/releases/latest"

// SetReleasesAPIForTest points this package at a stand-in for GitHub and
// returns a function restoring the previous endpoint. Exported for tests
// in other packages that drive the tool surface.
func SetReleasesAPIForTest(url string) func() {
	prev := releasesAPI
	releasesAPI = url
	return func() { releasesAPI = prev }
}

const (
	// signatureSuffix is what a release asset's detached signature is
	// named beside it.
	signatureSuffix = ".sig"
	// maxAssetBytes bounds a download. The client is ~11MB; this leaves
	// generous headroom while refusing to write something absurd to disk.
	maxAssetBytes = 128 << 20
)

var httpTimeout = 60 * time.Second

// AssetName is the release asset this build would replace itself with.
// Platform-specific by construction — a linux/amd64 process must never
// install a darwin/arm64 binary over itself, and the name is what enforces
// that rather than any check after the download.
func AssetName() string { return assetNameFor(runtime.GOOS, runtime.GOARCH) }

func assetNameFor(goos, goarch string) string {
	name := fmt.Sprintf("mcp-hub-client-%s-%s", goos, goarch)
	if goos == "windows" {
		// Not cosmetic: Windows will not execute a file without it, so a
		// release asset that omits it installs something unrunnable.
		name += ".exe"
	}
	return name
}

type release struct {
	TagName string `json:"tag_name"`
	Assets  []struct {
		Name string `json:"name"`
		URL  string `json:"browser_download_url"`
	} `json:"assets"`
}

// Latest fetches the newest published release.
func Latest(ctx context.Context) (release, error) {
	var r release
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, releasesAPI, nil)
	if err != nil {
		return r, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	resp, err := (&http.Client{Timeout: httpTimeout}).Do(req)
	if err != nil {
		return r, fmt.Errorf("could not reach GitHub: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return r, fmt.Errorf("GitHub answered %s asking for the latest release", resp.Status)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&r); err != nil {
		return r, fmt.Errorf("could not read GitHub's answer: %w", err)
	}
	if r.TagName == "" {
		return r, fmt.Errorf("GitHub reported a release with no tag")
	}
	return r, nil
}

// Result describes what an update attempt did, in terms a caller can
// report without having to interpret anything.
type Result struct {
	Running   string
	Latest    string
	Replaced  bool
	AssetName string
	Path      string
}

// Apply checks for a newer release and, only if one exists AND its
// signature verifies, replaces this process's own binary with it.
//
// The order of the checks is the design. Newer-than-running is enforced
// before anything is downloaded, because a signature says "we published
// this", not "this is current" — an old asset stays validly signed
// forever, so signature alone would allow a downgrade. Verification then
// happens entirely in memory, before a single byte reaches the executable
// path: a binary that fails to verify is never written anywhere it could
// be run from.
func Apply(ctx context.Context, running string) (Result, error) {
	res := Result{Running: running, AssetName: AssetName()}

	if releasePublicKey == "" {
		return res, fmt.Errorf("this build carries no release public key, so a downloaded binary " +
			"could not be verified — refusing to replace anything. Update it by hand from " +
			"https://github.com/secforge/mcp-hub/releases")
	}

	rel, err := Latest(ctx)
	if err != nil {
		return res, err
	}
	res.Latest = rel.TagName

	newer, err := isNewer(rel.TagName, running)
	if err != nil {
		return res, fmt.Errorf("cannot tell whether %s is newer than %s: %w — refusing to replace "+
			"a binary on a comparison this client does not understand", rel.TagName, running, err)
	}
	if !newer {
		return res, nil
	}

	var assetURL, sigURL string
	for _, a := range rel.Assets {
		switch a.Name {
		case res.AssetName:
			assetURL = a.URL
		case res.AssetName + signatureSuffix:
			sigURL = a.URL
		}
	}
	if assetURL == "" {
		return res, fmt.Errorf("release %s publishes no %s asset — nothing to install for this "+
			"platform", rel.TagName, res.AssetName)
	}
	if sigURL == "" {
		return res, fmt.Errorf("release %s publishes %s but no %s%s beside it, so it cannot be "+
			"verified — refusing to install it", rel.TagName, res.AssetName, res.AssetName,
			signatureSuffix)
	}

	binary, err := download(ctx, assetURL, maxAssetBytes)
	if err != nil {
		return res, err
	}
	sig, err := download(ctx, sigURL, 4096)
	if err != nil {
		return res, err
	}
	if err := verify(binary, sig); err != nil {
		return res, fmt.Errorf("%s failed signature verification (%w) — NOT installed. Either the "+
			"download was corrupted or it was not signed by this project's release key; the "+
			"binary you are running has not been touched", res.AssetName, err)
	}

	path, err := replaceExecutable(binary)
	if err != nil {
		return res, err
	}
	res.Replaced, res.Path = true, path
	return res, nil
}

func download(ctx context.Context, url string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Timeout: httpTimeout}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not download %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("downloading %s answered %s", url, resp.Status)
	}
	// LimitReader+1 so a file at exactly the limit is distinguishable from
	// one that was truncated to it — a silently short binary is the kind
	// of thing a signature check exists to catch, but saying which
	// happened is better than reporting a verification failure.
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("could not read %s: %w", url, err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("%s is larger than the %d bytes this client is willing to install", url, limit)
	}
	return body, nil
}

// VerifyWithEmbeddedKey is verify, exported so the release tooling can
// check a signature against the SAME key the client has compiled in.
// Signing with a key no shipped client trusts is otherwise a mistake
// nothing catches until an update fails on someone's machine.
func VerifyWithEmbeddedKey(payload, signature []byte) error { return verify(payload, signature) }

// verify checks a detached Ed25519 signature over the binary's bytes. The
// signature file is base64, one line, nothing else — a format small enough
// that there is no parser to get wrong.
func verify(binary, sig []byte) error {
	pub, err := base64.StdEncoding.DecodeString(strings.TrimSpace(releasePublicKey))
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("this build's embedded public key is malformed")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(sig)))
	if err != nil {
		return fmt.Errorf("the signature file is not base64")
	}
	if len(raw) != ed25519.SignatureSize {
		return fmt.Errorf("the signature is %d bytes, expected %d", len(raw), ed25519.SignatureSize)
	}
	if !ed25519.Verify(ed25519.PublicKey(pub), binary, raw) {
		return fmt.Errorf("signature does not match")
	}
	return nil
}

// replaceExecutable writes the new binary beside the current one and
// renames it into place.
//
// Written beside rather than to a temp directory so the rename is within
// one filesystem and therefore atomic: there is no moment where the path
// holds a partial file. Overwriting in place is not an option either —
// the kernel refuses to write to a running executable, and even where it
// did not, a half-written binary at that path is what the next restart
// would run.
//
// Windows needs one extra step. It allows a running image to be RENAMED
// but not replaced, so renaming the new file onto the old path fails with
// a sharing violation; the old one is moved aside first and cleaned up on
// a later run, once nothing is executing it.
func replaceExecutable(binary []byte) (string, error) {
	exe, err := ExecutablePath()
	if err != nil {
		return "", err
	}
	dir := filepath.Dir(exe)
	tmp, err := os.CreateTemp(dir, filepath.Base(exe)+".new-*")
	if err != nil {
		return "", fmt.Errorf("could not stage the new binary in %s: %w", dir, err)
	}
	staged := tmp.Name()
	defer os.Remove(staged) // no-op once the rename below succeeds

	if _, err := tmp.Write(binary); err != nil {
		tmp.Close()
		return "", fmt.Errorf("could not write the new binary: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("could not finish writing the new binary: %w", err)
	}
	// Match the mode of what is being replaced rather than assuming 0755,
	// so an install that was deliberately group- or owner-restricted stays
	// that way.
	mode := os.FileMode(0o755)
	if fi, err := os.Stat(exe); err == nil {
		mode = fi.Mode().Perm()
	}
	if err := os.Chmod(staged, mode); err != nil {
		return "", fmt.Errorf("could not make the new binary executable: %w", err)
	}
	if err := swapIntoPlace(staged, exe); err != nil {
		return "", err
	}
	return exe, nil
}

// supersededSuffix marks a binary moved aside because the platform would
// not let it be replaced while running.
const supersededSuffix = ".superseded"

// swapIntoPlace puts staged at exe. On every platform but Windows that is
// a single rename. On Windows the running image is moved aside first,
// because it cannot be overwritten while executing — and a leftover from a
// previous update is removed on the way, since by now nothing is running
// it.
func swapIntoPlace(staged, exe string) error {
	if runtimeGOOS() != "windows" {
		if err := os.Rename(staged, exe); err != nil {
			return fmt.Errorf("could not move the new binary into place at %s: %w", exe, err)
		}
		return nil
	}
	aside := exe + supersededSuffix
	_ = os.Remove(aside)
	if err := os.Rename(exe, aside); err != nil {
		return fmt.Errorf("could not move the running binary aside from %s: %w", exe, err)
	}
	if err := os.Rename(staged, exe); err != nil {
		// Put the original back rather than leaving the path empty: a
		// missing binary is worse than an un-updated one, since the next
		// restart then has nothing to run at all.
		if restoreErr := os.Rename(aside, exe); restoreErr != nil {
			return fmt.Errorf("could not install the new binary at %s (%w) AND could not restore "+
				"the original from %s (%v) — that path now has no binary and needs fixing by hand",
				exe, err, aside, restoreErr)
		}
		return fmt.Errorf("could not install the new binary at %s: %w (the original is back in place)", exe, err)
	}
	return nil
}

// runtimeGOOS is indirected so the Windows swap can be exercised on a
// machine that isn't Windows — the branch that only runs somewhere the
// tests never execute is exactly the one worth testing.
var runtimeGOOS = func() string { return runtime.GOOS }

// isNewer compares two release tags. Deliberately strict: anything it
// cannot parse is an error rather than a guess, because the caller's
// response to "I don't know" is to refuse to replace a binary, and the
// response to a wrong guess could be a downgrade.
func isNewer(candidate, running string) (bool, error) {
	c, err := parseSemver(candidate)
	if err != nil {
		return false, err
	}
	// A development build carries a revision, not a tag. It cannot be
	// compared, and anything published is by definition more official
	// than an unreleased local build — but that is the caller's decision
	// to describe, not something to smuggle through a version compare.
	r, err := parseSemver(running)
	if err != nil {
		return false, fmt.Errorf("this build reports %q, which is not a release version", running)
	}
	for i := range c {
		if c[i] != r[i] {
			return c[i] > r[i], nil
		}
	}
	return false, nil
}

func parseSemver(v string) ([3]int, error) {
	var out [3]int
	s := strings.TrimPrefix(strings.TrimSpace(v), "v")
	// Anything after a "+" or "-" (build metadata, a +modified marker, a
	// pre-release suffix) is not part of the ordering this compares.
	if i := strings.IndexAny(s, "+-"); i >= 0 {
		s = s[:i]
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return out, fmt.Errorf("%q is not a three-part version", v)
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return out, fmt.Errorf("%q is not a three-part version", v)
		}
		out[i] = n
	}
	return out, nil
}
