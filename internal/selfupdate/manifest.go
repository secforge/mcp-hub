package selfupdate

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ManifestName and ManifestSigName are the two assets a release
// publishes beside its binaries. Agreed on the hub, 2026-09-19, with
// chat-relay's author and a Codex reviewer: chat-relay verifies the same
// file to learn the current client release, so the names are part of the
// agreement rather than a local detail.
const (
	ManifestName    = "mcp-hub-release-manifest.json"
	ManifestSigName = ManifestName + signatureSuffix
)

// ManifestSchema is the only schema this client understands. A manifest
// declaring anything else is refused rather than read leniently: the
// number exists so a future shape can be introduced without an old
// client guessing at it.
const ManifestSchema = 1

// Manifest is the signed statement of what a release IS.
//
// The problem it solves: every binary in a release is signed, so the
// BYTES are accounted for, and the version is not. The version lives in
// the git tag, which is GitHub metadata — unsigned, and, until the
// --target fix of 2026-09-19, not even reliably pointing at the commit
// the binaries were built from. Whoever can shape the API response can
// serve tag v99.0.0 alongside a genuine, genuinely signed old binary and
// its genuine signature: every signature check passes and a known-buggy
// build installs over a good one.
//
// Putting the version INSIDE the signed material is what closes that.
// The tag becomes a convenience for finding the release, and the
// manifest becomes the thing that says what it is.
type Manifest struct {
	Schema int `json:"schema"`
	// Version is the release this manifest describes, tag form ("v3.1.4").
	// This is the authority — see Apply, which compares against this and
	// uses the tag only as a cross-check.
	Version string `json:"version"`
	// Commit is the 40-hex commit the binaries were built from. Signed
	// metadata, so a release can be tied back to a tree even though the
	// tag that names it cannot be trusted to.
	Commit string `json:"commit"`
	// KeyID names the signing key, so one can be rotated without a
	// schema change: the first 16 hex characters of the SHA-256 of the
	// public key — see KeyIDFor.
	//
	// DERIVED from the key rather than chosen for it. A human-readable
	// name ("2026-release", "key2") can be attached to any key, which
	// makes it a label rather than evidence — the same mistake as
	// trusting the tag. This one is checkable: a client holding a pinned
	// key can confirm the id names THAT key, and a manifest signed by
	// another cannot claim it.
	//
	// Empty means the original key, which is what every manifest
	// published before a rotation ever happens says.
	KeyID string `json:"keyId,omitempty"`
	// Binaries lists every published binary with its digest, sorted by
	// filename so the bytes signed are reproducible from the same inputs.
	Binaries []ManifestBinary `json:"binaries"`
}

// ManifestBinary is one published artifact and its digest.
type ManifestBinary struct {
	Filename string `json:"filename"`
	SHA256   string `json:"sha256"`
}

// Digest returns the lowercase hex SHA-256 of b, the form a manifest
// carries.
func Digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// MarshalManifest renders m as the exact bytes to sign and publish:
// indented for a human reading the published file, and with NO trailing
// newline, because the signature covers these bytes and "the file as
// published" has to mean one thing. encoding/json's MarshalIndent adds
// no trailing newline; json.Encoder does, which is the trap this exists
// to keep out of the release script.
func MarshalManifest(m Manifest) ([]byte, error) {
	sort.Slice(m.Binaries, func(i, j int) bool {
		return m.Binaries[i].Filename < m.Binaries[j].Filename
	})
	return json.MarshalIndent(m, "", "  ")
}

// ParseManifest decodes and CHECKS a manifest's own internal
// consistency, before anything downstream trusts a field of it.
//
// Every check here is one that a valid signature does not make for you:
// a correctly signed manifest can still name a schema this client cannot
// read, carry a digest that is not a digest, or list one filename twice
// with two different hashes — the last of which is how a verifier that
// takes "the first match" and an installer that takes "the last match"
// can be made to disagree about what was signed.
func ParseManifest(raw []byte) (Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return m, fmt.Errorf("the release manifest is not valid JSON: %w", err)
	}
	if m.Schema != ManifestSchema {
		return m, fmt.Errorf("the release manifest declares schema %d and this build understands "+
			"only %d — refusing to read it as though the shapes matched", m.Schema, ManifestSchema)
	}
	if !strings.HasPrefix(m.Version, "v") {
		return m, fmt.Errorf("the release manifest's version %q is not in tag form", m.Version)
	}
	if !isHex(m.Commit, 40) {
		return m, fmt.Errorf("the release manifest's commit %q is not a 40-character hex sha",
			m.Commit)
	}
	if len(m.Binaries) == 0 {
		return m, fmt.Errorf("the release manifest lists no binaries")
	}
	seen := make(map[string]bool, len(m.Binaries))
	for _, b := range m.Binaries {
		if b.Filename == "" {
			return m, fmt.Errorf("the release manifest has an entry with no filename")
		}
		if seen[b.Filename] {
			return m, fmt.Errorf("the release manifest lists %q twice, so which digest it claims "+
				"depends on who reads it", b.Filename)
		}
		seen[b.Filename] = true
		if !isHex(b.SHA256, 64) {
			return m, fmt.Errorf("the release manifest's digest for %q is not a 64-character hex "+
				"sha256", b.Filename)
		}
	}
	return m, nil
}

// DigestFor returns the digest this manifest claims for filename, and
// whether it claims one at all. The second return is the whole point: a
// missing entry must never read as an empty digest that something then
// compares equal to.
func (m Manifest) DigestFor(filename string) (string, bool) {
	for _, b := range m.Binaries {
		if b.Filename == filename {
			return b.SHA256, true
		}
	}
	return "", false
}

// KeyIDFor derives the identifier for a base64 Ed25519 public key: the
// first 16 hex characters of its SHA-256. Both halves of a release use
// this, so an id in a manifest and an id computed from a pinned key are
// the same function of the same bytes.
func KeyIDFor(publicKeyBase64 string) (string, error) {
	pub, err := base64.StdEncoding.DecodeString(strings.TrimSpace(publicKeyBase64))
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return "", fmt.Errorf("not a base64 Ed25519 public key")
	}
	return Digest(pub)[:16], nil
}

// CheckKeyID reports whether a manifest's stated key id is consistent
// with the key this build verifies against. An EMPTY id is accepted:
// it means the original key, which is every manifest published before a
// rotation. A stated id that names a different key is refused — the
// signature would already have failed, but saying which key was expected
// turns "signature does not match" into something a reader can act on.
func (m Manifest) CheckKeyID(publicKeyBase64 string) error {
	if m.KeyID == "" {
		return nil
	}
	want, err := KeyIDFor(publicKeyBase64)
	if err != nil {
		return fmt.Errorf("this build's embedded public key is malformed")
	}
	if m.KeyID != want {
		return fmt.Errorf("the release manifest is signed by key %s and this build verifies "+
			"against key %s — it was published for a different key than the one compiled in here",
			m.KeyID, want)
	}
	return nil
}

// VerifyManifest checks sig against raw with this build's embedded
// public key and returns the parsed manifest. The signature is checked
// FIRST and the contents are parsed only after, so nothing in an
// unverified file decides anything — not even which error to report.
func VerifyManifest(raw, sig []byte) (Manifest, error) {
	if err := verify(raw, sig); err != nil {
		return Manifest{}, fmt.Errorf("the release manifest failed signature verification (%w) — "+
			"nothing in it can be trusted, including the version it claims", err)
	}
	m, err := ParseManifest(raw)
	if err != nil {
		return m, err
	}
	// After the signature, never instead of it: this cannot make an
	// unsigned manifest acceptable, only explain a verified one.
	if err := m.CheckKeyID(releasePublicKey); err != nil {
		return m, err
	}
	return m, nil
}

func isHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}

// SignManifest is the signing half, used by the release script's signer
// so the two sides cannot drift in how the bytes are produced.
func SignManifest(raw []byte, privateKey string) (string, error) {
	priv, err := base64.StdEncoding.DecodeString(strings.TrimSpace(privateKey))
	if err != nil || len(priv) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("the signing key is not a base64 Ed25519 private key")
	}
	return base64.StdEncoding.EncodeToString(ed25519.Sign(ed25519.PrivateKey(priv), raw)), nil
}
