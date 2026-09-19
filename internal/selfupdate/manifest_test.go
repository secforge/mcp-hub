package selfupdate

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

// A manifest's own consistency is checked separately from its signature,
// because a valid signature makes none of these true: a correctly signed
// manifest can still name a schema this build cannot read, or list one
// filename twice with two different digests — which is how a verifier
// taking the first match and an installer taking the last can be made to
// disagree about what was signed.
func TestAManifestIsCheckedForWhatASignatureCannotSay(t *testing.T) {
	good := Manifest{
		Schema: ManifestSchema, Version: "v1.2.3",
		Commit:   "0123456789abcdef0123456789abcdef01234567",
		Binaries: []ManifestBinary{{Filename: "a", SHA256: strings.Repeat("a", 64)}},
	}
	raw, err := MarshalManifest(good)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := ParseManifest(raw); err != nil {
		t.Fatalf("a well-formed manifest must parse: %v", err)
	}

	for _, tc := range []struct {
		name string
		edit func(m *Manifest)
		want string
	}{
		{"a schema this build cannot read", func(m *Manifest) { m.Schema = 99 }, "schema"},
		{"a version that is not a tag", func(m *Manifest) { m.Version = "1.2.3" }, "tag form"},
		{"a commit that is not a sha", func(m *Manifest) { m.Commit = "nope" }, "hex sha"},
		{"no binaries at all", func(m *Manifest) { m.Binaries = nil }, "no binaries"},
		{"a digest that is not a digest", func(m *Manifest) {
			m.Binaries = []ManifestBinary{{Filename: "a", SHA256: "short"}}
		}, "hex sha256"},
		{"the same file twice", func(m *Manifest) {
			m.Binaries = []ManifestBinary{
				{Filename: "a", SHA256: strings.Repeat("a", 64)},
				{Filename: "a", SHA256: strings.Repeat("b", 64)},
			}
		}, "twice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := good
			m.Binaries = append([]ManifestBinary(nil), good.Binaries...)
			tc.edit(&m)
			raw, err := json.Marshal(m)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			_, err = ParseManifest(raw)
			if err == nil {
				t.Fatal("expected this to be refused")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected the error to mention %q, got: %v", tc.want, err)
			}
		})
	}
}

// The signed bytes must be reproducible from the same inputs, or the
// release script and the verifier are signing and checking different
// files. Sorted entries, and no trailing newline — json.Encoder adds
// one, MarshalIndent does not, and that difference alone breaks every
// signature.
func TestTheSignedBytesAreReproducible(t *testing.T) {
	m := Manifest{
		Schema: ManifestSchema, Version: "v1.2.3",
		Commit: "0123456789abcdef0123456789abcdef01234567",
		Binaries: []ManifestBinary{
			{Filename: "zebra", SHA256: strings.Repeat("b", 64)},
			{Filename: "alpha", SHA256: strings.Repeat("a", 64)},
		},
	}
	first, err := MarshalManifest(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	second, err := MarshalManifest(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(first) != string(second) {
		t.Fatal("two marshals of one manifest produced different bytes")
	}
	if strings.HasSuffix(string(first), "\n") {
		t.Fatal("the signed bytes must not end in a newline: the signature covers the file as " +
			"published, and a stray newline makes that two different files")
	}
	if strings.Index(string(first), "alpha") > strings.Index(string(first), "zebra") {
		t.Fatal("binaries must be sorted by filename, or the same release signs differently " +
			"depending on map iteration order")
	}
}

// DigestFor must distinguish "not listed" from "listed as empty", or a
// missing entry compares equal to an unset digest somewhere downstream.
func TestAMissingEntryIsNotAnEmptyDigest(t *testing.T) {
	m := Manifest{Binaries: []ManifestBinary{{Filename: "a", SHA256: strings.Repeat("a", 64)}}}
	if _, ok := m.DigestFor("b"); ok {
		t.Fatal("a file the manifest does not list must report that it is not listed")
	}
	got, ok := m.DigestFor("a")
	if !ok || got != strings.Repeat("a", 64) {
		t.Fatalf("a listed file must report its digest, got %q ok=%v", got, ok)
	}
}

// A key id is DERIVED from the key, so it cannot name the wrong one. A
// label chosen by hand can be attached to any key, which makes it a
// claim rather than evidence — the same mistake as trusting the tag.
func TestAKeyIDNamesTheKeyItWasDerivedFrom(t *testing.T) {
	pubA, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	pubB, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	a := base64.StdEncoding.EncodeToString(pubA)
	b := base64.StdEncoding.EncodeToString(pubB)

	idA, err := KeyIDFor(a)
	if err != nil {
		t.Fatalf("deriving: %v", err)
	}
	if len(idA) != 16 {
		t.Fatalf("a key id is 16 hex characters, got %q", idA)
	}
	if again, _ := KeyIDFor(a); again != idA {
		t.Fatal("the same key must derive the same id")
	}
	if idB, _ := KeyIDFor(b); idB == idA {
		t.Fatal("two different keys derived the same id")
	}

	// Empty means the original key: every manifest published before a
	// rotation ever happens says nothing, and that must stay acceptable.
	if err := (Manifest{}).CheckKeyID(a); err != nil {
		t.Fatalf("an unstated key id must be accepted: %v", err)
	}
	if err := (Manifest{KeyID: idA}).CheckKeyID(a); err != nil {
		t.Fatalf("a manifest naming this very key must be accepted: %v", err)
	}
	err = Manifest{KeyID: idA}.CheckKeyID(b)
	if err == nil {
		t.Fatal("a manifest naming a different key must be refused")
	}
	if !strings.Contains(err.Error(), idA) {
		t.Fatalf("the error must name the key the manifest claims, got: %v", err)
	}
}
