// Command manifest builds and signs a release manifest — the one file
// that states, under this project's release key, what a release IS.
//
// It exists as a program rather than a few lines of shell because the
// bytes it writes are the bytes the client verifies: the ordering, the
// indentation and the absence of a trailing newline all change the
// signature. Producing them from the same code the verifier parses is
// what stops the release script and the client disagreeing about what
// was signed.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/secforge/mcp-hub/internal/selfupdate"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "manifest:", err)
		os.Exit(1)
	}
}

// binaryPrefix is what every published client binary is named with —
// see selfupdate.AssetName, which builds the same prefix per platform.
const binaryPrefix = "mcp-hub-client-"

func run() error {
	version := flag.String("version", "", "release tag, e.g. v3.1.4")
	commit := flag.String("commit", "", "40-character commit the binaries were built from")
	keyID := flag.String("key-id", "", "optional identifier for the signing key; derived from "+
		"-public-key when that is given instead")
	publicKey := flag.String("public-key", "", "path to the base64 Ed25519 PUBLIC key, used to "+
		"derive the manifest's keyId")
	dir := flag.String("dir", "", "directory holding the built binaries")
	keyPath := flag.String("key", "", "path to the base64 Ed25519 private key")
	out := flag.String("out", "", "path to write the manifest to (its signature goes beside it)")
	expect := flag.Int("expect", 0, "how many binaries the manifest must describe; 0 to skip the check")
	flag.Parse()

	if *version == "" || *commit == "" || *dir == "" || *keyPath == "" || *out == "" {
		return fmt.Errorf("version, commit, dir, key and out are all required")
	}

	entries, err := os.ReadDir(*dir)
	if err != nil {
		return fmt.Errorf("reading %s: %w", *dir, err)
	}
	m := selfupdate.Manifest{
		Schema:  selfupdate.ManifestSchema,
		Version: *version,
		Commit:  *commit,
		KeyID:   *keyID,
	}
	// DERIVED where the public key is given, rather than typed. A key id
	// that is a function of the key cannot name the wrong one; one typed
	// on a command line can, and would then be a label that disagrees
	// with the signature beside it.
	if *publicKey != "" {
		raw, err := os.ReadFile(*publicKey)
		if err != nil {
			return fmt.Errorf("reading the public key: %w", err)
		}
		id, err := selfupdate.KeyIDFor(string(raw))
		if err != nil {
			return fmt.Errorf("deriving the key id: %w", err)
		}
		if *keyID != "" && *keyID != id {
			return fmt.Errorf("-key-id says %s but the public key given derives %s", *keyID, id)
		}
		m.KeyID = id
	}
	for _, e := range entries {
		name := e.Name()
		// NAMED, not "whatever is in the directory". A first version of
		// this described every file it found, which in a dry run meant
		// the signing key and its public half were listed as release
		// binaries — signed, published, and stated as things to install.
		// A manifest is a claim about a known set of artifacts, so the
		// set is recognised rather than swept up.
		if e.IsDir() || !strings.HasPrefix(name, binaryPrefix) || strings.HasSuffix(name, ".sig") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(*dir, name))
		if err != nil {
			return fmt.Errorf("reading %s: %w", name, err)
		}
		m.Binaries = append(m.Binaries, selfupdate.ManifestBinary{
			Filename: name,
			SHA256:   selfupdate.Digest(body),
		})
	}
	if len(m.Binaries) == 0 {
		return fmt.Errorf("%s holds no %s* binaries to describe", *dir, binaryPrefix)
	}
	if *expect > 0 && len(m.Binaries) != *expect {
		return fmt.Errorf("found %d binaries in %s but this release publishes %d — a manifest "+
			"that accounts for a different set than the release does is worse than none",
			len(m.Binaries), *dir, *expect)
	}

	raw, err := selfupdate.MarshalManifest(m)
	if err != nil {
		return fmt.Errorf("rendering the manifest: %w", err)
	}
	// Parsed back before it is signed. Signing something this build
	// could not itself read would publish a manifest that fails
	// verification everywhere, and the release would look fine until
	// somebody tried to update.
	if _, err := selfupdate.ParseManifest(raw); err != nil {
		return fmt.Errorf("the manifest this built does not pass its own checks: %w", err)
	}

	key, err := os.ReadFile(*keyPath)
	if err != nil {
		return fmt.Errorf("reading the signing key: %w", err)
	}
	sig, err := selfupdate.SignManifest(raw, string(key))
	if err != nil {
		return err
	}
	if err := os.WriteFile(*out, raw, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", *out, err)
	}
	// The signature file carries a trailing newline, like every other
	// .sig this project publishes; the MANIFEST does not, and the
	// verifier trims the signature rather than the manifest.
	if err := os.WriteFile(*out+".sig", []byte(sig+"\n"), 0o644); err != nil {
		return fmt.Errorf("writing %s.sig: %w", *out, err)
	}
	fmt.Printf("%s: %d binaries, version %s, commit %s\n", filepath.Base(*out), len(m.Binaries),
		m.Version, m.Commit)
	return nil
}
