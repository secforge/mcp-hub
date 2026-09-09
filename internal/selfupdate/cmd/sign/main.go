// Command sign produces and checks the detached signatures that let a
// client verify a binary before replacing itself with it.
//
// Verification lives here as well as in the client on purpose: a release
// that cannot be verified by the same key the client has compiled in is
// broken for every client at once, and finding that out at release time
// costs one failed script rather than one failed update per machine.
package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/secforge/mcp-hub/internal/selfupdate"
)

func main() {
	key := flag.String("key", "", "path to the base64 Ed25519 private key")
	in := flag.String("in", "", "path to the file to sign or verify")
	out := flag.String("out", "", "where to write the detached signature")
	sig := flag.String("sig", "", "path to the signature to verify")
	doVerify := flag.Bool("verify", false, "verify -in against -sig instead of signing")
	flag.Parse()

	if err := run(*key, *in, *out, *sig, *doVerify); err != nil {
		fmt.Fprintln(os.Stderr, "sign:", err)
		os.Exit(1)
	}
}

func run(keyPath, inPath, outPath, sigPath string, doVerify bool) error {
	if inPath == "" {
		return fmt.Errorf("-in is required")
	}
	payload, err := os.ReadFile(inPath)
	if err != nil {
		return err
	}

	if doVerify {
		if sigPath == "" {
			return fmt.Errorf("-sig is required with -verify")
		}
		raw, err := os.ReadFile(sigPath)
		if err != nil {
			return err
		}
		if err := selfupdate.VerifyWithEmbeddedKey(payload, raw); err != nil {
			return fmt.Errorf("%s does not verify against the key compiled into this client: %w", inPath, err)
		}
		fmt.Printf("%s verifies against the embedded release key\n", inPath)
		return nil
	}

	if keyPath == "" || outPath == "" {
		return fmt.Errorf("-key and -out are required when signing")
	}
	encoded, err := os.ReadFile(keyPath)
	if err != nil {
		return err
	}
	priv, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encoded)))
	if err != nil {
		return fmt.Errorf("the key at %s is not base64: %w", keyPath, err)
	}
	if len(priv) != ed25519.PrivateKeySize {
		return fmt.Errorf("the key at %s is %d bytes, expected %d", keyPath, len(priv), ed25519.PrivateKeySize)
	}
	signature := ed25519.Sign(ed25519.PrivateKey(priv), payload)
	return os.WriteFile(outPath, []byte(base64.StdEncoding.EncodeToString(signature)+"\n"), 0o644)
}
