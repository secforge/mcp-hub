package hubenc

import (
	"encoding/json"
	"os"
	"testing"
)

// The shared vectors (chat-relay docs/hub-encryption-vectors.json) pin the
// derivation, the AAD spelling and the sealing to what chat-relay and every
// other client compute.
func TestTheSharedVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Key, Salt, EncKey, Check, SenderPeerID, Mid, Nonce string
		Cases                                              []struct {
			Rev                       int
			Field, AAD, Plaintext, Ct string
		}
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	key, err := ParseKey(v.Key)
	if err != nil {
		t.Fatal(err)
	}
	salt, _ := b64.DecodeString(v.Salt)
	encKey, check, err := Derive(key, salt)
	if err != nil {
		t.Fatal(err)
	}
	if b64.EncodeToString(encKey) != v.EncKey || b64.EncodeToString(check) != v.Check {
		t.Fatalf("derivation: encKey %s check %s", b64.EncodeToString(encKey), b64.EncodeToString(check))
	}
	s, err := Open(key, v.Salt, v.Check)
	if err != nil {
		t.Fatal(err)
	}
	nonce, _ := b64.DecodeString(v.Nonce)
	for _, c := range v.Cases {
		if got := string(s.AAD(v.SenderPeerID, v.Mid, c.Rev, c.Field)); got != c.AAD {
			t.Errorf("%s rev %d: AAD %q, want %q", c.Field, c.Rev, got, c.AAD)
		}
		ct := s.SealWithNonce(nonce, []byte(c.Plaintext), v.SenderPeerID, v.Mid, c.Rev, c.Field)
		if b64.EncodeToString(ct) != c.Ct {
			t.Errorf("%s rev %d: ct %s, want %s", c.Field, c.Rev, b64.EncodeToString(ct), c.Ct)
		}
		want, _ := b64.DecodeString(c.Ct)
		plain, err := s.OpenValue(nonce, want, v.SenderPeerID, v.Mid, c.Rev, c.Field)
		if err != nil || string(plain) != c.Plaintext {
			t.Errorf("%s rev %d: opened %q, %v", c.Field, c.Rev, plain, err)
		}
	}
}
