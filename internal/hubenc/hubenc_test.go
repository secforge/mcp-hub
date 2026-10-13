package hubenc

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func testSession(t *testing.T) (*Session, []byte, string) {
	t.Helper()
	k, err := NewKey()
	if err != nil {
		t.Fatal(err)
	}
	key, err := ParseKey(k)
	if err != nil {
		t.Fatal(err)
	}
	salt, _ := NewSalt()
	check, err := Check(key, salt)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(key, salt, check)
	if err != nil {
		t.Fatal(err)
	}
	return s, key, salt
}

func TestSealedTextOpensOnlyInItsOwnPlace(t *testing.T) {
	s, _, _ := testSession(t)
	nonce, ct, err := s.Seal([]byte("hello"), "peer-a", "mid-1", 0, FieldText())
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.OpenValue(nonce, ct, "peer-a", "mid-1", 0, FieldText())
	if err != nil || string(got) != "hello" {
		t.Fatalf("own place: %q %v", got, err)
	}
	for name, open := range map[string]func() ([]byte, error){
		"another sender":   func() ([]byte, error) { return s.OpenValue(nonce, ct, "peer-b", "mid-1", 0, FieldText()) },
		"another message":  func() ([]byte, error) { return s.OpenValue(nonce, ct, "peer-a", "mid-2", 0, FieldText()) },
		"another revision": func() ([]byte, error) { return s.OpenValue(nonce, ct, "peer-a", "mid-1", 1, FieldText()) },
		"another field":    func() ([]byte, error) { return s.OpenValue(nonce, ct, "peer-a", "mid-1", 0, FieldFile(0)) },
	} {
		if _, err := open(); !errors.Is(err, ErrOpen) {
			t.Errorf("%s: opened (err=%v)", name, err)
		}
	}
	other, _, _ := testSession(t)
	if _, err := other.OpenValue(nonce, ct, "peer-a", "mid-1", 0, FieldText()); !errors.Is(err, ErrOpen) {
		t.Error("opened under another session's key")
	}
}

func TestTheWrongKeyIsRefusedAtOpen(t *testing.T) {
	_, _, salt := testSession(t)
	k1, _ := NewKey()
	k2, _ := NewKey()
	key1, _ := ParseKey(k1)
	key2, _ := ParseKey(k2)
	check, _ := Check(key1, salt)
	if _, err := Open(key2, salt, check); !errors.Is(err, ErrWrongKey) {
		t.Fatalf("wrong key not refused: %v", err)
	}
}

func TestAKeyFileOthersCanReadIsRefused(t *testing.T) {
	k, _ := NewKey()
	p := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(p, []byte(k+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadKeyFile(p); err == nil {
		t.Fatal("a world-readable key file was accepted")
	}
	os.Chmod(p, 0o600)
	key, err := ReadKeyFile(p)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := ParseKey(k)
	if !bytes.Equal(key, want) {
		t.Fatal("key file read back differently")
	}
}

func TestTheAADHasTheAgreedShape(t *testing.T) {
	s, _, salt := testSession(t)
	if got := string(s.AAD("P", "M", 2, FieldFileMeta(1))); got != "v1|"+salt+"|P|M|2|filemeta:1" {
		t.Fatalf("AAD %q", got)
	}
}
