// Package hubenc implements end-to-end encryption for hub sessions
// (wire-protocol §2.11): every client of an encrypted session holds the
// same random key, and the server relays ciphertext it cannot read.
//
// A 32-byte key and the session's 16-byte salt derive two values with
// HKDF-SHA256: the AES-256-GCM key that seals message bodies and
// attachments, and a check value the server stores so that a client with
// the wrong key can say "wrong key" instead of failing on every message.
// Each sealed value is bound by its AAD to the session (by its salt, which
// is per-session random and public), the sender, the
// message, its revision and the field it fills, so the server can neither
// move a ciphertext to another message or sender nor show an older
// revision as an edit.
package hubenc

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
)

const (
	// Version is the envelope version this package seals and opens.
	Version = 1

	KeySize   = 32
	SaltSize  = 16
	nonceSize = 12

	infoKey   = "chat-relay hub-enc v1 key"
	infoCheck = "chat-relay hub-enc v1 check"
)

// ErrWrongKey reports a key whose check value does not match the
// session's: every message would fail to open, for the same reason.
var ErrWrongKey = errors.New("the key does not match this session's encryption check — it is the wrong key")

// ErrOpen reports a ciphertext that does not open under this key and AAD:
// corrupted, or moved to another message, sender or revision.
var ErrOpen = errors.New("the message does not decrypt: it was altered, or belongs to another message or sender")

var b64 = base64.RawURLEncoding

// Session holds what one encrypted session needs: the derived key and the
// values the AAD is built from.
type Session struct {
	aead cipher.AEAD
	salt string
}

// ParseKey decodes a key as stored in a key file: base64url without
// padding, surrounding whitespace ignored.
func ParseKey(s string) ([]byte, error) {
	key, err := b64.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("the key is not base64url without padding: %w", err)
	}
	if len(key) != KeySize {
		return nil, fmt.Errorf("the key is %d bytes; it must be %d", len(key), KeySize)
	}
	return key, nil
}

// ReadKeyFile reads a key file, refusing one that anyone but its owner can
// read: the file is the whole of the session's secret.
func ReadKeyFile(path string) ([]byte, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("reading the key file: %w", err)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("the key file %s is readable by others (mode %v); restrict it to its owner (chmod 600)",
			path, fi.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading the key file: %w", err)
	}
	return ParseKey(string(raw))
}

// NewKey mints a key and returns it in key-file form.
func NewKey() (string, error) {
	key := make([]byte, KeySize)
	if _, err := rand.Read(key); err != nil {
		return "", err
	}
	return b64.EncodeToString(key), nil
}

// NewSalt mints a session salt, base64url without padding — what a
// creating client sends with its check value.
func NewSalt() (string, error) {
	salt := make([]byte, SaltSize)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	return b64.EncodeToString(salt), nil
}

// Derive returns the encryption key and the check value for key and salt.
func Derive(key, salt []byte) (encKey, check []byte, err error) {
	if encKey, err = hkdf.Key(sha256.New, key, salt, infoKey, 32); err != nil {
		return nil, nil, err
	}
	if check, err = hkdf.Key(sha256.New, key, salt, infoCheck, 32); err != nil {
		return nil, nil, err
	}
	return encKey, check, nil
}

// Check returns the check value a creating client sends, base64url.
func Check(key []byte, salt string) (string, error) {
	s, err := b64.DecodeString(salt)
	if err != nil {
		return "", fmt.Errorf("the salt is not base64url: %w", err)
	}
	_, check, err := Derive(key, s)
	if err != nil {
		return "", err
	}
	return b64.EncodeToString(check), nil
}

// Open prepares a session from the key and the encryption parameters the
// server stated at join, refusing a key whose check does not match.
func Open(key []byte, salt, check string) (*Session, error) {
	s, err := b64.DecodeString(salt)
	if err != nil {
		return nil, fmt.Errorf("the session's salt is not base64url: %w", err)
	}
	want, err := b64.DecodeString(check)
	if err != nil {
		return nil, fmt.Errorf("the session's check value is not base64url: %w", err)
	}
	encKey, got, err := Derive(key, s)
	if err != nil {
		return nil, err
	}
	if !hmac.Equal(got, want) {
		return nil, ErrWrongKey
	}
	block, err := aes.NewCipher(encKey)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Session{aead: aead, salt: salt}, nil
}

// Field names what a sealed value fills, as it appears in the AAD.
func FieldText() string          { return "text" }
func FieldItem() string          { return "item" }
func FieldFile(i int) string     { return fmt.Sprintf("file:%d", i) }
func FieldFileMeta(i int) string { return fmt.Sprintf("filemeta:%d", i) }

// AAD is the associated data binding a sealed value to its place.
func (s *Session) AAD(sender, mid string, rev int, field string) []byte {
	return []byte(fmt.Sprintf("v%d|%s|%s|%s|%d|%s", Version, s.salt, sender, mid, rev, field))
}

// Seal encrypts plaintext for the given place, returning nonce and
// ciphertext‖tag.
func (s *Session) Seal(plaintext []byte, sender, mid string, rev int, field string) (nonce, ct []byte, err error) {
	nonce = make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, err
	}
	return nonce, s.SealWithNonce(nonce, plaintext, sender, mid, rev, field), nil
}

// SealWithNonce is Seal with a given nonce, for test vectors. A nonce must
// never be used twice with one key.
func (s *Session) SealWithNonce(nonce, plaintext []byte, sender, mid string, rev int, field string) []byte {
	return s.aead.Seal(nil, nonce, plaintext, s.AAD(sender, mid, rev, field))
}

// OpenValue decrypts a sealed value for the given place.
func (s *Session) OpenValue(nonce, ct []byte, sender, mid string, rev int, field string) ([]byte, error) {
	if len(nonce) != nonceSize {
		return nil, ErrOpen
	}
	plain, err := s.aead.Open(nil, nonce, ct, s.AAD(sender, mid, rev, field))
	if err != nil {
		return nil, ErrOpen
	}
	return plain, nil
}

// Overhead is how many bytes sealing adds to a value, nonce included.
func Overhead() int { return nonceSize + 16 }

// B64 is the encoding of nonce and ct in an envelope.
func B64() *base64.Encoding { return b64 }
