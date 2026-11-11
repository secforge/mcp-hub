package hubfile

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"testing"
)

// The vectors pin the format across implementations: chat-relay's browser
// implementation is held to the same file, byte for byte. Plaintext i is
// (j*7+i) % 251 at byte j; the key and the sealed bytes' sha256 are in
// each vector.
func TestTheFormatMatchesTheSharedVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/hub-file-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Vectors []struct {
			Name      string `json:"name"`
			Key       string `json:"key"`
			Size      int64  `json:"size"`
			SealedB64 string `json:"sealedB64"`
			SHA256    string `json:"sha256"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Vectors) == 0 {
		t.Fatal("no vectors")
	}
	for i, v := range file.Vectors {
		key, err := ParseKey(v.Key)
		if err != nil {
			t.Fatalf("%s: key: %v", v.Name, err)
		}
		sealed, err := base64.StdEncoding.DecodeString(v.SealedB64)
		if err != nil {
			t.Fatalf("%s: sealed: %v", v.Name, err)
		}
		plain := make([]byte, v.Size)
		for j := range plain {
			plain[j] = byte((j*7 + i) % 251)
		}
		// sha256 is of the sealed bytes, so a truncated vector is caught
		// before it is blamed on the format.
		sum := sha256.Sum256(sealed)
		if hex.EncodeToString(sum[:]) != v.SHA256 {
			t.Fatalf("%s: the vector's sha256 does not describe its sealed bytes", v.Name)
		}

		fr, err := NewReader(bytes.NewReader(sealed), key)
		if err != nil {
			t.Fatalf("%s: open: %v", v.Name, err)
		}
		if h := fr.Header(); h.Name != v.Name || h.Size != v.Size {
			t.Fatalf("%s: header %+v", v.Name, h)
		}
		got, err := io.ReadAll(fr)
		if err != nil {
			t.Fatalf("%s: read: %v", v.Name, err)
		}
		if !bytes.Equal(got, plain) {
			t.Fatalf("%s: decrypted content differs", v.Name)
		}

		var out bytes.Buffer
		if err := Encrypt(&out, bytes.NewReader(plain), key, Header{Name: v.Name, Size: v.Size}); err != nil {
			t.Fatalf("%s: encrypt: %v", v.Name, err)
		}
		if !bytes.Equal(out.Bytes(), sealed) {
			t.Fatalf("%s: encrypting gives other bytes than the vector", v.Name)
		}
	}
}
