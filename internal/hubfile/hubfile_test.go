package hubfile

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"testing"
)

func roundTrip(t *testing.T, size int) {
	t.Helper()
	ks, _ := NewKey()
	key, err := ParseKey(ks)
	if err != nil {
		t.Fatal(err)
	}
	plain := make([]byte, size)
	rand.Read(plain)
	h := Header{Name: "dump.bin", Size: int64(size)}
	var sealed bytes.Buffer
	if err := Encrypt(&sealed, bytes.NewReader(plain), key, h); err != nil {
		t.Fatal(err)
	}
	if want, _ := SealedSize(h); int64(sealed.Len()) != want {
		t.Fatalf("size %d: sealed %d bytes, SealedSize says %d", size, sealed.Len(), want)
	}
	var again bytes.Buffer
	Encrypt(&again, bytes.NewReader(plain), key, h)
	if !bytes.Equal(sealed.Bytes(), again.Bytes()) {
		t.Fatal("the same key and input sealed differently")
	}
	fr, err := NewReader(bytes.NewReader(sealed.Bytes()), key)
	if err != nil {
		t.Fatal(err)
	}
	if fr.Header() != h {
		t.Fatalf("header %+v", fr.Header())
	}
	got, err := io.ReadAll(fr)
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("size %d: err %v, equal %v", size, err, bytes.Equal(got, plain))
	}
}

func TestAFileOfAnySizeComesBackAsItWent(t *testing.T) {
	for _, n := range []int{0, 1, ChunkSize - 1, ChunkSize, ChunkSize + 1, 3*ChunkSize + 17} {
		roundTrip(t, n)
	}
}

func sealedOf(t *testing.T, size int) ([]byte, []byte) {
	ks, _ := NewKey()
	key, _ := ParseKey(ks)
	var b bytes.Buffer
	if err := Encrypt(&b, bytes.NewReader(make([]byte, size)), key, Header{Name: "x", Size: int64(size)}); err != nil {
		t.Fatal(err)
	}
	return b.Bytes(), key
}

func readAll(sealed, key []byte) error {
	fr, err := NewReader(bytes.NewReader(sealed), key)
	if err != nil {
		return err
	}
	_, err = io.ReadAll(fr)
	return err
}

func TestACutFileIsRefusedEvenAtAChunkBoundary(t *testing.T) {
	sealed, key := sealedOf(t, 3*ChunkSize)
	full := len(sealed)
	for _, cut := range []int{full - 1, full - (ChunkSize + tagSize), full - (ChunkSize + tagSize) - 5} {
		err := readAll(sealed[:cut], key)
		if !errors.Is(err, ErrTruncated) && !errors.Is(err, ErrDamaged) {
			t.Errorf("cut at %d of %d: %v", cut, full, err)
		}
	}
	if err := readAll(sealed[:full-(ChunkSize+tagSize)], key); !errors.Is(err, ErrTruncated) {
		t.Errorf("a cut at a chunk boundary reads as %v, not as incomplete", err)
	}
}

func TestAnAlteredOrForeignFileIsRefused(t *testing.T) {
	sealed, key := sealedOf(t, 2*ChunkSize+5)
	bad := append([]byte(nil), sealed...)
	bad[len(bad)-3] ^= 1
	if err := readAll(bad, key); !errors.Is(err, ErrDamaged) {
		t.Errorf("a flipped bit: %v", err)
	}
	other, _ := NewKey()
	otherKey, _ := ParseKey(other)
	if err := readAll(sealed, otherKey); !errors.Is(err, ErrNotAFile) {
		t.Errorf("another key: %v", err)
	}
}

func TestAFileThatChangesWhileSealedIsRefused(t *testing.T) {
	ks, _ := NewKey()
	key, _ := ParseKey(ks)
	if err := Encrypt(io.Discard, bytes.NewReader(make([]byte, 10)), key, Header{Size: 20}); err == nil {
		t.Error("a shorter file was sealed")
	}
	if err := Encrypt(io.Discard, bytes.NewReader(make([]byte, 30)), key, Header{Size: 20}); err == nil {
		t.Error("a longer file was sealed")
	}
}
