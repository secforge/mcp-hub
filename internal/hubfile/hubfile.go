// Package hubfile is the encrypted format of a one-time file: what the
// upload tool stores on the server and the download tool opens again. The
// server only ever holds these bytes; the key travels in the link.
//
// Layout:
//
//	magic "MCPHUBF1"
//	uint32 big-endian length of the sealed header, then the sealed header
//	sealed data chunks, each ChunkSize plaintext bytes but the last
//
// Every piece is AES-256-GCM under the file's key with the magic as
// additional data. The nonce is an 11-byte big-endian counter followed by
// a byte that is 1 on the last data chunk and 0 elsewhere: the header is
// counter 0, the data chunks count from 1. A cut, reordered or swapped
// chunk fails to open, and so does a file that ends before its last chunk.
// The key is random per file and used for that one file only, so the
// counter nonces never repeat under it.
//
// The format is deterministic for a given key and input, which lets the
// upload measure the stored size and hash in one pass and send in a
// second without keeping a copy.
package hubfile

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	magic = "MCPHUBF1"
	// ChunkSize is the plaintext size of every data chunk but the last.
	ChunkSize = 64 << 10
	tagSize   = 16
	keySize   = 32
	// maxHeader bounds the sealed header a reader accepts.
	maxHeader = 64 << 10
)

// Header is what the file says about itself, sealed like its content.
type Header struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// NewKey returns a fresh random key in the form a link carries.
func NewKey() (string, error) {
	k := make([]byte, keySize)
	if _, err := rand.Read(k); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(k), nil
}

// ParseKey reads a key in the form a link carries.
func ParseKey(s string) ([]byte, error) {
	k, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(k) != keySize {
		return nil, errors.New("not a one-time file key")
	}
	return k, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	if len(key) != keySize {
		return nil, errors.New("a one-time file key is 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func nonce(counter uint64, last bool) []byte {
	n := make([]byte, 12)
	binary.BigEndian.PutUint64(n[3:11], counter)
	if last {
		n[11] = 1
	}
	return n
}

// SealedSize is the stored size of a file of size plaintext bytes with
// header h.
func SealedSize(h Header) (int64, error) {
	raw, err := json.Marshal(h)
	if err != nil {
		return 0, err
	}
	chunks := h.Size / ChunkSize
	if h.Size%ChunkSize != 0 || h.Size == 0 {
		chunks++
	}
	return int64(len(magic)) + 4 + int64(len(raw)+tagSize) + h.Size + chunks*tagSize, nil
}

// Encrypt writes r, which must hold exactly h.Size bytes, to w in the
// format above.
func Encrypt(w io.Writer, r io.Reader, key []byte, h Header) error {
	aead, err := newAEAD(key)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(h)
	if err != nil {
		return err
	}
	sealed := aead.Seal(nil, nonce(0, false), raw, []byte(magic))
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(sealed)))
	if _, err := w.Write([]byte(magic)); err != nil {
		return err
	}
	if _, err := w.Write(head[:]); err != nil {
		return err
	}
	if _, err := w.Write(sealed); err != nil {
		return err
	}
	buf := make([]byte, ChunkSize)
	out := make([]byte, 0, ChunkSize+tagSize)
	var done int64
	for counter := uint64(1); ; counter++ {
		n, err := io.ReadFull(r, buf)
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			return err
		}
		done += int64(n)
		if done > h.Size {
			return fmt.Errorf("the file grew while it was being read")
		}
		last := done == h.Size
		if !last && n < ChunkSize {
			return fmt.Errorf("the file shrank while it was being read")
		}
		out = aead.Seal(out[:0], nonce(counter, last), buf[:n], []byte(magic))
		if _, err := w.Write(out); err != nil {
			return err
		}
		if last {
			var extra [1]byte
			if m, _ := r.Read(extra[:]); m > 0 {
				return fmt.Errorf("the file grew while it was being read")
			}
			return nil
		}
	}
}

// Reader opens a file in the format above as it streams in. Read returns
// only plaintext whose chunk has been authenticated, and an error rather
// than io.EOF when the file ends before its last chunk.
type Reader struct {
	r       *bufio.Reader
	aead    cipher.AEAD
	header  Header
	counter uint64
	buf     []byte
	plain   []byte
	got     int64
	done    bool
}

// ErrNotAFile is a stream that is not in this format, or not under this key.
var ErrNotAFile = errors.New("not a one-time file, or not for this key")

// NewReader reads and opens the header.
func NewReader(r io.Reader, key []byte) (*Reader, error) {
	aead, err := newAEAD(key)
	if err != nil {
		return nil, err
	}
	br := bufio.NewReaderSize(r, ChunkSize+tagSize+1)
	head := make([]byte, len(magic)+4)
	if _, err := io.ReadFull(br, head); err != nil || string(head[:len(magic)]) != magic {
		return nil, ErrNotAFile
	}
	n := binary.BigEndian.Uint32(head[len(magic):])
	if n < tagSize || n > maxHeader {
		return nil, ErrNotAFile
	}
	sealed := make([]byte, n)
	if _, err := io.ReadFull(br, sealed); err != nil {
		return nil, ErrNotAFile
	}
	raw, err := aead.Open(nil, nonce(0, false), sealed, []byte(magic))
	if err != nil {
		return nil, ErrNotAFile
	}
	var h Header
	if err := json.Unmarshal(raw, &h); err != nil || h.Size < 0 {
		return nil, ErrNotAFile
	}
	return &Reader{r: br, aead: aead, header: h, counter: 1, buf: make([]byte, ChunkSize+tagSize)}, nil
}

// Header is what the file says about itself.
func (fr *Reader) Header() Header { return fr.header }

// ErrTruncated is a file that ended before its last chunk.
var ErrTruncated = errors.New("the file ended before its last part: it is incomplete")

// ErrDamaged is a chunk that did not open.
var ErrDamaged = errors.New("part of the file is damaged or was altered")

func (fr *Reader) Read(p []byte) (int, error) {
	for len(fr.plain) == 0 {
		if fr.done {
			return 0, io.EOF
		}
		if err := fr.next(); err != nil {
			return 0, err
		}
	}
	n := copy(p, fr.plain)
	fr.plain = fr.plain[n:]
	return n, nil
}

func (fr *Reader) next() error {
	n, err := io.ReadFull(fr.r, fr.buf)
	switch {
	case err == io.EOF:
		return ErrTruncated
	case err == io.ErrUnexpectedEOF:
		// A short chunk can only be the last.
	case err != nil:
		return err
	}
	last := n < len(fr.buf)
	if !last {
		if _, perr := fr.r.Peek(1); perr == io.EOF {
			last = true
		} else if perr != nil {
			return perr
		}
	}
	plain, oerr := fr.aead.Open(fr.buf[:0:0], nonce(fr.counter, last), fr.buf[:n], []byte(magic))
	if oerr != nil {
		if last {
			// Either damaged, or cut exactly at a chunk boundary: a chunk
			// sealed as not-last ended the stream.
			if _, err := fr.aead.Open(nil, nonce(fr.counter, false), fr.buf[:n], []byte(magic)); err == nil {
				return ErrTruncated
			}
		}
		return ErrDamaged
	}
	fr.counter++
	fr.got += int64(len(plain))
	if fr.got > fr.header.Size || (last && fr.got != fr.header.Size) {
		return ErrDamaged
	}
	fr.plain, fr.done = plain, last
	return nil
}
