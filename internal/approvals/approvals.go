// Package approvals implements the client side of signed approvals: an
// agent asks a person, the person answers on a paired phone, and the
// answer is an Ed25519 signature by a key that exists only on that phone.
// This client trusts only the phone keys it finds in its own approver
// files, never a key a server or a peer names.
//
// What is hashed and signed is always the exact bytes that travelled;
// nothing is re-serialised before it is checked, because JSON encoders
// disagree on U+2028/2029, <>& and lone surrogates.
package approvals

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"filippo.io/age"
	"filippo.io/age/armor"
)

// SigningPrefix is put in front of a statement before it is signed, so a
// phone key signs nothing that is valid anywhere else.
const SigningPrefix = "chat-relay-approval v1\n"

// keyLinePrefix starts every approver key line a phone shows.
const keyLinePrefix = "chat-relay-approver"

// Choices are the quick answers a request offers when the agent names none.
var Choices = []string{"yes", "no"}

// Bounds on an agent's own choices (wire revision 3): the phone shows them as
// buttons.
const (
	MaxChoices      = 8
	MaxChoiceLength = 60
)

var b64 = base64.RawURLEncoding

var (
	labelPattern     = regexp.MustCompile(`^[A-Za-z0-9._-]{1,40}$`)
	requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{8,64}$`)
)

// Key is one phone of an approver, from a key line.
type Key struct {
	Label        string
	SignKey      string
	AgeRecipient string
	Fingerprint  string
	pub          ed25519.PublicKey
	recipient    *age.X25519Recipient
}

// ParseKeyLine reads "chat-relay-approver v1 <label> <signKey> <ageRecipient>".
func ParseKeyLine(line string) (Key, error) {
	f := strings.Fields(line)
	if len(f) != 5 || f[0] != keyLinePrefix || f[1] != "v1" {
		return Key{}, errors.New(`not a key line of the form "chat-relay-approver v1 <label> <signKey> <ageRecipient>"`)
	}
	label, signKey, recipient := f[2], f[3], f[4]
	if !labelPattern.MatchString(label) {
		return Key{}, fmt.Errorf("label %q is not 1 to 40 of A-Z a-z 0-9 . _ -", label)
	}
	pub, err := b64.DecodeString(signKey)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return Key{}, errors.New("the signing key is not 32 bytes of base64url")
	}
	r, err := age.ParseX25519Recipient(recipient)
	if err != nil {
		return Key{}, fmt.Errorf("the age recipient is malformed: %v", err)
	}
	return Key{Label: label, SignKey: signKey, AgeRecipient: recipient,
		Fingerprint: Fingerprint(signKey, recipient), pub: pub, recipient: r}, nil
}

// Fingerprint names a phone to the server: the first 22 characters of
// base64url(SHA-256("v1 " + signKey + " " + ageRecipient)).
func Fingerprint(signKey, ageRecipient string) string {
	sum := sha256.Sum256([]byte("v1 " + signKey + " " + ageRecipient))
	return b64.EncodeToString(sum[:])[:22]
}

// Asker is how the asking client names itself and where an answer is
// encrypted to.
type Asker struct {
	AgeRecipient string `json:"ageRecipient"`
	Name         string `json:"name"`
}

// Request is a request's plaintext. Fields are declared in key order, so
// it encodes with its keys sorted.
type Request struct {
	Actors  []string `json:"actors,omitempty"`
	Asker   Asker    `json:"asker"`
	Choices []string `json:"choices"`
	// Comments says this client reads a comment beside a choice (wire
	// revision 3); without it the phone attaches none.
	Comments  bool   `json:"comments,omitempty"`
	CreatedAt string `json:"createdAt"`
	ExpiresAt string `json:"expiresAt"`
	RequestID string `json:"requestId"`
	Text      string `json:"text"`
	V         int    `json:"v"`
}

// Approver is a statement's signer as the statement names it.
type Approver struct {
	Label   string `json:"label"`
	SignKey string `json:"signKey"`
}

// Statement is what a phone signs.
type Statement struct {
	Answer      string   `json:"answer"`
	Approver    Approver `json:"approver"`
	Asker       Asker    `json:"asker"`
	ExpiresAt   string   `json:"expiresAt"`
	RequestHash string   `json:"requestHash"`
	RequestID   string   `json:"requestId"`
	SignedAt    string   `json:"signedAt"`
	Text        string   `json:"text,omitempty"`
	V           int      `json:"v"`
}

// Signed is an answer envelope's plaintext: the statement as the exact
// string the phone signed, and the signature.
type Signed struct {
	Signature string `json:"signature"`
	Statement string `json:"statement"`
}

// Proof is an answer passed on to another agent: the request and the
// statement as their exact strings, and the signature.
type Proof struct {
	Request   string `json:"request"`
	Signature string `json:"signature"`
	Statement string `json:"statement"`
}

// NewRequestID returns a fresh request id.
func NewRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "r" + b64.EncodeToString(b[:])
}

// ValidRequestID reports whether id has the shape a request id must have.
func ValidRequestID(id string) bool { return requestIDPattern.MatchString(id) }

// Encode returns a request's plaintext bytes: keys sorted, no whitespace,
// <>& written as themselves.
func (r Request) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(r); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// Hash is a request's hash as a statement names it.
func Hash(plain []byte) string {
	sum := sha256.Sum256(plain)
	return b64.EncodeToString(sum[:])
}

// Seal encrypts plain to every key's age recipient, ASCII-armored.
func Seal(plain []byte, keys []Key) (string, error) {
	if len(keys) == 0 {
		return "", errors.New("no recipients")
	}
	recipients := make([]age.Recipient, 0, len(keys))
	for _, k := range keys {
		recipients = append(recipients, k.recipient)
	}
	var buf bytes.Buffer
	aw := armor.NewWriter(&buf)
	w, err := age.Encrypt(aw, recipients...)
	if err != nil {
		return "", err
	}
	if _, err := w.Write(plain); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}
	if err := aw.Close(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// Open decrypts an armored envelope with identity, reading at most max
// bytes of plaintext.
func Open(envelope string, identity *age.X25519Identity, max int) ([]byte, error) {
	r, err := age.Decrypt(armor.NewReader(strings.NewReader(envelope)), identity)
	if err != nil {
		return nil, err
	}
	plain, err := io.ReadAll(io.LimitReader(r, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(plain) > max {
		return nil, fmt.Errorf("the plaintext is over %d bytes", max)
	}
	return plain, nil
}

// Reason is why an answer was rejected.
type Reason string

const (
	BadSignature     Reason = "the signature does not verify with any phone key of this approver in this machine's own files"
	Malformed        Reason = "the statement is not a well-formed version 1 statement"
	MalformedRequest Reason = "the request is not a well-formed version 1 request (keys must be exact, once each)"
	WrongRequestHash Reason = "it answers a request with different content"
	WrongRequest     Reason = "it names a different request"
	WrongAsker       Reason = "it names a different asker"
	NotOffered       Reason = "the answer is neither one of the request's choices nor non-empty free text"
	SignedAfter      Reason = "it was signed after the request expired"
)

// Check verifies a signed statement against the exact request plaintext
// and the phone keys this machine trusts, and returns the statement and
// the key that signed it. It does not judge whether the request is still
// open or whether the asker is this client: that is the caller's.
func Check(statement, signature string, request []byte, keys []Key) (Statement, Key, Reason) {
	sig, err := b64.DecodeString(signature)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return Statement{}, Key{}, BadSignature
	}
	msg := append([]byte(SigningPrefix), statement...)
	var signer Key
	found := false
	for _, k := range keys {
		if ed25519.Verify(k.pub, msg, sig) {
			signer, found = k, true
			break
		}
	}
	if !found {
		return Statement{}, Key{}, BadSignature
	}
	var st Statement
	if err := strictUnmarshal([]byte(statement), statementShape, &st); err != nil || st.V != 1 {
		return Statement{}, Key{}, Malformed
	}
	if st.Approver.SignKey != signer.SignKey {
		return Statement{}, Key{}, BadSignature
	}
	if st.RequestHash != Hash(request) {
		return Statement{}, Key{}, WrongRequestHash
	}
	var rq Request
	if err := strictUnmarshal(request, requestShape, &rq); err != nil || rq.V != 1 || ValidateChoices(rq.Choices) != nil {
		return Statement{}, Key{}, MalformedRequest
	}
	if st.RequestID != rq.RequestID || st.ExpiresAt != rq.ExpiresAt {
		return Statement{}, Key{}, WrongRequest
	}
	if st.Asker != rq.Asker {
		return Statement{}, Key{}, WrongAsker
	}
	if st.Answer == "text" {
		if st.Text == "" {
			return Statement{}, Key{}, NotOffered
		}
	} else if !slices.Contains(rq.Choices, st.Answer) {
		return Statement{}, Key{}, NotOffered
	} else if st.Text != "" && !rq.Comments {
		// A comment beside a choice only where this client said it reads one
		// (revision 3); the empty string is how an absent one decodes.
		return Statement{}, Key{}, NotOffered
	}
	signedAt, err1 := time.Parse(time.RFC3339, st.SignedAt)
	expiresAt, err2 := time.Parse(time.RFC3339, st.ExpiresAt)
	if err1 != nil || err2 != nil {
		return Statement{}, Key{}, Malformed
	}
	if signedAt.After(expiresAt) {
		return Statement{}, Key{}, SignedAfter
	}
	return st, signer, ""
}

// ValidateChoices: 1 to MaxChoices distinct strings of 1 to MaxChoiceLength
// characters, never "text", which names a free-text answer (revision 3).
func ValidateChoices(choices []string) error {
	if len(choices) == 0 || len(choices) > MaxChoices {
		return fmt.Errorf("offer 1 to %d choices", MaxChoices)
	}
	seen := map[string]bool{}
	for _, c := range choices {
		switch {
		case c == "":
			return fmt.Errorf("an empty choice")
		case c == "text":
			return fmt.Errorf(`"text" cannot be a choice: it names a free-text answer`)
		case utf8.RuneCountInString(c) > MaxChoiceLength:
			return fmt.Errorf("choice %q is longer than %d characters", c, MaxChoiceLength)
		case strings.IndexFunc(c, unprintable) >= 0:
			// A choice is echoed into notices and proof checks other agents
			// read: no line breaks, control or invisible format characters
			// that could forge a line there (security review, 2026-11-28).
			return fmt.Errorf("choice %q holds a character other than letters, digits, punctuation, symbols or spaces", c)
		case seen[c]:
			return fmt.Errorf("choice %q twice", c)
		}
		seen[c] = true
	}
	return nil
}

// AnswerText is a verified answer as one phrase: the choice ("yes", "no",
// or the agent's own), with the person's comment beside it if they gave one,
// or the person's free text in quotes.
func AnswerText(st Statement) string {
	if st.Answer == "text" {
		return fmt.Sprintf("free text: %q", st.Text)
	}
	// Only yes and no stand bare; any other choice is the agent's own and is
	// quoted, so it can never read as a bare yes.
	answer := st.Answer
	if answer != "yes" && answer != "no" {
		answer = fmt.Sprintf("%q", answer)
	}
	if st.Text != "" {
		// A comment beside a choice (revision 3): reported with it, never dropped.
		return fmt.Sprintf("%s, comment: %q", answer, st.Text)
	}
	return answer
}

// unprintable: anything but letters, marks, digits, punctuation, symbols and
// the plain space -- an allow-list, so line and paragraph separators,
// control, format, private-use and unassigned characters are all out
// (security review, 2026-11-28).
func unprintable(r rune) bool {
	return !unicode.IsPrint(r)
}

// ParseSigned reads an answer envelope's plaintext strictly.
func ParseSigned(plain []byte) (Signed, error) {
	var s Signed
	err := strictUnmarshal(plain, signedShape, &s)
	return s, err
}

// ParseProof reads a proof strictly.
func ParseProof(raw []byte) (Proof, error) {
	var p Proof
	err := strictUnmarshal(raw, proofShape, &p)
	return p, err
}

// ParseRequest reads a request plaintext strictly.
func ParseRequest(plain []byte) (Request, error) {
	var r Request
	err := strictUnmarshal(plain, requestShape, &r)
	return r, err
}

// ParseStatement reads a statement strictly.
func ParseStatement(statement string) (Statement, error) {
	var st Statement
	err := strictUnmarshal([]byte(statement), statementShape, &st)
	return st, err
}
