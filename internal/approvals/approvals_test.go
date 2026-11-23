package approvals

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
)

// vectors are chat-relay's, copied from its docs/approval-vectors.json.
type vectors struct {
	Seed        string `json:"ed25519SeedHex"`
	AgeIdentity string `json:"ageIdentity"`
	KeyLine     string `json:"keyLine"`
	Fingerprint string `json:"fingerprint"`
	Request     struct {
		Exact string `json:"exact"`
		Hash  string `json:"hash"`
	} `json:"request"`
	Statements []struct {
		Statement string `json:"statement"`
		Signature string `json:"signature"`
	} `json:"statements"`
	Negatives []struct {
		Case      string `json:"case"`
		Statement string `json:"statement"`
		Signature string `json:"signature"`
		Reason    string `json:"reason"`
	} `json:"negatives"`
}

func loadVectors(t *testing.T) vectors {
	t.Helper()
	raw, err := os.ReadFile("testdata/approval-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestKeyLineAndFingerprintMatchTheVectors(t *testing.T) {
	v := loadVectors(t)
	k, err := ParseKeyLine(v.KeyLine)
	if err != nil {
		t.Fatal(err)
	}
	seed, _ := hex.DecodeString(v.Seed)
	pub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	if k.SignKey != b64.EncodeToString(pub) || k.Fingerprint != v.Fingerprint || k.Label != "Test-Phone" {
		t.Fatalf("key %+v", k)
	}
	if Hash([]byte(v.Request.Exact)) != v.Request.Hash {
		t.Fatal("request hash differs")
	}
	for _, bad := range []string{
		"chat-relay-approver v2 Phone " + k.SignKey + " " + k.AgeRecipient,
		"chat-relay-approver v1 Ph/one " + k.SignKey + " " + k.AgeRecipient,
		"chat-relay-approver v1 Phone AAAA " + k.AgeRecipient,
		"chat-relay-approver v1 Phone " + k.SignKey + " age1nope",
	} {
		if _, err := ParseKeyLine(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestEveryValidVectorIsAcceptedAndEveryNegativeRejected(t *testing.T) {
	v := loadVectors(t)
	k, _ := ParseKeyLine(v.KeyLine)
	keys := []Key{k}
	for i, s := range v.Statements {
		st, signer, reason := Check(s.Statement, s.Signature, []byte(v.Request.Exact), keys)
		if reason != "" || signer.SignKey != k.SignKey {
			t.Errorf("valid %d rejected: %s", i, reason)
		}
		if st.Answer == "text" && !strings.Contains(st.Text, " ") {
			t.Errorf("valid %d lost U+2028: %q", i, st.Text)
		}
	}
	if len(v.Negatives) < 8 {
		t.Fatalf("only %d negatives", len(v.Negatives))
	}
	for _, n := range v.Negatives {
		if _, _, reason := Check(n.Statement, n.Signature, []byte(v.Request.Exact), keys); reason == "" {
			t.Errorf("%s: accepted, want %s", n.Case, n.Reason)
		}
	}
}

func TestAnAnswerFromAPhoneNotInTheFilesIsRejected(t *testing.T) {
	v := loadVectors(t)
	other := otherKey(t)
	s := v.Statements[0]
	if _, _, reason := Check(s.Statement, s.Signature, []byte(v.Request.Exact), []Key{other}); reason != BadSignature {
		t.Fatalf("reason %q", reason)
	}
}

func otherKey(t *testing.T) Key {
	t.Helper()
	pub, _, _ := ed25519.GenerateKey(nil)
	id, _ := age.GenerateX25519Identity()
	k, err := ParseKeyLine("chat-relay-approver v1 Other " + b64.EncodeToString(pub) + " " + id.Recipient().String())
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestASealedRequestOpensWithThePhoneIdentityOnly(t *testing.T) {
	v := loadVectors(t)
	k, _ := ParseKeyLine(v.KeyLine)
	plain := []byte(`{"text":"<b>&amp;</b>  "}`)
	env, err := Seal(plain, []Key{k, otherKey(t)})
	if err != nil || !strings.HasPrefix(env, "-----BEGIN AGE ENCRYPTED FILE-----") {
		t.Fatalf("seal: %v %q", err, env)
	}
	phone, _ := age.ParseX25519Identity(v.AgeIdentity)
	got, err := Open(env, phone, 1<<16)
	if err != nil || string(got) != string(plain) {
		t.Fatalf("open: %v %q", err, got)
	}
	stranger, _ := age.GenerateX25519Identity()
	if _, err := Open(env, stranger, 1<<16); err == nil {
		t.Fatal("a stranger opened it")
	}
	if _, err := Open(env, phone, 4); err == nil {
		t.Fatal("the size limit did not hold")
	}
}

func TestARequestEncodesWithSortedKeysAndLiteralHTML(t *testing.T) {
	r := Request{Asker: Asker{AgeRecipient: "age1x", Name: "finance"}, Choices: Choices,
		CreatedAt: "2026-11-23T07:00:00Z", ExpiresAt: "2026-11-23T08:00:00Z", RequestID: "req-0001",
		Text: "a <b> & c", V: 1}
	got, _ := r.Encode()
	want := `{"asker":{"ageRecipient":"age1x","name":"finance"},"choices":["yes","no"],"createdAt":"2026-11-23T07:00:00Z","expiresAt":"2026-11-23T08:00:00Z","requestId":"req-0001","text":"a <b> & c","v":1}`
	if string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	if !ValidRequestID(NewRequestID()) {
		t.Fatal("a new request id has the wrong shape")
	}
}

func TestApproverFilesAreStrictAndStateIsPrivate(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_CONNSTORE_DIR", dir)
	v := loadVectors(t)
	os.MkdirAll(ApproversDir(), 0o700)
	os.WriteFile(ApproverFile("steffen"), []byte("# phones\n\n"+v.KeyLine+"\n"), 0o600)
	keys, err := LoadApprover("steffen")
	if err != nil || len(keys) != 1 {
		t.Fatalf("load: %v %d", err, len(keys))
	}
	os.WriteFile(ApproverFile("typo"), []byte(v.KeyLine+"x\n"), 0o600)
	if _, err := LoadApprover("typo"); err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("typo: %v", err)
	}
	if _, err := LoadApprover("../steffen"); err == nil {
		t.Fatal("a path was accepted as a name")
	}
	if name, _, ok := FindSigner(keys[0].SignKey); !ok || name != "steffen" {
		t.Fatalf("signer %q %v", name, ok)
	}

	now := time.Date(2026, 11, 23, 7, 0, 0, 0, time.UTC)
	err = Update("/p", now, func(st *State) error {
		st.Requests["r1"] = &Entry{RequestID: "r1", State: StateOpen, ExpiresAt: "2026-11-23T08:00:00Z"}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(statePath("/p"))
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", info.Mode())
	}
	var identity string
	Update("/p", now.Add(2*time.Hour), func(st *State) error {
		identity = st.Identity
		if st.Requests["r1"].State != StateExpired {
			t.Errorf("state %s", st.Requests["r1"].State)
		}
		return nil
	})
	Update("/p", now.Add(40*24*time.Hour), func(st *State) error {
		if st.Identity != identity || st.Requests["r1"] != nil {
			t.Errorf("identity kept %v, r1 %v", st.Identity == identity, st.Requests["r1"])
		}
		return nil
	})
	if _, err := os.Stat(filepath.Join(dir, "approvals")); err != nil {
		t.Fatal(err)
	}
}

// A request or statement that could read differently to the phone (JSON
// is case-sensitive there, last duplicate wins) and to this client is
// refused before anything in it is believed.
func TestAmbiguousJSONIsRefusedEvenWhenSigned(t *testing.T) {
	v := loadVectors(t)
	k, _ := ParseKeyLine(v.KeyLine)
	seed, _ := hex.DecodeString(v.Seed)
	priv := ed25519.NewKeyFromSeed(seed)
	exact := v.Request.Exact
	for name, request := range map[string]string{
		"case variant": strings.Replace(exact, `"text":`, `"Text":"wire 10000 EUR","text":`, 1),
		"duplicate":    strings.Replace(exact, `"text":`, `"text":"wire 10000 EUR","text":`, 1),
		"unknown key":  strings.Replace(exact, `"text":`, `"extra":1,"text":`, 1),
		"nested case":  strings.Replace(exact, `"name":"finance"`, `"name":"finance","Name":"office"`, 1),
		"text choice":  strings.Replace(exact, `"choices":["yes","no"]`, `"choices":["yes","text"]`, 1),
		"no choices":   strings.Replace(exact, `"choices":["yes","no"]`, `"choices":[]`, 1),
	} {
		var st Statement
		json.Unmarshal([]byte(v.Statements[0].Statement), &st)
		st.RequestHash = Hash([]byte(request))
		raw, _ := json.Marshal(st)
		sig := b64.EncodeToString(ed25519.Sign(priv, append([]byte(SigningPrefix), raw...)))
		if _, _, reason := Check(string(raw), sig, []byte(request), []Key{k}); reason != MalformedRequest {
			t.Errorf("%s: reason %q", name, reason)
		}
	}
	dup := strings.Replace(v.Statements[0].Statement, `"answer":"yes"`, `"answer":"no","answer":"yes"`, 1)
	sig := b64.EncodeToString(ed25519.Sign(priv, append([]byte(SigningPrefix), dup...)))
	if _, _, reason := Check(dup, sig, []byte(exact), []Key{k}); reason != Malformed {
		t.Errorf("duplicate answer: reason %q", reason)
	}
	if _, err := ParseProof([]byte(`{"request":"a","Request":"b","signature":"c","statement":"d"}`)); err == nil {
		t.Error("an ambiguous proof was read")
	}
}
