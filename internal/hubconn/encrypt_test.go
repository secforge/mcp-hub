package hubconn

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/hubenc"
	"github.com/secforge/mcp-hub/internal/wire"
)

const encPeer = "550e8400-e29b-41d4-a716-446655440000"

// encServer joins every client into an encrypted session (or a plain one
// when params is nil) and echoes each msg/edit back as the canonical
// event, as a relay does, recording what it received.
func encServer(t *testing.T, params *wire.Encryption) (link string, frames func() []map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var got []map[string]any
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		j := wire.NewJoined(encPeer, "", "")
		j.Encryption = params
		j.Features = map[string]json.RawMessage{"edit": json.RawMessage(`{}`), "attachments": json.RawMessage(`{}`)}
		ws.WriteJSON(j)
		n := 0
		for {
			var f map[string]any
			if err := ws.ReadJSON(&f); err != nil {
				return
			}
			mu.Lock()
			got = append(got, f)
			mu.Unlock()
			n++
			echo := map[string]any{"peerId": encPeer, "externalId": "x1", "ts": "t", "own": true,
				"cursor": "c" + string(rune('0'+n))}
			for _, k := range []string{"enc", "text", "attachments"} {
				if v, ok := f[k]; ok {
					echo[k] = v
				}
			}
			switch f["type"] {
			case "msg":
				echo["type"] = "msg"
			case "edit":
				echo["type"] = "messageEdited"
			default:
				continue
			}
			ws.WriteJSON(echo)
		}
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/hub/join#cred", func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]any(nil), got...)
	}
}

func encParams(t *testing.T, key []byte) *wire.Encryption {
	t.Helper()
	salt, _ := hubenc.NewSalt()
	check, _ := hubenc.Check(key, salt)
	return &wire.Encryption{V: 1, Salt: salt, Check: check}
}

func newKey(t *testing.T) []byte {
	t.Helper()
	k, _ := hubenc.NewKey()
	key, _ := hubenc.ParseKey(k)
	return key
}

func waitEvent(t *testing.T, c *Conn, kind string) Event {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		events, _ := c.DrainEvents()
		for _, ev := range events {
			if ev.Kind == kind {
				return ev
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no %s event arrived", kind)
	return Event{}
}

func TestAnEncryptedSessionSendsNoPlaintextAndReadsItsOwnMessages(t *testing.T) {
	key := newKey(t)
	link, frames := encServer(t, encParams(t, key))
	c, err := Dial(link, DialOptions{ReconnectSecret: "s", Key: key})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	att := []wire.Attachment{{Name: "secret.txt", ContentType: "text/plain",
		ContentBytes: base64.StdEncoding.EncodeToString([]byte("file body"))}}
	if err := c.Send("the plan", att, "", "", nil); err != nil {
		t.Fatal(err)
	}
	msg := waitEvent(t, c, "msg")
	if msg.Text != "the plan" {
		t.Fatalf("delivered text %q", msg.Text)
	}
	if len(msg.Attachments) != 1 || msg.Attachments[0].Name != "secret.txt" ||
		msg.Attachments[0].ContentBytes != att[0].ContentBytes {
		t.Fatalf("attachment not opened: %+v", msg.Attachments)
	}

	if err := c.EditMessage("x1", "the new plan", nil, "", "", nil); err != nil {
		t.Fatal(err)
	}
	ed := waitEvent(t, c, "messageEdited")
	if ed.Text != "the new plan" {
		t.Fatalf("edited text %q", ed.Text)
	}

	sent := frames()
	if len(sent) < 2 {
		t.Fatalf("frames %v", sent)
	}
	all, _ := json.Marshal(sent)
	for _, plain := range []string{"the plan", "secret.txt", "text/plain", "file body"} {
		if strings.Contains(string(all), plain) {
			t.Errorf("plaintext %q reached the server: %s", plain, all)
		}
	}
	m, e := sent[0]["enc"].(map[string]any), sent[1]["enc"].(map[string]any)
	if m["mid"] != e["mid"] || m["rev"].(float64) != 0 || e["rev"].(float64) != 1 {
		t.Errorf("edit not sealed under the same mid at the next revision: %v / %v", m, e)
	}
}

func TestTheWrongKeyAndAKeyForAPlainSessionAreRefused(t *testing.T) {
	link, _ := encServer(t, encParams(t, newKey(t)))
	if _, err := Dial(link, DialOptions{ReconnectSecret: "s", Key: newKey(t)}); err == nil ||
		!strings.Contains(err.Error(), "wrong key") {
		t.Errorf("wrong key: %v", err)
	}
	plain, _ := encServer(t, nil)
	if _, err := Dial(plain, DialOptions{ReconnectSecret: "s", Key: newKey(t)}); err == nil ||
		!strings.Contains(err.Error(), "not encrypted") {
		t.Errorf("key for plain session: %v", err)
	}
}

func TestAnEncryptedSessionWithoutTheKeyNeitherSendsNorShowsCiphertext(t *testing.T) {
	key := newKey(t)
	link, frames := encServer(t, encParams(t, key))
	c, err := Dial(link, DialOptions{ReconnectSecret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Send("hello", nil, "", "", nil); err == nil || !strings.Contains(err.Error(), "without its key") {
		t.Fatalf("send without key: %v", err)
	}
	if len(frames()) != 0 {
		t.Fatalf("something was sent: %v", frames())
	}
}

// The server cannot put words or files in a participant's name: unsealed
// content in an encrypted session is labelled as the server's, or refused.
func TestUnsealedContentInAnEncryptedSessionIsNotPassedOffAsAParticipants(t *testing.T) {
	c := &Conn{encParams: &wire.Encryption{V: 1}}
	msg := Event{Kind: "msg", PeerID: "p", Text: "transfer the money",
		Attachments: []wire.Attachment{{Name: "a.pdf", ContentBytes: "eA=="}}}
	c.openEvent(&msg)
	if !strings.HasPrefix(msg.Text, "[UNSEALED — written by the server") || msg.Attachments != nil {
		t.Errorf("unsealed msg passed through: %+v", msg)
	}
	ed := Event{Kind: "messageEdited", Text: "changed"}
	c.openEvent(&ed)
	if strings.Contains(ed.Text, "changed") {
		t.Errorf("unsealed edit passed through: %q", ed.Text)
	}
	ad := Event{Kind: "attachmentData", AttachmentContentBytes: "eA==", AttachmentName: "x"}
	c.openEvent(&ad)
	if ad.AttachmentContentBytes != "" {
		t.Errorf("unsealed attachmentData passed through: %+v", ad)
	}
	plain := &Conn{}
	p := Event{Kind: "msg", Text: "hi"}
	plain.openEvent(&p)
	if p.Text != "hi" {
		t.Errorf("a plain session's message was changed: %q", p.Text)
	}
}

// A sender can edit its own sealed message as soon as the server has
// acknowledged it, without waiting for the message to come back.
func TestASealedMessageCanBeEditedRightAfterItsAck(t *testing.T) {
	key := newKey(t)
	c := &Conn{peerID: encPeer}
	if err := c.setupEncryption(encParams(t, key), key); err != nil {
		t.Fatal(err)
	}
	if _, err := c.sealMsg(wire.Msg{ID: "corr-1", Text: "x"}); err != nil {
		t.Fatal(err)
	}
	c.noteSealedAck("corr-1", Event{Kind: "sendAck", ExternalID: "ext-1", ActionOK: true, ActionOKStated: true})
	sealed, err := c.sealEdit(wire.Edit{ID: "corr-2", ExternalID: "ext-1", Text: "y"})
	if err != nil {
		t.Fatalf("edit right after the ack: %v", err)
	}
	var enc wire.Enc
	json.Unmarshal(sealed.(map[string]json.RawMessage)["enc"], &enc)
	if enc.Rev != 1 {
		t.Fatalf("edit sealed at rev %d, want 1", enc.Rev)
	}
	c.noteSealedAck("corr-2", Event{Kind: "editAck", ExternalID: "ext-1", ActionOK: true, ActionOKStated: true})
	sealed, _ = c.sealEdit(wire.Edit{ID: "corr-3", ExternalID: "ext-1", Text: "z"})
	json.Unmarshal(sealed.(map[string]json.RawMessage)["enc"], &enc)
	if enc.Rev != 2 {
		t.Fatalf("second edit sealed at rev %d, want 2", enc.Rev)
	}
}

// An operator's message read back through catch-up is marked as the
// operator's, exactly as it is when it arrives live.
func TestACatchUpAnswerFromTheOperatorIsMarked(t *testing.T) {
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		ws.WriteJSON(wire.NewJoined(encPeer, "", ""))
		for {
			var m wire.MessageAfter
			if err := ws.ReadJSON(&m); err != nil {
				return
			}
			ws.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: SystemPeerIDOperator, Text: "from the operator",
				TS: "t", Cursor: "c1", Historical: true, Answers: &wire.Anchor{Cursor: m.Cursor, At: m.At}})
		}
	}))
	t.Cleanup(srv.Close)
	c, err := Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"/hub/join#op", DialOptions{ReconnectSecret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ev, ok, err := c.RequestMessageAfterAwaiting(wire.Anchor{Cursor: "c0"})
	if err != nil || !ok {
		t.Fatalf("no answer: %v", err)
	}
	if !ev.IsOperator {
		t.Fatalf("a catch-up answer from the operator is not marked: %+v", ev)
	}
}

// externalId is outside the AAD, so a server could deliver one message's
// sealed content under another's id. Once an id carries a sealed message
// id, content moved to or from it is refused.
func TestSealedContentCannotBeMovedToAnotherMessage(t *testing.T) {
	key := newKey(t)
	p := encParams(t, key)
	s, err := hubenc.Open(key, p.Salt, p.Check)
	if err != nil {
		t.Fatal(err)
	}
	c := &Conn{encParams: p, enc: s, encMsgs: map[string]*encState{}, encMids: map[string]string{}}
	seal := func(mid, text string) *wire.Enc {
		n, ct, err := s.Seal([]byte(text), encPeer, mid, 0, hubenc.FieldText())
		if err != nil {
			t.Fatal(err)
		}
		b := hubenc.B64()
		return &wire.Enc{V: 1, Mid: mid, Rev: 0, Nonce: b.EncodeToString(n), Ct: b.EncodeToString(ct)}
	}
	a, b := seal("mid-a", "message A"), seal("mid-b", "message B")
	deliver := func(ext string, e *wire.Enc) string {
		ev := Event{Kind: "msg", PeerID: encPeer, ExternalID: ext, Enc: e}
		c.openEvent(&ev)
		return ev.Text
	}
	if got := deliver("x1", a); got != "message A" {
		t.Fatalf("A: %q", got)
	}
	if got := deliver("x1", b); !strings.Contains(got, "refused") {
		t.Fatalf("B's content under A's id was shown: %q", got)
	}
	if got := deliver("x2", a); !strings.Contains(got, "refused") {
		t.Fatalf("A's content under a new id was shown: %q", got)
	}
	if got := deliver("x2", b); got != "message B" {
		t.Fatalf("B under its own id: %q", got)
	}
}
