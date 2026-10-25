package hubconn

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/secforge/mcp-hub/internal/hubenc"
	"github.com/secforge/mcp-hub/internal/wire"
)

// v2Message is a message as a v2 writer sends it: the body sealed under
// text:{n} for its n attachments, each attachment sealed as in v1.
func v2Message(t *testing.T, w *Conn, mid, text string, files []string) Event {
	t.Helper()
	b := hubenc.B64()
	nonce, ct, err := w.enc.Seal([]byte(text), w.peerID, mid, 0, hubenc.FieldTextCount(len(files)))
	if err != nil {
		t.Fatal(err)
	}
	ev := Event{Kind: "msg", ExternalID: "x-" + mid, PeerID: w.peerID,
		Enc: &wire.Enc{V: 2, Mid: mid, Nonce: b.EncodeToString(nonce), Ct: b.EncodeToString(ct)}}
	for i, name := range files {
		n, c, _ := w.enc.Seal([]byte("content of "+name), w.peerID, mid, 0, hubenc.FieldFile(i))
		meta, _ := json.Marshal(map[string]string{"name": name, "contentType": "text/plain"})
		mn, mc, _ := w.enc.Seal(meta, w.peerID, mid, 0, hubenc.FieldFileMeta(i))
		ev.Attachments = append(ev.Attachments, wire.Attachment{
			ContentBytes: base64.StdEncoding.EncodeToString(append(n, c...)),
			EncMeta:      &wire.EncValue{Nonce: b.EncodeToString(mn), Ct: b.EncodeToString(mc)}})
	}
	return ev
}

func TestAV2BodyOpensOnlyWithEveryAttachmentItWasSentWith(t *testing.T) {
	key := newKey(t)
	params := encParams(t, key)
	writer, reader := &Conn{peerID: webPeerID}, &Conn{peerID: agentPeerID}
	for _, c := range []*Conn{writer, reader} {
		if err := c.setupEncryption(params, key); err != nil {
			t.Fatal(err)
		}
	}
	ev := v2Message(t, writer, "m1", "two files", []string{"a.txt", "b.txt"})
	reader.openEvent(&ev)
	if ev.Text != "two files" || len(ev.Attachments) != 2 || ev.Attachments[1].Name != "b.txt" {
		t.Fatalf("a whole v2 message opened as %q with %+v", ev.Text, ev.Attachments)
	}

	dropped := v2Message(t, writer, "m2", "two files", []string{"a.txt", "b.txt"})
	dropped.Attachments = dropped.Attachments[:1]
	reader.openEvent(&dropped)
	if !strings.Contains(dropped.Text, "could not be opened") {
		t.Fatalf("a v2 message missing an attachment opened: %q", dropped.Text)
	}

	relabelled := v2Message(t, writer, "m3", "no files", nil)
	relabelled.Enc.V = 1
	reader.openEvent(&relabelled)
	if !strings.Contains(relabelled.Text, "could not be opened") {
		t.Fatalf("a v2 body relabelled v1 opened: %q", relabelled.Text)
	}

	future := v2Message(t, writer, "m4", "x", nil)
	future.Enc.V = 3
	reader.openEvent(&future)
	if !strings.Contains(future.Text, "envelope version 3") {
		t.Fatalf("an unknown envelope version: %q", future.Text)
	}
}

func TestAV2EditKeepsTheCountOfTheOriginalSend(t *testing.T) {
	key := newKey(t)
	params := encParams(t, key)
	writer, reader := &Conn{peerID: webPeerID}, &Conn{peerID: agentPeerID}
	for _, c := range []*Conn{writer, reader} {
		if err := c.setupEncryption(params, key); err != nil {
			t.Fatal(err)
		}
	}
	orig := v2Message(t, writer, "m1", "with a file", []string{"a.txt"})
	reader.openEvent(&orig)
	b := hubenc.B64()
	nonce, ct, _ := writer.enc.Seal([]byte("edited"), writer.peerID, "m1", 1, hubenc.FieldTextCount(1))
	edit := Event{Kind: "messageEdited", ExternalID: orig.ExternalID, PeerID: writer.peerID,
		Enc: &wire.Enc{V: 2, Mid: "m1", Rev: 1, Nonce: b.EncodeToString(nonce), Ct: b.EncodeToString(ct)}}
	reader.openEvent(&edit)
	if edit.Text != "edited" {
		t.Fatalf("a v2 edit without its attachments on the wire: %q", edit.Text)
	}
}
