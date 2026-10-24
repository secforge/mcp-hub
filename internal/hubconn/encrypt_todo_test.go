package hubconn

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/secforge/mcp-hub/internal/wire"
)

const (
	agentPeerID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	webPeerID   = "ffffffff-ffff-ffff-ffff-ffffffffffff"
)

// todoConn is a client of an encrypted todo list, as peer, holding key.
func todoConn(t *testing.T, peer string, params *wire.Encryption, key []byte) *Conn {
	t.Helper()
	c := &Conn{peerID: peer, todo: &wire.TodoFeature{}}
	if err := c.setupEncryption(params, key); err != nil {
		t.Fatal(err)
	}
	return c
}

// sealedBy seals an item the way peer would, returning its envelope.
func sealedItem(t *testing.T, c *Conn, mid string, rev int, text, notes string) *wire.Enc {
	t.Helper()
	frame, _, err := c.sealItem(wire.TodoAdd{Type: wire.TypeMsg}, mid, rev, itemPlain{Text: text, Notes: notes})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := frame["text"]; ok {
		t.Fatal("the sealed frame still carries text")
	}
	if _, ok := frame["notes"]; ok {
		t.Fatal("the sealed frame still carries notes")
	}
	var e wire.Enc
	json.Unmarshal(frame["enc"], &e)
	return &e
}

func TestAnItemSealedByOnePeerOpensForAnotherWithItsNotes(t *testing.T) {
	key := newKey(t)
	params := encParams(t, key)
	web, agent := todoConn(t, webPeerID, params, key), todoConn(t, agentPeerID, params, key)
	e := sealedItem(t, web, "m1", 0, "Buy milk", "2 litres")
	ev := Event{Kind: "msg", ExternalID: "x1", PeerID: webPeerID, Enc: e,
		Todo: &wire.TodoFields{SealedBy: webPeerID}}
	agent.openEvent(&ev)
	if ev.Text != "Buy milk" || ev.Todo.Notes == nil || *ev.Todo.Notes != "2 litres" {
		t.Fatalf("opened as %q / %v", ev.Text, ev.Todo.Notes)
	}
	// The same envelope under another sealer does not open: the AAD names
	// who sealed it.
	ev = Event{Kind: "msg", ExternalID: "x2", Enc: sealedItem(t, web, "m2", 0, "a", ""),
		Todo: &wire.TodoFields{SealedBy: agentPeerID}}
	agent.openEvent(&ev)
	if !strings.Contains(ev.Text, "could not be opened") {
		t.Fatalf("an envelope opened under the wrong sealer: %q", ev.Text)
	}
}

func TestADoneOnlyChangeKeepsTheEnvelopeAndAnythingElseAtThatRevisionIsRefused(t *testing.T) {
	key := newKey(t)
	params := encParams(t, key)
	web, agent := todoConn(t, webPeerID, params, key), todoConn(t, agentPeerID, params, key)
	e := sealedItem(t, web, "m1", 3, "Buy milk", "")
	open := func(enc *wire.Enc, kind string) string {
		ev := Event{Kind: kind, ExternalID: "x1", Enc: enc, Todo: &wire.TodoFields{SealedBy: webPeerID}}
		agent.openEvent(&ev)
		return ev.Text
	}
	if got := open(e, "msg"); got != "Buy milk" {
		t.Fatalf("first delivery: %q", got)
	}
	if got := open(e, "messageEdited"); got != "Buy milk" {
		t.Fatalf("the unchanged envelope of a done-only change was refused: %q", got)
	}
	other := sealedItem(t, web, "m1", 3, "Buy beer", "")
	if got := open(other, "messageEdited"); !strings.Contains(got, "different envelope under revision 3") {
		t.Fatalf("a different envelope at the shown revision was accepted: %q", got)
	}
	older := sealedItem(t, web, "m1", 2, "Buy water", "")
	if got := open(older, "messageEdited"); !strings.Contains(got, "older than revision 3") {
		t.Fatalf("an older revision was accepted: %q", got)
	}
}

func TestAnItemsEnvelopeCannotBeMovedToAnotherItem(t *testing.T) {
	key := newKey(t)
	params := encParams(t, key)
	web, agent := todoConn(t, webPeerID, params, key), todoConn(t, agentPeerID, params, key)
	items := agent.openItems([]wire.TodoItem{
		{ExternalID: "x1", SealedBy: webPeerID, Enc: sealedItem(t, web, "m1", 0, "first", "")},
	})
	if items[0].Text != "first" || items[0].Enc != nil {
		t.Fatalf("list item opened as %+v", items[0])
	}
	moved := agent.openItems([]wire.TodoItem{
		{ExternalID: "x2", SealedBy: webPeerID, Enc: sealedItem(t, web, "m1", 1, "first, moved", "")},
	})
	if !strings.Contains(moved[0].Text, "does not belong to this item") {
		t.Fatalf("an envelope moved to another item was opened: %q", moved[0].Text)
	}
}

func TestChangingTextKeepsTheNotesAndNeedsTheItemOpenedFirst(t *testing.T) {
	key := newKey(t)
	params := encParams(t, key)
	web, agent := todoConn(t, webPeerID, params, key), todoConn(t, agentPeerID, params, key)
	text := "Buy beer"
	if _, _, _, err := agent.itemChange("x1", &text, nil); err == nil ||
		!strings.Contains(err.Error(), "read the list first") {
		t.Fatalf("a change to an item never opened was not refused: %v", err)
	}
	agent.openItems([]wire.TodoItem{
		{ExternalID: "x1", SealedBy: webPeerID, Enc: sealedItem(t, web, "m1", 4, "Buy milk", "2 litres")},
	})
	mid, rev, p, err := agent.itemChange("x1", &text, nil)
	if err != nil || mid != "m1" || rev != 5 || p.Text != "Buy beer" || p.Notes != "2 litres" {
		t.Fatalf("change sealed as mid=%s rev=%d %+v (%v)", mid, rev, p, err)
	}
}

func TestAnEncryptedListWithoutTheKeyShowsItemsSealedAndRefusesChanges(t *testing.T) {
	key := newKey(t)
	params := encParams(t, key)
	web := todoConn(t, webPeerID, params, key)
	blind := todoConn(t, agentPeerID, params, nil)
	items := blind.openItems([]wire.TodoItem{
		{ExternalID: "x1", SealedBy: webPeerID, Enc: sealedItem(t, web, "m1", 0, "secret", "notes")},
	})
	if !strings.Contains(items[0].Text, "encrypted item") || strings.Contains(items[0].Text, "secret") ||
		items[0].Notes != "" {
		t.Fatalf("an item was shown without its key: %+v", items[0])
	}
	if err := blind.requireSendable(); err == nil {
		t.Fatal("a client without the key may write")
	}
}

func TestAnItemNamingNoItemOrArrivingUnsealedIsNotPassedOff(t *testing.T) {
	key := newKey(t)
	params := encParams(t, key)
	web, agent := todoConn(t, webPeerID, params, key), todoConn(t, agentPeerID, params, key)
	ev := Event{Kind: "msg", Enc: sealedItem(t, web, "m1", 0, "x", ""), Todo: &wire.TodoFields{SealedBy: webPeerID}}
	agent.openEvent(&ev)
	if !strings.Contains(ev.Text, "did not say which item") {
		t.Fatalf("an envelope naming no item was opened: %q", ev.Text)
	}
	notes := "server notes"
	ev = Event{Kind: "msg", ExternalID: "x9", Text: "server text", Todo: &wire.TodoFields{Notes: &notes},
		Attachments: []wire.Attachment{{Name: "f.txt"}}}
	agent.openEvent(&ev)
	if !strings.HasPrefix(ev.Text, "[UNSEALED item") || !strings.HasPrefix(*ev.Todo.Notes, "[UNSEALED notes") ||
		ev.Attachments != nil {
		t.Fatalf("unsealed content passed as a participant's: %q / %q / %v", ev.Text, *ev.Todo.Notes, ev.Attachments)
	}
}
