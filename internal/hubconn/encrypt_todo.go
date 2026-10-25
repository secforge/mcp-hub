package hubconn

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/secforge/mcp-hub/internal/hubenc"
	"github.com/secforge/mcp-hub/internal/wire"
)

// An encrypted todo list seals each item's text and notes together in
// one envelope (field "item"), so the server cannot tell which of the two
// a change touched. Done, position and timestamps stay readable: the
// server orders by them. A change of only done or position carries no
// envelope; the server keeps the old one, and its event repeats it
// unchanged under the same mid, rev and ciphertext.

// itemPlain is what an item's envelope seals.
type itemPlain struct {
	Text  string `json:"text"`
	Notes string `json:"notes"`
}

// sealItem turns an outgoing add or edit into its sealed form: text and
// notes removed, the envelope added. It returns the ciphertext too, so
// the echo of this very change is recognised as the same envelope.
func (c *Conn) sealItem(frame any, mid string, rev int, p itemPlain) (map[string]json.RawMessage, string, error) {
	raw, err := json.Marshal(frame)
	if err != nil {
		return nil, "", err
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, "", err
	}
	delete(out, "text")
	delete(out, "notes")
	plain, err := json.Marshal(p)
	if err != nil {
		return nil, "", err
	}
	nonce, ct, err := c.enc.Seal(plain, c.peerID, mid, rev, hubenc.FieldItem())
	if err != nil {
		return nil, "", err
	}
	b := hubenc.B64()
	ctText := b.EncodeToString(ct)
	out["enc"], _ = json.Marshal(wire.Enc{V: hubenc.Version, Mid: mid, Rev: rev,
		Nonce: b.EncodeToString(nonce), Ct: ctText})
	return out, ctText, nil
}

// withID is a sealed frame with the request's correlation id set.
func withID(frame map[string]json.RawMessage, id string) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(frame)+1)
	for k, v := range frame {
		out[k] = v
	}
	if id != "" {
		out["id"], _ = json.Marshal(id)
	}
	return out
}

func (c *Conn) notePendingItem(corrID, mid string, rev int, p itemPlain, ct string) {
	c.mu.Lock()
	if c.encPending == nil {
		c.encPending = map[string]encState{}
	}
	c.encPending[corrID] = encState{sender: c.peerID, mid: mid, rev: rev, item: true,
		text: p.Text, notes: p.Notes, ct: ct}
	c.mu.Unlock()
}

// sealedTodoAdd adds an item to an encrypted list under a new mid.
func (c *Conn) sealedTodoAdd(text string, notes *string, done *bool, at Placement) (Event, bool, error) {
	if err := c.requireSendable(); err != nil {
		return Event{}, false, err
	}
	p := itemPlain{Text: text}
	if notes != nil {
		p.Notes = *notes
	}
	mid := newMid()
	sealed, ct, err := c.sealItem(wire.TodoAdd{Type: wire.TypeMsg, Done: done, AfterID: at.raw()}, mid, 0, p)
	if err != nil {
		return Event{}, false, err
	}
	return c.awaitAnswer("sendAck", func(id string) any {
		c.notePendingItem(id, mid, 0, p, ct)
		return withID(sealed, id)
	})
}

// sealedTodoUpdate changes an encrypted item's text or notes. Both are
// sealed every time, so the unchanged one is taken from what this client
// last opened for the item — which it must therefore have seen.
func (c *Conn) sealedTodoUpdate(externalID string, text, notes *string, done *bool, at Placement) (Event, bool, error) {
	if err := c.requireSendable(); err != nil {
		return Event{}, false, err
	}
	mid, rev, p, err := c.itemChange(externalID, text, notes)
	if err != nil {
		return Event{}, false, err
	}
	sealed, ct, err := c.sealItem(wire.TodoEdit{Type: wire.TypeEdit, ExternalID: externalID, Done: done,
		AfterID: at.raw()}, mid, rev, p)
	if err != nil {
		return Event{}, false, err
	}
	return c.awaitAnswer("editAck", func(id string) any {
		c.notePendingItem(id, mid, rev, p, ct)
		return withID(sealed, id)
	})
}

// itemChange is what a change of an encrypted item seals: its mid, the
// next revision, and text and notes with the given ones replaced.
func (c *Conn) itemChange(externalID string, text, notes *string) (string, int, itemPlain, error) {
	c.mu.Lock()
	st := c.encMsgs[externalID]
	var cur encState
	if st != nil {
		cur = *st
	}
	c.mu.Unlock()
	if st == nil || !cur.item {
		return "", 0, itemPlain{}, fmt.Errorf("this client has not opened item %s on this connection, "+
			"and changing an encrypted item's text or notes needs both as they stand — read the list "+
			"first (hub_todo_items), then change it", externalID)
	}
	p := itemPlain{Text: cur.text, Notes: cur.notes}
	if text != nil {
		p.Text = *text
	}
	if notes != nil {
		p.Notes = *notes
	}
	return cur.mid, cur.rev + 1, p, nil
}

// openItemEvent opens an item add or change on an encrypted list. What
// cannot be opened is replaced by a notice saying why, never shown as
// the item's text.
func (c *Conn) openItemEvent(ev *Event) {
	// An item has no attachments; on an encrypted list any that arrive
	// were put there by the server.
	ev.Attachments = nil
	if ev.Enc == nil {
		if ev.Kind == "msg" {
			ev.Text = "[UNSEALED item — written by the server, not by any participant] " + ev.Text
			if ev.Todo != nil && ev.Todo.Notes != nil && *ev.Todo.Notes != "" {
				unsealed := "[UNSEALED notes — written by the server] " + *ev.Todo.Notes
				ev.Todo.Notes = &unsealed
			}
			return
		}
		ev.Text = "[unsealed change to an item of an encrypted list refused — the server sent plaintext]"
		clearNotes(ev)
		return
	}
	sealedBy := ""
	if ev.Todo != nil {
		sealedBy = ev.Todo.SealedBy
	}
	p, err := c.openItem(ev.ExternalID, sealedBy, ev.Enc)
	if err != nil {
		ev.Text = "[" + err.Error() + "]"
		clearNotes(ev)
		return
	}
	ev.Text = p.Text
	if ev.Todo == nil {
		ev.Todo = &wire.TodoFields{}
	}
	ev.Todo.Notes = &p.Notes
}

func clearNotes(ev *Event) {
	if ev.Todo != nil {
		empty := ""
		ev.Todo.Notes = &empty
	}
}

// openItems opens every item of a list as joined or asked for.
func (c *Conn) openItems(items []wire.TodoItem) []wire.TodoItem {
	if c.encParams == nil || c.todo == nil {
		return items
	}
	out := make([]wire.TodoItem, len(items))
	for i, it := range items {
		switch {
		case it.Enc == nil:
			it.Text = "[UNSEALED item — written by the server, not by any participant] " + it.Text
			if it.Notes != "" {
				it.Notes = "[UNSEALED notes — written by the server] " + it.Notes
			}
		default:
			p, err := c.openItem(it.ExternalID, it.SealedBy, it.Enc)
			if err != nil {
				it.Text, it.Notes = "["+err.Error()+"]", ""
			} else {
				it.Text, it.Notes = p.Text, p.Notes
			}
		}
		it.Enc = nil
		out[i] = it
	}
	return out
}

// openItem opens one item envelope and checks it belongs where it
// arrived: externalId and mid stay paired once seen, a revision older
// than one already shown is refused, and the same revision is accepted
// only as the very same envelope (a done- or position-only change).
func (c *Conn) openItem(externalID, sealedBy string, e *wire.Enc) (itemPlain, error) {
	if c.enc == nil {
		return itemPlain{}, errors.New("encrypted item — " + c.encProblemOr("this client holds no key for it"))
	}
	// The binding between an item and its envelope is checked by
	// externalId, so an envelope that names no item cannot be checked and
	// is not opened.
	if externalID == "" {
		return itemPlain{}, errors.New("encrypted item refused: the server did not say which item it is")
	}
	c.mu.Lock()
	st := c.encMsgs[externalID]
	var cur encState
	if st != nil {
		cur = *st
	}
	other, midSeen := c.encMids[e.Mid]
	c.mu.Unlock()
	if (st != nil && cur.mid != e.Mid) || (midSeen && other != externalID) {
		return itemPlain{}, errors.New("encrypted item refused: its sealed id does not belong to this " +
			"item — the server presented another item's content under it")
	}
	if st != nil && cur.item {
		switch {
		case e.Rev < cur.rev:
			return itemPlain{}, fmt.Errorf("encrypted item refused: revision %d is older than revision "+
				"%d already shown", e.Rev, cur.rev)
		case e.Rev == cur.rev && e.Ct == cur.ct:
			return itemPlain{Text: cur.text, Notes: cur.notes}, nil
		case e.Rev == cur.rev:
			return itemPlain{}, fmt.Errorf("encrypted item refused: a different envelope under revision "+
				"%d, which was already shown", e.Rev)
		}
	}
	if sealedBy == "" {
		return itemPlain{}, errors.New("encrypted item could not be opened: the server did not say who " +
			"sealed it (sealedBy)")
	}
	b := hubenc.B64()
	nonce, err1 := b.DecodeString(e.Nonce)
	ct, err2 := b.DecodeString(e.Ct)
	if err1 != nil || err2 != nil {
		return itemPlain{}, errors.New("encrypted item could not be decoded")
	}
	plain, err := c.enc.OpenValue(nonce, ct, sealedBy, e.Mid, e.Rev, hubenc.FieldItem())
	if err != nil {
		return itemPlain{}, errors.New("encrypted item could not be opened: " + err.Error())
	}
	var p itemPlain
	if err := json.Unmarshal(plain, &p); err != nil {
		return itemPlain{}, errors.New("encrypted item opened, but its content is not an item")
	}
	c.mu.Lock()
	if now := c.encMsgs[externalID]; now == nil || e.Rev >= now.rev {
		c.encMsgs[externalID] = &encState{sender: sealedBy, mid: e.Mid, rev: e.Rev, item: true,
			text: p.Text, notes: p.Notes, ct: e.Ct}
	}
	c.encMids[e.Mid] = externalID
	c.mu.Unlock()
	return p, nil
}
