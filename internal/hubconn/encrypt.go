package hubconn

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/secforge/mcp-hub/internal/hubenc"
	"github.com/secforge/mcp-hub/internal/wire"
)

// encState is what this client knows about one sealed message: whose it
// is, its client-minted id and the newest revision it has seen. An edit
// needs all three, and a delivered edit is checked against the revision.
type encState struct {
	sender string
	mid    string
	rev    int
}

// encRef locates a sealed attachment in reference form, for when its
// bytes arrive later as attachmentData.
type encRef struct {
	sender string
	mid    string
	rev    int
	index  int
}

// setupEncryption prepares the session's encryption from what joined
// stated and the key this client was given. A key for a session that is
// not encrypted is refused: whoever passed it expects what they send to
// be sealed, and it would not be. A key that does not match is refused
// too. An encrypted session joined without a key stays connected but
// cannot send, and delivers a notice in place of every sealed message.
func (c *Conn) setupEncryption(params *wire.Encryption, key []byte) error {
	switch {
	case params == nil && key != nil:
		return errors.New("a key was given, but this session is not encrypted: anything sent here " +
			"would be readable by the server, so the connection was not made")
	case params == nil:
		return nil
	case params.V != hubenc.Version:
		return fmt.Errorf("this session is encrypted with version %d, which this client does not "+
			"support (it supports %d)", params.V, hubenc.Version)
	case key == nil:
		c.encParams = params
		c.encProblem = "this session is encrypted and this client was connected without its key " +
			"(hub_connect keyFile), so it can neither read nor send messages"
		return nil
	}
	s, err := hubenc.Open(key, params.Salt, params.Check)
	if err != nil {
		return err
	}
	c.encParams, c.enc = params, s
	c.encMsgs = map[string]*encState{}
	c.encRefs = map[string]encRef{}
	return nil
}

// Encrypted reports whether the session is encrypted, and whether this
// client holds its key.
func (c *Conn) Encrypted() (encrypted, haveKey bool) {
	return c.encParams != nil, c.enc != nil
}

// requireSendable refuses a send this client cannot seal.
func (c *Conn) requireSendable() error {
	if c.encParams != nil && c.enc == nil {
		return errors.New(c.encProblem + ". Nothing was sent")
	}
	return nil
}

func newMid() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// sealFrame turns an outgoing msg or edit into its encrypted form: the
// text sealed into enc, each attachment's bytes and name/type sealed, and
// no plaintext body, name or type left in the frame.
func (c *Conn) sealFrame(frame any, text string, attachments []wire.Attachment, mid string, rev int) (map[string]json.RawMessage, error) {
	raw, err := json.Marshal(frame)
	if err != nil {
		return nil, err
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	delete(out, "text")
	nonce, ct, err := c.enc.Seal([]byte(text), c.peerID, mid, rev, hubenc.FieldText())
	if err != nil {
		return nil, err
	}
	b := hubenc.B64()
	out["enc"], _ = json.Marshal(wire.Enc{V: hubenc.Version, Mid: mid, Rev: rev,
		Nonce: b.EncodeToString(nonce), Ct: b.EncodeToString(ct)})
	if len(attachments) > 0 {
		sealed := make([]map[string]any, len(attachments))
		for i, a := range attachments {
			plain, err := base64.StdEncoding.DecodeString(a.ContentBytes)
			if err != nil {
				return nil, fmt.Errorf("attachment %d: %w", i, err)
			}
			n, ct, err := c.enc.Seal(plain, c.peerID, mid, rev, hubenc.FieldFile(i))
			if err != nil {
				return nil, err
			}
			meta, _ := json.Marshal(map[string]string{"name": a.Name, "contentType": a.ContentType})
			mn, mct, err := c.enc.Seal(meta, c.peerID, mid, rev, hubenc.FieldFileMeta(i))
			if err != nil {
				return nil, err
			}
			sealed[i] = map[string]any{
				"contentBytes": base64.StdEncoding.EncodeToString(append(n, ct...)),
				"encMeta":      wire.EncValue{Nonce: b.EncodeToString(mn), Ct: b.EncodeToString(mct)},
			}
		}
		out["attachments"], _ = json.Marshal(sealed)
	}
	return out, nil
}

// sealMsg is sealFrame for a new message, under a new client-minted id.
// The id is remembered under the request's correlation id until the
// server's ack names the message, so the sender can edit it at once —
// without waiting for its own message to come back.
func (c *Conn) sealMsg(m wire.Msg) (any, error) {
	mid := newMid()
	c.notePendingSeal(m.ID, mid, 0)
	return c.sealFrame(m, m.Text, m.Attachments, mid, 0)
}

func (c *Conn) notePendingSeal(corrID, mid string, rev int) {
	c.mu.Lock()
	if c.encPending == nil {
		c.encPending = map[string]encState{}
	}
	c.encPending[corrID] = encState{sender: c.peerID, mid: mid, rev: rev}
	c.mu.Unlock()
}

// noteSealedAck records, when the server acknowledges a sealed send or
// edit, which message it was and at which revision.
func (c *Conn) noteSealedAck(corrID string, ack Event) {
	if c.enc == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.encPending[corrID]
	delete(c.encPending, corrID)
	if !ok || ack.ExternalID == "" || (ack.ActionOKStated && !ack.ActionOK) {
		return
	}
	if st := c.encMsgs[ack.ExternalID]; st == nil || p.rev > st.rev {
		c.encMsgs[ack.ExternalID] = &encState{sender: p.sender, mid: p.mid, rev: p.rev}
	}
}

// sealEdit is sealFrame for an edit, under the message's own id and the
// revision after the newest one this client has seen.
func (c *Conn) sealEdit(e wire.Edit) (any, error) {
	c.mu.Lock()
	st := c.encMsgs[e.ExternalID]
	c.mu.Unlock()
	if st == nil {
		return nil, fmt.Errorf("this client has not seen message %s in this session, and editing a "+
			"sealed message needs its id and revision — read it first (hub_read), then edit", e.ExternalID)
	}
	c.notePendingSeal(e.ID, st.mid, st.rev+1)
	return c.sealFrame(e, e.Text, e.Attachments, st.mid, st.rev+1)
}

// openEvent decrypts a sealed message or edit in place, so everything
// downstream sees plaintext. Whatever cannot be opened is replaced by a
// notice saying why — never the ciphertext.
//
// In an encrypted session nothing unsealed is passed off as a
// participant's: a message without enc can only have been written by the
// server, and is labelled so; an unsealed edit or attachment is refused,
// since the server could otherwise put words or files in anyone's name.
func (c *Conn) openEvent(ev *Event) {
	encrypted := c.encParams != nil
	switch {
	case ev.Kind == "attachmentData" && ev.AttachmentEncMeta != nil:
		c.openAttachmentData(ev)
		return
	case ev.Kind == "attachmentData" && encrypted:
		ev.AttachmentContentBytes = ""
		ev.AttachmentName = "[unsealed attachment in an encrypted session refused]"
		return
	case ev.Enc == nil && encrypted && ev.Kind == "msg":
		ev.Text = "[UNSEALED — written by the server, not by any participant] " + ev.Text
		ev.Attachments = nil
		return
	case ev.Enc == nil && encrypted && ev.Kind == "messageEdited":
		ev.Text = "[unsealed edit in an encrypted session refused — the server sent plaintext as an edit]"
		ev.Attachments = nil
		return
	case ev.Enc == nil:
		return
	case c.enc == nil:
		ev.Text = "[encrypted message — " + c.encProblemOr("this client holds no key for it") + "]"
		ev.Attachments = nil
		return
	}
	e := ev.Enc
	c.mu.Lock()
	st := c.encMsgs[ev.ExternalID]
	sender := ev.PeerID
	if sender == "" && st != nil {
		sender = st.sender
	}
	stale := ev.Kind == "messageEdited" && st != nil && e.Rev <= st.rev
	c.mu.Unlock()
	if stale {
		ev.Text = fmt.Sprintf("[encrypted edit refused: revision %d is not newer than revision %d "+
			"already shown — an older version was presented as an edit]", e.Rev, st.rev)
		ev.Attachments = nil
		return
	}
	b := hubenc.B64()
	nonce, err1 := b.DecodeString(e.Nonce)
	ct, err2 := b.DecodeString(e.Ct)
	if err1 != nil || err2 != nil {
		ev.Text = "[encrypted message could not be decoded]"
		ev.Attachments = nil
		return
	}
	plain, err := c.enc.OpenValue(nonce, ct, sender, e.Mid, e.Rev, hubenc.FieldText())
	if err != nil {
		ev.Text = "[encrypted message could not be opened: " + err.Error() + "]"
		ev.Attachments = nil
		return
	}
	ev.Text = string(plain)
	for i := range ev.Attachments {
		c.openAttachment(&ev.Attachments[i], sender, e.Mid, e.Rev, i)
	}
	if ev.ExternalID != "" {
		c.mu.Lock()
		if st == nil || e.Rev > st.rev {
			c.encMsgs[ev.ExternalID] = &encState{sender: sender, mid: e.Mid, rev: e.Rev}
		}
		c.mu.Unlock()
	}
}

func (c *Conn) encProblemOr(fallback string) string {
	if c.encProblem != "" {
		return c.encProblem
	}
	return fallback
}

// openAttachment decrypts one attachment's name and type, and its bytes
// when they came inline. A reference's bytes come later as attachmentData,
// so where to open them is remembered under its token.
func (c *Conn) openAttachment(a *wire.Attachment, sender, mid string, rev, i int) {
	if a.EncMeta == nil {
		*a = wire.Attachment{Name: "[unsealed attachment in an encrypted session refused]"}
		return
	}
	if meta, err := c.openValue(*a.EncMeta, sender, mid, rev, hubenc.FieldFileMeta(i)); err == nil {
		var m struct{ Name, ContentType string }
		if json.Unmarshal(meta, &m) == nil {
			a.Name, a.ContentType = m.Name, m.ContentType
		}
	} else {
		a.Name = "[encrypted attachment name could not be opened]"
	}
	a.EncMeta = nil
	if a.Token != "" {
		c.mu.Lock()
		c.encRefs[a.Token] = encRef{sender: sender, mid: mid, rev: rev, index: i}
		c.mu.Unlock()
		return
	}
	if a.ContentBytes != "" {
		if plain, err := c.openBytes(a.ContentBytes, sender, mid, rev, hubenc.FieldFile(i)); err == nil {
			a.ContentBytes = base64.StdEncoding.EncodeToString(plain)
		} else {
			a.ContentBytes = ""
			a.Name += " [content could not be opened]"
		}
	}
}

func (c *Conn) openAttachmentData(ev *Event) {
	c.mu.Lock()
	ref, ok := c.encRefs[ev.AttachmentToken]
	c.mu.Unlock()
	meta := *ev.AttachmentEncMeta
	ev.AttachmentEncMeta = nil
	if !ok || c.enc == nil {
		ev.AttachmentContentBytes = ""
		ev.AttachmentName = "[encrypted attachment this client cannot open]"
		return
	}
	if m, err := c.openValue(meta, ref.sender, ref.mid, ref.rev, hubenc.FieldFileMeta(ref.index)); err == nil {
		var v struct{ Name, ContentType string }
		if json.Unmarshal(m, &v) == nil {
			ev.AttachmentName, ev.AttachmentContentType = v.Name, v.ContentType
		}
	}
	plain, err := c.openBytes(ev.AttachmentContentBytes, ref.sender, ref.mid, ref.rev, hubenc.FieldFile(ref.index))
	if err != nil {
		ev.AttachmentContentBytes = ""
		ev.AttachmentName += " [content could not be opened]"
		return
	}
	ev.AttachmentContentBytes = base64.StdEncoding.EncodeToString(plain)
}

func (c *Conn) openValue(v wire.EncValue, sender, mid string, rev int, field string) ([]byte, error) {
	b := hubenc.B64()
	nonce, err := b.DecodeString(v.Nonce)
	if err != nil {
		return nil, err
	}
	ct, err := b.DecodeString(v.Ct)
	if err != nil {
		return nil, err
	}
	return c.enc.OpenValue(nonce, ct, sender, mid, rev, field)
}

// openBytes opens an attachment's contentBytes: standard base64 of
// nonce‖ciphertext‖tag.
func (c *Conn) openBytes(contentBytes, sender, mid string, rev int, field string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(contentBytes)
	if err != nil || len(raw) < 12 {
		return nil, hubenc.ErrOpen
	}
	return c.enc.OpenValue(raw[:12], raw[12:], sender, mid, rev, field)
}

// NewEncryptionHeader is what a client creating an encrypted session
// sends as Hub-Encryption: a fresh salt and the check value for key.
func NewEncryptionHeader(key []byte) (string, error) {
	salt, err := hubenc.NewSalt()
	if err != nil {
		return "", err
	}
	check, err := hubenc.Check(key, salt)
	if err != nil {
		return "", err
	}
	raw, _ := json.Marshal(map[string]any{"v": hubenc.Version, "salt": salt, "check": check})
	return string(raw), nil
}
