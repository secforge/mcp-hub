package wire

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestJoinedRoundTrip(t *testing.T) {
	j := NewJoined("550e8400-e29b-41d4-a716-446655440000", 3, "", "")
	raw, err := json.Marshal(j)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(raw); got != `{"type":"joined","peerId":"550e8400-e29b-41d4-a716-446655440000","peerCount":3,"serverVersion":1}` {
		t.Fatalf("unexpected json: %s", got)
	}
	typ, err := DecodeType(raw)
	if err != nil {
		t.Fatalf("decode type: %v", err)
	}
	if typ != TypeJoined {
		t.Fatalf("got type %q, want %q", typ, TypeJoined)
	}
}

func TestJoinedIncludesNameAndAgePublicKey(t *testing.T) {
	j := NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "Alice", "age1scdm7mae5t68c9ch0sqfzlusqyflpgxlrgk3zwl44zwl9vvq2guqtdv4fk")
	raw, _ := json.Marshal(j)
	var decoded Joined
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Name != "Alice" || decoded.AgePublicKey != "age1scdm7mae5t68c9ch0sqfzlusqyflpgxlrgk3zwl44zwl9vvq2guqtdv4fk" {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
}

func TestNewJoinedStampsCurrentProtocolVersion(t *testing.T) {
	j := NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
	if j.ServerVersion != ProtocolVersion {
		t.Fatalf("got ServerVersion %d, want %d", j.ServerVersion, ProtocolVersion)
	}
}

func TestProtocolVersionIsOne(t *testing.T) {
	// The explicit baseline: everything shipped before version exchange
	// existed is retroactively "v1", and absence of a client-sent version
	// must be treated as v1 too (see wsserver's clientVersion parsing).
	if ProtocolVersion != 1 {
		t.Fatalf("got ProtocolVersion %d, want 1", ProtocolVersion)
	}
}

func TestBroadcastMsgFields(t *testing.T) {
	m := NewBroadcastMsg("peer-1", "hello", "2026-08-21T10:00:00Z")
	raw, _ := json.Marshal(m)
	var decoded Msg
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Type != TypeMsg || decoded.PeerID != "peer-1" || decoded.Text != "hello" || decoded.TS != "2026-08-21T10:00:00Z" {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
}

func TestOutgoingMsgHasNoPeerOrTS(t *testing.T) {
	m := NewOutgoingMsg("hi")
	raw, _ := json.Marshal(m)
	if got := string(raw); got != `{"type":"msg","text":"hi"}` {
		t.Fatalf("outgoing msg should omit empty peerId/ts, got: %s", got)
	}
}

func TestOutgoingDirectedMsgHasTo(t *testing.T) {
	m := NewOutgoingDirectedMsg("hi", "peer-2")
	raw, _ := json.Marshal(m)
	if got := string(raw); got != `{"type":"msg","text":"hi","to":"peer-2"}` {
		t.Fatalf("unexpected json: %s", got)
	}
}

func TestDirectedMsgIsMarkedPrivate(t *testing.T) {
	m := NewDirectedMsg("peer-1", "hello", "2026-08-21T10:00:00Z")
	raw, _ := json.Marshal(m)
	var decoded Msg
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !decoded.Private {
		t.Fatalf("expected Private to be true, got: %+v", decoded)
	}
	if decoded.PeerID != "peer-1" || decoded.Text != "hello" || decoded.TS != "2026-08-21T10:00:00Z" {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
}

func TestBroadcastMsgIsNotPrivate(t *testing.T) {
	m := NewBroadcastMsg("peer-1", "hello", "ts")
	if m.Private {
		t.Fatal("broadcast messages must not be marked private")
	}
}

func TestPeerJoinedIncludesNameAndAgePublicKey(t *testing.T) {
	pe := NewPeerJoined("peer-1", "Alice", "age1scdm7mae5t68c9ch0sqfzlusqyflpgxlrgk3zwl44zwl9vvq2guqtdv4fk")
	raw, _ := json.Marshal(pe)
	var decoded PeerEvent
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Name != "Alice" || decoded.AgePublicKey != "age1scdm7mae5t68c9ch0sqfzlusqyflpgxlrgk3zwl44zwl9vvq2guqtdv4fk" {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
}

func TestJoinedDecodesBridgeFields(t *testing.T) {
	raw := []byte(`{"type":"joined","peerId":"550e8400-e29b-41d4-a716-446655440000",` +
		`"peerCount":0,"serverVersion":1,"latestCursor":"cursor-9","historyLimitMax":50,` +
		`"canSend":true,"conversationKind":"oneOnOne","topic":"Support chat"}`)
	var j Joined
	if err := json.Unmarshal(raw, &j); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if j.LatestCursor == nil || *j.LatestCursor != "cursor-9" {
		t.Fatalf("got LatestCursor %v", j.LatestCursor)
	}
	if j.HistoryLimitMax != 50 {
		t.Fatalf("got HistoryLimitMax %d", j.HistoryLimitMax)
	}
	if !j.CanSend {
		t.Fatal("expected CanSend true")
	}
	if j.ConversationKind != "oneOnOne" {
		t.Fatalf("got ConversationKind %q", j.ConversationKind)
	}
	if j.Topic == nil || *j.Topic != "Support chat" {
		t.Fatalf("got Topic %v", j.Topic)
	}
}

func TestJoinedOmitsBridgeFieldsWhenUnset(t *testing.T) {
	j := NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
	raw, _ := json.Marshal(j)
	for _, field := range []string{"latestCursor", "historyLimitMax", "canSend", "conversationKind", "topic"} {
		if strings.Contains(string(raw), field) {
			t.Fatalf("expected %q to be omitted from a plain hub_connect joined, got: %s", field, raw)
		}
	}
}

func TestPeerJoinedOmitsEmptyNameAndAgePublicKey(t *testing.T) {
	pe := NewPeerJoined("peer-1", "", "")
	raw, _ := json.Marshal(pe)
	if got := string(raw); got != `{"type":"peerJoined","peerId":"peer-1"}` {
		t.Fatalf("expected empty name/agePublicKey to be omitted, got: %s", got)
	}
}

func TestHistoryRequestRoundTrip(t *testing.T) {
	h := NewHistoryRequest("cursor-123", 50)
	raw, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(raw); got != `{"type":"history","before":"cursor-123","limit":50}` {
		t.Fatalf("unexpected marshal: %s", got)
	}
	var decoded History
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Before != "cursor-123" || decoded.Limit != 50 {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
}

func TestHistoryRequestOmitsEmptyBefore(t *testing.T) {
	h := NewHistoryRequest("", 50)
	raw, _ := json.Marshal(h)
	if got := string(raw); got != `{"type":"history","limit":50}` {
		t.Fatalf("expected empty before to be omitted, got: %s", got)
	}
}

func TestHistoryAfterRequestRoundTrip(t *testing.T) {
	h := NewHistoryAfterRequest("cursor-123", 50)
	raw, err := json.Marshal(h)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(raw); got != `{"type":"history","after":"cursor-123","limit":50}` {
		t.Fatalf("unexpected marshal: %s", got)
	}
	var decoded History
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.After != "cursor-123" || decoded.Before != "" || decoded.Limit != 50 {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
}

func TestJoinedDecodesHistoryAfter(t *testing.T) {
	raw := []byte(`{"type":"joined","peerId":"550e8400-e29b-41d4-a716-446655440000",` +
		`"peerCount":0,"serverVersion":1,"historyAfter":true}`)
	var j Joined
	if err := json.Unmarshal(raw, &j); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !j.HistoryAfter {
		t.Fatal("expected HistoryAfter true")
	}
}

func TestJoinedOmitsHistoryAfterWhenUnset(t *testing.T) {
	j := NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
	raw, _ := json.Marshal(j)
	if strings.Contains(string(raw), "historyAfter") {
		t.Fatalf("expected historyAfter to be omitted from a plain hub_connect joined, got: %s", raw)
	}
}

func TestHistoryCompleteRoundTrip(t *testing.T) {
	raw, _ := json.Marshal(NewHistoryComplete())
	if got := string(raw); got != `{"type":"historyComplete"}` {
		t.Fatalf("unexpected marshal: %s", got)
	}
	typ, err := DecodeType(raw)
	if err != nil || typ != TypeHistoryComplete {
		t.Fatalf("expected type %q, got %q (err=%v)", TypeHistoryComplete, typ, err)
	}
}

func TestAckRoundTrip(t *testing.T) {
	a := NewAck("cursor-123")
	raw, err := json.Marshal(a)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got := string(raw); got != `{"type":"ack","ackCursor":"cursor-123"}` {
		t.Fatalf("unexpected marshal: %s", got)
	}
	var decoded Ack
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.AckCursor != "cursor-123" || decoded.OK {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
}

func TestAckDecodesServerReplyWithOK(t *testing.T) {
	raw := []byte(`{"type":"ack","ackCursor":"cursor-9","ok":true}`)
	var a Ack
	if err := json.Unmarshal(raw, &a); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if a.AckCursor != "cursor-9" || !a.OK {
		t.Fatalf("unexpected decode: %+v", a)
	}
}

func TestMsgAckCursorRoundTrip(t *testing.T) {
	m := NewOutgoingMsg("hi")
	m.AckCursor = "cursor-1"
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Msg
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.AckCursor != "cursor-1" {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
}

func TestMsgOmitsAckCursorWhenUnset(t *testing.T) {
	raw, _ := json.Marshal(NewOutgoingMsg("hi"))
	if strings.Contains(string(raw), "ackCursor") {
		t.Fatalf("expected ackCursor to be omitted when unset, got: %s", raw)
	}
}

func TestReactionEditDeleteHistoryCarryAckCursor(t *testing.T) {
	r := NewReactionRequest("ext-1", "thumbsup", "add")
	r.AckCursor = "cursor-r"
	if raw, _ := json.Marshal(r); !strings.Contains(string(raw), `"ackCursor":"cursor-r"`) {
		t.Fatalf("expected Reaction to carry ackCursor, got: %s", raw)
	}

	e := NewEditRequest("ext-1", "new text")
	e.AckCursor = "cursor-e"
	if raw, _ := json.Marshal(e); !strings.Contains(string(raw), `"ackCursor":"cursor-e"`) {
		t.Fatalf("expected Edit to carry ackCursor, got: %s", raw)
	}

	d := NewDeleteRequest("ext-1")
	d.AckCursor = "cursor-d"
	if raw, _ := json.Marshal(d); !strings.Contains(string(raw), `"ackCursor":"cursor-d"`) {
		t.Fatalf("expected Delete to carry ackCursor, got: %s", raw)
	}

	h := NewHistoryRequest("", 10)
	h.AckCursor = "cursor-h"
	if raw, _ := json.Marshal(h); !strings.Contains(string(raw), `"ackCursor":"cursor-h"`) {
		t.Fatalf("expected History to carry ackCursor, got: %s", raw)
	}
}

func TestMsgHistoricalFlagRoundTrip(t *testing.T) {
	m := NewBroadcastMsg("peer-1", "hi", "2026-01-01T00:00:00Z")
	m.Historical = true
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Msg
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !decoded.Historical {
		t.Fatalf("expected historical:true to round-trip, got: %s", raw)
	}
}

func TestMsgOmitsHistoricalWhenFalse(t *testing.T) {
	raw, _ := json.Marshal(NewBroadcastMsg("peer-1", "hi", "2026-01-01T00:00:00Z"))
	if got := string(raw); strings.Contains(got, "historical") {
		t.Fatalf("expected historical:false to be omitted, got: %s", got)
	}
}

func TestErrorCodeAndRetryableRoundTrip(t *testing.T) {
	e := Error{Type: TypeError, Message: "this link has been revoked", Code: "revoked", Retryable: false}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Error
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Code != "revoked" || decoded.Retryable != false {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
}

func TestNewErrorOmitsCodeAndRetryable(t *testing.T) {
	raw, _ := json.Marshal(NewError("plain error"))
	if got := string(raw); got != `{"type":"error","message":"plain error"}` {
		t.Fatalf("expected code/retryable to be omitted for a plain error, got: %s", got)
	}
}

func TestMsgExternalIDAndOwnRoundTrip(t *testing.T) {
	m := NewBroadcastMsg("peer-1", "hi", "2026-01-01T00:00:00Z")
	m.ExternalID = "ext-1"
	m.Own = true
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Msg
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.ExternalID != "ext-1" || !decoded.Own {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
}

func TestMsgOmitsExternalIDAndOwnWhenUnset(t *testing.T) {
	raw, _ := json.Marshal(NewBroadcastMsg("peer-1", "hi", "2026-01-01T00:00:00Z"))
	if got := string(raw); strings.Contains(got, "externalId") || strings.Contains(got, "own") {
		t.Fatalf("expected externalId/own to be omitted, got: %s", got)
	}
}

func TestReactionChangedRoundTrip(t *testing.T) {
	raw := []byte(`{"type":"reactionChanged","externalId":"ext-1","peerId":"550e8400-e29b-41d4-a716-446655440000",` +
		`"reaction":"👍","label":"Like","action":"add","ts":"2026-01-01T00:00:00Z","own":true}`)
	var r ReactionChanged
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r.ExternalID != "ext-1" || r.PeerID != "550e8400-e29b-41d4-a716-446655440000" ||
		r.Reaction != "👍" || r.Label != "Like" || r.Action != "add" || !r.Own {
		t.Fatalf("unexpected decode: %+v", r)
	}
	typ, err := DecodeType(raw)
	if err != nil || typ != TypeReactionChanged {
		t.Fatalf("expected type %q, got %q (err=%v)", TypeReactionChanged, typ, err)
	}
}

func TestReactionChangedOmitsPeerIDWhenUnattributed(t *testing.T) {
	r := ReactionChanged{Type: TypeReactionChanged, ExternalID: "ext-1", Reaction: "👀", Label: "Eyes", Action: "remove"}
	raw, _ := json.Marshal(r)
	if got := string(raw); strings.Contains(got, "peerId") {
		t.Fatalf("expected an unattributed reactionChanged to omit peerId, got: %s", got)
	}
}

func TestMessageEditedRoundTrip(t *testing.T) {
	raw := []byte(`{"type":"messageEdited","externalId":"ext-1","text":"corrected text","ts":"2026-01-01T00:00:00Z"}`)
	var m MessageEdited
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m.ExternalID != "ext-1" || m.Text != "corrected text" || m.Own {
		t.Fatalf("unexpected decode: %+v", m)
	}
	typ, err := DecodeType(raw)
	if err != nil || typ != TypeMessageEdited {
		t.Fatalf("expected type %q, got %q (err=%v)", TypeMessageEdited, typ, err)
	}
}

func TestMsgCursorRoundTrip(t *testing.T) {
	m := NewBroadcastMsg("peer-1", "hi", "2026-01-01T00:00:00Z")
	m.Cursor = "cursor-123"
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Msg
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Cursor != "cursor-123" {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
}

func TestMsgOmitsCursorWhenUnset(t *testing.T) {
	raw, _ := json.Marshal(NewBroadcastMsg("peer-1", "hi", "2026-01-01T00:00:00Z"))
	if got := string(raw); strings.Contains(got, "cursor") {
		t.Fatalf("expected cursor to be omitted when unset, got: %s", got)
	}
}

func TestDeleteRequestRoundTrip(t *testing.T) {
	d := NewDeleteRequest("ext-1")
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Delete
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.ExternalID != "ext-1" {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
	typ, err := DecodeType(raw)
	if err != nil || typ != TypeDelete {
		t.Fatalf("expected type %q, got %q (err=%v)", TypeDelete, typ, err)
	}
}

func TestDeleteAckRoundTrip(t *testing.T) {
	raw := []byte(`{"type":"deleteAck","externalId":"ext-1","ok":true}`)
	var a DeleteAck
	if err := json.Unmarshal(raw, &a); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if a.ExternalID != "ext-1" || !a.OK {
		t.Fatalf("unexpected decode: %+v", a)
	}
	typ, err := DecodeType(raw)
	if err != nil || typ != TypeDeleteAck {
		t.Fatalf("expected type %q, got %q (err=%v)", TypeDeleteAck, typ, err)
	}
}

func TestMessageDeletedRoundTrip(t *testing.T) {
	raw := []byte(`{"type":"messageDeleted","externalId":"ext-1","cursor":"cursor-1","ts":"2026-01-01T00:00:00Z","own":true}`)
	var d MessageDeleted
	if err := json.Unmarshal(raw, &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if d.ExternalID != "ext-1" || d.Cursor != "cursor-1" || !d.Own {
		t.Fatalf("unexpected decode: %+v", d)
	}
	typ, err := DecodeType(raw)
	if err != nil || typ != TypeMessageDeleted {
		t.Fatalf("expected type %q, got %q (err=%v)", TypeMessageDeleted, typ, err)
	}
}

func TestReactionRequestRoundTrip(t *testing.T) {
	r := NewReactionRequest("ext-1", "👍", "add")
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Reaction
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.ExternalID != "ext-1" || decoded.Reaction != "👍" || decoded.Action != "add" {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
	typ, err := DecodeType(raw)
	if err != nil || typ != TypeReaction {
		t.Fatalf("expected type %q, got %q (err=%v)", TypeReaction, typ, err)
	}
}

func TestEditRequestRoundTrip(t *testing.T) {
	e := NewEditRequest("ext-1", "corrected")
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Edit
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.ExternalID != "ext-1" || decoded.Text != "corrected" {
		t.Fatalf("unexpected round trip: %+v", decoded)
	}
	typ, err := DecodeType(raw)
	if err != nil || typ != TypeEdit {
		t.Fatalf("expected type %q, got %q (err=%v)", TypeEdit, typ, err)
	}
}

func TestReactionAckRoundTrip(t *testing.T) {
	raw := []byte(`{"type":"reactionAck","externalId":"ext-1","reaction":"👍","action":"add","ok":true}`)
	var a ReactionAck
	if err := json.Unmarshal(raw, &a); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if a.ExternalID != "ext-1" || a.Reaction != "👍" || a.Action != "add" || !a.OK {
		t.Fatalf("unexpected decode: %+v", a)
	}
	typ, err := DecodeType(raw)
	if err != nil || typ != TypeReactionAck {
		t.Fatalf("expected type %q, got %q (err=%v)", TypeReactionAck, typ, err)
	}
}

func TestEditAckRoundTrip(t *testing.T) {
	raw := []byte(`{"type":"editAck","externalId":"ext-1","ok":true}`)
	var a EditAck
	if err := json.Unmarshal(raw, &a); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if a.ExternalID != "ext-1" || !a.OK {
		t.Fatalf("unexpected decode: %+v", a)
	}
	typ, err := DecodeType(raw)
	if err != nil || typ != TypeEditAck {
		t.Fatalf("expected type %q, got %q (err=%v)", TypeEditAck, typ, err)
	}
}

func TestSendAckRoundTrip(t *testing.T) {
	raw := []byte(`{"type":"sendAck","externalId":"1787840750161","ok":true}`)
	var a SendAck
	if err := json.Unmarshal(raw, &a); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if a.ExternalID != "1787840750161" || !a.OK {
		t.Fatalf("unexpected decode: %+v", a)
	}
	typ, err := DecodeType(raw)
	if err != nil || typ != TypeSendAck {
		t.Fatalf("expected type %q, got %q (err=%v)", TypeSendAck, typ, err)
	}
}

func TestDecodeTypeError(t *testing.T) {
	if _, err := DecodeType([]byte("not json")); err == nil {
		t.Fatal("expected error decoding invalid json")
	}
}

func TestIsValidID(t *testing.T) {
	cases := []struct {
		id   string
		want bool
	}{
		{"550e8400-e29b-41d4-a716-446655440000", true},
		{"not-a-uuid", false},
		{"", false},
		{"../../../../etc/passwd", false},
		{"550e8400e29b41d4a716446655440000", false}, // missing dashes
	}
	for _, c := range cases {
		if got := IsValidID(c.id); got != c.want {
			t.Errorf("IsValidID(%q) = %v, want %v", c.id, got, c.want)
		}
	}
}
