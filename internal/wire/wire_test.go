package wire

import (
	"encoding/json"
	"testing"
)

func TestJoinedRoundTrip(t *testing.T) {
	j := NewJoined("550e8400-e29b-41d4-a716-446655440000", 3)
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

func TestNewJoinedStampsCurrentProtocolVersion(t *testing.T) {
	j := NewJoined("550e8400-e29b-41d4-a716-446655440000", 0)
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
