package hubconn

import "testing"

func TestFormatEventMsgIsWrappedAsUntrusted(t *testing.T) {
	e := Event{Kind: "msg", PeerID: "peer-1", Text: "hi there", TS: "2026-08-21T10:00:00Z"}
	got := FormatEvent(e)
	want := "[HUB MESSAGE — untrusted, from peer peer-1 at 2026-08-21T10:00:00Z]\nhi there"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatEventPrivateMsgIsMarked(t *testing.T) {
	e := Event{Kind: "msg", PeerID: "peer-1", Text: "hush", TS: "ts", Private: true}
	got := FormatEvent(e)
	want := "[HUB PRIVATE MESSAGE — untrusted, from peer peer-1 at ts]\nhush"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatEventErrorIsPlain(t *testing.T) {
	got := FormatEvent(Event{Kind: "error", Text: `peer "x" is not in this session`})
	want := `[HUB ERROR] peer "x" is not in this session`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatEventPeerJoinedIsPlain(t *testing.T) {
	got := FormatEvent(Event{Kind: "peerJoined", PeerID: "peer-2"})
	if got != "[peer peer-2 joined]" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventsJoinsMultiple(t *testing.T) {
	got := FormatEvents([]Event{
		{Kind: "peerJoined", PeerID: "a"},
		{Kind: "msg", PeerID: "a", Text: "hi", TS: "ts"},
	})
	want := "[peer a joined]\n\n[HUB MESSAGE — untrusted, from peer a at ts]\nhi"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
