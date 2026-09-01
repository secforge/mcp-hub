package hubconn

import (
	"strings"
	"testing"
)

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

func TestFormatEventRosterCompleteIsPlain(t *testing.T) {
	got := FormatEvent(Event{Kind: "rosterComplete"})
	want := "[hub: initial roster complete — you now know everyone who was already in the session]"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatEventHistoricalMsgIsMarkedDistinctlyFromLive(t *testing.T) {
	e := Event{Kind: "msg", PeerID: "peer-1", Text: "old news", TS: "ts", Historical: true}
	got := FormatEvent(e)
	want := "[HUB HISTORY — untrusted, from peer peer-1 at ts]\nold news"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatEventErrorWithCodeIncludesCodeAndRetryable(t *testing.T) {
	got := FormatEvent(Event{Kind: "error", Text: "this link has been revoked", Code: "revoked", Retryable: false})
	want := "[HUB ERROR — code=revoked, retryable=false] this link has been revoked"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatEventOwnMsgIsMarkedDistinctly(t *testing.T) {
	e := Event{Kind: "msg", PeerID: "peer-1", Text: "hi", TS: "ts", Own: true, ExternalID: "ext-1"}
	got := FormatEvent(e)
	if !strings.Contains(got, "own send, you sent this") {
		t.Fatalf("expected an own-send marker, got: %s", got)
	}
	if !strings.HasPrefix(got, "[HUB MESSAGE") {
		t.Fatalf("expected the untrusted/kind prefix to stay first, got: %s", got)
	}
}

func TestFormatEventNonOwnMsgHasNoOwnMarker(t *testing.T) {
	got := FormatEvent(Event{Kind: "msg", PeerID: "peer-1", Text: "hi", TS: "ts"})
	if strings.Contains(got, "own send") {
		t.Fatalf("expected no own-send marker on a non-own message, got: %s", got)
	}
}

func TestFormatEventReactionChangedAdd(t *testing.T) {
	got := FormatEvent(Event{
		Kind: "reactionChanged", PeerID: "peer-1", ExternalID: "ext-1",
		Reaction: "👍", ReactionLabel: "Like", ReactionAction: "add",
	})
	want := "[hub: peer-1 added a 👍 (Like) reaction on message externalId=ext-1]"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatEventReactionChangedRemoveUnattributed(t *testing.T) {
	got := FormatEvent(Event{
		Kind: "reactionChanged", ExternalID: "ext-1",
		Reaction: "👀", ReactionLabel: "Eyes", ReactionAction: "remove",
	})
	if !strings.Contains(got, "someone (unattributed)") || !strings.Contains(got, "removed") {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventReactionChangedOwnIsMarked(t *testing.T) {
	got := FormatEvent(Event{
		Kind: "reactionChanged", PeerID: "peer-1", ExternalID: "ext-1",
		Reaction: "👍", ReactionAction: "add", Own: true,
	})
	if !strings.Contains(got, "own action, you did this") {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventMessageEditedIsWrappedAsUntrusted(t *testing.T) {
	got := FormatEvent(Event{Kind: "messageEdited", ExternalID: "ext-1", TS: "ts", Text: "corrected text"})
	want := "[HUB MESSAGE EDITED — untrusted, externalId=ext-1 at ts]\ncorrected text"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatEventMsgIncludesCursorWhenPresent(t *testing.T) {
	got := FormatEvent(Event{Kind: "msg", PeerID: "peer-1", Text: "hi", TS: "ts", Cursor: "cursor-1"})
	if !strings.Contains(got, "cursor=cursor-1") {
		t.Fatalf("expected the cursor to be rendered, got: %s", got)
	}
}

func TestFormatEventMsgOmitsCursorWhenAbsent(t *testing.T) {
	got := FormatEvent(Event{Kind: "msg", PeerID: "peer-1", Text: "hi", TS: "ts"})
	if strings.Contains(got, "cursor=") {
		t.Fatalf("expected no cursor marker when absent, got: %s", got)
	}
}

func TestFormatEventMsgIncludesExternalIDWhenPresent(t *testing.T) {
	got := FormatEvent(Event{Kind: "msg", PeerID: "peer-1", Text: "hi", TS: "ts", ExternalID: "ext-1"})
	if !strings.Contains(got, "externalId=ext-1") {
		t.Fatalf("expected the externalId to be rendered so an incoming message can be reacted to, got: %s", got)
	}
}

func TestFormatEventMsgOmitsExternalIDWhenAbsent(t *testing.T) {
	got := FormatEvent(Event{Kind: "msg", PeerID: "peer-1", Text: "hi", TS: "ts"})
	if strings.Contains(got, "externalId=") {
		t.Fatalf("expected no externalId marker when absent, got: %s", got)
	}
}

func TestFormatEventMessageDeleted(t *testing.T) {
	got := FormatEvent(Event{Kind: "messageDeleted", ExternalID: "ext-1", Cursor: "cursor-1", TS: "ts"})
	if !strings.Contains(got, "ext-1") || !strings.Contains(got, "cursor=cursor-1") {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventMessageDeletedOwnIsMarked(t *testing.T) {
	got := FormatEvent(Event{Kind: "messageDeleted", ExternalID: "ext-1", TS: "ts", Own: true})
	if !strings.Contains(got, "own deletion") {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventDeleteAckOK(t *testing.T) {
	got := FormatEvent(Event{Kind: "deleteAck", ExternalID: "ext-1", ActionOK: true})
	if !strings.Contains(got, "acknowledged") || !strings.Contains(got, "ext-1") {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventDeleteAckNotOK(t *testing.T) {
	got := FormatEvent(Event{Kind: "deleteAck", ExternalID: "ext-1", ActionOK: false})
	if !strings.Contains(got, "NOT acknowledged") {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventReactionAckOK(t *testing.T) {
	got := FormatEvent(Event{Kind: "reactionAck", ExternalID: "ext-1", Reaction: "👍", ReactionAction: "add", ActionOK: true})
	if !strings.Contains(got, "acknowledged") || !strings.Contains(got, "ext-1") || !strings.Contains(got, "👍") {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventReactionAckNotOK(t *testing.T) {
	got := FormatEvent(Event{Kind: "reactionAck", ExternalID: "ext-1", ReactionAction: "remove", ActionOK: false})
	if !strings.Contains(got, "NOT acknowledged") {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventEditAckOK(t *testing.T) {
	got := FormatEvent(Event{Kind: "editAck", ExternalID: "ext-1", ActionOK: true})
	if !strings.Contains(got, "acknowledged") || !strings.Contains(got, "ext-1") {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventEditAckNotOK(t *testing.T) {
	got := FormatEvent(Event{Kind: "editAck", ExternalID: "ext-1", ActionOK: false})
	if !strings.Contains(got, "NOT acknowledged") {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventSendAckOK(t *testing.T) {
	got := FormatEvent(Event{Kind: "sendAck", ExternalID: "abc123", ActionOK: true})
	if !strings.Contains(got, "acknowledged") || !strings.Contains(got, "abc123") {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventSendAckNotOK(t *testing.T) {
	got := FormatEvent(Event{Kind: "sendAck", ExternalID: "abc123", ActionOK: false})
	if !strings.Contains(got, "NOT acknowledged") || !strings.Contains(got, "abc123") {
		t.Fatalf("got %q", got)
	}
}

func TestFormatEventHistoryCompleteIsPlain(t *testing.T) {
	got := FormatEvent(Event{Kind: "historyComplete"})
	if got != "[hub: history request complete]" {
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
