package hubconn

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

// correlatingFeatures is a server that declares both action acks and the
// correlation id. Without the second key this client sends no id at all,
// which is what every other test in this package exercises.
func correlatingFeatures() map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"actionAcks":            json.RawMessage(`{}`),
		wire.FeatureCorrelation: json.RawMessage(`{}`),
	}
}

// serialWriter is the fixture rule this package learned the hard way: a
// websocket permits one concurrent writer, and a test server that answers
// on a goroutine while its read loop also writes is a data race in the
// test rather than in the code under test.
type serialWriter struct {
	mu sync.Mutex
	ws *websocket.Conn
}

func (w *serialWriter) write(v any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ws.WriteJSON(v)
}

// startCorrelatingServer serves one connection, declaring the correlation
// feature, and hands every inbound frame to handle.
func startCorrelatingServer(t *testing.T, features map[string]json.RawMessage,
	handle func(w *serialWriter, frame map[string]any)) string {
	t.Helper()
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(rw, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", "")
		joined.Features = features
		ws.WriteJSON(joined)
		w := &serialWriter{ws: ws}
		for {
			var frame map[string]any
			if err := ws.ReadJSON(&frame); err != nil {
				return
			}
			handle(w, frame)
		}
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func shortAckTimeout(t *testing.T) {
	t.Helper()
	prev := AckWaitTimeout
	AckWaitTimeout = 150 * time.Millisecond
	t.Cleanup(func() { AckWaitTimeout = prev })
}

// AN ANSWER NOBODY ASKED FOR MUST NOT BE TAKEN AS AN ANSWER. A sendAck
// arrives bearing an id this client never sent, while a send of its own
// is waiting. Keyed by kind — with no late-answer debt in play, because
// nothing has timed out — that ack is diverted to the waiting caller and
// its externalId is reported as that send's outcome: a confident wrong
// answer about a message that was never this caller's.
//
// The debt cannot reach this one. It is recorded only by a timeout, and
// there has not been one; the whole defence here is that the id does not
// match. Deliberately chosen as the case that separates the two schemes
// rather than one both happen to handle.
func TestAnAnswerBearingAnUnknownIdIsNotDivertedToAWaitingCaller(t *testing.T) {
	shortAckTimeout(t)

	base := startCorrelatingServer(t, correlatingFeatures(), func(w *serialWriter, frame map[string]any) {
		if frame["type"] != string(wire.TypeMsg) {
			return
		}
		// Answers the send with an ack for some OTHER request.
		w.write(wire.SendAck{Type: wire.TypeSendAck, ID: "an-id-this-client-never-chose",
			ExternalID: "somebody-elses-message", OK: true})
	})

	c, err := dialTest(base, "corr-foreign", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	c.mu.Lock()
	debt := len(c.lateAnswers)
	c.mu.Unlock()
	if debt != 0 {
		t.Fatalf("this test is only meaningful with no late-answer debt outstanding; found %d", debt)
	}

	ev, ok, err := c.SendAwaitingAck("mine", "", nil, "", "", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if ok {
		t.Fatalf("an ack for %q, carrying an id this client never sent, was reported as this send's "+
			"own outcome (%+v) — the id names the request and a foreign one answers nothing here",
			ev.ExternalID, ev)
	}
}

// The late-answer debt must not swallow a CORRELATED answer — that is
// the ratchet, and it is the only thing the debt was ever accused of.
//
// This test previously asserted that no debt was recorded at all against
// a correlating server, and that assertion was wrong in a way worth
// keeping a note of. Suppressing the debt at the connection's
// DECLARATION left a server that declares the feature and has not yet
// echoed it on some kind — every server mid-rollout — with kind-only
// matching and no mitigation, which is strictly worse than never
// declaring it. Found by the external reviewer.
//
// The debt is recorded again. What changed is where the knowledge is
// applied: it can only be SPENT against an answer that carries no id,
// because an answer that names its request is knowledge and the debt is
// a guess. So it protects exactly the kinds a server has not echoed, and
// cannot touch the ones it has.
func TestACorrelatedAnswerIsNotSwallowedByAnEarlierTimeoutsDebt(t *testing.T) {
	shortAckTimeout(t)

	var mu sync.Mutex
	n := 0
	base := startCorrelatingServer(t, correlatingFeatures(), func(w *serialWriter, frame map[string]any) {
		if frame["type"] != string(wire.TypeMsg) {
			return
		}
		mu.Lock()
		n++
		seq := n
		mu.Unlock()
		if seq == 1 {
			return // never answered: this caller times out and records a debt
		}
		id, _ := frame["id"].(string)
		text, _ := frame["text"].(string)
		w.write(wire.SendAck{Type: wire.TypeSendAck, ID: id, ExternalID: text, OK: true})
	})

	c, err := dialTest(base, "corr-noratchet", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if _, ok, _ := c.SendAwaitingAck("first", "", nil, "", "", nil); ok {
		t.Fatal("the first send was supposed to time out")
	}

	// The debt from that timeout exists. It must not be charged against
	// the next send's own, correctly correlated, answer.
	ev, ok, err := c.SendAwaitingAck("second", "", nil, "", "", nil)
	if err != nil || !ok {
		t.Fatalf("the second send was answered promptly, with its own correlation id, and still did "+
			"not resolve: ok=%v err=%v — the earlier timeout's debt was spent against an answer whose "+
			"owner was certain", ok, err)
	}
	if ev.ExternalID != "second" {
		t.Fatalf("got %q, want the second send's own ack", ev.ExternalID)
	}
}

// An error naming a request reaches that request even with others
// outstanding. Uncorrelated, this is the case given to NOBODY — two
// honest timeouts rather than one confident wrong answer. With an id
// there is nothing to be ambiguous about, so the honest outcome is the
// accurate one.
func TestACorrelatedErrorReachesItsOwnRequestWithOthersPending(t *testing.T) {
	shortAckTimeout(t)

	base := startCorrelatingServer(t, correlatingFeatures(), func(w *serialWriter, frame map[string]any) {
		switch frame["type"] {
		case string(wire.TypeMessageAfter):
			// Left outstanding on purpose: a second claim pending is what
			// made the uncorrelated path ambiguous.
		case string(wire.TypeMsg):
			id, _ := frame["id"].(string)
			w.write(wire.Error{Type: wire.TypeError, ID: id, Code: "send_refused",
				Message: "the tenant refused this send", Retryable: false})
		}
	})

	c, err := dialTest(base, "corr-error", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// The walk is left outstanding on purpose and must be JOINED before
	// the test returns: shortAckTimeout restores the package-level
	// AckWaitTimeout from t.Cleanup, and a goroutine still inside
	// RequestMessageAfterAwaiting reads it — a fixture race, not a race
	// in the code under test, and the same one this package has now been
	// bitten by three times.
	walking := make(chan struct{})
	walked := make(chan struct{})
	go func() {
		defer close(walked)
		close(walking)
		c.RequestMessageAfterAwaiting(wire.Anchor{Cursor: "somewhere"})
	}()
	defer func() { <-walked }()
	<-walking
	time.Sleep(20 * time.Millisecond)

	ev, ok, err := c.SendAwaitingAck("refused", "", nil, "", "", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if !ok {
		t.Fatal("the send's own refusal did not reach it, although the error named its id — with a " +
			"correlation id there is no ambiguity for the two-claims rule to protect against")
	}
	if ev.Kind != "error" || ev.Code != "send_refused" {
		t.Fatalf("got %+v, want the send_refused error", ev)
	}
}

// An error with NO id, from a server that promised to echo one, is not
// anybody's outcome. This is the inference the feature exists to remove:
// before it, an unsolicited error arriving while one claim was pending
// was reported to that caller as its own result.
func TestAnUncorrelatedErrorFromACorrelatingServerReachesNobody(t *testing.T) {
	shortAckTimeout(t)

	base := startCorrelatingServer(t, correlatingFeatures(), func(w *serialWriter, frame map[string]any) {
		if frame["type"] != string(wire.TypeMsg) {
			return
		}
		// No id: by the protocol this is unsolicited — a close reason, a
		// server-side refusal with no frame behind it — not this send's
		// answer.
		w.write(wire.Error{Type: wire.TypeError, Code: "conversation_renamed",
			Message: "somebody renamed the conversation", Retryable: false})
	})

	c, err := dialTest(base, "corr-unsolicited", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.SendAwaitingAck("hello", "", nil, "", "", nil)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if ok {
		t.Fatalf("an error carrying no id was reported as this send's outcome (%+v) — the server "+
			"declares the correlation feature, so a missing id means the error is not about a "+
			"request, and reading the absence as an answer is the misattribution being removed", ev)
	}
	// It is not swallowed either: the reader still sees it.
	var seen bool
	events, _ := c.DrainEvents()
	for _, e := range events {
		if e.Kind == "error" && e.Code == "conversation_renamed" {
			seen = true
		}
	}
	if !seen {
		t.Fatal("the unsolicited error reached neither the caller nor the buffer — delivering it to " +
			"nobody must not mean discarding it")
	}
}

// bad_correlation refuses one request and says nothing about the
// connection. It must never be treated as fatal, and the allowlist that
// guarantees that is pinned here rather than left true by luck.
func TestBadCorrelationIsNotFatalToTheConnection(t *testing.T) {
	if fatalErrorCodes[wire.CodeBadCorrelation] {
		t.Fatalf("%q is in fatalErrorCodes — it refuses one request whose id was malformed or over "+
			"length, which says nothing about whether this conversation still exists",
			wire.CodeBadCorrelation)
	}
	if len(fatalErrorCodes) != 1 || !fatalErrorCodes["conversation_unavailable"] {
		t.Fatalf("fatalErrorCodes is %v; it is deliberately the single code whose meaning is "+
			"\"this conversation is gone\", and every addition needs the server's own guarantee",
			fatalErrorCodes)
	}
}

// The id is sent only where a server said it would be echoed. Against a
// server without the feature the wire is byte-for-byte what it was, which
// is what makes the feature safe to adopt unilaterally.
func TestNoIdIsSentToAServerThatDoesNotDeclareTheFeature(t *testing.T) {
	shortAckTimeout(t)

	frames := make(chan map[string]any, 4)
	base := startCorrelatingServer(t,
		map[string]json.RawMessage{"actionAcks": json.RawMessage(`{}`)},
		func(w *serialWriter, frame map[string]any) {
			if frame["type"] != string(wire.TypeMsg) {
				return
			}
			frames <- frame
			w.write(wire.SendAck{Type: wire.TypeSendAck, ExternalID: "x", OK: true})
		})

	c, err := dialTest(base, "corr-absent", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if c.correlates {
		t.Fatal("the server declared no correlation feature and this connection believes it did")
	}
	if _, _, err := c.SendAwaitingAck("plain", "", nil, "", "", nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case frame := <-frames:
		if _, present := frame["id"]; present {
			t.Fatalf("an id was sent to a server that never promised to echo it: %+v", frame)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the server never saw the send")
	}
}

// A correlating server's answer to a send must carry the id this client
// chose, and the client must actually put one on the wire — the two
// halves of the same contract, checked together so neither can pass by
// the other being broken.
func TestTheIdThisClientChoseIsWhatComesBack(t *testing.T) {
	shortAckTimeout(t)

	sent := make(chan string, 1)
	base := startCorrelatingServer(t, correlatingFeatures(), func(w *serialWriter, frame map[string]any) {
		if frame["type"] != string(wire.TypeMsg) {
			return
		}
		id, _ := frame["id"].(string)
		sent <- id
		w.write(wire.SendAck{Type: wire.TypeSendAck, ID: id, ExternalID: "landed", OK: true})
	})

	c, err := dialTest(base, "corr-echo", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.SendAwaitingAck("hello", "", nil, "", "", nil)
	if err != nil || !ok {
		t.Fatalf("send: ok=%v err=%v", ok, err)
	}
	id := <-sent
	if id == "" {
		t.Fatal("no correlation id was sent although the server declared the feature")
	}
	if len(id) > wire.MaxCorrelationIDLen {
		t.Fatalf("the id this client mints is %d bytes, over the agreed %d cap",
			len(id), wire.MaxCorrelationIDLen)
	}
	if ev.CorrelationID != id {
		t.Fatalf("the ack came back with id %q, want the %q this client sent", ev.CorrelationID, id)
	}
}

// The declared cap is read, not assumed. A server that says it accepts
// less than this client's id would refuse every correlated request, so
// the feature is declined and the late-answer debt covers the connection
// instead — a working connection rather than one where every request
// comes back bad_correlation.
func TestACapTooSmallForTheIdMeansNotCorrelating(t *testing.T) {
	shortAckTimeout(t)

	frames := make(chan map[string]any, 4)
	base := startCorrelatingServer(t, map[string]json.RawMessage{
		"actionAcks":            json.RawMessage(`{}`),
		wire.FeatureCorrelation: json.RawMessage(`{"maxLength":8}`),
	}, func(w *serialWriter, frame map[string]any) {
		if frame["type"] != string(wire.TypeMsg) {
			return
		}
		frames <- frame
		w.write(wire.SendAck{Type: wire.TypeSendAck, ExternalID: "x", OK: true})
	})

	c, err := dialTest(base, "corr-small-cap", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if c.correlates {
		t.Fatal("a maxLength of 8 cannot hold a uuid, so every correlated request would be refused " +
			"with bad_correlation — this connection must fall back rather than send one")
	}
	if _, _, err := c.SendAwaitingAck("plain", "", nil, "", "", nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case frame := <-frames:
		if _, present := frame["id"]; present {
			t.Fatalf("an id was sent to a server whose declared cap is too small for it: %+v", frame)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the server never saw the send")
	}
}

// A cap large enough, and a declaration with no cap at all, both
// correlate. The second matters on its own: an absent maxLength says the
// server states no limit, and reading it as zero would decline a feature
// that was offered.
func TestADeclaredCapThatFitsOrIsAbsentStillCorrelates(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
	}{
		{"the agreed cap", `{"maxLength":128}`},
		{"no cap stated", `{}`},
		{"a later extension this client does not know", `{"somethingElse":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := startCorrelatingServer(t, map[string]json.RawMessage{
				"actionAcks":            json.RawMessage(`{}`),
				wire.FeatureCorrelation: json.RawMessage(tc.payload),
			}, func(w *serialWriter, frame map[string]any) {})
			c, err := dialTest(base, "corr-cap-"+tc.name, DialOptions{})
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer c.Close()
			if !c.correlates {
				t.Fatalf("features payload %s declined the feature; it should be taken at its word",
					tc.payload)
			}
		})
	}
}

// THE CASE NEITHER SIDE HAD. A history walk's terminator, noMoreMessages,
// carries no correlation id today: the echo agreed with chat-relay covers
// the acks and "error", and this frame is neither. A client that demanded
// an id on a history answer would therefore never see a walk end against
// the very server that declares the feature — every hub_catch_up would
// report a timeout instead of "caught up", which is a worse failure than
// the misattribution being fixed and one this client would have inflicted
// on itself by assuming an echo nobody promised.
//
// So an answer with no id falls back to the anchor rule. Both directions
// are asserted here because each one alone passes a broken
// implementation: the id case passes a client that ignores ids if the
// anchors differ, and the fallback passes a client that ignores ids
// entirely.
func TestAnIdLessTerminatorStillEndsAWalkAgainstACorrelatingServer(t *testing.T) {
	shortAckTimeout(t)

	base := startCorrelatingServer(t, map[string]json.RawMessage{
		wire.FeatureCorrelation: json.RawMessage(`{"maxLength":128}`),
		"messageAfter":          json.RawMessage(`{}`),
	}, func(w *serialWriter, frame map[string]any) {
		if frame["type"] != string(wire.TypeMessageAfter) {
			return
		}
		// Exactly what chat-relay sends today: the terminator, echoing
		// the anchor and nothing else.
		w.write(wire.NoMoreMessages{Type: wire.TypeNoMoreMessages,
			Answers: &wire.Anchor{Cursor: "anchor-here"}})
	})

	c, err := dialTest(base, "corr-terminator", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if !c.correlates {
		t.Fatal("the server declared the correlation feature and this connection did not take it up")
	}

	ev, ok, err := c.RequestMessageAfterAwaiting(wire.Anchor{Cursor: "anchor-here"})
	if err != nil {
		t.Fatalf("history request: %v", err)
	}
	if !ok {
		t.Fatal("an id-less noMoreMessages did not end the walk — a client that demands a correlation " +
			"id on a history answer hangs on every catch-up against a correlating server, because " +
			"the terminator was never in the agreed echo")
	}
	if ev.Kind != "noMoreMessages" {
		t.Fatalf("got %q, want the terminator", ev.Kind)
	}
}

// And once a server DOES echo it on the terminator, the id is believed:
// a terminator bearing another request's id must not end this walk. The
// field exists for that day; nothing sends it yet.
func TestATerminatorBearingAnotherRequestsIdDoesNotEndThisWalk(t *testing.T) {
	shortAckTimeout(t)

	base := startCorrelatingServer(t, map[string]json.RawMessage{
		wire.FeatureCorrelation: json.RawMessage(`{"maxLength":128}`),
		"messageAfter":          json.RawMessage(`{}`),
	}, func(w *serialWriter, frame map[string]any) {
		if frame["type"] != string(wire.TypeMessageAfter) {
			return
		}
		w.write(wire.NoMoreMessages{Type: wire.TypeNoMoreMessages,
			ID:      "the-id-of-some-earlier-request",
			Answers: &wire.Anchor{Cursor: "anchor-here"}})
	})

	c, err := dialTest(base, "corr-foreign-terminator", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.RequestMessageAfterAwaiting(wire.Anchor{Cursor: "anchor-here"})
	if err != nil {
		t.Fatalf("history request: %v", err)
	}
	if ok {
		t.Fatalf("a terminator carrying another request's id ended this walk (%+v) — where the answer "+
			"states an id, that id is the whole test", ev)
	}
}

// attachmentData answers a request and carries no id today — the echo
// agreed with chat-relay covers the acks and "error", and this frame is
// neither. The claim for it mints an id like every other, so a rule that
// demands one on the answer makes every attachment fetch time out against
// the server that declares the feature: the same hang the history path
// was just saved from, one path over.
func TestAnIdLessAttachmentAnswerStillResolvesItsRequest(t *testing.T) {
	shortAckTimeout(t)

	base := startCorrelatingServer(t, map[string]json.RawMessage{
		wire.FeatureCorrelation: json.RawMessage(`{"maxLength":128}`),
		"attachments":           json.RawMessage(`{}`),
	}, func(w *serialWriter, frame map[string]any) {
		if frame["type"] != string(wire.TypeAttachment) {
			return
		}
		token, _ := frame["token"].(string)
		w.write(wire.AttachmentData{Type: wire.TypeAttachmentData, Token: token,
			Name: "f.txt", ContentType: "text/plain", ContentBytes: "aGk="})
	})

	c, err := dialTest(base, "corr-attach", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if !c.correlates {
		t.Fatal("the server declared the correlation feature and this connection did not take it up")
	}

	ev, ok, err := c.RequestAttachment("tok-1", nil)
	if err != nil {
		t.Fatalf("attachment request: %v", err)
	}
	if !ok {
		t.Fatal("an id-less attachmentData did not resolve its request — the claim minted an id the " +
			"server never promised to echo, so every attachment fetch hangs against a correlating " +
			"server. The token already narrows this claim; the id must not be demanded on top of it")
	}
	if ev.AttachmentToken != "tok-1" {
		t.Fatalf("got token %q, want tok-1", ev.AttachmentToken)
	}
}

// Every frame chat-relay echoes the id on must arrive with it readable.
//
// This exists because a decode line being PRESENT proved nothing twice
// today: `msg` had no CorrelationID at all while ten siblings did, and
// the server side had a reflection test proving its records could carry
// the field while four call sites set nothing. Both gaps were invisible
// from a green suite and from reading the list.
//
// So the list is walked rather than reasoned about — chat-relay's own
// enumeration of what it echoes, checked end to end through the real
// decoder.
func TestTheIdSurvivesDecodingOnEveryFrameThatEchoesIt(t *testing.T) {
	const id = "the-echoed-id"
	for _, tc := range []struct {
		kind  string
		frame any
	}{
		{"sendAck", wire.SendAck{Type: wire.TypeSendAck, ID: id}},
		{"reactionAck", wire.ReactionAck{Type: wire.TypeReactionAck, ID: id}},
		{"editAck", wire.EditAck{Type: wire.TypeEditAck, ID: id}},
		{"deleteAck", wire.DeleteAck{Type: wire.TypeDeleteAck, ID: id}},
		{"pinAck", wire.PinAck{Type: wire.TypePinAck, ID: id}},
		{"unpinAck", wire.UnpinAck{Type: wire.TypeUnpinAck, ID: id}},
		{"ack", wire.Ack{Type: wire.TypeAck, ID: id}},
		{"error", wire.Error{Type: wire.TypeError, ID: id, Message: "no"}},
		{"msg", wire.Msg{Type: wire.TypeMsg, ID: id, Text: "hi",
			PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8"}},
		{"noMoreMessages", wire.NoMoreMessages{Type: wire.TypeNoMoreMessages, ID: id}},
		{"pins", wire.PinsResponse{Type: wire.TypePins, ID: id}},
		{"attachmentData", wire.AttachmentData{Type: wire.TypeAttachmentData, ID: id,
			Token: "t", ContentType: "text/plain"}},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			raw, err := json.Marshal(tc.frame)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			ev, ok := decodeEvent(raw)
			if !ok {
				t.Fatalf("decodeEvent rejected %s: %s", tc.kind, raw)
			}
			if ev.Kind != tc.kind {
				t.Fatalf("decoded as %q, want %q", ev.Kind, tc.kind)
			}
			if ev.CorrelationID != id {
				t.Fatalf("the id was dropped decoding %s: got %q, want %q — the frame carries it and "+
					"nothing downstream can correlate what the decoder threw away",
					tc.kind, ev.CorrelationID, id)
			}
		})
	}
}

// A confirm reminder is composed on a ticker and appended to the buffer;
// the model sees it whenever it next drains. Everything in it is a
// snapshot of the compose moment, so a reader that confirms in between is
// then told to confirm what it has already confirmed.
//
// Reported four times by a peer who could see the text and not the cause:
// the reminder named the cursor just piggybacked, once with a count of 2
// and once with an age that had advanced five minutes while nothing
// arrived.
func TestAConfirmReminderIsNotDeliveredAfterItsPremiseIsAnswered(t *testing.T) {
	c := &Conn{}

	c.mu.Lock()
	c.lastSeenCursor = "cursor-1"
	c.liveUnconfirmed = true
	c.unconfirmedCount = 2
	c.unconfirmedSince = time.Now().Add(-8 * time.Minute)
	c.buffer = []Event{
		{Kind: "msg", Text: "something", Cursor: "cursor-1"},
		{Kind: "confirmReminder", Text: "cursor-1", UnconfirmedCount: 2},
	}
	c.mu.Unlock()

	// The reader confirms before draining, which is exactly what a
	// piggybacked confirm on a reply does.
	c.MarkConsumed([]Event{{Cursor: "cursor-1"}})

	events, _ := c.DrainEvents()
	for _, e := range events {
		if e.Kind == "confirmReminder" {
			t.Fatalf("a reminder composed before the confirm was delivered after it, telling the "+
				"reader to confirm %q — which it had already confirmed: %+v", e.Text, e)
		}
	}
	if len(events) != 1 || events[0].Kind != "msg" {
		t.Fatalf("dropping the stale reminder disturbed the rest of the buffer: %+v", events)
	}
}

// A reminder whose premise still holds is delivered, and rewritten with
// the position as it stands at the drain rather than at the tick — a
// stale COUNT is the same defect one size smaller.
func TestASurvivingConfirmReminderCarriesTheCurrentPosition(t *testing.T) {
	c := &Conn{}
	since := time.Now().Add(-3 * time.Minute)

	c.mu.Lock()
	c.lastSeenCursor = "cursor-9"
	c.liveUnconfirmed = true
	c.unconfirmedCount = 4
	c.unconfirmedSince = since
	// Two ticks passed before anything drained, and the older one names a
	// position that has since been overtaken.
	c.buffer = []Event{
		{Kind: "confirmReminder", Text: "cursor-3", UnconfirmedCount: 1},
		{Kind: "msg", Cursor: "cursor-9"},
		{Kind: "confirmReminder", Text: "cursor-7", UnconfirmedCount: 3},
	}
	c.mu.Unlock()

	events, _ := c.DrainEvents()
	var reminders []Event
	for _, e := range events {
		if e.Kind == "confirmReminder" {
			reminders = append(reminders, e)
		}
	}
	if len(reminders) != 1 {
		t.Fatalf("got %d reminders, want exactly one — repeating one instruction per missed tick "+
			"says nothing the first did not: %+v", len(reminders), reminders)
	}
	r := reminders[0]
	if r.Text != "cursor-9" {
		t.Errorf("the reminder names %q, want the position as it stands at the drain", r.Text)
	}
	if r.UnconfirmedCount != 4 {
		t.Errorf("the reminder says %d outstanding, want 4 — the count at the drain", r.UnconfirmedCount)
	}
	if !r.UnconfirmedSince.Equal(since) {
		t.Errorf("the reminder's age was not refreshed from the connection's own state")
	}
	if len(events) != 2 {
		t.Fatalf("the rest of the buffer was disturbed: %+v", events)
	}
}

// THE BUG THIS SPLIT EXISTS FOR. A reader is handed messages 1..5, sends
// something (which piggybacks a receipt), then finds message 4 arrived
// truncated and confirms 3 — exactly what this client's own confirm
// reminder asks for, "the last message you have COMPLETE, possibly
// earlier than" the last delivered.
//
// While the piggyback carried lastConsumed, that send had already told a
// monotonic server the position was 5, and the reader's honest 3 came
// back refused. The reader was punished for obeying an instruction this
// client wrote, because this client had answered the question on its
// behalf first.
func TestASendDoesNotAssertAPositionTheReaderHasNotConfirmed(t *testing.T) {
	sends := make(chan wire.Msg, 4)
	base := startCorrelatingServer(t, map[string]json.RawMessage{},
		func(w *serialWriter, frame map[string]any) {
			if frame["type"] != string(wire.TypeMsg) {
				return
			}
			raw, _ := json.Marshal(frame)
			var m wire.Msg
			json.Unmarshal(raw, &m)
			sends <- m
		})

	c, err := dialTest(base, "confirmed-only", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// Five delivered and handed to the model; nothing confirmed.
	var events []Event
	for _, cur := range []string{"c1", "c2", "c3", "c4", "c5"} {
		events = append(events, Event{Kind: "msg", Cursor: cur})
	}
	c.MarkConsumed(events)

	if err := c.Send("a reply", nil, "", "", nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case m := <-sends:
		if m.AckCursor != "" {
			t.Fatalf("the send asserted position %q, which the reader never confirmed — a monotonic "+
				"server now holds it, and the reader's own honest, lower confirm will be refused",
				m.AckCursor)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the server never saw the send")
	}

	// The reader confirms what it actually has complete: c3, not c5.
	if _, err := c.ConfirmReceived("c3"); err != nil {
		t.Fatalf("ConfirmReceived: %v", err)
	}
	if got := c.ConfirmedCursor(); got != "c3" {
		t.Fatalf("ConfirmedCursor is %q, want c3", got)
	}

	// And THAT is what rides out from now on.
	if err := c.Send("another", nil, "", "", nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case m := <-sends:
		if m.AckCursor != "c3" {
			t.Fatalf("the send piggybacked %q, want the confirmed c3", m.AckCursor)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the server never saw the second send")
	}
}
