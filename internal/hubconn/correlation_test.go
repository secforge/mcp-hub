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

// The late-answer debt is the fallback for a server with no id, and it
// must not run alongside one. Recorded here, it would never be spendable
// — a late answer carries the dead request's id and matches no claim — so
// it would only ever increment, and the next correctly-correlated answer
// would be swallowed against it. That is the ratchet in reverse.
func TestTheLateAnswerDebtIsNotRecordedAgainstACorrelatingServer(t *testing.T) {
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
			return // never answered: this caller times out
		}
		id, _ := frame["id"].(string)
		text, _ := frame["text"].(string)
		w.write(wire.SendAck{Type: wire.TypeSendAck, ID: id, ExternalID: text, OK: true})
	})

	c, err := dialTest(base, "corr-nodebt", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if _, ok, _ := c.SendAwaitingAck("first", "", nil, "", "", nil); ok {
		t.Fatal("the first send was supposed to time out")
	}
	c.mu.Lock()
	debt := c.lateAnswers["sendAck"]
	c.mu.Unlock()
	if debt != 0 {
		t.Fatalf("a late-answer debt of %d was recorded against a correlating server — the id "+
			"already makes a late answer unattributable, so this counter can only ratchet", debt)
	}

	ev, ok, err := c.SendAwaitingAck("second", "", nil, "", "", nil)
	if err != nil || !ok {
		t.Fatalf("the second send was answered promptly and still did not resolve: ok=%v err=%v", ok, err)
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
