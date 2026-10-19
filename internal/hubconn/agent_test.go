package hubconn

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/agenttest"
	"github.com/secforge/mcp-hub/internal/wire"
)

const testPeer = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

func grant(conv, title, access string) wire.Assignment {
	return wire.Assignment{Conv: conv, Title: title, Kind: "group", Channel: "teams", Access: access}
}

func dialAgent(t *testing.T, srv *agenttest.Server) *Agent {
	t.Helper()
	conn, agent, err := DialAny(srv.Link(), DialOptions{ReconnectSecret: "agent-secret", Name: "tester"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if conn != nil || agent == nil {
		t.Fatalf("an agent link came back as a single conversation (conn=%v agent=%v)", conn, agent)
	}
	t.Cleanup(func() { agent.Close() })
	return agent
}

func openConv(t *testing.T, a *Agent, conv string) *Conn {
	t.Helper()
	c, err := a.Open(conv, nil)
	if err != nil {
		t.Fatalf("open %s: %v", conv, err)
	}
	return c
}

func waitEvents(t *testing.T, c *Conn, want func([]Event) bool) []Event {
	t.Helper()
	var got []Event
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		evs, _ := c.DrainEvents()
		got = append(got, evs...)
		if want(got) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out; got %+v", got)
	return nil
}

func hasText(evs []Event, text string) bool {
	for _, e := range evs {
		if e.Kind == "msg" && e.Text == text {
			return true
		}
	}
	return false
}

func waitDisconnected(t *testing.T, c *Conn) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for c.Connected() {
		if time.Now().After(deadline) {
			t.Fatal("conversation still connected")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAnAgentLinkIsRecognisedByItsFirstFrameAndListsItsConversations(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite), grant("c-b", "Beta", wire.AccessRead))
	a := dialAgent(t, srv)
	if a.AgentID() != agenttest.AgentID {
		t.Errorf("agent id %q", a.AgentID())
	}
	got := a.Assignments()
	if len(got) != 2 || got[0].Conv != "c-a" || got[1].Access != wire.AccessRead {
		t.Errorf("assignments %+v", got)
	}
	if h := srv.Headers(); len(h) != 1 || h[0].Get("Hub-Protocol-Version") != "5" {
		t.Errorf("the dial did not declare protocol v5: %v", h)
	}
}

func TestDialRefusesAnAgentLinkRatherThanStandingForOneConversation(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite))
	_, err := Dial(srv.Link(), DialOptions{ReconnectSecret: "s"})
	if !errors.Is(err, ErrAgentLink) {
		t.Fatalf("got %v, want ErrAgentLink", err)
	}
}

func TestAnOrdinaryLinkStillDialsAsOneConversationWithTheAgentHeaderSent(t *testing.T) {
	base := startTestServer(t)
	conn, agent, err := DialAny(testLink(base, "550e8400-e29b-41d4-a716-446655440077"),
		DialOptions{ReconnectSecret: "s-ordinary"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if agent != nil || conn == nil {
		t.Fatal("an ordinary link came back as an agent")
	}
}

func TestEachConversationReceivesOnlyItsOwnFrames(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite), grant("c-b", "Beta", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	ca, cb := openConv(t, a, "c-a"), openConv(t, a, "c-b")
	if ca.PeerID() != agenttest.PeerFor("c-a") || cb.PeerID() != agenttest.PeerFor("c-b") {
		t.Fatalf("peer ids %s %s", ca.PeerID(), cb.PeerID())
	}
	srv.Msg("c-a", testPeer, "for alpha", "1")
	srv.Msg("c-b", testPeer, "for beta", "2")
	evA := waitEvents(t, ca, func(e []Event) bool { return hasText(e, "for alpha") })
	evB := waitEvents(t, cb, func(e []Event) bool { return hasText(e, "for beta") })
	if hasText(evA, "for beta") || hasText(evB, "for alpha") {
		t.Fatalf("a frame reached the other conversation: a=%+v b=%+v", evA, evB)
	}
}

func TestASendIsWrappedAndStampedForItsOwnConversationOnly(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite), grant("c-b", "Beta", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	ca := openConv(t, a, "c-a")
	openConv(t, a, "c-b")
	if err := ca.Send("hello alpha", nil, "", "", nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	if !srv.WaitFor(3*time.Second, func() bool { return len(srv.Frames("c-a")) > 0 }) {
		t.Fatal("nothing arrived for c-a")
	}
	for conv, frames := range srv.AllFrames() {
		for _, f := range frames {
			if f.Wrapper != f.Inner {
				t.Errorf("frame %s wrapped for %q stamped %q", f.Type, f.Wrapper, f.Inner)
			}
			if f.Type == wire.TypeMsg && conv != "c-a" {
				t.Errorf("the send reached %q", conv)
			}
		}
	}
}

func TestAReadOnlyConversationRefusesWritesBeforeSending(t *testing.T) {
	srv := agenttest.New(t, grant("c-r", "Readonly", wire.AccessRead))
	a := dialAgent(t, srv)
	c := openConv(t, a, "c-r")
	if !c.ReadOnly() || c.CanSend() {
		t.Fatalf("read grant not reflected: readOnly=%v canSend=%v", c.ReadOnly(), c.CanSend())
	}
	if err := c.Send("must not go", nil, "", "", nil); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("send on a read grant: %v", err)
	}
	if err := c.React("ext-1", "👍", "add"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("react on a read grant: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	for _, f := range srv.Frames("c-r") {
		if readOnlyRefused[f.Type] {
			t.Fatalf("a %s frame reached the server", f.Type)
		}
	}
	if _, err := c.ConfirmReceived("cur-1"); err != nil {
		t.Fatalf("a confirm is a read and must still work: %v", err)
	}
}

func TestARevokedConversationEndsForGoodAndTheOthersStay(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite), grant("c-b", "Beta", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	var mu sync.Mutex
	var events []AgentEvent
	a.OnEvent(func(ev AgentEvent) { mu.Lock(); events = append(events, ev); mu.Unlock() })
	ca, cb := openConv(t, a, "c-a"), openConv(t, a, "c-b")
	srv.Revoke("c-b", "removed by operator")
	waitDisconnected(t, cb)
	if _, permanent := cb.PermanentFailure(); !permanent {
		t.Error("a revoked conversation reads as retryable")
	}
	if !ca.Connected() || !a.Connected() {
		t.Fatal("revoking one conversation ended another")
	}
	if !srv.WaitFor(time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) > 0 && events[0].Kind == "revoked" && events[0].Conv == "c-b"
	}) {
		t.Fatalf("no revoked event: %+v", events)
	}
}

func TestAClosedForReopenConversationIsNotPermanent(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	c := openConv(t, a, "c-a")
	srv.Change(grant("c-a", "Alpha", wire.AccessRead))
	waitDisconnected(t, c)
	if _, permanent := c.PermanentFailure(); permanent {
		t.Fatal("a session closed for reopening reads as permanent")
	}
	again := openConv(t, a, "c-a")
	if !again.ReadOnly() {
		t.Fatal("the reopened session did not take the new access")
	}
}

func TestTheAgentSocketEndingEndsEveryConversationAsRetryable(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite), grant("c-b", "Beta", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	down := make(chan AgentEvent, 1)
	a.OnEvent(func(ev AgentEvent) {
		if ev.Kind == "down" {
			down <- ev
		}
	})
	ca, cb := openConv(t, a, "c-a"), openConv(t, a, "c-b")
	srv.Drop()
	waitDisconnected(t, ca)
	waitDisconnected(t, cb)
	for _, c := range []*Conn{ca, cb} {
		if _, permanent := c.PermanentFailure(); permanent {
			t.Error("a conversation ended by the agent socket reads as permanent")
		}
	}
	select {
	case <-down:
	case <-time.After(2 * time.Second):
		t.Fatal("no down event")
	}
}

func TestARevokedAgentIsPermanent(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	srv.CloseAgent(4001, "agent revoked")
	select {
	case <-a.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("agent still up")
	}
	if _, permanent := a.PermanentFailure(); !permanent {
		t.Fatal("4001 on the agent socket reads as retryable")
	}
}

func TestOpeningAConversationNotGrantedSaysWhy(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	_, err := a.Open("c-unknown", nil)
	if err == nil || !strings.Contains(err.Error(), "not_assigned") {
		t.Fatalf("got %v", err)
	}
}

func TestAFrameForAConversationNotOpenHereIsDropped(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite), grant("c-b", "Beta", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	ca := openConv(t, a, "c-a")
	srv.Msg("c-b", testPeer, "for beta", "1")
	srv.Msg("c-a", testPeer, "for alpha", "2")
	evs := waitEvents(t, ca, func(e []Event) bool { return hasText(e, "for alpha") })
	if hasText(evs, "for beta") {
		t.Fatal("a frame for an unopened conversation was delivered to another")
	}
}

func TestClosingOneConversationTellsTheServerAndLeavesTheRest(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite), grant("c-b", "Beta", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	ca, cb := openConv(t, a, "c-a"), openConv(t, a, "c-b")
	defer SetCloseFlushGraceForTesting(0)()
	if err := ca.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !srv.WaitFor(time.Second, func() bool { return !srv.IsOpen("c-a") }) {
		t.Fatal("the server was not told")
	}
	if !cb.Connected() || !a.Connected() {
		t.Fatal("closing one conversation ended others")
	}
}

func rosterAndTexts(evs []Event) []string {
	var out []string
	for _, e := range evs {
		switch e.Kind {
		case "roster":
			out = append(out, fmt.Sprintf("roster(%d)", len(e.RosterPeers)))
		case "msg":
			out = append(out, e.Text)
		}
	}
	return out
}

func drainUntil(t *testing.T, c *Conn, n int) []string {
	t.Helper()
	var got []string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		evs, _ := c.DrainEvents()
		got = append(got, rosterAndTexts(evs)...)
		if len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out with %v", got)
	return nil
}

func TestTheRosterGoesOutInFrontOfAMessageNotOnItsOwn(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	c := openConv(t, a, "c-a")
	c.PushRosterBeforeMessages(time.Hour)
	srv.Conv("c-a", wire.NewRoster([]wire.RosterMember{{PeerID: testPeer, Name: "Alice"}}))
	time.Sleep(100 * time.Millisecond)
	if evs, _ := c.DrainEvents(); len(rosterAndTexts(evs)) != 0 {
		t.Fatalf("the roster was delivered on its own: %v", rosterAndTexts(evs))
	}
	srv.Msg("c-a", testPeer, "first", "1")
	if got := drainUntil(t, c, 2); got[0] != "roster(1)" || got[1] != "first" {
		t.Fatalf("got %v, want the roster in front of the first message", got)
	}
	srv.Msg("c-a", testPeer, "second", "2")
	if got := drainUntil(t, c, 1); len(got) != 1 || got[0] != "second" {
		t.Fatalf("got %v, want the second message alone", got)
	}
	srv.Conv("c-a", wire.NewRoster([]wire.RosterMember{{PeerID: testPeer, Name: "Alice"},
		{PeerID: "6ba7b811-9dad-11d1-80b4-00c04fd430c8", Name: "Bob"}}))
	srv.Msg("c-a", testPeer, "third", "3")
	if got := drainUntil(t, c, 2); got[0] != "roster(2)" || got[1] != "third" {
		t.Fatalf("got %v, want the changed roster in front of the next message", got)
	}
}

func TestTheRosterGoesOutAgainAfterAQuietSpell(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	c := openConv(t, a, "c-a")
	c.PushRosterBeforeMessages(50 * time.Millisecond)
	srv.Conv("c-a", wire.NewRoster([]wire.RosterMember{{PeerID: testPeer, Name: "Alice"}}))
	srv.Msg("c-a", testPeer, "first", "1")
	drainUntil(t, c, 2)
	time.Sleep(100 * time.Millisecond)
	srv.Msg("c-a", testPeer, "after quiet", "2")
	if got := drainUntil(t, c, 2); got[0] != "roster(1)" || got[1] != "after quiet" {
		t.Fatalf("got %v, want the roster again after the quiet spell", got)
	}
}

func TestACatchUpGetsTheRosterWhenItIsDueAndOnlyThen(t *testing.T) {
	srv := agenttest.New(t, grant("c-a", "Alpha", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	c := openConv(t, a, "c-a")
	c.PushRosterBeforeMessages(time.Hour)
	srv.Conv("c-a", wire.NewRoster([]wire.RosterMember{{PeerID: testPeer, Name: "Alice"}}))
	deadline := time.Now().Add(3 * time.Second)
	for !c.RosterComplete() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if r, ok := c.RosterDue(); !ok || len(r.RosterPeers) != 1 {
		t.Fatalf("no roster in front of the first message of a catch-up: %+v %v", r, ok)
	}
	if _, ok := c.RosterDue(); ok {
		t.Fatal("the roster was due again right after it was shown")
	}
	srv.Msg("c-a", testPeer, "live", "1")
	if got := drainUntil(t, c, 1); got[0] != "live" {
		t.Fatalf("a live message after the catch-up repeated the roster: %v", got)
	}
}
