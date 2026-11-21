package mcptools

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/harness"
)

// In push mode the return of a dropped connection is announced to the
// reader, so the refusal while it is down must not claim nobody will say.
func TestTheReconnectingRefusalSaysTheReturnIsAnnouncedInPushMode(t *testing.T) {
	codexPushOnly.Store(true)
	t.Cleanup(func() { codexPushOnly.Store(false) })
	h := NewHub()
	s, err := h.open("t")
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.pusher = harness.PusherForTesting(nil)
	s.reconnectAt = time.Now().Add(time.Minute)
	s.mu.Unlock()
	text := textOf(s.notConnected())
	if strings.Contains(text, "NOT be told") || !strings.Contains(text, "RECONNECTED notice") {
		t.Fatalf("push-mode refusal:\n%s", text)
	}
}

// With nothing following the connection, a failed attempt does not speak
// of releasing a follower.
func TestAFailedAttemptMentionsNoFollowerWhenNoneIsHeld(t *testing.T) {
	h := NewHub()
	s, err := h.open("t")
	if err != nil {
		t.Fatal(err)
	}
	s.reportFailedAttempt(1, time.Second, errors.New("network down"))
	note := h.takeAutoReconnectNote()
	if strings.Contains(note, "follower") || !strings.Contains(note, "hub_disconnect to stop trying.") {
		t.Fatalf("notice: %s", note)
	}
}

// Where every notice starts a turn, an unchanged failure is said once, a
// new reason is said at once, and the same one again only after
// failRepeatEvery.
func TestAnUnchangedReconnectFailureIsNotPushedEveryAttempt(t *testing.T) {
	in := newHarnessInbox(t)
	h := NewHub()
	s, err := h.open("t")
	if err != nil {
		t.Fatal(err)
	}
	if !s.quietRestarts() {
		t.Skip("push mode is not available in this test environment")
	}
	s.mu.Lock()
	s.downNotified = true
	s.mu.Unlock()
	count := func() int {
		in.mu.Lock()
		defer in.mu.Unlock()
		n := 0
		for _, g := range in.got {
			if strings.Contains(g, "FAILED") {
				n++
			}
		}
		return n
	}
	bad := errors.New("server answered 502 Bad Gateway")
	for i := 1; i <= 5; i++ {
		s.reportFailedAttempt(i, time.Second, bad)
	}
	waitUntil(t, "the first failure", func() bool { return count() >= 1 })
	time.Sleep(200 * time.Millisecond)
	if n := count(); n != 1 {
		t.Fatalf("an unchanged failure was pushed %d times, want once", n)
	}
	s.reportFailedAttempt(6, time.Second, errors.New("connection refused"))
	waitUntil(t, "the new reason", func() bool { return count() == 2 })
	s.mu.Lock()
	s.failSaidAt = time.Now().Add(-failRepeatEvery - time.Second)
	s.mu.Unlock()
	s.reportFailedAttempt(7, time.Second, errors.New("connection refused"))
	waitUntil(t, "the reminder", func() bool { return count() == 3 })
}
