package mcptools

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/hubconn"
)

// A restart is not news. In push mode every notice is a delivery, and a
// delivery starts a turn: a server restart used to start about six — the
// announcement, the drop, the member list, the reconnect, the joins and
// leaves the catch-up found, its summary — none of which asked anything of
// the reader. Now the drop is said only when the reconnect does not follow
// within the announced delay plus downGrace, and what the reconnect brings
// that is not a message is gathered into ONE notice, sent once nothing has
// been added to it for restartQuiet, or at once in front of the first real
// message.

// downGrace is how long past the expected reconnect the drop is kept
// quiet, and downMin the least: a drop nobody announced, or one announced
// as short, is still not said within its first minute.
var (
	downGrace = 15 * time.Second
	downMin   = time.Minute
)

// downHold is how long a drop is kept quiet when the reconnect is
// expected after delay.
func downHold(delay time.Duration) time.Duration {
	if hold := delay + downGrace; hold > downMin {
		return hold
	}
	return downMin
}

// restartQuiet is how long the after-reconnect notice waits for more.
var restartQuiet = 5 * time.Second

// The catch-up after an automatic reconnect runs by itself only for a
// short outage with a small backlog: past either bound, what arrived is
// worth the reader deciding when to read it.
const (
	autoCatchUpMaxDown     = time.Hour
	autoCatchUpMaxMessages = 100
)

// restartNote gathers what a reconnect brings that is not a message.
type restartNote struct {
	mu     sync.Mutex
	active bool
	lines  []string
	// cursor is the last presence entry folded in, so one confirm of the
	// notice covers them.
	cursor string
	timer  *time.Timer
	// act says something in the notice asks the reader to do something.
	// A notice without it is not pushed at all: it goes in front of the
	// next tool result instead, so a routine restart wakes nobody.
	act bool
}

// quietRestarts reports whether restarts are kept quiet here: in push
// mode, where every notice starts a turn.
func (s *session) quietRestarts() bool {
	if !pushOnly() || s.pusher == nil {
		return false
	}
	ok, _ := s.pusher.Available()
	return ok
}

// restartArm starts gathering at the drop, so what the new connection
// sends the moment it is up — its member list — is gathered too.
func (s *session) restartArm() {
	n := &s.restart
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.timer != nil {
		n.timer.Stop()
		n.timer = nil
	}
	n.active, n.lines, n.cursor, n.act = true, nil, "", false
}

// restartLead puts the line saying what happened at the top of the
// gathering notice; an empty line adds nothing and only starts the wait.
func (s *session) restartLead(line string) {
	n := &s.restart
	n.mu.Lock()
	n.active = true
	if line != "" {
		n.lines = append([]string{line}, n.lines...)
	}
	if n.timer != nil {
		n.timer.Stop()
	}
	n.timer = time.AfterFunc(restartQuiet, s.restartFlush)
	n.mu.Unlock()
}

// restartActive reports whether the after-reconnect notice is gathering.
func (s *session) restartActive() bool {
	n := &s.restart
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.active
}

// restartAdd adds a line to the gathering notice, with the cursor of the
// presence entry it covers if any, and reports whether it was taken. A
// notice already sent takes nothing: the caller delivers as usual then.
func (s *session) restartAdd(line, cursor string) bool {
	n := &s.restart
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.active {
		return false
	}
	n.lines = append(n.lines, line)
	if cursor != "" {
		n.cursor = cursor
	}
	if n.timer != nil {
		n.timer.Stop()
	}
	n.timer = time.AfterFunc(restartQuiet, s.restartFlush)
	return true
}

// restartAct adds a line that asks the reader to do something, which
// makes the notice worth a push.
func (s *session) restartAct(line string) bool {
	n := &s.restart
	n.mu.Lock()
	n.act = n.act || n.active
	n.mu.Unlock()
	return s.restartAdd(line, "")
}

// restartFlush sends the gathered notice, if one is gathering: pushed when
// it asks something of the reader, in front of the next tool result when
// it does not.
func (s *session) restartFlush() {
	n := &s.restart
	n.mu.Lock()
	if !n.active {
		n.mu.Unlock()
		return
	}
	if len(n.lines) == 0 {
		n.active = false
		n.mu.Unlock()
		return
	}
	body := strings.Join(n.lines, "\n")
	cursor, act := n.cursor, n.act
	n.active, n.lines, n.cursor, n.act = false, nil, "", false
	if n.timer != nil {
		n.timer.Stop()
		n.timer = nil
	}
	n.mu.Unlock()
	if !act {
		if cursor != "" {
			body += "\nThese joins and leaves end at cursor " + cursor + "; confirm it with hub_confirm " +
				"along with whatever you confirm next."
		}
		s.note(body)
		return
	}
	text := "[hub: on " + s.name + ": " + body + "]"
	pusher := s.pusher
	s.hub.gate().budget.Deliver(hubconn.Delivery{
		Owner: s.name, Cursor: cursor, Bytes: len(text),
		Send: func() {
			if _, err := pusher.Push(cursor, text, false); err != nil {
				s.note("a notice about the reconnect could not be delivered: " + noticeText(err.Error()))
			}
		},
		Pause: func(paused string) { _, _ = pusher.Push("", paused, false) },
	})
}

// downLater says the connection is down only once the reconnect is
// overdue: after the expected delay plus downGrace, if it is still not
// back.
func (s *session) downLater(delay time.Duration, gen int, text string) {
	t := time.AfterFunc(downHold(delay), func() {
		s.mu.Lock()
		still := s.reconnecting && s.reconnectGen == gen && s.conn == nil
		if still {
			s.downNotified = true
		}
		s.mu.Unlock()
		if still {
			s.alert(text)
		}
	})
	s.mu.Lock()
	if s.downTimer != nil {
		s.downTimer.Stop()
	}
	s.downTimer = t
	s.mu.Unlock()
}

// reconnectedQuietly gathers the reconnect into one notice and walks what
// arrived meanwhile without being asked: the reader is told about the
// restart once, together with what it brought.
//
// because says why for a connection of its own; a conversation of an agent
// connection passes "", and its agent says it once for all of them.
func (s *session) reconnectedQuietly(conn *hubconn.Conn, because string) {
	s.mu.Lock()
	if s.downTimer != nil {
		s.downTimer.Stop()
		s.downTimer = nil
	}
	s.downNotified = false
	away := time.Since(s.downSince)
	s.mu.Unlock()
	lead := ""
	if because != "" {
		lead = "Reconnected " + because + ", away " + away.Round(time.Second).String() + "."
	}
	s.restartLead(lead)
	switch {
	case conn.BehindStated() && conn.Behind() == 0:
		return
	case away >= autoCatchUpMaxDown:
		s.restartAct(fmt.Sprintf("It was down for more than %s, so what arrived meanwhile was not "+
			"walked by itself: call hub_catch_up when you are ready to read it.", autoCatchUpMaxDown))
		return
	case conn.BehindStated() && conn.Behind() > autoCatchUpMaxMessages:
		s.restartAct(fmt.Sprintf("%d messages arrived meanwhile, more than the %d walked by "+
			"themselves: call hub_catch_up when you are ready to read them.",
			conn.Behind(), autoCatchUpMaxMessages))
		return
	}
	go func() {
		req := mcp.CallToolRequest{}
		// Capped, so a server that did not say how much is waiting cannot
		// make the walk deliver more than the bound either.
		req.Params.Arguments = map[string]any{"connection": s.name, "limit": autoCatchUpMaxMessages}
		res, err := s.hub.handleCatchUp(context.Background(), req)
		switch {
		case err != nil:
			s.restartAct("Catching up on what arrived meanwhile could not start (" + noticeText(err.Error()) +
				") — call hub_catch_up.")
		case res != nil && res.IsError:
			s.restartAct("Catching up on what arrived meanwhile could not start (" + noticeText(textOfResult(res)) +
				") — call hub_catch_up.")
		case res != nil && strings.Contains(textOfResult(res), "hub_catch_up(gap: true)"):
			s.restartAct("More arrived than is walked one by one, so the catch-up skipped to recent " +
				"context; hub_catch_up(gap: true) retrieves the rest.")
		}
	}()
}

// textOfResult is a result's text, its first text content.
func textOfResult(res *mcp.CallToolResult) string {
	for _, c := range res.Content {
		if tc, ok := c.(mcp.TextContent); ok {
			return tc.Text
		}
	}
	return ""
}
