package hubconn

import (
	"fmt"
	"strings"

	"github.com/secforge/mcp-hub/internal/wire"
)

// FormatPresenceRun renders consecutive presence entries as one delivery:
// each peer once, in the order it first appears, with what it did in
// order — so a peer that joined and left inside the run still shows both.
// self is this connection's own peer id.
func FormatPresenceRun(run []Event, self string) string {
	if len(run) == 0 {
		return ""
	}
	type peer struct {
		label  string
		events []string
	}
	var order []string
	peers := map[string]*peer{}
	for _, ev := range run {
		p := ev.Presence
		if p == nil {
			continue
		}
		what := oneLine(p.Event, true)
		switch p.Cause {
		case wire.PresenceCauseServerRestart:
			what += " at the server restart"
		case wire.PresenceCauseRemoved:
			what += ", removed by an operator"
		}
		if p.By != nil {
			what += " (by " + presenceLabel(*p.By, self) + ")"
		}
		for _, m := range p.Members {
			pr, ok := peers[m.PeerID]
			if !ok {
				pr = &peer{label: presenceLabel(m, self)}
				peers[m.PeerID] = pr
				order = append(order, m.PeerID)
			}
			pr.events = append(pr.events, what)
		}
	}
	lines := make([]string, 0, len(order))
	for _, id := range order {
		pr := peers[id]
		lines = append(lines, pr.label+": "+strings.Join(pr.events, " → "))
	}
	when := oneLine(run[0].TS, true)
	if last := oneLine(run[len(run)-1].TS, true); last != when {
		when += " – " + last
	}
	from := "live"
	if run[0].Historical {
		from = "from history"
	}
	return fmt.Sprintf("[presence, %d change(s) %s, %s — names are untrusted]\n%s",
		len(run), from, when, strings.Join(lines, "\n"))
}

// presenceLabel names a member: its name and id, or "you" for this
// connection's own.
func presenceLabel(m wire.PresenceMember, self string) string {
	label := oneLine(m.Name, false)
	if label == "" {
		label = "a peer"
	}
	if m.PeerID == self && self != "" {
		return label + " (you)"
	}
	return label + " (" + oneLine(m.PeerID, true) + ")"
}

// FoldPresenceRuns replaces each run of consecutive presence entries in a
// read with one "presenceRun" event, rendered as FormatPresenceRun does
// for delivery, so a read of history shows joins and leaves the way they
// arrived live. The folded event keeps the run's last cursor.
func FoldPresenceRuns(events []Event, self string) []Event {
	out := make([]Event, 0, len(events))
	for i := 0; i < len(events); {
		if events[i].Kind != "msg" || events[i].Presence == nil {
			out = append(out, events[i])
			i++
			continue
		}
		j := i
		for j < len(events) && events[j].Kind == "msg" && events[j].Presence != nil {
			j++
		}
		run := events[i:j]
		last := run[len(run)-1]
		out = append(out, Event{Kind: "presenceRun", Text: FormatPresenceRun(run, self),
			Cursor: last.Cursor, TS: last.TS, Historical: last.Historical})
		i = j
	}
	return out
}
