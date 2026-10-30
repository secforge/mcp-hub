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
