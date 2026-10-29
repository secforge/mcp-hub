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
		pr, ok := peers[p.PeerID]
		if !ok {
			label := oneLine(p.Name, false)
			if label == "" {
				label = "a peer"
			}
			if p.PeerID == self && self != "" {
				label += " (you)"
			} else {
				label += " (" + oneLine(p.PeerID, true) + ")"
			}
			pr = &peer{label: label}
			peers[p.PeerID] = pr
			order = append(order, p.PeerID)
		}
		what := oneLine(p.Event, true)
		switch p.Cause {
		case wire.PresenceCauseServerRestart:
			what += " at the server restart"
		case wire.PresenceCauseRemoved:
			what += ", removed by an operator"
		}
		pr.events = append(pr.events, what)
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
	return fmt.Sprintf("[presence, %d change(s) from history, %s — names are untrusted]\n%s",
		len(run), when, strings.Join(lines, "\n"))
}
