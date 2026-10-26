package mcptools

import (
	"fmt"
	"sort"
	"strings"
)

// withheldNotice says, in every tool result, what this Claude Code session
// is keeping from the model: pushes it refuses or drops, and pushes it
// holds for the user's approval, as the session itself reported them. A
// pushed notice would be withheld the same way, so a tool result is the
// only place this can be said.
func withheldNotice(sessions []*session) string {
	var refused, held []string
	heldCount := 0
	reasons := map[string]bool{}
	for _, s := range sessions {
		n, ref, reason := s.inbox.Withheld()
		if ref {
			refused = append(refused, s.name)
			if reason != "" {
				reasons[reason] = true
			}
		}
		if n > 0 {
			held = append(held, s.name)
			heldCount += n
		}
	}
	var parts []string
	if len(refused) > 0 {
		sort.Strings(refused)
		why := ""
		if len(reasons) > 0 {
			list := make([]string, 0, len(reasons))
			for r := range reasons {
				list = append(list, noticeText(r))
			}
			sort.Strings(list)
			why = " (" + strings.Join(list, "; ") + ")"
		}
		parts = append(parts, fmt.Sprintf("LIVE DELIVERY IS REFUSED BY THIS SESSION: Claude Code "+
			"refused messages pushed to you from %s%s. Nothing pushed reaches you — no messages, "+
			"catch-ups or notices. Read with hub_read(connection, after: <last cursor>) and keep "+
			"your position with hub_confirm. The cause is this session's crossSessionInbound setting "+
			"(user, project or local settings); tell the user.", strings.Join(refused, ", "), why))
	}
	if heldCount > 0 {
		sort.Strings(held)
		parts = append(parts, fmt.Sprintf("Claude Code is HOLDING %d pushed message(s) from %s for "+
			"the user's approval: they reach you only once the user approves them in this session. "+
			"Meanwhile hub_read(connection, after: <last cursor>) reads them.", heldCount, strings.Join(held, ", ")))
	}
	return strings.Join(parts, " ")
}
