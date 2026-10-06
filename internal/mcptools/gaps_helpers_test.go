package mcptools

import "github.com/secforge/mcp-hub/internal/connstore"

// resetCatchUpGaps drops every recorded range, so a test starts from none.
func resetCatchUpGaps(id connstore.Target) {
	connstore.UpdateCatchUp(id, func(cs *connstore.CatchUpState) { cs.Gaps = nil })
}

// oldestGap is the range retrieval works on, as a reader is shown it.
func oldestGap(id connstore.Target) (from, to string, ok bool) {
	g, ok := loadCatchUpGap(id)
	if !ok {
		return "", "", false
	}
	return g.From(), g.To, true
}
