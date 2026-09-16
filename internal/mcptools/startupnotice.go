package mcptools

import (
	"fmt"
	"sort"
	"strings"

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/harness"
)

// reportAbandonedConnections tells the model, on its very next tool call,
// which connections a PREVIOUS run of this client was holding and are not
// restored.
//
// It exists because of the one gap this client could not close from
// inside a session: when the MCP server process ends, every connection
// ends with it, and the thing that would have reported that died in the
// same instant. Queued notices live in that process's memory. The
// connstore entries are marked cleanly or not at all depending on how far
// shutdown got. Nothing reaches the model.
//
// Measured, 2026-09-16: four connections ended at 15:29:47 and the model
// found out at 16:00, from its user, having spent half an hour reading
// silence as a quiet conversation. Inside a connection, silence now means
// something. About whether there IS a connection, this is what makes it
// mean something.
//
// Deliberately says "not restored" and nothing stronger. It does not
// reconnect: which conversations are still wanted is not this code's
// judgement, and a client that silently rejoined something the user had
// finished with would be its own kind of surprise.
func (h *Hub) reportAbandonedConnections() {
	entries, err := connstore.ListForProject(connstore.CurrentProject())
	if err != nil {
		// Nothing to report FROM is different from nothing to report, and
		// only the first is worth a word — a store that cannot be read is
		// also the store a position is kept in.
		h.noteAutoReconnect(fmt.Sprintf("this client could not read its own record of previous "+
			"connections (%v), so it cannot say whether any were open when it last ran.", err))
		return
	}
	var abandoned []string
	var reported []connstore.Target
	for _, le := range entries {
		// Connected means "a run was holding this and did not
		// deliberately let go". At startup this process has connected
		// nothing, so every such entry belongs to a previous run —
		// which is the whole question, answered without knowing or
		// caring which process it was.
		//
		// A drop clears it, because the model was told while it was
		// running. An explicit disconnect clears it, because that was a
		// decision. A process simply ending does NOT, because that is
		// the case nothing else can report.
		if !le.Entry.Connected {
			continue
		}
		name := le.Entry.LocalName
		if name == "" {
			name = le.Entry.Topic
		}
		if name == "" {
			name = le.Target.Link
		}
		abandoned = append(abandoned, name)
		reported = append(reported, le.Target)
	}
	if len(abandoned) == 0 {
		return
	}
	sort.Strings(abandoned)
	// CLEARED once reported, which is what makes "said once" true across
	// restarts rather than only within one.
	//
	// Seen live: an entry left marked by a process killed an hour
	// earlier was reported as something "a previous run was holding",
	// alongside a connection whose session no longer existed at all. The
	// mark says a run held this and never let go; once that has been
	// said, it has been said, and leaving it set turns one fact into a
	// notice that repeats forever and ages into a lie.
	for _, t := range reported {
		_ = connstore.MarkDisconnected(t)
	}
	note := fmt.Sprintf("a previous run of this client was holding %d connection(s) — %s — and "+
		"this process has NOT restored them. Nothing was lost: everything is on the server and "+
		"each position only moved for what was confirmed. Reconnect the ones you still want with "+
		"hub_connect (hub_list_connections shows the link and the name each was last opened as), "+
		"then hub_catch_up. Said once.", len(abandoned), strings.Join(abandoned, ", "))
	// PUSHED if this harness takes deliveries, rather than queued for
	// whatever tool happens to be called first.
	//
	// The queue means the notice waits for a call that may not come for
	// an hour — or at all, since the whole point is to say something the
	// reader has no reason to ask about.
	//
	// Not available everywhere: Codex latches its delivery target from
	// the first request's _meta, so at startup there is nothing to
	// address and the queue is the only way. The queue is therefore the
	// FALLBACK rather than the alternative, and it is used when the push
	// does not happen — never as well, which would say it twice.
	pusher := harness.Open()
	if ok, _ := pusher.Available(); ok {
		if _, err := pusher.Push("", "[hub: "+note+"]", false); err == nil {
			return
		}
	}
	h.noteAutoReconnect(note)
}
