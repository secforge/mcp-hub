package mcptools

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

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
	// Only when this session's scope is certain — see scopeIfKnown,
	// which is the same resolver every tool call uses. Roots cannot be
	// asked for before a request exists, so $PWD may name a different
	// project than the one every later call will use, and reporting
	// another scope's connections is the disclosure this scoping exists
	// to stop.
	project, known := scopeIfKnown()
	if !known {
		h.abandonedPending.Store(true)
		return
	}
	h.reportAbandonedConnectionsFor(project)
}

// predecessorWait is how long a restarted client waits for the run it
// replaces to exit before giving up on reporting its connections.
var predecessorWait = 15 * time.Second

// reportAbandonedConnectionsFor reports, once, the connections a previous
// run in project was holding when it ended.
func (h *Hub) reportAbandonedConnectionsFor(project string) {
	h.reportAbandoned(project, true)
}

// reportAbandoned is reportAbandonedConnectionsFor; waitForPredecessor
// says whether a connection still held by the run this process replaces
// may be waited for.
func (h *Hub) reportAbandoned(project string, waitForPredecessor bool) {
	entries, err := connstore.ListForProject(project)
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
	var predecessors []*connstore.Holder
	defer func() {
		if len(predecessors) > 0 {
			go h.reportAfterPredecessor(project, predecessors)
		}
	}()
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
		// HELD BY A LIVE PROCESS is not abandoned. Another client in the
		// same project — a second session, or a process started from
		// inside one — shares this store, and reporting its connection as
		// a previous run's, then clearing the mark, would erase live state
		// on the word of a process that never held it. An unknown answer
		// counts as live, for the same reason.
		//
		// Except the run this process REPLACES. An MCP client restarting
		// its server (Claude Code's /mcp reconnect) starts the new process
		// before the old one has exited, so at this moment the old one is
		// still alive — and its connections are about to end with it,
		// unreported. It is told apart from another client by being a
		// child of the same session: it is waited for, and the scan runs
		// again once it is gone.
		if hd := le.Entry.Holder; hd != nil && hd.PID != os.Getpid() && holderLive(hd) {
			if waitForPredecessor && isPredecessor(hd.PID) {
				predecessors = append(predecessors, hd)
			}
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
		if _, err := h.gate().push(pusher, "[hub: "+note+"]"); err == nil {
			return
		}
	}
	h.noteAutoReconnect(note)
}

// isPredecessor reports whether pid is another child of the process that
// started this one — the run being replaced, rather than another client.
var isPredecessor = func(pid int) bool {
	parent := os.Getppid()
	return parent > 1 && processParent(pid) == parent
}

// reportAfterPredecessor waits for the run this process replaces to exit,
// then reports its connections. If it outlives the wait, its connections
// are left as they are, as for any live holder.
func (h *Hub) reportAfterPredecessor(project string, holders []*connstore.Holder) {
	deadline := time.Now().Add(predecessorWait)
	for time.Now().Before(deadline) {
		live := false
		for _, hd := range holders {
			if holderLive(hd) {
				live = true
				break
			}
		}
		if !live {
			h.reportAbandoned(project, false)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}
