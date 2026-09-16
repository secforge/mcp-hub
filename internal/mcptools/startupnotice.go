package mcptools

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"syscall"

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
	lingering := false
	for _, le := range entries {
		e := le.Entry
		// A holder of 0 means nobody was holding it: never connected, or
		// given up on purpose. Our own pid means this process.
		//
		// ANY OTHER PID is reported, alive or not. One client holds every
		// connection now, so another process holding one is not a
		// colleague to defer to — it is the process this one replaced,
		// either gone or still on its way out. A /mcp restart produces
		// exactly that, and whether the old process has finished exiting
		// when the new one starts is a race: staying silent for a live
		// pid would make the notice fire or not depending on timing,
		// which is the one thing it exists to stop.
		if e.HolderPID == 0 || e.HolderPID == os.Getpid() {
			continue
		}
		if processAlive(e.HolderPID) {
			lingering = true
		}
		name := e.LocalName
		if name == "" {
			name = e.Topic
		}
		if name == "" {
			name = le.Target.Link
		}
		abandoned = append(abandoned, name)
	}
	if len(abandoned) == 0 {
		return
	}
	sort.Strings(abandoned)
	note := fmt.Sprintf("a previous run of this client was holding %d connection(s) — %s — and "+
		"this process has NOT restored them. Nothing was lost: everything is on the server and "+
		"each position only moved for what was confirmed. Reconnect the ones you still want with "+
		"hub_connect (hub_list_connections shows the link and the name each was last opened as), "+
		"then hub_catch_up. Said once.", len(abandoned), strings.Join(abandoned, ", "))
	if lingering {
		// Worth saying, because the old process may still be delivering
		// into a harness that no longer routes to it, and because
		// reconnecting here while it holds the same identity is what the
		// server resolves by superseding one of them.
		note += " One of those is still claimed by a process that has not finished exiting; its " +
			"connections are not this process's either way."
	}
	// PUSHED if this harness takes deliveries, rather than queued for
	// whatever tool happens to be called first.
	//
	// The queue was the only route when this was written, and it means
	// the notice waits for a call that may not come for an hour — or at
	// all, if the reader has no reason to touch the hub. The whole point
	// is to say something the reader would otherwise never learn, so
	// waiting to be asked is the wrong shape for it.
	//
	// Not available everywhere: Codex latches its delivery target from
	// the first request's _meta, so at startup there is nothing to
	// address and the queue is still the only way. The queue is
	// therefore the FALLBACK rather than the alternative, and it is used
	// when the push does not happen — never as well, which would say it
	// twice.
	pusher := harness.Open()
	if ok, _ := pusher.Available(); ok {
		if _, err := pusher.Push("", "[hub: "+note+"]", false); err == nil {
			return
		}
	}
	h.noteAutoReconnect(note)
}

// processAlive reports whether pid is a process this user can still see.
//
// Signal 0 asks the kernel the question without delivering anything. A
// permission error is an answer too — something is running under that pid
// — while "no such process" is the only result that means gone.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	return err == nil || err == os.ErrPermission || err == syscall.EPERM
}
