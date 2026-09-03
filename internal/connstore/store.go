// Package connstore persists mcp-hub-client's own record of which
// (project, host, sessionId) targets it has connected to before — the
// peerId it was assigned and the reconnectSecret that earns it back — so
// the model no longer has to remember and repeat a reconnectSecret itself
// across turns/sessions to keep the same identity.
//
// One shared file for the whole machine, but keyed by Project (see
// CurrentProject) as well as host+sessionId — every mcp-hub-client
// invocation on a machine still writes to the same connections.json, but
// two different projects (e.g. two separate Claude Code working
// directories) connecting to the identical host+sessionId no longer
// collide on one shared reconnectSecret and fight over the same peerId.
// This was a real, reported problem: without it, whichever agent
// connected first held the live identity and every other agent on the
// same machine reconnecting to that same session got reassigned a fresh
// peerId every time, losing read-position/"own" continuity. Two
// mcp-hub-client processes in the *same* project can still race each
// other here (see save's doc comment) — that scope is deliberately not
// narrowed further, since two agents legitimately sharing one project's
// identity is the common case this is meant to support, not a bug.
//
// This stores the raw reconnectSecret, not a hash of it (unlike
// internal/identitystore, which is server-side and only ever needs to
// verify a secret it's handed, never to produce one) — the client needs
// the actual value back to reconnect. This is not a new exposure: the
// secret already lived in the model's own conversation context before
// this package existed: storing it locally on the user's own machine
// instead is, if anything, a narrower trust boundary than that.
//
// Reads/writes are a full read-modify-write of one JSON file per call,
// guarded by a cross-process advisory file lock (see withLock) — an
// exclusive lock around each write's whole load-modify-save sequence, a
// shared lock around each read, so two mcp-hub-client processes racing a
// write here (e.g. two Claude Code sessions in the same project) can no
// longer silently lose one's update the way an earlier, unguarded version
// of this package could.
package connstore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
)

// Target identifies which hub session an Entry is about.
type Target struct {
	Host      string
	SessionID string
	// Project scopes this target to one working directory — see
	// CurrentProject. Two Targets with the same Host+SessionID but
	// different Project are entirely distinct entries.
	Project string
}

// Entry is what's remembered about one prior connection to a Target.
type Entry struct {
	Host            string
	SessionID       string
	Project         string
	PeerID          string
	Name            string
	ReconnectSecret string
	LastConnectedAt time.Time
	// Connected is true from a successful connect until an explicit
	// disconnect (or this client noticing on its own that the connection
	// died) clears it. A leftover true after a restart means the process
	// that set it never got a chance to clear it — most commonly the
	// Claude Code session simply ending mid-conversation, not necessarily
	// a crash.
	Connected bool
}

// CurrentProject identifies "this working directory" for Target.Project —
// MCP_HUB_PROJECT_DIR if set (an explicit override, and how tests get
// isolation without touching the real cwd), else the process's actual
// working directory. mcp-hub-client is launched by its MCP client (e.g.
// Claude Code) as a local stdio subprocess, which normally inherits the
// project's own working directory — that's what makes this a meaningful
// project identifier at all, not an arbitrary choice. Empty (rather than
// erroring) if the working directory can't be determined, which just
// means every such invocation shares one "unknown project" scope — a
// degraded-but-safe fallback, not a crash.
func CurrentProject() string {
	if p := os.Getenv("MCP_HUB_PROJECT_DIR"); p != "" {
		return p
	}
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	return wd
}

func key(t Target) string {
	return t.Host + "\x00" + t.SessionID + "\x00" + t.Project
}

func dir() string {
	if d := os.Getenv("MCP_HUB_CONNSTORE_DIR"); d != "" {
		return d
	}
	if d, err := os.UserConfigDir(); err == nil {
		return filepath.Join(d, "mcp-hub")
	}
	return "."
}

func path() string {
	return filepath.Join(dir(), "connections.json")
}

// lockPath is a separate file from connections.json itself — flock
// locking and the atomic write-tmp-then-rename save() both want exclusive
// use of "their" path, and rename replacing the locked inode out from
// under a held lock is exactly the kind of subtlety not worth relying on
// working consistently across platforms. Locking a dedicated, never-
// renamed file sidesteps the question entirely.
func lockPath() string {
	return filepath.Join(dir(), "connections.json.lock")
}

// withLock runs fn while holding a cross-process advisory lock on
// lockPath — exclusive for a write (nothing else may read or write while
// held), shared for a read (any number of readers may hold it
// concurrently, but never alongside a writer). This is what actually
// closes the lost-update race a bare load-then-save has: two writers
// racing here now serialize instead of one silently overwriting the
// other's change with a stale snapshot.
func withLock(exclusive bool, fn func() error) error {
	if err := os.MkdirAll(dir(), 0o700); err != nil {
		return err
	}
	fl := flock.New(lockPath())
	var err error
	if exclusive {
		err = fl.Lock()
	} else {
		err = fl.RLock()
	}
	if err != nil {
		return err
	}
	defer fl.Unlock()
	return fn()
}

// load reads the persisted store. A missing or unreadable file is not an
// error — either just means nothing has been persisted yet — so load
// always returns a usable (possibly empty) map.
func load() map[string]Entry {
	data, err := os.ReadFile(path())
	if err != nil {
		return map[string]Entry{}
	}
	var m map[string]Entry
	if err := json.Unmarshal(data, &m); err != nil {
		return map[string]Entry{}
	}
	return m
}

// save durably persists entries, overwriting whatever was stored before,
// via a write-to-temp-then-rename so a concurrent load never observes a
// partially-written file.
func save(entries map[string]Entry) error {
	if err := os.MkdirAll(dir(), 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	target := path()
	tmp := target + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, target)
}

// Get returns the stored entry for target, if any.
func Get(target Target) (Entry, bool) {
	var e Entry
	var ok bool
	_ = withLock(false, func() error {
		e, ok = load()[key(target)]
		return nil
	})
	return e, ok
}

// Upsert stores entry, replacing whatever was stored before for the same
// (entry.Host, entry.SessionID, entry.Project).
func Upsert(entry Entry) error {
	return withLock(true, func() error {
		entries := load()
		entries[key(Target{Host: entry.Host, SessionID: entry.SessionID, Project: entry.Project})] = entry
		return save(entries)
	})
}

// MarkDisconnected clears Connected on target's stored entry, if one
// exists — a no-op, not an error, if there's nothing stored for it.
func MarkDisconnected(target Target) error {
	return withLock(true, func() error {
		entries := load()
		e, ok := entries[key(target)]
		if !ok {
			return nil
		}
		e.Connected = false
		entries[key(target)] = e
		return save(entries)
	})
}

// List returns every stored entry, in no particular order.
func List() ([]Entry, error) {
	var out []Entry
	err := withLock(false, func() error {
		entries := load()
		out = make([]Entry, 0, len(entries))
		for _, e := range entries {
			out = append(out, e)
		}
		return nil
	})
	return out, err
}
