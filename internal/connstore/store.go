// Package connstore persists mcp-hub-client's own record of which
// (host, sessionId) targets it has connected to before — the peerId it
// was assigned and the reconnectSecret that earns it back — so the model
// no longer has to remember and repeat a reconnectSecret itself across
// turns/sessions to keep the same identity. One shared file for the whole
// machine: mcp-hub-client has no project/working-directory awareness at
// all (it reads no CWD, no project env var), so there is no natural way
// to scope this any narrower than "every invocation of this client."
//
// This stores the raw reconnectSecret, not a hash of it (unlike
// internal/identitystore, which is server-side and only ever needs to
// verify a secret it's handed, never to produce one) — the client needs
// the actual value back to reconnect. This is not a new exposure: the
// secret already lived in the model's own conversation context before
// this package existed: storing it locally on the user's own machine
// instead is, if anything, a narrower trust boundary than that.
//
// Reads/writes are a full read-modify-write of one JSON file per call, no
// cross-process locking. Two mcp-hub-client processes racing a write here
// (e.g. two Claude Code sessions in different projects) can lose one's
// update — an accepted PoC-grade limitation, consistent with this
// project's existing precedent elsewhere (e.g. hublog's session logs),
// not silently pretended away.
package connstore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// Target identifies which hub session an Entry is about.
type Target struct {
	Host      string
	SessionID string
}

// Entry is what's remembered about one prior connection to a Target.
type Entry struct {
	Host            string
	SessionID       string
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

func key(t Target) string {
	return t.Host + "\x00" + t.SessionID
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
	e, ok := load()[key(target)]
	return e, ok
}

// Upsert stores entry, replacing whatever was stored before for the same
// (entry.Host, entry.SessionID).
func Upsert(entry Entry) error {
	entries := load()
	entries[key(Target{Host: entry.Host, SessionID: entry.SessionID})] = entry
	return save(entries)
}

// MarkDisconnected clears Connected on target's stored entry, if one
// exists — a no-op, not an error, if there's nothing stored for it.
func MarkDisconnected(target Target) error {
	entries := load()
	e, ok := entries[key(target)]
	if !ok {
		return nil
	}
	e.Connected = false
	entries[key(target)] = e
	return save(entries)
}

// List returns every stored entry, in no particular order.
func List() ([]Entry, error) {
	entries := load()
	out := make([]Entry, 0, len(entries))
	for _, e := range entries {
		out = append(out, e)
	}
	return out, nil
}
