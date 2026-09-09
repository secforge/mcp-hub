// Package connstore persists mcp-hub-client's own record of the sessions
// it has connected to before — each one's identity (the peerId a server
// assigned and the secret this client manages to reclaim it) and
// hub_catch_up's read position — so nothing about resuming a session is
// ever the model's to remember, and a reconnect resumes reading where it
// left off instead of walking everything again.
//
// One file, one JSON object: project first (see CurrentProject), then one
// flat map of that project's connections keyed by the link each was
// reached by. Every connection has the same shape here, because a client
// reaches all of them the same way — one link, whatever kind of
// conversation is behind it.
//
// Project scoping is load-bearing, not tidiness: every mcp-hub-client
// invocation on a machine writes this one file, and without it two
// projects connecting to the identical host+sessionId collide on one
// shared secret and fight over the same peerId — whichever agent connects
// first holds the live identity while every other agent reconnecting to
// that session is reassigned a fresh peerId each time, losing read
// position and "own message" continuity. Two mcp-hub-client processes in
// the SAME project can still race here (see save); that scope is
// deliberately not narrowed further, since two agents sharing one
// project's identity is the case this exists to support.
//
// This file holds credentials, and its permissions are part of its
// contract: 0700 on the directory, 0600 on the file and on the temp file a
// write renames through. Two kinds live here. The reconnect secret is
// stored raw rather than hashed (unlike internal/identitystore, which is
// server-side and only ever verifies a secret it is handed) because the
// client needs the value itself to reconnect. And each KEY is itself a
// capability: a link carries its credential in the fragment, so the key is
// enough to join the conversation. Keeping both in a file on the user's own
// machine is a narrower trust boundary than the alternative of carrying
// them in a model's conversation context.
//
// Reads and writes are a full read-modify-write of the file per call,
// guarded by a cross-process advisory lock (see withLock): exclusive
// around a write's whole load-modify-save sequence, shared around a read,
// so two mcp-hub-client processes racing a write cannot silently lose one
// another's update.
package connstore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
)

// Target identifies one connection: the link it was reached by, scoped to
// one working directory.
type Target struct {
	// Link is the WHOLE link, credential included — see the package doc
	// comment on what that means for this file. It has to be the whole
	// thing: on a hub link the fragment IS the session id, so keying on the
	// address alone would collapse every session on one server into a
	// single entry sharing one identity and one read position.
	Link string
	// Project scopes this target to one working directory — see
	// CurrentProject. The same Link under two different Projects is two
	// entirely distinct entries.
	Project string
}

// GapState is a persisted record of a hub_catch_up seek's abandoned
// range — see mcptools' catchUpGap for the full rationale (recorded as
// ongoing state, not a one-time notice). Nil (not a zero-value struct) on
// CatchUpState.Gap when there is no open gap, so a caller doesn't need a
// separate boolean to tell "no gap" from "a gap with blank fields."
type GapState struct {
	From         string `json:"from,omitempty"`
	To           string `json:"to,omitempty"`
	AnchorCursor string `json:"anchorCursor,omitempty"`
}

// DiscardedGap records a skipped range that was deliberately written off
// instead of retrieved. Kept rather than deleted so the decision stays
// auditable: "nobody ever read this range, and that was chosen" is a
// different fact from "this range was read", and a file that cannot tell
// them apart turns a decision into an absence.
type DiscardedGap struct {
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
	// At is when the range was written off.
	At time.Time `json:"at,omitempty"`
}

// CatchUpState is hub_catch_up's full persisted state for one session —
// see mcptools.Hub.lastHandedOverCursor/handedOverAhead/catchUpGap for
// what each field backs.
type CatchUpState struct {
	// Cursor is the contiguous high-water mark of positions actually
	// handed over to the model — mcptools.Hub.lastHandedOverCursor.
	Cursor string `json:"cursor,omitempty"`
	// Gap is the currently-open seek gap, if any — see GapState.
	Gap *GapState `json:"gap,omitempty"`
	// Ahead is the set of cursors confirmed handed over to the model at
	// a position ahead of Cursor (via live delivery) — mcptools.Hub.
	// handedOverAhead. A slice, not a set, in the JSON form (Go maps
	// don't round-trip key order and a set-of-strings has no other
	// fields to key on); mcptools converts to/from its own map[string]bool
	// at the boundary.
	Ahead []string `json:"ahead,omitempty"`
	// Discarded holds ranges deliberately written off unread — see
	// DiscardedGap. Append-only; retrieving a gap never adds to it.
	Discarded []DiscardedGap `json:"discarded,omitempty"`
}

// Empty reports whether cs carries no state worth persisting at all —
// used to decide whether an Entry is worth keeping once its last
// non-catch-up-identifying field is cleared.
func (cs CatchUpState) empty() bool {
	return cs.Cursor == "" && cs.Gap == nil && len(cs.Ahead) == 0 && len(cs.Discarded) == 0
}

// Entry is what's remembered about one connection: the identity to
// reclaim on the next connect, the conversation's name, and hub_catch_up's
// position. A field a given server has no concept of stays empty rather
// than justifying a second, near-identical struct.
type Entry struct {
	PeerID          string    `json:"peerId,omitempty"`
	Name            string    `json:"name,omitempty"`
	ReconnectSecret string    `json:"reconnectSecret,omitempty"`
	LastConnectedAt time.Time `json:"lastConnectedAt,omitempty"`
	// Connected is true from a successful connect until an explicit
	// disconnect (or this client noticing on its own that the connection
	// died) clears it. A leftover true after a restart means the process
	// that set it never got a chance to clear it — most commonly the
	// session simply ending mid-conversation, not necessarily a crash.
	Connected bool `json:"connected,omitempty"`
	// Topic is the CONVERSATION's own display name when the server sets
	// one (wire.Joined.Topic, e.g. a Teams chat's title) — distinct from
	// Name above, which is this connection's OWN peer display name rather
	// than what conversation it is in. Empty when the server sets none.
	Topic   string       `json:"topic,omitempty"`
	CatchUp CatchUpState `json:"catchUp,omitempty"`
}

// empty reports whether e carries nothing worth keeping on disk.
func (e Entry) empty() bool {
	return e.PeerID == "" && e.Name == "" && e.ReconnectSecret == "" &&
		e.Topic == "" && !e.Connected && e.LastConnectedAt.IsZero() && e.CatchUp.empty()
}

// ListedEntry pairs a stored entry with the identity it is stored under,
// for listing.
type ListedEntry struct {
	Target Target
	Entry  Entry
}

// state is the file's whole on-disk shape. Project is the OUTERMOST key,
// because every project has its own connections: "what does this project
// have" — the question anyone reading the file actually has — is then one
// contiguous block rather than scattered across every link the machine has
// ever seen. Below project, each connection is keyed by its own link,
// which is already unique and already the thing a human recognises.
type state map[string]map[string]Entry

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
	return filepath.Join(dir(), "state.json")
}

// lockPath is a separate file from state.json itself — flock locking and
// the atomic write-tmp-then-rename save() both want exclusive use of
// "their" path, and rename replacing the locked inode out from under a
// held lock is exactly the kind of subtlety not worth relying on working
// consistently across platforms. Locking a dedicated, never-renamed file
// sidesteps the question entirely.
func lockPath() string {
	return filepath.Join(dir(), "state.json.lock")
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
// always returns a usable, non-nil state.
func load() state {
	data, err := os.ReadFile(path())
	if err != nil {
		return state{}
	}
	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		return state{}
	}
	return s
}

// save durably persists s, overwriting whatever was stored before, via a
// write-to-temp-then-rename so a concurrent load never observes a
// partially-written file.
func save(s state) error {
	if err := os.MkdirAll(dir(), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
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

func getEntry(s state, project, key string) (Entry, bool) {
	byKey, ok := s[project]
	if !ok {
		return Entry{}, false
	}
	e, ok := byKey[key]
	return e, ok
}

// setEntry writes e at (project, key), creating the intermediate map as
// needed. An empty e (see Entry.empty) removes the position entirely
// instead of leaving a pointless empty entry behind, pruning the
// project map too once it's the last one there.
func setEntry(s *state, project, key string, e Entry) {
	if e.empty() {
		if byKey, ok := (*s)[project]; ok {
			delete(byKey, key)
			if len(byKey) == 0 {
				delete(*s, project)
			}
		}
		return
	}
	if *s == nil {
		*s = state{}
	}
	byKey, ok := (*s)[project]
	if !ok {
		byKey = map[string]Entry{}
		(*s)[project] = byKey
	}
	byKey[key] = e
}

// Get returns the stored entry for target, if any.
func Get(target Target) (Entry, bool) {
	var e Entry
	var ok bool
	_ = withLock(false, func() error {
		e, ok = getEntry(load(), target.Project, target.Link)
		return nil
	})
	return e, ok
}

// Upsert records identity for target, preserving whatever catch-up state
// is already stored for it — identity and read position are written by
// different callers at different times, so neither may clobber the other.
func Upsert(target Target, identity Entry) error {
	return withLock(true, func() error {
		s := load()
		existing, _ := getEntry(s, target.Project, target.Link)
		identity.CatchUp = existing.CatchUp
		setEntry(&s, target.Project, target.Link, identity)
		return save(s)
	})
}

// MarkDisconnected clears target's Connected flag, leaving everything
// else — including the identity needed to reconnect — in place.
func MarkDisconnected(target Target) error {
	return withLock(true, func() error {
		s := load()
		e, ok := getEntry(s, target.Project, target.Link)
		if !ok {
			return nil
		}
		e.Connected = false
		setEntry(&s, target.Project, target.Link, e)
		return save(s)
	})
}

// SetPeerID records the identity a server assigned, so a later connect can
// ask for it back (see hubconn.DialOptions.AgentID).
func SetPeerID(target Target, peerID string) error {
	return withLock(true, func() error {
		s := load()
		e, _ := getEntry(s, target.Project, target.Link)
		e.PeerID = peerID
		e.LastConnectedAt = time.Now().UTC()
		setEntry(&s, target.Project, target.Link, e)
		return save(s)
	})
}

// SetReconnectSecret records the secret this client manages for target, so
// the same one is presented on every later connect and the identity keeps
// resuming without a caller ever holding it.
func SetReconnectSecret(target Target, secret string) error {
	return withLock(true, func() error {
		s := load()
		e, _ := getEntry(s, target.Project, target.Link)
		e.ReconnectSecret = secret
		setEntry(&s, target.Project, target.Link, e)
		return save(s)
	})
}

// SetTopic persists topic as target's conversation display name (see
// Entry.Topic).
func SetTopic(target Target, topic string) error {
	return withLock(true, func() error {
		s := load()
		e, _ := getEntry(s, target.Project, target.Link)
		e.Topic = topic
		setEntry(&s, target.Project, target.Link, e)
		return save(s)
	})
}

// List returns every stored connection across all projects.
func List() ([]ListedEntry, error) {
	var out []ListedEntry
	err := withLock(false, func() error {
		for project, byLink := range load() {
			for link, e := range byLink {
				out = append(out, ListedEntry{
					Target: Target{Link: link, Project: project},
					Entry:  e,
				})
			}
		}
		return nil
	})
	return out, err
}

// ListForProject is List scoped to one project — a direct single-key
// lookup rather than a scan, since project is the outermost key on disk.
func ListForProject(project string) ([]ListedEntry, error) {
	var out []ListedEntry
	err := withLock(false, func() error {
		for link, e := range load()[project] {
			out = append(out, ListedEntry{
				Target: Target{Link: link, Project: project},
				Entry:  e,
			})
		}
		return nil
	})
	return out, err
}

// GetCatchUp returns target's persisted hub_catch_up state, if any.
func GetCatchUp(target Target) (CatchUpState, bool) {
	var cs CatchUpState
	var ok bool
	_ = withLock(false, func() error {
		var e Entry
		e, ok = getEntry(load(), target.Project, target.Link)
		cs = e.CatchUp
		return nil
	})
	return cs, ok
}

// SetCatchUp records target's hub_catch_up state, preserving the identity
// fields stored alongside it.
func SetCatchUp(target Target, cs CatchUpState) error {
	return withLock(true, func() error {
		s := load()
		e, _ := getEntry(s, target.Project, target.Link)
		e.CatchUp = cs
		setEntry(&s, target.Project, target.Link, e)
		return save(s)
	})
}
