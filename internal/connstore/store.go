// Package connstore persists mcp-hub-client's own record of hub sessions
// it has connected to before — a plain hub_connect session's identity
// (peerId, reconnectSecret) and, for either connection kind, hub_catch_up's
// persisted read position — so the model no longer has to remember and
// repeat a reconnectSecret itself across turns/sessions, and a reconnect
// resumes reading from where it left off instead of walking everything
// again.
//
// One file, one JSON object, structured hierarchically rather than by
// concatenating identity fields into a single string key — rewritten
// 2026-09-08 at the project owner's own request ("put everything into one
// json file... structure the keys hierarchically, do not append them...
// do not keep backwards compatible code"), replacing the prior two-file
// (connections.json/catchup.json), flat-string-keyed design. Two top-level
// maps, "hubs" and "teams", because the two connection kinds have
// genuinely different shapes: a plain hub_connect session has connstore-
// managed identity (peerId, reconnectSecret) on top of catch-up state; a
// teams_relay_connect (bridge) session's identity is caller-managed and
// connstore has never tracked it — only its catch-up state lives here,
// under its own identity (the link's stable, non-secret portion, plus
// project scope).
//
// Project-scoped throughout (see CurrentProject) as well as by
// Host+SessionID (or link target) — every mcp-hub-client invocation on a
// machine writes to the same file, but two different projects (e.g. two
// separate Claude Code working directories) connecting to the identical
// host+sessionId no longer collide on one shared reconnectSecret and fight
// over the same peerId. This was a real, reported problem: without it,
// whichever agent connected first held the live identity and every other
// agent on the same machine reconnecting to that same session got
// reassigned a fresh peerId every time, losing read-position/"own"
// continuity. Two mcp-hub-client processes in the *same* project can still
// race each other here (see save's doc comment) — that scope is
// deliberately not narrowed further, since two agents legitimately sharing
// one project's identity is the common case this is meant to support, not
// a bug.
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
	"strings"
	"time"

	"github.com/gofrs/flock"
)

// Target identifies a plain hub_connect session: which server, which
// session on it, and which project scope.
type Target struct {
	Host      string
	SessionID string
	// Project scopes this target to one working directory — see
	// CurrentProject. Two Targets with the same Host+SessionID but
	// different Project are entirely distinct entries.
	Project string
}

// TeamsID identifies a teams_relay_connect (bridge) session's catch-up
// state — connstore has no broader Entry for one (see package doc
// comment: bridge-session identity/reconnectSecret has never been
// connstore's to track). LinkTarget is the link's own stable, non-secret
// portion (everything before the "#"-delimited secret — see
// hubconn.DialRelay's identical split), which names the same conversation
// across every reconnect to it.
type TeamsID struct {
	LinkTarget string
	Project    string
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
}

// Empty reports whether cs carries no state worth persisting at all —
// used to decide whether an Entry is worth keeping once its last
// non-catch-up-identifying field is cleared.
func (cs CatchUpState) empty() bool {
	return cs.Cursor == "" && cs.Gap == nil && len(cs.Ahead) == 0
}

// Entry is what's remembered about one connection — either a
// hub_connect Target (PeerID/ReconnectSecret/Connected all set) or a
// teams_relay_connect session (those left zero, since identity there is
// caller-managed — see TeamsID's doc comment; only Topic/CatchUp apply).
// One type for both, merged 2026-09-08 at the project owner's own
// request ("merge hub and teams lists"): the two kinds already shared
// Topic and CatchUp, and a hub-only field simply stays empty on a teams
// entry rather than needing a second, near-identical struct.
type Entry struct {
	PeerID          string    `json:"peerId,omitempty"`
	Name            string    `json:"name,omitempty"`
	ReconnectSecret string    `json:"reconnectSecret,omitempty"`
	LastConnectedAt time.Time `json:"lastConnectedAt,omitempty"`
	// Connected is true from a successful connect until an explicit
	// disconnect (or this client noticing on its own that the connection
	// died) clears it. A leftover true after a restart means the process
	// that set it never got a chance to clear it — most commonly the
	// Claude Code session simply ending mid-conversation, not necessarily
	// a crash. Meaningless (always false) for a teams entry.
	Connected bool `json:"connected,omitempty"`
	// Topic is the CONVERSATION's own display name, when the server sets
	// one (wire.Joined.Topic — a bridge-session-only field, e.g. a Teams
	// chat's title) — added 2026-09-08 at the project owner's own
	// request: distinct from Name above, which is this connection's OWN
	// peer display name, not what conversation it's in. Empty when the
	// server doesn't set one (including every mcp-hub-server).
	Topic   string       `json:"topic,omitempty"`
	CatchUp CatchUpState `json:"catchUp,omitempty"`
}

// empty reports whether e carries nothing worth persisting — used by
// setEntry to prune a position back out of the hierarchy entirely rather
// than leaving a pointless empty entry behind.
func (e Entry) empty() bool {
	return e.PeerID == "" && e.Name == "" && e.ReconnectSecret == "" && e.Topic == "" &&
		e.LastConnectedAt.IsZero() && !e.Connected && e.CatchUp.empty()
}

// ListedEntry pairs a hub Target with its Entry, for List()'s callers
// (e.g. hub_list_connections) that need both identity and content
// together.
type ListedEntry struct {
	Target Target
	Entry  Entry
}

// state is the file's whole on-disk shape: ONE map, nested by identity
// field rather than a concatenated string key — see the package doc
// comment for why. Project is the OUTERMOST key, at the project owner's
// own request, 2026-09-08: every project has its own connections, so
// "what does this project have" — the question a human actually has
// when reading the file — should be one contiguous block, not scattered
// across every host+sessionId/link a machine has ever seen. Below
// project, every connection (hub or teams) lives in ONE flat map — also
// the project owner's own call ("merge hub and teams lists") — keyed by
// a single readable line: "hub <host> <sessionId>" or "teams
// <linkTarget>" (see hubKey/teamsKey). The "hub "/"teams " prefix is
// what makes the merge unambiguous: nothing else in either key shape can
// produce a collision, and it's what a human scanning the file uses to
// tell the two kinds apart at a glance.
type state map[string]map[string]Entry

// hubKey and teamsKey build state's inner map key for each connection
// kind. Host/SessionID/LinkTarget never contain a literal space, so
// splitHubKey can always recover Host as everything before the FIRST
// space after the "hub " prefix and SessionID as everything after, with
// no ambiguity.
func hubKey(host, sessionID string) string {
	return "hub " + host + " " + sessionID
}

func teamsKey(linkTarget string) string {
	return "teams " + linkTarget
}

func splitHubKey(key string) (host, sessionID string) {
	host, sessionID, _ = strings.Cut(strings.TrimPrefix(key, "hub "), " ")
	return host, sessionID
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

func getHubEntry(s state, t Target) (Entry, bool) {
	return getEntry(s, t.Project, hubKey(t.Host, t.SessionID))
}

func setHubEntry(s *state, t Target, e Entry) {
	setEntry(s, t.Project, hubKey(t.Host, t.SessionID), e)
}

func getTeamsEntry(s state, t TeamsID) (Entry, bool) {
	return getEntry(s, t.Project, teamsKey(t.LinkTarget))
}

func setTeamsEntry(s *state, t TeamsID, e Entry) {
	setEntry(s, t.Project, teamsKey(t.LinkTarget), e)
}

// Get returns the stored entry for target, if any.
func Get(target Target) (Entry, bool) {
	var e Entry
	var ok bool
	_ = withLock(false, func() error {
		e, ok = getHubEntry(load(), target)
		return nil
	})
	return e, ok
}

// Upsert stores identity, replacing whatever identity fields (PeerID,
// Name, ReconnectSecret, LastConnectedAt, Connected) were stored before
// for target — but always PRESERVES target's existing CatchUp state
// rather than overwriting it with identity.CatchUp's zero value: a
// reconnect calls this with a fresh Entry that never carries catch-up
// state itself (see mcptools' handleConnect), and clobbering an already-
// persisted read position on every reconnect would defeat the entire
// point of persisting it.
func Upsert(target Target, identity Entry) error {
	return withLock(true, func() error {
		s := load()
		existing, _ := getHubEntry(s, target)
		identity.CatchUp = existing.CatchUp
		setHubEntry(&s, target, identity)
		return save(s)
	})
}

// MarkDisconnected clears Connected on target's stored entry, if one
// exists — a no-op, not an error, if there's nothing stored for it.
func MarkDisconnected(target Target) error {
	return withLock(true, func() error {
		s := load()
		e, ok := getHubEntry(s, target)
		if !ok {
			return nil
		}
		e.Connected = false
		setHubEntry(&s, target, e)
		return save(s)
	})
}

// List returns every stored hub Target+Entry pair, in no particular
// order.
func List() ([]ListedEntry, error) {
	var out []ListedEntry
	err := withLock(false, func() error {
		s := load()
		for project, byKey := range s {
			for key, e := range byKey {
				if !strings.HasPrefix(key, "hub ") {
					continue
				}
				host, sessionID := splitHubKey(key)
				out = append(out, ListedEntry{
					Target: Target{Host: host, SessionID: sessionID, Project: project},
					Entry:  e,
				})
			}
		}
		return nil
	})
	return out, err
}

// GetCatchUp returns target's persisted hub_catch_up state, if any is
// worth reporting (see CatchUpState.empty).
func GetCatchUp(target Target) (CatchUpState, bool) {
	var cs CatchUpState
	var ok bool
	_ = withLock(false, func() error {
		e, found := getHubEntry(load(), target)
		if !found || e.CatchUp.empty() {
			return nil
		}
		cs, ok = e.CatchUp, true
		return nil
	})
	return cs, ok
}

// SetCatchUp persists cs as target's current hub_catch_up state,
// replacing whatever was stored before — creating target's entry if it
// didn't already exist (defensive: in normal operation Upsert always
// runs first, at connect time).
func SetCatchUp(target Target, cs CatchUpState) error {
	return withLock(true, func() error {
		s := load()
		e, _ := getHubEntry(s, target)
		e.CatchUp = cs
		setHubEntry(&s, target, e)
		return save(s)
	})
}

// GetTeamsCatchUp is GetCatchUp for a teams_relay_connect (bridge)
// session — see TeamsID.
func GetTeamsCatchUp(t TeamsID) (CatchUpState, bool) {
	var cs CatchUpState
	var ok bool
	_ = withLock(false, func() error {
		e, found := getTeamsEntry(load(), t)
		if !found || e.CatchUp.empty() {
			return nil
		}
		cs, ok = e.CatchUp, true
		return nil
	})
	return cs, ok
}

// SetTeamsCatchUp is SetCatchUp for a teams_relay_connect (bridge)
// session — see TeamsID.
func SetTeamsCatchUp(t TeamsID, cs CatchUpState) error {
	return withLock(true, func() error {
		s := load()
		e, _ := getTeamsEntry(s, t)
		e.CatchUp = cs
		setTeamsEntry(&s, t, e)
		return save(s)
	})
}

// SetTeamsTopic persists topic as t's conversation display name (see
// Entry.Topic), preserving whatever catch-up state was already recorded
// for it.
func SetTeamsTopic(t TeamsID, topic string) error {
	return withLock(true, func() error {
		s := load()
		e, _ := getTeamsEntry(s, t)
		e.Topic = topic
		setTeamsEntry(&s, t, e)
		return save(s)
	})
}

// CatchUpID identifies EITHER a hub_connect session's catch-up state or
// a teams_relay_connect one's — exactly one of Hub/Teams is set. This is
// what mcptools.Hub keeps a single field of (replacing the old bare
// opaque string key) so its catch-up logic doesn't need to branch on
// connection kind at every call site — see mcptools.setCatchUpKey/
// catchUpKeyForRelay.
type CatchUpID struct {
	Hub   *Target
	Teams *TeamsID
}

// HubCatchUpID and TeamsCatchUpID construct a CatchUpID for each
// connection kind.
func HubCatchUpID(t Target) CatchUpID    { return CatchUpID{Hub: &t} }
func TeamsCatchUpID(t TeamsID) CatchUpID { return CatchUpID{Teams: &t} }

// Valid reports whether id identifies an actual session — the zero
// CatchUpID (no active connection) does not.
func (id CatchUpID) Valid() bool { return id.Hub != nil || id.Teams != nil }

// Get and Set dispatch to GetCatchUp/SetCatchUp or GetTeamsCatchUp/
// SetTeamsCatchUp depending on which of Hub/Teams is set. A no-op
// (false, or nil error doing nothing) on the zero CatchUpID.
func (id CatchUpID) Get() (CatchUpState, bool) {
	switch {
	case id.Hub != nil:
		return GetCatchUp(*id.Hub)
	case id.Teams != nil:
		return GetTeamsCatchUp(*id.Teams)
	default:
		return CatchUpState{}, false
	}
}

func (id CatchUpID) Set(cs CatchUpState) error {
	switch {
	case id.Hub != nil:
		return SetCatchUp(*id.Hub, cs)
	case id.Teams != nil:
		return SetTeamsCatchUp(*id.Teams, cs)
	default:
		return nil
	}
}
