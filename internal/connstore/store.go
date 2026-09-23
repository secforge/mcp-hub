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
	"errors"
	"fmt"
	"io/fs"
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

// ErrUnreadable reports a store file that exists but cannot be parsed.
// Distinguished from "nothing stored" because the two call for opposite
// responses: one is an ordinary first run, the other is a file holding
// credentials that must not be written over.
var ErrUnreadable = errors.New("connection store is unreadable")

// GapState is a persisted record of a hub_catch_up seek's abandoned
// range — see mcptools' catchUpGap for the full rationale (recorded as
// ongoing state, not a one-time notice). Nil (not a zero-value struct) on
// CatchUpState.Gap when there is no open gap, so a caller doesn't need a
// separate boolean to tell "no gap" from "a gap with blank fields."
type GapState struct {
	// From is where the skipped range STARTS, and the two fields are
	// separate because a cursor and a timestamp are not
	// interchangeable: a cursor is opaque, and a server asked to treat
	// one as a timestamp refuses the request outright (bad_anchor),
	// which made the one advertised recovery path for skipped history
	// fail. Exactly one is set — FromCursor when the seek started from a
	// position this client had actually reached, FromAt when all that is
	// known is how far back the server said the backlog went.
	FromCursor string `json:"fromCursor,omitempty"`
	FromAt     string `json:"fromAt,omitempty"`
	To         string `json:"to,omitempty"`
	// AnchorCursor is how far INTO the gap this client has walked, and
	// takes precedence over both fields above once set.
	AnchorCursor string `json:"anchorCursor,omitempty"`
	// legacyFrom is the single field these two replaced, which held
	// either kind of value with nothing to say which. Read on load and
	// sorted into the right one (see normalise) rather than dropped: a
	// gap record says messages exist that nothing has walked to, and
	// discarding it on upgrade would lose exactly the range it was
	// recorded to keep reachable.
	LegacyFrom string `json:"from,omitempty"`
}

// normalise sorts a legacy start value into the field it belongs in. The
// test is whether it parses as a timestamp — the one property that
// actually distinguishes the two, rather than a guess about how cursors
// tend to look.
func (g *GapState) normalise() {
	if g.LegacyFrom == "" {
		return
	}
	if _, err := time.Parse(time.RFC3339, g.LegacyFrom); err == nil {
		if g.FromAt == "" {
			g.FromAt = g.LegacyFrom
		}
	} else if g.FromCursor == "" {
		g.FromCursor = g.LegacyFrom
	}
	g.LegacyFrom = ""
}

// From renders where the gap starts for a reader. A cursor is shown as
// one rather than dressed up as a time, because a reader who is told a
// time that is actually a cursor cannot tell that anything is wrong.
func (g GapState) From() string {
	if g.FromAt != "" {
		return g.FromAt
	}
	if g.FromCursor != "" {
		return "cursor " + g.FromCursor
	}
	return ""
}

// Started reports whether this gap records a start at all — the one
// thing every caller has to check before using it, since a gap with
// neither is not a gap.
func (g GapState) Started() bool { return g.FromCursor != "" || g.FromAt != "" }

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
// see mcptools' session.lastHandedOverCursor/handedOverAhead/catchUpGap for
// what each field backs.
type CatchUpState struct {
	// Cursor is the contiguous high-water mark of positions actually
	// handed over to the model — mcptools' session.lastHandedOverCursor.
	Cursor string `json:"cursor,omitempty"`
	// Gap is the currently-open seek gap, if any — see GapState.
	Gap *GapState `json:"gap,omitempty"`
	// Ahead is the set of cursors confirmed handed over to the model at
	// a position ahead of Cursor (via live delivery) — mcptools'
	// session.handedOverAhead. A slice, not a set, in the JSON form (Go maps
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
	Topic string `json:"topic,omitempty"`
	// LocalName is what this link was last addressed as inside one
	// client process (hub_connect's "as"). Not an identity and nothing on
	// the wire — purely so a later session can offer the name this
	// project used last, rather than the caller having to remember it or
	// invent a second one for the same conversation.
	LocalName string       `json:"localName,omitempty"`
	CatchUp   CatchUpState `json:"catchUp,omitempty"`
}

// empty reports whether e carries nothing worth keeping on disk.
func (e Entry) empty() bool {
	return e.PeerID == "" && e.Name == "" && e.ReconnectSecret == "" &&
		e.Topic == "" && e.LocalName == "" && !e.Connected &&
		e.LastConnectedAt.IsZero() && e.CatchUp.empty()
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

// ProjectOverride is MCP_HUB_PROJECT_DIR, or "" when it is unset.
//
// It is an EXPLICIT override and outranks everything a caller can infer,
// including an MCP client's advertised roots. Roots is the better guess
// at which project a client has open, but a guess is what it is, and a
// variable someone set by hand is a statement. A caller that consults
// roots must consult this first, or the variable silently does nothing in
// the one place it is most likely to be used.
func ProjectOverride() string { return NormalizeProject(os.Getenv("MCP_HUB_PROJECT_DIR")) }

// NormalizeProject is the one spelling a project scope is filed and
// looked up under. "/source/x/" and "/source/x" name the same directory,
// and as two map keys they are two scopes: the second finds nothing,
// mints a new identity, and strands the first. Trailing separators and
// other redundant path elements are dropped; "" stays "" (filepath.Clean
// would make it ".", which is a different scope).
func NormalizeProject(p string) string {
	if p == "" {
		return ""
	}
	return filepath.Clean(p)
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
	if p := ProjectOverride(); p != "" {
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
	// NOT the working directory. This file's map KEYS are whole links,
	// fragment included, so the reconnect credentials are not merely
	// inside it — they are the key names, visible in any diff. Writing it
	// wherever the process happens to be standing puts them in a repo the
	// moment this runs somewhere without HOME: a container, a systemd
	// unit, some CI. 0600 is no defence against a commit.
	return filepath.Join(os.TempDir(), "mcp-hub")
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
	if err := ensureDir(); err != nil {
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
// load reads the store, distinguishing "nothing has been persisted yet"
// from "the persisted state could not be read".
//
// Those were the same answer once — both returned an empty state — and
// the consequence is the worst failure this package can have. Every
// writer takes what load returns, sets its one entry, and saves over the
// top, so a single unreadable read (an EMFILE, a permission blip, a
// truncated write from a full disk, a hand-edit, a future type change
// that makes the map fail to unmarshal) silently replaced every project's
// reconnect secret, peerId and catch-up position with nothing. The
// symptom is the one all of this exists to prevent: every session comes
// back as a stranger with no position, and nothing anywhere reports why.
//
// A missing file is still not an error — that genuinely means nothing has
// been persisted. Anything else stops the write.
func load() (state, error) {
	data, err := os.ReadFile(path())
	if errors.Is(err, fs.ErrNotExist) {
		return state{}, nil
	}
	if err != nil {
		return state{}, fmt.Errorf("reading %s: %w", path(), err)
	}
	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		// LEFT EXACTLY WHERE IT IS. Moving it aside from a read looked
		// like protecting the bytes, and did the opposite: the next
		// write found no file, concluded nothing was stored, and created
		// a fresh store — so every identity and reading position stopped
		// being active state while connect reported success. The file is
		// the only copy of credentials that cannot be regenerated, so
		// nothing here touches it; recovery is a deliberate act (see
		// MoveAside), taken under an exclusive lock, by someone who has
		// been told.
		return state{}, fmt.Errorf("%w: parsing %s: %v — stored identities and positions are "+
			"unavailable until it is repaired, restored from a backup, or moved aside "+
			"(nothing here will overwrite it)", ErrUnreadable, path(), err)
	}
	// A gap written by an older build carries its start in one field
	// that held either kind of value; sort it out once, here, where
	// every reader goes through.
	for _, byLink := range s {
		for _, e := range byLink {
			if e.CatchUp.Gap != nil {
				e.CatchUp.Gap.normalise()
			}
		}
	}
	return s, nil
}

// save durably persists s, overwriting whatever was stored before, via a
// write-to-temp-then-rename so a concurrent load never observes a
// partially-written file.
//
// "Durably" is two properties and the rename only buys one. The rename is
// what makes the replacement ATOMIC to a concurrent reader. It does
// nothing about a crash or a power loss: without an fsync the rename can
// be visible while the bytes behind it are not, and this file holds every
// reconnect secret and reading position this client has — losing it means
// every session comes back as a stranger with no position and nothing
// anywhere reporting why. So the data is flushed before the rename and
// the directory entry after it. An external reviewer pointed out that the
// word was doing work the code did not.
//
// The directory fsync is best-effort: a platform that refuses to open a
// directory for this still leaves a correct file, just one whose NAME may
// not survive a power loss, and failing the save outright over that would
// be the worse trade.
func save(s state) error {
	if err := ensureDir(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	target := path()
	tmp := target + ".tmp"
	if err := writeFileSynced(tmp, data); err != nil {
		return err
	}
	if err := os.Rename(tmp, target); err != nil {
		return err
	}
	syncDir(dir())
	return nil
}

// ensureDir creates the state directory and ENFORCES its mode rather than
// merely requesting it. os.MkdirAll returns nil without touching a
// directory that already exists, so a pre-existing loose
// ~/.config/mcp-hub stayed loose for the life of the install while the
// package doc claimed 0700 as part of its contract. Found by an external
// reviewer.
func ensureDir() error {
	d := dir()
	if err := os.MkdirAll(d, 0o700); err != nil {
		return err
	}
	if fi, err := os.Stat(d); err == nil && fi.Mode().Perm() != 0o700 {
		return os.Chmod(d, 0o700)
	}
	return nil
}

// writeFileSynced writes data to name at 0600 and flushes it to the disk
// before returning.
//
// O_TRUNC plus an explicit Chmod, not os.WriteFile: WriteFile's perm
// applies only when it CREATES the file, so a state.json.tmp left behind
// by a save that died between the write and the rename was reopened with
// whatever mode it already carried — and the rename then handed that mode
// to state.json. A credential store cannot inherit its permissions from
// the wreckage of an earlier crash.
func writeFileSynced(name string, data []byte) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func syncDir(d string) {
	f, err := os.Open(d)
	if err != nil {
		return
	}
	defer f.Close()
	_ = f.Sync()
}

func getEntry(s state, project, key string) (Entry, bool) {
	byKey, ok := s[NormalizeProject(project)]
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
	project = NormalizeProject(project)
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
func Get(target Target) (Entry, bool, error) {
	var e Entry
	var ok bool
	// The error is RETURNED, not swallowed. Reporting "nothing stored"
	// for a store that could not be read states an absence that was never
	// established — and it is the one answer that makes a caller go on to
	// mint a new identity over the top of the old one.
	err := withLock(false, func() error {
		st, err := load()
		if err != nil {
			return err
		}
		e, ok = getEntry(st, target.Project, target.Link)
		return nil
	})
	return e, ok, err
}

// MoveAside renames an unreadable store so a fresh one can be created,
// and is the only thing here that does: it is a deliberate act, taken
// under the exclusive lock, never a side effect of reading. Returns where
// the old bytes went so a caller can say it.
func MoveAside() (string, error) {
	aside := path() + ".corrupt"
	err := withLock(true, func() error { return os.Rename(path(), aside) })
	if err != nil {
		return "", err
	}
	return aside, nil
}

// Upsert records identity for target, preserving whatever catch-up state
// is already stored for it — identity and read position are written by
// different callers at different times, so neither may clobber the other.
func Upsert(target Target, identity Entry) error {
	return withLock(true, func() error {
		s, err := load()
		if err != nil {
			return err
		}
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
		s, err := load()
		if err != nil {
			return err
		}
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
		s, err := load()
		if err != nil {
			return err
		}
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
		s, err := load()
		if err != nil {
			return err
		}
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
		s, err := load()
		if err != nil {
			return err
		}
		e, _ := getEntry(s, target.Project, target.Link)
		e.Topic = topic
		setEntry(&s, target.Project, target.Link, e)
		return save(s)
	})
}

// THERE IS NO CROSS-PROJECT READ HERE, AND THERE MUST NOT BE ONE.
//
// A List() over every project stood here, and a count of which other
// scopes held a given link stood beside it. Both looked legitimate: one
// counted sessions left open, the other warned a connect that a link was
// resumable under a different scope. Both handed a session facts about
// projects that are not its own — their paths, their number, that they
// exist — and a connect result is quoted onward by whatever reads it.
//
// A client acting in one project interacts with that project's data and
// nothing else. That is not a rule applied at each call site, where the
// next caller has to remember it; it is the absence of any function that
// could do otherwise. Every accessor takes a Target or a project, so the
// scope is an argument the caller must supply and cannot omit.
//
// load() reads the whole file because one file holds every project, and
// it is unexported for exactly that reason.

// ListForProject returns one project's stored connections — a direct
// single-key lookup rather than a scan, since project is the outermost
// key on disk.
func ListForProject(project string) ([]ListedEntry, error) {
	project = NormalizeProject(project)
	var out []ListedEntry
	err := withLock(false, func() error {
		st, err := load()
		if err != nil {
			return err
		}
		for link, e := range st[project] {
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
// A READ FAILURE IS NOT "NOTHING STORED". ok=false with a nil error
// means nothing is stored for target; a non-nil error means the store
// could not be read or locked, and says nothing about what it holds.
// Treating the second as the first starts a connection from no position,
// which re-walks a backlog at best and, where something else moves the
// position, skips one — so the error is returned and callers decide.
func GetCatchUp(target Target) (cs CatchUpState, ok bool, err error) {
	err = withLock(false, func() error {
		st, lerr := load()
		if lerr != nil {
			return lerr
		}
		var e Entry
		e, ok = getEntry(st, target.Project, target.Link)
		cs = e.CatchUp
		return nil
	})
	if err != nil {
		return CatchUpState{}, false, err
	}
	return cs, ok, nil
}

// UpdateCatchUp mutates target's catch-up state inside ONE exclusive lock,
// so a caller never holds a snapshot across the read and the write.
//
// This is the shape every other mutator in this file already has:
// SetPeerID takes a peerID, SetTopic a topic, MarkDisconnected takes
// nothing. SetCatchUp was the only one taking a composite value, which
// forced its callers to GetCatchUp, mutate, and SetCatchUp back — three
// operations with the lock released between the first and the third.
//
// The window is not a narrow one. GetCatchUp takes a SHARED lock, which
// by design lets any number of readers in at once, so two processes
// reading the same state simultaneously is the documented behaviour
// rather than an unlucky interleaving: under real concurrency the lost
// update is the expected outcome. On this machine that is two MCP servers
// registered against the same store, and what gets lost is the gap record
// — the one thing that says messages exist which nothing will walk to.
//
// Upsert already defended against exactly this by preserving CatchUp
// across an identity write. The hazard was seen once, fixed where it was
// seen, and left in the contract of the API whose whole job is that state.
func UpdateCatchUp(target Target, mutate func(*CatchUpState)) error {
	return withLock(true, func() error {
		s, err := load()
		if err != nil {
			return err
		}
		e, _ := getEntry(s, target.Project, target.Link)
		mutate(&e.CatchUp)
		setEntry(&s, target.Project, target.Link, e)
		return save(s)
	})
}

// SetCatchUp records target's hub_catch_up state, preserving the identity
// fields stored alongside it.
func SetCatchUp(target Target, cs CatchUpState) error {
	return withLock(true, func() error {
		s, err := load()
		if err != nil {
			return err
		}
		e, _ := getEntry(s, target.Project, target.Link)
		e.CatchUp = cs
		setEntry(&s, target.Project, target.Link, e)
		return save(s)
	})
}
