package approvals

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"filippo.io/age"
	"github.com/gofrs/flock"

	"github.com/secforge/mcp-hub/internal/connstore"
)

// Request states.
const (
	StateOpen      = "open"
	StateAnswered  = "answered"
	StateWithdrawn = "withdrawn"
	StateExpired   = "expired"
)

// keepClosed is how long a closed request stays on record, so a late or
// conflicting answer can still be told apart from an unknown one.
const keepClosed = 30 * 24 * time.Hour

var approverNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ApproversDir is where approver key files are kept, one
// "<approver>.keys" file per person, written by hand.
func ApproversDir() string { return filepath.Join(connstore.Dir(), "approvers") }

// ApproverFile is the key file of one approver.
func ApproverFile(name string) string { return filepath.Join(ApproversDir(), name+".keys") }

// ValidApproverName reports whether name can name an approver file.
func ValidApproverName(name string) bool { return approverNamePattern.MatchString(name) }

// LoadApprover reads one approver's phone keys. A malformed line is an
// error rather than skipped: a typo must not silently revoke a phone.
func LoadApprover(name string) ([]Key, error) {
	if !ValidApproverName(name) {
		return nil, fmt.Errorf("%q is not an approver name (letters, digits, . _ -)", name)
	}
	path := ApproverFile(name)
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("there is no approver %q: %s does not exist", name, path)
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var keys []Key
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, err := ParseKeyLine(line)
		if err != nil {
			return nil, fmt.Errorf("%s line %d: %v", path, n, err)
		}
		keys = append(keys, k)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("%s holds no key lines", path)
	}
	return keys, nil
}

// Approvers lists the approver names this machine has files for.
func Approvers() []string {
	entries, _ := os.ReadDir(ApproversDir())
	var names []string
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".keys")
		if ok && !e.IsDir() && ValidApproverName(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// FindSigner returns the approver whose files hold signKey, and that
// key. Approver files that cannot be read are skipped.
func FindSigner(signKey string) (string, []Key, bool) {
	for _, name := range Approvers() {
		keys, err := LoadApprover(name)
		if err != nil {
			continue
		}
		for _, k := range keys {
			if k.SignKey == signKey {
				return name, keys, true
			}
		}
	}
	return "", nil, false
}

// Entry is one request this client asked.
type Entry struct {
	RequestID  string `json:"requestId"`
	Approver   string `json:"approver"`
	Connection string `json:"connection"`
	// Request is the exact plaintext that was sent.
	Request   string `json:"request"`
	ExpiresAt string `json:"expiresAt"`
	State     string `json:"state"`
	ClosedAt  string `json:"closedAt,omitempty"`
	// Statement and Signature are the first valid answer.
	Statement string `json:"statement,omitempty"`
	Signature string `json:"signature,omitempty"`
	// Notes records every answer that was not taken, and why.
	Notes []string `json:"notes,omitempty"`
}

// Proof is the entry's answer as a proof another agent can verify.
func (e *Entry) Proof() (Proof, bool) {
	if e.Statement == "" {
		return Proof{}, false
	}
	return Proof{Request: e.Request, Statement: e.Statement, Signature: e.Signature}, true
}

// State is everything this client keeps about approvals for one project
// scope.
type State struct {
	Project  string            `json:"project"`
	Identity string            `json:"identity"`
	Requests map[string]*Entry `json:"requests"`
	// Used records each request id a proof was acted on, and when.
	Used map[string]string `json:"used"`
}

func stateDir() string { return filepath.Join(connstore.Dir(), "approvals") }

func statePath(project string) string {
	sum := sha256.Sum256([]byte(project))
	return filepath.Join(stateDir(), hex.EncodeToString(sum[:8])+".json")
}

// Update runs fn on the project's state under an exclusive lock and saves
// what it leaves, unless fn fails. A state with no age identity gets one.
func Update(project string, now time.Time, fn func(*State) error) error {
	d := stateDir()
	if err := os.MkdirAll(d, 0o700); err != nil {
		return err
	}
	path := statePath(project)
	fl := flock.New(path + ".lock")
	if err := fl.Lock(); err != nil {
		return err
	}
	defer fl.Unlock()
	st := State{}
	raw, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(raw, &st); err != nil {
			return fmt.Errorf("%s is unreadable: %v", path, err)
		}
	case !errors.Is(err, os.ErrNotExist):
		return err
	}
	st.Project = project
	if st.Requests == nil {
		st.Requests = map[string]*Entry{}
	}
	if st.Used == nil {
		st.Used = map[string]string{}
	}
	if st.Identity == "" {
		id, err := age.GenerateX25519Identity()
		if err != nil {
			return err
		}
		st.Identity = id.String()
	}
	st.tidy(now)
	if err := fn(&st); err != nil {
		return err
	}
	out, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(out); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// tidy closes requests whose time ran out and forgets closed ones after
// keepClosed, and used proofs once they could no longer be accepted.
func (st *State) tidy(now time.Time) {
	for id, e := range st.Requests {
		if e.State == StateOpen && expired(e.ExpiresAt, now) {
			e.State, e.ClosedAt = StateExpired, now.UTC().Format(time.RFC3339)
		}
		if e.State != StateOpen {
			if closed, err := time.Parse(time.RFC3339, e.ClosedAt); err == nil && now.Sub(closed) > keepClosed {
				delete(st.Requests, id)
			}
		}
	}
	for id, at := range st.Used {
		if t, err := time.Parse(time.RFC3339, at); err == nil && now.Sub(t) > 8*24*time.Hour {
			delete(st.Used, id)
		}
	}
}

func expired(expiresAt string, now time.Time) bool {
	t, err := time.Parse(time.RFC3339, expiresAt)
	return err != nil || !now.Before(t)
}

// AgeIdentity is the state's identity, parsed.
func (st *State) AgeIdentity() (*age.X25519Identity, error) {
	return age.ParseX25519Identity(st.Identity)
}

// Close marks an entry closed in state.
func (e *Entry) Close(state string, now time.Time) {
	e.State, e.ClosedAt = state, now.UTC().Format(time.RFC3339)
}
