// Package identitystore durably persists the mapping a
// hubsession.Session uses to reassign a peerId when a reconnectSecret
// matches a prior connection's — so that mapping survives a server
// restart and the session itself being fully torn down (its last peer
// leaving), not just individual peer disconnects. There is currently no
// expiry or cleanup: a mapping persists indefinitely once written,
// matching this project's existing PoC-log precedent (also never
// rotated/cleaned). It never persists a reconnectSecret's own value, only
// a one-way hash of it, so the file at rest can't be replayed to
// impersonate anyone even if read.
package identitystore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/secforge/mcp-hub/internal/wire"
)

func dir() string {
	if d := os.Getenv("MCP_HUB_LOG_DIR"); d != "" {
		return d
	}
	return "."
}

// path builds the persistence file path for sessionID, which by the time
// it reaches this package should already be a validated UUID (wsserver
// rejects anything else before a session is ever created — see
// wire.IsValidID). Re-validating here anyway, rather than trusting the
// caller, is deliberate defense in depth: sessionID becomes part of a
// filesystem path, and this package has no other way to guarantee a future
// caller won't pass through something attacker-controlled and unvalidated
// (e.g. "../../etc/cron.d/x") that could otherwise write or read outside
// dir() entirely.
func path(sessionID string) (string, error) {
	if !wire.IsValidID(sessionID) {
		return "", fmt.Errorf("identitystore: invalid sessionID %q", sessionID)
	}
	return filepath.Join(dir(), sessionID+".secrets.json"), nil
}

// HashSecret returns the lookup key a reconnectSecret is stored/matched
// under — never the secret itself.
func HashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// Load reads the persisted secretHash->peerID mapping for a session. A
// missing or unreadable file, or an invalid sessionID, is not an error —
// either just means nothing usable has been persisted for this session —
// so Load always returns a usable (possibly empty) map.
func Load(sessionID string) map[string]string {
	p, err := path(sessionID)
	if err != nil {
		return map[string]string{}
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return map[string]string{}
	}
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return map[string]string{}
	}
	return m
}

// Save durably persists mapping, overwriting whatever was stored before,
// via a write-to-temp-then-rename so a concurrent Load never observes a
// partially-written file. Best-effort: an error here is not meant to be
// fatal to the caller — surviving a restart is a convenience on top of the
// in-memory mapping the session already works from, not something session
// correctness itself depends on.
func Save(sessionID string, mapping map[string]string) error {
	target, err := path(sessionID)
	if err != nil {
		return err
	}
	data, err := json.Marshal(mapping)
	if err != nil {
		return err
	}
	tmp := target + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, target)
}
