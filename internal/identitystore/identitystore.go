// Package identitystore durably persists the mapping a
// hubsession.Session uses to reassign a peerId when a reconnectSecret
// matches a prior, now-departed connection's — so that mapping survives a
// server restart, not just individual peer disconnects. It never persists
// a reconnectSecret's own value, only a one-way hash of it, so the file at
// rest can't be replayed to impersonate anyone even if read.
package identitystore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
)

func dir() string {
	if d := os.Getenv("MCP_HUB_LOG_DIR"); d != "" {
		return d
	}
	return "."
}

func path(sessionID string) string {
	return filepath.Join(dir(), sessionID+".secrets.json")
}

// HashSecret returns the lookup key a reconnectSecret is stored/matched
// under — never the secret itself.
func HashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// Load reads the persisted secretHash->peerID mapping for a session. A
// missing or unreadable file is not an error — it just means nothing has
// been persisted for this session yet (or ever) — so Load always returns a
// usable (possibly empty) map.
func Load(sessionID string) map[string]string {
	data, err := os.ReadFile(path(sessionID))
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
	data, err := json.Marshal(mapping)
	if err != nil {
		return err
	}
	target := path(sessionID)
	tmp := target + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, target)
}

// Delete removes a session's persisted mapping, if any — called when a
// session is deliberately torn down (its last peer left), as opposed to a
// server restart: identity is scoped to "the same still-alive channel," so
// an intentional end of that channel should end persisted identity too,
// while an unplanned restart should not.
func Delete(sessionID string) {
	_ = os.Remove(path(sessionID))
}
