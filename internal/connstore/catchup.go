package connstore

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// catchupPath is a separate file from connections.json — a small
// key→cursor map for hub_catch_up's persisted position (see
// internal/mcptools' Hub.lastHandedOverCursor), not connection identity.
// Kept as its own file rather than a field on Entry because its key
// space is different: an ordinary hub_connect session already has a
// Target (Host+SessionID+Project) to key on, but a teams_relay_connect
// session's Target is always the zero value (connstore has never tracked
// bridge-session identity/reconnectSecret — that's managed by the
// caller, not this package), so a catch-up cursor for a bridge session
// needs a key connections.json's own schema doesn't have: see
// mcptools.catchUpStoreKeyForRelay.
func catchupPath() string {
	return filepath.Join(dir(), "catchup.json")
}

func loadCatchup() map[string]string {
	data, err := os.ReadFile(catchupPath())
	if err != nil {
		return map[string]string{}
	}
	var m map[string]string
	if err := json.Unmarshal(data, &m); err != nil {
		return map[string]string{}
	}
	if m == nil {
		m = map[string]string{}
	}
	return m
}

func saveCatchup(m map[string]string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir(), 0o700); err != nil {
		return err
	}
	tmp := catchupPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, catchupPath())
}

// GetCatchUpCursor returns the persisted "last message actually handed to
// the model" position for key — an opaque, caller-chosen identity (a
// hub_connect Target's own key, or a stable identifier derived from a
// teams_relay_connect link) — if one was ever stored. Shares
// connections.json's own lock file (coarser than a dedicated lock for
// this smaller file would be, but correct and simple: both files are
// small and writes to either are infrequent, so serializing them costs
// nothing that matters).
func GetCatchUpCursor(key string) (string, bool) {
	if key == "" {
		return "", false
	}
	var cursor string
	var ok bool
	_ = withLock(false, func() error {
		cursor, ok = loadCatchup()[key]
		return nil
	})
	return cursor, ok
}

// SetCatchUpCursor persists cursor as the last-handed-over position for
// key, replacing whatever was stored before. A no-op (not an error) for
// an empty key — see GetCatchUpCursor.
func SetCatchUpCursor(key, cursor string) error {
	if key == "" {
		return nil
	}
	return withLock(true, func() error {
		m := loadCatchup()
		m[key] = cursor
		return saveCatchup(m)
	})
}
