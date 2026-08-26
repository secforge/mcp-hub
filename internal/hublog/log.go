package hublog

import (
	"os"
	"path/filepath"
	"sync"
)

type SessionLog struct {
	mu   sync.Mutex
	file *os.File
}

// OpenSessionLog opens (creating if needed) the append-only log file for a
// session, under MCP_HUB_LOG_DIR (default ".").
func OpenSessionLog(sessionID string) (*SessionLog, error) {
	path := filepath.Join(logDir(), sessionID+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &SessionLog{file: f}, nil
}

func logDir() string {
	if d := os.Getenv("MCP_HUB_LOG_DIR"); d != "" {
		return d
	}
	return "."
}

func (l *SessionLog) Append(peerID, text, ts string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := l.file.WriteString(FormatEntry(ts, peerID, text))
	return err
}

func (l *SessionLog) AppendDirected(peerID, targetPeerID, text, ts string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := l.file.WriteString(FormatDirectedEntry(ts, peerID, targetPeerID, text))
	return err
}

// AppendJoined logs a peer's join. Note what's deliberately absent:
// reconnectSecret's own value is never passed in or logged, only whether
// one was involved and whether it matched (reused/secretGiven) — logging
// the secret itself would defeat its purpose (see hubsession.Session).
func (l *SessionLog) AppendJoined(peerID, name, agePublicKey string, reused, secretGiven bool, ts string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := l.file.WriteString(FormatJoinedEntry(ts, peerID, name, agePublicKey, reused, secretGiven))
	return err
}

func (l *SessionLog) AppendLeft(peerID, ts string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := l.file.WriteString(FormatLeftEntry(ts, peerID))
	return err
}

func (l *SessionLog) Close() error {
	return l.file.Close()
}
