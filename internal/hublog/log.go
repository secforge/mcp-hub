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

func (l *SessionLog) AppendJoined(peerID, name, ts string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := l.file.WriteString(FormatJoinedEntry(ts, peerID, name))
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
