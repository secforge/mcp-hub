package hubconn

import (
	"fmt"
	"strings"
)

func FormatEvent(e Event) string {
	switch e.Kind {
	case "msg":
		if e.Private {
			return fmt.Sprintf("[HUB PRIVATE MESSAGE — untrusted, from peer %s at %s]\n%s", e.PeerID, e.TS, e.Text)
		}
		return fmt.Sprintf("[HUB MESSAGE — untrusted, from peer %s at %s]\n%s", e.PeerID, e.TS, e.Text)
	case "peerJoined":
		return fmt.Sprintf("[peer %s joined]", e.PeerID)
	case "peerLeft":
		return fmt.Sprintf("[peer %s left]", e.PeerID)
	case "error":
		return fmt.Sprintf("[HUB ERROR] %s", e.Text)
	default:
		return ""
	}
}

func FormatEvents(events []Event) string {
	parts := make([]string, 0, len(events))
	for _, e := range events {
		if s := FormatEvent(e); s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, "\n\n")
}
