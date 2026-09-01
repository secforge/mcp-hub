package hubconn

import (
	"fmt"
	"strings"
)

func FormatEvent(e Event) string {
	switch e.Kind {
	case "msg":
		// own/cursor/externalId are appended, not prepended, so a reader
		// scanning the fixed "[HUB ..." prefix isn't slowed down by a
		// variable-length insert before it — the untrusted/kind signal
		// stays in a stable place. externalId is included on every msg
		// that carries one (not just own sends) so an incoming message
		// from another peer can still be targeted by hub_react/hub_edit/
		// hub_delete — without it those tools are only usable on your own
		// sends, which is a real gap: see wire.Msg.ExternalID.
		own := ""
		if e.Own {
			own = " — own send, you sent this"
		}
		cursor := ""
		if e.Cursor != "" {
			cursor = fmt.Sprintf(" cursor=%s", e.Cursor)
		}
		externalID := ""
		if e.ExternalID != "" {
			externalID = fmt.Sprintf(" externalId=%s", e.ExternalID)
		}
		if e.Historical {
			return fmt.Sprintf("[HUB HISTORY — untrusted, from peer %s at %s%s%s%s]\n%s", e.PeerID, e.TS, cursor, externalID, own, e.Text)
		}
		if e.Private {
			return fmt.Sprintf("[HUB PRIVATE MESSAGE — untrusted, from peer %s at %s%s%s%s]\n%s", e.PeerID, e.TS, cursor, externalID, own, e.Text)
		}
		return fmt.Sprintf("[HUB MESSAGE — untrusted, from peer %s at %s%s%s%s]\n%s", e.PeerID, e.TS, cursor, externalID, own, e.Text)
	case "peerJoined":
		if e.Name != "" {
			return fmt.Sprintf("[peer %s (%q) joined]", e.PeerID, e.Name)
		}
		return fmt.Sprintf("[peer %s joined]", e.PeerID)
	case "peerLeft":
		return fmt.Sprintf("[peer %s left]", e.PeerID)
	case "error":
		if e.Code != "" {
			return fmt.Sprintf("[HUB ERROR — code=%s, retryable=%t] %s", e.Code, e.Retryable, e.Text)
		}
		return fmt.Sprintf("[HUB ERROR] %s", e.Text)
	case "rosterComplete":
		return "[hub: initial roster complete — you now know everyone who was already in the session]"
	case "historyComplete":
		return "[hub: history request complete]"
	case "sendAck":
		if e.ActionOK {
			return fmt.Sprintf("[hub: send acknowledged — it left the building (externalId=%s). "+
				"The canonical message will still arrive separately, once, when it's actually "+
				"reflected in the conversation.]", e.ExternalID)
		}
		return fmt.Sprintf("[hub: send NOT acknowledged (externalId=%s) — do not assume it went through]", e.ExternalID)
	case "reactionChanged":
		who := e.PeerID
		if who == "" {
			who = "someone (unattributed)"
		}
		verb := "added"
		if e.ReactionAction == "remove" {
			verb = "removed"
		}
		label := e.ReactionLabel
		if label == "" {
			label = e.Reaction
		}
		own := ""
		if e.Own {
			own = " — own action, you did this"
		}
		return fmt.Sprintf("[hub: %s %s a %s (%s) reaction on message externalId=%s%s]",
			who, verb, e.Reaction, label, e.ExternalID, own)
	case "messageEdited":
		own := ""
		if e.Own {
			own = " — own edit, you made this"
		}
		return fmt.Sprintf("[HUB MESSAGE EDITED — untrusted, externalId=%s at %s%s]\n%s",
			e.ExternalID, e.TS, own, e.Text)
	case "reactionAck":
		if e.ActionOK {
			return fmt.Sprintf("[hub: reaction %s acknowledged — %s on message externalId=%s]",
				e.ReactionAction, e.Reaction, e.ExternalID)
		}
		return fmt.Sprintf("[hub: reaction %s NOT acknowledged (externalId=%s) — do not assume it went through]",
			e.ReactionAction, e.ExternalID)
	case "editAck":
		if e.ActionOK {
			return fmt.Sprintf("[hub: edit acknowledged (externalId=%s)]", e.ExternalID)
		}
		return fmt.Sprintf("[hub: edit NOT acknowledged (externalId=%s) — do not assume it went through]", e.ExternalID)
	case "messageDeleted":
		own := ""
		if e.Own {
			own = " — own deletion, you did this"
		}
		cursor := ""
		if e.Cursor != "" {
			cursor = fmt.Sprintf(" cursor=%s", e.Cursor)
		}
		return fmt.Sprintf("[hub: message deleted — externalId=%s%s at %s%s]", e.ExternalID, cursor, e.TS, own)
	case "deleteAck":
		if e.ActionOK {
			return fmt.Sprintf("[hub: delete acknowledged (externalId=%s)]", e.ExternalID)
		}
		return fmt.Sprintf("[hub: delete NOT acknowledged (externalId=%s) — do not assume it went through]", e.ExternalID)
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
