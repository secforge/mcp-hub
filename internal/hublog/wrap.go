package hublog

import "strings"

const wrapWidth = 100

// FormatEntry renders one log entry: "<ts> <peerId>" then the message text
// with its original line breaks preserved (each further word-wrapped at
// wrapWidth columns only if it's still too long), each output line indented
// by two spaces, then a blank line.
func FormatEntry(ts, peerID, text string) string {
	return formatEntry(ts+" "+peerID, text)
}

// FormatDirectedEntry renders a private-message log entry: "<ts> <peerId> ->
// <targetPeerId>" then the message body, formatted identically to
// FormatEntry.
func FormatDirectedEntry(ts, peerID, targetPeerID, text string) string {
	return formatEntry(ts+" "+peerID+" -> "+targetPeerID, text)
}

// FormatJoinedEntry renders a peer-joined log entry: "<ts> <peerId> joined"
// at minimum, plus optional pieces for whatever that peer supplied — name
// (already sanitized of control characters/newlines by the caller, so
// always safe to embed on this one line), agePublicKey, and a
// reconnectSecret status marker — followed by a blank line. No message
// body — peerJoined events carry none. reconnectSecret's own value is
// never logged (see AppendJoined); only whether one was involved, and how.
//
// reused and secretGiven distinguish reconnectSecret's three possible
// states for this join, each surfaced explicitly rather than left to be
// inferred from a repeated peerId elsewhere in the log:
//   - reused: this connection presented a reconnectSecret that matched a
//     prior, now-departed connection's — peerId was reclaimed. Logged as
//     "(reconnected)".
//   - !reused && secretGiven: a reconnectSecret was given but didn't (yet)
//     match anything — either the first time it's been used, or its
//     previous holder is still connected. Registered for a future
//     reconnect. Logged as "(reconnectSecret set)".
//   - neither: no reconnectSecret was involved. No marker.
func FormatJoinedEntry(ts, peerID, name, agePublicKey string, reused, secretGiven bool) string {
	suffix := ""
	switch {
	case reused:
		suffix = " (reconnected)"
	case secretGiven:
		suffix = " (reconnectSecret set)"
	}
	line := ts + " " + peerID
	if name != "" {
		line += " (" + name + ")"
	}
	if agePublicKey != "" {
		line += " agePublicKey=" + agePublicKey
	}
	return line + " joined" + suffix + "\n\n"
}

// FormatLeftEntry renders a peer-left log entry: "<ts> <peerId> left"
// followed by a blank line.
func FormatLeftEntry(ts, peerID string) string {
	return ts + " " + peerID + " left\n\n"
}

func formatEntry(header, text string) string {
	var b strings.Builder
	b.WriteString(header)
	b.WriteByte('\n')
	for _, line := range wrapText(text, wrapWidth) {
		b.WriteString("  ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.String()
}

// wrapText preserves the text's original line breaks — each source line is
// wrapped independently (and only split further if it doesn't fit width),
// so a message's own formatting survives instead of being flattened into
// one paragraph.
func wrapText(text string, width int) []string {
	srcLines := strings.Split(text, "\n")
	out := make([]string, 0, len(srcLines))
	for _, srcLine := range srcLines {
		out = append(out, wrapLine(srcLine, width)...)
	}
	return out
}

func wrapLine(line string, width int) []string {
	words := strings.Fields(line)
	if len(words) == 0 {
		return []string{""}
	}
	lines := make([]string, 0, len(words))
	cur := words[0]
	for _, w := range words[1:] {
		if len(cur)+1+len(w) > width {
			lines = append(lines, cur)
			cur = w
			continue
		}
		cur += " " + w
	}
	lines = append(lines, cur)
	return lines
}
