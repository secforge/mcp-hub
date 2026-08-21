package hublog

import "strings"

const wrapWidth = 100

// FormatEntry renders one log entry: "<ts> <peerId>" then the message text
// word-wrapped at wrapWidth columns, each line indented by two spaces, then a
// blank line.
func FormatEntry(ts, peerID, text string) string {
	return formatEntry(ts+" "+peerID, text)
}

// FormatDirectedEntry renders a private-message log entry: "<ts> <peerId> ->
// <targetPeerId>" then the message body, formatted identically to
// FormatEntry.
func FormatDirectedEntry(ts, peerID, targetPeerID, text string) string {
	return formatEntry(ts+" "+peerID+" -> "+targetPeerID, text)
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

func wrapText(text string, width int) []string {
	words := strings.Fields(text)
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
