// Package sanitize cleans up untrusted, peer-supplied free text (e.g. a
// display name) before it's logged or relayed to other peers/the model.
package sanitize

import (
	"strings"
	"unicode"
)

// Text strips all control characters (including newlines and tabs — a
// display name must never be able to inject extra lines into the log file
// or a delivered event) and truncates to at most maxRunes runes, then trims
// surrounding whitespace.
func Text(s string, maxRunes int) string {
	var b strings.Builder
	count := 0
	for _, r := range s {
		if count >= maxRunes {
			break
		}
		if r == '�' || unicode.IsControl(r) {
			continue
		}
		b.WriteRune(r)
		count++
	}
	return strings.TrimSpace(b.String())
}
