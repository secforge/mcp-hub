// Package sanitize cleans up untrusted, peer-supplied free text (e.g. a
// display name) before it's logged or relayed to other peers/the model.
package sanitize

import (
	"strings"
	"unicode"
)

// Text strips everything that can act on a transcript rather than appear
// in it, and truncates to at most maxRunes runes, then trims surrounding
// whitespace.
//
// Two categories, and the second was missing for a while. Cc — the
// classic control characters, newlines and tabs among them — because a
// display name must never inject extra lines into a log file or a
// delivered event. And Cf, FORMAT characters, which are invisible and
// change how everything around them renders: U+202E RIGHT-TO-LEFT
// OVERRIDE reverses the display of text that follows it, the bidi
// isolates U+2066..U+2069 do the same in a scoped way, and U+200B..U+200D
// carry content that is simply not there to the eye.
//
// unicode.IsControl covers Cc alone, so a name relied on to be inert could
// reorder a transcript after it. The gap survived because the outer
// layers happen to strip invisibles too — which is precisely why a
// function whose stated job is this cannot depend on them.
func Text(s string, maxRunes int) string {
	var b strings.Builder
	count := 0
	for _, r := range s {
		if count >= maxRunes {
			break
		}
		if r == '�' || unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			continue
		}
		b.WriteRune(r)
		count++
	}
	return strings.TrimSpace(b.String())
}
