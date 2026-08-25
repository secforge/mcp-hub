package hublog

import "testing"

func TestFormatEntryBasic(t *testing.T) {
	got := FormatEntry("2026-08-21T10:00:00Z", "peer-1", "hello world")
	want := "2026-08-21T10:00:00Z peer-1\n  hello world\n\n"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatDirectedEntryIncludesTarget(t *testing.T) {
	got := FormatDirectedEntry("2026-08-21T10:00:00Z", "peer-1", "peer-2", "hello world")
	want := "2026-08-21T10:00:00Z peer-1 -> peer-2\n  hello world\n\n"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatJoinedEntry(t *testing.T) {
	got := FormatJoinedEntry("2026-08-21T10:00:00Z", "peer-1")
	want := "2026-08-21T10:00:00Z peer-1 joined\n\n"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatLeftEntry(t *testing.T) {
	got := FormatLeftEntry("2026-08-21T10:00:00Z", "peer-1")
	want := "2026-08-21T10:00:00Z peer-1 left\n\n"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatEntryWrapsOnWordBoundary(t *testing.T) {
	word := "abcdefghij" // 10 chars
	text := ""
	for i := 0; i < 12; i++ { // 12*10 + 11 spaces = 131 chars, must wrap
		if i > 0 {
			text += " "
		}
		text += word
	}
	got := FormatEntry("ts", "p1", text)
	lines := splitLines(got)
	for _, l := range lines {
		if len(l) > 102 { // 100 + 2-space indent
			t.Fatalf("line too long (%d): %q", len(l), l)
		}
	}
}

func TestFormatEntryNeverSplitsAWord(t *testing.T) {
	longWord := ""
	for i := 0; i < 150; i++ {
		longWord += "x"
	}
	got := FormatEntry("ts", "p1", "short "+longWord)
	if !contains(got, longWord) {
		t.Fatalf("long word was split across lines: %q", got)
	}
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i, c := range s {
		if c == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	return lines
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
