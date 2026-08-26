package sanitize

import "testing"

func TestTextStripsControlCharsAndNewlines(t *testing.T) {
	got := Text("Steffen\n2026-01-01T00:00:00Z fake-peer joined\x00\x1b[31m", 64)
	want := "Steffen2026-01-01T00:00:00Z fake-peer joined[31m"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestTextTruncatesToMaxRunes(t *testing.T) {
	got := Text("héllo wörld", 5)
	if got != "héllo" {
		t.Fatalf("got %q", got)
	}
}

func TestTextTrimsSurroundingWhitespace(t *testing.T) {
	got := Text("  Steffen  ", 64)
	if got != "Steffen" {
		t.Fatalf("got %q", got)
	}
}

func TestTextDropsInvalidUTF8(t *testing.T) {
	got := Text("ok\xffbad", 64)
	if got != "okbad" {
		t.Fatalf("got %q", got)
	}
}
