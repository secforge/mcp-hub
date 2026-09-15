package sanitize

import "testing"

func TestTextStripsControlCharsAndNewlines(t *testing.T) {
	got := Text("Alice\n2026-01-01T00:00:00Z fake-peer joined\x00\x1b[31m", 64)
	want := "Alice2026-01-01T00:00:00Z fake-peer joined[31m"
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
	got := Text("  Alice  ", 64)
	if got != "Alice" {
		t.Fatalf("got %q", got)
	}
}

func TestTextDropsInvalidUTF8(t *testing.T) {
	got := Text("ok\xffbad", 64)
	if got != "okbad" {
		t.Fatalf("got %q", got)
	}
}

// Format characters are invisible and act on what surrounds them, which
// is exactly what this function exists to prevent — and unicode.IsControl
// does not cover them.
func TestTextStripsInvisibleFormatCharacters(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"right-to-left override reorders what follows", "alice‮bob", "alicebob"},
		{"zero-width space", "a​b", "ab"},
		{"zero-width joiner", "a‍b", "ab"},
		{"bidi isolate", "a⁦b⁩c", "abc"},
		{"soft hyphen", "a­b", "ab"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Text(tc.in, 64); got != tc.want {
				t.Fatalf("Text(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
