package mcptools

import (
	"strings"
	"testing"
)

// The header is a parser between a model's prose and the wire, so the
// thing it must never do is act on text that was not meant as an
// instruction — and the thing it must never do quietly is ignore one that
// was.
func TestInboxHeaderRecognisesOnlyAFirstLineDirective(t *testing.T) {
	for _, tc := range []struct {
		name     string
		in       string
		wantBody string
		wantTo   string
		present  bool
	}{
		{
			"plain prose is untouched",
			"just a message", "just a message", "", false,
		},
		{
			"to= in the body is prose",
			"I will send to=someone later", "I will send to=someone later", "", false,
		},
		{
			"to= on a later line is prose",
			"hello\n#hub to=peer-1", "hello\n#hub to=peer-1", "", false,
		},
		{
			"a first-line directive is parsed off",
			"#hub to=peer-1\nthe message", "the message", "peer-1", true,
		},
		{
			"blank lines after the header are not part of the body",
			"#hub to=peer-1\n\n\nthe message", "the message", "peer-1", true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, body, err := parseInboxHeader(tc.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if body != tc.wantBody {
				t.Errorf("body = %q, want %q", body, tc.wantBody)
			}
			if h.To != tc.wantTo {
				t.Errorf("to = %q, want %q", h.To, tc.wantTo)
			}
			if h.Present != tc.present {
				t.Errorf("Present = %v, want %v", h.Present, tc.present)
			}
		})
	}
}

// A mistyped directive relayed as prose would put a private message on
// the broadcast and tell nobody. Refusing is the only outcome that
// reaches the sender.
func TestAMistypedDirectiveIsRefusedRatherThanRelayed(t *testing.T) {
	for _, in := range []string{
		"#hub too=peer-1\nbody",         // typo
		"#hub mentions=alice\nbody",     // needs hub_send
		"#hub attach=/etc/passwd\nbody", // needs hub_send
		"#hub format=markdown\nbody",    // not a format
		"#hub to\nbody",                 // not key=value
		"#hub to=\nbody",                // empty value
		"#hub \nbody",                   // no directives
	} {
		if _, _, err := parseInboxHeader(in); err == nil {
			t.Errorf("accepted %q — a directive this parser does not understand must not be sent", in)
		}
	}
}

// Every accepted directive is echoed, so a misparse is visible at once
// rather than inferred later from where the message ended up.
func TestAcceptedDirectivesAreEchoed(t *testing.T) {
	h, body, err := parseInboxHeader("#hub to=peer-1 replyTo=ext-9 format=html\nhello")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if body != "hello" {
		t.Fatalf("body = %q", body)
	}
	got := h.Summary()
	for _, want := range []string{"to=peer-1", "replyTo=ext-9", "format=html"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary %q does not report %q", got, want)
		}
	}
}
