package hubconn

import "testing"

func TestNormalizeHostAcceptsWsAndWss(t *testing.T) {
	cases := map[string]string{
		"ws://localhost:8765":        "ws://localhost:8765",
		"wss://mcp-hub.secforge.de":  "wss://mcp-hub.secforge.de",
		"wss://mcp-hub.secforge.de/": "wss://mcp-hub.secforge.de",
	}
	for in, want := range cases {
		got, err := normalizeHost(in)
		if err != nil {
			t.Errorf("normalizeHost(%q) unexpected error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("normalizeHost(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeHostRewritesHTTPSchemes(t *testing.T) {
	cases := map[string]string{
		"http://localhost:8765":       "ws://localhost:8765",
		"https://mcp-hub.secforge.de": "wss://mcp-hub.secforge.de",
	}
	for in, want := range cases {
		got, err := normalizeHost(in)
		if err != nil {
			t.Errorf("normalizeHost(%q) unexpected error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("normalizeHost(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeHostRejectsPathQueryOrFragment(t *testing.T) {
	cases := []string{
		"wss://mcp-hub.secforge.de/ws",
		"wss://mcp-hub.secforge.de/some/path",
		"wss://mcp-hub.secforge.de?x=1",
		"wss://mcp-hub.secforge.de#frag",
	}
	for _, in := range cases {
		if _, err := normalizeHost(in); err == nil {
			t.Errorf("normalizeHost(%q): expected an error, got none", in)
		}
	}
}

func TestNormalizeHostRejectsMissingScheme(t *testing.T) {
	if _, err := normalizeHost("mcp-hub.secforge.de"); err == nil {
		t.Fatal("expected an error for a host with no scheme")
	}
}

func TestNormalizeHostRejectsUnknownScheme(t *testing.T) {
	if _, err := normalizeHost("ftp://mcp-hub.secforge.de"); err == nil {
		t.Fatal("expected an error for an unsupported scheme")
	}
}
