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

func TestNormalizeHostAcceptsBasePath(t *testing.T) {
	// A server may be served under a path prefix (e.g. behind a reverse
	// proxy or alongside a SPA that owns the bare root) — sessionId is
	// still appended as a further path segment, so this must survive
	// unchanged other than a stripped trailing slash.
	cases := map[string]string{
		"wss://chat-relay.secforge.de/hub":         "wss://chat-relay.secforge.de/hub",
		"wss://chat-relay.secforge.de/hub/":         "wss://chat-relay.secforge.de/hub",
		"wss://mcp-hub.secforge.de/some/deep/path":  "wss://mcp-hub.secforge.de/some/deep/path",
		"wss://mcp-hub.secforge.de/some/deep/path/": "wss://mcp-hub.secforge.de/some/deep/path",
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

func TestNormalizeHostRejectsQueryOrFragment(t *testing.T) {
	cases := []string{
		"wss://mcp-hub.secforge.de?x=1",
		"wss://mcp-hub.secforge.de#frag",
		"wss://mcp-hub.secforge.de/hub?x=1",
		"wss://mcp-hub.secforge.de/hub#frag",
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
