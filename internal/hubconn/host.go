package hubconn

import (
	"fmt"
	"net/url"
	"strings"
)

// normalizeHost validates a hub_connect "host" value and returns it in
// canonical form (scheme + authority + optional base path, no trailing
// slash). It rewrites the common mistake of using http(s):// instead of
// ws(s):// — mcp-hub servers are also reachable over plain HTTPS for
// humans browsing to them, so this mix-up is expected.
//
// A path is allowed and treated as a base prefix a server is served
// under (e.g. behind a reverse proxy, or alongside a SPA that owns the
// bare root) — sessionId is still appended as a further path segment on
// top of it, exactly as it would be on a bare host. Query and fragment
// are still rejected outright: unlike a base path, neither has any
// legitimate reason to appear in a "host" value, and their presence is a
// much stronger signal that the caller pasted something meant for a
// specific session (e.g. a full connect URL, or a fragment-delimited
// relay link — see DialRelay) rather than a server address.
func normalizeHost(host string) (string, error) {
	u, err := url.Parse(host)
	if err != nil {
		return "", fmt.Errorf("invalid host %q: %v", host, err)
	}
	switch u.Scheme {
	case "ws", "wss":
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf(
			"host must use ws:// or wss:// (got %q) — sessionId is appended "+
				"automatically, do not include it in host", host)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf(
			"host must not contain a query or fragment (got %q) — "+
				"pass just the server address (a base path is fine), e.g. "+
				"wss://mcp-hub.secforge.de or wss://example.com/hub", host)
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	return u.String(), nil
}
