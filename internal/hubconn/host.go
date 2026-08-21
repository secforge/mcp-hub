package hubconn

import (
	"fmt"
	"net/url"
)

// normalizeHost validates a hub_connect "host" value and returns it in
// canonical form (scheme + authority only, no trailing slash). It rewrites
// the common mistake of using http(s):// instead of ws(s):// — mcp-hub
// servers are also reachable over plain HTTPS for humans browsing to them,
// so this mix-up is expected — but rejects anything containing a path,
// query, or fragment, since sessionId is appended as a path segment
// automatically and guessing what the caller meant there would be unsafe.
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
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf(
			"host must not contain a path, query, or fragment (got %q) — "+
				"pass just the server address, e.g. wss://mcp-hub.secforge.de", host)
	}
	u.Path = ""
	return u.String(), nil
}
