package wsserver

import (
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/secforge/mcp-hub/internal/hubconn"
)

func TestCurrentClientPreservesIdentityAgainstOwnServer(t *testing.T) {
	skipKnownIssue(t, "the client's identity headers are ignored by the bundled server")
	ts := httptest.NewServer(NewHandler())
	defer ts.Close()
	// Even supply the legacy UUID path so routing is not the failure.
	link := "ws" + strings.TrimPrefix(ts.URL, "http") + "/550e8400-e29b-41d4-a716-446655440000#550e8400-e29b-41d4-a716-446655440000"
	opts := hubconn.DialOptions{ReconnectSecret: "review-secret", Name: "review-client"}
	first, err := hubconn.Dial(link, opts)
	if err != nil {
		t.Fatal(err)
	}
	id := first.PeerID()
	if first.Name() != opts.Name {
		t.Errorf("server ignored Agent-Name: got %q", first.Name())
	}
	first.Close()
	second, err := hubconn.Dial(link, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if second.PeerID() != id {
		t.Error("server ignored Agent-Secret; reconnect received a new peer identity")
	}
}

// skipKnownIssue skips a reproduction of a defect recorded in
// docs/known-issues.md that is still present. Set MCP_HUB_KNOWN_ISSUES=1 to
// run it; it fails until the defect is fixed, and then the skip goes.
func skipKnownIssue(t *testing.T, issue string) {
	t.Helper()
	if os.Getenv("MCP_HUB_KNOWN_ISSUES") == "" {
		t.Skipf("known issue, still present: %s (docs/known-issues.md); MCP_HUB_KNOWN_ISSUES=1 runs it", issue)
	}
}
