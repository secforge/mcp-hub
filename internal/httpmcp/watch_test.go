package httpmcp

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/hubsession"
)

func TestWatchUnknownTokenReturns404(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	srv := httptest.NewServer(s.WatchHandler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "?token=does-not-exist")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestWatchSingleShotReturnsOnePendingEventThenCloses(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	_, sessionID, watchToken, err := s.connect("mcp-a", "", "", "", "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, _, _, err := s.connect("mcp-b", sessionID, "", "", ""); err != nil {
		t.Fatalf("connect (b): %v", err)
	}
	callTool(t, ctxForSession("mcp-b"), s, s.handleSend, map[string]any{"text": "hi from b"})

	srv := httptest.NewServer(s.WatchHandler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "?token=" + watchToken)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body := make([]byte, 4096)
	n, _ := resp.Body.Read(body)
	if !strings.Contains(string(body[:n]), "hi from b") {
		t.Fatalf("expected the watch stream to contain the pending message, got %q", string(body[:n]))
	}
}

func TestWatchFollowStreamsLiveEvents(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	_, sessionID, watchToken, err := s.connect("mcp-a", "", "", "", "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, _, _, err := s.connect("mcp-b", sessionID, "", "", ""); err != nil {
		t.Fatalf("connect (b): %v", err)
	}

	srv := httptest.NewServer(s.WatchHandler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"?token="+watchToken+"&follow=1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	go func() {
		time.Sleep(30 * time.Millisecond)
		callTool(t, ctxForSession("mcp-b"), s, s.handleSend, map[string]any{"text": "live event"})
	}()

	scanner := bufio.NewScanner(resp.Body)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && scanner.Scan() {
		if strings.Contains(scanner.Text(), "live event") {
			return
		}
	}
	t.Fatal("expected to see the live event on the follow stream")
}
