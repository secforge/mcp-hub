package wsserver

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

// The client sends its protocol version in the Hub-Protocol-Version
// header. A current client must not be logged as a version-1 one, and a
// real mismatch must still be logged with the version actually sent.
func TestProtocolVersionIsReadFromTheHeader(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/550e8400-e29b-41d4-a716-446655440000"

	for _, tc := range []struct {
		version int
		logged  bool
	}{
		{wire.ProtocolVersion, false},
		{wire.ProtocolVersion - 1, true},
	} {
		buf.Reset()
		header := http.Header{"Hub-Protocol-Version": {strconv.Itoa(tc.version)}}
		c, _, err := websocket.DefaultDialer.Dial(url, header)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		readTyped(t, c)
		c.Close()
		got := buf.String()
		if strings.Contains(got, "protocol version") != tc.logged {
			t.Errorf("version %d: logged %q, want a mismatch line: %v", tc.version, got, tc.logged)
		}
		if tc.logged && !strings.Contains(got, "protocol version "+strconv.Itoa(tc.version)) {
			t.Errorf("version %d: the line does not name the version sent: %q", tc.version, got)
		}
	}
}
