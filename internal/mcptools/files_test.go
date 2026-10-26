package mcptools

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/wire"
)

// fileServer is a hub session offering one-time files, and the /files/
// endpoint behind it, on one origin, with the rules agreed with
// chat-relay: a stored file is served once, then every GET is 404.
type fileServer struct {
	srv *httptest.Server
	// uploadURL, when set, is named instead of the server's own.
	uploadURL string

	mu       sync.Mutex
	want     map[string]wire.FileUpload
	upTok    map[string]string
	downTok  map[string]string
	stored   map[string][]byte
	sent     map[string]bool
	deleted  map[string]bool
	requests []wire.FileUpload
}

func newFileServer(t *testing.T) *fileServer {
	fs := &fileServer{want: map[string]wire.FileUpload{}, upTok: map[string]string{}, downTok: map[string]string{},
		stored: map[string][]byte{}, sent: map[string]bool{}, deleted: map[string]bool{}}
	fs.srv = httptest.NewServer(http.HandlerFunc(fs.serve))
	t.Cleanup(fs.srv.Close)
	return fs
}

func (fs *fileServer) link() string {
	return "ws" + strings.TrimPrefix(fs.srv.URL, "http") + "/hub/join#files-test"
}

func (fs *fileServer) serve(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/files/") {
		fs.hub(w, r)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/files/")
	auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	fs.mu.Lock()
	defer fs.mu.Unlock()
	switch r.Method {
	case http.MethodPut:
		want, ok := fs.want[id]
		if !ok || auth != fs.upTok[id] {
			http.NotFound(w, r)
			return
		}
		if r.ContentLength != want.Size {
			http.Error(w, "size_mismatch", http.StatusBadRequest)
			return
		}
		body, _ := io.ReadAll(r.Body)
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != want.SHA256 {
			http.Error(w, "hash_mismatch", http.StatusBadRequest)
			return
		}
		fs.stored[id] = body
		w.WriteHeader(http.StatusCreated)
	case http.MethodGet:
		body, ok := fs.stored[id]
		if !ok || fs.sent[id] || auth != fs.downTok[id] {
			http.NotFound(w, r)
			return
		}
		fs.sent[id] = true
		w.Write(body)
	case http.MethodDelete:
		if _, ok := fs.stored[id]; !ok || auth != fs.downTok[id] {
			http.NotFound(w, r)
			return
		}
		delete(fs.stored, id)
		fs.deleted[id] = true
		w.WriteHeader(http.StatusNoContent)
	}
}

func (fs *fileServer) hub(w http.ResponseWriter, r *http.Request) {
	conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.WriteJSON(wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000",
		ServerVersion: wire.ProtocolVersion, CanSend: true,
		Features: map[string]json.RawMessage{
			"files":       json.RawMessage(`{"maxBytes":1073741824,"maxSeconds":31536000}`),
			"correlation": json.RawMessage(`{}`),
		}})
	for n := 1; ; n++ {
		var f map[string]any
		if err := conn.ReadJSON(&f); err != nil {
			return
		}
		if f["type"] != "fileUpload" {
			continue
		}
		raw, _ := json.Marshal(f)
		var q wire.FileUpload
		json.Unmarshal(raw, &q)
		id := "file" + strings.Repeat("x", n)
		fs.mu.Lock()
		fs.requests = append(fs.requests, q)
		fs.want[id], fs.upTok[id], fs.downTok[id] = q, "up-"+id, "down-"+id
		target := fs.srv.URL + "/files/" + id
		if fs.uploadURL != "" {
			target = fs.uploadURL
		}
		fs.mu.Unlock()
		conn.WriteJSON(wire.FileUploadReady{Type: wire.TypeFileUploadReady, ID: q.ID, OK: true, FileID: id,
			UploadURL: target, UploadToken: "up-" + id, DownloadToken: "down-" + id,
			ExpiresAt: "2026-11-02T12:00:00Z"})
	}
}

func fileCall(t *testing.T, handler func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error), args map[string]any) *mcp.CallToolResult {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = args
	res, err := handler(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

var linkPattern = regexp.MustCompile(`(?m)^(https?://\S+/file#\S+)$`)

func connectFiles(t *testing.T, fs *fileServer) *Hub {
	t.Helper()
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	h := NewHub()
	res := fileCall(t, h.handleConnect, map[string]any{"as": testConn, "link": fs.link()})
	if res.IsError || !strings.Contains(textOf(res), "One-time files: hub_file_upload uploads a local file of up to 1.0 GB") {
		t.Fatalf("connect: %s", textOf(res))
	}
	t.Cleanup(func() { h.handleDisconnect(context.Background(), connReqFor(testConn)) })
	return h
}

func TestAFileGoesUpEncryptedAndComesDownOnceUnderItsName(t *testing.T) {
	fs := newFileServer(t)
	h := connectFiles(t, fs)
	src := filepath.Join(t.TempDir(), "build.tar")
	plain := make([]byte, 200_000)
	rand.Read(plain)
	os.WriteFile(src, plain, 0o600)

	res := fileCall(t, h.handleFileUpload, map[string]any{"connection": testConn, "path": src, "keepDays": 2})
	m := linkPattern.FindStringSubmatch(textOf(res))
	if res.IsError || m == nil || !strings.Contains(textOf(res), "downloaded ONCE") {
		t.Fatalf("upload: %s", textOf(res))
	}
	link := m[1]
	fs.mu.Lock()
	if len(fs.requests) != 1 || fs.requests[0].ExpiresIn != 2*86400 {
		t.Fatalf("requests: %+v", fs.requests)
	}
	for _, stored := range fs.stored {
		if bytes.Contains(stored, plain[:64]) || bytes.Contains(stored, []byte("build.tar")) {
			t.Fatal("the server holds plaintext")
		}
	}
	fs.mu.Unlock()

	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "build.tar"), []byte("already here"), 0o600)
	res = fileCall(t, h.handleFileDownload, map[string]any{"link": link, "directory": dir})
	if res.IsError || !strings.Contains(textOf(res), "has deleted the file") {
		t.Fatalf("download: %s", textOf(res))
	}
	got, err := os.ReadFile(filepath.Join(dir, "build-1.tar"))
	if err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("saved file: %v, equal %v", err, bytes.Equal(got, plain))
	}
	if old, _ := os.ReadFile(filepath.Join(dir, "build.tar")); string(old) != "already here" {
		t.Fatal("an existing file was overwritten")
	}
	res = fileCall(t, h.handleFileDownload, map[string]any{"link": link, "directory": dir})
	if !res.IsError || !strings.Contains(textOf(res), "gone after one download") {
		t.Fatalf("second download: %s", textOf(res))
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 2 {
		t.Fatalf("directory holds %d entries, want the two files only", len(entries))
	}
}

func TestAnUploadAddressOffTheServersOriginIsRefused(t *testing.T) {
	fs := newFileServer(t)
	fs.uploadURL = "https://elsewhere.example/files/x"
	h := connectFiles(t, fs)
	src := filepath.Join(t.TempDir(), "a.txt")
	os.WriteFile(src, []byte("secret"), 0o600)
	res := fileCall(t, h.handleFileUpload, map[string]any{"connection": testConn, "path": src})
	if !res.IsError || !strings.Contains(textOf(res), "not on its own origin") {
		t.Fatalf("upload: %s", textOf(res))
	}
}

func TestALinkToAnUnknownHostOrBrokenLinkIsRefused(t *testing.T) {
	fs := newFileServer(t)
	h := connectFiles(t, fs)
	for link, want := range map[string]string{
		"https://elsewhere.example/file#abc.def.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA": "is not a host",
		fs.srv.URL + "/file#abc.def":       "incomplete",
		fs.srv.URL + "/file#abc.def.short": "key is damaged",
		"ftp://x/file#a.b.c":               "must be https",
		fs.srv.URL + "/other#a.b.c":        "not a one-time file link",
		"http://example.com/file#a.b.c":    "must be https",
	} {
		res := fileCall(t, h.handleFileDownload, map[string]any{"link": link})
		if !res.IsError || !strings.Contains(textOf(res), want) {
			t.Errorf("%s: %s", link, textOf(res))
		}
	}
}

func TestASendersFileNameCannotLeaveTheDirectory(t *testing.T) {
	for in, want := range map[string]string{
		"../../etc/passwd": "passwd",
		`..\..\boot.ini`:   "boot.ini",
		"":                 "downloaded-file",
		"..":               "downloaded-file",
		".bashrc":          "bashrc",
		"a\nb":             "a_b",
	} {
		if got := safeSavedName(in); got != want {
			t.Errorf("%q → %q, want %q", in, got, want)
		}
	}
}
