package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/wire"
)

const (
	selfPeer  = "550e8400-e29b-41d4-a716-446655440000"
	alicePeer = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
)

// rosterServer joins every client into a conversation whose member list
// holds the client and Alice, acknowledges every msg, and records them.
func rosterServer(t *testing.T) (link string, sent func() []map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var got []map[string]any
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		j := wire.NewJoined(selfPeer, "", "")
		j.Features = map[string]json.RawMessage{"roster": json.RawMessage(`{}`),
			"actionAcks": json.RawMessage(`{}`), "correlation": json.RawMessage(`{}`)}
		ws.WriteJSON(j)
		ws.WriteJSON(wire.NewRoster([]wire.RosterMember{{PeerID: selfPeer, Name: "me"}, {PeerID: alicePeer, Name: "Alice"}}))
		for {
			var f map[string]any
			if err := ws.ReadJSON(&f); err != nil {
				return
			}
			if f["type"] != "msg" {
				continue
			}
			mu.Lock()
			got = append(got, f)
			mu.Unlock()
			id, _ := f["id"].(string)
			ws.WriteJSON(wire.SendAck{Type: wire.TypeSendAck, ID: id, ExternalID: "e1", OK: true})
		}
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/hub/join#send-test", func() []map[string]any {
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]any(nil), got...)
	}
}

func TestSendGoesPrivatelyOnlyWhenThePeerIsPresent(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	t.Setenv("MCP_HUB_PROJECT_DIR", t.TempDir())
	link, sent := rosterServer(t)
	run := func(args ...string) (int, string, string) {
		var out, errb bytes.Buffer
		code := runSend(append([]string{"--link", link}, args...), strings.NewReader(""), &out, &errb)
		return code, out.String(), errb.String()
	}

	if code, out, errs := run("--if-present", "alice", "hi Alice"); code != sendOK || !strings.Contains(out, "privately to "+alicePeer) {
		t.Fatalf("present by name: code %d out %q err %q", code, out, errs)
	}
	if f := sent()[0]; f["to"] != alicePeer || f["text"] != "hi Alice" {
		t.Fatalf("not sent privately to Alice: %v", f)
	}

	if code, _, errs := run("--if-present", "Bob", "hi Bob"); code != sendNotPresent || !strings.Contains(errs, "nothing was sent") {
		t.Fatalf("absent peer: code %d err %q", code, errs)
	}
	if len(sent()) != 1 {
		t.Fatalf("something was sent for an absent peer: %v", sent())
	}

	if code, _, errs := run("hello all"); code != sendOK {
		t.Fatalf("broadcast: code %d err %q", code, errs)
	}
	if f := sent()[1]; f["to"] != nil || f["text"] != "hello all" {
		t.Fatalf("broadcast went out wrong: %v", f)
	}
}

func TestSendNeedsALink(t *testing.T) {
	var out, errb bytes.Buffer
	if code := runSend([]string{"hi"}, strings.NewReader(""), &out, &errb); code != sendUsage {
		t.Fatalf("code %d", code)
	}
}

// A link another client has claimed is refused at the upgrade; the error
// says to create a new link rather than reuse that one.
func TestSendExplainsALinkClaimedByAnotherClient(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	t.Setenv("MCP_HUB_PROJECT_DIR", t.TempDir())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)
	var out, errb bytes.Buffer
	code := runSend([]string{"--link", "ws" + strings.TrimPrefix(srv.URL, "http") + "/hub/join#taken", "hi"},
		strings.NewReader(""), &out, &errb)
	if code != sendError || !strings.Contains(errb.String(), "create a new link") {
		t.Fatalf("code %d err %q", code, errb.String())
	}
}

// --connection takes link and identity from the connection stored under
// --project by that name, and refuses one a running client holds.
func TestSendByStoredConnectionName(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	project := t.TempDir() + "+notifier"
	link, sent := rosterServer(t)
	target := connstore.Target{Link: link, Project: connstore.NormalizeProject(project)}
	if err := connstore.Upsert(target, connstore.Entry{LocalName: "notifier", ReconnectSecret: "s1",
		PeerID: selfPeer}); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (int, string) {
		var out, errb bytes.Buffer
		code := runSend(args, strings.NewReader(""), &out, &errb)
		return code, errb.String()
	}
	if code, errs := run("--project", project, "--connection", "notifier", "via name"); code != sendOK {
		t.Fatalf("by name: code %d err %q", code, errs)
	}
	if f := sent(); len(f) != 1 || f[0]["text"] != "via name" {
		t.Fatalf("sent %v", f)
	}
	if code, errs := run("--project", project, "--connection", "nope", "x"); code != sendError ||
		!strings.Contains(errs, `no connection named "nope"`) {
		t.Fatalf("unknown name: code %d err %q", code, errs)
	}
	if err := connstore.Upsert(target, connstore.Entry{LocalName: "notifier", ReconnectSecret: "s1",
		PeerID: selfPeer, Connected: true, Holder: &connstore.Holder{PID: os.Getpid()}}); err != nil {
		t.Fatal(err)
	}
	if code, errs := run("--project", project, "--connection", "notifier", "x"); code != sendError ||
		!strings.Contains(errs, "knock that connection off") {
		t.Fatalf("held connection: code %d err %q", code, errs)
	}
	agent := connstore.Target{Link: link + "-agent", Project: target.Project}
	if err := connstore.Upsert(agent, connstore.Entry{LocalName: "desk", ReconnectSecret: "s2", Agent: true}); err != nil {
		t.Fatal(err)
	}
	conv := connstore.AgentConvTarget(agent, "conv-1")
	if err := connstore.Upsert(conv, connstore.Entry{LocalName: "desk-alpha-1a2b3c"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"desk", "desk-alpha-1a2b3c"} {
		if code, errs := run("--project", project, "--connection", name, "x"); code != sendError ||
			!strings.Contains(errs, "agent connection") {
			t.Fatalf("%s: code %d err %q", name, code, errs)
		}
	}
}
