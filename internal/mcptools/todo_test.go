package mcptools

import (
	"context"
	"encoding/json"
	"fmt"
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

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/hubenc"
	"github.com/secforge/mcp-hub/internal/wire"
)

// A connection to a todo list (§2.8b) shows the list at connect, offers the
// hub_todo_* tools, sends only the fields a call names, and refuses the
// message tools.
func TestATodoListIsReadAndChangedThroughTheTodoTools(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	var mu sync.Mutex
	var frames []map[string]any
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		items := []wire.TodoItem{
			{ExternalID: "i1", Text: "buy milk", Notes: "", Position: 1},
			{ExternalID: "i2", Text: "write flyer", Notes: "two variants", Done: true, DoneAt: "2026-10-06T10:00:00Z", Position: 2},
		}
		conn.WriteJSON(wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000",
			ServerVersion: wire.ProtocolVersion, ConversationKind: "todo",
			Features: map[string]json.RawMessage{
				"todo": json.RawMessage(`{"maxText":20,"maxNotes":100,"notesFormat":"markdown"}`), "actionAcks": json.RawMessage(`{}`),
				"delete": json.RawMessage(`{}`), "edit": json.RawMessage(`{}`), "correlation": json.RawMessage(`{}`),
			},
			Items: &items, ItemsCursor: "c7"})
		for {
			var f map[string]any
			if err := conn.ReadJSON(&f); err != nil {
				return
			}
			mu.Lock()
			frames = append(frames, f)
			mu.Unlock()
			id, _ := f["id"].(string)
			switch f["type"] {
			case "msg":
				conn.WriteJSON(wire.SendAck{Type: wire.TypeSendAck, ID: id, ExternalID: "i3", OK: true})
			case "edit":
				if f["afterId"] == "gone" {
					conn.WriteJSON(wire.Error{Type: wire.TypeError, ID: id, Code: "bad_after_id",
						Message: "afterId must be an item's externalId or null"})
					continue
				}
				conn.WriteJSON(wire.EditAck{Type: wire.TypeEditAck, ID: id, ExternalID: f["externalId"].(string), OK: true})
			case "items":
				conn.WriteJSON(wire.ItemsResponse{Type: wire.TypeItems, ID: id, List: &items, ItemsCursor: "c7"})
			}
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/hub/join#todo-test"
	ctx := context.Background()
	hub := NewHub()
	call := func(handler func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error), args map[string]any) *mcp.CallToolResult {
		t.Helper()
		req := mcp.CallToolRequest{}
		req.Params.Arguments = args
		res, err := handler(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	sent := func(kind string) map[string]any {
		mu.Lock()
		defer mu.Unlock()
		for i := len(frames) - 1; i >= 0; i-- {
			if frames[i]["type"] == kind {
				return frames[i]
			}
		}
		return nil
	}

	res := call(hub.handleConnect, map[string]any{"as": testConn, "link": link})
	t.Cleanup(func() { hub.handleDisconnect(ctx, connReqFor(testConn)) })
	text := textOf(res)
	for _, absent := range []string{"Who else is in this", "SendMessage", "mirrors a real chat"} {
		if strings.Contains(text, absent) {
			t.Errorf("a todo list's connect result carries chat guidance %q:\n%s", absent, text)
		}
	}
	for _, want := range []string{"This connection is a TODO LIST", "notes are rendered as Markdown", "[ ] buy milk (id=i1)",
		"[x] write flyer (id=i2, done 2026-10-06T10:00:00Z)", "notes: two variants"} {
		if !strings.Contains(text, want) {
			t.Fatalf("connect result lacks %q:\n%s", want, text)
		}
	}
	if id := sole(t, hub).catchUpID; true {
		if cs, _, _ := connstore.GetCatchUp(id); cs.Cursor != "c7" {
			t.Errorf("the snapshot's cursor was not adopted as the read position: %q", cs.Cursor)
		}
	}

	res = call(hub.handleTodoAdd, map[string]any{"connection": testConn, "text": "call Bob", "top": true})
	if res.IsError {
		t.Fatalf("add failed: %s", textOf(res))
	}
	if f := sent("msg"); f["text"] != "call Bob" || f["afterId"] != nil || !hasKey(f, "afterId") || hasKey(f, "notes") {
		t.Errorf("add sent %v; want text, afterId null, no notes", f)
	}

	res = call(hub.handleTodoUpdate, map[string]any{"connection": testConn, "itemId": "i1", "notes": ""})
	if res.IsError {
		t.Fatalf("update failed: %s", textOf(res))
	}
	if f := sent("edit"); f["notes"] != "" || hasKey(f, "text") || hasKey(f, "done") || hasKey(f, "afterId") {
		t.Errorf("update sent %v; want only externalId and notes \"\"", f)
	}

	res = call(hub.handleTodoUpdate, map[string]any{"connection": testConn, "itemId": "i1", "afterId": "gone"})
	if !res.IsError || !strings.Contains(textOf(res), "bad_after_id") {
		t.Errorf("a refused update was not reported as an error: %s", textOf(res))
	}

	res = call(hub.handleTodoItems, map[string]any{"connection": testConn})
	if !strings.Contains(textOf(res), "1 open, 1 done") {
		t.Errorf("items did not list the list: %s", textOf(res))
	}

	res = call(hub.handleTodoAdd, map[string]any{"connection": testConn, "text": strings.Repeat("x", 21)})
	if !res.IsError || !strings.Contains(textOf(res), "at most 20") {
		t.Errorf("an over-long item was not refused locally: %s", textOf(res))
	}

	res = call(hub.handleSend, map[string]any{"connection": testConn, "text": "hello"})
	if !res.IsError || !strings.Contains(textOf(res), "hub_todo_add") {
		t.Errorf("hub_send was not refused on a todo list: %s", textOf(res))
	}
}

func hasKey(m map[string]any, k string) bool {
	_, ok := m[k]
	return ok
}

// A todo list's catch-up walks its change log, whose entries are edits
// and removals as well as additions. Each must answer the walk's request
// and be delivered, not fall through as unsolicited.
func TestATodoCatchUpWalksEditsAndRemovals(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		empty := []wire.TodoItem{}
		conn.WriteJSON(wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000",
			ServerVersion: wire.ProtocolVersion, Items: &empty,
			Features: map[string]json.RawMessage{"todo": json.RawMessage(`{}`), "messageAfter": json.RawMessage(`{}`)}})
		for {
			var m wire.MessageAfter
			if err := conn.ReadJSON(&m); err != nil {
				return
			}
			answers := &wire.Anchor{At: m.At, Cursor: m.Cursor}
			done := true
			switch m.Cursor {
			case "":
				ed := wire.MessageEdited{Type: wire.TypeMessageEdited, ExternalID: "i1", Text: "buy oat milk",
					Cursor: "c1", Historical: true, Answers: answers}
				ed.Done = &done
				conn.WriteJSON(ed)
			case "c1":
				conn.WriteJSON(wire.MessageDeleted{Type: wire.TypeMessageDeleted, ExternalID: "i2", Cursor: "c2",
					Historical: true, Answers: answers})
			default:
				conn.WriteJSON(wire.NewNoMoreMessages(*answers))
			}
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/hub/join#todo-walk"
	ctx := context.Background()
	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": testConn, "link": link}
	if res, err := hub.handleConnect(ctx, req); err != nil || res.IsError {
		t.Fatalf("connect failed: %v %+v", err, res)
	}
	t.Cleanup(func() { hub.handleDisconnect(ctx, connReqFor(testConn)) })
	s := sole(t, hub)
	conn, _ := s.activeConn()

	var got []string
	anchor := wire.Anchor{}
	for i := 0; i < 3; i++ {
		ev, ok, err := conn.RequestMessageAfterAwaiting(anchor)
		if err != nil || !ok {
			t.Fatalf("step %d: no answer (ok=%v err=%v)", i, ok, err)
		}
		got = append(got, ev.Kind)
		if ev.Kind == "noMoreMessages" {
			break
		}
		if ev.Kind == "messageEdited" && (ev.Todo == nil || ev.Todo.Done == nil || !*ev.Todo.Done) {
			t.Errorf("the edit's item fields were lost: %+v", ev)
		}
		anchor = wire.Anchor{Cursor: ev.Cursor}
	}
	if strings.Join(got, ",") != "messageEdited,messageDeleted,noMoreMessages" {
		t.Errorf("walk answered %v", got)
	}
}

// An encrypted todo list (§2.8b, encrypted) is read and changed through the
// same tools: items open with the key, and nothing readable goes out.
func TestAnEncryptedTodoListIsReadAndChangedSealed(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	const self = "550e8400-e29b-41d4-a716-446655440000"
	const web = "ffffffff-ffff-ffff-ffff-ffffffffffff"
	keyText, _ := hubenc.NewKey()
	key, _ := hubenc.ParseKey(keyText)
	salt, _ := hubenc.NewSalt()
	check, _ := hubenc.Check(key, salt)
	sess, err := hubenc.Open(key, salt, check)
	if err != nil {
		t.Fatal(err)
	}
	b := hubenc.B64()
	seal := func(sender, mid string, rev int, text, notes string) *wire.Enc {
		plain, _ := json.Marshal(map[string]string{"text": text, "notes": notes})
		nonce, ct, _ := sess.Seal(plain, sender, mid, rev, hubenc.FieldItem())
		return &wire.Enc{V: 1, Mid: mid, Rev: rev, Nonce: b.EncodeToString(nonce), Ct: b.EncodeToString(ct)}
	}
	open := func(sender string, e map[string]any) map[string]string {
		nonce, _ := b.DecodeString(e["nonce"].(string))
		ct, _ := b.DecodeString(e["ct"].(string))
		plain, err := sess.OpenValue(nonce, ct, sender, e["mid"].(string), int(e["rev"].(float64)), hubenc.FieldItem())
		if err != nil {
			t.Fatalf("the client's envelope does not open: %v", err)
		}
		var p map[string]string
		json.Unmarshal(plain, &p)
		return p
	}
	var mu sync.Mutex
	var frames []map[string]any
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		items := []wire.TodoItem{{ExternalID: "i1", Position: 1, SealedBy: web, Enc: seal(web, "m1", 2, "buy milk", "2 litres")}}
		conn.WriteJSON(wire.Joined{Type: wire.TypeJoined, PeerID: self, ServerVersion: wire.ProtocolVersion,
			ConversationKind: "todo", Encryption: &wire.Encryption{V: 1, Salt: salt, Check: check},
			Features: map[string]json.RawMessage{"todo": json.RawMessage(`{}`), "actionAcks": json.RawMessage(`{}`),
				"edit": json.RawMessage(`{}`), "correlation": json.RawMessage(`{}`)},
			Items: &items, ItemsCursor: "c1"})
		for {
			var f map[string]any
			if err := conn.ReadJSON(&f); err != nil {
				return
			}
			mu.Lock()
			frames = append(frames, f)
			mu.Unlock()
			id, _ := f["id"].(string)
			switch f["type"] {
			case "msg":
				conn.WriteJSON(wire.SendAck{Type: wire.TypeSendAck, ID: id, ExternalID: "i2", OK: true})
			case "edit":
				conn.WriteJSON(wire.EditAck{Type: wire.TypeEditAck, ID: id, ExternalID: f["externalId"].(string), OK: true})
			}
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/hub/join#todo-enc"
	keyFile := filepath.Join(t.TempDir(), "list.key")
	os.WriteFile(keyFile, []byte(keyText+"\n"), 0o600)
	ctx := context.Background()
	hub := NewHub()
	call := func(handler func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error), args map[string]any) string {
		t.Helper()
		req := mcp.CallToolRequest{}
		req.Params.Arguments = args
		res, err := handler(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		return textOf(res)
	}
	last := func(kind string) map[string]any {
		mu.Lock()
		defer mu.Unlock()
		for i := len(frames) - 1; i >= 0; i-- {
			if frames[i]["type"] == kind {
				return frames[i]
			}
		}
		return nil
	}

	out := call(hub.handleConnect, map[string]any{"as": testConn, "link": link, "keyFile": keyFile})
	t.Cleanup(func() { hub.handleDisconnect(ctx, connReqFor(testConn)) })
	if !strings.Contains(out, "[ ] buy milk (id=i1)") || !strings.Contains(out, "2 litres") {
		t.Fatalf("the sealed list was not opened at connect:\n%s", out)
	}

	call(hub.handleTodoAdd, map[string]any{"connection": testConn, "text": "call Bob"})
	f := last("msg")
	if hasKey(f, "text") || hasKey(f, "notes") || !hasKey(f, "enc") {
		t.Fatalf("the add went out readable: %v", f)
	}
	if p := open(self, f["enc"].(map[string]any)); p["text"] != "call Bob" || p["notes"] != "" {
		t.Fatalf("the add sealed %v", p)
	}

	call(hub.handleTodoUpdate, map[string]any{"connection": testConn, "itemId": "i1", "text": "buy oat milk"})
	f = last("edit")
	enc, _ := f["enc"].(map[string]any)
	if hasKey(f, "text") || enc == nil || enc["mid"] != "m1" || enc["rev"].(float64) != 3 {
		t.Fatalf("the text change went out as %v", f)
	}
	if p := open(self, enc); p["text"] != "buy oat milk" || p["notes"] != "2 litres" {
		t.Fatalf("the change sealed %v; the notes must be kept", p)
	}

	call(hub.handleTodoUpdate, map[string]any{"connection": testConn, "itemId": "i1", "done": true})
	if f := last("edit"); hasKey(f, "enc") || f["done"] != true {
		t.Fatalf("a done-only change went out as %v; want done and no envelope", f)
	}
}

func TestALongTodoListIsWrittenToAFileAndTheConnectShowsAnOverview(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	var items []wire.TodoItem
	for i := 0; i < 300; i++ {
		items = append(items, wire.TodoItem{ExternalID: fmt.Sprintf("i%d", i), Text: fmt.Sprintf("item %d", i),
			Notes: strings.Repeat("n", 500), Position: float64(i)})
	}
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000",
			ServerVersion: wire.ProtocolVersion, ConversationKind: "todo",
			Features: map[string]json.RawMessage{"todo": json.RawMessage(`{}`), "correlation": json.RawMessage(`{}`)},
			Items:    &items, ItemsCursor: "c1"})
		for {
			var f map[string]any
			if err := conn.ReadJSON(&f); err != nil {
				return
			}
			if f["type"] == "items" {
				id, _ := f["id"].(string)
				conn.WriteJSON(wire.ItemsResponse{Type: wire.TypeItems, ID: id, List: &items, ItemsCursor: "c1"})
			}
		}
	}))
	t.Cleanup(srv.Close)
	ctx := context.Background()
	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": testConn, "link": "ws" + strings.TrimPrefix(srv.URL, "http") + "/hub/join#todo-big"}
	res, _ := hub.handleConnect(ctx, req)
	t.Cleanup(func() { hub.handleDisconnect(ctx, connReqFor(testConn)) })
	if res.IsError || len(textOf(res)) > 20<<10 || !strings.Contains(textOf(res), "more open item(s)") {
		t.Fatalf("connect result (%d bytes): %.2000s", len(textOf(res)), textOf(res))
	}
	req.Params.Arguments = map[string]any{"connection": testConn}
	res, _ = hub.handleTodoItems(ctx, req)
	m := regexp.MustCompile(`written to (\S+)\. Read it`).FindStringSubmatch(textOf(res))
	if res.IsError || m == nil {
		t.Fatalf("items result: %.2000s", textOf(res))
	}
	data, err := os.ReadFile(m[1])
	if err != nil || !strings.Contains(string(data), "item 299 (id=i299)") {
		t.Fatalf("the file %s: %v, %d bytes", m[1], err, len(data))
	}
	os.Remove(m[1])
}
