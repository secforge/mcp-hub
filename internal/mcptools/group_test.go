package mcptools

import (
	"context"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/agenttest"
	"github.com/secforge/mcp-hub/internal/wire"
)

func groupServer(t *testing.T, grants ...wire.Assignment) *agenttest.Server {
	srv := agenttest.New(t, grants...)
	srv.AgentFeatures = map[string]json.RawMessage{"startConversation": json.RawMessage(
		`{"accounts":[{"account":"acc-1","name":"assistant@secforge.de","channel":"teams","groups":true},` +
			`{"account":"acc-2","name":"desk@secforge.de","channel":"mail"}]}`)}
	return srv
}

func groupReq(args map[string]any) mcp.CallToolRequest {
	req := mcp.CallToolRequest{}
	req.Params.Arguments = args
	return req
}

func TestAGroupIsStartedAndOpenAsItsOwnConnection(t *testing.T) {
	srv := groupServer(t, agentGrant("conv-1", "Alpha", wire.AccessReadWrite))
	var got wire.StartGroup
	var mu sync.Mutex
	srv.OnStartGroup = func(q wire.StartGroup) any {
		mu.Lock()
		got = q
		mu.Unlock()
		srv.Assign(agentGrant("conv-g", "Project", wire.AccessReadWrite))
		return wire.GroupStarted{Type: wire.TypeGroupStarted, ID: q.ID, Conv: "conv-g"}
	}
	h := NewHub()
	out := connectAgentForTest(t, h, srv)
	if !strings.Contains(out, "and group chats (hub_start_group) as: assistant@secforge.de (teams)") ||
		strings.Contains(out, "hub_start_group) as: assistant@secforge.de (teams); desk") {
		t.Fatalf("the connect result does not name exactly the group account:\n%s", out)
	}
	res, _ := h.handleStartGroup(context.Background(), groupReq(map[string]any{"connection": "desk",
		"account": "assistant@secforge.de", "contacts": []any{"a@example.com", " b@example.com"},
		"topic": "  Project  "}))
	text := textOf(res)
	if res.IsError || !strings.Contains(text, "a new group chat with 2 people") {
		t.Fatalf("result: %s", text)
	}
	if s := childNamed(t, h, "conv-g"); !strings.Contains(text, strconv.Quote(s.name)) {
		t.Fatalf("the result does not name the new connection %q: %s", s.name, text)
	}
	mu.Lock()
	defer mu.Unlock()
	if !strings.HasPrefix(got.ID, "group-") || got.Account != "acc-1" || got.Topic != "Project" ||
		strings.Join(got.Contacts, ",") != "a@example.com,b@example.com" {
		t.Fatalf("request sent: %+v", got)
	}
}

func TestABadGroupStartIsRefusedBeforeAnythingIsSent(t *testing.T) {
	srv := groupServer(t, agentGrant("conv-1", "Alpha", wire.AccessReadWrite))
	h := NewHub()
	connectAgentForTest(t, h, srv)
	before := len(srv.Controls())
	for _, tc := range []struct {
		args map[string]any
		want string
	}{
		{map[string]any{"account": "acc-2", "contacts": []any{"a@x.de", "b@x.de"}}, "one-to-one conversations only"},
		{map[string]any{"account": "acc-1", "contacts": []any{"a@x.de"}}, "between 2 and 249"},
		{map[string]any{"account": "acc-1", "contacts": []any{"a@x.de", "A@x.de"}}, "listed twice"},
		{map[string]any{"account": "acc-1", "contacts": []any{"a@x.de", "b@x.de, c@x.de"}}, "one email address"},
		{map[string]any{"account": "acc-1", "contacts": []any{"a@x.de", "b@x.de"},
			"topic": strings.Repeat("t", 251)}, "at most 250"},
		{map[string]any{"account": "acc-1", "contacts": []any{"a@x.de", "b@x.de"},
			"requestId": "start-1"}, "is not one a hub_start_group result gave"},
	} {
		tc.args["connection"] = "desk"
		res, _ := h.handleStartGroup(context.Background(), groupReq(tc.args))
		if !res.IsError || !strings.Contains(textOf(res), tc.want) {
			t.Errorf("%v: %s", tc.args, textOf(res))
		}
	}
	if len(srv.Controls()) != before {
		t.Fatal("a refused-locally start was sent anyway")
	}
}

// Every startGroup creates a new chat, so an unfinished one is finished by
// repeating its id, never by starting again under a new one.
func TestAnIncompleteGroupStartOffersItsIdAndTheRepeatSendsIt(t *testing.T) {
	srv := groupServer(t, agentGrant("conv-1", "Alpha", wire.AccessReadWrite))
	var ids []string
	var mu sync.Mutex
	srv.OnStartGroup = func(q wire.StartGroup) any {
		mu.Lock()
		ids = append(ids, q.ID)
		n := len(ids)
		mu.Unlock()
		if n == 1 {
			return wire.Error{Type: wire.TypeError, ID: q.ID, Code: "start_incomplete",
				Message: "sharing did not complete", Retryable: true}
		}
		srv.Assign(agentGrant("conv-g", "Project", wire.AccessReadWrite))
		return wire.GroupStarted{Type: wire.TypeGroupStarted, ID: q.ID, Conv: "conv-g"}
	}
	h := NewHub()
	connectAgentForTest(t, h, srv)
	args := map[string]any{"connection": "desk", "account": "acc-1", "contacts": []any{"a@x.de", "b@x.de"}}
	res, _ := h.handleStartGroup(context.Background(), groupReq(args))
	m := regexp.MustCompile(`requestId "(group-[^"]+)"`).FindStringSubmatch(textOf(res))
	if !res.IsError || m == nil || !strings.Contains(textOf(res), "never creates a second one") {
		t.Fatalf("the incomplete start: %s", textOf(res))
	}
	args["requestId"] = m[1]
	res, _ = h.handleStartGroup(context.Background(), groupReq(args))
	if res.IsError || !strings.Contains(textOf(res), "no second group was created") {
		t.Fatalf("the repeat: %s", textOf(res))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ids) != 2 || ids[0] != ids[1] || ids[0] != m[1] {
		t.Fatalf("ids sent: %v, offered %q", ids, m[1])
	}
}

func addReq(conn string, contacts []any, history string) mcp.CallToolRequest {
	return groupReq(map[string]any{"connection": conn, "contacts": contacts, "history": history})
}

func TestMembersAreAddedWithTheHistoryAsked(t *testing.T) {
	srv := groupServer(t, agentGrant("conv-1", "Alpha", wire.AccessReadWrite))
	srv.Features = map[string]json.RawMessage{wire.FeatureAddMembers: json.RawMessage(`true`),
		wire.FeatureCorrelation: json.RawMessage(`{}`)}
	var frames []json.RawMessage
	var mu sync.Mutex
	srv.OnFrame = func(f agenttest.Frame) any {
		if f.Type != wire.TypeAddMembers {
			return nil
		}
		var q wire.AddMembers
		_ = json.Unmarshal(f.Raw, &q)
		mu.Lock()
		frames = append(frames, f.Raw)
		mu.Unlock()
		return wire.AddMembersAck{Type: wire.TypeAddMembersAck, ID: q.ID, OK: true, Added: 1, Skipped: 1}
	}
	h := NewHub()
	connectAgentForTest(t, h, srv)
	name := childNamed(t, h, "conv-1").name
	res, _ := h.handleListConnections(context.Background(), mcp.CallToolRequest{})
	if !strings.Contains(textOf(res), "can add members (hub_add_members)") {
		t.Fatalf("the listing does not say members can be added:\n%s", textOf(res))
	}
	res, _ = h.handleAddMembers(context.Background(), addReq(name, []any{"a@x.de", "b@x.de"}, "none"))
	if res.IsError || !strings.Contains(textOf(res), "Added 1") || !strings.Contains(textOf(res), "1 was a member already") {
		t.Fatalf("result: %s", textOf(res))
	}
	res, _ = h.handleAddMembers(context.Background(), addReq(name, []any{"c@x.de"}, "2026-10-20T00:00:00+02:00"))
	if res.IsError {
		t.Fatalf("result: %s", textOf(res))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(frames) != 2 {
		t.Fatalf("frames: %d", len(frames))
	}
	var first, second struct {
		Conv     string          `json:"conv"`
		Contacts []string        `json:"contacts"`
		History  json.RawMessage `json:"history"`
	}
	_ = json.Unmarshal(frames[0], &first)
	_ = json.Unmarshal(frames[1], &second)
	if first.Conv != "conv-1" || string(first.History) != `"none"` || len(first.Contacts) != 2 {
		t.Fatalf("first frame: %s", frames[0])
	}
	if string(second.History) != `{"from":"2026-10-20T00:00:00+02:00"}` {
		t.Fatalf("second frame: %s", frames[1])
	}
}

func TestAPartlyDoneAdditionNamesWhoWasNotAdded(t *testing.T) {
	srv := groupServer(t, agentGrant("conv-1", "Alpha", wire.AccessReadWrite))
	srv.Features = map[string]json.RawMessage{wire.FeatureCorrelation: json.RawMessage(`{}`)}
	srv.OnFrame = func(f agenttest.Frame) any {
		if f.Type != wire.TypeAddMembers {
			return nil
		}
		var q wire.AddMembers
		_ = json.Unmarshal(f.Raw, &q)
		added, skipped := 1, 0
		return wire.Error{Type: wire.TypeError, ID: q.ID, Code: "add_failed", Message: "Graph refused one",
			Retryable: true, Added: &added, Skipped: &skipped, Failed: []string{"b@x.de"}}
	}
	h := NewHub()
	connectAgentForTest(t, h, srv)
	name := childNamed(t, h, "conv-1").name
	res, _ := h.handleListConnections(context.Background(), mcp.CallToolRequest{})
	if strings.Contains(textOf(res), "can add members") {
		t.Fatalf("the listing offers adding where the server does not declare it:\n%s", textOf(res))
	}
	res, _ = h.handleAddMembers(context.Background(), addReq(name, []any{"a@x.de", "b@x.de"}, "all"))
	text := textOf(res)
	if !res.IsError || !strings.Contains(text, "1 added") || !strings.Contains(text, "NOT added: b@x.de") {
		t.Fatalf("result: %s", text)
	}
	before := len(srv.Frames("conv-1"))
	for _, h2 := range []string{"", "yesterday", "2026-10-20T00:00:00", "2999-01-01T00:00:00Z"} {
		res, _ = h.handleAddMembers(context.Background(), addReq(name, []any{"a@x.de"}, h2))
		if !res.IsError {
			t.Errorf("history %q accepted", h2)
		}
	}
	if len(srv.Frames("conv-1")) != before {
		t.Fatal("a bad history was sent anyway")
	}
}

func TestARepeatedIdWithOtherArgumentsSaysSo(t *testing.T) {
	srv := groupServer(t, agentGrant("conv-1", "Alpha", wire.AccessReadWrite))
	srv.OnStartGroup = func(q wire.StartGroup) any {
		return wire.Error{Type: wire.TypeError, ID: q.ID, Code: "bad_request", Message: "id asks for another group"}
	}
	h := NewHub()
	connectAgentForTest(t, h, srv)
	res, _ := h.handleStartGroup(context.Background(), groupReq(map[string]any{"connection": "desk",
		"account": "acc-1", "contacts": []any{"a@x.de", "b@x.de"}, "requestId": "group-1"}))
	if !res.IsError || !strings.Contains(textOf(res), "belongs to a start with other arguments") {
		t.Fatalf("result: %s", textOf(res))
	}
}
