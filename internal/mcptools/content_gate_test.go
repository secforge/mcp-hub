package mcptools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/wire"
)

func connectWithFeatures(t *testing.T, features map[string]json.RawMessage) *Hub {
	t.Helper()
	joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", "")
	joined.Features = features
	link := startRelayTestServerWithJoined(t, joined)
	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": testConn, "link": link}
	if res, err := hub.handleConnect(context.Background(), req); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	t.Cleanup(func() { hub.handleDisconnect(context.Background(), connReqFor(testConn)) })
	return hub
}

func sendWith(hub *Hub, extra map[string]any) *mcp.CallToolResult {
	args := map[string]any{"connection": testConn, "text": "hello"}
	for k, v := range extra {
		args[k] = v
	}
	req := mcp.CallToolRequest{}
	req.Params.Arguments = args
	res, _ := hub.handleSend(context.Background(), req)
	return res
}

// A server that declares its features, and not these three, refuses them
// on arrival — so they are refused here, with the reason, before anything
// is read or sent. The attachment case uses a path that does not exist:
// being told about the missing feature rather than the missing file is
// what shows the file was never opened.
func TestUndeclaredContentIsRefusedLocally(t *testing.T) {
	hub := connectWithFeatures(t, map[string]json.RawMessage{"messageAfter": json.RawMessage(`{}`)})
	for _, tc := range []struct {
		name    string
		extra   map[string]any
		feature string
	}{
		{"attachment", map[string]any{"filePath": "/does-not-exist/file.bin"}, `"attachments"`},
		{"image", map[string]any{"imagePath": "/does-not-exist/pic.png"}, `"attachments"`},
		{"replyTo", map[string]any{"replyTo": "ext-1"}, `"replyTo"`},
		{"mentions", map[string]any{"mentions": []any{map[string]any{"name": "Someone"}}}, `"mentions"`},
	} {
		res := sendWith(hub, tc.extra)
		if res == nil || !res.IsError || !strings.Contains(textOf(res), tc.feature) {
			t.Errorf("%s: want a local refusal naming %s, got %+v", tc.name, tc.feature, res)
		}
	}
	if res := sendWith(hub, map[string]any{"mentions": []any{}}); res == nil || res.IsError {
		t.Errorf("an empty mentions list asks for no mention and must not be refused: %+v", res)
	}
}

func TestDeclaredContentIsSent(t *testing.T) {
	hub := connectWithFeatures(t, map[string]json.RawMessage{
		"replyTo": json.RawMessage(`{}`), "mentions": json.RawMessage(`{}`),
	})
	if res := sendWith(hub, map[string]any{"replyTo": "ext-1"}); res == nil || res.IsError {
		t.Errorf("replyTo is declared and was refused: %+v", res)
	}
	if res := sendWith(hub, map[string]any{"mentions": []any{map[string]any{"name": "Someone"}}}); res == nil || res.IsError {
		t.Errorf("mentions are declared and were refused: %+v", res)
	}
}

// A server that declares nothing has said nothing either way, so content
// goes out as before and the server's answer decides.
func TestContentIsSentWhenNoFeaturesAreDeclared(t *testing.T) {
	hub := connectWithFeatures(t, nil)
	if res := sendWith(hub, map[string]any{"replyTo": "ext-1"}); res == nil || res.IsError {
		t.Errorf("a server declaring no features refused replyTo locally: %+v", res)
	}
}

// A declared formats list is the whole vocabulary for that conversation:
// an unlisted value is refused here with the list in the reason, a listed
// one goes out. The Slack shape is used because html there is the case
// that posted literal tags before.
func TestFormatIsGatedOnTheDeclaredList(t *testing.T) {
	hub := connectWithFeatures(t, map[string]json.RawMessage{
		"formats": json.RawMessage(`{"accepted":["text","markdown"]}`),
	})
	res := sendWith(hub, map[string]any{"format": "html"})
	if res == nil || !res.IsError || !strings.Contains(textOf(res), "[text, markdown]") {
		t.Errorf("html on a text/markdown conversation: want a refusal listing what is accepted, got %+v", res)
	}
	for _, ok := range []string{"markdown", "text", ""} {
		if res := sendWith(hub, map[string]any{"format": ok}); res == nil || res.IsError {
			t.Errorf("format %q is accepted and was refused: %+v", ok, res)
		}
	}
}

func TestFormatPassesThroughWithoutADeclaredList(t *testing.T) {
	hub := connectWithFeatures(t, map[string]json.RawMessage{"messageAfter": json.RawMessage(`{}`)})
	if res := sendWith(hub, map[string]any{"format": "html"}); res == nil || res.IsError {
		t.Errorf("a server declaring no formats had html refused locally: %+v", res)
	}
}

// The connect result names what this conversation takes and which to
// reach for: markdown where accepted, html only for what markdown cannot
// say. Nothing is said where the server declares no formats.
func TestFormatsNoteRecommendsMarkdown(t *testing.T) {
	for _, tc := range []struct {
		accepted []string
		want     string
	}{
		{[]string{"text", "markdown", "html"}, `use "html" only for what markdown cannot express`},
		{[]string{"text", "markdown"}, `Use format="markdown" whenever`},
		{[]string{"text", "html"}, "markdown is not rendered here"},
		{[]string{"text"}, "Plain text only"},
	} {
		if got := formatsNote(tc.accepted, ""); !strings.Contains(got, tc.want) || !strings.Contains(got, strings.Join(tc.accepted, ", ")) {
			t.Errorf("formats %v: note %q does not list them or lacks %q", tc.accepted, got, tc.want)
		}
	}
	if got := formatsNote(nil, "markdown"); got != "" {
		t.Errorf("no declared formats should say nothing, got %q", got)
	}
}

// What an omitted format means is said when the server declares it, and
// only then — on a markdown-default conversation with the warning that
// literal content needs format="text".
func TestFormatsNoteStatesTheDeclaredDefault(t *testing.T) {
	if got := formatsNote([]string{"markdown", "text"}, "markdown"); !strings.Contains(got, "rendered as MARKDOWN") ||
		!strings.Contains(got, `format="text"`) {
		t.Errorf("markdown default: %q", got)
	}
	if got := formatsNote([]string{"text", "markdown"}, "text"); !strings.Contains(got, "shown as plain text") {
		t.Errorf("text default: %q", got)
	}
	if got := formatsNote([]string{"text", "markdown"}, ""); strings.Contains(got, "no format") {
		t.Errorf("an undeclared default must not be described: %q", got)
	}
}

func TestTheDeclaredDefaultIsReadFromJoined(t *testing.T) {
	hub := connectWithFeatures(t, map[string]json.RawMessage{
		"formats": json.RawMessage(`{"accepted":["markdown","text"],"default":"markdown"}`),
	})
	s := sole(t, hub)
	conn, _ := s.activeConn()
	if got := conn.DefaultFormat(); got != "markdown" {
		t.Fatalf("DefaultFormat = %q, want markdown", got)
	}
}
