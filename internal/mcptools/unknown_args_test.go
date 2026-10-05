package mcptools

import (
	"context"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

func TestRefuseUnknownArgumentsNamesTheClosestParameter(t *testing.T) {
	tool := mcp.NewTool("hub_send",
		mcp.WithString("connection"), mcp.WithString("text"), mcp.WithString("filePath"))
	ran := false
	h := refuseUnknownArguments(tool, func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ran = true
		return mcp.NewToolResultText("sent"), nil
	})
	call := func(args map[string]any) *mcp.CallToolResult {
		var req mcp.CallToolRequest
		req.Params.Arguments = args
		res, err := h(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	res := call(map[string]any{"connection": "relay", "text": "hi", "attachments": []any{"/tmp/x"}})
	if ran || !res.IsError {
		t.Fatal("a call with an unknown parameter ran")
	}
	msg := res.Content[0].(mcp.TextContent).Text
	if !strings.Contains(msg, `"attachments" (did you mean "filePath"?)`) {
		t.Fatalf("refusal does not point at filePath: %s", msg)
	}

	res = call(map[string]any{"conection": "relay", "text": "hi"})
	if msg := res.Content[0].(mcp.TextContent).Text; ran || !strings.Contains(msg, `did you mean "connection"?`) {
		t.Fatalf("a misspelling was not matched by edit distance: %s", msg)
	}

	res = call(map[string]any{"zzzzzzzzzz": 1, "text": "hi"})
	if msg := res.Content[0].(mcp.TextContent).Text; ran || strings.Contains(msg, "did you mean") {
		t.Fatalf("an unrelated name got a suggestion: %s", msg)
	}

	if res = call(map[string]any{"connection": "relay", "text": "hi"}); !ran || res.IsError {
		t.Fatal("a call with only known parameters did not run")
	}
}
