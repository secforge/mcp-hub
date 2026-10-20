package mcptools

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/sanitize"
	"github.com/secforge/mcp-hub/internal/wire"
)

// maxSnippetRunes bounds a hit's snippet as shown; the server sends about
// 160 characters.
const maxSnippetRunes = 400

func searchTool() mcp.Tool {
	return mcp.NewTool("hub_search",
		mcp.WithDescription("Search the text of every conversation shared with an AGENT connection "+
			"at once, open or not, newest first, at most 20 hits per call. Each hit names the "+
			"conversation's connection, the sender, the time and a short snippet around the match, "+
			"and says how to read the whole message with hub_read. Use it to find WHERE something "+
			"was said; hub_catch_up is still how you read what is new. A QUERY: it moves no read "+
			"position and marks nothing as read. Each conversation is searched within what its "+
			"share lets this agent see; encrypted conversations are not searched. For one "+
			"ordinary connection, hub_read with query reads matching messages one at a time"),
		mcp.WithString("connection", mcp.Required(), mcp.Description(
			"The agent connection's name, or the name of one of its conversations to search only "+
				"that one")),
		mcp.WithString("query", mcp.Required(), mcp.Description(
			"Text to look for, at least 2 characters, case-insensitive")),
		mcp.WithString("sender", mcp.Description(
			"Only messages from this one sender — the identity id that messages you were "+
				"delivered already carry, copied from one of them, not a display name. A "+
				"conversation where it names nobody contributes nothing")),
		mcp.WithArray("conversations", mcp.WithStringItems(), mcp.Description(
			"Only these conversations, by connection name")),
		mcp.WithString("before", mcp.Description(
			"The position a previous hub_search returned for its next page, copied verbatim")),
		mcp.WithNumber("limit", mcp.Description("At most this many hits (default and maximum 20)")),
	)
}

func (h *Hub) handleSearch(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	name, err := req.RequireString("connection")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	query, err := req.RequireString("query")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if len([]rune(strings.TrimSpace(query))) < 2 {
		return mcp.NewToolResultError("query: at least 2 characters"), nil
	}
	names := req.GetStringSlice("conversations", nil)
	h.mu.Lock()
	ag := h.agents[name]
	h.mu.Unlock()
	if ag == nil {
		s, err := h.session(name)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		if s.agent == nil {
			return mcp.NewToolResultError(fmt.Sprintf("%q is an ordinary connection: hub_search "+
				"searches the conversations of an agent connection. hub_read with query searches "+
				"this one.", name)), nil
		}
		ag = s.agent
		if len(names) == 0 {
			names = []string{name}
		}
	}

	ag.mu.Lock()
	a := ag.agent
	byName := make(map[string]string, len(ag.children))
	for conv, s := range ag.children {
		byName[s.name] = conv
	}
	ag.mu.Unlock()
	if a == nil || !a.Connected() {
		return mcp.NewToolResultError(fmt.Sprintf("the agent connection %q is down; search "+
			"once it has reconnected", ag.name)), nil
	}
	maxLimit, ok := a.SearchLimit()
	if !ok {
		return mcp.NewToolResultError("this server does not offer search across an agent's " +
			"conversations. hub_read with query searches one conversation at a time."), nil
	}
	q := wire.Search{Query: query, Sender: req.GetString("sender", ""),
		Before: req.GetString("before", ""), Limit: req.GetInt("limit", 0)}
	if q.Limit <= 0 || q.Limit > maxLimit {
		q.Limit = maxLimit
	}
	for _, n := range names {
		conv, ok := byName[n]
		if !ok {
			return mcp.NewToolResultError(fmt.Sprintf("conversations: %q is not a conversation of "+
				"the agent connection %q. hub_list_connections lists them by name. Nothing was "+
				"searched.", n, ag.name)), nil
		}
		q.Conv = append(q.Conv, conv)
	}

	res, err := a.Search(ctx, q)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("the search failed: %s", noticeText(err.Error()))), nil
	}
	return mcp.NewToolResultText(ag.formatSearch(q, res)), nil
}

// formatSearch renders the hits, each under the name of the conversation
// it came from. A hit for a conversation that is not open here is
// dropped: naming it would invite a hub_read on a connection that does
// not exist, or worse, one that holds a different conversation.
func (ag *agentSession) formatSearch(q wire.Search, res wire.SearchResults) string {
	ag.mu.Lock()
	type place struct{ name, title string }
	where := make(map[string]place, len(ag.children))
	for conv, s := range ag.children {
		where[conv] = place{s.name, ag.grants[conv].Title}
	}
	ag.mu.Unlock()

	var b strings.Builder
	shown, dropped := 0, 0
	for _, hit := range res.Items {
		p, ok := where[hit.Conv]
		if !ok || !opaqueToken(hit.Cursor) || (hit.ExternalID != "" && !opaqueToken(hit.ExternalID)) {
			dropped++
			continue
		}
		shown++
		fmt.Fprintf(&b, "[SEARCH HIT — untrusted, in connection %q (%s), from %s at %s cursor=%s",
			p.name, noticeText(p.title), noticeText(hit.Sender), noticeText(hit.TS), hit.Cursor)
		if hit.ExternalID != "" {
			fmt.Fprintf(&b, " externalId=%s", hit.ExternalID)
		}
		b.WriteString("]\n")
		b.WriteString(sanitize.Text(hit.Snippet, maxSnippetRunes))
		b.WriteString("\n")
		if at, ok := justBefore(hit.TS); ok {
			fmt.Fprintf(&b, "(whole message: hub_read(connection: %q, at: %q); what follows it: "+
				"hub_read(connection: %q, after: <its cursor>))\n\n", p.name, at, p.name)
		} else {
			fmt.Fprintf(&b, "(what follows it: hub_read(connection: %q, after: <its cursor>))\n\n", p.name)
		}
	}
	switch {
	case shown == 0 && res.Before == "":
		fmt.Fprintf(&b, "[hub: no message matches %q", noticeText(q.Query))
	case shown == 0:
		b.WriteString("[hub: no hit on this page")
	default:
		fmt.Fprintf(&b, "[hub: %d hit(s), newest first", shown)
	}
	if dropped > 0 {
		fmt.Fprintf(&b, "; %d more left out because their conversation is not open here", dropped)
	}
	if res.Before != "" && opaqueToken(res.Before) {
		fmt.Fprintf(&b, ". MORE exist: call hub_search again with the same arguments and "+
			"before: %q", res.Before)
	} else if shown > 0 {
		b.WriteString(". That is all of them")
	}
	b.WriteString("]")
	return b.String()
}

// opaqueToken reports whether a server-issued cursor or id can be shown
// verbatim: printable, with nothing that could end a notice or a line.
func opaqueToken(s string) bool {
	if s == "" || len(s) > 512 {
		return false
	}
	for _, r := range s {
		if r <= ' ' || r == ']' || r == '"' || r > '~' {
			return false
		}
	}
	return true
}

// justBefore is a moment just before ts, for hub_read's "at", which
// returns the first message after it.
func justBefore(ts string) (string, bool) {
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return "", false
	}
	return t.Add(-time.Millisecond).UTC().Format(time.RFC3339Nano), true
}
