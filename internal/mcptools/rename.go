package mcptools

import (
	"context"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/connstore"
)

func renameTool() mcp.Tool {
	return mcp.NewTool("hub_rename",
		mcp.WithDescription("Give an OPEN connection a new local name — the name every tool's "+
			"`connection` argument takes. It reconnects under the new name with the SAME identity, "+
			"key file and read position, so nothing is lost and other peers see only a reconnect. "+
			"For an agent connection, its conversations get names under the new one, except those "+
			"renamed one by one; a single conversation of an agent connection can be renamed too. "+
			"Use it when "+
			"the user or an operator asks for a different name; it does not change your display "+
			"name or the conversation's title"),
		mcp.WithString("connection", mcp.Required(), mcp.Description(
			"The connection's current name, as hub_list_connections shows it")),
		mcp.WithString("as", mcp.Required(), mcp.Description(
			"The new name: lowercase letters, digits, - and _, up to 32 characters (16 for an "+
				"agent connection)")),
	)
}

func (h *Hub) handleRename(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	from, err := req.RequireString("connection")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	to, err := req.RequireString("as")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if from == to {
		return mcp.NewToolResultText(fmt.Sprintf("%q already has that name; nothing changed.", from)), nil
	}
	if err := validSessionName(to); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	var link string
	var target connstore.Target
	var ownEchoes, agent bool
	h.mu.Lock()
	ag := h.agents[from]
	_, toSession := h.sessions[to]
	_, toAgent := h.agents[to]
	h.mu.Unlock()
	if toSession || toAgent {
		return mcp.NewToolResultError(fmt.Sprintf("a connection named %q is already open; "+
			"nothing changed", to)), nil
	}
	if ag != nil {
		if len(to) > 16 {
			return mcp.NewToolResultError("an agent connection's name is at most 16 characters: " +
				"its conversations are named after it; nothing changed"), nil
		}
		link, target, ownEchoes, agent = ag.link, ag.target, ag.ownEchoes, true
	} else {
		s, err := h.session(from)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		if s.agent != nil {
			if err := s.agent.rename(s, to); err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("renaming %q to %q: %v", from, to, err)), nil
			}
			return mcp.NewToolResultText(fmt.Sprintf("Renamed: the conversation %q of the agent "+
				"connection %q is now %q — same identity and read position, and the name is kept "+
				"when it reopens. Use connection: %q from now on. If anything arrived during the "+
				"rename, hub_catch_up(connection: %q) reads it.", from, s.agent.name, to, to, to)), nil
		}
		s.mu.Lock()
		target = s.redialTarget
		if target == (connstore.Target{}) {
			target = s.connTarget
		}
		link, ownEchoes = target.Link, s.ownEchoes
		s.mu.Unlock()
	}
	if link == "" {
		return mcp.NewToolResultError(fmt.Sprintf("%q has no link on record to reconnect with; "+
			"nothing changed", from)), nil
	}

	off := mcp.CallToolRequest{}
	off.Params.Arguments = map[string]any{"connection": from}
	if res, err := h.handleDisconnect(ctx, off); err != nil {
		return nil, err
	} else if res.IsError {
		return mcp.NewToolResultError(fmt.Sprintf("could not close %q, so nothing was renamed: %s",
			from, textOfResult(res))), nil
	}
	on := mcp.CallToolRequest{}
	args := map[string]any{"as": to, "link": link, "ownEchoes": ownEchoes}
	if target.Project != "" && connstore.NormalizeProject(target.Project) != connstore.CurrentProject() {
		args["projectDir"] = target.Project
	}
	on.Params.Arguments = args
	res, err := h.handleConnect(ctx, on)
	if err != nil {
		return nil, err
	}
	if res.IsError {
		return mcp.NewToolResultError(fmt.Sprintf("%q was closed, but reconnecting it as %q failed: "+
			"%s\nIts identity and read position are still stored: hub_connect with its link (see "+
			"hub_list_connections) and as=%q tries again.", from, to, textOfResult(res), to)), nil
	}
	kind := "connection"
	if agent {
		kind = "agent connection (its conversations are now named " + to + "-…)"
	}
	head := fmt.Sprintf("Renamed: the %s %q is now %q — same identity, key file and read position. "+
		"Use connection: %q from now on.\n\n", kind, from, to, to)
	return mcp.NewToolResultText(head + strings.TrimSpace(textOfResult(res))), nil
}
