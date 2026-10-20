package mcptools

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
)

func linkTool() mcp.Tool {
	return mcp.NewTool("hub_link",
		mcp.WithDescription("The link to one message in the platform it came from (Teams), to "+
			"open it there or hand it to the user. Messages do not show their link; ask for it "+
			"here by the externalId the message was delivered with. Known for messages this "+
			"connection received recently (live, by hub_catch_up or by hub_read); for an older "+
			"one, read it with hub_read first. Read-only"),
		connectionParam(),
		mcp.WithString("externalId", mcp.Required(), mcp.Description(
			"The message's externalId, copied from its header")),
	)
}

func (h *Hub) handleLink(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	s, conn, bad := h.forRequest(req)
	if bad != nil {
		return bad, nil
	}
	id, err := req.RequireString("externalId")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if !conn.OffersExternalLinks() {
		return mcp.NewToolResultError(fmt.Sprintf("%q gives no links to its messages: the server "+
			"does not offer them for this conversation.", s.name)), nil
	}
	link, known := conn.ExternalLink(id)
	switch {
	case !known:
		return mcp.NewToolResultError(fmt.Sprintf("no message with externalId %q was received on %q "+
			"recently, or since it last connected. Read it with hub_read, then ask again.",
			noticeText(id), s.name)), nil
	case link == "":
		return mcp.NewToolResultText(fmt.Sprintf("The message %q on %q has no link in its platform.",
			noticeText(id), s.name)), nil
	}
	return mcp.NewToolResultText(link), nil
}
