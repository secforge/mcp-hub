package mcptools

import (
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/wire"
)

// listFilter narrows hub_list_connections. Every field is optional and
// they combine: a connection is listed when it matches all that are set.
type listFilter struct {
	query  string
	kind   string
	access string
	unread bool
	limit  int
}

func listFilterFrom(req mcp.CallToolRequest) (listFilter, error) {
	f := listFilter{
		query:  strings.ToLower(strings.TrimSpace(req.GetString("query", ""))),
		kind:   strings.ToLower(strings.TrimSpace(req.GetString("kind", ""))),
		access: strings.ToLower(strings.TrimSpace(req.GetString("access", ""))),
		unread: req.GetBool("unread", false),
		limit:  req.GetInt("limit", 0),
	}
	if f.access != "" && f.access != wire.AccessRead && f.access != wire.AccessReadWrite {
		return f, fmt.Errorf("access is %q or %q, not %q", wire.AccessRead, wire.AccessReadWrite, f.access)
	}
	if f.limit < 0 {
		return f, fmt.Errorf("limit cannot be negative")
	}
	return f, nil
}

func (f listFilter) active() bool {
	return f.query != "" || f.kind != "" || f.access != "" || f.unread || f.limit > 0
}

func (f listFilter) matchTitle(title string) bool {
	return f.query == "" || strings.Contains(strings.ToLower(title), f.query)
}

// matchSession reports whether s matches. title, kind and access are what
// an agent's grant says about the conversation; for an ordinary
// connection they are empty and taken from the connection itself.
func (f listFilter) matchSession(s *session, conn *hubconn.Conn, title, kind, access string) bool {
	if conn != nil {
		if title == "" && conn.Topic() != nil {
			title = *conn.Topic()
		}
		if kind == "" {
			kind = conn.ConversationKind()
		}
		if access == "" {
			access = conn.Access()
		}
	}
	if access == "" {
		access = wire.AccessReadWrite
	}
	if f.query != "" && !strings.Contains(strings.ToLower(s.name), f.query) &&
		!strings.Contains(strings.ToLower(title), f.query) {
		return false
	}
	if f.kind != "" && !strings.Contains(strings.ToLower(kind), f.kind) {
		return false
	}
	if f.access != "" && access != f.access {
		return false
	}
	if f.unread && (conn == nil || conn.Behind() <= 0) {
		return false
	}
	return true
}
