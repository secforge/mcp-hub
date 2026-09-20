package mcptools

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/wire"
)

// A gap retrieval hands over messages from BEHIND the reading position, so
// it must not move the wire-level read receipt.
//
// The rationale this test was first written with was wrong and is recorded
// because the correction is the point: chat-relay's author said on
// 2026-09-20 that their acked position was last-write-wins, so a regressed
// receipt would move that server's view of this peer backwards. Reading
// their code later the same day, they found it monotonic — a backwards
// standalone ack is refused, a backwards piggyback ignored. So the
// consequence this test was named for does not occur against that server.
//
// The behaviour is still required, and against a monotonic server the cost
// of getting it wrong is merely different, not absent: the client would
// send a receipt that is silently dropped, believe a position acknowledged
// that never was, and no wire signal would ever say otherwise. hub_read
// already keeps the receipt out; the gap walk did not.
func TestGapRetrievalDoesNotMoveTheWireReceipt(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000",
			ServerVersion: wire.ProtocolVersion, Features: teamsTestFeatures(), ConversationKind: "group"}
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		for {
			var m wire.MessageAfter
			if err := conn.ReadJSON(&m); err != nil {
				return
			}
			if m.At == "2026-09-01T09:00:00Z" {
				conn.WriteJSON(wire.Msg{
					Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
					Text: "inside the gap", TS: "2026-09-01T09:05:00Z", Historical: true,
					Cursor: "cursor-inside-gap", ExternalID: "ext-gap",
					Answers: &wire.Anchor{At: "2026-09-01T09:00:00Z"},
				})
				continue
			}
			conn.WriteJSON(wire.NewNoMoreMessages(wire.Anchor{At: m.At, Cursor: m.Cursor}))
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc-" + uniqueConvID() + "#gap-receipt-secret"
	ctx := context.Background()

	id := targetForLink(ctx, link)
	clearCatchUpGap(id)
	setCatchUpGapFromAt(id, "2026-09-01T09:00:00Z", "2026-09-01T10:00:00Z")

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"as": testConn, "link": link}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, connReqFor(testConn))

	s, err := hub.session(testConn)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	conn, _ := s.activeConn()

	gapReq := mcp.CallToolRequest{}
	gapReq.Params.Arguments = map[string]any{"connection": testConn, "gap": true}
	if res, err := hub.handleCatchUp(ctx, gapReq); err != nil || res.IsError {
		t.Fatalf("hub_catch_up(gap: true) failed: err=%v result=%+v", err, res)
	}

	if got := conn.ConsumedCursor(); got == "cursor-inside-gap" {
		t.Fatalf("a gap retrieval moved the wire receipt to a cursor inside the gap (%q) — the "+
			"next ack would tell the server this peer is further behind than it is", got)
	}
}

// A push-mode backlog walk that the server REFUSES — bad_anchor for a
// stored cursor that no longer resolves is the live case — must report the
// refusal. The error frame carries no cursor, so it used to fall into the
// "a message arrived with no cursor" branch and every retry said the same
// thing while the real reason was never shown.
func TestPushCatchUpReportsAServerRefusalRatherThanACursorlessMessage(t *testing.T) {
	inPushMode(t)

	upgrader := websocket.Upgrader{}
	var mu sync.Mutex
	asked := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000",
			ServerVersion: wire.ProtocolVersion, Features: teamsTestFeatures(), ConversationKind: "group"}
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		for {
			var m wire.MessageAfter
			if err := conn.ReadJSON(&m); err != nil {
				return
			}
			mu.Lock()
			asked++
			mu.Unlock()
			conn.WriteJSON(wire.Error{Type: wire.TypeError, Code: "bad_anchor",
				Message: "that cursor no longer resolves", Retryable: false})
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc-" + uniqueConvID() + "#push-refusal-secret"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"as": testConn, "link": link}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, connReqFor(testConn))

	s, err := hub.session(testConn)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	conn, _ := s.activeConn()

	// The walk is driven directly: the push target is unreachable in a
	// test, and what is under test is which ending the run records.
	res := s.walkCatchUpPush(conn, connstore.Target{}, wire.Anchor{Cursor: "stale-cursor"},
		catchUpWant{Messages: 1})
	if res.Err == nil {
		t.Fatalf("a refused walk was not reported as an error: %+v", res)
	}
	if !strings.Contains(res.Err.Error(), "bad_anchor") {
		t.Fatalf("the refusal's own code is not in what the reader is told: %v", res.Err)
	}
	if strings.Contains(res.StoppedBy, "no cursor") {
		t.Fatalf("a refusal was reported as a cursorless message: %+v", res)
	}
}

// A server declares attachments.maxRawBytes and attachments.imagesOnly so
// a client can act on them BEFORE encoding a file. The helper that
// decodes that shape had no caller on this side at all, so a .pdf to an
// images-only server was read, base64-encoded and pushed in full, and the
// refusal arrived after the transfer — as an asynchronous error the
// caller then has to attribute.
//
// Both directions are asserted. A client that refuses everything passes
// the first half alone; a client that refuses nothing passes the second.
func TestTheServersDeclaredAttachmentLimitsAreConsultedBeforeEncoding(t *testing.T) {
	for _, tc := range []struct {
		name     string
		feature  string
		file     string
		content  []byte
		wantErr  string
		wantPass bool
	}{
		{
			name:    "an images-only server refuses a pdf locally",
			feature: `{"imagesOnly":true}`,
			file:    "doc.pdf", content: []byte("%PDF-1.4 not an image"),
			wantErr: "images only",
		},
		{
			name:    "and still takes an image",
			feature: `{"imagesOnly":true}`,
			file:    "pic.png", content: []byte("\x89PNG\r\n\x1a\n"),
			wantPass: true,
		},
		{
			name:    "a declared byte cap is applied with the server's own number",
			feature: `{"maxRawBytes":16}`,
			file:    "big.png", content: []byte("\x89PNG\r\n\x1a\naaaaaaaaaaaaaaaaaaaaaaaa"),
			wantErr: "at most 16 bytes",
		},
		{
			name:    "a server that declares nothing refuses nothing locally",
			feature: `{}`,
			file:    "doc.pdf", content: []byte("%PDF-1.4 not an image"),
			wantPass: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tc.file)
			if err := os.WriteFile(path, tc.content, 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}

			features := teamsTestFeatures()
			features["attachments"] = json.RawMessage(tc.feature)
			link := startRelayTestServerWithJoined(t, wire.Joined{
				Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000",
				ServerVersion: wire.ProtocolVersion, Features: features,
				ConversationKind: "group", CanSend: true,
			})

			ctx := context.Background()
			hub := NewHub()
			connReq := mcp.CallToolRequest{}
			connReq.Params.Arguments = map[string]any{"as": testConn, "link": link}
			if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
				t.Fatalf("connect: err=%v result=%+v", err, res)
			}
			defer hub.handleDisconnect(ctx, connReqFor(testConn))
			s, err := hub.session(testConn)
			if err != nil {
				t.Fatalf("session: %v", err)
			}
			conn, _ := s.activeConn()

			req := mcp.CallToolRequest{}
			req.Params.Arguments = map[string]any{"connection": testConn, "filePath": path}
			_, err = readAttachmentParam(req, conn)

			if tc.wantPass {
				if err != nil {
					t.Fatalf("refused an attachment the server never said it would reject: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("the server declared %s and this attachment was accepted for encoding "+
					"anyway — the whole file would be transferred and then refused", tc.feature)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("refusal does not state the server's own limit: %v", err)
			}
		})
	}
}

// MCP_HUB_PROJECT_DIR outranks the MCP client's advertised roots.
//
// It did not, and the arrangement that broke is the one no test here
// could reach: roots was consulted first, a client that answers roots
// always answers, and the fallback that reads the variable was therefore
// unreachable in an MCP session. Setting it did nothing in the only place
// anyone would set it, and the store recorded connections under the
// client's root instead. Observed live: a process with
// MCP_HUB_PROJECT_DIR=/source/mcp-hub/x+test wrote under
// /source/mcp-hub/x.
//
// The ordering is asserted here rather than the outcome alone: a winner
// that is merely returned first is indistinguishable from one that won
// after asking, and asking is what made the variable useless.
func TestAnExplicitProjectOverrideOutranksTheClientsRoots(t *testing.T) {
	for _, tc := range []struct {
		name      string
		override  string
		root      string
		cwd       string
		want      string
		wantAsked bool
	}{
		{
			name:     "the override wins and roots is never consulted",
			override: "/source/mcp-hub/x+test", root: "/source/mcp-hub/x", cwd: "/source/mcp-hub/x",
			want: "/source/mcp-hub/x+test", wantAsked: false,
		},
		{
			name: "without it, roots still beats the working directory",
			root: "/source/mcp-hub/x", cwd: "/somewhere/else",
			want: "/source/mcp-hub/x", wantAsked: true,
		},
		{
			name: "and the working directory is the last resort",
			cwd:  "/somewhere/else",
			want: "/somewhere/else", wantAsked: true,
		},
		{
			name: "everything absent degrades to the unnamed scope, not a crash",
			want: "", wantAsked: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			asked := false
			got := resolveProject(tc.override,
				func() string { asked = true; return tc.root },
				func() string { return tc.cwd })
			if got != tc.want {
				t.Errorf("resolved %q, want %q", got, tc.want)
			}
			if asked != tc.wantAsked {
				if asked {
					t.Errorf("the client was asked for its roots although an explicit override was " +
						"set — an override that is consulted after an inference is an override that " +
						"does nothing wherever the inference succeeds")
				} else {
					t.Errorf("the client was never asked for its roots")
				}
			}
		})
	}
}

// And the override reaches that decision from the environment at all.
func TestTheProjectOverrideIsReadFromTheEnvironment(t *testing.T) {
	t.Setenv("MCP_HUB_PROJECT_DIR", "/source/mcp-hub/x+test")
	if got := connstore.ProjectOverride(); got != "/source/mcp-hub/x+test" {
		t.Fatalf("ProjectOverride() = %q, want the value of MCP_HUB_PROJECT_DIR", got)
	}
	t.Setenv("MCP_HUB_PROJECT_DIR", "")
	if got := connstore.ProjectOverride(); got != "" {
		t.Fatalf("ProjectOverride() = %q with the variable unset, want empty", got)
	}
}
