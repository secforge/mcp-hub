package mcptools

import (
	"context"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type capSession struct{ caps mcp.ClientCapabilities }

func (s *capSession) Initialize()                                         {}
func (s *capSession) Initialized() bool                                   { return true }
func (s *capSession) NotificationChannel() chan<- mcp.JSONRPCNotification { return nil }
func (s *capSession) SessionID() string                                   { return "cap" }
func (s *capSession) GetClientInfo() mcp.Implementation                   { return mcp.Implementation{} }
func (s *capSession) SetClientInfo(mcp.Implementation)                    {}
func (s *capSession) GetClientCapabilities() mcp.ClientCapabilities       { return s.caps }
func (s *capSession) SetClientCapabilities(c mcp.ClientCapabilities)      { s.caps = c }

type plainSession struct{}

func (plainSession) Initialize()                                         {}
func (plainSession) Initialized() bool                                   { return true }
func (plainSession) NotificationChannel() chan<- mcp.JSONRPCNotification { return nil }
func (plainSession) SessionID() string                                   { return "plain" }

// A client that declared no roots capability is never asked for roots: it
// would not answer, and the connect waiting on it would never return.
func TestRootsAreAskedOnlyOfAClientThatDeclaresThem(t *testing.T) {
	srv := server.NewMCPServer("t", "1")
	var withRoots mcp.ClientCapabilities
	withRoots.Roots = &struct {
		ListChanged bool `json:"listChanged,omitempty"`
	}{}
	for _, tc := range []struct {
		name    string
		session server.ClientSession
		want    bool
	}{
		{"declares no roots", &capSession{}, false},
		{"declares roots", &capSession{caps: withRoots}, true},
		{"records nothing about the client", plainSession{}, true},
	} {
		ctx := srv.WithContext(context.Background(), tc.session)
		if got := clientDeclaresRoots(ctx); got != tc.want {
			t.Errorf("%s: clientDeclaresRoots = %v, want %v", tc.name, got, tc.want)
		}
	}
}
