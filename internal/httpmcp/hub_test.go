package httpmcp

import (
	"context"
	"testing"

	"github.com/secforge/mcp-hub/internal/hubsession"
)

func TestConnectWithoutSessionIDCreatesOne(t *testing.T) {
	s := NewServer(hubsession.NewManager())

	peerID, sessionID, watchToken, err := s.connect("mcp-session-1", "", "Alice", "", "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if peerID == "" || sessionID == "" || watchToken == "" {
		t.Fatalf("expected non-empty peerID/sessionID/watchToken, got %q/%q/%q", peerID, sessionID, watchToken)
	}
}

func TestConnectWithExplicitSessionIDJoinsIt(t *testing.T) {
	s := NewServer(hubsession.NewManager())

	_, sessionID, _, err := s.connect("mcp-session-1", "", "Alice", "", "")
	if err != nil {
		t.Fatalf("connect (creator): %v", err)
	}

	peerID2, sessionID2, _, err := s.connect("mcp-session-2", sessionID, "Bob", "", "")
	if err != nil {
		t.Fatalf("connect (joiner): %v", err)
	}
	if sessionID2 != sessionID {
		t.Fatalf("expected joiner to join the same session %q, got %q", sessionID, sessionID2)
	}

	hub := s.hubFor("mcp-session-1")
	peers := hub.session().Peers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers in the shared session, got %d", len(peers))
	}
	found := false
	for _, p := range peers {
		if p.ID() == peerID2 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected the shared session's roster to include the joiner")
	}
}

func TestConnectRejectsInvalidSessionID(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	if _, _, _, err := s.connect("mcp-session-1", "not-a-valid-uuid", "", "", ""); err == nil {
		t.Fatal("expected an error for an invalid sessionId")
	}
}

func TestConnectRejectsInvalidAgePublicKey(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	if _, _, _, err := s.connect("mcp-session-1", "", "", "not-a-valid-key", ""); err == nil {
		t.Fatal("expected an error for an invalid agePublicKey")
	}
}

func TestConnectTwiceOnSameMCPSessionErrors(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	if _, _, _, err := s.connect("mcp-session-1", "", "", "", ""); err != nil {
		t.Fatalf("first connect: %v", err)
	}
	if _, _, _, err := s.connect("mcp-session-1", "", "", "", ""); err == nil {
		t.Fatal("expected an error connecting twice without disconnecting first")
	}
}

func TestDisconnectClearsPeerAndRemovesEmptySession(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	_, sessionID, _, err := s.connect("mcp-session-1", "", "", "", "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	s.disconnect("mcp-session-1")

	if hub := s.hubFor("mcp-session-1"); hub.peer() != nil {
		t.Fatal("expected peer to be cleared after disconnect")
	}
	// Reconnecting to the same sessionId should find it empty (a fresh
	// session), proving the old one was torn down rather than left with a
	// phantom member.
	if _, _, _, err := s.connect("mcp-session-1", sessionID, "", "", ""); err != nil {
		t.Fatalf("reconnect after disconnect: %v", err)
	}
	if peers := s.hubFor("mcp-session-1").session().Peers(); len(peers) != 1 {
		t.Fatalf("expected exactly the reconnecting peer, got %d peers", len(peers))
	}
}

func TestDisconnectWithoutConnectIsANoop(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	s.disconnect("mcp-session-1") // must not panic
}

func TestHooksUnregisterSessionDisconnectsThePeer(t *testing.T) {
	s := NewServer(hubsession.NewManager())
	_, _, _, err := s.connect("mcp-session-1", "", "", "", "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	hooks := s.Hooks()
	for _, hook := range hooks.OnUnregisterSession {
		hook(context.Background(), &fakeSession{id: "mcp-session-1"})
	}

	if s.hubFor("mcp-session-1").peer() != nil {
		t.Fatal("expected the peer to be cleared once the MCP session unregisters")
	}
}
