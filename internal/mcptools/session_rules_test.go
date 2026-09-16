package mcptools

import (
	"strings"
	"testing"
)

// The name is the address, so the rules around it are the contract. Each
// of these is a way the wrong conversation gets written to.

func TestAConnectionNameIsRequiredAndNarrow(t *testing.T) {
	h := NewHub()
	for _, name := range []string{"", "has space", "UPPER", "with.dot", strings.Repeat("x", 33)} {
		if _, err := h.open(name); err == nil {
			t.Fatalf("expected %q to be refused as a connection name", name)
		}
	}
	if _, err := h.open("chat-relay_2"); err != nil {
		t.Fatalf("expected a plain name to be accepted: %v", err)
	}
}

// Refused, never reattached: "connect me as X" when X exists means either
// "reconnect that one" or "a second connection whose name collided", and
// those want opposite handling.
func TestANameInUseIsRefused(t *testing.T) {
	h := NewHub()
	s, err := h.open("ops")
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	again, err := h.open("ops")
	if err == nil {
		t.Fatal("expected the second open under the same name to be refused")
	}
	if again != nil {
		t.Fatal("expected no session back from a refused open")
	}
	if !strings.Contains(err.Error(), "already open") {
		t.Fatalf("expected the refusal to say why, got: %v", err)
	}
	// And the first one is untouched — a refusal must not disturb what it
	// refused to replace.
	if got, err := h.session("ops"); err != nil || got != s {
		t.Fatalf("expected the original session to survive the refusal, got %v %v", got, err)
	}
}

// Giving one up frees the name, so the obvious next move works.
func TestClosingReleasesTheName(t *testing.T) {
	h := NewHub()
	if _, err := h.open("ops"); err != nil {
		t.Fatalf("open: %v", err)
	}
	h.close("ops")
	if _, err := h.session("ops"); err == nil {
		t.Fatal("expected the name to be gone after closing")
	}
	if _, err := h.open("ops"); err != nil {
		t.Fatalf("expected the name to be reusable after closing: %v", err)
	}
}

func TestTheCapIsEnforcedAndNamesWhatIsOpen(t *testing.T) {
	h := NewHub()
	for i := 0; i < maxSessions; i++ {
		if _, err := h.open(string(rune('a' + i))); err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
	}
	_, err := h.open("one-too-many")
	if err == nil {
		t.Fatalf("expected the %dth connection to be refused", maxSessions+1)
	}
	// The refusal has to say what is holding the slots, or it cannot be
	// acted on without another call.
	for _, want := range []string{"a", "h", "limit"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected the refusal to mention %q, got: %v", want, err)
		}
	}
}

// An unknown name is an error even when exactly one connection is open.
// Resolving it from "the only one" would work today and fail silently the
// first time a second connection exists.
func TestAnUnknownNameIsNeverResolvedToTheOnlyOpenOne(t *testing.T) {
	h := NewHub()
	if _, err := h.open("ops"); err != nil {
		t.Fatalf("open: %v", err)
	}
	got, err := h.session("chat")
	if err == nil {
		t.Fatalf("expected an unknown name to be refused, got %v", got)
	}
	if !strings.Contains(err.Error(), "ops") {
		t.Fatalf("expected the error to list what IS open, got: %v", err)
	}
}

func TestWithNothingOpenTheErrorSaysNotConnected(t *testing.T) {
	h := NewHub()
	_, err := h.session("ops")
	if err == nil {
		t.Fatal("expected an error with nothing open")
	}
	// "not connected" is the phrase every caller already keys on; a
	// different sentence for the same fact is a second thing to learn.
	if !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("expected the familiar wording, got: %v", err)
	}
}
