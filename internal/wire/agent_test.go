package wire

import (
	"encoding/json"
	"testing"
)

func TestAnAgentIDMayBeAStringOrANumber(t *testing.T) {
	for raw, want := range map[string]OpaqueID{
		`{"type":"agentJoined","agentId":"a-1"}`: "a-1",
		`{"type":"agentJoined","agentId":42}`:    "42",
	} {
		var j AgentJoined
		if err := json.Unmarshal([]byte(raw), &j); err != nil || j.AgentID != want {
			t.Errorf("%s: got %q, %v", raw, j.AgentID, err)
		}
	}
	var j AgentJoined
	if err := json.Unmarshal([]byte(`{"type":"agentJoined","agentId":{}}`), &j); err == nil {
		t.Error("an object was accepted as an id")
	}
}
