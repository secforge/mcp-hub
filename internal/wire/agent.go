package wire

import (
	"encoding/json"
	"fmt"
)

// An agent connection is one socket carrying many conversations, each
// shared with the agent from the server's UI and changed at runtime.
// Every frame about a conversation travels wrapped in a Conv frame naming
// it; the wrapped frame is exactly the protocol a single-conversation
// connection speaks. Frames that are not wrapped are about the agent
// connection itself. The design is chat-relay's
// docs/superpowers/specs/2026-10-19-agent-connection-design.md, agreed
// between the two projects.

// An agent endpoint refuses a client whose Hub-Protocol-Version is below
// 5, the version that introduced agent connections.

const (
	TypeAgentJoined   Type = "agentJoined"
	TypeConv          Type = "conv"
	TypeAssignments   Type = "assignments"
	TypeAssigned      Type = "assigned"
	TypeChanged       Type = "changed"
	TypeRevoked       Type = "revoked"
	TypeClosed        Type = "closed"
	TypeActivity      Type = "activity"
	TypeOpen          Type = "open"
	TypeClose         Type = "close"
	TypeSearch        Type = "search"
	TypeSearchResults Type = "searchResults"
)

// Access values of a grant.
const (
	AccessRead      = "read"
	AccessReadWrite = "readwrite"
)

// FrameType reads a frame's type without decoding the rest of it. Empty
// for anything that is not a JSON object with a string "type".
func FrameType(raw []byte) Type {
	var head struct {
		Type Type `json:"type"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return ""
	}
	return head.Type
}

// AgentJoined is the agent connection's handshake, the first frame on the
// socket.
type AgentJoined struct {
	Type          Type                       `json:"type,case:strict"`
	AgentID       OpaqueID                   `json:"agentId,case:strict"`
	Name          string                     `json:"name,omitempty,case:strict"`
	ServerVersion int                        `json:"serverVersion,omitempty,case:strict"`
	Features      map[string]json.RawMessage `json:"features,omitempty,case:strict"`
}

// OpaqueID is an identifier the server issues and the client only
// repeats. It may arrive as a JSON string or number; either way it is
// kept as the text the server sent.
type OpaqueID string

func (o *OpaqueID) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		*o = OpaqueID(s)
		return nil
	}
	var n json.Number
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("an id must be a string or a number, got %s", b)
	}
	*o = OpaqueID(n.String())
	return nil
}

// Assignment is one conversation shared with the agent.
type Assignment struct {
	Conv    string `json:"conv,case:strict"`
	Title   string `json:"title,omitempty,case:strict"`
	Kind    string `json:"kind,omitempty,case:strict"`
	Channel string `json:"channel,omitempty,case:strict"`
	Access  string `json:"access,case:strict"`
	// HistoryFrom is where visible history starts; nil means all of it.
	HistoryFrom *string `json:"historyFrom,omitempty,case:strict"`
	Behind      *int    `json:"behind,omitempty,case:strict"`
}

// Writable reports whether the grant allows writing.
func (a Assignment) Writable() bool { return a.Access == AccessReadWrite }

// Assignments is the full list of grants, sent right after AgentJoined.
type Assignments struct {
	Type  Type         `json:"type,case:strict"`
	Items []Assignment `json:"items,case:strict"`
}

// AssignmentChange is "assigned" (a new grant) or "changed" (a grant's
// access or history window changed; its session is closed and reopened).
type AssignmentChange struct {
	Type Type       `json:"type,case:strict"`
	Item Assignment `json:"item,case:strict"`
}

// Revoked says a grant is gone.
type Revoked struct {
	Type   Type   `json:"type,case:strict"`
	Conv   string `json:"conv,case:strict"`
	Reason string `json:"reason,omitempty,case:strict"`
}

// Closed says a conversation's session ended, with the close code a
// single-conversation socket would have ended with. Reopen accompanies a
// "changed": the grant stands and the session is to be opened again.
type Closed struct {
	Type   Type   `json:"type,case:strict"`
	Conv   string `json:"conv,case:strict"`
	Code   int    `json:"code,omitempty,case:strict"`
	Reason string `json:"reason,omitempty,case:strict"`
	Reopen bool   `json:"reopen,omitempty,case:strict"`
}

// Activity says a message arrived in a conversation that is not open.
type Activity struct {
	Type   Type   `json:"type,case:strict"`
	Conv   string `json:"conv,case:strict"`
	Behind int    `json:"behind,omitempty,case:strict"`
}

// ConvFrame wraps one conversation's frame.
type ConvFrame struct {
	Type  Type            `json:"type,case:strict"`
	Conv  string          `json:"conv,case:strict"`
	Frame json.RawMessage `json:"frame,case:strict"`
}

// OpenConv asks the server to start a conversation's session; the answer
// is that conversation's wrapped "joined".
type OpenConv struct {
	Type Type   `json:"type,case:strict"`
	Conv string `json:"conv,case:strict"`
}

// CloseConv stops a conversation's live delivery; the grant stays.
type CloseConv struct {
	Type Type   `json:"type,case:strict"`
	Conv string `json:"conv,case:strict"`
}

// WrapConv wraps an encoded inner frame for conv, stamping conv inside it
// too. The server refuses a write whose inner conv is missing or differs
// from the wrapper's, which catches a frame built for one conversation and
// wrapped for another.
func WrapConv(conv string, inner []byte) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(inner, &fields); err != nil {
		return nil, fmt.Errorf("inner frame is not a JSON object: %w", err)
	}
	stamp, err := json.Marshal(conv)
	if err != nil {
		return nil, err
	}
	fields["conv"] = stamp
	stamped, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	return json.Marshal(ConvFrame{Type: TypeConv, Conv: conv, Frame: stamped})
}

// Search looks for text across every conversation the agent holds live,
// open or not, each within its share's rules. It is answered unwrapped,
// with SearchResults or an error, either carrying ID. Declared as
// agentJoined.features.search = SearchFeature.
type Search struct {
	Type   Type     `json:"type,case:strict"`
	ID     string   `json:"id,omitempty,case:strict"`
	Query  string   `json:"query,case:strict"`
	Sender string   `json:"sender,omitempty,case:strict"`
	Conv   []string `json:"conv,omitempty,case:strict"`
	// Before is the position a previous answer returned, for the next
	// page.
	Before string `json:"before,omitempty,case:strict"`
	Limit  int    `json:"limit,omitempty,case:strict"`
}

// SearchHit is one matching message. Cursor is that conversation's own
// messageAfter cursor; Sender is a display name.
type SearchHit struct {
	Conv       string `json:"conv,case:strict"`
	ExternalID string `json:"externalId,omitempty,case:strict"`
	Cursor     string `json:"cursor,case:strict"`
	TS         string `json:"ts,omitempty,case:strict"`
	Sender     string `json:"sender,omitempty,case:strict"`
	Snippet    string `json:"snippet,omitempty,case:strict"`
}

// SearchResults answers a Search, newest first. Before is present only
// when more results exist.
type SearchResults struct {
	Type   Type        `json:"type,case:strict"`
	ID     string      `json:"id,omitempty,case:strict"`
	Items  []SearchHit `json:"items,case:strict"`
	Before string      `json:"before,omitempty,case:strict"`
}

// SearchFeature is agentJoined.features.search.
type SearchFeature struct {
	MaxLimit int `json:"maxLimit,omitempty,case:strict"`
}
