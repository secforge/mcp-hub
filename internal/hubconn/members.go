package hubconn

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/secforge/mcp-hub/internal/wire"
)

// MemberHistory says how much of a conversation added members see: from
// now on (None), everything (All), or from From on.
type MemberHistory struct {
	None bool
	All  bool
	From time.Time
}

func (h MemberHistory) raw() (json.RawMessage, error) {
	switch {
	case h.None && !h.All && h.From.IsZero():
		return json.RawMessage(`"none"`), nil
	case h.All && !h.None && h.From.IsZero():
		return json.RawMessage(`"all"`), nil
	case !h.None && !h.All && !h.From.IsZero():
		return json.Marshal(map[string]string{"from": h.From.Format(time.RFC3339Nano)})
	}
	return nil, fmt.Errorf("history must be exactly one of none, all, or a moment")
}

// CanAddMembers reports whether the server declares, right now, that it
// would add members here. It is a hint: the answer to a request decides.
func (c *Conn) CanAddMembers() bool { return c.HasFeature(wire.FeatureAddMembers) }

// AddMembers adds contacts to the conversation and waits for the answer:
// an "addMembersAck", or an "error" carrying the server's code (and, on
// add_failed, what was and was not added).
func (c *Conn) AddMembers(contacts []string, history MemberHistory) (Event, bool, error) {
	if len(contacts) == 0 || len(contacts) > wire.MaxGroupContacts {
		return Event{}, false, fmt.Errorf("between 1 and %d contacts may be added at once, got %d",
			wire.MaxGroupContacts, len(contacts))
	}
	h, err := history.raw()
	if err != nil {
		return Event{}, false, err
	}
	return c.awaitAnswer("addMembersAck", func(id string) any {
		return wire.AddMembers{Type: wire.TypeAddMembers, ID: id, Contacts: contacts, History: h}
	})
}
