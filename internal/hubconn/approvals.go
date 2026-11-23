package hubconn

import (
	"encoding/json"
	"fmt"

	"github.com/secforge/mcp-hub/internal/wire"
)

// ApprovalFrame is what an approval frame carries.
type ApprovalFrame struct {
	RequestID  string
	Envelope   string
	AnsweredAt string
	State      string
	OK         bool
	OKStated   bool
}

// ApprovalHandler processes an approvalAnswer or approvalClosed as it
// arrives, before it is buffered: it verifies and records the outcome,
// acks an answer, and returns the notice the reader is shown. An empty
// notice drops the event. Set once, by whoever holds the approval state.
var ApprovalHandler func(c *Conn, ev Event) string

func decodeApproval(t wire.Type, raw []byte) (Event, bool) {
	switch t {
	case wire.TypeApprovalRequestAck:
		var a wire.ApprovalRequestAck
		if json.Unmarshal(raw, &a) != nil {
			return Event{}, false
		}
		return Event{Kind: "approvalRequestAck", CorrelationID: a.ID, ActionOK: a.OK,
			ActionOKStated: statesOK(raw),
			Approval: &ApprovalFrame{RequestID: a.RequestID, State: a.State, OK: a.OK}}, true
	case wire.TypeApprovalWithdrawAck:
		var a wire.ApprovalWithdrawAck
		if json.Unmarshal(raw, &a) != nil {
			return Event{}, false
		}
		return Event{Kind: "approvalWithdrawAck", CorrelationID: a.ID, ActionOK: a.OK,
			ActionOKStated: statesOK(raw), Approval: &ApprovalFrame{State: a.State, OK: a.OK}}, true
	case wire.TypeApprovalAnswer:
		var a wire.ApprovalAnswer
		if json.Unmarshal(raw, &a) != nil || a.RequestID == "" {
			return Event{}, false
		}
		return Event{Kind: "approvalAnswer",
			Approval: &ApprovalFrame{RequestID: a.RequestID, Envelope: a.Envelope, AnsweredAt: a.AnsweredAt}}, true
	case wire.TypeApprovalClosed:
		var a wire.ApprovalClosed
		if json.Unmarshal(raw, &a) != nil || a.RequestID == "" {
			return Event{}, false
		}
		return Event{Kind: "approvalClosed", Approval: &ApprovalFrame{RequestID: a.RequestID, State: a.State}}, true
	}
	return Event{}, false
}

// handleApproval runs ApprovalHandler on an arriving answer or close and
// reports whether the event is kept. Without a handler an answer is left
// unacked, so the server delivers it again to a client that can read it.
func (c *Conn) handleApproval(ev *Event) bool {
	h := ApprovalHandler
	if h == nil {
		return false
	}
	ev.Text = h(c, *ev)
	return ev.Text != ""
}

// ApprovalsLimits is what the server declares about approvals here, and
// whether this connection may ask for them at all.
func (c *Conn) ApprovalsLimits() (wire.ApprovalsFeature, bool) {
	raw, ok := c.features[wire.FeatureApprovals]
	if !ok {
		return wire.ApprovalsFeature{}, false
	}
	var f wire.ApprovalsFeature
	_ = json.Unmarshal(raw, &f)
	return f, true
}

// ApprovalRequest sends a request and waits for the answer: an
// "approvalRequestAck", or an "error" carrying the server's code.
func (c *Conn) ApprovalRequest(requestID string, approvers []string, envelope, expiresAt string) (Event, bool, error) {
	return c.awaitAnswer("approvalRequestAck", func(id string) any {
		return wire.ApprovalRequest{Type: wire.TypeApprovalRequest, ID: id, RequestID: requestID,
			Approvers: approvers, Envelope: envelope, ExpiresAt: expiresAt}
	})
}

// ApprovalWithdraw takes back a request and waits for the answer: an
// "approvalWithdrawAck", or an "error".
func (c *Conn) ApprovalWithdraw(requestID string) (Event, bool, error) {
	return c.awaitAnswer("approvalWithdrawAck", func(id string) any {
		return wire.ApprovalWithdraw{Type: wire.TypeApprovalWithdraw, ID: id, RequestID: requestID}
	})
}

// ApprovalAnswerAck stops the server redelivering an answer. Send it only
// once the outcome is durably recorded.
func (c *Conn) ApprovalAnswerAck(requestID string) error {
	return c.writeJSON(wire.ApprovalAnswerAck{Type: wire.TypeApprovalAnswerAck, RequestID: requestID})
}

func formatApprovalEvent(e Event) (string, bool) {
	switch e.Kind {
	case "approvalAnswer", "approvalClosed":
		return e.Text, true
	case "approvalRequestAck", "approvalWithdrawAck":
		id := ""
		if e.Approval != nil && e.Approval.RequestID != "" {
			id = " " + e.Approval.RequestID
		}
		return fmt.Sprintf("[hub: a late answer arrived to an approval %s%s: ok=%t — hub_approvals() "+
			"shows where it stands]", map[string]string{"approvalRequestAck": "request",
			"approvalWithdrawAck": "withdraw"}[e.Kind], id, e.ActionOK), true
	}
	return "", false
}
