package mcptools

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/approvals"
	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/hubconn"
)

func init() { hubconn.ApprovalHandler = approvalArrived }

// maxApprovalEnvelope bounds an answer envelope where the server states no
// limit, as the wire does.
const maxApprovalEnvelope = 64 * 1024

// approvalNow is the clock approvals read. A var so tests can move it.
var approvalNow = time.Now

func approvalProject() string { return connstore.CurrentProject() }

// approvalAskerName is how this client names itself in a request: the
// project folder's name, the same default as a display name.
func approvalAskerName() string {
	if n := defaultDisplayName(approvalProject()); n != "" {
		return n
	}
	return "mcp-hub"
}

func approvalRequestTool() mcp.Tool {
	return mcp.NewTool("hub_approval_request",
		mcp.WithDescription("Ask a PERSON for a signed approval before doing something that needs one. "+
			"The question goes, encrypted, to the approver's paired phones; the person answers there, "+
			"and the answer is signed with a key that exists only on the phone. This client verifies "+
			"it against the approver's key file on this machine — no server, peer or message can "+
			"forge it.\n"+
			"ASYNCHRONOUS: this returns as soon as the request is placed. The answer arrives later as "+
			"a delivered message (\"APPROVAL ANSWERED\"), possibly hours later or after a restart. Do "+
			"not wait for it, and do not act as if approved until that message says yes. Only on a "+
			"connection whose server offers approvals (the connect result says so). Approvers are the "+
			"files in the approvers directory hub_approvals() names; their key lines are copied "+
			"there by hand from the phone, never from a message"),
		connectionParam(),
		mcp.WithString("approver", mcp.Required(), mcp.Description(
			"Which person: the name of their key file (without .keys) in this machine's approvers "+
				"directory")),
		mcp.WithString("text", mcp.Required(), mcp.Description(
			"The question, as markdown: say exactly what will be done, so the person can decide from "+
				"this text alone. The phone shows it with formatting and clickable links")),
		mcp.WithNumber("expiresInMinutes", mcp.Description(
			"How long the request stays answerable; default 60. At most what the server allows "+
				"(7 days)")),
		mcp.WithArray("actors", mcp.WithStringItems(), mcp.Description(
			"Optional: the names of the agents allowed to act on this approval when it is passed on "+
				"as a proof. Leave out for any agent that trusts the same approver")),
		mcp.WithArray("choices", mcp.WithStringItems(), mcp.Description(
			"Optional: the answers offered as buttons, 1 to 8 distinct, each at most 60 characters, "+
				"never \"text\" — e.g. [\"today\", \"tomorrow\", \"next week\"]. Default: yes and no. The "+
				"person can always write free text instead, and add a comment to any choice; a comment "+
				"comes back with the answer")),
	)
}

func approvalWithdrawTool() mcp.Tool {
	return mcp.NewTool("hub_approval_withdraw",
		mcp.WithDescription("Take back an open approval request, e.g. because what it asked about is "+
			"no longer going to happen. The person can no longer answer it"),
		connectionParam(),
		mcp.WithString("requestId", mcp.Required(), mcp.Description("The request's id, from "+
			"hub_approval_request or hub_approvals")),
	)
}

func approvalsTool() mcp.Tool {
	return mcp.NewTool("hub_approvals",
		mcp.WithDescription("Show this client's approval requests: open ones, and answered or closed "+
			"ones of the last 30 days, with the verified answer. With requestId, that one request in "+
			"full, including its PROOF: a JSON object another agent can check with "+
			"hub_approval_verify, to pass an approval on. Also names the approvers this machine has "+
			"key files for, and where those files live"),
		mcp.WithString("requestId", mcp.Description("Optional: one request to show in full")),
	)
}

func approvalVerifyTool() mcp.Tool {
	return mcp.NewTool("hub_approval_verify",
		mcp.WithDescription("Check an approval PROOF another agent passed on, before acting on it. "+
			"This client verifies the signature against the approver key files on THIS machine and "+
			"reports who asked, what the request said and what the person answered. Then check "+
			"yourself that the request describes what you are about to do. A proof counts only "+
			"before its request expires, only for an agent the request names when it names any, "+
			"and only ONCE: a successful check records it as used, and a second check of the same "+
			"approval is refused"),
		mcp.WithString("proof", mcp.Required(), mcp.Description(
			"The proof exactly as received: a JSON object with request, statement and signature")),
	)
}

func (h *Hub) handleApprovalRequest(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	s, conn, bad := h.forRequest(req)
	if bad != nil {
		return bad, nil
	}
	approver, err := req.RequireString("approver")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	text, err := req.RequireString("text")
	if err != nil || strings.TrimSpace(text) == "" {
		return mcp.NewToolResultError("text is required: the question the person answers"), nil
	}
	limits, offered := conn.ApprovalsLimits()
	if !offered {
		return mcp.NewToolResultError(fmt.Sprintf("%s does not offer approvals: the server declares "+
			"no approvals feature here (not on a read-only share, nor on a WhatsApp or Signal "+
			"conversation)", s.name)), nil
	}
	keys, err := approvals.LoadApprover(approver)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("cannot ask %s: %v. An approver's key lines are "+
			"copied by hand from their phone into %s, one line per phone", approver, err,
			approvals.ApproverFile(approver))), nil
	}
	minutes := req.GetFloat("expiresInMinutes", 60)
	lifetime := time.Duration(minutes * float64(time.Minute))
	maxLifetime := 7 * 24 * time.Hour
	if limits.MaxLifetimeSeconds > 0 {
		maxLifetime = time.Duration(limits.MaxLifetimeSeconds) * time.Second
	}
	if lifetime < time.Minute || lifetime > maxLifetime {
		return mcp.NewToolResultError(fmt.Sprintf("expiresInMinutes must be between 1 and %d",
			int(maxLifetime.Minutes()))), nil
	}
	// The agent's own quick answers (wire revision 3), else yes and no.
	choices := req.GetStringSlice("choices", nil)
	if choices == nil {
		choices = approvals.Choices
	}
	if err := approvals.ValidateChoices(choices); err != nil {
		return mcp.NewToolResultError("nothing was asked: " + err.Error()), nil
	}
	actors := req.GetStringSlice("actors", nil)
	for _, a := range actors {
		if strings.TrimSpace(a) == "" {
			return mcp.NewToolResultError("an empty name in actors"), nil
		}
	}
	maxEnvelope := maxApprovalEnvelope
	if limits.MaxEnvelopeBytes > 0 {
		maxEnvelope = limits.MaxEnvelopeBytes
	}

	now := approvalNow().UTC().Truncate(time.Second)
	expiresAt := now.Add(lifetime).Truncate(time.Second).Format(time.RFC3339)
	requestID := approvals.NewRequestID()
	var envelope string
	err = approvals.Update(approvalProject(), now, func(st *approvals.State) error {
		identity, err := st.AgeIdentity()
		if err != nil {
			return err
		}
		plain, err := approvals.Request{Actors: actors,
			Asker: approvals.Asker{AgeRecipient: identity.Recipient().String(), Name: approvalAskerName()},
			// Comments: this client reports a comment beside a choice (AnswerText),
			// so the phone may attach one (revision 3).
			Choices: choices, Comments: true, CreatedAt: now.Format(time.RFC3339), ExpiresAt: expiresAt,
			RequestID: requestID, Text: text, V: 1}.Encode()
		if err != nil {
			return err
		}
		if envelope, err = approvals.Seal(plain, keys); err != nil {
			return err
		}
		if len(envelope) > maxEnvelope {
			return fmt.Errorf("the request is %s encrypted; this server takes at most %s — shorten "+
				"the text", byteText(int64(len(envelope))), byteText(int64(maxEnvelope)))
		}
		// Recorded before it is sent: an answer can come back faster than
		// this call returns.
		st.Requests[requestID] = &approvals.Entry{RequestID: requestID, Approver: approver,
			Connection: s.name, Request: string(plain), ExpiresAt: expiresAt, State: approvals.StateOpen}
		return nil
	})
	if err != nil {
		return mcp.NewToolResultError("nothing was asked: " + err.Error()), nil
	}
	forget := func() {
		_ = approvals.Update(approvalProject(), approvalNow(), func(st *approvals.State) error {
			delete(st.Requests, requestID)
			return nil
		})
	}
	fingerprints := make([]string, 0, len(keys))
	for _, k := range keys {
		fingerprints = append(fingerprints, k.Fingerprint)
	}
	ev, ok, err := conn.ApprovalRequest(requestID, fingerprints, envelope, expiresAt)
	switch {
	case err != nil:
		forget()
		return mcp.NewToolResultError("the request could not be sent: " + noticeText(err.Error())), nil
	case !ok:
		return mcp.NewToolResultError(fmt.Sprintf("the server did not answer request %s in time. It "+
			"may still have been placed: hub_approvals(requestId: %q) shows it, and an answer is "+
			"still taken if one comes", requestID, requestID)), nil
	case ev.Kind == "error":
		forget()
		return mcp.NewToolResultError(approvalRefusal(ev, approver)), nil
	case !ev.ActionOK:
		forget()
		return mcp.NewToolResultError("the server did not accept the request; nothing was asked"), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Asked %s for approval (%d phone(s)): request %s, open "+
		"until %s.\nNOT APPROVED YET. The answer arrives later as a delivered message saying APPROVAL "+
		"ANSWERED, possibly much later — do not wait for it here, and do not act as if approved "+
		"before it says yes. hub_approval_withdraw takes the request back if it is no longer needed.",
		approver, len(keys), requestID, expiresAt)), nil
}

func approvalRefusal(ev hubconn.Event, approver string) string {
	why := map[string]string{
		"not_allowed": approver + " has not allowed this conversation to ask for approvals, or can " +
			"no longer read it",
		"unknown_approver":   "the server knows no paired, unrevoked phone with " + approver + "'s keys",
		"ambiguous_approver": approver + "'s key file holds phones of two different people",
		"bad_request":        "the server found the request malformed or out of range",
		"read_only":          "this conversation may not write",
		"too_many": "too many requests are open (at most 20 for this client, 50 per approver in " +
			"this conversation); wait for answers or withdraw some with hub_approval_withdraw",
	}[ev.Code]
	if why == "" {
		why = noticeText(ev.Text)
	}
	return fmt.Sprintf("nothing was asked: %s (%s)", why, ev.Code)
}

func (h *Hub) handleApprovalWithdraw(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	s, conn, bad := h.forRequest(req)
	if bad != nil {
		return bad, nil
	}
	requestID, err := req.RequireString("requestId")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	var entry approvals.Entry
	found := false
	_ = approvals.Update(approvalProject(), approvalNow(), func(st *approvals.State) error {
		if e := st.Requests[requestID]; e != nil {
			entry, found = *e, true
		}
		return nil
	})
	switch {
	case !found:
		return mcp.NewToolResultError(fmt.Sprintf("this client has no request %s", requestID)), nil
	case entry.Connection != s.name:
		return mcp.NewToolResultError(fmt.Sprintf("request %s was asked on %s, not %s", requestID,
			entry.Connection, s.name)), nil
	case entry.State != approvals.StateOpen:
		return mcp.NewToolResultText(fmt.Sprintf("Request %s is already %s; nothing to withdraw.",
			requestID, entry.State)), nil
	}
	ev, ok, err := conn.ApprovalWithdraw(requestID)
	switch {
	case err != nil:
		return mcp.NewToolResultError("the withdraw could not be sent: " + noticeText(err.Error())), nil
	case !ok:
		return mcp.NewToolResultError("the server did not answer the withdraw in time; the request " +
			"may still be open"), nil
	case ev.Kind == "error":
		return mcp.NewToolResultError(fmt.Sprintf("the server refused the withdraw: %s (%s)",
			noticeText(ev.Text), ev.Code)), nil
	}
	state := approvals.StateWithdrawn
	if ev.Approval != nil && ev.Approval.State != "" && ev.Approval.State != approvals.StateWithdrawn {
		state = ev.Approval.State
	}
	_ = approvals.Update(approvalProject(), approvalNow(), func(st *approvals.State) error {
		// An answered request is closed by its answer, once that arrives
		// and is verified.
		if e := st.Requests[requestID]; e != nil && e.State == approvals.StateOpen &&
			state != approvals.StateAnswered {
			e.Close(state, approvalNow())
		}
		return nil
	})
	if state == approvals.StateAnswered {
		return mcp.NewToolResultText(fmt.Sprintf("Request %s was already answered; the answer is "+
			"delivered as its own message once verified.", requestID)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("Request %s is %s; the person can no longer answer it.",
		requestID, state)), nil
}

func (h *Hub) handleApprovals(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	one := req.GetString("requestId", "")
	var b strings.Builder
	err := approvals.Update(approvalProject(), approvalNow(), func(st *approvals.State) error {
		if one != "" {
			e := st.Requests[one]
			if e == nil {
				return fmt.Errorf("this client has no request %s", one)
			}
			describeEntry(&b, e, true)
			return nil
		}
		ids := make([]string, 0, len(st.Requests))
		for id := range st.Requests {
			ids = append(ids, id)
		}
		slices.SortFunc(ids, func(a, c string) int {
			return strings.Compare(st.Requests[c].ExpiresAt, st.Requests[a].ExpiresAt)
		})
		if len(ids) == 0 {
			b.WriteString("No approval requests on record.\n")
		}
		for _, id := range ids {
			describeEntry(&b, st.Requests[id], false)
		}
		return nil
	})
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	names := approvals.Approvers()
	if len(names) == 0 {
		fmt.Fprintf(&b, "\nNo approvers: there are no key files in %s. A key line is copied by hand "+
			"from the person's phone into <name>.keys there, one line per phone.", approvals.ApproversDir())
	} else {
		fmt.Fprintf(&b, "\nApprovers (key files in %s): %s. This client asks as %q.",
			approvals.ApproversDir(), strings.Join(names, ", "), approvalAskerName())
	}
	return mcp.NewToolResultText(b.String()), nil
}

func describeEntry(b *strings.Builder, e *approvals.Entry, full bool) {
	var rq approvals.Request
	_ = json.Unmarshal([]byte(e.Request), &rq)
	fmt.Fprintf(b, "- %s to %s on %s: %s", e.RequestID, e.Approver, e.Connection, strings.ToUpper(e.State))
	if e.State == approvals.StateOpen {
		fmt.Fprintf(b, " until %s", e.ExpiresAt)
	} else if e.ClosedAt != "" {
		fmt.Fprintf(b, " at %s", e.ClosedAt)
	}
	if e.Statement != "" {
		var st approvals.Statement
		_ = json.Unmarshal([]byte(e.Statement), &st)
		fmt.Fprintf(b, " — answer: %s (phone %s, signed %s)", approvals.AnswerText(st), st.Approver.Label,
			st.SignedAt)
	}
	text := rq.Text
	if !full && len([]rune(text)) > 120 {
		text = string([]rune(text)[:120]) + "…"
	}
	fmt.Fprintf(b, "\n  asked: %q\n", text)
	if !full {
		return
	}
	for _, n := range e.Notes {
		fmt.Fprintf(b, "  not taken: %s\n", n)
	}
	if p, ok := e.Proof(); ok {
		raw, _ := json.Marshal(p)
		fmt.Fprintf(b, "  proof (pass it on exactly as it is; the receiver checks it with hub_approval_verify):\n%s\n", raw)
	}
}

func (h *Hub) handleApprovalVerify(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	raw, err := req.RequireString("proof")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	p, err := approvals.ParseProof([]byte(raw))
	if err != nil || p.Request == "" || p.Statement == "" || p.Signature == "" {
		return mcp.NewToolResultError("NOT VALID: the proof is not a JSON object with request, " +
			"statement and signature"), nil
	}
	named, err := approvals.ParseStatement(p.Statement)
	if err != nil {
		return mcp.NewToolResultError("NOT VALID: " + string(approvals.Malformed) + ". Do not act on it"), nil
	}
	approver, keys, ok := approvals.FindSigner(named.Approver.SignKey)
	if !ok {
		return mcp.NewToolResultError("NOT VALID: the phone that signed it is in none of this " +
			"machine's approver key files. Do not act on it"), nil
	}
	st, key, reason := approvals.Check(p.Statement, p.Signature, []byte(p.Request), keys)
	if reason != "" {
		return mcp.NewToolResultError(fmt.Sprintf("NOT VALID: %s. Do not act on it", reason)), nil
	}
	rq, _ := approvals.ParseRequest([]byte(p.Request))
	now := approvalNow()
	if expired(st.ExpiresAt, now) {
		return mcp.NewToolResultError(fmt.Sprintf("NOT VALID ANY MORE: the request expired at %s. "+
			"Do not act on it", st.ExpiresAt)), nil
	}
	me := approvalAskerName()
	if len(rq.Actors) > 0 && !slices.Contains(rq.Actors, me) {
		return mcp.NewToolResultError(fmt.Sprintf("NOT FOR YOU: the request names %s as allowed to "+
			"act on it, and this client is %q. Do not act on it", strings.Join(rq.Actors, ", "), me)), nil
	}
	var usedAt string
	err = approvals.Update(approvalProject(), now, func(s *approvals.State) error {
		if at, ok := s.Used[st.RequestID]; ok {
			usedAt = at
			return nil
		}
		s.Used[st.RequestID] = now.UTC().Format(time.RFC3339)
		return nil
	})
	if err != nil {
		return mcp.NewToolResultError("the proof could not be recorded as used, so it is not " +
			"accepted: " + err.Error()), nil
	}
	if usedAt != "" {
		return mcp.NewToolResultError(fmt.Sprintf("ALREADY USED: this approval (request %s) was "+
			"checked and taken at %s. An approval counts once; do not act on it again",
			st.RequestID, usedAt)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf("VALID, signed by %s (phone %s) at %s and verified against "+
		"this machine's key file.\nAsked by: %s\nRequest %s: %q\nAnswer: %s\nNow recorded as used. "+
		"Act on it only if the request describes exactly what you are about to do, and only if the "+
		"answer is yes.", approver, key.Label, st.SignedAt, rq.Asker.Name, st.RequestID, rq.Text,
		approvals.AnswerText(st))), nil
}

func expired(at string, now time.Time) bool {
	t, err := time.Parse(time.RFC3339, at)
	return err != nil || !now.Before(t)
}

// approvalArrived is hubconn.ApprovalHandler: it verifies and records an
// answer or a close as it arrives, acks an answer once it is recorded,
// and returns what the reader is told.
func approvalArrived(c *hubconn.Conn, ev hubconn.Event) string {
	a := ev.Approval
	if a == nil {
		return ""
	}
	now := approvalNow()
	if ev.Kind == "approvalClosed" {
		var text string
		_ = approvals.Update(approvalProject(), now, func(st *approvals.State) error {
			e := st.Requests[a.RequestID]
			if e == nil || e.State != approvals.StateOpen {
				return nil
			}
			how := "expired"
			state := approvals.StateExpired
			if a.State == approvals.StateWithdrawn {
				how, state = "withdrawn by the server (the person switched approvals off for this "+
					"conversation)", approvals.StateWithdrawn
			}
			e.Close(state, now)
			text = fmt.Sprintf("[hub: APPROVAL CLOSED — request %s to %s was %s, unanswered. Nobody "+
				"can answer it any more; it is NOT an approval. Ask again if it is still needed]",
				a.RequestID, e.Approver, how)
			return nil
		})
		return text
	}

	var notice string
	ack := true
	err := approvals.Update(approvalProject(), now, func(st *approvals.State) error {
		notice, ack = answerArrived(st, a, now)
		return nil
	})
	if err != nil {
		return fmt.Sprintf("[hub: an approval answer for request %s arrived but could not be "+
			"recorded (%s). It is not acknowledged, so the server delivers it again on the next "+
			"connect. Until then it is NOT an approval]", a.RequestID, noticeText(err.Error()))
	}
	if ack {
		_ = c.ApprovalAnswerAck(a.RequestID)
	}
	return notice
}

// answerArrived judges one answer against the state and records the
// outcome. It reports the notice and whether the answer may be acked.
func answerArrived(st *approvals.State, a *hubconn.ApprovalFrame, now time.Time) (string, bool) {
	id := a.RequestID
	e := st.Requests[id]
	if e == nil {
		return fmt.Sprintf("[hub: an approval answer arrived for request %s, which this client has "+
			"no record of (asked from another project scope, or forgotten). It is NOT an approval "+
			"and is not shown]", noticeText(id)), true
	}
	invalid := func(why string) (string, bool) {
		e.Notes = append(e.Notes, fmt.Sprintf("%s: invalid — %s", now.UTC().Format(time.RFC3339), why))
		return fmt.Sprintf("[hub: INVALID APPROVAL ANSWER for request %s to %s — %s. It is NOT an "+
			"answer: do not act on it. The request stays %s]", id, e.Approver, why, e.State), true
	}
	identity, err := st.AgeIdentity()
	if err != nil {
		return invalid("this client's own key is unreadable")
	}
	plain, err := approvals.Open(a.Envelope, identity, maxApprovalEnvelope)
	if err != nil {
		return invalid("it cannot be decrypted with this client's key")
	}
	signed, err := approvals.ParseSigned(plain)
	if err != nil || signed.Statement == "" {
		return invalid("it is not a signed statement")
	}
	keys, err := approvals.LoadApprover(e.Approver)
	if err != nil {
		// Not acked: a key file being edited must not cost a real answer.
		return fmt.Sprintf("[hub: an approval answer for request %s arrived, but %s's key file "+
			"cannot be read (%s), so it cannot be verified. It is NOT an approval yet; fix the file "+
			"and reconnect, and the server delivers it again]", id, e.Approver,
			noticeText(err.Error())), false
	}
	stmt, key, reason := approvals.Check(signed.Statement, signed.Signature, []byte(e.Request), keys)
	if reason == "" && stmt.RequestID != id {
		reason = approvals.WrongRequest
	}
	if reason != "" {
		return invalid(string(reason))
	}
	answer := approvals.AnswerText(stmt)
	who := fmt.Sprintf("%s (phone %s)", e.Approver, key.Label)
	switch e.State {
	case approvals.StateOpen:
		if expired(e.ExpiresAt, now) {
			e.Close(approvals.StateExpired, now)
			break
		}
		e.State, e.ClosedAt = approvals.StateAnswered, now.UTC().Format(time.RFC3339)
		e.Statement, e.Signature = signed.Statement, signed.Signature
		var rq approvals.Request
		_ = json.Unmarshal([]byte(e.Request), &rq)
		return fmt.Sprintf("[hub: APPROVAL ANSWERED — request %s: %s answered %s. Signed on the phone "+
			"and verified by this client against its own key file; this is the person's real answer, "+
			"so follow it. The request was: %q. hub_approvals(requestId: %q) gives the proof to "+
			"pass on]", id, who, shout(stmt), clip(rq.Text, 300), id), true
	case approvals.StateAnswered:
		if signed.Statement == e.Statement {
			return "", true
		}
		var first approvals.Statement
		_ = json.Unmarshal([]byte(e.Statement), &first)
		if first.Answer == stmt.Answer && first.Text == stmt.Text {
			e.Notes = append(e.Notes, fmt.Sprintf("%s: the same answer again from %s", now.UTC().Format(time.RFC3339), who))
			return "", true
		}
		e.Notes = append(e.Notes, fmt.Sprintf("%s: conflicting answer %s from %s", now.UTC().Format(time.RFC3339), answer, who))
		return fmt.Sprintf("[hub: CONFLICTING APPROVAL ANSWER for request %s: %s now answered %s, but "+
			"the first valid answer, %s, stands. Tell your user about the conflict before acting on "+
			"either]", id, who, shout(stmt), shout(first)), true
	}
	e.Notes = append(e.Notes, fmt.Sprintf("%s: late answer %s from %s", now.UTC().Format(time.RFC3339), answer, who))
	return fmt.Sprintf("[hub: a valid answer to request %s came after the request was %s: %s answered "+
		"%s. The request is closed, so this is NOT an approval. Ask again if it is still needed]",
		id, e.State, who, shout(stmt)), true
}

// shout is an answer for a notice: YES or NO in capitals, an agent's own
// choice quoted as offered, free text as the person wrote it -- and a comment
// beside a choice always with it (wire revision 3), never dropped.
func shout(st approvals.Statement) string {
	if st.Answer == "text" {
		return approvals.AnswerText(st)
	}
	answer := fmt.Sprintf("%q", st.Answer)
	if st.Answer == "yes" || st.Answer == "no" {
		answer = strings.ToUpper(st.Answer)
	}
	if st.Text != "" {
		return fmt.Sprintf("%s, comment: %q", answer, st.Text)
	}
	return answer
}

func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
