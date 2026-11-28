package mcptools

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/approvals"
	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/wire"
)

// The phone of these tests is chat-relay's test-vector phone.
const (
	phoneSeedHex  = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	phoneIdentity = "AGE-SECRET-KEY-13ALZQQXXTTSZYUS24960G30JTHQDCG57WWLPE7E73KGLDF7WGPPS3TPEHM"
	phoneKeyLine  = "chat-relay-approver v1 Test-Phone ebVWLo_mVPlAeLES6KmLp5AfhTrmlb7X4OORC60ElmQ age14zulfzhfst25k860d2qkyduu285a05ar7e3tlesa429qcld77qssf0f63t"
)

// approvalServer is a hub session offering approvals whose phone answers
// every request at once: answer(request) decides what it signs.
type approvalServer struct {
	srv      *httptest.Server
	features map[string]json.RawMessage
	refuse   string
	answer   func(rq approvals.Request, plain []byte) []approvals.Signed

	mu       sync.Mutex
	requests []wire.ApprovalRequest
	acks     []string
	withdraw []string
}

func newApprovalServer(t *testing.T) *approvalServer {
	as := &approvalServer{features: map[string]json.RawMessage{
		"approvals":   json.RawMessage(`{"maxEnvelopeBytes":65536,"maxLifetimeSeconds":604800}`),
		"correlation": json.RawMessage(`{}`),
	}}
	as.srv = httptest.NewServer(http.HandlerFunc(as.hub))
	t.Cleanup(as.srv.Close)
	return as
}

func (as *approvalServer) link() string {
	return "ws" + strings.TrimPrefix(as.srv.URL, "http") + "/hub/join#approvals-test"
}

func sign(statement string) approvals.Signed {
	seed, _ := hex.DecodeString(phoneSeedHex)
	sig := ed25519.Sign(ed25519.NewKeyFromSeed(seed), append([]byte(approvals.SigningPrefix), statement...))
	return approvals.Signed{Statement: statement, Signature: base64.RawURLEncoding.EncodeToString(sig)}
}

// statementFor is the statement the test phone signs for rq.
func statementFor(rq approvals.Request, plain []byte, answer, text string) string {
	st := approvals.Statement{Answer: answer, Text: text,
		Approver: approvals.Approver{Label: "Test-Phone", SignKey: "ebVWLo_mVPlAeLES6KmLp5AfhTrmlb7X4OORC60ElmQ"},
		Asker:    rq.Asker, ExpiresAt: rq.ExpiresAt, RequestHash: approvals.Hash(plain), RequestID: rq.RequestID,
		SignedAt: rq.CreatedAt, V: 1}
	raw, _ := json.Marshal(st)
	return string(raw)
}

func (as *approvalServer) hub(w http.ResponseWriter, r *http.Request) {
	conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.WriteJSON(wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000",
		ServerVersion: wire.ProtocolVersion, CanSend: true, Features: as.features})
	phone, _ := age.ParseX25519Identity(phoneIdentity)
	for {
		var f map[string]any
		if err := conn.ReadJSON(&f); err != nil {
			return
		}
		raw, _ := json.Marshal(f)
		switch f["type"] {
		case "approvalAnswerAck":
			as.mu.Lock()
			as.acks = append(as.acks, f["requestId"].(string))
			as.mu.Unlock()
		case "approvalWithdraw":
			as.mu.Lock()
			as.withdraw = append(as.withdraw, f["requestId"].(string))
			as.mu.Unlock()
			conn.WriteJSON(wire.ApprovalWithdrawAck{Type: wire.TypeApprovalWithdrawAck, ID: f["id"].(string),
				OK: true, State: "withdrawn"})
		case "approvalRequest":
			var q wire.ApprovalRequest
			json.Unmarshal(raw, &q)
			as.mu.Lock()
			as.requests = append(as.requests, q)
			as.mu.Unlock()
			if as.refuse != "" {
				conn.WriteJSON(wire.Error{Type: wire.TypeError, ID: q.ID, Code: as.refuse, Message: "refused"})
				continue
			}
			conn.WriteJSON(wire.ApprovalRequestAck{Type: wire.TypeApprovalRequestAck, ID: q.ID, OK: true,
				RequestID: q.RequestID, State: "open"})
			plain, err := approvals.Open(q.Envelope, phone, 1<<16)
			if err != nil || as.answer == nil {
				continue
			}
			var rq approvals.Request
			json.Unmarshal(plain, &rq)
			asker, _ := age.ParseX25519Recipient(rq.Asker.AgeRecipient)
			for _, signed := range as.answer(rq, plain) {
				body, _ := json.Marshal(signed)
				env := sealTo(body, asker)
				conn.WriteJSON(wire.ApprovalAnswer{Type: wire.TypeApprovalAnswer, RequestID: q.RequestID,
					Envelope: env, AnsweredAt: rq.CreatedAt})
			}
		}
	}
}

func sealTo(plain []byte, r *age.X25519Recipient) string {
	k, _ := approvals.ParseKeyLine("chat-relay-approver v1 Asker ebVWLo_mVPlAeLES6KmLp5AfhTrmlb7X4OORC60ElmQ " + r.String())
	env, _ := approvals.Seal(plain, []approvals.Key{k})
	return env
}

func connectApprovals(t *testing.T, as *approvalServer) *Hub {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("MCP_HUB_CONNSTORE_DIR", dir)
	os.MkdirAll(approvals.ApproversDir(), 0o700)
	os.WriteFile(approvals.ApproverFile("steffen"), []byte(phoneKeyLine+"\n"), 0o600)
	h := NewHub()
	res := fileCall(t, h.handleConnect, map[string]any{"as": testConn, "link": as.link()})
	if res.IsError {
		t.Fatalf("connect: %s", textOf(res))
	}
	t.Cleanup(func() { h.handleDisconnect(context.Background(), connReqFor(testConn)) })
	return h
}

// received polls hub_receive until want appears.
func received(t *testing.T, h *Hub, want string) string {
	t.Helper()
	var all strings.Builder
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		res, _ := h.handleReceive(context.Background(), connReqFor(testConn))
		all.WriteString(textOf(res))
		if strings.Contains(all.String(), want) {
			return all.String()
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("never received %q; got:\n%s", want, all.String())
	return ""
}

var requestIDIn = regexp.MustCompile(`request (r[A-Za-z0-9_-]{22})`)

func TestAnApprovalIsAskedAnsweredVerifiedAckedAndPassedOnOnce(t *testing.T) {
	as := newApprovalServer(t)
	as.answer = func(rq approvals.Request, plain []byte) []approvals.Signed {
		return []approvals.Signed{sign(statementFor(rq, plain, "yes", ""))}
	}
	h := connectApprovals(t, as)
	res := fileCall(t, h.handleApprovalRequest, map[string]any{"connection": testConn, "approver": "steffen",
		"text": "Delete invoice **844**? <b>&</b>", "expiresInMinutes": 30})
	if res.IsError || !strings.Contains(textOf(res), "NOT APPROVED YET") {
		t.Fatalf("request: %s", textOf(res))
	}
	id := requestIDIn.FindStringSubmatch(textOf(res))[1]
	got := received(t, h, "APPROVAL ANSWERED")
	if !strings.Contains(got, "request "+id+": steffen (phone Test-Phone) answered YES") {
		t.Fatalf("notice: %s", got)
	}
	as.mu.Lock()
	q := as.requests[0]
	acks := append([]string(nil), as.acks...)
	as.mu.Unlock()
	if len(q.Approvers) != 1 || q.Approvers[0] != "5s3w8ZbPBmOuQFpOSMyhpm" || strings.Contains(q.Envelope, "invoice") {
		t.Fatalf("request frame: %+v", q)
	}
	if len(acks) != 1 || acks[0] != id {
		t.Fatalf("acks %v", acks)
	}

	res = fileCall(t, h.handleApprovals, map[string]any{"requestId": id})
	proof := regexp.MustCompile(`(?m)^(\{"request".*\})$`).FindStringSubmatch(textOf(res))
	if res.IsError || !strings.Contains(textOf(res), "ANSWERED") || proof == nil {
		t.Fatalf("approvals: %s", textOf(res))
	}
	res = fileCall(t, h.handleApprovalVerify, map[string]any{"proof": proof[1]})
	if res.IsError || !strings.Contains(textOf(res), "VALID, signed by steffen") || !strings.Contains(textOf(res), "Answer: yes") {
		t.Fatalf("verify: %s", textOf(res))
	}
	res = fileCall(t, h.handleApprovalVerify, map[string]any{"proof": proof[1]})
	if !res.IsError || !strings.Contains(textOf(res), "ALREADY USED") {
		t.Fatalf("second verify: %s", textOf(res))
	}
	tampered := strings.Replace(proof[1], `invoice **844**`, `invoice **845**`, 1)
	res = fileCall(t, h.handleApprovalVerify, map[string]any{"proof": tampered})
	if !res.IsError || !strings.Contains(textOf(res), "NOT VALID") {
		t.Fatalf("tampered verify: %s", textOf(res))
	}
}

func TestAForgedOrConflictingAnswerIsNeverTakenAsTheAnswer(t *testing.T) {
	as := newApprovalServer(t)
	as.answer = func(rq approvals.Request, plain []byte) []approvals.Signed {
		forged := sign(statementFor(rq, plain, "yes", ""))
		forged.Statement = strings.Replace(forged.Statement, `"answer":"yes"`, `"answer":"yes" `, 1)
		return []approvals.Signed{
			forged,
			sign(statementFor(rq, plain, "maybe", "")),
			sign(statementFor(rq, plain, "no", "")),
			sign(statementFor(rq, plain, "text", "only after the export")),
		}
	}
	h := connectApprovals(t, as)
	res := fileCall(t, h.handleApprovalRequest, map[string]any{"connection": testConn, "approver": "steffen",
		"text": "Restart vm5?"})
	if res.IsError {
		t.Fatal(textOf(res))
	}
	got := received(t, h, "CONFLICTING APPROVAL ANSWER")
	for _, want := range []string{
		"INVALID APPROVAL ANSWER", "the signature does not verify",
		"neither one of the request's choices", "answered NO", "free text: \"only after the export\"",
		"the first valid answer, NO, stands",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	as.mu.Lock()
	defer as.mu.Unlock()
	if len(as.acks) != 4 {
		t.Fatalf("every answer is acked once recorded; acks %v", as.acks)
	}
}

func TestARefusedOrUnofferedRequestLeavesNothingOpen(t *testing.T) {
	as := newApprovalServer(t)
	as.refuse = "not_allowed"
	h := connectApprovals(t, as)
	res := fileCall(t, h.handleApprovalRequest, map[string]any{"connection": testConn, "approver": "steffen",
		"text": "x"})
	if !res.IsError || !strings.Contains(textOf(res), "has not allowed this conversation") {
		t.Fatalf("refusal: %s", textOf(res))
	}
	res = fileCall(t, h.handleApprovalRequest, map[string]any{"connection": testConn, "approver": "nobody",
		"text": "x"})
	if !res.IsError || !strings.Contains(textOf(res), "nobody.keys") {
		t.Fatalf("unknown approver: %s", textOf(res))
	}
	res = fileCall(t, h.handleApprovals, map[string]any{})
	if !strings.Contains(textOf(res), "No approval requests on record") || !strings.Contains(textOf(res), "Approvers (key files in") {
		t.Fatalf("approvals: %s", textOf(res))
	}

	plain := newApprovalServer(t)
	delete(plain.features, "approvals")
	h2 := NewHub()
	if r := fileCall(t, h2.handleConnect, map[string]any{"as": testConn, "link": plain.link()}); r.IsError {
		t.Fatal(textOf(r))
	}
	defer h2.handleDisconnect(context.Background(), connReqFor(testConn))
	res = fileCall(t, h2.handleApprovalRequest, map[string]any{"connection": testConn, "approver": "steffen",
		"text": "x"})
	if !res.IsError || !strings.Contains(textOf(res), "does not offer approvals") {
		t.Fatalf("unoffered: %s", textOf(res))
	}
}

func TestAWithdrawnRequestTakesNoAnswer(t *testing.T) {
	as := newApprovalServer(t)
	h := connectApprovals(t, as)
	res := fileCall(t, h.handleApprovalRequest, map[string]any{"connection": testConn, "approver": "steffen",
		"text": "x"})
	id := requestIDIn.FindStringSubmatch(textOf(res))[1]
	res = fileCall(t, h.handleApprovalWithdraw, map[string]any{"connection": testConn, "requestId": id})
	if res.IsError || !strings.Contains(textOf(res), "is withdrawn") {
		t.Fatalf("withdraw: %s", textOf(res))
	}
	var plain string
	approvals.Update(approvalProject(), time.Now(), func(st *approvals.State) error {
		plain = st.Requests[id].Request
		return nil
	})
	var rq approvals.Request
	json.Unmarshal([]byte(plain), &rq)
	notice, ack := answerArrivedFor(t, rq, []byte(plain), "yes")
	if !ack || !strings.Contains(notice, "came after the request was withdrawn") || !strings.Contains(notice, "NOT an approval") {
		t.Fatalf("late: %q %v", notice, ack)
	}
}

func answerArrivedFor(t *testing.T, rq approvals.Request, plain []byte, answer string) (string, bool) {
	t.Helper()
	asker, _ := age.ParseX25519Recipient(rq.Asker.AgeRecipient)
	body, _ := json.Marshal(sign(statementFor(rq, plain, answer, "")))
	var notice string
	var ack bool
	approvals.Update(approvalProject(), time.Now(), func(st *approvals.State) error {
		notice, ack = answerArrived(st, &hubconn.ApprovalFrame{RequestID: rq.RequestID, Envelope: sealTo(body, asker)}, time.Now())
		return nil
	})
	return notice, ack
}

// Wire revision 3 (2026-11-28): the agent's own choices reach the phone with
// "comments": true, and a comment beside the chosen one reaches the agent.
func TestOwnChoicesAndAComment(t *testing.T) {
	as := newApprovalServer(t)
	as.answer = func(rq approvals.Request, plain []byte) []approvals.Signed {
		return []approvals.Signed{sign(statementFor(rq, plain, "morgen", "erst nach 10 Uhr"))}
	}
	h := connectApprovals(t, as)
	res := fileCall(t, h.handleApprovalRequest, map[string]any{"connection": testConn, "approver": "steffen",
		"text": "Wann?", "choices": []any{"heute", "morgen"}})
	if res.IsError {
		t.Fatalf("request: %s", textOf(res))
	}
	got := received(t, h, "APPROVAL ANSWERED")
	if !strings.Contains(got, `answered "morgen", comment: "erst nach 10 Uhr"`) {
		t.Fatalf("notice: %s", got)
	}
	as.mu.Lock()
	q := as.requests[0]
	as.mu.Unlock()
	phone, _ := age.ParseX25519Identity(phoneIdentity)
	plain, err := approvals.Open(q.Envelope, phone, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	var rq approvals.Request
	json.Unmarshal(plain, &rq)
	if !rq.Comments || strings.Join(rq.Choices, ",") != "heute,morgen" {
		t.Fatalf("request: %+v", rq)
	}

	res = fileCall(t, h.handleApprovalRequest, map[string]any{"connection": testConn, "approver": "steffen",
		"text": "Wann?", "choices": []any{"heute", "text"}})
	if !res.IsError || !strings.Contains(textOf(res), `"text" cannot be a choice`) {
		t.Fatalf("bad choices: %s", textOf(res))
	}
}
