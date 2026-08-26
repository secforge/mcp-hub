package wsserver

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

// dial connects directly to base+"/"+sessionID, which auto-joins the
// session as part of the handshake (no separate join message).
func dial(t *testing.T, base, sessionID string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(base+"/"+sessionID, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return c
}

func readTyped(t *testing.T, c *websocket.Conn) (wire.Type, []byte) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, raw, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	typ, err := wire.DecodeType(raw)
	if err != nil {
		t.Fatalf("decode type: %v", err)
	}
	return typ, raw
}

func TestJoinRelayAndTeardown(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a := dial(t, url, sessionID)
	defer a.Close()
	if typ, _ := readTyped(t, a); typ != wire.TypeJoined {
		t.Fatalf("a: expected joined, got %s", typ)
	}
	if typ, _ := readTyped(t, a); typ != wire.TypeRosterComplete {
		t.Fatalf("a: expected rosterComplete, got %s", typ)
	}

	b := dial(t, url, sessionID)
	if typ, _ := readTyped(t, b); typ != wire.TypeJoined {
		t.Fatalf("b: expected joined, got %s", typ)
	}
	if typ, _ := readTyped(t, a); typ != wire.TypePeerJoined {
		t.Fatalf("a: expected peerJoined for b, got %s", typ)
	}

	b.WriteJSON(wire.NewOutgoingMsg("hello from b"))
	typ, raw := readTyped(t, a)
	if typ != wire.TypeMsg {
		t.Fatalf("a: expected msg, got %s", typ)
	}
	var m wire.Msg
	decodeJSON(t, raw, &m)
	if m.Text != "hello from b" || m.PeerID == "" || m.TS == "" {
		t.Fatalf("unexpected msg: %+v", m)
	}

	b.Close()
	if typ, _ := readTyped(t, a); typ != wire.TypePeerLeft {
		t.Fatalf("a: expected peerLeft for b, got %s", typ)
	}

	data, err := os.ReadFile(dir + "/" + sessionID + ".log")
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if !strings.Contains(string(data), "hello from b") {
		t.Fatalf("log missing message: %s", data)
	}
}

func TestJoinAndLeaveAreLogged(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a := dial(t, url, sessionID)
	defer a.Close()
	readTyped(t, a) // a: joined
	readTyped(t, a) // a: rosterComplete

	b := dial(t, url, sessionID)
	_, rawJoinedB := readTyped(t, b) // b: joined
	var joinedB wire.Joined
	decodeJSON(t, rawJoinedB, &joinedB)
	readTyped(t, a) // a: peerJoined for b (broadcast) — by now b's join is logged

	b.Close()
	readTyped(t, a) // a: peerLeft for b — by now b's leave is logged

	data, err := os.ReadFile(dir + "/" + sessionID + ".log")
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	log := string(data)
	if !strings.Contains(log, joinedB.PeerID+" joined") {
		t.Fatalf("log missing joined entry for b: %s", log)
	}
	if !strings.Contains(log, joinedB.PeerID+" left") {
		t.Fatalf("log missing left entry for b: %s", log)
	}
}

func TestJoinTellsNewPeerAboutExistingRoster(t *testing.T) {
	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a := dial(t, url, sessionID)
	defer a.Close()
	readTyped(t, a) // a: joined
	readTyped(t, a) // a: rosterComplete

	b := dial(t, url, sessionID)
	defer b.Close()
	readTyped(t, b) // b: joined
	readTyped(t, a) // a: peerJoined for b

	c := dial(t, url, sessionID)
	defer c.Close()
	readTyped(t, c) // c: joined

	// c should be told about both a and b, in some order, before anything else.
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		typ, raw := readTyped(t, c)
		if typ != wire.TypePeerJoined {
			t.Fatalf("c: expected peerJoined, got %s", typ)
		}
		var pe wire.PeerEvent
		decodeJSON(t, raw, &pe)
		seen[pe.PeerID] = true
	}
	if len(seen) != 2 {
		t.Fatalf("expected c to be told about 2 distinct existing peers, got: %+v", seen)
	}
}

func TestJoinedIncludesAccuratePeerCountAndServerVersion(t *testing.T) {
	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a := dial(t, url, sessionID)
	defer a.Close()
	_, rawJoinedA := readTyped(t, a)
	var joinedA wire.Joined
	decodeJSON(t, rawJoinedA, &joinedA)
	if joinedA.PeerCount != 0 {
		t.Fatalf("a: expected peerCount 0 (first in session), got %d", joinedA.PeerCount)
	}
	if joinedA.ServerVersion != wire.ProtocolVersion {
		t.Fatalf("a: expected serverVersion %d, got %d", wire.ProtocolVersion, joinedA.ServerVersion)
	}

	b := dial(t, url, sessionID)
	defer b.Close()
	_, rawJoinedB := readTyped(t, b)
	var joinedB wire.Joined
	decodeJSON(t, rawJoinedB, &joinedB)
	if joinedB.PeerCount != 1 {
		t.Fatalf("b: expected peerCount 1 (a already present), got %d", joinedB.PeerCount)
	}
}

func TestConnectWithoutVersionQueryParamStillWorks(t *testing.T) {
	// No "?v=" at all - simulates an older client, or any plain websocket
	// client that doesn't know about version exchange. Must be treated as
	// v1 and must not be rejected.
	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	c, _, err := websocket.DefaultDialer.Dial(url+"/550e8400-e29b-41d4-a716-446655440000", nil)
	if err != nil {
		t.Fatalf("dial without version param should still succeed: %v", err)
	}
	defer c.Close()
	typ, _ := readTyped(t, c)
	if typ != wire.TypeJoined {
		t.Fatalf("expected joined, got %s", typ)
	}
}

func TestConnectWithVersionQueryParamAlsoWorks(t *testing.T) {
	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	c, _, err := websocket.DefaultDialer.Dial(url+"/550e8400-e29b-41d4-a716-446655440000?v=1", nil)
	if err != nil {
		t.Fatalf("dial with version param should succeed: %v", err)
	}
	defer c.Close()
	typ, _ := readTyped(t, c)
	if typ != wire.TypeJoined {
		t.Fatalf("expected joined, got %s", typ)
	}
}

func TestDirectedMessageGoesOnlyToTarget(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a := dial(t, url, sessionID)
	defer a.Close()
	readTyped(t, a) // a: joined
	readTyped(t, a) // a: rosterComplete

	b := dial(t, url, sessionID)
	defer b.Close()
	_, rawJoinedB := readTyped(t, b) // b: joined
	var joinedB wire.Joined
	decodeJSON(t, rawJoinedB, &joinedB)
	readTyped(t, b) // b: peerJoined for a (roster notification)
	readTyped(t, b) // b: rosterComplete
	readTyped(t, a) // a: peerJoined for b

	c := dial(t, url, sessionID)
	defer c.Close()
	readTyped(t, c) // c: joined
	readTyped(t, c) // c: peerJoined for a
	readTyped(t, c) // c: peerJoined for b
	readTyped(t, c) // c: rosterComplete
	readTyped(t, a) // a: peerJoined for c
	readTyped(t, b) // b: peerJoined for c

	// a sends a private message to b only.
	a.WriteJSON(wire.NewOutgoingDirectedMsg("just for you", joinedB.PeerID))

	typ, raw := readTyped(t, b)
	if typ != wire.TypeMsg {
		t.Fatalf("b: expected msg, got %s", typ)
	}
	var m wire.Msg
	decodeJSON(t, raw, &m)
	if m.Text != "just for you" || !m.Private {
		t.Fatalf("unexpected directed msg: %+v", m)
	}

	// c must never see it: send a broadcast afterward and confirm it's the
	// first thing c reads (i.e. nothing private was queued ahead of it).
	a.WriteJSON(wire.NewOutgoingMsg("broadcast after private"))
	typ, raw = readTyped(t, c)
	if typ != wire.TypeMsg {
		t.Fatalf("c: expected msg, got %s", typ)
	}
	var m2 wire.Msg
	decodeJSON(t, raw, &m2)
	if m2.Text != "broadcast after private" || m2.Private {
		t.Fatalf("c appears to have received the private message: %+v", m2)
	}

	data, err := os.ReadFile(dir + "/" + sessionID + ".log")
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if !strings.Contains(string(data), "just for you") || !strings.Contains(string(data), "->") {
		t.Fatalf("log missing directed entry: %s", data)
	}
}

func TestDirectedMessageToUnknownPeerReturnsErrorToSender(t *testing.T) {
	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a := dial(t, url, sessionID)
	defer a.Close()
	readTyped(t, a) // a: joined
	readTyped(t, a) // a: rosterComplete

	a.WriteJSON(wire.NewOutgoingDirectedMsg("hello?", "00000000-0000-0000-0000-000000000000"))
	typ, _ := readTyped(t, a)
	if typ != wire.TypeError {
		t.Fatalf("expected error for unknown target, got %s", typ)
	}
}

func TestDeadPeerIsDroppedViaPingPongTimeout(t *testing.T) {
	origPeriod, origWait, origWriteWait := pingPeriod, pongWait, writeWait
	pingPeriod = 30 * time.Millisecond
	pongWait = 100 * time.Millisecond
	writeWait = 30 * time.Millisecond
	defer func() { pingPeriod, pongWait, writeWait = origPeriod, origWait, origWriteWait }()

	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a := dial(t, url, sessionID)
	defer a.Close()
	readTyped(t, a) // a: joined
	readTyped(t, a) // a: rosterComplete

	b := dial(t, url, sessionID)
	defer b.Close()
	readTyped(t, b) // b: joined
	readTyped(t, a) // a: peerJoined for b

	// b never reads again, so it never responds to the server's pings. The
	// server should give up on it once pongWait elapses, and a should see a
	// peerLeft as a result.
	if typ, _ := readTyped(t, a); typ != wire.TypePeerLeft {
		t.Fatalf("expected peerLeft after b's ping/pong keepalive times out, got %s", typ)
	}
}

func TestInvalidSessionIDRejectedBeforeUpgrade(t *testing.T) {
	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	_, resp, err := websocket.DefaultDialer.Dial(url+"/not-a-uuid", nil)
	if err == nil {
		t.Fatal("expected the handshake to fail for an invalid sessionId")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		status := "<nil response>"
		if resp != nil {
			status = resp.Status
		}
		t.Fatalf("expected HTTP 400 before any websocket upgrade, got: %s", status)
	}
}

const testAgePublicKey = "age1scdm7mae5t68c9ch0sqfzlusqyflpgxlrgk3zwl44zwl9vvq2guqtdv4fk"

func TestInvalidAgePublicKeyRejectedBeforeUpgrade(t *testing.T) {
	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	_, resp, err := websocket.DefaultDialer.Dial(
		url+"/550e8400-e29b-41d4-a716-446655440000?agePublicKey=not-a-key", nil)
	if err == nil {
		t.Fatal("expected the handshake to fail for a malformed agePublicKey")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		status := "<nil response>"
		if resp != nil {
			status = resp.Status
		}
		t.Fatalf("expected HTTP 400 before any websocket upgrade, got: %s", status)
	}
}

func TestOverlongReconnectSecretRejectedBeforeUpgrade(t *testing.T) {
	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	_, resp, err := websocket.DefaultDialer.Dial(
		url+"/550e8400-e29b-41d4-a716-446655440000?reconnectSecret="+strings.Repeat("x", maxReconnectSecretRunes+1), nil)
	if err == nil {
		t.Fatal("expected the handshake to fail for an overlong reconnectSecret")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		status := "<nil response>"
		if resp != nil {
			status = resp.Status
		}
		t.Fatalf("expected HTTP 400 before any websocket upgrade, got: %s", status)
	}
}

func TestJoinedEchoesSanitizedNameAndValidAgePublicKey(t *testing.T) {
	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	c, _, err := websocket.DefaultDialer.Dial(
		url+"/550e8400-e29b-41d4-a716-446655440000?name=Steffen%0Afake+line&agePublicKey="+testAgePublicKey, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	_, raw := readTyped(t, c)
	var joined wire.Joined
	decodeJSON(t, raw, &joined)
	if joined.Name != "Steffenfake line" {
		t.Fatalf("expected the newline stripped from the name, got %q", joined.Name)
	}
	if joined.AgePublicKey != testAgePublicKey {
		t.Fatalf("got AgePublicKey %q, want %q", joined.AgePublicKey, testAgePublicKey)
	}
}

func TestPeerJoinedCarriesNameAndAgePublicKeyToOtherPeers(t *testing.T) {
	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a := dial(t, url, sessionID)
	defer a.Close()
	readTyped(t, a) // a: joined
	readTyped(t, a) // a: rosterComplete

	b, _, err := websocket.DefaultDialer.Dial(
		url+"/"+sessionID+"?name=Steffen&agePublicKey="+testAgePublicKey, nil)
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()
	readTyped(t, b) // b: joined

	_, raw := readTyped(t, a) // a: peerJoined for b
	var pe wire.PeerEvent
	decodeJSON(t, raw, &pe)
	if pe.Name != "Steffen" || pe.AgePublicKey != testAgePublicKey {
		t.Fatalf("expected a to be told b's name/agePublicKey, got %+v", pe)
	}
}

func TestReconnectingWithSameReconnectSecretReusesPeerID(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MCP_HUB_LOG_DIR", dir)

	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	const secret = "super-secret-reconnect-token"

	// another peer stays connected throughout, so the session (and its
	// secret memory) survives first's departure instead of being torn down.
	other := dial(t, url, sessionID)
	defer other.Close()
	readTyped(t, other) // other: joined
	readTyped(t, other) // other: rosterComplete

	first, _, err := websocket.DefaultDialer.Dial(
		url+"/"+sessionID+"?reconnectSecret="+secret, nil)
	if err != nil {
		t.Fatalf("dial first: %v", err)
	}
	_, raw := readTyped(t, first)
	var joinedFirst wire.Joined
	decodeJSON(t, raw, &joinedFirst)
	readTyped(t, other) // other: peerJoined for first
	first.Close()
	readTyped(t, other) // other: peerLeft for first — by now first is fully deregistered

	second, _, err := websocket.DefaultDialer.Dial(
		url+"/"+sessionID+"?reconnectSecret="+secret, nil)
	if err != nil {
		t.Fatalf("dial second: %v", err)
	}
	defer second.Close()
	_, raw = readTyped(t, second)
	var joinedSecond wire.Joined
	decodeJSON(t, raw, &joinedSecond)

	if joinedSecond.PeerID != joinedFirst.PeerID {
		t.Fatalf("expected reconnecting with the same reconnectSecret to reuse peerID %q, got %q",
			joinedFirst.PeerID, joinedSecond.PeerID)
	}
}

// TestObservingAgePublicKeyDoesNotAllowImpersonation is a security
// regression test at the HTTP/websocket layer: agePublicKey is broadcast to
// every other peer (see TestPeerJoinedCarriesNameAndAgePublicKeyToOtherPeers),
// so an "impersonator" who merely saw it on the wire — without knowing
// first's reconnectSecret — must not be able to reclaim first's peerId by
// presenting that same agePublicKey.
func TestObservingAgePublicKeyDoesNotAllowImpersonation(t *testing.T) {
	srv := httptest.NewServer(NewHandler())
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	other := dial(t, url, sessionID)
	defer other.Close()
	readTyped(t, other) // other: joined
	readTyped(t, other) // other: rosterComplete

	first, _, err := websocket.DefaultDialer.Dial(
		url+"/"+sessionID+"?agePublicKey="+testAgePublicKey, nil)
	if err != nil {
		t.Fatalf("dial first: %v", err)
	}
	_, raw := readTyped(t, first)
	var joinedFirst wire.Joined
	decodeJSON(t, raw, &joinedFirst)
	readTyped(t, other) // other: peerJoined for first (this is what "leaks" the key)
	first.Close()
	readTyped(t, other) // other: peerLeft for first

	impersonator, _, err := websocket.DefaultDialer.Dial(
		url+"/"+sessionID+"?agePublicKey="+testAgePublicKey, nil)
	if err != nil {
		t.Fatalf("dial impersonator: %v", err)
	}
	defer impersonator.Close()
	_, raw = readTyped(t, impersonator)
	var joinedImpersonator wire.Joined
	decodeJSON(t, raw, &joinedImpersonator)

	if joinedImpersonator.PeerID == joinedFirst.PeerID {
		t.Fatal("agePublicKey alone must never grant peerId reuse — it's broadcast to every peer, so this would allow impersonation")
	}
}

func decodeJSON(t *testing.T, raw []byte, v any) {
	t.Helper()
	if err := jsonUnmarshal(raw, v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
}
