package hubconn

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

// RelayDialOptions carries what a bridge-style relay connection (e.g.
// teams_relay_connect) needs, distinct from DialOptions: there is no
// sessionId/host pair to validate or normalize, and every credential is
// carried as a handshake header rather than a query parameter, so it can
// never end up in a URL — see DialRelay.
type RelayDialOptions struct {
	// ReconnectSecret is required (mirroring hub_connect's own contract,
	// for the identical reason: an agent should never be able to
	// accidentally make itself unresumable). It authorizes resuming a
	// spent link's session within whatever window the relay server
	// grants — it does not identify a peer the way hub_connect's
	// ReconnectSecret does, since a relay link already fixes which
	// conversation a connection belongs to.
	ReconnectSecret string
	// Name is optional, free-text, sent only if non-empty. A relay server
	// is not obligated to make it visible to anyone on the other side of
	// the bridge (e.g. chat-relay logs it for audit but never posts it
	// into the Teams conversation it relays into) — treat it purely as a
	// caller-side label, not a guarantee.
	Name string
}

// DialRelay connects to a bridge-style relay server via an opaque link
// rather than a host+sessionId pair. The link is everything up to and
// including a "#": the part before it is dialed as an ordinary WebSocket
// URL, and the part after it — the fragment — is sent as a Bearer
// credential on the handshake instead of appearing anywhere in the request
// itself. This is deliberate, not incidental: a URL fragment is defined to
// never be transmitted to a server, so putting a secret there means it
// cannot leak into that server's access logs, any proxy in front of it, or
// our own logs — by construction, not by remembering to redact it
// afterward. A link with no "#" is rejected outright rather than dialed
// with no credential, since a relay server that requires one would just
// reject the handshake anyway, less clearly.
//
// Reuses the same read loop, buffering, keepalive, and disconnect
// detection as Dial via finishHandshake — a relay connection is otherwise
// an ordinary Conn once established.
func DialRelay(link string, opts RelayDialOptions) (*Conn, error) {
	snapPongWait, snapWriteWait, snapAckIdleInterval, snapConfirmReminderInterval := pongWait, writeWait, ackIdleInterval, confirmReminderInterval

	target, secret, ok := strings.Cut(link, "#")
	if !ok || secret == "" {
		return nil, fmt.Errorf("relay link must include a #-delimited secret")
	}
	if opts.ReconnectSecret == "" {
		return nil, fmt.Errorf("reconnectSecret is required")
	}

	header := http.Header{}
	header.Set("Authorization", "Bearer "+secret)
	header.Set("Reconnect-Secret", opts.ReconnectSecret)
	if opts.Name != "" {
		header.Set("Agent-Name", opts.Name)
	}

	ws, _, err := websocket.DefaultDialer.Dial(target, header)
	if err != nil {
		return nil, err
	}
	return finishHandshake(ws, snapPongWait, snapWriteWait, snapAckIdleInterval, snapConfirmReminderInterval, true)
}
