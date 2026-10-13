package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/hubenc"
	"github.com/secforge/mcp-hub/internal/mcptools"
	"github.com/secforge/mcp-hub/internal/wire"
)

// Exit codes of the send command, so a script can tell outcomes apart.
const (
	sendOK         = 0
	sendError      = 1
	sendUsage      = 2
	sendNotPresent = 3
	sendRefused    = 4
)

const sendUsageText = `usage: mcp-hub-client send --link <link> [options] <text | ->

Sends one message into a hub conversation and exits.

  --connection NAME   a connection stored under the --project scope, by the
                      name it was opened as: its link, identity and key file
                      are taken from there
  --link URL          a link issued for this command, instead of --connection.
                      A link token connects one client at a time, so create a
                      new one rather than reusing a session's link
  --if-present PEER   only if PEER is in the conversation (peer id, or exact
                      display name), and then privately to PEER; exits 3
                      without sending when PEER is not there
  --key-file PATH     key file of an end-to-end encrypted session
  --file PATH         attach a file
  --format F          markdown or text (default: the conversation's default)
  --name NAME         display name to join under (default "mcp-hub-client send")
  --project DIR       identity scope (default: this project's scope + "+send")
  --timeout D         how long to wait for the connection, the member list
                      and the acknowledgement (default 15s)

Text "-" or no text reads the message from stdin.
Exit codes: 0 sent, 1 error, 2 usage, 3 peer not present (nothing sent),
4 refused by the server or not acknowledged.`

func runSend(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	link := fs.String("link", "", "")
	connection := fs.String("connection", "", "")
	ifPresent := fs.String("if-present", "", "")
	keyFile := fs.String("key-file", "", "")
	file := fs.String("file", "", "")
	format := fs.String("format", "", "")
	name := fs.String("name", "mcp-hub-client send", "")
	project := fs.String("project", "", "")
	timeout := fs.Duration("timeout", 15*time.Second, "")
	if err := fs.Parse(args); err != nil || (*link == "") == (*connection == "") || fs.NArg() > 1 {
		fmt.Fprintln(stderr, sendUsageText)
		return sendUsage
	}
	text := fs.Arg(0)
	if text == "" || text == "-" {
		raw, err := io.ReadAll(stdin)
		if err != nil {
			fmt.Fprintf(stderr, "send: reading stdin: %v\n", err)
			return sendError
		}
		text = strings.TrimRight(string(raw), "\n")
	}
	if strings.TrimSpace(text) == "" && *file == "" {
		fmt.Fprintln(stderr, "send: nothing to send — the message is empty and no --file was given")
		return sendUsage
	}

	var attachments []wire.Attachment
	if *file != "" {
		a, err := wire.ReadFileAttachment(*file)
		if err != nil {
			fmt.Fprintf(stderr, "send: %v\n", err)
			return sendError
		}
		attachments = a
	}
	if *project == "" {
		*project = connstore.CurrentProject() + "+send"
	}
	if *connection != "" {
		stored, err := storedConnection(*project, *connection)
		if err != nil {
			fmt.Fprintf(stderr, "send: %v\n", err)
			return sendError
		}
		*link = stored.Target.Link
		if *keyFile == "" {
			*keyFile = stored.Entry.KeyFile
		}
	}
	var key []byte
	if *keyFile != "" {
		k, err := hubenc.ReadKeyFile(*keyFile)
		if err != nil {
			fmt.Fprintf(stderr, "send: %v\n", err)
			return sendError
		}
		key = k
	}

	conn, err := dialForSend(*link, *project, *name, key)
	if err != nil {
		fmt.Fprintf(stderr, "send: %v\n", err)
		return sendError
	}
	defer conn.Close()
	deadline := time.Now().Add(*timeout)

	to := ""
	if *ifPresent != "" {
		peer, err := findPresentPeer(conn, *ifPresent, deadline)
		if errors.Is(err, errNotPresent) {
			fmt.Fprintf(stderr, "send: %s is not in the conversation — nothing was sent\n", *ifPresent)
			return sendNotPresent
		}
		if err != nil {
			fmt.Fprintf(stderr, "send: %v — nothing was sent\n", err)
			return sendError
		}
		to = peer.ID
	}

	ev, answered, err := conn.SendAwaitingAck(text, to, attachments, *format, "", nil)
	if err != nil {
		fmt.Fprintf(stderr, "send: %v\n", err)
		return sendError
	}
	switch {
	case !conn.WantsActionAcks():
		fmt.Fprintln(stdout, "sent (this server does not acknowledge sends)")
		return sendOK
	case !answered:
		fmt.Fprintln(stderr, "send: sent, but the server did not acknowledge it in time — it may or may not have arrived")
		return sendRefused
	case ev.Kind == "sendAck" && ev.ActionOK:
		where := "to the conversation"
		if to != "" {
			where = "privately to " + to
		}
		fmt.Fprintf(stdout, "sent %s (externalId=%s)\n", where, ev.ExternalID)
		return sendOK
	}
	fmt.Fprintf(stderr, "send: refused by the server: %s\n", hubconn.FormatEvent(ev))
	return sendRefused
}

// dialForSend connects under an identity of the command's own: the
// project scope plus "+send", unless --project names one. Sharing the MCP
// session's identity would supersede that session's live connection.
func dialForSend(link, project, name string, key []byte) (*hubconn.Conn, error) {
	target := connstore.Target{Link: link, Project: connstore.NormalizeProject(project)}
	stored, _, err := connstore.Get(target)
	if err != nil {
		return nil, fmt.Errorf("reading the connection store: %w", err)
	}
	// A link once joined encrypted stays encrypted; see hub_connect.
	if key == nil && stored.KeyFile != "" {
		k, err := hubenc.ReadKeyFile(stored.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("this link was joined encrypted, and its key file cannot be read: %w", err)
		}
		key = k
	}
	secret := stored.ReconnectSecret
	if secret == "" {
		secret = uuid.NewString()
		if err := connstore.SetReconnectSecret(target, secret); err != nil {
			return nil, fmt.Errorf("recording this command's identity: %w", err)
		}
	}
	conn, err := hubconn.Dial(link, hubconn.DialOptions{
		ReconnectSecret: secret, AgentID: stored.PeerID, Name: name, Key: key,
	})
	if err != nil {
		var refused *hubconn.HandshakeRefusedError
		if errors.As(err, &refused) && (refused.Status == 401 || refused.Status == 403) {
			return nil, fmt.Errorf("%w — the link is already claimed by another client (a link token "+
				"connects one client at a time); create a new link for this command", err)
		}
		return nil, err
	}
	if conn.PeerID() != stored.PeerID {
		_ = connstore.SetPeerID(target, conn.PeerID())
	}
	if encrypted, haveKey := conn.Encrypted(); encrypted && !haveKey {
		conn.Close()
		return nil, errors.New("this conversation is end-to-end encrypted; pass --key-file")
	}
	return conn, nil
}

// storedConnection finds the connection stored under project by the name
// it was opened as. One a live process holds right now is refused:
// connecting under its identity would knock that connection off.
func storedConnection(project, name string) (connstore.ListedEntry, error) {
	entries, err := connstore.ListForProject(project)
	if err != nil {
		return connstore.ListedEntry{}, fmt.Errorf("reading the connection store: %w", err)
	}
	var found []connstore.ListedEntry
	for _, e := range entries {
		if e.Entry.LocalName == name {
			found = append(found, e)
		}
	}
	switch {
	case len(found) == 0:
		return connstore.ListedEntry{}, fmt.Errorf("no connection named %q is stored under %s", name,
			connstore.NormalizeProject(project))
	case len(found) > 1:
		return connstore.ListedEntry{}, fmt.Errorf("%d connections named %q are stored under %s; pass --link",
			len(found), name, connstore.NormalizeProject(project))
	}
	e := found[0]
	if e.Entry.Connected && mcptools.HolderLive(e.Entry.Holder) {
		return connstore.ListedEntry{}, fmt.Errorf("connection %q is open in a running client (pid %d); "+
			"sending under its identity would knock that connection off — use a link of its own",
			name, e.Entry.Holder.PID)
	}
	return e, nil
}

var errNotPresent = errors.New("not present")

// findPresentPeer waits for the member list, then finds want by peer id or
// exact display name. A name two members share is refused rather than
// guessed.
func findPresentPeer(conn *hubconn.Conn, want string, deadline time.Time) (hubconn.PeerInfo, error) {
	if conn.FeaturesDeclared() && !conn.HasFeature("roster") {
		return hubconn.PeerInfo{}, errors.New("this server does not state who is in the conversation, so presence cannot be checked")
	}
	for !conn.RosterComplete() {
		if time.Now().After(deadline) {
			return hubconn.PeerInfo{}, errors.New("the member list did not arrive in time")
		}
		time.Sleep(50 * time.Millisecond)
	}
	var byName []hubconn.PeerInfo
	for _, p := range conn.Peers() {
		if p.ID == want {
			return p, nil
		}
		if strings.EqualFold(p.Name, want) {
			byName = append(byName, p)
		}
	}
	switch len(byName) {
	case 0:
		return hubconn.PeerInfo{}, errNotPresent
	case 1:
		return byName[0], nil
	}
	ids := make([]string, len(byName))
	for i, p := range byName {
		ids[i] = p.ID
	}
	return hubconn.PeerInfo{}, fmt.Errorf("%d members are named %q (%s); pass a peer id", len(byName), want, strings.Join(ids, ", "))
}
