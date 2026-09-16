# One client, several hub connections

Today one `mcp-hub-client` process holds at most one connection. Holding a
second one means registering a second MCP server (`mcp-hub2`), which is
why that registration exists at all. The goal is for one instance to
manage several connections at once.

The mechanical part is small: `activeConn()` is a single chokepoint with
16 call sites. The hard part is that every connection competes for the
same scarce thing — one model's attention — and that a reader who cannot
tell which conversation a message came from will answer the wrong one.

## The rule that decides most of the design

**Every connection is named when it is opened, and addressed by that name
afterwards.** Not inferred, not defaulted to the only open one, not a
"current" connection carried in hidden state. A tool call that does not
say which connection it means is an error, even when only one is open —
because the reader's habit is what has to survive the second connection
being opened, and a parameter that is optional today is one the model
will have stopped writing by then.

A wrong guess sends a private message to the wrong conversation, and
nothing downstream recovers that.

## 1. Names

`hub_connect` takes the name and refuses without it.

- **Clash to resolve first:** `hub_connect` already has a `name`
  parameter, and it means something else — the display name other peers
  see in the roster ("Claude Code (mcp-hub)"). The local name is a
  different thing with different rules (short, typeable, unique within
  this process). Proposal: the new one is `as`, and `name` keeps its
  meaning. The alternative — one name serving both — makes roster names
  into identifiers and is rejected.
- Validated at connect: non-empty, short, no whitespace, and **not
  already in use by an open connection in this process**. A duplicate is
  refused rather than suffixed; silently renaming a connection the caller
  just named is how a reader ends up addressing the wrong one.
- Persisted in connstore alongside the target, so a reconnect to the same
  link in the same project resumes under the same name. Reconnecting with
  a *different* name is allowed and re-labels it, since the model may be
  the one who chose badly the first time.
- `hub_list_connections` shows it. It is the address for every other tool.

## 2. Hub → session split

Everything currently on `Hub` that describes *a* connection moves to a
`session` struct:

```
conn, waiter, connTarget, catchUpID,
lastHandedOverCursor, knownContiguous, seekedSinceConnect, handedOverAhead,
redialLink, redialName, autoReconnect, reconnecting, reconnectAt,
attachDir, attachSeq
```

`Hub` keeps what belongs to the process: `sessions map[string]*session`,
insertion order, `pusher`, `inbox`, the queued-notes buffer, and the
shared budget (§5).

`activeConn()` becomes `session(name) (*session, error)`, which errors
with the list of open names when the name is unknown or missing. This is
the bulk of the diff.

A field left on `Hub` by accident is the likely bug of this whole
refactor: `knownContiguous` or `handedOverAhead` shared between two
connections would let a confirmation on one advance a position on the
other. Each of those fields carries its own reasoning comment today; the
comments move with them.

## 3. Labelling — every delivered line says where it came from

A message that does not name its connection cannot be answered, and a
cursor that does not name its connection can be confirmed against the
wrong one. Cursors from two servers are opaque and not comparable; a
cross-connection confirm must be **refused by construction**, not
detected by comparison.

- Push shape gains the name in its existing bracket header:
  `[untrusted, on chat-relay, from peer 43a7… ]`.
- The trailer becomes `[cursor: … on chat-relay]`.
- `hub_confirm(cursor, connection)` refuses a cursor whose name does not
  match, and says so.

## 4. One process, one socket per connection, no registry row

A connection appears to the reader as `mcp:<connection name>`, and with
its own inbox address a reply lands on the right connection **by
construction** — the model answers the `from=` address it was handed, and
nothing has to be selected, parsed or guessed.

This needs neither a supervisor nor a registry entry:

- The socket name grammar `^(\d+(-[0-9a-f]{8})?|[0-9a-f]{1,16})\.sock$`
  has the 8-hex discriminator precisely so sockets can share a pid.
- Reply-target validation accepts any `.sock` in the receiver's own
  directory outright; the cross-directory path needs the name grammar, a
  standard directory and a uid match. No registry lookup anywhere in it.
- `from-name` is an envelope attribute the SENDER sets per message, with
  no lookup on the receive path. So one process labels each push
  `mcp:<connection name>` and each connection displays as itself.

**Tested, not assumed.** One process bound two sockets and published no
registry row at all; both accepted a message from the harness:

```
alpha pid=1147531 addr=uds:/run/user/0/cc-socks/1147531-dcb798f9.sock
beta  pid=1147531 addr=uds:/run/user/0/cc-socks/1147531-78cb4c6a.sock
RECEIVED on alpha from pid 340459
RECEIVED on beta  from pid 340459
(no /root/.claude/sessions/1147531.json)
```

Both sockets were gone on exit.

**No registry row is published**, so connections do not appear in
`ListAgents` — settled by the owner, conditional on delivery working
without one, which the test above is exactly the evidence for. If a case
is ever found where delivery DOES need the row, the row comes back for
that case; the design does not otherwise depend on it either way.

Dropping the `<pid>.json` write costs one thing: the detail field on
`peer_idle_notice`, which is gated on the reply address matching a
registered socket for that pid. For a hub client that is nothing.

### Why not a required directive

A required `conn=<name>` fails open in the way that matters: an omitted
name is caught, a valid-but-wrong name is not — and that is the mistake a
model under context pressure actually makes. Misrouting across hub
connections is a confidentiality boundary, not an annoyance: a reply meant
for one conversation landing in another leaks between channels that were
deliberately kept apart.

And this week already paid for the alternative: the delivered-means-
accepted hazard was described in plain prose in a comment above the code
that had it, and shipped anyway. Documenting a mistake is not removing it.

### What binds these sockets must get right

1. **Unlink every socket on exit, and sweep stale ones at startup.** An
   advertised address whose owner is gone accepts nothing and reports
   nothing — worse than no address. `cc-socks` holds roughly twenty
   orphans from dead clients right now.
2. **A bind that fails, fails loudly** — a visible error on the
   connection, never a quiet degrade to pull-only. The first hour of one
   incident this week was exactly that silent downgrade.

## 5. The budget is per-reader, not per-connection

The delivery budget (500 KB / 200 message window, 128 KB spill) exists to
protect one model's attention. Left inside `Conn`, N connections means
N × the window, which defeats it exactly when it matters most.

- **One shared window**, moved out of `hubconn.Conn` into a budget owned
  by the Hub; connections charge against it. A noisy conversation can
  crowd out a quiet one — accepted, with the cap in §7 bounding it.
- **One push catch-up walk at a time**, queued across connections. Two
  backlogs interleaving produce a stream no reader can follow. The closing
  summary of each run names its connection.

This is the only part that is not a mechanical refactor, and the main
risk in the plan.

## 6. Wait socket (pull-mode harnesses only)

Still ONE SOCKET PER CONNECTION, deliberately, and this is a change from
what this section first proposed.

Multiplexing would save follower processes and cost correctness in the
worst place to spend it. The waiter's state machine is built on one
source: "the connection is coming back" (`expecting`), "the connection
ended" and "we are holding" are each a fact about one conversation, and
one socket serving eight would have to answer a follower asking about all
of them at once — telling it alpha has gone while beta is fine, without
releasing it, and without a silence that means either. That is the same
class of ambiguity this whole client exists to remove, introduced into
the one component whose entire job is to make silence mean something.

The cost of not doing it is one `wait --follow` per connection, which a
pull harness can run. Claude binds no socket at all here, so nothing in
this project pays it today.

## 7. Limits

**Eight simultaneous connections.** Not a resource limit: the shared
budget means each additional conversation costs the reader, and a cap is
the honest way to say so.

## 8. What this retires

`mcp-hub2` exists only to hold a second connection, and becomes
redundant. `MCP_HUB_SERVER_NAME` stays — it names the inbox and the
registry row — but the second MCP registration can go.

## Status

Phases 1–4 are built and released in this repository; §6 is the pull-mode
socket, which is still one per connection — see the note there.

## Phases

1. **session split + names.** `as` required at connect, `connection`
   required on every connection-bound tool. Still one connection at a
   time, so the rule is in force before anything depends on it.
2. **N concurrent.** Name persistence in connstore; `hub_list_connections`
   reports them; per-session redial and auto-reconnect notices say which
   connection they are about.
3. **Labelling.** Push shape, cursor trailer, `#hub conn=`, cross-connection
   confirm refusal.
4. **Shared budget + serialized catch-up.**
5. **Multiplexed wait socket.**
6. **Retire the `mcp-hub2` registration; docs.**

Phases 1–3 are what make it usable; 4 is what makes it safe under load.

## Decided

- Each connection is named at connect and addressed by that name
  afterwards, including when only one is open.
- A name already in use is **refused** at connect — never silently
  reattached to the existing connection.
- Maximum **8** simultaneous connections.
- **One shared** delivery budget across all connections.
- **One** push catch-up walk at a time.
- A drop affecting several connections produces **one combined notice**,
  not one per connection.
- The `mcp-hub2` MCP registration is retired at the end.
- A connection presents itself as `mcp:<connection name>` — one process,
  one socket per connection, see §4.
- No registry row, so connections stay out of `ListAgents`, unless a case
  turns up where delivery needs one.

Parameter spelling is left to implementation.

## Open

Nothing. Ready to build.
