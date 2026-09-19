# Known issues

Bugs found while working on something else, recorded rather than fixed on
the spot. Each entry says what was observed, not what is suspected.

## Intermittent: `TestCatchUpWithNoPriorPositionAndAZeroBehindReportsNothingToCatchUp`

(Renamed 2026-09-19, when the absent-versus-zero distinction below split
it in two: this entry is about the flakiness, not about that fix.)

**Observed** 2026-09-15, twice, both times in a full `go test ./...` run of
`internal/mcptools`:

```
teams_relay_test.go:1139: expected a nothing-to-catch-up result, got:
catch-up request timed out waiting for the server — call hub_catch_up again to retry
```

**Does not reproduce in isolation.** After the second failure: 5 runs of the
test alone and 3 full runs of the package, all clean. Earlier in the same
session the same test failed once and 8 subsequent runs (4 on the working
tree, 4 at HEAD) were clean.

**What that narrows it to.** The failure is a timeout, and it appears only
when the whole package runs — so the likely cause is the test's own
server-response deadline being missed under the load of the rest of the
suite, not a defect in catch-up itself. That is a hypothesis; nothing here
has been instrumented to confirm it.

**Why it matters anyway.** A test that fails only under load and passes on
every retry trains a reader to re-run rather than to look, which is exactly
the habit that makes a real intermittent failure invisible. Three other tests
(`TestSendWithAttachmentsDeliversThem`,
`TestHubSendWithoutConfirmCursorDoesNotConfirm`,
`TestCatchUpGapDedupBranchPrunesHandedOverAhead`) each failed once the same
way and likewise never reproduced — the last on 2026-09-16, clean on three
isolated runs and three full-package runs immediately after.
`TestCatchUpWithNoPriorPositionAndAZeroBehindReportsNothingToCatchUp` joined
them with the same shape: one failure inside a full-package run, then
clean on an isolated run and on repeated full-package runs.

`TestCatchUpResultStatesWhereItStartedAndWhatItDecided` joined the family
on 2026-09-19: one failure inside a full `-race` run, taking 5.09s, then
three isolated runs and three further full `-race` runs all clean. Filed
here only after those six, and with the same caveat as the rest — the
shape (a timeout under load, never reproducible) is what puts it here,
not a diagnosis.

`TestAnnouncedRestartKeepsTheFollowerAlive` was filed here too and did not
belong: it was failing for a stated reason (it took the first pending
note, which had become the new drop notice rather than the reconnect
report) and is fixed. Worth recording because filing it here delayed
reading the message it had been printing all along.

**Not investigated further** because it was found while building something
unrelated. The next step would be to raise or parameterise the deadline
that produced the message above and see whether the failures stop.

## The push path's tail sentinel is not pinned by a test

**What it is.** `FormatEventForPush` deliberately emits no `[end cursor=…]`
marker: the deliver library appends its own `[cursor: …]` line after the
body, and a tail sentinel works by being *last*, so two of them detect
nothing one does not. Two readers on two different delivery paths confirmed
seeing the same value on adjacent lines before this was changed.

**The gap.** That makes a safety property — the only way a reader can tell a
pushed message was cut — depend on another package's formatting, with
nothing failing loudly if it changes. `deliver.compose` is unexported, so it
cannot be asserted directly; the test has to drive a real delivery through a
unix socket and inspect the bytes that arrive.

**Until then**, an upstream change to that line would silently remove the
sentinel from every pushed message, and the connect guidance would be
describing a marker that never arrives — which reads as every message being
truncated. If that happens, mcp-hub must render its own end marker again;
adjusting the guidance instead would be treating the symptom.

**Worse than the gap itself**, for a while: `format.go` claimed a test named
`TestTheLibraryStillAppendsTheCursorAsTheLastThing` pinned this. No such
test existed — it was written, failed to compile because `compose` is
unexported, and was deleted while the comment that named it stayed. A
comment that states a risk correctly and names an absent mitigation is worse
than silence, because a reader who checks the reasoning finds it sound and
stops. Found by harness-transport, 2026-09-16.

**The property is pinned upstream**, by the library's own
`TestComposeAlwaysEndsWithATrailer`, across every Cursor/More/Body
combination. So a break fails their suite immediately and reaches this one
only at a dependency bump. Doubling it here is still worth doing, and needs
either an exported `Compose` or an end-to-end test through a real socket.

**Assert the shape, not the prefix.** There are two trailer forms since
v0.1.1: `[cursor: …]` and `[no cursor: this message cannot be re-fetched]`.
A test matching `[cursor:` would pass today and fail on the first
client-authored notice.

## Codex push is built but has never run against a Codex session

**What is built.** A Codex caller is push-only, like a Claude one: events
are delivered as they arrive, `hub_wait` and `hub_receive` are not
offered, and no wait socket is created. The switch happens on that
caller's FIRST tool call, because tools are registered before any client
has identified itself — the tool set is re-registered at that point so no
description still names a tool that has just been removed.

It is gated on the harness being reachable, not on the client's name
alone. A name is a claim; unregistering the pull tools for a caller this
process cannot push to would leave it no way to receive anything, which
is the failure this exists to prevent rather than cause. Where a Codex
caller appears with no reachable harness, the blocking loop and its
instructions stay exactly as they were.

**What has not happened.** None of it has been exercised against a real
Codex session. The thread-id latching from `_meta.threadId`, the receipt
handling that distinguishes `ObservedStored`/`ObservedAccepted` from
`ObservedNothing`, and the mode switch are verified only by this
repository's own tests and the library's contract.

**What to check first when it is tried.** Whether the first push lands at
all (the target is latched from the connect call's own metadata, so the
connect result already says which guidance it chose); whether the tool
list a Codex client sees actually loses `hub_wait` after its first call;
and whether the persisted reading position advances on a Codex receipt,
which it should and on Claude deliberately does not.

## The self-update version is not signed — FIXED 2026-09-19

`Apply` compares `rel.TagName` against the running version before
downloading, and the comment there used to present that ordering as what
prevents a downgrade. It does not: the tag comes from the same JSON as the
asset URL and nothing signs it. Whoever can shape that response serves tag
`v99.0.0` alongside the genuine, genuinely signed `v0.0.1` binary and its
genuine `.sig` — verification passes because the bytes really were
published, the comparison passes because the tag says 99, and a
known-buggy old binary installs over a good one.

Found by chat-relay, 2026-09-15.

**What prevents it today** is TLS to api.github.com. That is a reasonable
control, and it is a different one from the reasoning the code claimed.

**The fix, now implemented.** Each release publishes
`mcp-hub-release-manifest.json` and its detached signature, carrying the
version, the commit, and every binary's sha256, signed with the same
release key. The client verifies that signature first, takes the version
from inside it, requires the release tag to agree as a cross-check
(never as the authority), compares the SIGNED version against what is
running, and then requires the downloaded binary's sha256 to match what
the manifest is signed for. A replayed old release wearing a new tag is
refused on the disagreement; replayed honestly, it is refused by the
ordering.

The per-binary `.sig` files are still published, for anyone verifying a
download by hand. The client no longer reads them: one signature over a
statement about every artifact is a stronger claim than one signature
per artifact with nothing tying them to a release.

Shipping half of it would have broken updating, so both halves landed
together: `scripts/release.sh` builds and signs the manifest through
`internal/selfupdate/cmd/manifest` — the same code the verifier parses,
so the two cannot drift on ordering, indentation or the absent trailing
newline that the signature covers.

A release published BEFORE this (v3.1.3 and earlier) has no manifest,
and a client carrying this change will refuse to install from one,
saying so. That is the intended direction: the first release with a
manifest is installable from any older client, because the check lives
in the new client rather than in the old one.

## Four bundled-server defects, left unfixed by scope

Found by an external Codex review, 2026-09-19 (`docs/review-2026-09-19/`,
which keeps the report and the reviewer's own failing tests). The project
owner stated the same day that `/source/chat-relay` is the only supported
server and the bundled one is out of scope, so these are recorded rather
than fixed. They are real: each has a test that fails against this code.

- **An unreadable identity file is silently overwritten.**
  `identitystore.Load` treats malformed JSON and read errors alike as an
  empty map, and the next join writes that map back over the file. This
  is the same corruption-to-empty defect `connstore` already fixed, and
  it destroys the evidence of what went wrong along with the identity
  mappings. Worth doing first if the scope changes.
- **Superseded HTTP peers can still send as the replacement identity.**
  Supersede only appends an error to the old peer; nothing checks that a
  sender is still the registered peer object.
- **HTTP disconnect leaves waits and watch streams blocked.** Removing
  the token and membership neither marks the old peer closed nor wakes
  anything waiting on it.
- **The client's identity headers are ignored by the bundled server.**
  The client sends `Agent-Name`/`Agent-Secret`/`Hub-Protocol-Version`;
  the bundled server reads query parameters. Against it, a name is lost
  and a reconnect with the same secret gets a new peer id.

The review also lists source-level concerns in the same server that have
no reproduction: HTTP peer and session read under separate locks, and
`broadcastRoster` publishing outside the lock it built the snapshot
under — the second is a real hazard for state-not-deltas, since two
publications can reorder and leave the older one winning.

## The fatal-error allowlist (settled 2026-09-19)

A coded error with `retryable=false` used to end a connection for good,
so a `bad_request` about one send disabled automatic reconnect for a
later unrelated drop. That is fixed: only an allowlist of
connection-level codes is fatal now
(`conversation_unavailable`, `conversation_deleted`, `unauthorized`,
`forbidden`, `revoked`, `expired`).

Those six were chosen from codes seen in practice, and chat-relay
answered from its own source the same day: on that server an error frame
NEVER means the connection is over. Every code it emits — unavailable,
send_refused, not_found, bad_request, bad_attachment, bad_ack,
bad_ack_cursor, bad_anchor, bad_filter, too_large, unsupported — refuses
one request and leaves the connection usable. Four of the six guessed
names are codes it does not send at all.

The allowlist is now `conversation_unavailable` alone, kept because the
frame carries the reason where the close (4003) carries the verdict, and
a reader that missed the close still learns why.

The authority is the CLOSE code: 4001 revoked, 4002 expired, 4003
conversation unavailable and 4004 superseded are not retried; everything
else is, 1006 included, because chat-relay's overflow and write-timeout
paths abort the transport rather than sending a code — a close frame is
itself a write, and the condition being signalled is that writes do not
complete.

4002 is a constant on chat-relay that nothing issues today — its expiry
path closes with 4001. Not retrying it is right whoever sends it, but a
4002 in the wild means something new shipped rather than anything about
the server as it stands.

4004 began as a deliberate departure from chat-relay's recommendation,
which was to reconnect on it; asked to push back, its author withdrew
the recommendation. An identity there is single-holder by design, so
automatic reclaim makes a single-holder resource contended, and
resolving contention by retry is a livelock rather than a recovery:
two clients with the same secret hand the identity back and forth for
ever, each one's recovery being the other's failure, and neither ever
wrong locally. Reconnecting means presenting the same
secret again, which takes the identity back from whoever just claimed
it; if that holder also reconnects automatically — and if it is this
same client, it does — the two take turns forever. Displacing this
client's OWN socket is unaffected, since the replacement is already
connected and nothing schedules anything.

**What is not possible today**, confirmed by chat-relay's author: nothing
on the wire distinguishes a foreign takeover from this client displacing
itself. The 4004 reason says another connection reconnected with this
identity, which is all the server knows — it authenticated a verified
secret, and a secret does not carry who holds it.

The mechanism they proposed was a client-supplied `Agent-Instance` on
the handshake, opaque to the server, echoed back in the supersede close
reason: a displaced connection compares it with its own — equal means it
displaced itself and may reconnect freely, different means a foreign
holder and the refusal applies automatically.

chat-relay relayed on 2026-09-19 that its owner declined it, and said
not to design around it arriving later. So the policy above is the
answer rather than a stopgap: nothing will distinguish the two cases on
the wire, self-displacement needs no signal because the replacement is
already connected, and the foreign case stops and tells a human.

(Recorded as chat-relay's report about its own server. It is not a
decision taken here, and if mcp-hub ever wants an instance identifier
for its own reasons that is still open.)

## The wire is read case-sensitively (settled 2026-09-19)

chat-relay found, 2026-09-19, that its server had serialised attachment
references `PascalCase` on the live path and `camelCase` on the history
path for as long as hub attachments had existed. Nothing failed, and
this client could not have reported it, because Go's `encoding/json`
matches field names case-insensitively: it read both spellings happily.

A tolerant reader does not fix a wrong field name — it hides it until
something stricter disagrees about what was sent. Their owner's rule
from this is that nothing is read case-insensitively, applied on their
side already.

APPLIED. The claim first recorded here — that Go offers no flag for it —
was wrong, and was corrected the same day: Go 1.25 ships
`encoding/json/v2`, where case-sensitive matching is the DEFAULT
(`MatchCaseInsensitiveNames` is the opt-in), and v1 honours a per-field
`case:strict` tag once the binary is built with `GOEXPERIMENT=jsonv2`.
Measured, not assumed: with the experiment off, `{"Token":…}` fills a
field tagged `token`; with it on, it does not.

Every field in `internal/wire` now carries `case:strict`. Unknown fields
stay ALLOWED on purpose — a server adding a name an older client has
never heard of is how this protocol grows, which is exactly how `size`
arrived — so `DisallowUnknownFields` is deliberately NOT used; it would
trade a spelling bug for a compatibility one.

**The build setting is the fragile part.** Without `GOEXPERIMENT=jsonv2`
the tags are parsed and ignored, so a binary would read tolerantly while
the suite asserted it does not. `scripts/release.sh` exports it (and
runs the suite under it before publishing), the Dockerfile sets it, and
`internal/wire`'s `TestTheWireIsReadCaseSensitively` FAILS rather than
skips when it is absent — so a plain `go test ./...` is meant to fail.
Run `GOEXPERIMENT=jsonv2 go test ./...`.
