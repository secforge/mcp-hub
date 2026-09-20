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

**What that narrowed it to, and why that was wrong.** The failure is a
timeout, it appears only when the whole package runs, and the hypothesis
recorded here for four days was the test's own server-response deadline
being missed under the load of the rest of the suite. It said plainly
that nothing had been instrumented to confirm it. Nothing ever was, and
it was at best half right.

**The mechanism, found by reading rather than by timing.** A connstore
Target is {link, project}, and a test link embeds an httptest server's
address. Those ports are RECYCLED. Once a server closes, a later test can
be handed the same port and compute a Target byte-for-byte identical to
an earlier test's, inheriting whatever that test persisted under it —
most damagingly a stored cursor, which several of these tests write as
"cursor-confirmed". No load is required for that. Load only changes the
timing that makes reuse likely, which is why the family looked like a
load problem and why every isolated re-run passed.

The file already knew the hazard in one place:
`TestCatchUpGapWalksThenClearsOnReachingTo` calls `clearCatchUpGap` first
and says why. The defence had been applied to gaps and not to the cursor.

Found by the external reviewer on 2026-09-20, from two consecutive full
runs failing on different tests, both complaining that a stored position
existed where none should.

**The fix**, and what it is not. Every test link now carries a unique
conversation id (`uniqueConvID`), so the Target is unique whatever the
port does; it goes in the query rather than the fragment because the
fragment is the link's secret and a test asserts its exact value. Four
consecutive full-module runs afterwards were clean, against the
reviewer's two failures in four immediately before.

Four clean runs is evidence and not proof, and the reviewer was explicit
that several members of this family failed as bare timeouts with no
cursor involved — those are not explained by port reuse and may still be
the load hypothesis, or may be something else again. The entry stays
open.

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

`TestCatchUpGapRetrievesPeerlessSystemMsg` failed once on 2026-09-20 in a
full `-race` run and never again in ten further runs (six of the package
in isolation, four of the whole package). Its failure text was NOT
captured before it stopped reproducing, so this entry records a failure
without a reason — which is the weakest kind of entry here and is said
plainly rather than dressed up as a diagnosis.

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

**The CLAUDE half is now measured, end to end.** On 2026-09-20 a
push-mode Claude Code peer on chat-relay's hub stated its confirmed
position and its predictions in advance, took six pushed messages
without confirming, reconnected, and caught up. It resumed from
639254998744309000.46740 — its last CONFIRMED cursor, below the last
pushed one (…46746) — and re-walked ten messages, including all six of
the unconfirmed set. chat-relay's server-side ACKED_CURSOR read the
same 46740, and its log shows the successive messageAfter requests
doing the walk.

THE MEASURED WINDOW, frozen before the outcome was known, is two
readings and nothing else:

	46740  unmoved, after six unconfirmed pushes and a process restart
	46753  moved, on a confirm carried as confirmCursor on a send,
	       naming exactly that cursor

Everything after 46753 on that peer is housekeeping and is not part of
the result. Quoting a later value would undo the freezing, which is the
thing that made this a measurement rather than a story about one.

BOTH DIRECTIONS were witnessed on the same peer inside twenty minutes,
which is what makes it a measurement: six unconfirmed pushes and a
process restart left the column at 46740, and one confirm then moved it
to 639254999518371000.46753. A position that never advances is
indistinguishable from one that CANNOT, so the second half is not a
formality.

BOTH ROUTES to that column are attested. The peer's confirm history, as
it posted it: one standalone hub_confirm to 46738, then four sends
carrying confirmCursor — the second of which set the 46740 this test was
measured against — then two further standalones after the test closed.
So the tool's own "or piggyback it on your next send" is a witnessed
claim rather than an assumption, and the standalone path is not untested
either.

ISOLATION, which is a stronger claim than attestation and was argued
over:

	standalone  ISOLATED. Read 46781 at 11:23:07Z, standalone confirms
	            and nothing else from that peer, read 46787 at
	            11:24:15Z — exactly the cursor named.
	piggyback   ISOLATED, on a bracket recovered rather than recorded.
	            Reading at 11:19:21.6604 showing 46740; one send carrying
	            confirmCursor 46753; reading POSTED at 11:20:06 showing
	            46753, with no other confirm from that peer until after
	            that post. The right edge's actual moment is unrecorded
	            and no claim is made about it.

	            What closes that bracket is not its narrowness but that
	            nothing else touched the column inside it — ordinary
	            traffic does not write this column, only an ack frame
	            does, so only another CONFIRM would have spoiled it.
	            Established by enumerating what was ABSENT rather than by
	            timing what was present.

ONE OF THOSE TWO BRACKETS WAS LUCK, and that is the point worth keeping.
The standalone one is clean because the server's author had by then
started stating the moment each reading was taken. The piggyback one had
to be reconstructed afterwards, and only worked because the traffic was
sparse enough to enumerate — on a busy connection it would not have been
recoverable at all. That is the argument for a receipt log carrying the
cursor AND the moment, made against this morning's own posts rather than
against a hypothetical.

An earlier version of this entry wrote that bracket's right edge as
11:19:39.4272. That figure is a LAST_SEEN value — an activity timestamp
— not the moment of the read, and using it as one is the same
column-confusion the same people had diagnosed hours earlier. Corrected
here, and recorded, because it happened to the person who had diagnosed
it.

THE FROZEN WINDOW ISOLATES NOTHING ABOUT CONFIRM ROUTES. Both routes had
been used before it opened; its job was the push receipt. It does
exclude one thing the brackets cannot: a spontaneous ack writer. If a
push emitted a receipt, the 46740 reading could not exist.

The recurring error worth recording is not any of those: it is that
"no standalone confirm occurred" was written independently by four
authors — the server's author, this file, and the reviewer — and
corrected from the same source each time. None had the history in front
of them; each had a summary of it. One mistake that the summary form
keeps producing, because "no standalone" is what the measured window
looks like from outside, and the window was never where the standalone
lived.

THE FOURTH INSTANCE IS THE INSTRUCTIVE ONE and it is this file. Having
watched the other three be corrected, and while actively correcting one
of them, this entry stated that the standalone established 46740 — right
about the standalone existing, wrong about what it did, and wrong in a
way nobody reading the channel could have caught, because only that
peer's own tool history distinguishes 46738 from 46740. The summary form
places the standalone and the 46740 adjacent, and adjacency reads as
causation once the interval between them is gone.

IT WENT ON. The clause reappeared six times in all, from four authors,
the last three with the correction in hand and paste-ready wording
already offered. Not stale summaries — the corrections were held at the
time of writing. The summary form does not merely transmit this error,
it regenerates it in whoever writes the next sentence.

The server's author stopped correcting it at the fourth attempt, on the
grounds that the oscillation had become more interesting than the
sentence, and that "attested but not isolated" is at least wrong in the
safe direction: it understates the evidence rather than inventing any.

So the finding is not that five people were careless. It is that the
record's SHAPE produced the same error in whoever handled the summary,
including the one whose job at that moment was to remove it. The fix is
the same as the logging item: a line carrying the cursor AND the moment,
so the interval cannot be lost in the first place. A record that cannot
be read against the order things happened in gets written up as whatever
the tidiest available sentence says.

It was nearly filed as a defect: a server read taken moments BEFORE the
confirm landed showed the old position while the client had already
reported success, and for one message the two were indistinguishable
from a client reporting a success the column never took. The runner
asked instead of assuming. Worth recording as the cheaper of the two
mistakes available.

THAT HAPPENED THREE TIMES and each reading was both correct and behind.
A read of a server column and a write to it from another process are not
ordered by the conversation discussing them, and none of the messages
carried a timestamp that would let anyone order them afterwards. It was
benign three times, which is the reason it will eventually be believed
once too often: anything comparing a client's stated position against a
server's column has to establish which came first, or it is comparing
two facts from different moments.

A detail the runner insisted on recording rather than letting stand:
chat-relay posted that column as a baseline "taken before anything is
pushed", and it was not — by then six messages had been pushed and the
reconnect had happened. That makes it better evidence than claimed
(an after-the-fact reading showing no advance, taken by someone who did
not yet know it would be used that way), and it is noted here because a
mislabelled measurement quietly corrected later is worth less than an
accurate one. So a Claude push receipt really is ObservedNothing
across the process boundary, not only at the gate this repository's own
tests drive.

Two limits stated by the people who ran it. It says nothing about the
CODEX receipt, which is this entry's subject. And chat-relay noted that
its log records the walk line by line and the receipt not at all — had
the column moved, it could have reported THAT it moved with nothing
saying why, which is a gap in the instrument rather than a finding about
the client.

**What has not happened.** None of the CODEX path has been exercised
against a real Codex session. One connected to this hub on 2026-09-20
and was in PULL mode: hub_wait and hub_receive were still in its tool
list, because the switch is gated on the harness being reachable and
that session had no harness messaging socket. That is the guard working
— unregistering the pull tools for a caller this process cannot push to
would leave it unable to receive at all — and it is why watching a Codex
peer receive messages proves nothing here: its harness relays the
conversation whether or not this client pushes anything. The thread-id latching from `_meta.threadId`, the receipt
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

## Late acks and uncorrelated errors: mitigated, id deferred

A timed-out request's answer used to be handed to whoever asked next —
send A times out, send B asks, and A's late ack tells B it succeeded,
with A's externalId. An uncorrelated error with several claims pending
went to whichever the map yielded first, so a send's refusal could be
reported as a history read's failure.

MITIGATED, 2026-09-20, without touching the wire — and the first
version of that mitigation carried a defect worse than the bug. The debt
a timeout recorded was only spent while a claim was waiting, so a late
answer arriving with nobody waiting left it standing: the next request's
own prompt answer paid it, that caller timed out, and its timeout
recorded the debt again. One timeout disabled acks of that kind for the
life of the connection. Found by an external reviewer with a
reproduction, hours after the mitigation was written, and fixed by
settling the debt on any arrival of that kind.

The mitigation itself: a timed-out request
records one owed answer of its kind, and routing spends the late answer
against that record rather than the next caller; an error arriving while
more than one claim is pending is delivered to nobody and falls through
to the buffer. Unattributed rather than misattributed, which is the
honest half of a trade the protocol cannot currently settle: an ack
carries externalId and ok, an error carries a code, and neither echoes
anything the request chose.

NOT FIXED, and deliberately so. chat-relay's owner deferred the
correlation id with a named trigger rather than refusing it: build it
the first time a send or a refusal is actually seen reported as a
timeout in real use, or the next time the wire is touched for another
reason. Both findings came from constructed tests with scheduling hooks
and neither has been observed in traffic, and everything else in this
protocol was built after something was measured.

The shape, agreed and recorded on both sides: optional, client-chosen,
opaque, echoed verbatim on ack AND error, declared in `features` —
because a server that does not echo it looks exactly like one that does
not implement it. externalId is explicitly not a substitute: it names a
stored message and is absent precisely when a send failed.

WHAT TO WATCH FOR, since it is the trigger: with two operations pending,
a real send_refused now reaches nobody, so a tenant-lock or allowlist
refusal reads to a model as silence and its next move is a retry that
will be refused identically. Seeing that in real traffic is the signal
to build the id.

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

## A plain `go test ./...` is red here, by design

`internal/wire`'s `TestTheWireIsReadCaseSensitively` FAILS without
`GOEXPERIMENT=jsonv2`, deliberately: every wire field is tagged
`case:strict`, Go honours that tag only under the experiment, and a
build without it reads the wire case-insensitively while the suite would
otherwise claim it does not. The test fails rather than skips so that a
binary which quietly lost the strictness cannot pass.

The cost is that the first thing anyone does with a fresh clone — run
the tests — goes red, and the failure message is the only thing that
says why. Run:

	GOEXPERIMENT=jsonv2 go test ./...

Recorded here as well as in the README because an external reviewer ran
into it, 2026-09-20, and reasonably expected the entry to mention it.

## Several sessions can share one working tree

Not a defect in this code, and worth knowing before committing from it.

On 2026-09-20 three agent sessions worked in `/source/mcp-hub` at once.
Two of them described themselves as being in `/source/mcp-hub/y` and
`/source/mcp-hub/x`, which sound like separate checkouts and are not:

	git worktree list  →  /source/mcp-hub   (one entry)
	x, y               →  plain directories inside it

So `git status` is a SHARED surface. `git add -A` from any session
sweeps in whatever the others have left mid-edit, and the failure is
silent — the commit succeeds, and it looks like yours. Use explicit
paths (`git add internal/`, `git add docs/`) and read `git status`
as everyone's work rather than your own.

SETTLED the same day by the project owner: one session writes to this
checkout and the others touch nothing in it — no edits, no new files, no
scratch files, no deletions, whether or not git ever sees them. A
session that needs to write anything makes a real worktree, a separate
clone, or a scratch directory outside the checkout. A plain directory
inside it, as `x/` and `y/` are, is none of those: it shares this index
completely and is the arrangement being retired.

Recorded as a near-miss rather than an incident: the commits of
2026-09-20 used explicit paths out of habit, before anyone knew the tree
was shared. `scripts/release.sh` is the other guard — it refuses a dirty
tree outright, so another session's uncommitted work blocks a release
rather than being published in one.

## A peerId names an identity, not an agent

Also 2026-09-20, and the reason entries here name a session and a date
rather than a peer id alone.

A peerId is resumed from the stored reconnect secret for a link. It is a
stable name for WHOEVER HOLDS THAT SECRET, which is not the same as the
agent behind it: that day, the id `f54fd3f8…` was a Codex session all
morning — the one whose pull-mode answers are the evidence in the Codex
push entry above — and then presented as a Claude Code session in the
afternoon, same id, same link — then as `reviewer` twenty minutes after
that. Three names, one id, and nothing in the roster history or the
transcript distinguishes one agent renaming itself from three sessions
sharing a secret. The peer that could have said which left mid-question.

The observations stay true; they were correct when made and the entry
dates them. What is not true is that the id identifies the agent. The
server has the same exposure in reverse: its rows and its "X joined"
lines carry a peerId, and the display name beside it is whatever the
client sent at that moment, so "who was in the room" is answerable as
which IDENTITY and never as which agent.

Neither side treats this as a defect and neither intends to change it —
it is what resuming an identity from a secret means.

## A handed-over position is piggybacked as if it were a confirmed one

Open, live, and this client's bug. Found 2026-09-20 by the external
reviewer, who hit it by following this client's own instruction.

`lastConsumed` means HANDED OVER TO THE MODEL. A confirm means THE MODEL
HAS IT COMPLETE. Those are two different facts, and `ackCursorForOutbound`
piggybacks the first into the wire column that holds the second, on every
outbound message.

The confirm reminder this client generates then asks the reader to
confirm the last message it has complete, "possibly earlier than" the
last one delivered, and not to confirm past anything that was cut. A
reader that obeys is refused: messages 1..5 are delivered and consumed, a
send has already piggybacked 5, message 4 arrived truncated, the reader
honestly confirms 3 — and chat-relay's column is monotonic, so the
standalone ack naming 3 comes back `ok:false` and the persisted position
stays where it was. The reader is told its read position has not moved,
for doing exactly what this client asked of it.

The honest confirm looks like the error because the client already
asserted, on the reader's behalf, something the reader had not decided.

**chat-relay was asked NOT to special-case it** (2026-09-20, agreed by
its author): an accommodation would make a client asserting what it does
not know survivable, and the refusal is the only pressure toward the real
fix. The failure is loud, which is why this is recorded rather than
rushed.

The fix is on this side and wants its own decision: the piggyback should
carry the last CONFIRMED position, and `lastConsumed` should stop being
an ack source at all. That is a behaviour change, not a repair, so it is
not being made under the heading of a bug fix.

Until then: a reader following the reminder on a monotonic server gets a
refusal. `ConfirmReceived` reports it truthfully and rolls back, so
nothing is corrupted — the confirm simply does not take.

## A correction that deletes the evidence

2026-09-20. Drafted by the external reviewer, who owns half of it, and
kept here because the other half is this repository's.

Twice in one afternoon, in opposite directions, around one fact.

**An absence read as a zero.** chat-relay grepped its own code for
`receipt|recorded an ack|ack from`, found nothing, and reported receipt
logging as missing. The log line says "confirmed". The feature had
shipped on 2026-09-16 and had been agreed with both other parties at the
time. No matches was read as not there; the result cannot distinguish
that from not matched, and nothing in it says which. Three of us built on
the report for three hours, and it was first on this project's list of
what chat-relay still owed.

**A correction that overshoots.** chat-relay then retracted a second
claim — that its acked cursor was last-write-wins. The column is
monotonic and always was. Two comments in this tree had cited the
retracted claim to justify a client rule, and the reviewer who had relied
on it proposed replacements removing the server sentence entirely. That
deletes the sharpest fact in the whole exchange: against a monotonic
server a regressed receipt is dropped WITHOUT A WORD, so this client
believes a position acknowledged that never was and nothing on the wire
ever says otherwise. A reader who meets only "do not offer a position you
cannot justify" files it as hygiene. A reader who meets the silence
understands why it is mandatory.

**Why they are one entry.** One reads an absence as a zero; the other
answers a bad inference by removing what it was drawn from. Both end with
a reader holding less than the code contains. The second is not a milder
form of the first and is not caught by being careful about the first —
the reviewer who wrote the replacement had today's false-absence lesson
fully in hand, and it is what made the deletion feel like rigour.

**They are not one class, and the bridge between them was wrong.** This
entry briefly carried a unifying sentence — that both were answers about
evidence given without the evidence in hand — proposed by chat-relay and
withdrawn within the hour, because it is false of the second. The
reviewer had read all three sites before proposing the deletion; that
reading is how they caught that `conn.go:1422` was never changed and
corrected their own count from three sites to two in the same message.

So the remedies do not match, and that is the point of keeping both:

- The first has a procedural fix. Answering a question about the code
  from a REPORT about the code — a grep, a summary, another party's
  self-description, anything on a relay channel — is repaired by opening
  the file. It was one command away.
- The second has none. The file had been read. The fact was seen, and
  the wrong thing was done with it: a bad inference had been drawn from
  it, so the fact went out with the inference. Reading it again would
  have shown exactly what was already seen.

The second is the harder class and filing it under the first would hand
a reader a checklist that cannot reach it. The bridge was itself an
instance of the day — a tidy sentence asserting something about another
party's process in order to hold two true findings together.

**The rule.** Do not argue a client invariant FROM a server's behaviour;
it holds whatever any server does. Do not pretend not to know that
behaviour either. A corrected fact, attributed and subordinate, is not
the error. An uncorrected one, load-bearing at the top of the paragraph,
was. `fd9b201` is what that looks like in this tree, and the test comment
it touches keeps both the wrong reason and the correction for the same
reason this entry exists.

**One more thing the day argued for.** Of twenty-four findings across two
review rounds, the two that mattered most on the last day were in a FIX,
not in the original code — two paths added while repairing a defect that
reintroduced its exact shape. That is an argument for the second pass
over the first, and for reviewing repairs at least as closely as the
code they repair.

### A relay is not a repository

The same afternoon, one level up, and the reason the entry above ends
where it does.

Between 15:42:54 and 15:43:28 the connecting paragraph was proposed,
refuted, withdrawn by its author, committed here as the entry, corrected
here, and then flagged as uncorrected by two separate parties — each of
whom was accurate about a commit and stale about the repository. Four
messages crossed a push in ten minutes.

Nothing in a commit can show that. `267e2bf` recorded a channel message
as a fact about the code's history; it was true when sent and false
ninety seconds later, and the commit looks identical either way. This is
the same error as `fd9b201`'s two comments, which recorded another
party's description of its own server as fact — once about a server's
behaviour, once about the conversation itself.

The remedy is not "read the file", because the file was fine both times.
It is that **a fact sourced from a conversation should not be written
into the tree in the minute it arrives.** A commit is durable, a relay is
not, and there is no ordering between a message on a channel and a push
to a remote.

What settled every one of these was the same act: reading `main`. A
commit hash quoted on a channel is a report about the repository exactly
as a grep is a report about a file, and both were true when sent.

One of the two sent an urgent correction built from the cursor timeline
on the channel — every message in order, with timestamps — and named
afterwards why it felt like proof: **a sequence of message times is not a
sequence of repository states, and nothing in it says so.** The
timestamps are what made it look like evidence rather than like the
recollection it was. That is a worse trap than the grep, because a grep
at least returns nothing.

**And once the artefact did not exist at all.** The closing message on
that channel cited two commits: `f70d1b2`, which is real, and
`c7a1b5f`, which was invented — in the same clause that declined to
quote hashes on the grounds that a hash is a report about the
repository. The real one is `af81163`.

No remedy on this page reaches that. The fact was not stale and not
sourced from a conversation; it was fabricated, in the one format here
that looks self-verifying. A hash carries its own air of proof, which is
precisely why a wrong one passes unsquinted — catching it costs a fetch,
and nobody reading it had a reason to spend one.

The only rule that covers it is the blunt one: do not write an
identifier you have not just read. Not a shortened hash, not a line
number, not a version. They are the cheapest things in this document to
verify and the most expensive to be wrong about, because everything
downstream treats them as the thing itself rather than as a claim.

## Two attribution schemes, one live per connection

The late-answer debt (`expectLateAnswer`/`spendLateAnswerLocked`) and the
correlation id do the same job by different means, and only one of them
runs on any given connection.

Without `features.correlation` a claim is keyed by ACK KIND alone, so a
late answer to a request that gave up matches the next request of that
kind. The debt is what stops that: a timeout records an owed answer,
routing spends it, and a late answer becomes unattributed rather than
misattributed. The cost is real — a caller can time out on an answer that
was actually its predecessor's.

With the feature, the server echoes a client-chosen id on the answer and
on any error refusing it. A late answer then carries the id of the
request that gave up, matches no claim, and reaches the buffer as the
unsolicited event it is.

**They must not be layered.** Recorded on a correlating connection, the
debt would never be spendable — no late answer can match a claim, so
nothing would ever consume it — and a counter that only increments is a
ratchet whose trigger nobody can reach. So `expectLateAnswer` returns
immediately when `Conn.correlates` is set, and the gate lives in that one
function rather than at its nine call sites.

**DELETION TRIGGER.** The debt machinery, the "two claims pending, so
this error goes to nobody" rule, and the tests for both can be deleted
when no server this client supports is still without
`wire.FeatureCorrelation`. Until then every one of them is load-bearing
against exactly the servers that do not declare it. Written down because
the alternative is meeting dead-looking code in six months with no way to
tell whether it is reachable.

The feature is DECLARED rather than inferred for the reason everything
else on this page is about: a server that does not echo an id looks
identical to one that failed to echo it, and an id-less error from a
correlating server is the ordinary case (an unsolicited notice, or
`bad_correlation`, which cannot echo the value it is bounding). The code
distinguishes them, never the absence.

## A green assertion between two stacked defects

2026-09-20, on the correlation id, and the only entry here that is about
a test passing rather than failing.

Two defects sat on the same path. `decodeEvent` never carried the echoed
correlation id onto a `msg`, so `ev.CorrelationID` was empty for every
history answer. The divert then matched a history claim on the anchor
instead of the id, so a late answer went to whoever retried the same
question.

A reproduction was written for the second. A fix was proposed for the
second. The reproduction passed against the proposed fix — and it passed
for a reason that had nothing to do with the fix: with the id dropped at
decode, a "match by id" rule refused EVERY history answer, and the test
read that blanket refusal as the misattribution being cured.

The proposed fix would have hung every history walk against a
correlating server. The test that was supposed to establish it was what
concealed it.

**What makes this its own entry.** The usual advice — write a
reproduction, watch it fail, apply the fix, watch it pass — was followed
exactly and produced a wrong conclusion. Red-then-green establishes that
behaviour changed, never that it changed for the stated reason. Where two
defects lie on one path, the first can make the second's test pass by
disabling the path altogether, and nothing in the transition says so.

**What would have caught it**, and did, on the second attempt: asserting
BOTH directions. A rule that believes an id must be tested by an answer
that carries the right one AND by an answer that carries none — the
first alone passes a client that ignores ids, the second alone passes a
client that refuses everything. Two of today's fixes now carry that pair,
and the terminator test exists only because the negative case was asked
for.

Related in shape and not in cause: chat-relay's reflection test proved
its records COULD carry the id and said nothing about whether any call
site set one; four of seven acks did not. The capability was present, the
wiring absent, and a green suite spanned the gap.

## A declared capability with nothing wired to it

Three times on 2026-09-20, in different code, by different authors, with
a green suite over all three.

- `decodeEvent` carried the correlation id onto ten frame kinds and not
  onto `msg` — the one kind a history answer arrives as. The id was
  minted, sent and echoed back, then dropped before anything compared it.
- `Joined.AttachmentsFeature()` decodes `maxRawBytes` and `imagesOnly`.
  Its only callers were on this module's SERVER side. So a `.pdf` to a
  server that declared images-only was read, base64-encoded and pushed in
  full, and learned it was unwanted from a refusal that arrived after the
  transfer.
- On the server side, a reflection test proved every ack record COULD
  carry the id; four of seven call sites set none. The failure beside
  each of them did set it, so a pin reported its refusal correlated and
  its success uncorrelated.

**The shape.** A capability exists — a field on the wire, a helper that
decodes it, a struct that can hold it — and nothing calls it. It is not
dead code, so a linter is quiet. The type is used, so the compiler is
quiet. The tests exercise the helper, so coverage is quiet. Every
instrument reports a wired feature and the wire is not connected.

**What found all three: enumeration.** Every construction site of an
answering frame; every `writeJSON` call that awaits a reply; every decode
case against the list of kinds a server says it echoes. Not one was found
by reasoning about the design, and reasoning is what left them out to
begin with — the design was right in all three cases.

**The cheap standing check.** Where two sides agree a list, walk the list
in a test rather than trusting that each item was handled. This
repository now does that for the twelve kinds that echo a correlation
id; the entry exists because writing that test took ten minutes and
would have caught two of the three on the day they were written.

Related: a capability read from a DECLARATION is a promise, not a
delivery — see the correlation entry's rollout window, where suppressing
a mitigation because a server said it correlates left the client
unprotected on every kind that server had not yet echoed.
