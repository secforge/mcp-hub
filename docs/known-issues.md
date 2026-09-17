# Known issues

Bugs found while working on something else, recorded rather than fixed on
the spot. Each entry says what was observed, not what is suspected.

## Intermittent: `TestCatchUpWithNoPriorPositionAndNoBehindReportsNothingToCatchUp`

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
`TestCatchUpWithNoPriorPositionAndNoBehindReportsNothingToCatchUp` joined
them with the same shape: one failure inside a full-package run, then
clean on an isolated run and on repeated full-package runs.

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

## The self-update version is not signed

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

**The fix** is to bind the version into the signed material: sign a small
manifest carrying the tag plus the asset's digest, verify that, then match
the tag against it. Not done here because it changes the release process as
well as the client, and shipping half of it — a client that expects a
manifest against a release process that does not publish one — would break
updating entirely.
