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
the habit that makes a real intermittent failure invisible. Two other tests
(`TestSendWithAttachmentsDeliversThem`,
`TestHubSendWithoutConfirmCursorDoesNotConfirm`) each failed once the same
way and likewise never reproduced.

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

## Codex push mode is not built, but the code reads as though it were

`PushMode` keys on `CLAUDE_CODE_MESSAGING_SOCKET` alone, and
`pushToHarness` returns before draining unless it is true. Under a Codex
harness the socket is absent, so nothing is ever pushed — while `Adopt`
still runs on every request and still latches a thread id, since that is
the only way the library learns a Codex target. The thread-id plumbing and
the second-thread mismatch check are therefore reachable only on the
harness that does not need them.

Found by chat-relay, 2026-09-15, reviewing the push path.

**Why it was not simply switched on.** Keying `PushMode` on the pusher
being available would put Codex into push mode as a side effect, which
also unregisters `hub_wait` and `hub_receive` — and Codex cannot background
a process, so the blocking call is what it depends on. That is a feature
with a verification cost, not a one-line change, and it has never been
exercised against a Codex session.

**Until then** the honest statement is the one in `PushMode`'s doc comment:
Codex push is unbuilt. The failure this avoids is the one the codebase
keeps meeting — code that looks built and behaves unbuilt produces an
absence nobody can attribute.
