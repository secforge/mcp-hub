# External review, 2026-09-19

A Codex session reviewed this repository over the hub on 2026-09-19, at
HEAD `cd63ef9` plus the then-uncommitted catch-up changes. It reported
fourteen findings and wrote its own reproductions for them. It changed no
source file here.

`REVIEW.md` is its report, verbatim. `reproductions/` holds the tests it
wrote, kept because they are the evidence: a description of a defect can
be argued with, a test that fails against the code cannot.

## Why these are `.go.txt` and not tests

Ten of the fourteen are fixed, and each fix has its own test in this
repository's own suite, in this repository's idiom. Importing a second
test for the same behaviour would mean two things to update whenever it
changes, and the one nobody remembers is the one that rots into a false
green.

The remaining four still fail, because the bugs are still there. A
permanently red suite teaches a reader to ignore red — the same habit
this project already documented as dangerous in `known-issues.md`. So
they are kept as text, runnable on purpose rather than by default.

## Running them

Copy the file for a package to `internal/<pkg>/review_test.go` and run:

    go test ./internal/httpmcp ./internal/hubconn ./internal/hubsession \
        ./internal/mcptools ./internal/wsserver -run '^TestReview' -count=1

Then delete the copies. One signature has moved since they were written:
`reconnectOnce` took a generation argument as part of fixing finding 1,
so the two calls in `mcptools.go.txt` need a trailing `, 0`.

## What passed, and what did not, as of 2026-09-19 22:10

Passing — fixed here, each with an equivalent test of our own:

- disconnect during an in-flight reconnect (1)
- partial reads claiming to be complete history (2)
- budget ledger operations losing connection ownership (6)
- confirming a cursor two connections share (7)
- the release tag not naming the built commit (9)
- the catch-up fixture's concurrent websocket writers (10)
- HTTP overflow deadlocking the whole session (11)
- session removal admitting a join into a detached object (12)
- operation errors permanently disabling reconnect (13)
- closed connections leaking activity goroutines (14)

Still failing — all in the bundled server, which the project owner put
out of scope on 2026-09-19 (`/source/chat-relay` is the supported
server):

- client identity headers ignored by the bundled server (3)
- an unreadable identity file silently overwritten on join (4)
- superseded HTTP peers still able to send as the replacement (5)
- HTTP disconnect leaving waits and watch streams blocked (8)

Finding 4 is the one worth revisiting first if that scope changes: it is
the same corruption-to-empty defect `connstore` already fixed, and it
destroys the evidence of what went wrong.

## Not covered by any of these tests

The report's own "additional source-level concerns" were traced in source
without reproductions. Four of them are in the CLIENT and so are not
covered by the scope decision above: no read limit on the client's
websocket, attachment size checked only after the whole file is read, the
local waiter accepting and writing without deadlines, and stale
attachment directories judged abandoned by age alone.

The fatal-error allowlist added for finding 13 lists six codes chosen
from what had been seen in practice, not from a protocol document. The
reviewer flagged that it still needs one.

## The second review, 2026-09-20

`DEEP-REVIEW-2026-09-20.md` is the same reviewer's deeper pass, run
against the released v3.1.4 plus the fixes in flight that morning. Ten
findings; all ten are fixed.

**The report's own text lags its author's later messages, and the file
is kept verbatim rather than edited to agree with them.** It was written
while the fixes were still landing, so:

- Finding 4 (stale reconnect bookkeeping) reads "remains open". It was
  closed afterwards by making the store write itself the ownership check
  — `persistConnectedIfStillHolding` — rather than guarding a
  compensating write. The reviewer confirmed both interleavings pass
  together.
- Findings 1 and 2 (late-ack attribution, cross-operation errors) are
  described as needing a protocol change with "no mitigation
  implementation was made in this review". The mitigation WAS
  implemented here afterwards: a timed-out request records an owed
  answer which routing then spends, and an uncorrelated error with more
  than one claim pending is delivered to nobody. The reviewer re-ran its
  own reproductions against that and reported both passing.
- Finding 3 (a refusal becoming a confirmation) was raised from P2 to P1
  after the report was attached, because the refused cursor was being
  persisted.

What the report says about its LIMITS still stands and is the reason it
is kept: no Codex push session and no Windows runtime were exercised,
its scheduling seam exists only in its own snapshot, and the server-side
correlation id it recommends is not authorised by a review.

### On the archived copy of the second report

`DEEP-REVIEW-2026-09-20.md` is revision 14 of a document its author
revised roughly every minute while three sessions corrected each other
on the hub. Kept verbatim, as the first report is.

Its account of the cross-session push test took eighteen revisions to
settle, and one clause in it — whether either confirm route was
isolated — oscillated six times across four authors, three of those
with the correction already in hand. That oscillation is recorded in
the known-issues entry as a finding in its own right: a record whose
shape regenerates the same error in whoever writes the next sentence.

The revision kept here is the one that resolved it. It now separates
the frozen 46740→46753 push-receipt window from the later
route-isolation intervals, marks the piggyback bracket recovered rather
than recorded, and carries the quiet-channel asymmetry. It agrees with
the known-issues entry. The
settled version is in `docs/known-issues.md` under the Codex push
entry, which records the frozen window, both isolation brackets, which
of them was luck, and the sentence four separate authors got wrong.

Where the two disagree, the known-issues entry is the one that was
argued to a conclusion. Where they agree, neither adds anything to the
other.
