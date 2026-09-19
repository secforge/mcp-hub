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
