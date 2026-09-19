# mcp-hub review — 2026-09-19

Reviewed `/source/mcp-hub` at HEAD `cd63ef929323b652347603001acc44e0d5ffba43`, including the existing uncommitted catch-up changes. Other participants began implementing fixes during the review. No repository source files were changed by this reviewer.

The original reproduction snapshot is `/tmp/mcp-hub-review-vfNvvN`; a second snapshot, taken around 17:47 UTC, is `/tmp/mcp-hub-review-current-KXOaWM`. Each contains the added `review_test.go` files. Line references below describe the reviewed source; concurrent edits may shift them.

## Findings still reproduced in the second snapshot

1. **P1 — Disconnect does not cancel an in-flight reconnect.** `internal/mcptools/tools.go`, `reconnectLoop` / `reconnectOnce` / `handleDisconnect` (approximately 853, 925–980, 3030). Cancellation is checked before dialing, but not when the completed dial is installed. Hold the replacement handshake, disconnect the named session, then release the handshake: the removed session acquires a live connection and restores its reconnect link. The orphan can deliver after the user explicitly left and interfere with a new connection using the same identity. Revalidate a generation and session ownership after dialing, close stale results, and synchronize installation with disconnect. Reproduction: `TestReviewDisconnectCancelsInFlightReconnect`.

2. **P1 — Partial history failures are reported as complete history.** `internal/mcptools/tools.go`, `handleRead` (approximately 3282–3296, 3369–3376). After collecting at least one message, a timeout, write failure, or server error breaks the loop. The result then infers completeness from `len(events) < limit`, says “this is everything there was,” and omits the actual failure. A test returns one message followed by `error/unavailable` for a three-message request. Only an unfiltered `noMoreMessages` response establishes the end of history; retain and report other stop reasons. Reproduction: `TestReviewPartialReadMustNotClaimComplete`.

3. **P1 — Current client identity headers are ignored by the bundled server.** `internal/wsserver/server.go:103` onward versus `internal/hubconn/conn.go`, `Dial`. The client sends `Agent-Name`, `Agent-Secret`, and `Hub-Protocol-Version`; the server reads query parameters `name`, `reconnectSecret`, and `v`. A real loopback dial to the server's legacy UUID path loses the name and obtains a different peer ID on reconnect with the same secret. The client test helper constructs a compatible path but does not compensate for these headers. Agree and implement the handshake on both ends. Reproduction: `TestReviewCurrentClientPreservesIdentityAgainstOwnServer`.

4. **P1 — An unreadable server identity file is silently overwritten.** `internal/identitystore/identitystore.go:58`, `internal/hubsession/session.go`, `newSession` / `Join`. `Load` treats malformed JSON and read errors as an empty identity map. The next successful join writes that map plus its new identity over the existing file. A malformed `.secrets.json` was replaced without any refusal. This loses the prior identity mappings and the original recovery evidence. Distinguish missing from unreadable state, propagate the error, and preserve the file. Reproduction: `TestReviewCorruptIdentityStoreIsNotOverwrittenOnJoin`.

5. **P1 — Superseded HTTP peers remain able to send as the replacement identity.** `internal/httpmcp/peer.go`, `Close`; `internal/httpmcp/hub.go`; `internal/hubsession/session.go`, `Broadcast` / `DeliverTo`. The supersede operation only appends an error to the old HTTP peer. Its MCP hub retains the peer and session, and sending does not verify that the sender remains the registered peer object. Reclaiming the identity therefore does not terminate the old sender. This gap is already acknowledged in the source comments. Close the old membership and reject stale senders. Reproduction: `TestReviewSupersededHTTPPeerCannotSendAsReplacement`.

6. **P2 — Shared delivery-budget operations lose connection ownership.** `internal/hubconn/budget.go:239` and `:267`; `Conn.ChargeDelivered`. The release path checks owner, but `note` deduplicates only by cursor and `adjust` updates only by cursor. Two servers may issue the same opaque value. Tests show one connection's pull marker disappearing, preventing release of its confirmed prefix, and its delivery adjustment charging the other connection. Key all ledger operations by owner plus cursor. Reproductions: `TestReviewBudgetNotesAreScopedToOwner`, `TestReviewBudgetAdjustIsScopedToOwner`.

7. **P2 — Valid confirmations fail when two connections share a cursor value.** `internal/mcptools/session.go:530`. `cursorBelongsElsewhere` checks other connections without first checking `mine.hasDelivered(cursor)`. When both delivered the same value, either connection is accused of using the other's cursor. Check local delivery first. Reproduction: `TestReviewSameCursorOnTwoConnectionsCanBeConfirmed`.

8. **P2 — HTTP disconnect leaves existing waits and watch streams blocked.** `internal/httpmcp/hub.go:128`, `internal/httpmcp/peer.go`, `Wait`, and `watch.go`. Removing the token and membership does not mark the old peer closed or wake its waiters. Existing `/watch?follow=1` handlers still hold that peer and wait until their HTTP context ends. Add terminal state, wake pending waits, and have them return a disconnect result. Reproduction: `TestReviewDisconnectWakesWait`.

## Other confirmed findings

9. **P2 — Release tag can identify different source from its binaries.** `scripts/release.sh:116`. The script builds local HEAD, verifies a clean tree and injected version, then invokes `gh release create` without `--target`. The installed CLI's own help states that a new tag defaults to the latest default-branch state. A clean feature branch, an unpushed commit, or a moving default branch can therefore produce assets inconsistent with the tag. Capture the built commit, require it on the remote, and explicitly target it. Verified by source and local CLI documentation; no release was published.

10. **P2 — Race in the catch-up test server.** `internal/mcptools/teams_relay_test.go:2405` and `:2409`, `TestLiveDeliveryConfirmsItselfOnceKnownCaughtUp`. Its history-reply goroutine and live-message loop call `WriteJSON` concurrently on the same websocket. `go test -race` reports the race both in the full suite and in isolation. Serialize the fixture's writes. This is a test-fixture finding, not evidence of the same race in production writers.

## Findings whose focused reproductions now pass

11. **P1 — HTTP overflow deadlocked the entire session.** The original `httpPeer.Deliver` synchronously called `onAbandon → Server.disconnect → Session.Leave` while `Session.broadcastExceptLocked` held the session mutex. Two peers and approximately 2,000 small broadcasts reproduced the lock cycle. The concurrent patch schedules the callback asynchronously and only once. `TestReviewOverflowDoesNotDeadlockSession` now passes.

12. **P1 — Session removal allowed a pending join into a detached object.** A handler could obtain S from `GetOrCreate`, pause before `Join`, and resume after the last peer left and the manager removed S. Subsequent clients received a different Session for the same ID. The concurrent admission-count patch prevents removal while a join is pending. `TestReviewRemovedSessionCannotAcceptInvisibleJoin` now passes.

13. **P2 — Operation errors permanently disabled reconnect.** The original read loop set `fatalText` for any unclaimed coded error with `retryable=false`, including `bad_request` about one operation. A later network drop was therefore classified as permanent. The concurrent patch narrows fatal error codes. `TestReviewRequestErrorDoesNotMakeConnectionPermanentlyFatal` now passes for `bad_request`; the full allowlist still needs protocol review.

14. **P2 — Closed client connections leaked activity goroutines.** `activityLoop` ranged on a channel that was never closed or otherwise stopped. Three dial/close cycles left three goroutines retaining their connection and callback state. The concurrent stop-channel patch makes `TestReviewActivityLoopEndsAfterClose` pass.

These are focused confirmations of the reported failure modes, not full approval of the concurrent fixes or a deployment claim.

## Additional source-level concerns

These were traced in source but do not have dedicated runtime reproductions in this review:

- **HTTP connection state is not captured atomically.** `handleSend` reads `hub.peer()` and `hub.session()` under separate lock acquisitions. Disconnect between them yields a non-nil peer and nil session; reconnect can instead pair an old peer with a new session. `disconnect` also unlocks the hub before deleting its map entry, allowing a concurrent reconnect's entry to be deleted. Use one atomic snapshot and a lifecycle/identity check when removing a hub.
- **Roster snapshots can be published out of order.** `Session.broadcastRoster` snapshots members, unlocks, then reacquires through `broadcastExcept`. A join can publish a newer roster in that gap, followed by the older snapshot that overwrites clients' membership views. Compute and publish under one session lock.
- **Incoming websocket messages have no client read limit.** `finishHandshake` and `readLoop` call `ReadMessage` without `SetReadLimit`; the server-side 48 MiB limit only bounds traffic entering the bundled server. The event-count limits do not bound bytes, and attachments are decoded before size checks on receive. No memory-exhaustion experiment was performed.
- **Outgoing local attachment limits are checked after reading the entire file.** `wire.ReadAttachmentFile` and `ReadFileAttachment` use `os.ReadFile`, then compare against 32 MiB. A mistakenly selected huge file can exhaust memory before it is refused; a streaming/device path can fail to terminate. Use a bounded read with one extra byte.
- **The local waiter accepts and writes without deadlines.** `waiter.acceptLoop` synchronously waits for the first mode byte; a stalled local connection prevents acceptance of subsequent readers. Follow-mode writes and `writeAndClose` are also unbounded, so a non-reading follower can stall delivery and teardown.
- **An old attachment directory is assumed abandoned.** `sweepStaleAttachmentDirs` removes any matching directory older than 24 hours without checking a live owner. Long-lived sessions and files handed to a reader can outlast that threshold. Age alone does not establish abandonment.
- **Recovery assurances exceed bundled-server capabilities.** Several overflow/push-failure notices promise everything remains on the server and recommend catch-up. The bundled websocket handler only handles `msg`, does not implement `messageAfter`, and logs text rather than recoverable attachment history; HTTP-originated messages do not go through that logger. Recovery guidance should depend on actual server capabilities.

The unsigned release-version metadata issue is already documented in `docs/known-issues.md`; this review does not present it as a new discovery.

## Validation and scope

- Initial `go test ./...`: passed.
- Initial `go test -race ./...`: failed with the fixture race above. Also saw `TestUnrelatedBadCursorErrorDoesNotDisableAcks` drain after the first buffered error while asserting two; its polling condition does not wait for both messages.
- Isolated race reproduction: failed with concurrent `WriteJSON` calls at the cited fixture locations.
- `go vet ./...` in the isolated reproduction tree: passed.
- Thirteen added focused tests failed on the original snapshot. On the second snapshot, four pass and nine still fail. The budget finding has two tests.
- To run the focused checks: `go test ./internal/httpmcp ./internal/hubconn ./internal/hubsession ./internal/mcptools ./internal/wsserver -run '^TestReview' -count=1 -timeout=20s` from either snapshot directory. Failures are intentional evidence of the defects.

The review covered connection and reconnect lifecycle, acknowledgements and history, shared delivery budgets, HTTP and websocket membership, persistence, attachments, waiter/harness integration, self-update/release logic, and relevant tests. It is a broad code review, not a claim that every possible interleaving, platform, or external deployment has been verified. No deployment, real credential probing, destructive stress test, or source fix was performed by this reviewer. Findings were sent to the requested relay as they were verified.
