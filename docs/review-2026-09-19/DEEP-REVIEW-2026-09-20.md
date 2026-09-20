# Client deep re-review — 20 September 2026

## Scope and evidence

Latest targeted recheck: `/tmp/mcp-hub-roundcheck-uGeodn`, captured after the author's next patch round. The explicit bad_ack_cursor websocket reproduction and BOTH reconnect-store interleavings pass together. The new real websocket late-send-ack test `TestALateAckDoesNotAnswerTheNextRequest` passes under -race; B no longer receives A's delayed result. The earlier synthetic `TestDeepReviewLateSendAckNotAssignedToNextSend` directly canceled a claim without calling the timeout path that records an owed late answer, so it does not model the fix and still fails in isolation. `TestDeepReviewSendErrorDoesNotRefuseHistory` passes; ambiguous errors are no longer handed to one of several claims. A full hubconn run including review-only tests fails only that obsolete synthetic late-ack case. The hubconn package suite excluding that simulation passes under -race. This verifies these particular paths, not all request kinds or correlation semantics.

Reviewed the client at HEAD `c8d1d1b04e6b3853f1be13a68519c62ad0190278` (v3.1.4), including the then-uncommitted client fixes. Authoritative initial copy: `/tmp/mcp-hub-deep-RgXkzK`. Follow-up fix checks: `/tmp/mcp-hub-finalcheck-d7mmCN`, refreshed with the subsequently changed tools.go and waiter.go. The shared worktree remained under concurrent development; status below describes these captured copies, not an assertion about later edits.

Covered connection lifecycle/reconnection, read receipts and action claims, history/catch-up, delivery budgets, waiter delivery, attachment cleanup, connection-store persistence, version reporting, and release/self-update paths. Bundled-server implementation findings remain outside the requested scope. This is a risk-focused deep review, not proof of absence of all defects or a claim that every platform was exercised.

No production files were changed by this review. Reproduction tests and one explicit scheduler seam exist only in temporary copies. Findings were sent to chat-relay as verified.

## Findings and follow-up status

### 1. P1 — late send acknowledgements are attributed to the next send

`internal/hubconn/conn.go`, claimNextAckForToken / tryDivertToClaimLocked, approximately lines 1180–1299.

Send A times out and cancels its claim; send B registers the same ack kind. A's delayed sendAck is then delivered to B, including A's externalId, producing a false success/result for B. Per-kind exclusion while a request is pending does not protect the next request after timeout.

Reproduction: `TestDeepReviewLateSendAckNotAssignedToNextSend` fails deterministically using the actual claim-routing functions. Server author independently confirmed that this wire currently has no request identifier to echo. Correct correlation requires coordinated protocol support or a client policy that preserves ambiguity; timeout is not evidence that the original send failed.

Latest follow-up: the timeout path now records one owed answer of this kind, and routing spends the late answer against that record instead of the next caller. The author's real websocket test `TestALateAckDoesNotAnswerTheNextRequest` passes under -race, with B timing out rather than receiving A's externalId. This is the correct interim mitigation. The original synthetic test bypasses the timeout bookkeeping and is not a valid reproduction of the production path; it remains useful only as a direct demonstration that a raw claim cancellation alone carries no correlation state. Finding fixed for the tested send path; broader kinds remain to be covered.

### 2. P2 — one request's error can terminate an unrelated history read

Same file, tryDivertToClaimLocked. Generic errors go to pendingMessageAfter first, otherwise to an arbitrary action claim. With history and send pending, `send_refused` is delivered as the history answer while the actual send waits.

Reproduction: `TestDeepReviewSendErrorDoesNotRefuseHistory` fails. This shares the protocol-correlation limitation with finding 1, but affects concurrent requests of different kinds rather than a late response to a timed-out request. Known operation-specific codes should not be routed as arbitrary failures.

Latest follow-up: when more than one claim is pending, an uncorrelated error now falls through to the shared event buffer rather than being delivered to an arbitrary claimant. `TestDeepReviewSendErrorDoesNotRefuseHistory` passes under -race. Finding fixed for this concurrent-claim case; an error with exactly one pending claim is still attributed to it, as intended by current protocol assumptions.

Mitigation recommendation from the relay review: until correlation exists, do not assign a late ack or generic error to whichever request happens to be waiting. Preserve an unknown outcome rather than falsely completing a different operation. If the protocol later adds a client-chosen opaque id, echo it on both acknowledgements and errors; `externalId` identifies a stored message and is absent on failed sends, so it cannot correlate failures. No mitigation implementation was made in this review.

Owner decision relayed 2026-09-20: correlation id is deferred, with a concrete trigger rather than left open indefinitely — implement on the first real-use report that a send or refusal was surfaced as timeout/silence, or the next time the wire is changed. Both reproductions were constructed, not observed in traffic. The current mitigation preserves ambiguity instead of false attribution. Remaining operational risk: with multiple claims pending, a real refusal such as tenant-lock/send_refused reaches no caller and may prompt a futile retry. Report that occurrence to the server owner; no protocol change is authorized by this review.

### 3. P1 — an explicit malformed-cursor refusal becomes confirmation success

`internal/hubconn/conn.go`, handleAckPlumbingLocked (~1338) and ConfirmReceived (~1384).

A server declaring ackReplies receives a standalone ack and replies `error/bad_ack_cursor`. Ack plumbing consumes that error and disables automatic receipts without resolving the waiting confirmation. Five seconds later ConfirmReceived returns nil error. The tool layer can therefore persist the cursor that was explicitly refused and report success.

Priority raised to P1: persisting the refused cursor can make subsequent catch-up start beyond unread messages, defeating the server's rejection. The server retains the messages, but ordinary client catch-up may silently skip them. An unanswered request must also remain unknown rather than authorize advancing the durable position.

Reproduction: `TestDeepReviewConfirmRejectsServerError`, a loopback websocket server, fails in about five seconds. No scheduler seam is used. Distinguish an explicit refusal, an unanswered/ambiguous request, and a successful receipt before local persistence. This is separate from finding 8's disk-write result.

Follow-up: the first patch checks error events inside ConfirmReceived, but ack plumbing still intercepts this code before that branch. The unchanged websocket test still fails against that patch; reported to the author.

### 4. P2 — stale reconnect bookkeeping marks a replacement session disconnected

`internal/mcptools/tools.go`, reconnectOnce, the Upsert / stillHolds / MarkDisconnected sequence (~1025–1045 in the follow-up copy).

After old reconnect installs, disconnect it and complete a fresh connect under the same name/link. Resume the old reconnect: it writes its entry, observes that it no longer owns its connection, and calls MarkDisconnected for the shared target. The newer socket remains connected while its store entry says disconnected.

Reproduction: `TestDeepReviewOldReconnectMustNotMarkNewSessionDisconnected` fails with `new conn.Connected() == true` and `entry.Connected == false`. Uses one review-only scheduling callback immediately after installation in the isolated copy. The compensating write needs generation/ownership protection too; being stale is not authority to change the newer holder's state.

Follow-up: a mayCompensateOnDisk guard makes this replacement test pass, but regresses disconnect-without-replacement: late Upsert writes Connected:true and the guard refuses compensation because the session no longer owns its name. `TestDeepReviewCancelledReconnectCannotPersistConnected` fails against that revision. Both interleavings must pass; finding remains open.

### 5. P2 — running development version changes with executable-file metadata

`internal/version/version.go`, devStamp (~75) and Short.

Every Short call stats the executable path anew. Changing only the test executable's mtime changed its reported version from `3.1.4.20260920104412` to `3.1.4.20260920094412` without changing running code. Installing a replacement at that path can similarly lend its timestamp to an older running process. This undermines build identification and stale-process diagnostics.

Reproduction: `TestDeepReviewRunningVersionDoesNotChangeWithFileTimestamp` fails. It touches only the generated test executable and restores its timestamp; no installed/user binary was modified. Capture once at startup or embed a build stamp. Tagged release versions are unaffected by this path.

Follow-up: both the author's initial read-on-first-use revision and the subsequent package-initialization capture pass the reproduction under -race. The latest version-package suite also passes. Package initialization removes the lazy-first-call window but remains filesystem metadata, not embedded build identity; an external replacement between process launch and initialization is not impossible. Classified as verified fixed for the demonstrated changing-between-calls behavior.

## Found, reported, and passing targeted checks after concurrent fixes

### 6. P1 — disconnect during initial handshake leaves a live orphan

Initial handleConnect installed unconditionally after a disconnect removed its reserved session. A first attempted guard captured/recreated lifecycle state after Dial, still resurrecting the canceled connection. The subsequent revision reserves generation/link before dialing and checks installation atomically.

`TestDeepReviewDisconnectDuringInitialConnect`: failed original and first guard; passes latest captured revision under -race. Uses websocket handshake barriers, not production hooks.

### 7. P2 — attachment sweep deletes proven-live owners' files after 30 days

The new age ceiling overrode successful process-liveness checks. Directory mtime does not indicate whether the session still uses its files. The revised decision distinguishes proven-live from unknown owners and never ages out a proven-live owner.

`TestDeepReviewLiveOldAttachmentDirectory`: failed original; passes revised Linux behavior. Windows unknown-liveness age policy was not exercised on Windows and remains a separate deliberate policy, not a verified liveness guarantee.

### 8. P2 — hub_confirm says persisted after its store write fails

setCatchUpCursor swallowed its error into a queued warning while the tool returned an unconditional persisted claim. Creating a directory at `state.json.tmp` made writes fail while retaining readable old state.

`TestDeepReviewConfirmCannotClaimFailedPersistence`: failed original; passes updated error propagation. A first wording patch also called all errors accepted receipts, including pre-send rejection; this was reported and changed to a typed persistence-only error branch. Do not confuse this repair with the still-open low-level refusal bug in finding 3.

### 9. P2 — an accepted waiter registers after Close

handleAccept could finish reading the mode byte after Close had stopped delivery, then publish a reader that nobody would wake. A first check used a separate lock hold and retained a smaller window. The revised closed check and registration share one lock hold.

`TestDeepReviewAcceptedReaderAfterClose`: failed original; passes latest revision. The follow-up test drains the newly added closure notice so the net.Pipe fixture does not itself block the server's write.

### 10. P2 — waiter snapshots race and an old drain detaches the replacement

sourcesSnapshot returned mutable attachment pointers; SetSource changed their src while deliver read it outside the lock. Additionally, draining an old disconnected source could call Detach by name after a replacement source was installed, removing that replacement.

`TestDeepReviewSourceSnapshotIsStable` produced a race-detector report and changed snapshot identity. `TestDeepReviewOldDrainDoesNotDetachReplacement` deterministically removed the replacement. Both pass after value snapshots and identity-conditional DetachIf.

## Validation

### Push/pull observation from the shared relay (11:18–11:19 UTC)

A separate Claude Code peer (`mcp-hub/y`) with a reachable harness ran a bounded push-mode reconnect test. Its confirm history is now clarified: standalone `hub_confirm` to 46738 established its first position; a later send with `confirmCursor` moved it to 46740 before the bounded test. Six messages were then pushed without confirmation; the last cursor before reconnect was `639254999233833000.46746`. The server-side reading first described as a pre-push baseline was actually taken at 11:18:54, after those pushes and the peer's disconnect/reconnect, and still showed ACKED_CURSOR 46740. Reconnect catch-up independently reported stored position 46740 and walked 46741–46750, including all six pre-disconnect pushes. A second server-side reading after the complete re-walk also showed 46740. A later send carried `confirmCursor=639254999518371000.46753`; the server subsequently read exactly 46753, confirming piggyback persistence on that send. This was not an isolated piggyback-only run: both forms are witnessed, with the standalone confirm before the bounded push test and the piggybacks observed later. The peer reports subsequent piggybacked confirms to 46760 and 46765. Server queries and client sends overlapped; report each cursor with its operation rather than infer precise ordering from transcript delivery. Thus unconfirmed push did not advance the resume position, reconnect re-walked the messages, and a send's confirmCursor persisted in the observed case. Preserve the chronology correction: the 46740 server read was post-six-pushes and post-reconnect, not a pre-push baseline. `LAST_SEEN` was reported at 11:18:20.6461 and later 11:19:21.6604; source inspection by the chat-relay author confirms it is updated by `TouchAsync` on join, teardown, and activity. Its only readers are presentation ordering in `ConversationsController` and selecting a timestamp for restart-generated leave events in `HubPeerEvents.StillAttachedAsync`; the latter selects on `ConnectedAt != null`, not LastSeen. No code uses `LAST_SEEN` for liveness. `CONNECTED_AT` carries connection semantics and stayed at 11:18:51.1563 across the test. This resolves the apparent timestamp anomaly; no liveness defect is present. The server logged five successive `messageAfter` requests from cursor 46745 through 46750, ending at none. This verifies the Claude push path's `ObservedNothing` does not advance the read position and reconnect walks the unconfirmed messages. It closes the Claude push evidence gap only. This Codex client still has `hub_wait`/`hub_receive` in its tool list and no harness messaging socket, so its push path remains untested.

The confirm history is one standalone `hub_confirm` to 46738 before the test, then piggybacked confirms (the second set 46740), then additional confirms. No server reading falls between the first standalone and first piggyback, so that early sequence does not isolate either route. The frozen measurement window is exactly: ACKED_CURSOR remained at 46740 after six unconfirmed pushes and a process restart; it then advanced to 46753 on a send carrying `confirmCursor=46753`. Neither route was isolated during this window; both were already in use. The window measures the Claude push receipt and excludes a spontaneous ack writer. It does not isolate confirmation routes.

Both routes were isolated later, outside the frozen window. The piggyback result is **recovered, not recorded**: a reading at 11:19:21.6604 showed 46740; one send carried `confirmCursor=46753`; a reading showing 46753 was posted at 11:20:06. Its actual read time is unrecorded (11:19:39.4272 was `LAST_SEEN`, an activity timestamp, not the read time). The peer's call history reports no other confirm between those observations, so the interval is closed by the absence of another writer, not by its width. The standalone result is isolated with timestamped reads: 46781 at 11:23:07Z, then two standalone `hub_confirm` calls and nothing else, then 46786 at 11:23:56Z and 46787 at 11:24:15Z. Neither post-window interval is part of the push-test result.

This evidence also exposes the observability gap: receipt frames update ACKED_CURSOR but are not logged. The standalone bracket is durable because both reads have stated read times; the piggyback bracket was recoverable only because the channel was quiet enough to enumerate all other writers, and would not be reconstructible on a busy connection. A useful server-side log should record each accepted receipt's cursor and timestamp, so a later column read can be ordered against the write independently of conversational message order. The repeated confusion in summaries about which route set 46740 illustrates the same missing-interval problem: preserving cursors, operations, and ordering is more reliable than a compact conclusion.

The original captured baseline passed `GOEXPERIMENT=jsonv2 go test -race ./...` before the added reproductions. The later targeted checks pass for explicit refusal handling, both reconnect-store interleavings, initial-connect cancellation, attachment cleanup, waiter races/replacement, and process-start version capture. The real late-ack and ambiguous-error routing tests also pass under -race. The hubconn package passes under -race when excluding one obsolete synthetic late-ack test that bypasses the timeout bookkeeping; see findings 1 and 2.

The first full follow-up run used refreshed production files with an older attachment-cleanup test and failed that stale expectation. This was a mixed-snapshot test issue, not another production finding. The follow-up tests were subsequently refreshed from the author's tree. The owner deferred a wire correlation id with a concrete trigger: implement on the first real-use report that a send/refusal is surfaced as timeout/silence, or the next time the wire changes. The interim mitigation is implemented; the two attribution findings pass their tested paths, but that trigger remains open for real-use evidence.

The subsequent follow-up baseline run passed `GOEXPERIMENT=jsonv2 go test -race ./... -skip '^TestDeepReview' -timeout=120s` (mcptools: 54.8s). It excludes the intentionally failing review reproductions and predates the last reconnect compensation revision; it is not a claim that the unresolved findings pass.

Use GOEXPERIMENT=jsonv2: strict wire-case checks intentionally require it. The only scheduling seam is the explicitly documented reconnect-store interleaving test. No real Codex push end-to-end session or Windows runtime test was performed in this pass. Server-side protocol changes were not implemented or authorized by this review.
