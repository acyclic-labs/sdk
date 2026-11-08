# Harness client views

The [headless event contract](headless.md) describes versioned JSONL output,
payload acknowledgements, replay identities, final artifacts and paged browsing
over this same client.

The [committed history contract](history.md) describes paged turn selection,
conditional file/context restoration, fork identity and current integration limits.

Agent observation uses `AgentClient` over the shared Rust `WireSession` and the
platform's authenticated Streams `session::Client`. Each focused agent selects
its `AgentPath::conversation()` and `AgentPath::status()` streams with explicit
exclusive cursors. Use a retained cursor or a bounded history page's edge;
opening the client does not fetch a historical snapshot. View demand and live
record/byte retention have finite policies. The canonical session owns receive
credit, transport, authentication and recovery.

Browser callers initialize the generated WASM module and construct `AgentClient`
from `client-query` with their authenticated `WasmStream`, canonical agent path,
explicit cursors and credential slot. The browser object drives the same Rust
`AgentClient`, with one pending `next()` read. `close()` releases follows and
wakes a pending read with a typed closure error. `viewRevision()` lets renderers
reuse an unchanged view; `record(index)` copies only the requested visible turn.
Sequences cross as exact `BigInt` values. `currentStatus()` reads the Rust fold
and returns null until a real status arrives, rather than fabricating idle state.

A selected follow failure is reported to the reader and retires that follow.
Continue reading to receive events from the remaining healthy selections; the
failed path does not remain in the polling rotation. Its final bounded view
remains available. When no live follow remains, reads return a typed closure
instead of waiting for a stream that can no longer wake them.

`Reply` correlates a message's identity and accepted inbox sequence with its
committed result. An ordinary prompt returns its own conversation entry. If
that turn continues an active goal, it remains pending until the same goal run
settles; the answered reply carries the original entry as `origin` and the
terminal self-continuation as `entry`. Both entries retain their immutable file
references. The terminal reference commits atomically with the conversation
entry and status event, so replay and a stale checkpoint cannot substitute a
later goal or expose uncommitted output. Cancellation that abandons the run
returns `Dropped`; closing its observation returns the existing typed closure
error. No current GOAL.md or latest-conversation lookup selects the reply.

Tool results are selected through the same original execution journal. Retain
the accepted inbox sequence, then send `AgentCommand::Events` with that `input`,
an exclusive journal `after` (initially zero), and `wait: true`. Consume every
page at its returned `end`. A full page's `next` continues that journal; a short
page reaches only its current tail. Complete selection requires both
`next: None` and the actor's `settled: true` input fence. Quiet reads wait on the
original journal and actor producers. A filesystem change delivery watermark
does not establish this fence.

Each `ToolStarted` observation selects its admitted invocation; each
`ToolCompleted` selects its validated result. Read those bytes with
`ReadTarget::Execution { input, sequence, field }` and ordinary bounded `Read`
ranges. Join the original operation, step and call identity across admission
and completion. These references remain under the selected agent's private
grant. An absent observation, an unrelated payload field, or an inherited input
cannot authorize a private payload read. Reopening the same execution cursor
replays the same positions and immutable references without another prompt.

`AgentControls::select(agent)` discovers a descendant's own tool resource using
the existing Session and credential slot. A root's tool resource does not select
a child's volume. Original tool results and `FileRef` values establish logical
publication; physical Machine execution and workspace isolation require the
owning host's corresponding execution evidence.

The public TS client exports actor/session views rather than `FileClient`,
`TaskVolume` or `ClientViews`. Native `FilesystemFileQuery`,
`MemoryHarnessStorage::file_query`, `LocalHarness::scratch_files` and
`LocalHarness::project_diff` are removed. Read a settled entry's immutable file
reference through the host's authorized content reader. Replacement filesystem
page, cut, backfill and diff bindings remain separate integration work; the
retired query adapter is not a compatibility entry point for them.

The whole-client migration is in progress: internal native TaskVolume folds and
replacement durable-host coverage still need work. The old browser task-volume
bindings and their mechanism-specific fixtures are removed. The generic
absent-stream follow, same-cut child-status projection, actor-native input
composition, project/skill catalog capture, remaining command dispatch,
approvals, terminal effects and historical backfill remain open. These source changes
do not claim that the complete migration, compilation or endpoint qualification
is complete.
