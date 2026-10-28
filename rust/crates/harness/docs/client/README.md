# Harness client views

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
