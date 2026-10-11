# `acyclic-native-runtime`

Small, platform-specific file and process primitives shared by Acyclic host consumers.

Most applications should depend on a higher-level crate such as
[`acyclic-fs`](https://docs.rs/acyclic-fs) instead. This crate is public so the
other Acyclic crates can be installed from crates.io without Git dependencies.

The implementation selects a platform backend at compile time, with runtime
fallback where needed:

- Linux: owned async batches use `io_uring`, with bounded-worker positional I/O
  when ring setup is denied or unsupported. Borrowed synchronous positional
  calls use OS positional I/O directly. Transient setup failures retry.
- macOS: positional I/O on bounded workers for arbitrary open file handles.
- Windows: overlapped I/O.
- Other targets: the portable fallback.

`NativeFile` is the owned-handle boundary for positional batches, length changes,
and durability flushes. Each operation owns its handle and buffers until its
completion is known. Dropping a future before admission discards it; dropping
one after admission detaches the observer but does not undo the operation.
Operations created from the same `NativeFile` run in first-poll order. Merely
creating a future does not submit I/O or hold up later operations. Once polled,
an operation keeps its place even if its observer is dropped.
Write batches reject overlapping ranges before submitting any write, so their
result does not depend on platform completion order.

Process ownership stays in `process_tree.rs` inside this existing crate. File
backends remain separate modules. `process_output(command, timeout, max_bytes)`
provides bounded short-command capture; `process_status(command, timeout)`
preserves configured streams. Streaming hosts take pipes from `ProcessTree`
and enforce their protocol bounds. `terminate_after(grace)` requests Unix
SIGTERM before mandatory cleanup; an exited leader never excuses surviving
descendants. Async callers use the existing `run_blocking_io` pool, whose
admitted operations retain ownership when observers are dropped. Its queue and
OS scheduling are outside the command's exit/capture observation window.
