# `acyclic-native-runtime`

Small, platform-specific file primitives shared by Acyclic's local storage providers.

Most applications should depend on a higher-level crate such as
[`acyclic-fs`](https://docs.rs/acyclic-fs) instead. This crate is public so the
other Acyclic crates can be installed from crates.io without Git dependencies.

The implementation selects a platform backend at compile time, with runtime
fallback where needed:

- Linux: owned async batches use `io_uring`, with bounded-worker positional I/O
  when ring setup is denied or unsupported. Borrowed synchronous positional
  calls use OS positional I/O directly. Transient setup failures retry;
  uncertain completions transfer the ring, file, and bounded submitted window
  to a process-owned retirement service. The shared file-health registry fences
  affected file aliases for the life of the process; reaping a CQE alone does
  not prove whether the write took effect. In-flight rings retain their file
  descriptors and buffers within a hard cap. Once terminal, the fence retains
  a bounded descriptor anchor to prevent device/inode reuse from denying an
  unrelated file. The identity cap is at most a quarter of the process's open-
  file limit (and never more than 4,096); exhausting it rejects new identities
  before submission. Recovery requires
  the owner to reconcile durable state before restarting the process.
  Unrelated files continue through their selected backend while capacity remains.
  An unwind while a submitted window may still own kernel-visible buffers
  terminates the process; it is never converted into an ordinary I/O error
  that could free those buffers or permit a later alias write.
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
An uncertain Linux completion poisons that `NativeFile` rather than allowing a
later sync, resize, or write to overtake a possibly in-flight operation.
Malformed CQ identities remain in bounded process-owned retirement state because
safe buffer release cannot be proved. If retirement capacity is exhausted, that
thread explicitly degrades to positional I/O for unaffected files before any
further SQE is submitted; affected file aliases remain fenced. A stopped
retirement worker does not discard unresolved buffers during shutdown.
Callers must await durability completions before publishing dependent state.
Write batches reject overlapping ranges before submitting any write, so their
result does not depend on platform completion order.

The current backend uses bounded completion workers and a separate bounded
host-operation pool for locks and namespace work without native completion.
Its owned file operations do not block the caller's async executor, but it is not yet a
thread-per-core proactor and not every host filesystem operation has moved to
this boundary. Run the local, non-gating backend baseline with
`cargo test -p acyclic-native-runtime --lib owned_io_backend_baseline -- --ignored --nocapture`.
It records the OS, architecture, bytes, and native-versus-baseline elapsed time;
release qualification additionally requires mounted Cargo and Lean workloads.

The API documentation is available on
[`docs.rs`](https://docs.rs/acyclic-native-runtime).

`ProcessTree` retains the direct child and OS containment through collection
and cleanup. `wait(timeout)` bounds exit observation; cleanup uses that same
path before reaping. `wait_with_output(timeout, max_bytes)` closes stdin, polls both
pipes without reader threads, and cleans up on exit, overflow, or timeout.
The output bound covers retained payload across both streams, with an 8 KiB
scratch buffer; allocator bookkeeping and OS pipe buffers are outside it.
Existing temporary-file capture consumers retain their separate file-size
polling policy, which is not a hard disk-usage limit.

| Invariant | Production mechanism | Assumptions | Verification / evidence |
| --- | --- | --- | --- |
| Windows successful cleanup leaves no executing owned descendant | Suspended spawn, non-breakaway kill-on-close Job, termination followed by `ActiveProcesses == 0` | Trusted OS Job semantics; no transfer of Job authority | Real child/grandchild termination, drop and exited-parent tests; Windows lane |
| Unix cleanup kills descendants remaining in the owned group and reaps the direct child | Process group SIGKILL and direct-child polling | Trusted OS; descendants do not escape groups/sessions; OS eventually completes termination and reparents/reaps descendants | Same real fixtures; existing Linux/macOS lanes. Signal success is not proof of descendant reaping |
| Unix exit observation cannot release the leader's PID/PGID before group termination | `waitid(WNOWAIT)` observes status; only cleanup reaps the retained leader | Exclusive child ownership; host does not install a competing reaper or `SIGCHLD` auto-reaping policy | Real repeatable exit observation, nonzero exit-code and signal-status fixture; Linux/macOS lanes |
| Repeated completed cleanup has no further OS effect | Guard becomes inactive only at its platform completion boundary; child removed only after observed exit | Exclusive owner; no reuse of containment before cleanup | Idempotence fixture |
| Captured payload never exceeds the shared byte budget; overflow is an error | Checked subtraction before append; fallible exact reservation | Exclusive pipe readers; allocator/OS calls progress | Real two-stream fixture and flood rejection |
| Deadline expires without detached readers or successful rollback claims | Nonblocking Unix reads / Windows pipe peek; separate five-second cleanup observation windows; errors retain child ownership | OS calls and scheduling progress; no hard real-time return guarantee | Real timeout and descendant-held-pipe fixtures; Windows reduced-rights Job test establishes cleanup-error precedence and retained ownership |
| Host-configured environment is preserved | Command builder owns `env_clear` and explicit `env` policy; containment does not add environment entries | Trusted host builder applies the required policy | Cleared-environment real fixture with explicit input and closed stdin; generic primitive provides no default credential policy |

These are implementation checks and test evidence, not machine-checked or
unrestricted correctness proofs. Reproduce with `cargo test -p
acyclic-native-runtime --locked --lib` and `cargo clippy -p
acyclic-native-runtime --locked --all-targets -- -D warnings` on each platform.
The existing ignored I/O performance baseline is not a process gate.
