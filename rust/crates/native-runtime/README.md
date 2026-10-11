# `acyclic-native-runtime`

Small, platform-specific file and process primitives shared by Acyclic host consumers.

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
Observer drop does not synchronously drain an accepted blocking job. Any mounted
original-work activation needed by that job therefore lives in its accepted
payload, not only in the waiting future. Polling a completed panicking job resumes
the original panic payload after its operation locals and terminal controls
retire; it does not replace that payload with an allocated I/O diagnostic.
An uncertain Linux completion poisons that `NativeFile` rather than allowing a
later sync, resize, or write to overtake a possibly in-flight operation.
Malformed CQ identities remain in bounded process-owned retirement state because
safe buffer release cannot be proved. If retirement capacity is exhausted, that
thread explicitly degrades to positional I/O for unaffected files before any
further SQE is submitted; affected file aliases remain fenced. A stopped
retirement worker does not discard unresolved buffers during shutdown.
Callers must await durability completions before publishing dependent state.

Measured native reads and measured host tasks share one synchronous completion
waiter. It charges its owned wake payload to the operation's original meter and
uses the same cancellation wake and deadline. Deadline expiry requests cooperative
stop; an accepted operation is still drained to terminal completion before returning.
An inline zero-length read creates no waiter. This is not an OS I/O cancellation
or hard elapsed-time guarantee.
Write batches reject overlapping ranges before submitting any write, so their
result does not depend on platform completion order.

Finite callers provide their original `NativeReadMeter`; read, write and
durability reservations are admitted before submission and retain actual partial
prefixes on failure. Exact reads reject premature EOF through the same engine.
Returned buffers, queued work and poisoned file anchors retain their original
custody until their payload or file is dropped, rather than until an observer
stops awaiting. Capacity reconciliation records completed SDK-owned capacity
before evaluating a late cancellation or quota refusal.

`NativeReadMeter::allocation_owner` can separate a captured request's pending
activation from its retained payload custody. It returns only the same original
meter and resource attribution, never a fresh allowance. Accepted jobs retain
the captured meter until physical terminal completion; returned buffers and
retired file fences retain the original allocation owner instead. A healthy
file tail must not keep a completed request's capture activation alive.

`ReadAllocationReservation` withholds one known allocation and its logical bytes
before accepting work that can create it. A pending credit is neither a reported
allocator attempt nor a retained payload. Consuming it immediately before the
allocator records the actual attempt and produces original-meter `ReadAllocation`
custody; dropping an unused token refunds only its withheld credit. Accepted
completion does not repeat quota or cancellation admission. The original global
and resource allowance remain unavailable to other allocations while withheld.

`prepare_blocking_io_measured` constructs a one-shot operation on the same
host-operation engine without submitting it. Its completion, exact job closure,
unlinked capacity waiter and synchronous wake owner are funded before the
caller starts a driver. An operative meter's original `allocation_owner`
funds those controls; the operative owned-credit meter remains responsible for
submission and actual cleanup prefixes. First polling queues the existing job,
and `wait_measured` reuses its prepared wake owner without a late allocation.
Preparation is accounting custody, not namespace authority, and accepted
observer drop still neither drains nor cancels the physical operation.

`wait_measured(deadline, cancellation, on_deadline)` uses the task's captured
original meter, not a caller-supplied replacement. Closing allocation admission
after preparation does not prevent the prepared wake owner from draining an
accepted physical operation and retiring its original custody.

`NativeFile::write_bounded_async_with_custody` accepts owned `AsRef<[u8]>` payloads
without converting or copying their backing. The same positional validation and
ordered completion driver retain the complete payload before its separate custody
guard, so SDK byte carriers keep their original guard through accepted I/O.

The synchronous held write adapter admits Windows mode queries and overlapped
reopens before using that same positional completion engine. Namespace publication
and durable rename likewise share ordinary/native syscall engines; measured calls
retain their actual terminated platform path buffers through the physical effect
and account for each namespace and durability attempt on the original meter.

These limits cover SDK-visible logical payload capacities and known metadata,
not allocator bookkeeping, RSS, kernel buffers, private codec allocations, or
physical transport acknowledgments. An `Arc`'s known value is charged for its
logical lifetime; opaque backing retained by weak aliases is not a physical
allocation guarantee. Meter callbacks must preserve the caller's original
resource attribution and typed first failure. Cancellation and deadlines are
cooperative; they do not prove that an accepted OS operation was rolled back.

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
On Unix, `ProcessTree::prepare_closed_process_measured` owns the host environment
snapshot and fixed argument-pointer storage before driver startup.
`spawn_closed_prepared` fills those pointers while the caller's terminated
arguments remain live, then uses the same closed-stream `posix_spawn` engine.
Acquired PID or native configuration custody retains the actual prepared frame,
including failures after spawn; physical reap/configuration retirement releases
its byte storage before its original allocation guards. The frame grants no
command authority. As with ordinary closed spawn, trusted host code must not
mutate the native environment concurrently with its snapshot.
`ProcessTree::TERMINATION_TIMEOUT` and `TERMINATION_POLL_INTERVAL` expose the
same five-second observation window and one-millisecond polling delay used by
physical termination and group-reaping loops. Native birth-credit plans reuse
this policy rather than copying it. Finite credit still gates every real call;
timeouts and exhausted credit retain genuine ownership, not proof of absence.
`wait_with_input_observed` uses the same collector and borrows the caller's
literal input. It writes nonblocking prefixes, closes stdin after the last byte
(including an explicitly empty input), and waits for normal child completion.
Unlike the existing interactive exchange, finishing literal input does not
terminate the child. Input, process containment and output allowances remain
the caller's explicit policy; this primitive adds no argument interpretation.
Existing temporary-file capture consumers retain their separate file-size
polling policy, which is not a hard disk-usage limit.

Process ownership stays in `process_tree.rs` inside this existing crate. File
backends remain separate modules. `process_output(command, timeout, max_bytes)`
provides bounded short-command capture; `process_status(command, timeout)`
preserves configured streams. Streaming hosts take pipes from `ProcessTree`
and enforce their protocol bounds. `terminate_after(grace)` requests Unix
SIGTERM before mandatory cleanup; an exited leader never excuses surviving
descendants. Async callers use the existing `run_blocking_io` pool, whose
admitted operations retain ownership when observers are dropped. Its queue and
OS scheduling are outside the command's exit/capture observation window.

SDK Rust runtime, tests and qualification callers use these primitives,
including mount recovery, Codex streaming, compiler/tool checks and parallel
qualification writers. The remaining direct process launches are platform
admission internals, independently durable service bootstrap, and descendant
fixtures that deliberately inherit their parent's containment. JavaScript and
shell build/release or external CI orchestrators remain outside this Rust SDK
ownership boundary; no command-execution binding or new crate is introduced.

A Windows service started by a client inside a non-breakaway Job remains in
that Job even when the client exits; detachment does not escape Job ownership.
Service-backed qualification sessions retain their client process trees until
authenticated service drain and then explicitly terminate the retained trees.
Linux service drain instead follows client cleanup because its service starts
a separate session and surviving clients can hold FUSE mounts busy.

On Darwin, group signalling returns EPERM for a zombie-only group. Cleanup
accepts that case only when a kernel membership snapshot is empty or contains
exactly the owned, independently observed exited leader. Additional members or
denied snapshot authority retain the error. Additional members get a separate
five-second window for OS reaping, with the same membership proof checked again;
EPERM itself never proves exit. Windows admission failures attempt
both Job and direct-child cleanup with bounded observation, and report
unresolved cleanup explicitly; failed admission is not a rollback guarantee.
Drop is best-effort and cannot report cleanup failure. Successful explicit
cleanup is required wherever a caller claims observed completion.

| Invariant | Production mechanism | Assumptions | Verification / evidence |
| --- | --- | --- | --- |
| Windows successful cleanup leaves no executing owned descendant | Suspended spawn, non-breakaway kill-on-close Job, termination followed by `ActiveProcesses == 0` | Trusted OS Job semantics; no transfer of Job authority | Real child/grandchild termination, drop and exited-parent tests; Windows lane |
| Unix cleanup kills descendants remaining in the owned group and reaps the direct child | Process group SIGKILL and direct-child polling | Trusted OS; descendants do not escape groups/sessions; OS eventually completes termination and reparents/reaps descendants | Same real fixtures; existing Linux/macOS lanes. Signal success is not proof of descendant reaping |
| Unix exit observation cannot release the leader's PID/PGID before group termination | `waitid(WNOWAIT)` observes status; only cleanup reaps the retained leader | Exclusive child ownership; host does not install a competing reaper or `SIGCHLD` auto-reaping policy | Real repeatable exit observation, nonzero exit-code and signal-status fixture; Linux/macOS lanes |
| Repeated completed cleanup has no further OS effect | Guard becomes inactive only at its platform completion boundary; child removed only after observed exit | Exclusive owner; no reuse of containment before cleanup | Idempotence fixture |
| Captured payload never exceeds the shared byte budget; overflow is an error | Checked subtraction before append; fallible exact reservation | Exclusive pipe readers; allocator/OS calls progress | Real two-stream fixture and flood rejection |
| Deadline expires without detached readers or successful rollback claims | Nonblocking Unix reads / Windows pipe peek; separate five-second cleanup observation windows; errors retain child ownership | OS calls and scheduling progress; no hard real-time return guarantee | Real timeout and descendant-held-pipe fixtures; Windows reduced-rights Job test establishes cleanup-error precedence and retained ownership |
| Host-configured environment is preserved | Command builder owns `env_clear` and explicit `env` policy; containment does not add environment entries | Trusted host builder applies the required policy | Cleared-environment real fixture with explicit input and closed stdin; generic primitive provides no default credential policy |
| Graceful cleanup still ends descendants after early leader exit | Unix SIGTERM request, retained leader observation, then mandatory SIGKILL/Job cleanup | Same containment and exclusive-reaping assumptions; OS progress | Real exited-leader and grandchild fixture on Windows/Linux/macOS; Codex background-descendant regression |
| Streaming Codex records cannot grow without a per-record bound or hide read errors as EOF | Eight-MiB record reader; explicit failure variant; reader abort on owner drop | Tokio runtime progresses; transcript limits remain host policy | Real oversized/invalid UTF-8 child streams; executor acceptance suite |

These are implementation checks and test evidence, not machine-checked or
unrestricted correctness proofs. Reproduce with `cargo test -p
acyclic-native-runtime --locked --lib` and `cargo clippy -p
acyclic-native-runtime --locked --all-targets -- -D warnings` on each platform.
The existing ignored I/O performance baseline is not a process gate.
