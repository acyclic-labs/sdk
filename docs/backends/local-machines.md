# Local Machines

Enable `acyclic-machines/local`. The public factory binds one existing,
source-admitted host workspace and its exact Machine incarnation:

```rust,ignore
let provider = acyclic_machines::local::provider(
    observation.clone(), native_options, streams, journal_path,
).await?; // Arc<dyn MachinesProvider>
let host = MachinesHost::new(provider, provider_ref)?;
let tools = host.workspace(observation, &selected_volume, original_scope, verifier)?
    .with_shell(resolved_shell, &acyclic_host_platform::shell::PLATFORM)?;
// The server and local application both install these same workspace tools.
bindings.workspace_tools = Some(Arc::new(tools));
```

`native_options` is `local::LocalOptions { cwd, environment,
inherit_environment, process_custody }`, from the existing host admission.
The observation's selected volume is that same canonical `VolumeRef`.
The workspace consumes that original observation, checks its exact Machine and
selected volume once, and retains the selection. No inspection RPC is added.
The factory supplies no filesystem authority, root issuer or second executor.
File tools retain the task's existing authenticated content bindings.
Hosted Linux uses the same `MachinesWorkspace` with its default bash selection.

Exec moves binary stdin into the original native pipe owner, then streams
stdout/stderr and the exact terminal exit or stopped capture. Dropping output
closes credit and wakes native cleanup, including a silent or stalled child.
One queued batch, the existing ProcessTree and its configured custody perform
execution; Machines adds no process or payload copy to this handoff. Relative
timeout starts at admission. Authenticated expiry uses the shared wall alarm.

ReadFile/WriteFile use native positional IO with at most 64 KiB per chunk.
There is no shell encoding, write replay or per-write durability barrier.
Only regular files are admitted; Unix FIFO opens are nonblocking. The original
deadline bounds observation, including a stalled filesystem. A timed-out,
stopped or abandoned transfer retains its physical worker lease and latches
uncertainty, so shutdown cannot attest cleanup before actual completion.
Suspend stops and joins admitted commands while retaining the workspace;
Wake admits fresh commands; Destroy is terminal. A physical completion callback
releases live execution admission independently of retained output observations.
Unproved cleanup remains uncertainty on the original operation, never success.

The shared core publishes keyed results and Machine events atomically through
the supplied Streams provider, whose persistence guarantees remain authoritative.
Enrollment conditionally publishes once before exposing the physical provider.
Clone that provider's Arc to share it. Reopening a live incarnation is refused,
including after owner loss; it cannot manufacture a new process owner beside
the original. A destroyed incarnation can reopen for retained result recovery,
without execution authority. An abandoned pending physical operation is
explicitly indeterminate and is never dispatched again. This is a scoped
workspace provider, not a fleet allocator. It does not change the admitted
volume/image, freeze process memory, snapshot a mutable tree, attest billing or
provide tenant isolation. Processes retain the host user's OS privileges.
Automatic suspension/expiry and snapshot capabilities are refused at enrollment.

Run `cargo test -p acyclic-machines --features conformance,local --lib`.
Real-process controls cover binary channels/input, exit, source buffer identity,
stop with withheld output credit, file offsets, expired writes, conditional
single-owner enrollment, FIFO refusal, blocked physical IO, original-result
recovery after terminal reopening and failed cleanup refusing destruction.
These controls do not establish the full lifecycle suite's snapshot/fork
requirements, crash-persistent storage or hosted guest volume enrollment.
