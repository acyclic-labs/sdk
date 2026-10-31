# acyclic-machines

Machines protocol 1.4 is shape-free. Customers choose an immutable image and
policies; the platform manages CPU, memory, and disk capacity. Create and
checkpoint fork no longer accept an Elastic/Dedicated performance selection.
The removed protobuf field numbers and names are reserved. Servers must reject
the predecessor protocol revision rather than reinterpret its requests.

Typed Machines contract for immutable image qualification, machine lifecycle, checkpoints, forks, operation recovery, events, and usage receipts.

Create and AttachVolume select the existing canonical `acyclic.harness.v2.VolumeRef`.
The service admits it through the original verified caller's hosted guest session
and physical incarnation owner, then retains the exact selection in the MachineContract.
The reference carries no capability or guest mapping fence. Settlement and recovery
check that original contract; a substituted selection remains indeterminate under
the original mutation key. The process-local simulator refuses hosted selection.

`Machine::exec(ExecCommand::new(argv, timeout_ms)?)` dispatches one bounded guest
command and returns a disposable stream of separate stdout (`Data`) and stderr
bytes followed by one exit code or a typed stopped capture. A terminal event
completes the stream without waiting for EOF. Dropping the stream cancels its RPC.
The producer must terminate and reap the command on RPC cancellation; a stopped
capture reports whether that cleanup completed. A lost reply or interrupted stream
returns `ProviderError::CommandIndeterminate(machine)`: the command may have run,
so never redispatch it automatically. Argument validation does not evaluate a
shell; shell execution requires an explicit shell program in `argv`.

Commands can carry binary stdin, a guest cwd, a combined output allowance and an
absolute authority expiry. Native clients reuse the shared wall-clock alarm so
expiry also wakes silent commands. Client-side expiry reports uncertainty; only
the producer can attest physical cleanup. Protocol 1.1 merged channels and cannot
satisfy this contract: a server must refuse unsupported revisions before dispatch.
`Machine::read_file` and `write_file` call the native binary file RPCs. Each
`FileAccess` carries an absolute guest path, a `u64` offset and one original
absolute deadline. Chunks are at most 65536 bytes; file size is unbounded.
Responses preserve short IO and nonzero POSIX errno. An interrupted write may be
partial and returns `FileIndeterminate(machine)` when completion is unknown.
Clients never repeat it automatically. Truncation is explicit, at offset zero.

Every `MachineContract` states its `Platform` (OS, architecture, device kind,
shell and path style) and its `LifecycleClass`. A `Provisioned` Machine is one
the provider creates and owns. An `Attached` Machine is an existing device the
provider only reaches: create, checkpoint, fork, suspend, wake and suspension
policy changes return `Unsupported`. Destroy detaches it: it stops the commands
dispatched through that Machine binding, whose authority ends with the binding,
and touches nothing else on the device. An unreachable attached device reports `MachineState::Offline`; wake and
exec on it return `Unavailable`.

Exec, read and write on a provisioned Machine suspended under
`SuspensionPolicy::AfterIdle` wake it first, inside the same call and under the
same deadline. The implicit wake is a journaled transition like an explicit one.
A manually suspended Machine is refused with `Conflict`. The timeout is `u64`
milliseconds, so it represents every shell timeout without clamping; a deadline
the clock cannot represent is rejected.

`Machine::read_output(offset, limit)` reads the retained live console with exact
`u64` offsets. A later response offset exposes discarded bytes; continue from
`output.offset + output.data.len()`. Empty data means no newer bytes now, not EOF.
The simulator returns `Unsupported` for guest I/O and never fabricates output or
a command result. Its exec still performs admission first, including an implicit
wake. `SimulatedMachines::attach` enrolls an attached device and `set_online`
moves it between Running and Offline.

```sh
cargo add acyclic-machines
```

`Machines` wraps a `MachinesProvider`; the crate also includes a gRPC client. Qualify an immutable image digest before creation and retain idempotency keys for mutation recovery. Inspect an operation when its outcome is indeterminate rather than assuming failure.

Once a gRPC mutation is admitted, its completion wait consumes the original
operation watch. Its producer publishes terminal success, failure or cancellation;
silence causes no inspection or subscription restart. An interrupted watch returns
the original idempotency key for explicit recovery and never resubmits the mutation.
Create, AttachVolume, checkpoint and fork take their settled observations from
the successful operation event's retained outcome. Recovery uses that original
result, so later resource changes or deletion cannot replace it. No current-state
read is needed for settlement; a missing original result remains indeterminate.
Dropping the waiting future stops local observation without cancelling the native
operation. Persist the original key for recovery after process restart; the SDK
uses the service's durable operation journal.

`results::ResultCursor` binds the original operation, terminal owner, immutable
extent and child index. Its `Settlement` distinguishes a Streams `CommitId` from
a native controller partition's nonzero revision; neither is interchangeable
with the other or with a watch watermark. `SettledResults` verifies the original
Streams fork cut once. `NativeResults` borrows the original controller's retained
observation slice, including canonical protobuf observations, and selects each
credited page without allocation, copying or IO. Both validate the same cursor
before reading payload. These helpers grant no authority: the server must retain
the original authenticated partition and verify terminal success. Native outcome
decoding and transport serialization remain the producer's work; the borrowed
view alone does not make a complete-array storage format lazy.

`Machine::fork` (`MachinesProvider::fork_machine`) forks a *running* machine into fresh children at any moment, with no checkpoint step. Its fidelity is declared, not inferred: `Capability::LiveFork` means children resume from the source's memory, processes, and disk (`ForkFidelity::MemoryAndDisk`); `Capability::DiskFork` means they boot fresh over a copy of its disk (`ForkFidelity::DiskOnly`) and the caller restarts its workload. A machine with neither returns `ProviderError::Unsupported`; fall back to `checkpoint` plus `fork`, or a restart. Children inherit the source's contract, credentials, and environment, get fresh identities and endpoints, and never inherit open network connections. Destroy children before their source: a provider may refuse the reverse order with `ProviderError::Conflict`. The trait method documents the full semantics.

`SimulatedMachines` is a bounded process-local state machine. It does **not** execute an operating system, isolate tenants, provide durability, or guarantee availability. Real providers must state their own assurance level. See the [Rust API](https://docs.rs/acyclic-machines/latest/acyclic_machines/) and [v1 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/machines).

With the `conformance` feature, downstream providers run the same public
`acyclic_machines::conformance::{Backend, Fixture, verify, bench, suite}`.
`Fixture::new(provider, image, network_policy)` wraps the real provider in the shared
`acyclic_stream::testing::Counted<P>` counter before type erasure. Factories retain
their services and disposable storage for the whole invocation, and supply the
actual enrolled immutable image and network policy commitment. The suite uses
those resources for every create without substituting their identities. Register an async
factory with `acyclic_machines::conformance::suite!(MyBackend::fresh)`.

The suite checks capability negotiation, lifecycle, operation recovery/watch,
idempotent replay, checkpoint and live fork fidelity, wake on exec, and exact
stable event reads. Providers of attached devices also run `verify_attached`. The checked benchmark measures event reads, with zero control calls after
provisioning; it does not claim simulator guest execution. Failed dispatches are
counted and keep their original error. Instrument the transport boundary as well
when qualifying physical request traffic. Run the reference adapter on demand:

```sh
cargo test --locked -p acyclic-machines --features conformance --lib conformance::
```
