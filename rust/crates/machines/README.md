# acyclic-machines

Machines protocol 1.1 is shape-free. Customers choose an immutable image and
policies; the platform manages CPU, memory, and disk capacity. Create and
checkpoint fork no longer accept an Elastic/Dedicated performance selection.
The removed protobuf field numbers and names are reserved. Servers must reject
the predecessor protocol revision rather than reinterpret its requests.

Typed Machines contract for immutable image qualification, machine lifecycle, checkpoints, forks, operation recovery, events, and usage receipts.

`Machine::exec(ExecCommand::new(argv, timeout_ms)?)` dispatches one bounded guest
command and returns a disposable stream of merged output bytes followed by one
exit code. Dropping the stream cancels its RPC. A lost reply or interrupted stream
returns `ProviderError::CommandIndeterminate(machine)`: the command may have run,
so never redispatch it automatically. Argument validation does not evaluate a
shell; shell execution requires an explicit shell program in `argv`.

`Machine::read_output(offset, limit)` reads the retained live console with exact
`u64` offsets. A later response offset exposes discarded bytes; continue from
`output.offset + output.data.len()`. Empty data means no newer bytes now, not EOF.
The simulator and Daytona adapter return `Unsupported` for guest I/O; neither
fabricates output or a command result.

```sh
cargo add acyclic-machines
```

`Machines` wraps a `MachinesProvider`; the crate also includes a gRPC client. Qualify an immutable image digest before creation and retain idempotency keys for mutation recovery. Inspect an operation when its outcome is indeterminate rather than assuming failure.

`Machine::fork` (`MachinesProvider::fork_machine`) forks a *running* machine into fresh children at any moment, with no checkpoint step. Its fidelity is declared, not inferred: `Capability::LiveFork` means children resume from the source's memory, processes, and disk (`ForkFidelity::MemoryAndDisk`); `Capability::DiskFork` means they boot fresh over a copy of its disk (`ForkFidelity::DiskOnly`) and the caller restarts its workload. A machine with neither returns `ProviderError::Unsupported`; fall back to `checkpoint` plus `fork`, or a restart. Children inherit the source's contract, credentials, and environment, get fresh identities and endpoints, and never inherit open network connections. Destroy children before their source: a provider may refuse the reverse order with `ProviderError::Conflict`. The trait method documents the full semantics.

`SimulatedMachines` is a bounded process-local state machine. It does **not** execute an operating system, isolate tenants, provide durability, or guarantee availability. Real providers must state their own assurance level. See the [Rust API](https://docs.rs/acyclic-machines/latest/acyclic_machines/) and [v1 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/machines).
