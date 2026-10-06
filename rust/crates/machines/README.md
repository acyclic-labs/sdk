# acyclic-machines

Machines protocol 1.1 is shape-free. Customers choose an immutable image and
policies; the platform manages CPU, memory, and disk capacity. Create and
checkpoint fork no longer accept an Elastic/Dedicated performance selection.
The removed protobuf field numbers and names are reserved. Servers must reject
the predecessor protocol revision rather than reinterpret its requests.

Typed Machines contract for immutable image qualification, machine lifecycle, checkpoints, forks, operation recovery, events, and usage receipts.

The `cargo add acyclic-machines` block is a caller manifest install instruction, not an offline qualification command; local checks use the source path and lockfile.

```sh
cargo add acyclic-machines
```

`Machines` wraps a `MachinesProvider`; remote clients default to gRPC over HTTPS with mutual TLS through `Machines::connect` or `Machines::from_env`. An endpoint beginning with `unix:` is the explicit local socket override; the native policy has no alternate Machines transport, and no browser transport is claimed. `SimulatedMachines` is the process-local contract provider. Qualify an immutable image digest before creation and retain idempotency keys for mutation recovery. Inspect an operation when its outcome is indeterminate rather than assuming failure.

`Machine::fork` (`MachinesProvider::fork_machine`) forks a *running* machine into fresh children at any moment, with no checkpoint step. Its fidelity is declared, not inferred: `Capability::LiveFork` means children resume from the source's memory, processes, and disk (`ForkFidelity::MemoryAndDisk`); `Capability::DiskFork` means they boot fresh over a copy of its disk (`ForkFidelity::DiskOnly`) and the caller restarts its workload. A machine with neither returns `ProviderError::Unsupported`; fall back to `checkpoint` plus `fork`, or a restart. Children inherit the source's contract, credentials, and environment, get fresh identities and endpoints, and never inherit open network connections. Destroy children before their source: a provider may refuse the reverse order with `ProviderError::Conflict`. The trait method documents the full semantics.

The source-owned [Rust guide](docs/guide.md) maps the legacy topics to these APIs. The executable `examples/machines-recovery-cancellation.rs` demonstrates retained idempotency, recovery, and terminal cancellation against the process-local simulator.

`SimulatedMachines` is a bounded process-local state machine. It does **not** execute an operating system, isolate tenants, provide durability, or guarantee availability. Real providers must state their own assurance level. See the [Rust API](https://docs.rs/acyclic-machines/latest/acyclic_machines/) and [v1 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/machines).
