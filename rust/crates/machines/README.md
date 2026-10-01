# acyclic-machines

Machines protocol 1.1 is shape-free. Customers choose an immutable image and
policies; the platform manages CPU, memory, and disk capacity. Create and
checkpoint fork no longer accept an Elastic/Dedicated performance selection.
The removed protobuf field numbers and names are reserved. Servers must reject
the predecessor protocol revision rather than reinterpret its requests.

Typed Machines contract for immutable image qualification, machine lifecycle, checkpoints, forks, operation recovery, events, and usage receipts.

```sh
cargo add acyclic-machines
```

`Machines` wraps a `MachinesProvider`; the crate also includes a gRPC client. Qualify an immutable image digest before creation and retain idempotency keys for mutation recovery. Inspect an operation when its outcome is indeterminate rather than assuming failure.

Once a gRPC mutation is admitted, its completion wait has no overall observation deadline. A silent or interrupted watch reconciles the same operation through inspection and resumes observation with a bounded retry delay. It never resubmits the mutation or changes its idempotency key. Authority-reported failure, cancellation and indeterminate outcomes remain visible; authentication and other permanent observation errors return the retained key for recovery. Dropping the waiting future stops local observation without cancelling the native operation. Persist the original idempotency key and recover its operation after process restart; the SDK does not replace the service's durable operation journal.

An `Unknown` observation status may indicate temporary client readiness or a server journal error. An unknown watch always triggers inspection of the same operation. The client returns the retained key after three unknown inspections without an intervening validated operation state. A valid pending state resets this ambiguity count. `Internal` is returned immediately. Silence and ordinary transport unavailability do not consume this count or establish an operation failure.

`Machine::fork` (`MachinesProvider::fork_machine`) forks a *running* machine into fresh children at any moment, with no checkpoint step. Its fidelity is declared, not inferred: `Capability::LiveFork` means children resume from the source's memory, processes, and disk (`ForkFidelity::MemoryAndDisk`); `Capability::DiskFork` means they boot fresh over a copy of its disk (`ForkFidelity::DiskOnly`) and the caller restarts its workload. A machine with neither returns `ProviderError::Unsupported`; fall back to `checkpoint` plus `fork`, or a restart. Children inherit the source's contract, credentials, and environment, get fresh identities and endpoints, and never inherit open network connections. Destroy children before their source: a provider may refuse the reverse order with `ProviderError::Conflict`. The trait method documents the full semantics.

`SimulatedMachines` is a bounded process-local state machine. It does **not** execute an operating system, isolate tenants, provide durability, or guarantee availability. Real providers must state their own assurance level. See the [Rust API](https://docs.rs/acyclic-machines/latest/acyclic_machines/) and [v1 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/machines).
