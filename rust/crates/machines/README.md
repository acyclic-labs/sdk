# acyclic-machines

Typed Machines contract for immutable image qualification, machine lifecycle, checkpoints, forks, operation recovery, events, and usage receipts.

```sh
cargo add acyclic-machines
```

`Machines` wraps a `MachinesProvider`; the crate also includes a gRPC client. Qualify an immutable image digest before creation and retain idempotency keys for mutation recovery. Inspect an operation when its outcome is indeterminate rather than assuming failure.

`SimulatedMachines` is a bounded process-local state machine. It does **not** execute an operating system, isolate tenants, provide durability, or guarantee availability. Real providers must state their own assurance level. See the [Rust API](https://docs.rs/acyclic-machines/latest/acyclic_machines/) and [v1 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/machines).
