# Machines v1 Rust contract model

Machines v1 is authored by the Rust model in
[`rust/crates/sdk-contract-wire/src/machines.rs`](../../rust/crates/sdk-contract-wire/src/machines.rs).
The existing runtime descriptor is retained as an immutable compatibility
oracle. Normal generation consumes the structured Rust model and does not parse
`proto/machines/v1/machines.proto`.

## Wire surface

`MACHINES` emits `machines/v1/machines.proto` in package
`acyclic.machines.v1`, with proto3 syntax and Go package
`github.com/acyclic-labs/sdk/go/gen/machines/v1;machinesv1`.

The model preserves 47 messages, 9 enums, and all 19 `MachinesService` RPCs:

`QualifyImage`, `Create`, `Checkpoint`, `Fork`, `ForkMachine`, `Suspend`,
`Wake`, `SetSuspensionPolicy`, `DestroyMachine`, `DestroyCheckpoint`,
`Recover`, `InspectMachine`, `InspectCheckpoint`, `ListMachines`, `Events`,
`Usage`, `Cancel`, `InspectOperation`, and `WatchOperation`.

`WatchOperation` is server streaming. The model retains the `Image` immutable
reference choice, `SuspensionPolicy.policy`, `RecoveredAdmission.result`, and
`MutationOutcome.result` oneofs, as well as reserved `performance` identities
and tag 5 in the published request and contract messages. Machines has no
HTTP route options in the compatibility descriptor, so `MACHINES_ROUTES` is
explicitly empty rather than inventing a transport projection.

The golden check is executable with:

```powershell
cargo test --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --test machines_model
```

The runtime oracle includes compiler source locations and unknown source
metadata. The test strips those annotations from a decoded copy in memory and
compares the canonical Rust descriptor byte-for-byte; the 30,060-byte fixture
itself remains unchanged.

## Semantics and availability

The contract covers image qualification, machine admission, checkpoints,
checkpoint and live-machine forks, suspension and wake transitions, policy and
destruction mutations, recovery, inspection, event history, usage receipts,
and operation cancellation/observation. `ForkMachine` preserves the declared
machine contract while producing fresh children; the `ForkFidelity` enum
records what the provider admitted. Capability and compatibility enums remain
wire values, not inferred SDK behavior.

The protobuf service has no hosted HTTP route metadata. A future HTTP adapter
must add an explicit Rust-owned projection and preserve every RPC’s request,
response, and streaming identity; it must not derive routes from method names.

The shared-core owner must route Machines documentation through the common
renderer. This model owns the messages, enums, oneofs, reserved identities,
RPCs, and compatibility evidence.
