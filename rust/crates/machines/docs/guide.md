# Machines Rust guide

`acyclic-machines` is the typed Rust contract for immutable image
qualification, machine lifecycle mutations, checkpoints, forks, operation
recovery, event pages, and usage receipts. `MachinesProvider` is the provider
boundary. `SimulatedMachines` is a deterministic process-local implementation
for bounded tests.

## Install and connect

Add `acyclic-machines` as a single dependency. The default package setup
selects the native gRPC client automatically:

```toml
[dependencies]
acyclic-machines = "0.2"
```

The `examples/machines-recovery-cancellation.rs` example exercises the
recovery scenario with `SimulatedMachines` and records process-local
assurance. A remote caller can use `Machines::connect` or `Machines::from_env`
without selecting a transport feature.

The package declaration is owned by `rust/crates/machines/Cargo.toml` and
pins version `0.2.0` with Rust `1.92` as its minimum toolchain. Its default
build reads `src/generated/acyclic-machines-v1.model.docs.bin` for generated
Rust comments. The source-info-free
`src/generated/acyclic-machines-v1.model.bin` remains the canonical model
descriptor, and `src/generated/acyclic-machines-v1.bin` remains the archived
runtime handshake fixture. The documentation overlay does not alter wire
fields, options, or handshake bytes.

## Capability, error, and operation policy

`ImageQualification.capabilities` is the provider's exact capability result;
`Capability`, `CompatibilityPolicy`, and `MachineContract::fork_fidelity`
control which lifecycle and fork requests may be admitted. The default
`SimulatedMachines` provider reports process-local capabilities for bounded
tests. A customer-hosted or managed implementation must supply its own
qualification result and assurance value.

The provider boundary exposes `ProviderError` variants for not-found,
conflict, unsupported, invalid, rejected, unavailable, failed, cancelled,
and indeterminate outcomes. `IdempotencyKey` binds a mutation intent;
`recover`, `recover_operation`, `inspect_operation`, `cancel`, and
`watch_operation` are the recovery policy for an uncertain mutation. The
current public model does not emit a shared `OperationPolicy` message; these
typed Rust APIs are the source-owned operation policy.

Remote Machines clients default to gRPC over HTTPS with mandatory mutual TLS
through `Machines::connect` or `Machines::from_env`; an endpoint beginning
with `unix:` is the explicit local socket override. The native policy has no
alternate Machines transport or browser transport. Machines exposes one gRPC
service with 19 RPC methods and no HTTP route table.
`ProviderAssurance` records whether an implementation is process-local,
customer-hosted, or managed.

## Run a bounded local lifecycle

This example uses only `SimulatedMachines`. It qualifies a nonzero image,
creates one machine, inspects it, checkpoints it, and reads its event page.
All identities are retained by the caller and the process-local assurance is
asserted explicitly.

Compile this exact fence with the path declaration above and a locked
workspace or package lock before executing it. A packaged consumer receipt
should retain the archive, extracted `Cargo.lock`, fence bytes, and command
output together.

```rust
use std::{num::NonZeroU32, sync::Arc};

use acyclic_machines::{
    CreateMachine, IdempotencyKey, Image, Machines, ProviderAssurance,
    SimulatedMachines,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(SimulatedMachines::default());
    let machines = Machines::new(provider);
    assert_eq!(machines.assurance(), ProviderAssurance::ProcessLocalSimulation);

    let image = Image::custom([7; 32])?;
    let qualification = machines.qualify_image(image.clone()).await?;
    assert!(!qualification.capabilities.is_empty());

    let request = CreateMachine::new(IdempotencyKey::new(), image, [8; 32]);
    let machine = machines.create(request).await?;
    let observation = machine.inspect().await?;
    assert_eq!(observation.state, acyclic_machines::MachineState::Running);

    let checkpoint = machine.checkpoint(IdempotencyKey::new()).await?;
    let children = checkpoint
        .fork(NonZeroU32::new(1).ok_or("zero fork count")?, IdempotencyKey::new())
        .await?;
    assert_eq!(children.len(), 1);

    let events = machine.events(None, 16).await?;
    assert!(!events.events.is_empty());
    Ok(())
}
```

The companion `examples/machines-recovery-cancellation.rs` retains one `IdempotencyKey`, recovers the admitted outcome, resolves its `OperationId`, inspects it, and demonstrates that cancelling an already terminal simulator operation preserves its terminal state.

`CreateMachine::new` defaults to a 15-second idle suspension policy and no
expiry. `Machine::checkpoint` returns an immutable `Checkpoint`; checkpoint
fork produces fresh machine identities. `IdempotencyKey` binds one mutation
intent, so callers should replay that same key after an uncertain result and
inspect the associated operation rather than silently issuing a new intent.

## Live forks, lifecycle, and recovery

`Machine::fork` requests a live fork without an intermediate checkpoint.
`MachineContract::fork_fidelity` reports the declared result: `LiveFork`
means `ForkFidelity::MemoryAndDisk`; `DiskFork` means `ForkFidelity::DiskOnly`
and a fresh child boot over provider-defined persistent disk. A provider with
neither capability returns `ProviderError::Unsupported`; use checkpoint plus
fork or restart. Children receive fresh identities and endpoints and open
network connections are not guaranteed to survive. Destroy live-fork children
before their source because a provider may reject the reverse order.

`Machines::operation_for`, `inspect_operation`, `cancel_operation`, and
`watch_operation` are the recovery surface for admitted mutations. A
`ProviderError::Indeterminate` or `ProviderError::OperationIndeterminate` result requires
operation inspection; it is not proof that the mutation failed. Event pages
use a bounded sequence cursor, and usage receipts cover a half-open Unix
millisecond interval with provider-specific assurance for lineage bytes.

## Legacy topic coverage

The website ledger preserves an overview plus these authored topics:
`durability-recovery`, `elastic-capacity`, `forks-checkpoints`, `images`,
`lifecycle`, `networking`, `performance-billing`, `quickstart`, `reference`,
and `security-limits`. Rust sources map them as follows:

| Legacy topic | Rust authority |
| --- | --- |
| overview and quickstart | `Machines`, `MachinesProvider`, `SimulatedMachines` |
| images and capability qualification | `Image`, `ImageDigest`, `ImageQualification`, `Capability` |
| lifecycle and suspension | `Machine`, `MachineState`, `SuspensionPolicy`, `ExpirationPolicy` |
| forks and checkpoints | `Machine::fork`, `Machine::checkpoint`, `Checkpoint`, `ForkFidelity` |
| durability and recovery | `IdempotencyKey`, `OperationObservation`, `Machines::recover` |
| elastic capacity and limits | `Budgets`, `MAX_PAGE_SIZE`, `MAX_FORK_CHILDREN`, `MAX_EVENT_PAGE_SIZE` |
| networking and billing | `Endpoint`, `UsageReceipt`, network-policy digest and provider receipts |
| security and assurance | `ProviderAssurance`, `ProviderError`, provider implementation boundary |

The generated `FILE_DESCRIPTOR_SET` and `wire` module describe the protocol;
Machines exposes its gRPC transport through the client API. Process-local
simulation and provider implementations use the same contract types.

