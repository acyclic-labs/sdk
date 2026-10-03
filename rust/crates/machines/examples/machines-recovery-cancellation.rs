//! Demonstrates retained idempotency, operation recovery, and terminal cancel
//! behavior using only the deterministic process-local simulator.
//!
//! Remote `Machines::connect`/`Machines::from_env` selects gRPC over HTTPS with
//! mutual TLS by default; a `unix:` endpoint is the explicit local override.

use std::sync::Arc;

use acyclic_machines::{
    CreateMachine, IdempotencyKey, Image, Machines, MutationOutcome, OperationPhase,
    ProviderAssurance, SimulatedMachines,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(SimulatedMachines::default());
    let machines = Machines::new(provider);
    assert_eq!(
        machines.assurance(),
        ProviderAssurance::ProcessLocalSimulation
    );

    let key = IdempotencyKey::new();
    let request = CreateMachine::new(key, Image::custom([7; 32])?, [8; 32]);
    let _machine = machines.create(request).await?;

    // Reuse the exact caller identity after admission to recover the outcome
    // and resolve the corresponding operation.
    assert!(matches!(
        machines.recover(key).await?,
        MutationOutcome::Created(_)
    ));
    let operation = machines.operation_for(key).await?;
    let observed = machines.inspect_operation(operation).await?;
    assert_eq!(observed.phase, OperationPhase::Succeeded);

    // The simulator has already reached a terminal state, so cancellation is
    // observed as the same terminal state rather than inventing a new result.
    let cancelled = machines.cancel_operation(operation).await?;
    assert_eq!(cancelled.id, operation);
    assert_eq!(cancelled.phase, OperationPhase::Succeeded);
    Ok(())
}
