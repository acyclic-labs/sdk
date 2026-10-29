//! Public, backend-independent Machines qualification and request budgets.

use crate::{
    Capability, CompatibilityPolicy, CreateMachine, IdempotencyKey, Image, MachineObservation,
    MachineState, MachinesProvider, MutationOutcome, OperationPhase, ProviderError,
};
use acyclic_stream::testing::{Counted, RequestLedger, Requests};
use futures::StreamExt as _;
use std::num::NonZeroU32;

mod counted;

/// Canonical language-neutral Machines inventory, shipped with this crate.
pub const SUITE: &[u8] = include_bytes!("../conformance/machines.json");

/// Fresh production provider and its dispatch accounting. Retain all temporary
/// resources in this value for the lifetime of a suite or benchmark invocation.
pub trait Backend: Send + Sync {
    /// Provider under test.
    fn provider(&self) -> &dyn MachinesProvider;
    /// Ledger measured at the provider or instrumented transport boundary.
    fn requests(&self) -> RequestLedger;
    /// Immutable image enrolled by this fixture's production provider.
    fn image(&self) -> &Image;
    /// Enrolled network policy commitment used by every create request.
    fn network_policy(&self) -> [u8; 32];
}

/// Wraps a concrete provider before type erasure, preserving all provider errors.
pub struct Fixture<P> {
    provider: Counted<P>,
    image: Image,
    network_policy: [u8; 32],
}
impl<P> Fixture<P> {
    /// Starts independent accounting with the provider's enrolled test resources.
    pub fn new(provider: P, image: Image, network_policy: [u8; 32]) -> Self {
        Self {
            provider: Counted::new(provider),
            image,
            network_policy,
        }
    }
}
impl<P: MachinesProvider> Backend for Fixture<P> {
    fn provider(&self) -> &dyn MachinesProvider {
        &self.provider
    }
    fn requests(&self) -> RequestLedger {
        self.provider.ledger()
    }
    fn image(&self) -> &Image {
        &self.image
    }
    fn network_policy(&self) -> [u8; 32] {
        self.network_policy
    }
}

/// Runs all lifecycle, replay, capability, fork, operation and payload-read cases.
pub async fn verify(backend: &dyn Backend) -> Result<(), String> {
    verify_provider(
        backend.provider(),
        backend.image().clone(),
        backend.network_policy(),
    )
    .await?;
    workload(backend, 3, "b0").await?;
    Ok(())
}

/// Checked event-history read workload. Provisioning and teardown are excluded;
/// this does not claim to measure guest execution in a process-local simulator.
#[derive(Debug)]
pub struct Measurement {
    /// Complete stable event-history reads.
    pub reads: usize,
    /// Native monotonic elapsed time.
    pub elapsed: std::time::Duration,
    /// Calls within the steady read interval, separated by class.
    pub requests: Requests,
}

/// Performs 256 exact history reads on a freshly provisioned machine. Use fresh
/// storage independently of verify. No metadata, list, head, lease or token calls
/// are allowed in the measured interval.
pub async fn bench(backend: &dyn Backend) -> Result<Measurement, String> {
    workload(backend, 256, "c0").await
}

async fn workload(
    backend: &dyn Backend,
    rounds: usize,
    suffix: &str,
) -> Result<Measurement, String> {
    let provider = backend.provider();
    let ledger = backend.requests();
    let key = |tail: &str| {
        IdempotencyKey::parse(&format!("00000000-0000-0000-0000-000000{suffix}{tail}"))
            .map_err(|error| error.to_string())
    };
    let created = provider
        .create(CreateMachine::new(
            key("0001")?,
            Some(backend.image().clone()),
            backend.network_policy(),
        ))
        .await
        .map_err(|error| error.to_string())?;
    let MutationOutcome::Created(machine) = created else {
        return Err("read workload create returned the wrong outcome".into());
    };
    // Bound the history to the machine's initial event. Concurrent hosted events
    // may extend the history but may never change this first committed fact.
    let expected = provider
        .events(machine.id, None, 1)
        .await
        .map_err(|error| error.to_string())?;
    if expected.events.len() != 1 {
        return Err("created machine has no initial lifecycle event".into());
    }
    let before = ledger.requests();
    let started = std::time::Instant::now();
    let result = async {
        for _ in 0..rounds {
            let before = ledger.requests();
            let observed = provider
                .events(machine.id, None, 1)
                .await
                .map_err(|error| error.to_string())?;
            let delta = ledger.requests().since(before);
            if delta.control() != 0 || delta.data() > 1 {
                return Err(format!("event read request budget exceeded: {delta:?}"));
            }
            if observed.events != expected.events {
                return Err("event read substituted the first committed lifecycle fact".into());
            }
        }
        Ok(Measurement {
            reads: rounds,
            elapsed: started.elapsed(),
            requests: ledger.requests().since(before),
        })
    }
    .await;
    let cleanup = provider
        .destroy_machine(machine.id, key("0002")?)
        .await
        .map_err(|error| error.to_string());
    let measured = result?;
    if cleanup? != MutationOutcome::MachineDestroyed(machine.id) {
        return Err("read workload teardown substituted its outcome".into());
    }
    Ok(measured)
}

/// Runs this same public suite with an async factory returning a fresh Backend.
pub use crate::machines_conformance_suite as suite;
/// Implementation of [`suite!`].
#[macro_export]
macro_rules! machines_conformance_suite {
    ($factory:path) => {
        $crate::machines_conformance_suite!(machines_conformance, $factory);
    };
    ($name:ident, $factory:path) => {
        #[tokio::test]
        async fn $name() -> std::result::Result<(), String> {
            let backend = $factory().await.map_err(|error| error.to_string())?;
            $crate::conformance::verify(&backend).await
        }
    };
}

/// Exercises the public shape-free lifecycle, replay, fork, and observation semantics.
#[allow(
    clippy::too_many_lines,
    reason = "linear conformance walkthrough; each step is a distinct sequential assertion \
              (qualification, capability negotiation, create/replay, operation lifecycle, \
              checkpoint, fork, suspend/wake, events, usage, teardown) threaded through shared \
              local state, and splitting it would only move the same checks behind indirection"
)]
async fn verify_provider(
    provider: &dyn MachinesProvider,
    image: Image,
    network_policy: [u8; 32],
) -> Result<(), String> {
    let key = |suffix: u8| {
        IdempotencyKey::parse(&format!("00000000-0000-0000-0000-0000000000{suffix:02x}"))
            .map_err(|error| error.to_string())
    };
    let assurance = provider.assurance();
    let qualification = provider
        .qualify_image(image.clone())
        .await
        .map_err(|error| error.to_string())?;
    if qualification.image != image {
        return Err("image qualification substituted its immutable image".into());
    }
    let capability_cases = [
        (Capability::ElasticCpu, 0x10, 0x20),
        (Capability::ElasticMemory, 0x11, 0x21),
        (Capability::LiveCheckpoint, 0x12, 0x22),
        (Capability::LiveFork, 0x13, 0x23),
        (Capability::SuspendResume, 0x14, 0x24),
        (Capability::LiveMovement, 0x15, 0x25),
        (Capability::DiskFork, 0x16, 0x26),
    ];
    for (capability, create_suffix, destroy_suffix) in capability_cases {
        let required = std::collections::BTreeSet::from([capability]);
        let mut capability_request =
            CreateMachine::new(key(create_suffix)?, Some(image.clone()), network_policy);
        capability_request.compatibility = CompatibilityPolicy::Require(required.clone());
        let admission = provider.create(capability_request).await;
        if qualification.capabilities.contains(&capability) {
            let MutationOutcome::Created(observation) =
                admission.map_err(|error| error.to_string())?
            else {
                return Err("supported capability returned the wrong create outcome".into());
            };
            if observation.contract.compatibility != CompatibilityPolicy::Require(required)
                || !observation.contract.capabilities.contains(&capability)
            {
                return Err("required capability was not retained in the machine contract".into());
            }
            let destroyed = provider
                .destroy_machine(observation.id, key(destroy_suffix)?)
                .await
                .map_err(|error| error.to_string())?;
            if destroyed != MutationOutcome::MachineDestroyed(observation.id) {
                return Err("capability conformance cleanup substituted its outcome".into());
            }
        } else if !matches!(admission, Err(ProviderError::Unsupported(_))) {
            return Err("unsupported capability intent did not fail explicitly".into());
        }
    }
    let create_key = key(1)?;
    let request = CreateMachine::new(create_key, Some(image), network_policy);
    let created = provider
        .create(request.clone())
        .await
        .map_err(|error| error.to_string())?;
    let MutationOutcome::Created(machine) = created else {
        return Err("create returned the wrong outcome".into());
    };
    if provider
        .create(request)
        .await
        .map_err(|error| error.to_string())?
        != MutationOutcome::Created(machine.clone())
    {
        return Err("create replay changed its outcome".into());
    }
    if machine.state != MachineState::Running || machine.endpoints.len() != 1 {
        return Err("created machine is not ready with one stable endpoint".into());
    }
    let operation = provider
        .recover_operation(create_key)
        .await
        .map_err(|error| error.to_string())?;
    let expected_operation = provider
        .inspect_operation(operation)
        .await
        .map_err(|error| error.to_string())?;
    if expected_operation.id != operation || expected_operation.phase != OperationPhase::Succeeded {
        return Err("create operation inspection is not correlated and terminal".into());
    }
    if provider
        .cancel(operation)
        .await
        .map_err(|error| error.to_string())?
        != expected_operation
    {
        return Err("terminal operation cancellation changed its observation".into());
    }
    let mut operation_stream = provider
        .watch_operation(operation)
        .await
        .map_err(|error| error.to_string())?;
    let watched = tokio::time::timeout(std::time::Duration::from_secs(1), operation_stream.next())
        .await
        .map_err(|_| "operation watch did not make bounded progress".to_owned())?
        .ok_or_else(|| "operation watch ended before its current state".to_owned())?
        .map_err(|error| error.to_string())?;
    if watched != expected_operation {
        return Err("operation watch substituted its requested identity or state".into());
    }
    let checkpointed = provider
        .checkpoint(machine.id, key(2)?)
        .await
        .map_err(|error| error.to_string())?;
    let MutationOutcome::Checkpointed(checkpoint) = checkpointed else {
        return Err("checkpoint returned the wrong outcome".into());
    };
    let forked = provider
        .fork(
            checkpoint.id,
            NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
            key(3)?,
        )
        .await
        .map_err(|error| error.to_string())?;
    let MutationOutcome::Forked(children) = forked else {
        return Err("fork returned the wrong outcome".into());
    };
    let [first_child, second_child] = children.as_slice() else {
        return Err("fork identities are not an exact fresh set".into());
    };
    if first_child.id == second_child.id || children.contains(&machine) {
        return Err("fork identities are not an exact fresh set".into());
    }
    provider
        .suspend(machine.id, key(4)?)
        .await
        .map_err(|error| error.to_string())?;
    if provider
        .inspect_machine(machine.id)
        .await
        .map_err(|error| error.to_string())?
        .state
        != MachineState::Suspended
    {
        return Err("suspend did not change observable state".into());
    }
    provider
        .wake(machine.id, key(5)?)
        .await
        .map_err(|error| error.to_string())?;
    let events = provider
        .events(machine.id, None, 16)
        .await
        .map_err(|error| error.to_string())?;
    let states = events
        .events
        .iter()
        .filter_map(|event| match event.fact {
            crate::EventFact::State(state) => Some(state),
            _ => None,
        })
        .collect::<Vec<_>>();
    if !states.windows(3).any(|states| {
        states
            == [
                MachineState::Running,
                MachineState::Suspended,
                MachineState::Running,
            ]
    }) {
        return Err("lifecycle state event history is incomplete".into());
    }
    let usage = provider
        .usage(machine.id, 1, 2)
        .await
        .map_err(|error| error.to_string())?;
    if usage.machine != machine.id
        || (assurance == crate::ProviderAssurance::ProcessLocalSimulation)
            != usage.receipt.is_empty()
    {
        return Err("simulation usage receipt is malformed".into());
    }
    live_fork(provider, &machine).await?;
    provider
        .destroy_checkpoint(checkpoint.id, key(6)?)
        .await
        .map_err(|error| error.to_string())?;
    if provider
        .inspect_checkpoint(checkpoint.id)
        .await
        .map_err(|error| error.to_string())?
        .forkable
    {
        return Err("destroyed checkpoint still accepts forks".into());
    }
    provider
        .destroy_machine(machine.id, key(7)?)
        .await
        .map_err(|error| error.to_string())?;
    if provider
        .inspect_machine(machine.id)
        .await
        .map_err(|error| error.to_string())?
        .state
        != MachineState::Destroyed
    {
        return Err("destroy did not retain its terminal state".into());
    }
    Ok(())
}

/// Live fork of a running machine: declared fidelity, exact fresh child set, exact replay,
/// rebinding rejection, and children joined before their source.
async fn live_fork(
    provider: &dyn MachinesProvider,
    machine: &MachineObservation,
) -> Result<(), String> {
    let key = |suffix: u8| {
        IdempotencyKey::parse(&format!("00000000-0000-0000-0000-0000000000{suffix:02x}"))
            .map_err(|error| error.to_string())
    };
    let count = NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN);
    let admission = provider.fork_machine(machine.id, count, key(0x30)?).await;
    let Some(expected) = machine.contract.fork_fidelity() else {
        return if matches!(admission, Err(ProviderError::Unsupported(_))) {
            Ok(())
        } else {
            Err("live fork without a declared capability did not fail as unsupported".into())
        };
    };
    let outcome = admission.map_err(|error| error.to_string())?;
    let MutationOutcome::MachineForked {
        source,
        fidelity,
        children,
    } = &outcome
    else {
        return Err("live fork returned the wrong outcome".into());
    };
    if *source != machine.id || *fidelity != expected {
        return Err("live fork substituted its source or declared fidelity".into());
    }
    let [first, second] = children.as_slice() else {
        return Err("live fork identities are not an exact fresh set".into());
    };
    if first.id == second.id || first.id == machine.id || second.id == machine.id {
        return Err("live fork identities are not an exact fresh set".into());
    }
    for child in children {
        if child.state != MachineState::Running
            || child.contract != machine.contract
            || child.last_checkpoint.is_some()
        {
            return Err("live fork child did not inherit the running source contract".into());
        }
    }
    if provider
        .fork_machine(machine.id, count, key(0x30)?)
        .await
        .map_err(|error| error.to_string())?
        != outcome
    {
        return Err("live fork replay changed its outcome".into());
    }
    if !matches!(
        provider
            .fork_machine(machine.id, NonZeroU32::MIN, key(0x30)?)
            .await,
        Err(ProviderError::Conflict(_))
    ) {
        return Err("live fork idempotency key was rebound".into());
    }
    if provider
        .inspect_machine(machine.id)
        .await
        .map_err(|error| error.to_string())?
        .state
        != MachineState::Running
    {
        return Err("live fork source did not keep running".into());
    }
    for (suffix, child) in [(0x31, first), (0x32, second)] {
        if provider
            .destroy_machine(child.id, key(suffix)?)
            .await
            .map_err(|error| error.to_string())?
            != MutationOutcome::MachineDestroyed(child.id)
        {
            return Err("live fork child join substituted its outcome".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SimulatedMachines;
    use acyclic_stream::testing::RequestClass;
    async fn simulated() -> Result<Fixture<SimulatedMachines>, String> {
        Ok(Fixture::new(
            SimulatedMachines::default(),
            Image::custom([17; 32]).map_err(|error| error.to_string())?,
            [19; 32],
        ))
    }
    suite!(public_simulated_suite, simulated);

    #[tokio::test]
    async fn live_fork_fidelities() -> Result<(), String> {
        for capabilities in [
            std::collections::BTreeSet::from([Capability::DiskFork, Capability::SuspendResume]),
            std::collections::BTreeSet::from([Capability::SuspendResume]),
        ] {
            verify(&Fixture::new(
                SimulatedMachines::with_capabilities(capabilities),
                Image::custom([17; 32]).map_err(|error| error.to_string())?,
                [19; 32],
            ))
            .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn benchmark_and_failure_counts() -> Result<(), String> {
        let fixture = simulated().await?;
        let measured = bench(&fixture).await?;
        assert_eq!(measured.reads, 256);
        assert_eq!(measured.requests.data(), 256);
        assert_eq!(measured.requests.control(), 0);
        let before = fixture.requests().requests();
        let missing = crate::MachineId::from_digest([0x55; 32]);
        assert!(matches!(
            fixture.provider().inspect_machine(missing).await,
            Err(ProviderError::NotFound(_))
        ));
        let failed = fixture.requests().requests().since(before);
        assert_eq!(failed.class(RequestClass::Metadata), 1);
        assert_eq!(failed.data(), 0);
        Ok(())
    }
}
