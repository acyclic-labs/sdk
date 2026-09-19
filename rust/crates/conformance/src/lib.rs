#![doc = include_str!("../README.md")]

/// Complete filesystem workload taxonomy, selectors, and portable vectors.
pub mod filesystem;
/// Machine-readable cross-language conformance reports and qualification receipts.
pub mod runner;

use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Fs};
use acyclic_machines::{
    Capability, CompatibilityPolicy, CreateMachine, IdempotencyKey, Image, MachineState,
    MachinesProvider, MutationOutcome, OperationPhase, Performance, ProviderError,
};
use acyclic_objects::ObjectsProvider;
use acyclic_stream::StreamProvider;
use futures::StreamExt;
use std::num::NonZeroU32;

/// Canonical language-neutral Objects conformance inventory.
pub const OBJECTS_SUITE: &[u8] = include_bytes!("../vectors/objects.json");

/// Canonical language-neutral harness conformance inventory.
pub const HARNESS_SUITE: &[u8] = include_bytes!("../vectors/harness.json");

/// Canonical language-neutral Machines conformance inventory.
pub const MACHINES_SUITE: &[u8] = include_bytes!("../vectors/machines.json");

/// Canonical language-neutral Stream conformance inventory.
pub const STREAM_SUITE: &[u8] = acyclic_stream::conformance::SUITE;

/// Exercises the minimum customer-level filesystem semantics.
pub async fn filesystem_smoke<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    provider: &Fs<A, O>,
) -> Result<(), String> {
    let workspace = provider
        .create_workspace("conformance")
        .await
        .map_err(|error| error.to_string())?;
    workspace
        .write("/answer", bytes::Bytes::from_static(b"42"))
        .await
        .map_err(|error| error.to_string())?;
    let observed = workspace
        .read("/answer", 2)
        .await
        .map_err(|error| error.to_string())?;
    (observed == bytes::Bytes::from_static(b"42"))
        .then_some(())
        .ok_or_else(|| "written value missing".into())
}

/// Runs the canonical Stream suite against a fresh, disposable provider.
pub async fn stream(provider: &dyn StreamProvider) -> Result<(), String> {
    acyclic_stream::conformance::verify(provider).await
}

/// Runs the canonical Objects suite against a fresh, disposable provider.
///
/// The suite retains state and idempotency keys under the `conformance` namespace.
pub async fn objects(provider: &dyn ObjectsProvider) -> Result<(), String> {
    acyclic_objects::conformance::verify(provider, "conformance")
        .await
        .map_err(|error| error.to_string())
}

/// Exercises the public shape-free lifecycle, replay, fork, and observation semantics.
#[allow(
    clippy::too_many_lines,
    reason = "linear conformance walkthrough; each step is a distinct sequential assertion \
              (qualification, capability negotiation, create/replay, operation lifecycle, \
              checkpoint, fork, suspend/wake, events, usage, teardown) threaded through shared \
              local state, and splitting it would only move the same checks behind indirection"
)]
pub async fn machines(provider: &dyn MachinesProvider) -> Result<(), String> {
    if MACHINES_SUITE.is_empty() {
        return Err("Machines conformance inventory is empty".into());
    }
    let key = |suffix: u8| {
        IdempotencyKey::parse(&format!("00000000-0000-0000-0000-0000000000{suffix:02x}"))
            .map_err(|error| error.to_string())
    };
    let assurance = provider.assurance();
    let image = Image::custom([7; 32]).map_err(|error| error.to_string())?;
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
    ];
    for (capability, create_suffix, destroy_suffix) in capability_cases {
        let required = std::collections::BTreeSet::from([capability]);
        let mut capability_request =
            CreateMachine::new(key(create_suffix)?, image.clone(), [8; 32]);
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
    let request = CreateMachine::new(create_key, image, [8; 32]);
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
            Performance::Elastic,
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
            acyclic_machines::EventFact::State(state) => Some(state),
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
        || (assurance == acyclic_machines::ProviderAssurance::ProcessLocalSimulation)
            != usage.receipt.is_empty()
    {
        return Err("simulation usage receipt is malformed".into());
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_memory::MemoryProfile;
    use acyclic_objects::ReadTarget;

    #[test]
    fn exported_stream_inventory_matches_the_executable_suite() {
        assert_eq!(STREAM_SUITE, include_bytes!("../vectors/stream.json"));
    }

    #[tokio::test]
    async fn memory_profile_conforms() -> Result<(), String> {
        let profile = MemoryProfile::new();
        filesystem_smoke(&profile.filesystem).await?;

        let stream_children = profile
            .stream
            .children(acyclic_stream::ChildrenRequest {
                parent: None,
                limit: 8,
            })
            .await
            .map_err(|error| error.to_string())?
            .collect::<Vec<_>>()
            .await;
        if !stream_children.iter().any(|child| {
            child
                .as_ref()
                .is_ok_and(|child| child.path.as_str() == "fs")
        }) {
            return Err("filesystem did not publish through the profile's public Stream".into());
        }
        let filesystem_objects = profile
            .objects
            .list(
                ReadTarget::Bucket(profile.filesystem_bucket.clone()),
                "fs/v1/".to_owned(),
                None,
                true,
                128,
                None,
            )
            .await
            .map_err(|error| error.to_string())?;
        if filesystem_objects.entries.is_empty() {
            return Err(
                "filesystem did not admit objects through the profile's public Objects".into(),
            );
        }

        stream(&profile.stream).await?;
        objects(&profile.objects).await?;
        machines(&profile.machines).await?;
        Ok(())
    }
}
