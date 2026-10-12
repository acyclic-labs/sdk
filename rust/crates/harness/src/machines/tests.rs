use super::*;
use acyclic_machines::SimulatedMachines;

#[test]
fn operation_indeterminate_preserves_the_exact_provider_operation() -> Result<()> {
    let operation = MachinesOperationId::parse("00000000-0000-0000-0000-000000000001")
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        map_error(ProviderError::OperationIndeterminate(operation), None),
        Error::Indeterminate(OperationId::from_bytes(operation.as_bytes()))
    );
    Ok(())
}

#[tokio::test]
async fn opaque_refs_round_trip_through_a_replaceable_provider() -> Result<()> {
    let provider_ref =
        ProviderRef::new("example".into(), "machines".into(), "1".into(), &mut None)?;
    let host = MachinesHost::new(Arc::new(SimulatedMachines::default()), provider_ref.clone())?;
    let artifact = ArtifactRef::new(provider_ref, [1; 32].into(), None)?;
    let sandbox = host
        .create(
            OperationId::from_bytes([1; 16]),
            &IdempotencyKey::new("create-sandbox")?,
            &artifact,
            [2; 32],
            |_| {},
        )
        .await?;
    let observed = host.inspect(&sandbox).await?;
    assert_eq!(sandbox.as_resource().key(), observed.id.as_bytes());

    let foreign = SandboxRef::new(
        ProviderRef::new("other".into(), "machines".into(), "1".into(), &mut None)?,
        observed.id.as_bytes().into(),
        None,
    )?;
    assert!(matches!(
        host.inspect(&foreign).await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[tokio::test]
async fn create_requires_explicit_network_policy_and_provider_ownership() -> Result<()> {
    let provider_ref =
        ProviderRef::new("example".into(), "machines".into(), "1".into(), &mut None)?;
    let host = MachinesHost::new(Arc::new(SimulatedMachines::default()), provider_ref)?;
    let foreign = ArtifactRef::new(
        ProviderRef::new("other".into(), "machines".into(), "1".into(), &mut None)?,
        [1; 32].into(),
        None,
    )?;
    assert!(matches!(
        host.qualify(&foreign).await,
        Err(Error::Invalid(_))
    ));

    let artifact = ArtifactRef::new(
        ProviderRef::new("example".into(), "machines".into(), "1".into(), &mut None)?,
        [1; 32].into(),
        None,
    )?;
    assert!(matches!(
        host.create(
            OperationId::from_bytes([2; 16]),
            &IdempotencyKey::new("zero-policy")?,
            &artifact,
            [0; 32],
            |_| {},
        )
        .await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}

#[tokio::test]
async fn fork_admission_proves_the_selected_checkpoint_exists() -> Result<()> {
    let machines = ProviderRef::new("example".into(), "machines".into(), "1".into(), &mut None)?;
    let simulator = Arc::new(SimulatedMachines::default());
    let host = MachinesHost::new(simulator.clone(), machines.clone())?;
    let artifact = ArtifactRef::new(machines.clone(), [1; 32].into(), None)?;
    let sandbox = host
        .create(
            OperationId::from_bytes([1; 16]),
            &IdempotencyKey::new("fork-verifier-machine")?,
            &artifact,
            [2; 32],
            |_| {},
        )
        .await?;
    let checkpoint = host
        .checkpoint(
            OperationId::from_bytes([2; 16]),
            &IdempotencyKey::new("fork-verifier-checkpoint")?,
            &sandbox,
        )
        .await?;
    let mut seed: ForkSeed = serde_json::from_str(include_str!("../../fixtures/v2/fork-seed.json"))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let process = ResourceRevision::Process(checkpoint);
    seed.resources.insert(
        2,
        CapturedResource {
            source: process.clone(),
            revision: process.clone(),
        },
    );
    host.verify(&seed, &mut None)?.await?;
    let mut request: ForkRequest =
        serde_json::from_str(include_str!("../../fixtures/v2/fork-request.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    request.preparation.maximum_inherited_messages = 1;
    request.preparation.maximum_inherited_bytes = 1_024;
    request.preparation.maximum_inherited_references = 1;
    request.selections.insert(
        2,
        ForkSelection {
            required: true,
            revision: process,
        },
    );
    request.validate(&mut None)?;
    assert_eq!(
        host.capture(&request, &request.selections[2]).await?,
        Capture::Captured(seed.resources[2].clone()),
    );
    simulator
        .destroy_checkpoint(
            host.checkpoint_id(match &seed.resources[2].revision {
                ResourceRevision::Process(reference) => reference,
                _ => return Err(Error::Invalid("test checkpoint is missing".into())),
            })?,
            MachinesKey::new(),
        )
        .await
        .map_err(|error| map_error(error, None))?;
    assert!(matches!(
        host.verify(&seed, &mut None)?.await,
        Err(Error::Conflict(_))
    ));
    let mut missing = seed;
    let unavailable =
        ResourceRevision::Process(CheckpointRef::new(machines.clone(), [9; 16].into(), None)?);
    missing.resources[2] = CapturedResource {
        source: unavailable.clone(),
        revision: unavailable,
    };
    assert!(matches!(
        host.verify(&missing, &mut None)?.await,
        Err(Error::NotFound(_))
    ));
    let forged = ResourceRevision::Process(CheckpointRef::new(
        machines,
        [9; 16].into(),
        Some("forged-generation".into()),
    )?);
    missing.resources[2] = CapturedResource {
        source: forged.clone(),
        revision: forged,
    };
    assert!(matches!(
        host.verify(&missing, &mut None)?.await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}
