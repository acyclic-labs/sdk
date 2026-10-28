use super::*;
use acyclic_machines::SimulatedMachines;

#[test]
fn operation_indeterminate_preserves_the_exact_provider_operation() -> Result<()> {
    let operation = MachinesOperationId::parse("00000000-0000-0000-0000-000000000001")
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(matches!(
        map_error(ProviderError::OperationIndeterminate(operation), None),
        Error::Indeterminate(actual) if actual == OperationId::from_bytes(operation.as_bytes())
    ));
    Ok(())
}

#[test]
fn provider_causes_survive_and_only_receipted_uncertainty_becomes_indeterminate() -> Result<()> {
    use crate::wire::ErrorCode;
    let receipt = OperationId::from_bytes([7; 16]);
    let machine = MachineId::parse("00000000-0000-0000-0000-000000000002")
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let causes = [
        (
            ProviderError::NotFound("m".into()),
            ErrorCode::NotFound,
            false,
        ),
        (
            ProviderError::Conflict("m".into()),
            ErrorCode::Conflict,
            false,
        ),
        (
            ProviderError::Unsupported("m".into()),
            ErrorCode::Unsupported,
            false,
        ),
        (
            ProviderError::Invalid("m".into()),
            ErrorCode::Invalid,
            false,
        ),
        (
            ProviderError::Rejected("m".into()),
            ErrorCode::Invalid,
            false,
        ),
        (ProviderError::Failed, ErrorCode::Storage, false),
        (ProviderError::Cancelled, ErrorCode::Cancelled, false),
        (ProviderError::Unavailable, ErrorCode::Storage, true),
        (
            ProviderError::Indeterminate(MachinesKey::new()),
            ErrorCode::Indeterminate,
            true,
        ),
        (
            ProviderError::CommandIndeterminate(machine),
            ErrorCode::Indeterminate,
            true,
        ),
    ];
    for (cause, code, uncertain) in causes {
        let retained = map_error(cause.clone(), None);
        assert!(matches!(&retained, Error::Machines(actual) if *actual == cause));
        assert_eq!(retained.code(), code);
        match map_error(cause.clone(), Some(receipt)) {
            Error::Indeterminate(actual) => assert!(uncertain && actual == receipt),
            Error::Machines(actual) => assert!(!uncertain && actual == cause),
            other => return Err(other),
        }
    }
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
