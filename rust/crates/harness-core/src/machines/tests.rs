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
        (
            ProviderError::FileIndeterminate(machine),
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
            Some(&artifact),
            None,
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
    let command = acyclic_machines::ExecCommand::new(vec!["true".into()], 300_000)
        .map_err(|error| map_error(error, None))?;
    assert!(matches!(
        host.exec(&foreign, command).await,
        Err(Error::Invalid(_))
    ));
    let access = acyclic_machines::FileAccess::new("/file".into(), 0, 4_102_444_800_000)
        .map_err(|error| map_error(error, None))?;
    assert!(matches!(
        host.read_file(&foreign, access.clone(), 1).await,
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        host.write_file(&foreign, access, vec![0, 255], true).await,
        Err(Error::Invalid(_))
    ));
    let stop_key = IdempotencyKey::new("stop-sandbox")?;
    assert!(matches!(
        host.suspend(OperationId::from_bytes([2; 16]), &stop_key, &foreign)
            .await,
        Err(Error::Invalid(_))
    ));
    host.suspend(OperationId::from_bytes([2; 16]), &stop_key, &sandbox)
        .await?;
    assert_eq!(
        host.inspect(&sandbox).await?.state,
        acyclic_machines::MachineState::Suspended
    );
    let delete_key = IdempotencyKey::new("delete-sandbox")?;
    assert!(matches!(
        host.destroy(OperationId::from_bytes([3; 16]), &delete_key, &foreign)
            .await,
        Err(Error::Invalid(_))
    ));
    host.destroy(OperationId::from_bytes([3; 16]), &delete_key, &sandbox)
        .await?;
    assert_eq!(
        host.inspect(&sandbox).await?.state,
        acyclic_machines::MachineState::Destroyed
    );
    Ok(())
}

#[tokio::test]
async fn machine_shell_admission_requires_parent_grants_and_rechecks_revocation() -> Result<()> {
    use crate::{
        conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
        core::{AggregateKind, Authority, AuthorityRoot},
    };
    let issuer = AuthorityRoot::new(
        "machine-shell",
        [106; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "machine-shell".into(),
        },
    );
    let volume = VolumeRef::new(
        ProviderRef::new("example".into(), "filesystem".into(), "1".into(), &mut None)?,
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    let provider_ref =
        ProviderRef::new("example".into(), "machines".into(), "1".into(), &mut None)?;
    let host = MachinesHost::new(Arc::new(SimulatedMachines::default()), provider_ref.clone())?;
    let sandbox = SandboxRef::new(provider_ref, vec![1; 16], None)?;
    let read = volume.capability(VolumeOperation::Read, &mut None)?;
    let write = volume.capability(VolumeOperation::Write, &mut None)?;
    let shell = crate::contract::capability::tool_call("shell");
    for missing in 0..3 {
        let grants = crate::Capabilities::new(
            [read.clone(), write.clone(), shell.clone()]
                .into_iter()
                .enumerate()
                .filter_map(|(index, grant)| (index != missing).then_some(grant)),
        );
        let scope = issuer.bootstrap_scope("missing", &grants, None, &mut None)?;
        assert!(matches!(
            MachinesWorkspace::new(
                host.clone(),
                sandbox.clone(),
                &volume,
                scope,
                issuer.verifier(&mut None)?
            ),
            Err(Error::Unauthorized(_))
        ));
    }
    let grants = crate::Capabilities::new([read, write, shell]);
    let scope = issuer.bootstrap_scope("full", &grants, None, &mut None)?;
    let workspace = MachinesWorkspace::new(
        host,
        sandbox,
        &volume,
        scope.clone(),
        issuer.verifier(&mut None)?,
    )?;
    issuer.revoke_workload(&scope)?;
    assert!(matches!(
        workspace
            .shell(
                crate::tools::ShellInput {
                    command: "true".into(),
                    cwd: None,
                    timeout: None
                },
                1024
            )
            .await,
        Err(Error::Unauthorized(_))
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
    assert!(matches!(
        host.create(
            OperationId::from_bytes([3; 16]),
            &IdempotencyKey::new("provider-default")?,
            None,
            None,
            [2; 32],
            |_| {},
        )
        .await,
        Err(Error::Machines(
            acyclic_machines::ProviderError::Unsupported(_)
        ))
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
            Some(&artifact),
            None,
            [0; 32],
            |_| {},
        )
        .await,
        Err(Error::Invalid(_))
    ));
    Ok(())
}
