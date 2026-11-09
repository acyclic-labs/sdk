//! The public local provider and the same admitted workspace shell binding.
use acyclic_harness_core::{Error, Result, machines::MachinesHost, resources::ProviderRef};
use acyclic_machines::{Image, MachineId, MachineObservation};
use std::sync::Arc;
async fn workspace_fixture() -> Result<(
    acyclic_harness_core::core::AuthorityRoot,
    acyclic_harness_core::core::Scope,
    acyclic_harness_core::machines::MachinesWorkspace,
)> {
    use acyclic_harness_core::conversation::{VolumeClass, VolumeOwner, VolumeRef};
    let volume = VolumeRef::new(
        ProviderRef::new("example".into(), "filesystem".into(), "1".into(), &mut None)?,
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    workspace_fixture_for_volume(volume).await
}

async fn workspace_fixture_for_volume(
    volume: acyclic_harness_core::conversation::VolumeRef,
) -> Result<(
    acyclic_harness_core::core::AuthorityRoot,
    acyclic_harness_core::core::Scope,
    acyclic_harness_core::machines::MachinesWorkspace,
)> {
    workspace_fixture_for_mode(volume, false).await
}

async fn workspace_fixture_for_mode(
    volume: acyclic_harness_core::conversation::VolumeRef,
    copied: bool,
) -> Result<(
    acyclic_harness_core::core::AuthorityRoot,
    acyclic_harness_core::core::Scope,
    acyclic_harness_core::machines::MachinesWorkspace,
)> {
    use acyclic_harness_core::{
        conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
        core::{AggregateKind, Authority, AuthorityRoot},
    };
    use acyclic_machines::{
        Budgets, CompatibilityPolicy, DeviceKind, ExpirationPolicy, ImageDigest, LifecycleClass,
        MachineContract, MachineState, Platform, SuspensionPolicy,
    };
    let issuer = AuthorityRoot::new(
        "local-machine-tools",
        [107; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "local-machine-tools".into(),
        },
    );
    let machine = MachineId::new();
    let other_volume = VolumeRef::new(
        volume.provider().clone(),
        "other-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("other-project".into()),
        &mut None,
    )?;
    let observation = MachineObservation {
        id: machine,
        state: MachineState::Running,
        contract: MachineContract {
            image: Image::Custom(ImageDigest::new([1; 32]).map_err(Error::Machines)?),
            selected_volume_ref: if copied { None } else { Some((&volume).into()) },
            capabilities: Default::default(),
            compatibility: CompatibilityPolicy::BestEffort,
            compatibility_revision: [1; 32],
            suspension: SuspensionPolicy::Manual,
            expiration: ExpirationPolicy::Never,
            network_policy_digest: [2; 32],
            budgets: Budgets::default(),
            platform: Platform::host(DeviceKind::Desktop)
                .ok_or_else(|| Error::Storage("local shell test has no host platform".into()))?,
            lifecycle: LifecycleClass::Provisioned,
        },
        endpoints: Vec::new(),
        last_checkpoint: None,
        created_at_unix_ms: 1,
        changed_at_unix_ms: 1,
    };
    let provider = acyclic_machines::local::provider(
        observation.clone(),
        acyclic_machines::local::LocalOptions {
            cwd: std::env::current_dir()
                .map_err(|_| Error::Storage("local shell test has no current directory".into()))?,
            environment: ["PATH", "PATHEXT", "SystemRoot"]
                .into_iter()
                .filter_map(|name| std::env::var(name).ok().map(|value| (name.into(), value)))
                .collect(),
            process_custody: None,
        },
        Arc::new(acyclic_stream::MemoryStream::default()),
        acyclic_stream::StreamPath::new(format!("workspace-tests/{machine}"))
            .map_err(|error| Error::Storage(error.to_string()))?,
    )
    .await
    .map_err(Error::Machines)?;
    let host = MachinesHost::new(
        provider,
        ProviderRef::new("example".into(), "machines".into(), "1".into(), &mut None)?,
    )?;
    let shell_grant = acyclic_harness_core::tools::catalog()
        .find(|spec| spec.name == "shell")
        .ok_or_else(|| Error::Invalid("public tool catalog has no shell".into()))?
        .capability();
    let grants = acyclic_harness_core::Capabilities::new([
        volume.capability(VolumeOperation::Read, &mut None)?,
        volume.capability(VolumeOperation::Write, &mut None)?,
        other_volume.capability(VolumeOperation::Read, &mut None)?,
        other_volume.capability(VolumeOperation::Write, &mut None)?,
        shell_grant,
    ]);
    let scope = issuer.bootstrap_scope(
        "local-machine",
        &grants,
        Some(acyclic_harness_core::AgentId::from_bytes([41; 16])),
        &mut None,
    )?;
    // Both volumes are authorized; only the original physical selection binds.
    if !copied {
        assert!(matches!(
            host.workspace(
                observation.clone(),
                &other_volume,
                scope.clone(),
                issuer.verifier(&mut None)?
            ),
            Err(Error::Unauthorized(_))
        ));
        let mut absent = observation.clone();
        absent.contract.selected_volume_ref = None;
        assert!(matches!(
            host.workspace(absent, &volume, scope.clone(), issuer.verifier(&mut None)?),
            Err(Error::Unauthorized(_))
        ));
    }
    let workspace = host
        .workspace(
            observation,
            &volume,
            scope.clone(),
            issuer.verifier(&mut None)?,
        )?
        .with_shell(
            if cfg!(windows) {
                "powershell.exe"
            } else {
                "bash"
            }
            .into(),
            &acyclic_host_platform::shell::PLATFORM,
        )?;
    Ok((issuer, scope, workspace))
}

#[tokio::test]
async fn shared_workspace_runs_the_actual_local_machine_shell() -> Result<()> {
    let (issuer, scope, workspace) = workspace_fixture().await?;
    let command = if cfg!(windows) {
        "[Console]::Out.Write('out'); [Console]::Error.Write('err'); exit 7"
    } else {
        "printf out; printf err >&2; exit 7"
    };
    let output = acyclic_machines::capture_exec(
        workspace
            .shell(
                acyclic_harness_core::tools::ShellInput {
                    command: command.into(),
                    cwd: None,
                    timeout_ms: None,
                    target: None,
                },
                1024,
            )
            .await?,
        1024,
    )
    .await
    .map_err(Error::Machines)?;
    assert_eq!(output.stdout, b"out");
    assert_eq!(output.stderr, b"err");
    assert_eq!(output.exit_code, Some(7));
    issuer.revoke_workload(&scope)?;
    assert!(matches!(
        workspace
            .shell(
                acyclic_harness_core::tools::ShellInput {
                    command: command.into(),
                    cwd: None,
                    timeout_ms: None,
                    target: None
                },
                1024
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}

#[path = "machine_workspace_device.rs"]
mod device_runner;

#[tokio::test]
#[cfg(target_os = "linux")]
async fn copied_workspace_round_trips_binary_edits_deletions_and_original_turn()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use acyclic_fs::{CancellationToken, Fs, WorkBudget};
    use acyclic_harness_core::{
        conversation::{VolumeClass, VolumeOwner, VolumeRef},
        filesystem::FilesystemHost,
        tool::ToolExecutor,
    };
    let volume = VolumeRef::new(
        ProviderRef::new("example".into(), "filesystem".into(), "1".into(), &mut None)?,
        "copied-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
        &mut None,
    )?;
    let fs = Fs::memory();
    let host = FilesystemHost::new(fs.clone(), volume.provider(), None)?;
    host.create_volume(&volume).await?;
    let selected = fs
        .open_workspace(
            volume.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|e| e.error)?
        .value;
    selected
        .write("/binary", bytes::Bytes::from_static(&[0, 128, 255]))
        .await?;
    selected.write_text("/deleted", "remove me").await?;
    selected.write_text("/unchanged", "retain identity").await?;
    let original = selected
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|e| e.error)?
        .value;
    let unchanged = original
        .stat(
            "/unchanged",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|e| e.error)?
        .value;
    let (issuer, scope, workspace) = workspace_fixture_for_mode(volume.clone(), true).await?;
    let workspace = host.bind_machine_workspace(workspace).await?;
    let output = workspace.run_shell(acyclic_harness_core::OperationId::new(), acyclic_harness_core::tools::ShellInput { command: "test $(wc -c < binary) -eq 3 && printf '\\000\\377' > binary && rm deleted && mkdir nested && printf created > nested/new && ln -s binary alias".into(), cwd: None, timeout_ms: None, target: None }, 4096, None).await?;
    assert_eq!(output.value["exit_code"], 0);
    let turn = acyclic_harness_core::OperationId::new();
    workspace.settle_workspace(turn).await?;
    let generation = selected
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|e| e.error)?
        .value;
    assert_eq!(
        generation
            .read(
                "/binary",
                u64::MAX,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map_err(|e| e.error)?
            .value
            .as_ref(),
        &[0, 255]
    );
    assert_eq!(
        generation
            .read(
                "/nested/new",
                u64::MAX,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map_err(|e| e.error)?
            .value
            .as_ref(),
        b"created"
    );
    assert!(matches!(
        generation
            .stat("/deleted", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|e| e.error),
        Err(acyclic_fs::WorkspaceError::NotFound)
    ));
    workspace.settle_workspace(turn).await?;
    assert_eq!(
        selected
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|e| e.error)?
            .value
            .id(),
        generation.id()
    );
    assert_eq!(workspace.selected_volume(), &volume);
    assert_eq!(
        generation
            .stat(
                "/unchanged",
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await
            .map_err(|e| e.error)?
            .value
            .file_id,
        unchanged.file_id
    );
    assert_eq!(
        generation
            .read_symbolic_link("/alias", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|e| e.error)?
            .value
            .bytes
            .as_ref(),
        b"binary"
    );
    workspace
        .run_shell(
            acyclic_harness_core::OperationId::new(),
            acyclic_harness_core::tools::ShellInput {
                command: "printf revoked > binary".into(),
                cwd: None,
                timeout_ms: None,
                target: None,
            },
            4096,
            None,
        )
        .await?;
    issuer.revoke_workload(&scope)?;
    assert!(matches!(
        workspace
            .settle_workspace(acyclic_harness_core::OperationId::new())
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(
        selected
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|e| e.error)?
            .value
            .id(),
        generation.id()
    );
    Ok(())
}
