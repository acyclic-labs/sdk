//! Actual process/file primitives composed with the production Streams owner.
use super::*;
use acyclic_stream::{MemoryStream, StreamPath};

pub(super) fn enrollment(machine: MachineId) -> MachineObservation {
    MachineObservation {
        id: machine,
        state: MachineState::Running,
        contract: MachineContract {
            image: Image::Custom(
                ImageDigest::new(Sha256::digest(include_bytes!("provider_tests.rs")).into())
                    .unwrap(),
            ),
            selected_volume_ref: None,
            capabilities: BTreeSet::new(),
            compatibility: CompatibilityPolicy::BestEffort,
            compatibility_revision: [1; 32],
            suspension: SuspensionPolicy::Manual,
            expiration: ExpirationPolicy::Never,
            network_policy_digest: [2; 32],
            budgets: Budgets::default(),
        },
        endpoints: Vec::new(),
        last_checkpoint: None,
        created_at_unix_ms: 1,
        changed_at_unix_ms: 1,
    }
}

pub(super) fn options() -> LocalOptions {
    LocalOptions {
        cwd: std::fs::canonicalize(std::env::temp_dir()).unwrap(),
        environment: BTreeMap::from([("ACYCLIC_TEST_MACHINE_PROCESS".into(), "credit".into())]),
        inherit_environment: true,
        custody: None,
    }
}

fn child() -> ExecCommand {
    ExecCommand::new(
        vec![
            std::env::current_exe().unwrap().to_str().unwrap().into(),
            "--exact".into(),
            "local::tests::child".into(),
            "--nocapture".into(),
        ],
        5_000,
    )
    .unwrap()
}

#[tokio::test]
async fn public_local_factory_joins_native_retirement_and_retains_original_results()
-> Result<(), Box<dyn std::error::Error>> {
    let machine = MachineId::new();
    let observation = enrollment(machine);
    let streams = Arc::new(MemoryStream::default());
    let root = StreamPath::new(format!("machine-tests/{machine}"))?;
    let factory = || {
        provider(
            observation.clone(),
            options(),
            streams.clone(),
            root.clone(),
        )
    };
    let (first, second) = futures::join!(factory(), factory());
    let local = match (first, second) {
        (Ok(owner), Err(ProviderError::Conflict(_)))
        | (Err(ProviderError::Conflict(_)), Ok(owner)) => owner,
        _ => panic!("conditional enrollment must admit exactly one physical owner"),
    };
    assert!(matches!(factory().await, Err(ProviderError::Conflict(_))));
    let create_key = IdempotencyKey::new();
    let mut request =
        CreateMachine::new(create_key, None, observation.contract.network_policy_digest);
    request.suspension = SuspensionPolicy::Manual;
    let created = local.create(request.clone()).await?;
    assert_eq!(created, MutationOutcome::Created(observation.clone()));
    let original_operation = local.recover_operation(create_key).await?;
    assert_eq!(
        local
            .watch_operation(original_operation)
            .await?
            .next()
            .await
            .unwrap()?
            .phase,
        OperationPhase::Succeeded
    );
    let mut conflicting = request.clone();
    conflicting.network_policy_digest = [3; 32];
    assert!(local.create(conflicting).await.is_err());
    assert_eq!(local.recover(create_key).await?, created);
    assert!(matches!(
        local
            .attach_volume(
                machine,
                wire::selection::VolumeRef::default(),
                IdempotencyKey::new()
            )
            .await,
        Err(ProviderError::Unsupported(_))
    ));
    assert!(matches!(
        local.checkpoint(machine, IdempotencyKey::new()).await,
        Err(ProviderError::Unsupported(_))
    ));

    // Keep the output receiver alive and withhold credit after real bytes.
    // Waiting on handler drop instead of physical completion hangs this path.
    let mut output = local.exec(machine, child()).await?;
    assert!(matches!(output.next().await, Some(Ok(ExecOutput::Data(_)))));
    assert!(matches!(factory().await, Err(ProviderError::Conflict(_))));
    let stop = IdempotencyKey::new();
    assert_eq!(
        local.suspend(machine, stop).await?,
        MutationOutcome::Suspended(machine)
    );
    assert_eq!(
        local.recover(stop).await?,
        MutationOutcome::Suspended(machine)
    );
    assert!(matches!(
        local.exec(machine, child()).await,
        Err(ProviderError::Conflict(_))
    ));
    let mut cleaned = false;
    while let Some(frame) = output.next().await {
        if let ExecOutput::Stopped(stop) = frame? {
            assert!(stop.cleanup_completed);
            cleaned = true;
        }
    }
    assert!(cleaned);
    // The completed output object remains alive across wake and delete.
    local.wake(machine, IdempotencyKey::new()).await?;
    let path = std::env::temp_dir().join(format!("acyclic-machine-file-{machine}"));
    let access = |offset, deadline| {
        FileAccess::new(path.to_str().unwrap().into(), offset, deadline).unwrap()
    };
    let deadline = 4_102_444_800_000;
    assert_eq!(
        local
            .write_file(machine, access(0, deadline), vec![0, 255, 7, 0], true)
            .await?,
        FileWrite::Written(4)
    );
    assert_eq!(
        local.read_file(machine, access(1, deadline), 3).await?,
        FileRead::Data(vec![255, 7, 0])
    );
    assert!(matches!(
        local.write_file(machine, access(0, 1), vec![1], true).await,
        Err(ProviderError::Rejected(_))
    ));
    assert_eq!(
        local.read_file(machine, access(0, deadline), 4).await?,
        FileRead::Data(vec![0, 255, 7, 0])
    );
    std::fs::remove_file(path)?;
    #[cfg(unix)]
    {
        let fifo = std::env::temp_dir().join(format!("acyclic-machine-fifo-{machine}"));
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            &fifo,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )?;
        let access = FileAccess::new(fifo.to_str().unwrap().into(), 0, deadline)?;
        // A FIFO has no offset-addressed completion producer. Opening it must
        // not wait for a peer, and truncation must never touch a device.
        assert!(matches!(
            local.read_file(machine, access.clone(), 1).await,
            Err(ProviderError::Rejected(_))
        ));
        assert!(matches!(
            local.write_file(machine, access, vec![1], true).await,
            Ok(FileWrite::Errno(_))
        ));
        std::fs::remove_file(fifo)?;
    }
    let destroy = IdempotencyKey::new();
    assert_eq!(
        local.destroy_machine(machine, destroy).await?,
        MutationOutcome::MachineDestroyed(machine)
    );
    drop(local);
    let reopened = factory().await?;
    assert_eq!(
        reopened.inspect_machine(machine).await?.state,
        MachineState::Destroyed
    );
    assert_eq!(reopened.recover(create_key).await?, created);
    assert_eq!(reopened.create(request.clone()).await?, created);
    request.idempotency_key = IdempotencyKey::new();
    assert!(matches!(
        reopened.create(request).await,
        Err(ProviderError::Conflict(_))
    ));
    assert!(matches!(
        reopened
            .set_suspension_policy(machine, SuspensionPolicy::Manual, IdempotencyKey::new())
            .await,
        Err(ProviderError::Conflict(_))
    ));
    assert_eq!(
        reopened.recover(destroy).await?,
        MutationOutcome::MachineDestroyed(machine)
    );
    assert!(matches!(
        reopened.wake(machine, IdempotencyKey::new()).await,
        Err(ProviderError::Conflict(_))
    ));
    let page = reopened.events(machine, None, 2).await?;
    assert_eq!(page.events.len(), 2);
    let next = reopened.events(machine, page.next_sequence, 100).await?;
    assert!(
        next.events
            .iter()
            .any(|event| event.fact == EventFact::State(MachineState::Destroyed))
    );
    assert_eq!(next.next_sequence, None);
    Ok(())
}

#[tokio::test]
async fn failed_spawn_cannot_become_a_positive_destroy_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let machine = MachineId::new();
    let local = provider(
        enrollment(machine),
        options(),
        Arc::new(MemoryStream::default()),
        StreamPath::new(format!("machine-tests/{machine}"))?,
    )
    .await?;
    let missing = std::env::temp_dir().join(format!("absent-machine-command-{machine}"));
    let mut result = local
        .exec(
            machine,
            ExecCommand::new(vec![missing.to_str().unwrap().into()], 5_000)?,
        )
        .await?;
    assert!(
        matches!(result.next().await, Some(Err(ProviderError::CommandIndeterminate(id))) if id == machine)
    );
    let key = IdempotencyKey::new();
    assert!(
        matches!(local.destroy_machine(machine, key).await, Err(ProviderError::Indeterminate(original)) if original == key)
    );
    assert!(
        matches!(local.recover(key).await, Err(ProviderError::Indeterminate(original)) if original == key)
    );
    let operation = local.recover_operation(key).await?;
    assert!(
        matches!(local.watch_operation(operation).await, Err(ProviderError::Indeterminate(original)) if original == key)
    );
    assert!(matches!(
        local.exec(machine, child()).await,
        Err(ProviderError::Conflict(_))
    ));
    Ok(())
}
