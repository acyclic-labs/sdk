//! Actual process/file primitives composed with the production Streams owner.
use super::*;
use acyclic_stream::{MemoryStream, StreamPath};
use std::process::Command;

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
            platform: Platform::host(DeviceKind::Desktop).unwrap(),
            lifecycle: LifecycleClass::Provisioned,
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
        process_custody: None,
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
async fn admitted_guest_environment_excludes_ambient_host_values()
-> Result<(), Box<dyn std::error::Error>> {
    const HOST: &str = "ACYCLIC_TEST_MACHINE_ENV_HOST";
    let executable = std::env::current_exe()?;
    if std::env::var_os(HOST).is_none() {
        // Seed a separate host before its runtime starts; never mutate this
        // test process's environment while other tests or workers can run.
        let output = Command::new(&executable)
            .args([
                "--exact",
                "local::provider_tests::admitted_guest_environment_excludes_ambient_host_values",
                "--nocapture",
            ])
            .env(HOST, "1")
            .env("OPENAI_API_KEY", "harmless-machine-sentinel")
            .env("BASH_ENV", "harmless-machine-startup-sentinel")
            .env(
                "ACYCLIC_MACHINE_PRIVATE_VALUE",
                "harmless-ordinary-sentinel",
            )
            .env("ACYCLIC_MACHINE_ROUTE", "ambient")
            .output()?;
        assert!(output.status.success(), "isolated host failed: {output:?}");
        return Ok(());
    }

    let mut environment = BTreeMap::from([
        ("ACYCLIC_TEST_MACHINE_PROCESS".into(), "environment".into()),
        ("ACYCLIC_MACHINE_ROUTE".into(), "admitted".into()),
    ]);
    // Native executable discovery belongs to the admitted map, too.
    for name in ["PATH", "PATHEXT", "SystemRoot"] {
        if let Ok(value) = std::env::var(name) {
            environment.insert(name.into(), value);
        }
    }
    let argv = ["--exact", "local::tests::child", "--nocapture"];
    // The former inherited launch reaches the SAME real-child oracle with
    // the same explicit values. Only the ambient host projection differs.
    let inherited = Command::new(&executable)
        .args(argv)
        .envs(&environment)
        .output()?;
    assert_eq!(inherited.status.code(), Some(91));

    let machine = MachineId::new();
    let local = provider(
        enrollment(machine),
        LocalOptions {
            cwd: std::fs::canonicalize(std::env::temp_dir())?,
            environment,
            process_custody: None,
        },
        Arc::new(MemoryStream::default()),
        StreamPath::new(format!("machine-tests/{machine}"))?,
    )
    .await?;
    let command = ExecCommand::new(
        std::iter::once(executable.to_str().unwrap().to_owned())
            .chain(argv.into_iter().map(str::to_owned))
            .collect(),
        5_000,
    )?;
    let mut output = local.exec(machine, command).await?;
    let mut exited = false;
    while let Some(frame) = output.next().await {
        match frame? {
            ExecOutput::ExitCode(code) => {
                assert_eq!(code, 0, "admitted child inherited ambient host values");
                assert!(!exited);
                exited = true;
            }
            ExecOutput::Stopped(stop) => panic!("original child did not settle: {stop:?}"),
            ExecOutput::Data(_) | ExecOutput::Stderr(_) => assert!(!exited),
        }
    }
    assert!(exited, "original child must publish its terminal result");
    assert_eq!(
        local
            .destroy_machine(machine, IdempotencyKey::new())
            .await?,
        MutationOutcome::MachineDestroyed(machine)
    );
    Ok(())
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

async fn enrolled(
    observation: MachineObservation,
) -> Result<Arc<dyn MachinesProvider>, ProviderError> {
    let root = StreamPath::new(format!("machine-tests/{}", observation.id))
        .map_err(|error| ProviderError::Invalid(error.to_string()))?;
    provider(
        observation,
        options(),
        Arc::new(MemoryStream::default()),
        root,
    )
    .await
}

fn last_state(page: &EventPage) -> Option<MachineState> {
    page.events.iter().rev().find_map(|event| match event.fact {
        EventFact::State(state) => Some(state),
        _ => None,
    })
}

#[tokio::test]
async fn idle_suspended_machine_wakes_for_exec_and_attached_devices_only_detach()
-> Result<(), Box<dyn std::error::Error>> {
    let machine = MachineId::new();
    let mut observation = enrollment(machine);
    observation.contract.suspension = SuspensionPolicy::AfterIdle(Duration::from_secs(3_600));
    let local = enrolled(observation).await?;
    local.suspend(machine, IdempotencyKey::new()).await?;
    // The manual-policy refusal is the negative control in the public factory
    // test above: the same exec on a Manual Machine returns Conflict.
    let mut output = local.exec(machine, child().with_stdin(vec![0])).await?;
    let mut exit = None;
    while let Some(frame) = output.next().await {
        if let ExecOutput::ExitCode(code) = frame? {
            exit = Some(code);
        }
    }
    assert_eq!(exit, Some(9));
    assert_eq!(
        local.inspect_machine(machine).await?.state,
        MachineState::Running
    );
    // The implicit wake is retained like an explicit one.
    let page = local.events(machine, None, 100).await?;
    assert_eq!(last_state(&page), Some(MachineState::Running));
    assert!(
        page.events
            .iter()
            .any(|event| event.fact == EventFact::State(MachineState::Suspended))
    );

    let device = MachineId::new();
    let mut attached = enrollment(device);
    attached.contract.lifecycle = LifecycleClass::Attached;
    let mut idle = attached.clone();
    idle.contract.suspension = SuspensionPolicy::AfterIdle(Duration::from_secs(1));
    assert!(matches!(
        enrolled(idle).await,
        Err(ProviderError::Unsupported(_))
    ));
    let mut foreign = attached.clone();
    foreign.contract.platform.path_style = match foreign.contract.platform.path_style {
        PathStyle::Posix => PathStyle::Windows,
        PathStyle::Windows => PathStyle::Posix,
    };
    assert!(matches!(
        enrolled(foreign).await,
        Err(ProviderError::Invalid(_))
    ));
    let mut matching = CreateMachine::new(
        IdempotencyKey::new(),
        None,
        attached.contract.network_policy_digest,
    );
    matching.suspension = SuspensionPolicy::Manual;
    let local = enrolled(attached).await?;
    // Every lifecycle intent passes the one admission, so even requests that
    // match the enrolled contract exactly are refused on an attached device.
    for refused in [
        local.create(matching).await,
        local
            .set_suspension_policy(device, SuspensionPolicy::Manual, IdempotencyKey::new())
            .await,
        local.suspend(device, IdempotencyKey::new()).await,
        local.wake(device, IdempotencyKey::new()).await,
    ] {
        assert!(matches!(refused, Err(ProviderError::Unsupported(_))));
    }

    // Detach stops exactly the commands this binding dispatched. A process the
    // device runs outside the binding keeps running and exits on its own.
    let mut bystander = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", "local::tests::child", "--nocapture"])
        .env("ACYCLIC_TEST_MACHINE_PROCESS", "input")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let mut output = local.exec(device, child()).await?;
    assert!(matches!(output.next().await, Some(Ok(ExecOutput::Data(_)))));
    assert_eq!(
        local.destroy_machine(device, IdempotencyKey::new()).await?,
        MutationOutcome::MachineDestroyed(device)
    );
    let mut stopped = false;
    while let Some(frame) = output.next().await {
        if let ExecOutput::Stopped(stop) = frame? {
            stopped = stop.cleanup_completed;
        }
    }
    assert!(stopped);
    assert!(bystander.try_wait()?.is_none());
    std::io::Write::write_all(
        bystander.stdin.as_mut().ok_or("bystander stdin")?,
        &[0, 255, 7, 0],
    )?;
    assert_eq!(bystander.wait()?.code(), Some(9));
    Ok(())
}

/// The implicit wake is the provider's transition, not its caller's: an exec
/// whose deadline ends mid-wake leaves the wake to complete, and the next exec
/// runs instead of finding the Machine stranded in Waking.
#[tokio::test(start_paused = true)]
async fn deadline_during_implicit_wake_never_strands_the_machine()
-> Result<(), Box<dyn std::error::Error>> {
    struct GatedWake {
        gate: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl crate::provider::admitted::NativeMachine for GatedWake {
        async fn exec(
            &self,
            _: ExecCommand,
            _: std::time::Instant,
        ) -> Result<ExecStream, ProviderError> {
            Ok(Box::pin(futures::stream::iter([Ok(ExecOutput::ExitCode(
                0,
            ))])))
        }
        async fn read(
            &self,
            _: FileAccess,
            _: u32,
            _: &crate::provider::deadline::GuestDeadline,
        ) -> Result<FileRead, ProviderError> {
            Err(ProviderError::Unsupported("no files".into()))
        }
        async fn write(
            &self,
            _: FileAccess,
            _: Vec<u8>,
            _: bool,
            _: &crate::provider::deadline::GuestDeadline,
        ) -> Result<FileWrite, ProviderError> {
            Err(ProviderError::Unsupported("no files".into()))
        }
        async fn stop(&self) -> Result<(), ProviderError> {
            Ok(())
        }
        async fn wake(&self) -> Result<(), ProviderError> {
            self.gate.notified().await;
            Ok(())
        }
    }
    let machine = MachineId::new();
    let mut observation = enrollment(machine);
    observation.contract.suspension = SuspensionPolicy::AfterIdle(Duration::from_secs(3_600));
    let gate = Arc::new(tokio::sync::Notify::new());
    let local = crate::provider::admitted::AdmittedMachine::open(
        GatedWake { gate: gate.clone() },
        observation,
        Arc::new(MemoryStream::default()),
        StreamPath::new(format!("machine-tests/{machine}"))?,
    )
    .await?;
    local.suspend(machine, IdempotencyKey::new()).await?;
    let command = |timeout| ExecCommand::new(vec!["noop".into()], timeout);
    // The physical wake is held, so this caller's deadline passes mid-wake.
    assert!(matches!(
        local.exec(machine, command(50)?).await,
        Err(ProviderError::CommandIndeterminate(_))
    ));
    gate.notify_one();
    let mut output = local.exec(machine, command(60_000)?).await?;
    assert!(matches!(
        output.next().await,
        Some(Ok(ExecOutput::ExitCode(0)))
    ));
    assert_eq!(
        local.inspect_machine(machine).await?.state,
        MachineState::Running
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn idle_producer_suspends_after_quiet_and_an_explicit_wake_rearms_it()
-> Result<(), Box<dyn std::error::Error>> {
    let idle = Duration::from_secs(60);
    let machine = MachineId::new();
    let mut observation = enrollment(machine);
    observation.contract.suspension = SuspensionPolicy::AfterIdle(idle);
    let local = enrolled(observation).await?;
    let state = || async {
        local
            .inspect_machine(machine)
            .await
            .map(|value| value.state)
    };
    tokio::time::sleep(idle / 2).await;
    assert_eq!(state().await?, MachineState::Running);
    tokio::time::sleep(idle).await;
    assert_eq!(state().await?, MachineState::Suspended);
    local.wake(machine, IdempotencyKey::new()).await?;
    tokio::time::sleep(idle / 2).await;
    assert_eq!(state().await?, MachineState::Running);
    tokio::time::sleep(idle).await;
    assert_eq!(state().await?, MachineState::Suspended);
    Ok(())
}
