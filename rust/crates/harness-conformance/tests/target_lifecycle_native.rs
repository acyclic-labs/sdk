//! Original actor control, durable publication and physical process retirement.
use super::*;
use acyclic_machines::{
    CreateMachine, DeviceKind, ExecOutput, Image, ImageDigest, LifecycleClass, MachineContract,
    MachineId, Platform,
};
use acyclic_native_runtime::{FileLock, FileLockMode};
use acyclic_stream::{LocalStream, StreamPath};
use futures::StreamExt as _;
use std::io::Write as _;

const GATE: &str = "pause-gate";
const LIFETIME: &str = "pause-child-lifetime";
const READY: &[u8] = b"ACYCLIC_PAUSE_CHILD_READY";

#[test]
fn child() -> Result {
    if std::env::var_os("ACYCLIC_PAUSE_CHILD").is_none() {
        return Ok(());
    }
    let open = |name| std::fs::File::options().read(true).write(true).open(name);
    let _lifetime = FileLock::acquire(open(LIFETIME)?, FileLockMode::Exclusive)?;
    {
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(READY)?;
        stdout.flush()?;
    }
    // The parent holds this gate throughout the pause. Only retirement can
    // release the child's lifetime lock while the gate is still held.
    let _gate = FileLock::acquire(open(GATE)?, FileLockMode::Exclusive)?;
    Err("the original child escaped the pause gate".into())
}

#[tokio::test]
async fn abandoned_pause_joins_original_native_retirement() -> Result {
    let directory = Arc::new(tempfile::tempdir()?);
    let streams = Arc::new(
        LocalStream::open_with_ownership_anchor(
            directory.path(),
            Default::default(),
            acyclic_native_runtime::OwnershipAnchor::new(directory.clone()),
        )
        .await?,
    );
    let machine = MachineId::new();
    let request = CreateMachine::new(acyclic_machines::IdempotencyKey::new(), None, [2; 32]);
    let enrolled = MachineObservation {
        id: machine,
        state: MachineState::Running,
        contract: MachineContract {
            image: Image::Custom(ImageDigest::new([1; 32])?),
            selected_volume_ref: request.selected_volume_ref,
            capabilities: Default::default(),
            compatibility: request.compatibility,
            compatibility_revision: [1; 32],
            suspension: request.suspension,
            expiration: request.expiration,
            network_policy_digest: request.network_policy_digest,
            budgets: request.budgets,
            platform: Platform::host(DeviceKind::Desktop).ok_or("no native host platform")?,
            lifecycle: LifecycleClass::Provisioned,
        },
        endpoints: Vec::new(),
        last_checkpoint: None,
        created_at_unix_ms: 1,
        changed_at_unix_ms: 1,
    };
    let new_file = |name| {
        std::fs::File::options()
            .read(true)
            .write(true)
            .create_new(true)
            .open(directory.path().join(name))
    };
    let _gate = FileLock::acquire(new_file(GATE)?, FileLockMode::Exclusive)?;
    let lifetime = FileLock::new(new_file(LIFETIME)?);
    let provider = acyclic_machines::local::provider(
        enrolled,
        acyclic_machines::local::LocalOptions {
            cwd: std::fs::canonicalize(directory.path())?,
            environment: [("ACYCLIC_PAUSE_CHILD".into(), "1".into())].into(),
            process_custody: None,
        },
        streams.clone(),
        StreamPath::new(format!("native-pause/{machine}"))?,
    )
    .await?;
    let machines = Arc::new(AgentMachines::new(
        streams.clone(),
        vec![ProvisionedTarget {
            name: CLOUD.into(),
            host: MachinesHost::new(provider, provider_ref()?)?,
            artifact: None,
            network_policy_digest: [2; 32],
        }],
    )?);
    let host = host(&streams, Some(machines.clone()))?;
    let agent = host
        .create(&intent("native-pause")?, adopted(id(7)))
        .await?;
    let other = host
        .create(&intent("native-bystander")?, adopted(id(8)))
        .await?;
    assert!(host.runtime().drain().await.is_empty());
    let sandbox = machines.machine(&agent, CLOUD).await?;
    let target = machines.targets().first().ok_or("no native target")?;
    let mut output = target
        .host
        .exec(
            &sandbox,
            ExecCommand::new(
                vec![
                    std::env::current_exe()?
                        .to_str()
                        .ok_or("non-UTF8 executable")?
                        .into(),
                    "--exact".into(),
                    acyclic_test_support::unit_test_name!(child),
                    "--nocapture".into(),
                ],
                None,
            )?,
        )
        .await?;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    loop {
        match output.next().await.transpose()? {
            Some(ExecOutput::Data(bytes)) => {
                stdout.extend(bytes);
                if stdout.windows(READY.len()).any(|bytes| bytes == READY) {
                    break;
                }
            }
            Some(ExecOutput::Stderr(bytes)) => stderr.extend(bytes),
            terminal => {
                return Err(format!(
                    "pause child settled before readiness: {terminal:?}\n{}\n{}",
                    String::from_utf8_lossy(&stdout),
                    String::from_utf8_lossy(&stderr),
                )
                .into());
            }
        }
    }
    assert!(
        matches!(lifetime.try_lock(FileLockMode::Exclusive), Err(error)
            if error.kind() == std::io::ErrorKind::WouldBlock),
        "ready child must own the lifetime lock"
    );
    // Retain the original output observer without granting any more credit.
    // Its lifetime must not own the child's physical retirement.
    let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::new();
    let mut pause = Box::pin(schedule.scope(host.pause(&agent, true)));
    let admission = loop {
        let boundary = tokio::select! {
            result = &mut pause => panic!("pause bypassed its original disk admission: {result:?}"),
            boundary = boundaries.recv() => boundary.ok_or("lost original native admission producer")?,
        };
        if boundary.name == "native:submitted" && boundary.detail.starts_with("commit ") {
            break boundary;
        }
        boundary.release();
    };
    drop(pause);
    admission.release();
    let drained = schedule.drained();
    tokio::pin!(drained);
    loop {
        tokio::select! {
            biased;
            () = &mut drained => break,
            boundary = boundaries.recv() => boundary.ok_or("lost original physical retirement producer")?.release(),
        }
    }
    assert_eq!(
        target.host.inspect(&sandbox).await?.state,
        MachineState::Suspended,
        "abandoned public pause lost its original native transition"
    );
    lifetime
        .try_lock(FileLockMode::Exclusive)
        .map_err(|error| format!("pause receipt preceded original child retirement: {error}"))?;
    let mut retired = false;
    while let Some(frame) = output.next().await {
        if let ExecOutput::Stopped(stop) = frame? {
            assert!(
                stop.cleanup_completed,
                "pause lost the original cleanup receipt"
            );
            retired = true;
        }
    }
    assert!(retired, "pause lost the original process terminal result");
    let store = host.runtime().store();
    assert!(
        store
            .load(&host.actor(&agent)?)
            .await?
            .state()
            .ok_or("missing paused actor")?
            .paused()
    );
    assert!(
        !store
            .load(&host.actor(&other)?)
            .await?
            .state()
            .ok_or("missing other actor")?
            .paused()
    );
    host.pause(&agent, false).await?;
    Ok(())
}
