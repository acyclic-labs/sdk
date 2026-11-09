//! Original actor control, durable publication and physical process retirement.
use super::*;
use acyclic_machines::{
    CreateMachine, DeviceKind, ExecOutput, Image, ImageDigest, LifecycleClass, MachineContract,
    MachineId, Platform,
};
use acyclic_stream::{LocalStream, StreamPath};
use futures::StreamExt as _;
use tokio::io::AsyncReadExt as _;

#[test]
fn child() -> Result {
    let Some(address) = std::env::var_os("ACYCLIC_PAUSE_CHILD_SOCKET") else {
        return Ok(());
    };
    let mut connection = std::net::TcpStream::connect(address.to_str().ok_or("non-UTF8 address")?)?;
    // The parent's accepted pause must stop this original process. No timer,
    // replacement child, or synthetic physical-completion receipt participates.
    let mut byte = [0];
    std::io::Read::read_exact(&mut connection, &mut byte)?;
    Err("the original child received unexpected input".into())
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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let provider = acyclic_machines::local::provider(
        enrolled,
        acyclic_machines::local::LocalOptions {
            cwd: std::fs::canonicalize(directory.path())?,
            environment: [(
                "ACYCLIC_PAUSE_CHILD_SOCKET".into(),
                listener.local_addr()?.to_string(),
            )]
            .into(),
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
    let agent = host.create(&intent("native-pause"), adopted(id(7))).await?;
    let other = host
        .create(&intent("native-bystander"), adopted(id(8)))
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
                    "target_lifecycle::native::child".into(),
                    "--nocapture".into(),
                ],
                None,
            )?,
        )
        .await?;
    let (mut connection, _) = listener.accept().await?;
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
    assert_eq!(
        connection.read_u8().await.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof,
        "pause receipt preceded original child retirement"
    );
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
