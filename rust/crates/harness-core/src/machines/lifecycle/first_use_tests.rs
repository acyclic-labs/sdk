use super::{AgentMachines, MachineExecutors, OnFirstUse, ProvisionedTarget, codec_fuzz};
use crate::{
    Error, Result,
    agent::AgentPath,
    machines::MachinesHost,
    resources::{ArtifactRef, ProviderRef, SandboxRef},
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_machines::{
    CheckpointId, CheckpointObservation, CreateMachine, EventPage, EventStream, ExecCommand,
    ExecStream, FileAccess, FileRead, FileWrite, IdempotencyKey as MachinesKey, Image,
    ImageQualification, MachineId, MachineObservation, MachineOutput, MachinePage,
    MachinesProvider, MutationOutcome, OperationId as MachinesOperationId, OperationObservation,
    OperationStream, ProviderAssurance, ProviderError, SimulatedMachines, SuspensionPolicy,
    UsageReceipt, wire,
};
use acyclic_stream::{
    BoxProviderFuture, MemoryStream, ReadRequest, StreamError, StreamProvider as _,
    StreamProviderExt as _,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::watch;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

// Capture the original MemoryStream envelope, then displace only its position.
// Opening metadata, payload, commit, source and all writes retain that provider.
struct DisplacedRead {
    inner: MemoryStream,
    position: AtomicUsize,
}

#[async_trait::async_trait]
impl acyclic_stream::StreamProvider for DisplacedRead {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        self.inner.context()
    }

    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, StreamError> {
        self.inner.heads(request).await
    }

    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> std::result::Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }

    fn write(
        &self,
        request: acyclic_stream::WriteRequest,
    ) -> BoxProviderFuture<'_, std::result::Result<acyclic_stream::WriteOutcome, StreamError>> {
        self.inner.write(request)
    }

    async fn read(
        &self,
        request: ReadRequest,
    ) -> std::result::Result<acyclic_stream::RecordStream, StreamError> {
        use futures::StreamExt as _;
        let displace = self.position.load(Ordering::Relaxed) as u64 == request.from;
        Ok(self.inner.read(request).await?.map_cursor(|cursor| {
            cursor
                .map(move |result| {
                    result.map(|mut record| {
                        if displace {
                            record.sequence += 1;
                        }
                        record
                    })
                })
                .boxed()
        }))
    }

    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> std::result::Result<acyclic_stream::FollowStream, StreamError> {
        self.inner.follow(request).await
    }

    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> std::result::Result<acyclic_stream::CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_stream::NativeStreamProvider for DisplacedRead {}

#[tokio::test]
async fn original_binding_positions_precede_allocation_and_recovery() -> TestResult {
    let stream = Arc::new(DisplacedRead {
        inner: MemoryStream::default(),
        position: AtomicUsize::new(usize::MAX),
    });
    let (_release, ready) = watch::channel(true);
    let provider = Arc::new(HeldInspection {
        inner: SimulatedMachines::default(),
        ready,
        entered: tokio::sync::Notify::new(),
        creates: AtomicUsize::new(0),
        inspections: AtomicUsize::new(0),
    });
    let identity = ProviderRef::new("example".into(), "machines".into(), "1".into(), &mut None)?;
    let machines = AgentMachines::new(
        stream.clone(),
        vec![ProvisionedTarget {
            name: "machine".into(),
            host: MachinesHost::new(provider.clone(), identity.clone())?,
            artifact: Some(ArtifactRef::new(identity, [1; 32].into(), None)?),
            network_policy_digest: [2; 32],
        }],
    )?;
    let agent = AgentPath::root(&crate::agent::AgentId::from_bytes([93; 16]));
    let target = machines.target("machine")?;
    // The production plan and original provider supply every fixture field.
    machines.plan(&agent, target).await?;
    let path = agent.machine("machine")?;
    assert_eq!(stream.inner.tail(path.clone()).await?, 1);
    stream.position.store(0, Ordering::Relaxed);
    assert!(
        matches!(
            machines.machine(&agent, "machine").await,
            Err(Error::Storage(_))
        ),
        "a displaced provisioning record reached allocation"
    );
    assert_eq!(provider.creates.load(Ordering::Relaxed), 0);
    assert_eq!(stream.inner.tail(path.clone()).await?, 1);

    // Restoring the original envelope completes that same retained plan.
    stream.position.store(usize::MAX, Ordering::Relaxed);
    let original = machines.machine(&agent, "machine").await?;
    assert_eq!(provider.creates.load(Ordering::Relaxed), 1);
    assert_eq!(stream.inner.tail(path.clone()).await?, 2);
    stream.position.store(1, Ordering::Relaxed);
    assert!(matches!(
        machines.machine(&agent, "machine").await,
        Err(Error::Storage(_))
    ));
    assert_eq!(provider.creates.load(Ordering::Relaxed), 1);
    assert_eq!(provider.inspections.load(Ordering::Relaxed), 0);
    assert_eq!(stream.inner.tail(path).await?, 2);
    stream.position.store(usize::MAX, Ordering::Relaxed);
    assert_eq!(machines.machine(&agent, "machine").await?, original);
    // Feed the permanent fuzz entrance the SAME original producer's values.
    let mut records = stream
        .inner
        .read(ReadRequest {
            path: agent.machine("machine")?,
            from: 0,
            limit: 2,
            at_least: Default::default(),
            key: None,
        })
        .await?;
    for sequence in 0..2 {
        let record = futures::TryStreamExt::try_next(&mut records)
            .await?
            .ok_or("original Machine plan or binding capture is missing")?;
        assert_eq!(record.sequence, sequence);
        codec_fuzz::check_record("machine", &record);
        codec_fuzz::check(&record.value);
        println!(
            "harness_codecs/machine-record-{}\tbase64:{}",
            record.sequence,
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                record.value.as_ref()
            )
        );
    }
    Ok(())
}

// Every operation reaches the real simulator. Only its original inspection
// response is held, so overlapping first uses are deterministic.
struct HeldInspection {
    inner: SimulatedMachines,
    ready: watch::Receiver<bool>,
    entered: tokio::sync::Notify,
    creates: AtomicUsize,
    inspections: AtomicUsize,
}

macro_rules! forward {
    ($($name:ident($($argument:ident: $kind:ty),*) -> $result:ty;)*) => {
        #[async_trait::async_trait]
        impl MachinesProvider for HeldInspection {
            fn assurance(&self) -> ProviderAssurance {
                self.inner.assurance()
            }

            async fn create(&self, request: CreateMachine) -> std::result::Result<MutationOutcome, ProviderError> {
                self.creates.fetch_add(1, Ordering::Relaxed);
                self.inner.create(request).await
            }

            async fn inspect_machine(&self, machine: MachineId) -> std::result::Result<MachineObservation, ProviderError> {
                self.inspections.fetch_add(1, Ordering::Relaxed);
                let observed = self.inner.inspect_machine(machine).await?;
                self.entered.notify_one();
                let mut ready = self.ready.clone();
                if !*ready.borrow_and_update() {
                    ready.changed().await.map_err(|_| ProviderError::Unavailable)?;
                }
                Ok(observed)
            }
        $(async fn $name(&self, $($argument: $kind),*) -> std::result::Result<$result, ProviderError> {
            self.inner.$name($($argument),*).await
        })*
        }
    };
}

forward! {
        read_file(machine: MachineId, access: FileAccess, limit: u32) -> FileRead;
        write_file(machine: MachineId, access: FileAccess, data: Vec<u8>, truncate: bool) -> FileWrite;
        exec(machine: MachineId, command: ExecCommand) -> ExecStream;
        read_output(machine: MachineId, offset: u64, limit: u32) -> MachineOutput;
        qualify_image(image: Image) -> ImageQualification;
        attach_volume(machine: MachineId, volume: wire::selection::VolumeRef, key: MachinesKey) -> MutationOutcome;
        list_machines(after: Option<MachineId>, limit: u32) -> MachinePage;
        checkpoint(machine: MachineId, key: MachinesKey) -> MutationOutcome;
        inspect_checkpoint(checkpoint: CheckpointId) -> CheckpointObservation;
        fork(checkpoint: CheckpointId, count: std::num::NonZeroU32, key: MachinesKey) -> MutationOutcome;
        fork_machine(machine: MachineId, count: std::num::NonZeroU32, key: MachinesKey) -> MutationOutcome;
        suspend(machine: MachineId, key: MachinesKey) -> MutationOutcome;
        wake(machine: MachineId, key: MachinesKey) -> MutationOutcome;
        set_suspension_policy(machine: MachineId, policy: SuspensionPolicy, key: MachinesKey) -> MutationOutcome;
        destroy_machine(machine: MachineId, key: MachinesKey) -> MutationOutcome;
        destroy_checkpoint(checkpoint: CheckpointId, key: MachinesKey) -> MutationOutcome;
        events(machine: MachineId, after_sequence: Option<u64>, limit: u32) -> EventPage;
        follow_events(machine: MachineId, after_sequence: Option<u64>) -> EventStream;
        usage(machine: MachineId, start_unix_ms: u64, end_unix_ms: u64) -> UsageReceipt;
        read_results(cursor: acyclic_machines::results::ResultCursor, credit: std::num::NonZeroU32) -> acyclic_machines::results::ResultPage;
        recover(key: MachinesKey) -> MutationOutcome;
        recover_operation(key: MachinesKey) -> MachinesOperationId;
        inspect_operation(operation: MachinesOperationId) -> OperationObservation;
        cancel(operation: MachinesOperationId) -> OperationObservation;
        watch_operation(operation: MachinesOperationId) -> OperationStream;
}

struct NeverRun;

impl ToolExecutor for NeverRun {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async { Err(Error::Unsupported("no command submitted".into())) })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<crate::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Err(Error::Unsupported("no command submitted".into())) })
    }
}

struct BoundExecutors {
    calls: AtomicUsize,
    executor: Arc<dyn ToolExecutor>,
}

impl MachineExecutors for BoundExecutors {
    fn bind(
        &self,
        _: &ProvisionedTarget,
        _: SandboxRef,
        _: MachineObservation,
    ) -> Result<Arc<dyn ToolExecutor>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::clone(&self.executor))
    }
}

#[tokio::test]
async fn first_use_joins_original_allocation_across_waiter_and_provider_loss() -> TestResult {
    for (target_count, callers) in [(1, 1), (1, 100), (1, 1_000), (100_000, 1)] {
        for drop_first in [false, true] {
            let (release, ready) = watch::channel(false);
            let provider = Arc::new(HeldInspection {
                inner: SimulatedMachines::default(),
                ready,
                entered: tokio::sync::Notify::new(),
                creates: AtomicUsize::new(0),
                inspections: AtomicUsize::new(0),
            });
            let provider_ref =
                ProviderRef::new("example".into(), "machines".into(), "1".into(), &mut None)?;
            let unrelated = Arc::new(SimulatedMachines::default());
            let unrelated_ref =
                ProviderRef::new("unrelated".into(), "machines".into(), "1".into(), &mut None)?;
            let unrelated_host = MachinesHost::new(unrelated.clone(), unrelated_ref)?;
            let mut targets = (1..target_count)
                .map(|position| ProvisionedTarget {
                    name: format!("unheld-{position}"),
                    host: unrelated_host.clone(),
                    artifact: None,
                    network_policy_digest: [3; 32],
                })
                .collect::<Vec<_>>();
            // The admitted target is last, so using the first attachment
            // would allocate from the unrelated provider instead.
            targets.push(ProvisionedTarget {
                name: "machine".into(),
                host: MachinesHost::new(provider.clone(), provider_ref.clone())?,
                artifact: Some(ArtifactRef::new(provider_ref, [1; 32].into(), None)?),
                network_policy_digest: [2; 32],
            });
            let machines = Arc::new(AgentMachines::new(
                Arc::new(MemoryStream::default()),
                targets,
            )?);
            let executors = Arc::new(BoundExecutors {
                calls: AtomicUsize::new(0),
                executor: Arc::new(NeverRun),
            });
            let target = OnFirstUse::new(
                &machines,
                AgentPath::root(&crate::agent::AgentId::from_bytes([1; 16])),
                "machine",
                executors.clone(),
            )?;
            let mut first = Box::pin(target.executor());
            tokio::select! {
                () = provider.entered.notified() => {},
                result = &mut first => panic!("first use ended before original inspection: {:?}", result.err()),
            }
            let mut waiting = (0..callers)
                .map(|_| Box::pin(target.executor()))
                .collect::<Vec<_>>();
            for waiter in &mut waiting {
                assert!(futures::poll!(waiter).is_pending());
            }
            assert_eq!(provider.creates.load(Ordering::Relaxed), 1);
            assert_eq!(
                provider.inspections.load(Ordering::Relaxed),
                1,
                "concurrent first use repeated the original inspection"
            );
            assert_eq!(executors.calls.load(Ordering::Relaxed), 0);
            assert!(unrelated.list_machines(None, 1).await?.machines.is_empty());
            if drop_first {
                drop(first);
                tokio::select! {
                    () = provider.entered.notified() => {},
                    result = &mut waiting[0] => panic!("reentry ended before original inspection: {:?}", result.err()),
                }
                assert_eq!(
                    provider.creates.load(Ordering::Relaxed),
                    1,
                    "lost first-use waiter allocated another Machine"
                );
                assert_eq!(provider.inspections.load(Ordering::Relaxed), 2);
            } else {
                // Producer loss wakes every original waiter with a typed cause.
                drop(release);
                assert!(matches!(
                    first.await,
                    Err(Error::Machines(ProviderError::Unavailable))
                ));
                for waiter in waiting {
                    assert!(matches!(
                        waiter.await,
                        Err(Error::Machines(ProviderError::Unavailable))
                    ));
                }
                assert_eq!(executors.calls.load(Ordering::Relaxed), 0);
                assert_eq!(provider.creates.load(Ordering::Relaxed), 1);
                continue;
            }
            release.send(true)?;
            for executor in futures::future::try_join_all(waiting).await? {
                assert!(Arc::ptr_eq(&executor, &executors.executor));
            }
            assert_eq!(executors.calls.load(Ordering::Relaxed), 1);
            let inspections = provider.inspections.load(Ordering::Relaxed);
            for _ in 0..callers {
                assert!(Arc::ptr_eq(&target.executor().await?, &executors.executor));
            }
            assert_eq!(provider.inspections.load(Ordering::Relaxed), inspections);
            assert_eq!(
                provider.inner.list_machines(None, 2).await?.machines.len(),
                1
            );

            // A fresh observer binds the same original Machine; an absent
            // original binding remains indeterminate without allocation.
            let invocation = ToolInvocation {
                operation_id: crate::OperationId::new(),
                call_id: "replay".into(),
                name: "shell".into(),
                arguments: serde_json::Value::Null,
            };
            for (agent, original) in [
                (target.agent.clone(), true),
                (
                    AgentPath::root(&crate::agent::AgentId::from_bytes([2; 16])),
                    false,
                ),
            ] {
                let recovered =
                    OnFirstUse::new(&target.machines, agent, "machine", executors.clone())?;
                let result = recovered.reconcile(invocation.clone(), None).await;
                if original {
                    assert!(matches!(result, Err(Error::Unsupported(_))));
                    assert_eq!(executors.calls.load(Ordering::Relaxed), 2);
                    assert_eq!(
                        provider.inspections.load(Ordering::Relaxed),
                        inspections + 1
                    );
                } else {
                    assert!(
                        matches!(result, Err(Error::Indeterminate(operation)) if operation == invocation.operation_id)
                    );
                    assert_eq!(executors.calls.load(Ordering::Relaxed), 2);
                    assert_eq!(
                        provider.inspections.load(Ordering::Relaxed),
                        inspections + 1
                    );
                }
                assert_eq!(provider.creates.load(Ordering::Relaxed), 1);
                assert!(unrelated.list_machines(None, 1).await?.machines.is_empty());
            }

            // Interrupted command recovery must refuse before inspecting or
            // binding the original UUID under a replacement provider identity.
            let mut replacement = target.machines.targets()[target.target].clone();
            replacement.host = MachinesHost::new(
                provider.clone(),
                ProviderRef::new(
                    "replacement".into(),
                    "machines".into(),
                    "1".into(),
                    &mut None,
                )?,
            )?;
            let changed = Arc::new(AgentMachines::new(
                Arc::clone(&target.machines.stream),
                vec![replacement],
            )?);
            let recovered =
                OnFirstUse::new(&changed, target.agent.clone(), "machine", executors.clone())?;
            assert!(matches!(
                recovered.reconcile(invocation, None).await,
                Err(Error::Invalid(_))
            ));
            assert_eq!(provider.creates.load(Ordering::Relaxed), 1);
            assert_eq!(
                provider.inspections.load(Ordering::Relaxed),
                inspections + 1
            );
            assert_eq!(executors.calls.load(Ordering::Relaxed), 2);
        }
    }
    Ok(())
}
