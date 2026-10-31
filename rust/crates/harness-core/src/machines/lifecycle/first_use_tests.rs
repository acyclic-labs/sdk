use super::*;
use acyclic_machines::*;
use acyclic_stream::MemoryStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::watch;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

// Every operation reaches the real simulator. Only its original inspection
// response is held, so overlapping first uses are deterministic.
struct HeldInspection {
    inner: SimulatedMachines,
    ready: watch::Receiver<bool>,
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
        usage(machine: MachineId, start_unix_ms: u64, end_unix_ms: u64) -> UsageReceipt;
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
    for callers in [1, 100, 1_000] {
        for drop_first in [false, true] {
            let (release, ready) = watch::channel(false);
            let provider = Arc::new(HeldInspection {
                inner: SimulatedMachines::default(),
                ready,
                creates: AtomicUsize::new(0),
                inspections: AtomicUsize::new(0),
            });
            let provider_ref =
                ProviderRef::new("example".into(), "machines".into(), "1".into(), &mut None)?;
            let machines = Arc::new(AgentMachines::new(
                Arc::new(MemoryStream::default()),
                vec![ProvisionedTarget {
                    name: "machine".into(),
                    host: MachinesHost::new(provider.clone(), provider_ref.clone())?,
                    artifact: Some(ArtifactRef::new(provider_ref, [1; 32].into(), None)?),
                    network_policy_digest: [2; 32],
                }],
            )?);
            let executors = Arc::new(BoundExecutors {
                calls: AtomicUsize::new(0),
                executor: Arc::new(NeverRun),
            });
            let target = OnFirstUse {
                machines,
                agent: AgentPath::root(&crate::agent::AgentId::from_bytes([1; 16])),
                target: "machine".into(),
                executors: executors.clone(),
                bound: OnceCell::new(),
            };
            let mut first = Box::pin(target.executor());
            assert!(futures::poll!(&mut first).is_pending());
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
            if drop_first {
                drop(first);
                assert!(futures::poll!(&mut waiting[0]).is_pending());
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
        }
    }
    Ok(())
}
