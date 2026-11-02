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
    entered: tokio::sync::Notify,
    creates: AtomicUsize,
    inspections: AtomicUsize,
    requests: AtomicUsize,
}

macro_rules! forward {
    ($($name:ident($($argument:ident: $kind:ty),*) -> $result:ty;)*) => {
        #[async_trait::async_trait]
        impl MachinesProvider for HeldInspection {
            fn assurance(&self) -> ProviderAssurance {
                self.inner.assurance()
            }

            async fn create(&self, request: CreateMachine) -> std::result::Result<MutationOutcome, ProviderError> {
                self.requests.fetch_add(1, Ordering::Relaxed);
                self.creates.fetch_add(1, Ordering::Relaxed);
                self.inner.create(request).await
            }

            async fn inspect_machine(&self, machine: MachineId) -> std::result::Result<MachineObservation, ProviderError> {
                self.requests.fetch_add(1, Ordering::Relaxed);
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
            self.requests.fetch_add(1, Ordering::Relaxed);
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
    for (target_count, callers) in [(1, 1), (1, 100), (1, 1_000), (100_000, 1)] {
        for drop_first in [false, true] {
            let (release, ready) = watch::channel(false);
            let provider = Arc::new(HeldInspection {
                inner: SimulatedMachines::default(),
                ready,
                entered: tokio::sync::Notify::new(),
                creates: AtomicUsize::new(0),
                inspections: AtomicUsize::new(0),
                requests: AtomicUsize::new(0),
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
        }
    }
    Ok(())
}

#[tokio::test]
async fn original_provider_precedes_machine_allocation_and_recovery() -> TestResult {
    for stage in 0..4 {
        let stream = Arc::new(MemoryStream::default());
        let (_release, ready) = watch::channel(true);
        let provider = Arc::new(HeldInspection {
            inner: SimulatedMachines::default(),
            ready,
            entered: tokio::sync::Notify::new(),
            creates: AtomicUsize::new(0),
            inspections: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
        });
        let original_ref =
            ProviderRef::new("original".into(), "machines".into(), "1".into(), &mut None)?;
        let target = ProvisionedTarget {
            name: "machine".into(),
            host: MachinesHost::new(provider.clone(), original_ref.clone())?,
            artifact: None,
            network_policy_digest: [2; 32],
        };
        let original = Arc::new(AgentMachines::new(stream.clone(), vec![target.clone()])?);
        let parent = AgentPath::root(&crate::agent::AgentId::from_bytes([1; 16]));
        let agent = parent.child(&crate::agent::AgentId::from_bytes([2; 16]))?;
        if stage == 3 {
            original.machine(&parent, "machine").await?;
        }
        let plan = original.plan(&agent, &target).await?;
        let allocated = match stage {
            1 => Some(original.create(&target, &agent).await?),
            2 => {
                original.machine(&agent, "machine").await?;
                original.binding(&agent, "machine").await?
            }
            _ => None,
        };
        assert_eq!(plan.source.is_some(), stage == 3);
        let choice = original.binding_record(&agent, "machine", 0).await?;
        let settled = original.binding(&agent, "machine").await?;

        // Keep the exact same underlying provider and Machine IDs. Only its
        // admitted identity changes, so a provider call could otherwise succeed.
        let mut changed = target.clone();
        changed.host = MachinesHost::new(
            provider.clone(),
            ProviderRef::new("foreign".into(), "machines".into(), "1".into(), &mut None)?,
        )?;
        let foreign = Arc::new(AgentMachines::new(stream.clone(), vec![changed])?);
        let executors = Arc::new(BoundExecutors {
            calls: AtomicUsize::new(0),
            executor: Arc::new(NeverRun),
        });
        let observer = OnFirstUse::new(&foreign, agent.clone(), "machine", executors.clone())?;
        let before = provider.requests.load(Ordering::Relaxed);
        assert!(matches!(
            foreign.machine(&agent, "machine").await,
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            foreign.suspend(&agent).await,
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            foreign.destroy(&agent).await,
            Err(Error::Invalid(_))
        ));
        assert!(matches!(observer.executor().await, Err(Error::Invalid(_))));
        let invocation = ToolInvocation {
            operation_id: crate::OperationId::new(),
            call_id: "replay".into(),
            name: "shell".into(),
            arguments: serde_json::Value::Null,
        };
        let reconciled = observer.reconcile(invocation, None).await;
        if stage == 2 {
            assert!(matches!(reconciled, Err(Error::Invalid(_))));
        } else {
            assert!(matches!(reconciled, Err(Error::Indeterminate(_))));
        }
        if stage == 3 {
            let sibling = parent.child(&crate::agent::AgentId::from_bytes([3; 16]))?;
            assert!(matches!(
                foreign.machine(&sibling, "machine").await,
                Err(Error::Invalid(_))
            ));
            assert!(original.original_plan(&sibling, "machine").await?.is_none());
        }
        assert_eq!(provider.requests.load(Ordering::Relaxed), before);
        assert_eq!(executors.calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            original
                .binding_record(&agent, "machine", 0)
                .await?
                .map(|record| record.value),
            choice.map(|record| record.value),
        );
        assert_eq!(original.binding(&agent, "machine").await?, settled);

        // Restoring the original provider resumes the same choice, including
        // accepted allocation whose binding was lost, and the chosen fork.
        let reopened = AgentMachines::new(stream, vec![target.clone()])?;
        let resumed = reopened.machine(&agent, "machine").await?;
        if let Some(allocated) = allocated {
            assert_eq!(resumed, sandbox(&target, &allocated)?);
        }
        let binding = reopened
            .binding(&agent, "machine")
            .await?
            .ok_or("binding lost")?;
        assert_eq!(binding.provider, original_ref);
        assert_eq!(binding.forked_from.is_some(), stage == 3);
        reopened.suspend(&agent).await?;
        assert_eq!(
            target.host.inspect(&resumed).await?.state,
            MachineState::Suspended
        );
        reopened.destroy(&agent).await?;
        assert_eq!(
            target.host.inspect(&resumed).await?.state,
            MachineState::Destroyed
        );
    }
    Ok(())
}
