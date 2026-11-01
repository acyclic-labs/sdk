use super::*;
use crate::Error;
use crate::agent::*;
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_machines::SimulatedMachines;
use acyclic_stream::{MemoryStream, testing::Counted};
use tokio::sync::Semaphore;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const TARGET: &str = "provisioned";

fn manager<P: crate::store::JournalProvider>(
    stream: Arc<P>,
    provider: SimulatedMachines,
) -> TestResult<Arc<AgentMachines<P>>> {
    let provider_ref = crate::resources::ProviderRef::new(
        "example".into(),
        "machines".into(),
        "1".into(),
        &mut None,
    )?;
    Ok(Arc::new(AgentMachines::new(
        stream,
        vec![ProvisionedTarget {
            name: TARGET.into(),
            host: MachinesHost::new(Arc::new(provider), provider_ref.clone())?,
            artifact: Some(ArtifactRef::new(provider_ref, [1; 32].into(), None)?),
            network_policy_digest: [2; 32],
        }],
    )?))
}

#[tokio::test]
async fn original_plans_bound_lifecycle_discovery_and_release() -> TestResult {
    for target_count in [1, 100_000] {
        let stream = Arc::new(Counted::new(MemoryStream::default()));
        let provider = SimulatedMachines::default();
        let original = manager(stream.clone(), provider.clone())?;
        let target = original.target(TARGET)?.clone();
        let unrelated = manager(
            Arc::new(MemoryStream::default()),
            SimulatedMachines::default(),
        )?;
        let mut targets = (1..target_count)
            .map(|position| {
                let mut target = unrelated.targets()[0].clone();
                target.name = format!("unheld-{position}");
                target
            })
            .collect::<Vec<_>>();
        targets.push(target.clone());
        let machines = AgentMachines::new(stream.clone(), targets)?;
        let agent = AgentPath::root(&AgentId::from_bytes([9; 16]));

        let before = stream.requests();
        machines.suspend(&agent).await?;
        machines.destroy(&agent).await?;
        let empty = stream.requests().since(before);
        assert_eq!(empty.data(), 0, "unheld targets were probed");
        assert_eq!(empty.control(), 2);

        // The original plan is the index even when no binding survived.
        machines.plan(&agent, &target).await?;
        let allocated = machines.create(&target, &agent).await?;
        let sandbox = sandbox(&target, &allocated)?;
        let reopened = AgentMachines::new(stream.clone(), machines.targets.clone())?;
        drop(machines);
        reopened.suspend(&agent).await?;
        assert_eq!(
            target.host.inspect(&sandbox).await?.state,
            MachineState::Suspended
        );
        assert_eq!(reopened.binding(&agent, TARGET).await?, Some(allocated));

        for destroy in [false, true, true] {
            let before = stream.requests();
            if destroy {
                reopened.destroy(&agent).await?;
            } else {
                reopened.suspend(&agent).await?;
            }
            let held = stream.requests().since(before);
            assert_eq!(held.data(), 1, "lifecycle read an unheld target");
            assert_eq!(held.control(), 1);
            assert_eq!(
                target.host.inspect(&sandbox).await?.state,
                if destroy {
                    MachineState::Destroyed
                } else {
                    MachineState::Suspended
                },
            );
        }
        assert!(
            unrelated.targets()[0]
                .host
                .provider
                .list_machines(None, 1)
                .await?
                .machines
                .is_empty()
        );

        // Losing the admitted configuration must refuse, not report that the
        // original holder disappeared and silently skip its release.
        let missing = AgentMachines::new(stream.clone(), Vec::new())?;
        assert!(matches!(
            missing.destroy(&agent).await,
            Err(Error::Invalid(_))
        ));
        let mut nested = target;
        nested.name = "nested/target".into();
        assert!(AgentMachines::new(stream, vec![nested]).is_err());
    }

    // More than one page is still bounded by original plans, never by the
    // target table or an accumulated list of bindings.
    let stream = Arc::new(Counted::new(MemoryStream::default()));
    let original = manager(stream.clone(), SimulatedMachines::default())?;
    let targets = (0..65)
        .map(|position| {
            let mut target = original.targets()[0].clone();
            target.name = format!("held-{position:03}");
            target
        })
        .collect();
    let machines = AgentMachines::new(stream.clone(), targets)?;
    let agent = AgentPath::root(&AgentId::from_bytes([10; 16]));
    for target in machines.targets() {
        machines.machine_on(&agent, target).await?;
    }
    let before = stream.requests();
    machines.destroy(&agent).await?;
    let released = stream.requests().since(before);
    assert_eq!(released.data(), 65);
    assert_eq!(released.control(), 2);
    for target in machines.targets() {
        let binding = machines
            .binding(&agent, &target.name)
            .await?
            .ok_or("binding lost")?;
        assert_eq!(
            target
                .host
                .inspect(&sandbox(target, &binding)?)
                .await?
                .state,
            MachineState::Destroyed
        );
    }

    // Agent control has fixed this holder set. Publication elsewhere in the
    // same namespace must not invalidate its next ownership page.
    let fenced = stream
        .children_page(ChildrenPageRequest {
            tail_stream: None,
            parent: Some(agent.machines()?),
            after: None,
            hierarchy_version: None,
            limit: 64,
        })
        .await?;
    let held = machines.held_targets(&agent)?;
    futures::pin_mut!(held);
    for _ in 0..64 {
        assert!(held.try_next().await?.is_some());
    }
    let other = AgentPath::root(&AgentId::from_bytes([11; 16]));
    machines.plan(&other, &machines.targets()[0]).await?;
    assert_eq!(
        held.try_next().await?.map(|target| target.name.as_str()),
        Some("held-064")
    );
    assert!(held.try_next().await?.is_none());
    assert_eq!(
        stream
            .children_page(ChildrenPageRequest {
                tail_stream: None,
                parent: Some(agent.machines()?),
                after: fenced.next_after,
                hierarchy_version: Some(fenced.hierarchy_version),
                limit: 64,
            })
            .await,
        Err(StreamError::HierarchyChanged)
    );
    Ok(())
}

#[tokio::test]
async fn original_machine_ownership_survives_missing_binding_and_changed_parent() -> TestResult {
    let stream = Arc::new(MemoryStream::default());
    let provider = SimulatedMachines::default();
    let machines = manager(stream.clone(), provider.clone())?;
    let parent = AgentPath::root(&AgentId::from_bytes([1; 16]));
    let child = parent.child(&AgentId::from_bytes([2; 16]))?;
    let target = machines.target(TARGET)?;

    // Stop after the real allocation, before its binding/acknowledgement.
    // The parent acquires a forkable Machine before the child's retry.
    assert!(machines.plan(&child, target).await?.source.is_none());
    let allocated = machines.create(target, &child).await?;
    assert!(machines.binding(&child, TARGET).await?.is_none());
    machines.machine(&parent, TARGET).await?;
    let reopened = manager(stream.clone(), provider.clone())?;
    let (one, two) = futures::try_join!(
        reopened.machine(&child, TARGET),
        machines.machine(&child, TARGET),
    )?;
    assert_eq!(one, two);
    assert_eq!(one, sandbox(target, &allocated)?);
    assert_eq!(reopened.binding(&child, TARGET).await?, Some(allocated));

    // A chosen fork also survives parent state changes and waiter loss.
    let fork = parent.child(&AgentId::from_bytes([3; 16]))?;
    let plan = machines.plan(&fork, target).await?;
    let source = plan.source.ok_or("parent was not selected")?;
    let allocated = machines.fork(target, &fork, &source).await?;
    machines.suspend(&parent).await?;
    assert_eq!(
        reopened.machine(&fork, TARGET).await?,
        sandbox(target, &allocated)?
    );
    assert_eq!(reopened.binding(&fork, TARGET).await?, Some(allocated));

    // Control parking must reconcile an allocation with no binding too.
    let pending = AgentPath::root(&AgentId::from_bytes([4; 16]));
    machines.plan(&pending, target).await?;
    let allocated = machines.create(target, &pending).await?;
    reopened.suspend(&pending).await?;
    assert_eq!(
        target
            .host
            .inspect(&sandbox(target, &allocated)?)
            .await?
            .state,
        MachineState::Suspended
    );
    assert_eq!(reopened.binding(&pending, TARGET).await?, Some(allocated));
    Ok(())
}

struct DelayedTurn {
    machines: Arc<AgentMachines<MemoryStream>>,
    started: Arc<Semaphore>,
    stopped: Arc<Semaphore>,
    cleanup: Arc<Semaphore>,
}

impl TurnDriver for DelayedTurn {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        _cursor: u64,
        _input: Option<Queued>,
        cancellation: acyclic_actors::guest::CancellationToken,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            if agent.path.id() == AgentId::from_bytes([1; 16]) {
                self.started.add_permits(1);
                cancellation.cancelled().await;
                // An admitted provider completion may arrive after pause was
                // requested. Pause must join it before suspending resources.
                self.machines
                    .machine(&agent.path, TARGET)
                    .await
                    .map_err(|error| crate::agent::Error::Turn(Box::new(error)))?;
                self.stopped.add_permits(1);
                self.cleanup
                    .acquire()
                    .await
                    .map_err(|_| crate::agent::Error::Record("cleanup producer closed"))?
                    .forget();
            }
            Ok(TurnStep {
                checkpoint: None,
                outs: Vec::new(),
                wait: Wait::Input,
                drops: None,
            })
        })
    }
}

fn adopted(byte: u8) -> ForkAdopted {
    ForkAdopted {
        agent: AgentId::from_bytes([byte; 16]),
        parent: None,
        forked_at: 0,
        conversation_tail: 0,
        checkpoint: None,
        volumes: Vec::new(),
        brief: None,
        policy: None,
    }
}

#[tokio::test]
async fn original_machine_ownership_pause_joins_pending_work_before_suspending() -> TestResult {
    let stream = Arc::new(MemoryStream::default());
    let machines = manager(stream.clone(), SimulatedMachines::default())?;
    let started = Arc::new(Semaphore::new(0));
    let stopped = Arc::new(Semaphore::new(0));
    let cleanup = Arc::new(Semaphore::new(0));
    let host = AgentHost::composing(
        stream,
        DelayedTurn {
            machines: machines.clone(),
            started: started.clone(),
            stopped: stopped.clone(),
            cleanup: cleanup.clone(),
        },
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
        None,
        Some(machines.clone()),
    )?;
    let first = host
        .create(&IntentId::try_from("first")?, adopted(1))
        .await?;
    let other = host
        .create(&IntentId::try_from("other")?, adopted(2))
        .await?;
    host.drain(&first).await?;
    host.drain(&other).await?;
    let sender = Sender::User {
        principal: "user".into(),
    };
    for agent in [&first, &other] {
        host.send(
            agent,
            &sender,
            &InboxRecord::ApprovalRequested(ApprovalRequested {
                child: agent.clone(),
                id: IntentId::try_from("input")?,
            }),
        )
        .await?;
    }
    let delivery = host.drain(&first);
    futures::pin_mut!(delivery);
    tokio::select! {
        permit = started.acquire() => permit?.forget(),
        result = &mut delivery => panic!("delivery ended before original work started: {result:?}"),
    }
    let pause = host.pause(&first, true);
    futures::pin_mut!(pause);
    assert!(
        futures::poll!(&mut pause).is_pending(),
        "pause must join the active delivery"
    );
    tokio::select! {
        permit = stopped.acquire() => permit?.forget(),
        result = &mut delivery => panic!("delivery ended before pending completion: {result:?}"),
    }
    assert!(
        futures::poll!(&mut pause).is_pending(),
        "provider completion is not handler cleanup"
    );
    // An unrelated coordinator makes progress while this cleanup is pending.
    host.drain(&other).await?;
    cleanup.add_permits(1);
    delivery.await?;
    pause.await?;
    let machine = machines.machine(&first, TARGET).await?;
    assert_eq!(
        machines.target(TARGET)?.host.inspect(&machine).await?.state,
        MachineState::Suspended
    );
    host.drain(&first).await?;
    assert_eq!(
        machines.target(TARGET)?.host.inspect(&machine).await?.state,
        MachineState::Suspended,
        "no new delivery wakes the paused Machine"
    );
    host.pause(&first, false).await?;

    // Resource parking remains inside that same control fence. A concurrent
    // resume cannot overtake the original owner's pending completion.
    let actor = ActorReference::new(b"account", b"agents", first.id().as_str().as_bytes())?;
    let parking = Arc::new(Semaphore::new(0));
    let parked = Arc::new(Semaphore::new(0));
    let parking_owner = Arc::clone(&parking);
    let parked_owner = Arc::clone(&parked);
    let pause = host.runtime().pause_with(&actor, true, async move {
        parked_owner.add_permits(1);
        parking_owner.acquire().await.map(|permit| permit.forget())
    });
    futures::pin_mut!(pause);
    assert!(futures::poll!(&mut pause).is_pending());
    tokio::select! {
        permit = parked.acquire() => permit?.forget(),
        result = &mut pause => panic!("pause ended before resource parking: {result:?}"),
    }
    let resume = host.pause(&first, false);
    futures::pin_mut!(resume);
    assert!(
        futures::poll!(&mut resume).is_pending(),
        "resume passed unfinished resource parking"
    );
    parking.add_permits(1);
    pause.await??;
    resume.await?;
    Ok(())
}
