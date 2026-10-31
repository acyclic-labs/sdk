use super::*;
use crate::agent::*;
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_machines::SimulatedMachines;
use acyclic_stream::MemoryStream;
use tokio::sync::Semaphore;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const TARGET: &str = "provisioned";

fn manager(
    stream: Arc<MemoryStream>,
    provider: SimulatedMachines,
) -> TestResult<Arc<AgentMachines<MemoryStream>>> {
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
                reserved_children: 0,
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
    let parking = Semaphore::new(0);
    let parked = Semaphore::new(0);
    let pause = host.runtime().pause_with(&actor, true, async {
        parked.add_permits(1);
        parking.acquire().await.map(|permit| permit.forget())
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
