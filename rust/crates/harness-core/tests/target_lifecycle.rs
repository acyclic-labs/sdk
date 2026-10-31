//! Conformance of an agent's Machines across its lifecycle: lazy creation on
//! first exec, fork from the parent, pause, provider idle, and archive. Each
//! transition is checked against a negative control that must not move.

use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_harness_core::{
    IdempotencyKey, OperationId,
    agent::*,
    machines::{AgentMachines, MachineExecutors, MachinesHost, ProvisionedTarget},
    resources::{ArtifactRef, ProviderRef, SandboxRef},
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_machines::{
    Capability, ExecCommand, MachineObservation, MachineState, SimulatedMachines,
};
use acyclic_stream::{BoxProviderFuture, MemoryStream};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const CLOUD: &str = "cloud";

fn id(byte: u8) -> AgentId {
    AgentId::from_bytes([byte; 16])
}

fn intent(text: &str) -> IntentId {
    IntentId::try_from(text).expect("valid intent")
}

fn provider_ref() -> Result<ProviderRef> {
    Ok(ProviderRef::new(
        "example".into(),
        "machines".into(),
        "1".into(),
        &mut None,
    )?)
}

fn target(provider: SimulatedMachines) -> Result<ProvisionedTarget> {
    let provider_ref = provider_ref()?;
    Ok(ProvisionedTarget {
        name: CLOUD.into(),
        host: MachinesHost::new(Arc::new(provider), provider_ref.clone())?,
        artifact: Some(ArtifactRef::new(provider_ref, [1; 32].into(), None)?),
        network_policy_digest: [2; 32],
    })
}

fn machines(
    stream: &Arc<MemoryStream>,
    provider: SimulatedMachines,
) -> Result<Arc<AgentMachines<MemoryStream>>> {
    Ok(Arc::new(AgentMachines::new(
        stream.clone(),
        vec![target(provider)?],
    )?))
}

fn cloud(machines: &AgentMachines<MemoryStream>) -> Result<&ProvisionedTarget> {
    Ok(machines.targets().first().ok_or("no target")?)
}

async fn state(machines: &AgentMachines<MemoryStream>, agent: &AgentPath) -> Result<MachineState> {
    let target = cloud(machines)?;
    let sandbox = machines.machine(agent, CLOUD).await?;
    Ok(target.host.inspect(&sandbox).await?.state)
}

/// Binds a Machine to an executor whose every command runs one `exec` on it.
#[derive(Default)]
struct Executors {
    bound: AtomicUsize,
}

struct Exec {
    host: MachinesHost,
    sandbox: SandboxRef,
}

impl MachineExecutors for Executors {
    fn bind(
        &self,
        target: &ProvisionedTarget,
        sandbox: SandboxRef,
        _observed: MachineObservation,
    ) -> acyclic_harness_core::Result<Arc<dyn ToolExecutor>> {
        self.bound.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Exec {
            host: target.host.clone(),
            sandbox,
        }))
    }
}

impl ToolExecutor for Exec {
    fn execute<'a>(
        &'a self,
        _invocation: ToolInvocation,
        _context: Option<acyclic_harness_core::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<ToolResult>> {
        Box::pin(async move {
            let command = ExecCommand::new(vec!["true".into()], 1_000)
                .map_err(acyclic_harness_core::Error::Machines)?;
            // The simulator admits (and wakes) before it reports that it
            // runs no processes; admission is what this suite observes.
            let _ = self.host.exec(&self.sandbox, command).await;
            Ok(ToolResult {
                value: serde_json::Value::Null,
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        _invocation: ToolInvocation,
        _context: Option<acyclic_harness_core::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

fn shell() -> ToolInvocation {
    ToolInvocation {
        operation_id: OperationId::from_bytes([7; 16]),
        call_id: "call".into(),
        name: "shell".into(),
        arguments: serde_json::json!({ "command": "true", "target": CLOUD }),
    }
}

#[tokio::test]
async fn first_exec_creates_the_agents_own_machine_once() -> Result {
    let stream = Arc::new(MemoryStream::default());
    let machines = machines(&stream, SimulatedMachines::default())?;
    let executors = Arc::new(Executors::default());
    let (one, two) = (AgentPath::root(&id(1)), AgentPath::root(&id(2)));
    let executor = machines.executor(one.clone(), CLOUD, executors.clone())?;

    // Spawning or binding the target provisions nothing.
    assert_eq!(machines.binding(&one, CLOUD).await?, None);
    assert_eq!(executors.bound.load(Ordering::SeqCst), 0);

    executor.execute(shell(), None).await?;
    let bound = machines.binding(&one, CLOUD).await?.ok_or("no binding")?;
    assert_eq!(bound.forked_from, None);
    executor.execute(shell(), None).await?;
    assert_eq!(executors.bound.load(Ordering::SeqCst), 1);
    assert_eq!(machines.binding(&one, CLOUD).await?, Some(bound.clone()));

    // A retried first use (a lost binding commit) names the same Machine.
    let target = cloud(&machines)?;
    let retried = target
        .host
        .create(
            OperationId::new(),
            &IdempotencyKey::new(format!("acyclic.harness.machine.create.v1:{one}:{CLOUD}"))?,
            target.artifact.as_ref(),
            None,
            target.network_policy_digest,
            |_| {},
        )
        .await?;
    assert_eq!(retried, machines.machine(&one, CLOUD).await?);

    // Negative control: another agent gets its own Machine.
    assert_eq!(machines.binding(&two, CLOUD).await?, None);
    assert_ne!(machines.machine(&two, CLOUD).await?, retried);

    // Every Machine is idle-suspended by the provider and woken by exec:
    // the harness keeps no idle timer.
    let observed = target.host.inspect(&retried).await?;
    assert!(observed.contract.wakes_on_demand());
    Ok(())
}

#[tokio::test]
async fn a_fork_forks_its_parents_machine_when_the_target_can() -> Result {
    let parent = AgentPath::root(&id(1));
    let child = parent.child(&id(2))?;
    for live_fork in [true, false] {
        let stream = Arc::new(MemoryStream::default());
        let provider = if live_fork {
            SimulatedMachines::default()
        } else {
            SimulatedMachines::with_capabilities([Capability::SuspendResume].into())
        };
        let machines = machines(&stream, provider)?;
        let source = machines.machine(&parent, CLOUD).await?;
        let forked = machines.machine(&child, CLOUD).await?;
        let binding = machines.binding(&child, CLOUD).await?.ok_or("no binding")?;
        assert_ne!(forked, source);
        let parent_binding = machines.binding(&parent, CLOUD).await?.ok_or("no parent")?;
        // Negative control: without a fork capability the child is created fresh.
        assert_eq!(
            binding.forked_from,
            live_fork.then_some(parent_binding.machine)
        );
        assert_eq!(state(&machines, &parent).await?, MachineState::Running);

        // A paused (or provider-idled) parent is a checkpoint: a later
        // child forks it without waking it.
        machines.suspend(&parent).await?;
        let late = parent.child(&id(3))?;
        machines.machine(&late, CLOUD).await?;
        let binding = machines.binding(&late, CLOUD).await?.ok_or("no binding")?;
        assert_eq!(binding.forked_from.is_some(), live_fork);
        assert_eq!(state(&machines, &parent).await?, MachineState::Suspended);
    }

    // A child whose parent never ran the target creates its own.
    let stream = Arc::new(MemoryStream::default());
    let machines = machines(&stream, SimulatedMachines::default())?;
    machines.machine(&child, CLOUD).await?;
    assert_eq!(machines.binding(&parent, CLOUD).await?, None);
    let binding = machines.binding(&child, CLOUD).await?.ok_or("no binding")?;
    assert_eq!(binding.forked_from, None);
    Ok(())
}

fn host(
    stream: &Arc<MemoryStream>,
    lifecycle: Option<Arc<dyn AgentLifecycle>>,
) -> Result<AgentHost<MemoryStream>> {
    Ok(AgentHost::composing(
        stream.clone(),
        Spawner::default(),
        ActorReference::new(b"account", b"agents", b"owner")?,
        ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(SystemClock),
        None,
        lifecycle,
    )?)
}

#[tokio::test]
async fn pause_suspends_and_the_next_exec_wakes() -> Result {
    let stream = Arc::new(MemoryStream::default());
    let machines = machines(&stream, SimulatedMachines::default())?;
    let host = host(&stream, Some(machines.clone()))?;
    let paused = host.create(&intent("one"), adopted(id(1))).await?;
    let running = host.create(&intent("two"), adopted(id(2))).await?;
    assert!(host.runtime().drain().await.is_empty());
    let executors = Arc::new(Executors::default());
    let executor = machines.executor(paused.clone(), CLOUD, executors)?;
    executor.execute(shell(), None).await?;
    machines.machine(&running, CLOUD).await?;

    host.pause(&paused, true).await?;
    assert_eq!(state(&machines, &paused).await?, MachineState::Suspended);
    // Negative control: pausing one agent parks only its own Machines.
    assert_eq!(state(&machines, &running).await?, MachineState::Running);
    // Pausing again is harmless.
    host.pause(&paused, true).await?;

    host.pause(&paused, false).await?;
    assert_eq!(state(&machines, &paused).await?, MachineState::Suspended);
    executor.execute(shell(), None).await?;
    assert_eq!(state(&machines, &paused).await?, MachineState::Running);
    Ok(())
}

#[tokio::test]
async fn archive_destroys_the_agents_machines_before_it_closes() -> Result {
    let stream = Arc::new(MemoryStream::default());
    let machines = machines(&stream, SimulatedMachines::default())?;
    let host = host(&stream, Some(machines.clone()))?;
    let root = host.create(&intent("root"), adopted(id(1))).await?;
    let other = host.create(&intent("other"), adopted(id(2))).await?;
    assert!(host.runtime().drain().await.is_empty());
    // The root's first turn (its first input after adoption) spawns `c`.
    let child = root.child(&AgentId::derive(&root.id(), "c"))?;
    let input = InboxRecord::ApprovalRequested(ApprovalRequested {
        child: child.clone(),
        id: intent("input"),
    });
    host.send(&root, &user(), &input).await?;
    assert!(host.runtime().drain().await.is_empty());
    assert!(
        host.state(&child).await.is_ok(),
        "the root spawned its child"
    );

    // The child forks the root's Machine, so the provider keeps the root's
    // until the fork is gone: the cascade must release both.
    machines.machine(&root, CLOUD).await?;
    machines.machine(&child, CLOUD).await?;
    machines.machine(&other, CLOUD).await?;
    let forked = machines.binding(&child, CLOUD).await?.ok_or("no binding")?;
    assert!(forked.forked_from.is_some());

    assert!(matches!(
        host.archive(&root, &user(), &intent("archive")).await?,
        Delivered::Committed { .. }
    ));
    assert!(host.runtime().drain().await.is_empty());
    assert!(host.state(&root).await?.closed);
    // The parent closed first and the provider keeps its source for the fork.
    assert_eq!(state(&machines, &root).await?, MachineState::Running);
    // The cascade's archive reaches the child (a running host discovers it
    // through its activation follow; this deterministic host names it).
    host.host(&child)?;
    assert!(host.runtime().drain().await.is_empty());
    assert!(host.state(&child).await?.closed);
    assert_eq!(state(&machines, &root).await?, MachineState::Destroyed);
    assert_eq!(state(&machines, &child).await?, MachineState::Destroyed);
    // Negative control: an agent outside the archived tree keeps its Machine.
    assert_eq!(state(&machines, &other).await?, MachineState::Running);
    // A retried release replays the original destruction.
    machines.destroy(&root).await?;
    machines.destroy(&child).await?;
    Ok(())
}

#[tokio::test]
async fn a_host_without_lifecycle_releases_nothing() -> Result {
    // Negative control for the archive participant: the closing step only
    // releases resources through the host's lifecycle.
    let stream = Arc::new(MemoryStream::default());
    let machines = machines(&stream, SimulatedMachines::default())?;
    let host = host(&stream, None)?;
    let root = host.create(&intent("root"), adopted(id(1))).await?;
    assert!(host.runtime().drain().await.is_empty());
    machines.machine(&root, CLOUD).await?;
    host.archive(&root, &user(), &intent("archive")).await?;
    assert!(host.runtime().drain().await.is_empty());
    assert!(host.state(&root).await?.closed);
    assert_eq!(state(&machines, &root).await?, MachineState::Running);
    host.pause(&root, true).await?;
    assert_eq!(state(&machines, &root).await?, MachineState::Running);
    Ok(())
}

/// Spawns child `c` on its first turn and idles afterwards.
#[derive(Default)]
struct Spawner(AtomicUsize);

impl TurnDriver for Spawner {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        _cursor: u64,
        _input: Option<Queued>,
        _cancellation: acyclic_actors::guest::CancellationToken,
    ) -> TurnFuture<'a> {
        let first =
            agent.path == AgentPath::root(&id(1)) && self.0.fetch_add(1, Ordering::SeqCst) == 0;
        Box::pin(async move {
            Ok(TurnStep {
                reserved_children: 0,
                checkpoint: None,
                outs: if first {
                    vec![Out::Spawn {
                        intent: intent("c"),
                        brief: None,
                        volumes: Vec::new(),
                    }]
                } else {
                    Vec::new()
                },
                wait: Wait::Input,
                drops: None,
            })
        })
    }
}

fn user() -> Sender {
    Sender::User {
        principal: "user-1".into(),
    }
}

fn adopted(agent: AgentId) -> ForkAdopted {
    ForkAdopted {
        agent,
        parent: None,
        forked_at: 0,
        conversation_tail: 0,
        checkpoint: None,
        volumes: Vec::new(),
        brief: None,
        policy: None,
    }
}
