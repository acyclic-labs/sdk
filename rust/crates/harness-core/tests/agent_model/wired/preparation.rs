//! Forwarding observations over the original stock executor and actor topology.
use super::*;
use acyclic_harness_core::{
    executor::Executor,
    filesystem::{Grants, StockExecutors, TurnAuthority},
    model::{ModelEvent, ToolCall, ToolResultContent},
    test_support::{FixedToolPolicy, ScriptedEvent},
};
use futures::{FutureExt as _, channel::oneshot};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Probe {
    reads: AtomicUsize,
    answers: Mutex<Vec<u64>>,
    gate: Mutex<Option<oneshot::Receiver<()>>>,
    stop: Mutex<Option<acyclic_actors::guest::CancellationToken>>,
}

struct Children {
    original: Arc<dyn TurnChildren>,
    probe: Arc<Probe>,
}

impl TurnChildren for Children {
    fn agent(&self) -> &AgentPath {
        self.original.agent()
    }

    fn settled<'a>(
        &'a self,
        child: &'a AgentPath,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Option<Settlement>>> {
        Box::pin(async move {
            self.probe.reads.fetch_add(1, Ordering::SeqCst);
            let selected = self.original.settled(child).await?;
            let gate = self.probe.gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.await.map_err(|_| {
                    acyclic_harness_core::Error::Storage(
                        "original child observation producer dropped".into(),
                    )
                })?;
            }
            Ok(selected)
        })
    }

    fn answer<'a>(
        &'a self,
        child: &'a AgentPath,
        sequence: u64,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<String>> {
        self.probe.answers.lock().unwrap().push(sequence);
        self.original.answer(child, sequence)
    }
}

struct Executors {
    original: StockExecutors,
    probe: Arc<Probe>,
}

impl TurnExecutors for Executors {
    fn render_bytes(&self) -> Option<u64> {
        self.original.render_bytes()
    }

    fn executor<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Arc<dyn Executor>>> {
        Box::pin(async move {
            // Exercise the direct Allow path, which has no approval callback.
            // All files, grants, journal, effects and child reads remain real.
            let authority = TurnAuthority {
                templates: authority.templates.clone(),
                issuer: authority.issuer.clone(),
                scope: authority.scope.clone(),
                grants: authority.grants.clone(),
                content: authority.content.clone(),
                approval: Arc::new(FixedToolPolicy::new(
                    acyclic_harness_core::runtime::ToolPolicyDecision::Allow,
                )?),
                effects: authority.effects.clone(),
                journal: authority.journal.clone(),
                children: authority.children.as_ref().map(|original| {
                    Arc::new(Children {
                        original: original.clone(),
                        probe: self.probe.clone(),
                    }) as Arc<dyn TurnChildren>
                }),
                workspace_tools: authority.workspace_tools.clone(),
            };
            self.original.executor(agent, turn, input, &authority).await
        })
    }
}

struct Driver<D> {
    original: D,
    probe: Arc<Probe>,
}

impl<D: TurnDriver> TurnDriver for Driver<D> {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        cursor: u64,
        input: Option<Queued>,
        cancellation: acyclic_actors::guest::CancellationToken,
    ) -> TurnFuture<'a> {
        *self.probe.stop.lock().unwrap() = Some(cancellation.clone());
        self.original.step(agent, cursor, input, cancellation)
    }
}

fn wait(child: &AgentPath) -> ScriptedTurn {
    // Derive this simulation's path from the captured original wait input.
    // The schedule and canonical event wrapper are generated test inputs.
    let (_, mut arguments): (String, serde_json::Value) = serde_json::from_str(include_str!(
        "../../../../harness-fuzz/corpus/agent_wait_selection/recorded-path.json"
    ))
    .unwrap();
    arguments["agents"][0] = serde_json::json!(child.as_str());
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
            "selected-child".into(),
            "wait".into(),
            arguments,
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: serde_json::Value::Null,
        }),
    ])
}

fn result(model: &ScriptedModelProvider) -> serde_json::Value {
    let requests = model.requests();
    requests
        .last()
        .unwrap()
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::ToolResult {
                name,
                content: ToolResultContent::Json { value },
                ..
            } if name == "wait" => Some(value.clone()),
            _ => None,
        })
        .last()
        .expect("original wait did not publish a result")
}

pub(super) async fn check(
    stream: &Arc<MemoryStream>,
    filesystem: &Host,
    parent: &AgentPath,
    child: &AgentPath,
) -> Result {
    let probe = Arc::new(Probe::default());
    let model = Arc::new(ScriptedModelProvider::new([
        wait(child),
        ScriptedTurn::Text("child advanced after selection".into()),
        ScriptedTurn::Text("selected answer retained".into()),
        wait(child),
        ScriptedTurn::Text("child releases deferred wait".into()),
        ScriptedTurn::Text("deferred wait finished".into()),
        wait(child),
    ]));
    let executors = Executors {
        original: StockExecutors::new(
            (
                Model::new("test".into(), "text".into(), "1".into())?,
                model.clone(),
            ),
            ToolRegistry::default(),
            (Default::default(), CompactionPolicy::Disabled),
            Arc::new(Grants(vec!["tool:call:wait".into()])),
        )
        .with_agent_tools(&["wait".into()]),
        probe: probe.clone(),
    };
    let turn_host = FilesystemTurnHost::new(
        stream.clone(),
        within(),
        filesystem.clone(),
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "agents".into(),
            },
        ),
        TurnAdmission::LocalRoot,
        Arc::new(executors),
        LIMITS,
        acyclic_harness_core::approvals::ApprovalPolicy::accept_all("owner"),
    );
    let host = committing(
        stream,
        filesystem,
        Driver {
            original: ExecutorDriver::new(turn_host),
            probe: probe.clone(),
        },
    )?;
    observe(stream, &host, &probe, &model, parent, child).await
}

async fn observe(
    stream: &Arc<MemoryStream>,
    host: &AgentHost<MemoryStream>,
    probe: &Probe,
    model: &ScriptedModelProvider,
    parent: &AgentPath,
    child: &AgentPath,
) -> Result {
    let old = settled(stream, &within(), child)
        .await?
        .unwrap()
        .answer
        .unwrap();
    let (release, gate) = oneshot::channel();
    *probe.gate.lock().unwrap() = Some(gate);
    let mut pending = Box::pin(deliver(host, parent, requested("observe-original", parent)));
    assert!(
        pending.as_mut().now_or_never().is_none(),
        "wait skipped original observation"
    );
    assert_eq!(probe.reads.load(Ordering::SeqCst), 1);
    deliver(host, child, requested("advance-after-selection", child)).await?;
    let latest = settled(stream, &within(), child)
        .await?
        .unwrap()
        .answer
        .unwrap();
    assert_ne!(old, latest);
    release
        .send(())
        .map_err(|_| "original observation lost its reader")?;
    assert_eq!(pending.await?.phase, Phase::Idle);
    assert_eq!(
        probe.reads.swap(0, Ordering::SeqCst),
        1,
        "ready wait reread child status"
    );
    assert_eq!(
        *probe.answers.lock().unwrap(),
        [old],
        "wait replaced the original answer position"
    );
    let value = result(model);
    assert_eq!(value["settled"][0]["answer"], "later child answer");
    assert_eq!(value["settled"][0]["agent"], child.as_str());
    assert_eq!(value["running"], serde_json::json!([]));

    // A real committed child input makes the original condition unsatisfied.
    let Wake::Inbox { record, .. } = requested("unconsumed-child", child) else {
        unreachable!()
    };
    host.send(child, &super::super::user(), &record).await?;
    let deferred = deliver(host, parent, requested("defer-original", parent)).await?;
    assert!(
        matches!(deferred.phase, Phase::Waiting { .. }),
        "wait retained delivery instead of committing its condition"
    );
    assert_eq!(probe.reads.swap(0, Ordering::SeqCst), 1);
    // A later delivery consumes the persisted condition and its actual producer.
    host.drain(child).await?;
    host.drain(parent).await?;
    assert_eq!(host.state(parent).await?.phase, Phase::Idle);
    assert_eq!(probe.reads.swap(0, Ordering::SeqCst), 1);
    assert_eq!(
        result(model)["settled"][0]["answer"],
        "child releases deferred wait"
    );

    let (producer, gate) = oneshot::channel();
    *probe.gate.lock().unwrap() = Some(gate);
    let mut pending = Box::pin(deliver(host, parent, requested("cancel-original", parent)));
    assert!(pending.as_mut().now_or_never().is_none());
    probe.stop.lock().unwrap().as_ref().unwrap().cancel();
    assert!(
        pending.now_or_never().is_some(),
        "original cancellation did not end preparation"
    );
    assert!(
        producer.is_canceled(),
        "cancelled preparation retained its original read"
    );
    assert_eq!(probe.reads.load(Ordering::SeqCst), 1);
    Ok(())
}
