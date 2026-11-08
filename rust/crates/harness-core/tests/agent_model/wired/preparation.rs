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
    answer_gate: Mutex<Option<oneshot::Receiver<()>>>,
    answering: Mutex<Option<oneshot::Sender<()>>>,
    gate: Mutex<Option<oneshot::Receiver<()>>>,
    observing: Mutex<Option<oneshot::Sender<()>>>,
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
                if let Some(observing) = self.probe.observing.lock().unwrap().take() {
                    let _ = observing.send(());
                }
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
        Box::pin(async move {
            self.probe.answers.lock().unwrap().push(sequence);
            let gate = self.probe.answer_gate.lock().unwrap().take();
            let answer = self.original.answer(child, sequence).await?;
            if let Some(gate) = gate {
                if let Some(answering) = self.probe.answering.lock().unwrap().take() {
                    let _ = answering.send(());
                }
                gate.await.map_err(|_| {
                    acyclic_harness_core::Error::Storage("original answer producer dropped".into())
                })?;
            }
            Ok(answer)
        })
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
    wait_selected(&[child])
}

fn wait_selected(children: &[&AgentPath]) -> ScriptedTurn {
    selected(children, None)
}

fn selected(children: &[&AgentPath], timeout_ms: Option<u64>) -> ScriptedTurn {
    // Derive this simulation's path from the captured original wait input.
    // The schedule and canonical event wrapper are generated test inputs.
    let (_, mut arguments): (String, serde_json::Value) = serde_json::from_str(include_str!(
        "../../../../harness-fuzz/corpus/agent_wait_selection/recorded-path.json"
    ))
    .unwrap();
    arguments["agents"] = serde_json::json!(
        children
            .iter()
            .map(|child| child.as_str())
            .collect::<Vec<_>>()
    );
    if let Some(timeout_ms) = timeout_ms {
        arguments["timeout_ms"] = serde_json::json!(timeout_ms);
    }
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
    let (clock, mut control) = acyclic_actors::local::ControlledClock::new(100);
    let host = AgentHost::with_spawn_volumes(
        stream.clone(),
        Driver {
            original: ExecutorDriver::new(turn_host),
            probe: probe.clone(),
        },
        within(),
        acyclic_actors::domain::ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
        Arc::new(clock),
        Arc::new(acyclic_harness_core::filesystem::FilesystemSpawnVolumes::new(filesystem.clone())),
    )?;
    observe(stream, &host, &probe, &model, parent, child, &mut control).await
}

async fn observe(
    stream: &Arc<MemoryStream>,
    host: &AgentHost<MemoryStream>,
    probe: &Probe,
    model: &ScriptedModelProvider,
    parent: &AgentPath,
    child: &AgentPath,
    control: &mut acyclic_actors::local::ClockControl,
) -> Result {
    let old = settled(stream, &within(), child)
        .await?
        .unwrap()
        .answer
        .unwrap();
    let (release, gate) = oneshot::channel();
    let (observing, observed) = oneshot::channel();
    *probe.observing.lock().unwrap() = Some(observing);
    *probe.gate.lock().unwrap() = Some(gate);
    let mut pending = Box::pin(deliver(host, parent, requested("observe-original", parent)));
    match futures::future::select(pending.as_mut(), observed).await {
        futures::future::Either::Right((observed, _)) => observed?,
        futures::future::Either::Left(_) => panic!("wait skipped original observation"),
    }
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

    deadlines(host, probe, model, parent, child, control).await?;
    parallel(stream, host, probe, model, parent, child).await?;
    model.extend([wait(child)]);

    let (producer, gate) = oneshot::channel();
    let (observing, observed) = oneshot::channel();
    *probe.observing.lock().unwrap() = Some(observing);
    *probe.gate.lock().unwrap() = Some(gate);
    let mut pending = Box::pin(deliver(host, parent, requested("cancel-original", parent)));
    match futures::future::select(pending.as_mut(), observed).await {
        futures::future::Either::Right((observed, _)) => observed?,
        futures::future::Either::Left(_) => panic!("wait skipped cancellation observation"),
    }
    probe.stop.lock().unwrap().as_ref().unwrap().cancel();
    let completed = pending.as_mut().now_or_never();
    assert!(
        producer.is_canceled(),
        "original cancellation did not end preparation"
    );
    match completed {
        Some(result) => result?,
        None => pending.await?,
    };
    assert_eq!(probe.reads.swap(0, Ordering::SeqCst), 1);
    dropped_answer(host, probe, model, parent, child).await
}

async fn dropped_answer(
    host: &AgentHost<MemoryStream>,
    probe: &Probe,
    model: &ScriptedModelProvider,
    parent: &AgentPath,
    child: &AgentPath,
) -> Result {
    model.extend([
        wait(child),
        ScriptedTurn::Text("masked answer failure".into()),
    ]);
    let (producer, gate) = oneshot::channel();
    let (answering, answered) = oneshot::channel();
    *probe.answer_gate.lock().unwrap() = Some(gate);
    *probe.answering.lock().unwrap() = Some(answering);
    let mut pending = Box::pin(deliver(
        host,
        parent,
        requested("lost-answer-producer", parent),
    ));
    match futures::future::select(pending.as_mut(), answered).await {
        futures::future::Either::Right((answered, _)) => answered?,
        futures::future::Either::Left(_) => panic!("wait skipped dropped answer observation"),
    }
    drop(producer);
    let error = pending
        .now_or_never()
        .expect("answer producer loss did not end preparation")
        .expect_err("answer producer loss became a successful answer");
    let Some(Error::Actor(error)) = error.downcast_ref::<Error>() else {
        return Err("answer producer loss lost its actor error class".into());
    };
    assert!(matches!(&**error, acyclic_actors::local::Error::Handler(
        acyclic_actors::guest::GuestError::Handler(message)
    ) if message.contains("original answer producer dropped")));
    assert_eq!(probe.reads.load(Ordering::SeqCst), 1);
    Ok(())
}

async fn deadlines(
    host: &AgentHost<MemoryStream>,
    probe: &Probe,
    model: &ScriptedModelProvider,
    parent: &AgentPath,
    child: &AgentPath,
    control: &mut acyclic_actors::local::ClockControl,
) -> Result {
    let Wake::Inbox { record, .. } = requested("deadline-child-work", child) else {
        unreachable!()
    };
    host.send(child, &super::super::user(), &record).await?;
    let requests = model.requests().len();
    model.extend([
        selected(&[child], Some(10)),
        selected(&[child], Some(20)),
        ScriptedTurn::Text("both original deadlines completed".into()),
        ScriptedTurn::Text("child releases deferred wait".into()),
    ]);
    let first = deliver(host, parent, requested("independent-deadlines", parent)).await?;
    assert_eq!(
        first.phase,
        Phase::Waiting {
            deadline_unix_ms: Some(110),
        },
        "first wait lost its original deadline"
    );
    assert_eq!(model.requests().len(), requests + 1);
    control.advance_to(110)?;
    host.drain(parent).await?;
    assert_eq!(
        host.state(parent).await?.phase,
        Phase::Waiting {
            deadline_unix_ms: Some(130),
        },
        "second wait reused the first deadline release"
    );
    assert_eq!(model.requests().len(), requests + 2);
    assert_eq!(
        result(model)["running"],
        serde_json::json!([child.as_str()])
    );
    control.advance_to(129)?;
    host.drain(parent).await?;
    assert_eq!(model.requests().len(), requests + 2);
    control.advance_to(130)?;
    host.drain(parent).await?;
    assert_eq!(host.state(parent).await?.phase, Phase::Idle);
    assert_eq!(model.requests().len(), requests + 3);
    assert_eq!(result(model)["settled"], serde_json::json!([]));
    assert_eq!(
        result(model)["running"],
        serde_json::json!([child.as_str()])
    );
    assert_eq!(probe.reads.swap(0, Ordering::SeqCst), 4);
    host.drain(child).await?;
    host.drain(parent).await?;
    Ok(())
}

async fn parallel(
    stream: &Arc<MemoryStream>,
    host: &AgentHost<MemoryStream>,
    probe: &Probe,
    model: &ScriptedModelProvider,
    parent: &AgentPath,
    child: &AgentPath,
) -> Result {
    let request = InboxRecord::Spawn(SpawnRequest {
        by: super::super::user(),
        id: intent("parallel-answer-child"),
        brief: Box::new(answer(&**stream, parent, 0).await?.output),
        profile: CreationConfig {
            context: SpawnContext::Fork,
            template: None,
            peers: Default::default(),
        },
        history: None,
    });
    let Delivered::Committed { inbox } = host.send(parent, &super::super::user(), &request).await?
    else {
        return Err("parallel child request was not committed".into());
    };
    let (_, other) = requested_child(parent, inbox)?;
    host.drain(parent).await?;
    model.extend([ScriptedTurn::Text("parallel child answer".into())]);
    host.drain(&other).await?;
    let selected = [
        settled(stream, &within(), child)
            .await?
            .unwrap()
            .answer
            .unwrap(),
        settled(stream, &within(), &other)
            .await?
            .unwrap()
            .answer
            .unwrap(),
    ];
    model.extend([
        wait_selected(&[child, &other]),
        ScriptedTurn::Text("parallel wait finished".into()),
    ]);
    probe.answers.lock().unwrap().clear();
    let (release, gate) = oneshot::channel();
    let (answering, answered) = oneshot::channel();
    *probe.answer_gate.lock().unwrap() = Some(gate);
    *probe.answering.lock().unwrap() = Some(answering);
    let mut pending = Box::pin(deliver(host, parent, requested("parallel-answers", parent)));
    match futures::future::select(pending.as_mut(), answered).await {
        futures::future::Either::Right((answered, _)) => answered?,
        futures::future::Either::Left(_) => panic!("wait skipped original answer"),
    }
    assert_eq!(
        *probe.answers.lock().unwrap(),
        selected,
        "selected answers waited for an unrelated answer producer"
    );
    release
        .send(())
        .map_err(|_| "original answer lost its reader")?;
    assert_eq!(pending.await?.phase, Phase::Idle);
    assert_eq!(probe.reads.swap(0, Ordering::SeqCst), 2);
    let value = result(model);
    for (index, (path, text)) in [
        (child, "child releases deferred wait"),
        (&other, "parallel child answer"),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(value["settled"][index]["agent"], path.as_str());
        assert_eq!(value["settled"][index]["answer"], text);
    }
    Ok(())
}
