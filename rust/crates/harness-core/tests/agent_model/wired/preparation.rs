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
    journal: Mutex<
        Option<(
            OperationId,
            Arc<dyn acyclic_harness_core::executor::ExecutionJournal>,
        )>,
    >,
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

    fn resolve<'a>(
        &'a self,
        parent: &'a AgentPath,
        name: &'a str,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<SpawnResolution>> {
        self.original.resolve(parent, name)
    }

    fn select<'a>(
        &'a self,
        operation: OperationId,
        references: &'a [String],
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Vec<(String, AgentPath)>>> {
        self.original.select(operation, references)
    }

    fn settled<'a>(
        &'a self,
        child: &'a AgentPath,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Settlement>> {
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
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<(String, TurnOutcome)>> {
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
    fn content_limits(&self) -> acyclic_harness_core::conversation::Limits {
        self.original.content_limits()
    }

    fn executor<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Arc<dyn Executor>>> {
        Box::pin(async move {
            *self.probe.journal.lock().unwrap() = Some((turn, authority.journal.clone()));
            // Exercise the direct Allow path, which has no approval callback.
            // All files, grants, journal, effects and child reads remain real.
            let authority = TurnAuthority {
                context: None,
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
    selected(children, None, None)
}

fn selected(children: &[&AgentPath], timeout_ms: Option<u64>, need: Option<u32>) -> ScriptedTurn {
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
    if let Some(need) = need {
        arguments["need"] = serde_json::json!(need);
    }
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
        ScriptedTurn::Text("none".into()),
        ScriptedTurn::Text("selected answer retained".into()),
        ScriptedTurn::Text("none".into()),
        wait(child),
        ScriptedTurn::Text("child releases deferred wait".into()),
        ScriptedTurn::Text("none".into()),
        ScriptedTurn::Text("deferred wait finished".into()),
        ScriptedTurn::Text("none".into()),
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
    let old = settled(stream, &within(), child).await?.answer.unwrap();
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
    pending_start(probe).await?;
    deliver(host, child, requested("advance-after-selection", child)).await?;
    let latest = settled(stream, &within(), child).await?.answer.unwrap();
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
    assert_eq!(value["reason"]["kind"], "need_met");
    assert_eq!(value["settled"][0]["answer"], "later child answer");
    assert_eq!(value["settled"][0]["agent"], child.as_str());
    assert_eq!(value["unsettled"], serde_json::json!([]));

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
    let (operation, journal, started) = pending_start(probe).await?;
    // A later delivery consumes the persisted condition and its actual producer.
    host.drain(child).await?;
    host.drain(parent).await?;
    assert_eq!(host.state(parent).await?.phase, Phase::Idle);
    assert_eq!(probe.reads.swap(0, Ordering::SeqCst), 1);
    let records = journal.replay(operation, 0, 100, None)?.await?;
    let starts = records
        .iter()
        .filter(|record| {
            matches!(
                record.event,
                acyclic_harness_core::executor::ExecutionEvent::ToolStarted { .. }
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(starts.len(), 1, "resuming a wait appended another start");
    assert_eq!(
        starts[0], &started,
        "resuming a wait replaced its start position or timestamp"
    );
    assert!(records.iter().any(|record| matches!(
        record.event,
        acyclic_harness_core::executor::ExecutionEvent::ToolCompleted { .. }
    )));
    assert_eq!(
        result(model)["reason"],
        serde_json::json!({"kind":"settlement", "child":child.as_str()})
    );
    assert_eq!(
        result(model)["settled"][0]["answer"],
        "child releases deferred wait"
    );

    deadlines(host, probe, model, parent, child, control).await?;
    admission(host, probe, model, parent, child).await?;
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

async fn pending_start(
    probe: &Probe,
) -> Result<(
    OperationId,
    Arc<dyn acyclic_harness_core::executor::ExecutionJournal>,
    acyclic_harness_core::executor::ExecutionRecord,
)> {
    use acyclic_harness_core::executor::ExecutionEvent;
    let (operation, journal) = probe
        .journal
        .lock()
        .unwrap()
        .clone()
        .ok_or("missing original journal")?;
    let records = journal.replay(operation, 0, 100, None)?.await?;
    assert!(
        !records.iter().any(|record| matches!(
            record.event,
            ExecutionEvent::ToolCompleted { .. } | ExecutionEvent::ToolFailed { .. }
        )),
        "parked wait already has a settlement"
    );
    let mut starts = records
        .into_iter()
        .filter(|record| matches!(record.event, ExecutionEvent::ToolStarted { .. }));
    let started = starts
        .next()
        .ok_or("wait parked without committing its start")?;
    assert!(starts.next().is_none(), "wait has duplicate starts");
    assert!(
        started.committed_at_micros.is_some(),
        "start has no physical commit timestamp"
    );
    Ok((operation, journal, started))
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
        selected(&[child], Some(10), None),
        selected(&[child], Some(20), None),
        ScriptedTurn::Text("both original deadlines completed".into()),
        ScriptedTurn::Text("none".into()),
        ScriptedTurn::Text("child releases deferred wait".into()),
        ScriptedTurn::Text("none".into()),
    ]);
    let first = deliver(host, parent, requested("independent-deadlines", parent)).await?;
    assert_eq!(
        first.phase,
        Phase::Waiting {
            deadline_unix_ms: Some(110),
            input: false,
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
            input: false,
        },
        "second wait reused the first deadline release"
    );
    assert_eq!(model.requests().len(), requests + 2);
    assert_eq!(result(model)["reason"]["kind"], "deadline");
    assert_eq!(
        result(model)["unsettled"],
        serde_json::json!([{ "name": child.as_str(), "phase": "running" }])
    );
    control.advance_to(129)?;
    host.drain(parent).await?;
    assert_eq!(model.requests().len(), requests + 2);
    control.advance_to(130)?;
    host.drain(parent).await?;
    assert_eq!(host.state(parent).await?.phase, Phase::Idle);
    assert_eq!(model.requests().len(), requests + 4);
    assert_eq!(result(model)["settled"], serde_json::json!([]));
    assert_eq!(result(model)["reason"]["kind"], "deadline");
    assert_eq!(
        result(model)["unsettled"],
        serde_json::json!([{ "name": child.as_str(), "phase": "running" }])
    );
    assert_eq!(probe.reads.swap(0, Ordering::SeqCst), 4);
    host.drain(child).await?;
    host.drain(parent).await?;
    Ok(())
}

async fn admission(
    host: &AgentHost<MemoryStream>,
    probe: &Probe,
    model: &ScriptedModelProvider,
    parent: &AgentPath,
    child: &AgentPath,
) -> Result {
    // A valid direct-child path is not evidence that an actor was created.
    let missing = parent.child(&id(251))?;
    assert!(matches!(
        host.state(&missing).await,
        Err(Error::NoSuchAgent)
    ));
    for (index, children) in [[child, &missing], [&missing, child]]
        .into_iter()
        .enumerate()
    {
        model.extend([
            selected(&children, None, Some(1)),
            ScriptedTurn::Text("invalid selection corrected".into()),
            ScriptedTurn::Text("none".into()),
        ]);
        probe.answers.lock().unwrap().clear();
        let before = model.requests().len();
        let key = format!("selected-child-admission-{index}");
        assert_eq!(
            deliver(host, parent, requested(&key, parent)).await?.phase,
            Phase::Idle,
        );
        assert_eq!(
            result(model),
            acyclic_harness_core::tools::rejected_input(acyclic_harness_core::Error::Invalid(
                format!("{} names no child of this agent", missing.as_str()),
            ))
            .value,
            "a settled sibling hid a nonexistent selected child",
        );
        assert!(
            probe.answers.lock().unwrap().is_empty(),
            "an invalid selection read a settled answer",
        );
        assert_eq!(model.requests().len(), before + 3);
        probe.reads.store(0, Ordering::SeqCst);
    }
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
        brief: Some(Box::new(answer(&**stream, parent, 0).await?.output)),
        profile: CreationConfig {
            question_plugin: false,
            topology: None,
            model: None,
            continuation: None,
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
    model.extend([
        ScriptedTurn::Text("parallel child answer".into()),
        ScriptedTurn::Text("none".into()),
    ]);
    host.drain(&other).await?;
    let selected = [
        settled(stream, &within(), child).await?.answer.unwrap(),
        settled(stream, &within(), &other).await?.answer.unwrap(),
    ];
    model.extend([
        wait_selected(&[child, &other]),
        ScriptedTurn::Text("parallel wait finished".into()),
        ScriptedTurn::Text("none".into()),
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
