//! Supervision is part of the original stock turn and its one terminal commit.
use super::*;
use acyclic_harness_core::{
    conversation::{ContentAccess, ContentChange, ContentPublisher},
    filesystem::{FilesystemContentPublisher, HOME_VOLUME, agent_authority},
    model::ModelEvent,
    supervision::{Action, Kind, Lifecycle, Policy, RunAdmission, streams::Queries},
    test_support::ScriptedEvent,
};
use futures::TryStreamExt as _;

struct RejectDuringSettlement<D> {
    driver: D,
    stream: Arc<MemoryStream>,
    once: std::sync::atomic::AtomicBool,
}

impl<D: TurnDriver> TurnDriver for RejectDuringSettlement<D> {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        cursor: u64,
        input: Option<Queued>,
        cancellation: acyclic_actors::guest::CancellationToken,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let step = self.driver.step(agent, cursor, input, cancellation).await?;
            if step.outs.iter().any(|out| matches!(out, Out::Commit(_)))
                && self.once.swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                let source =
                    acyclic_stream::StreamPath::new("control-input").map_err(Error::Stream)?;
                let event = acyclic_harness_core::supervision::Event {
                    version: 1,
                    agent: agent.path.clone(),
                    run: None,
                    source: acyclic_harness_core::supervision::Position {
                        stream: source.clone(),
                        sequence: 0,
                    },
                    event: Lifecycle::ControlRejected {
                        path: "control/steer.md".into(),
                    },
                };
                let request = acyclic_stream::CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: IdempotencyKey::unique(),
                    effects: vec![acyclic_stream::CommitEffect::hard(
                        vec![acyclic_stream::CommitCondition::Absent {
                            path: source.clone(),
                        }],
                        vec![
                            acyclic_stream::CommitMutation::Append {
                                path: source,
                                records: vec![b"rejected control".as_slice().into()],
                            },
                            acyclic_stream::CommitMutation::Append {
                                path: acyclic_harness_core::supervision::streams::path(&agent.path)
                                    .map_err(|_| Error::Record("event path"))?,
                                records: vec![
                                    serde_json::to_vec(&event)
                                        .map_err(|_| Error::Record("event encoding"))?
                                        .into(),
                                ],
                            },
                        ],
                    )],
                };
                assert!(matches!(
                    self.stream.commit(request).await.map_err(Error::Stream)?,
                    CommitOutcome::Committed(_)
                ));
            }
            Ok(step)
        })
    }
}

#[tokio::test]
async fn a_foreign_control_event_recomputes_settlement_without_redispatch_or_another_wake() -> Result
{
    let stream = Arc::new(MemoryStream::default());
    let path = AgentPath::root(&id(219));
    let operation = OperationId::from_bytes([219; 16]);
    let mut policy = Policy {
        no_progress_turns: Some(1),
        ..Default::default()
    };
    policy.actions.insert(Kind::NoProgress, Action::Pause);
    let (filesystem, volumes, issuer, publisher) =
        admitted_files(&stream, &path, operation, &policy).await?;
    let prompt = serde_json::to_vec(&ModelContent::Text("Finish this task".into()))?;
    let (body, _) = publisher
        .stage_once(
            OperationId::from_bytes([220; 16]),
            "input.json",
            &prompt,
            "application/json",
            "input.json",
            ContentAccess::default(),
        )?
        .await?;
    let model = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::Content {
            delta: "settled answer".into(),
        }),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: serde_json::json!({"usage":{"input_tokens":1,"output_tokens":1}}),
        }),
    ])]));
    let driver = RejectDuringSettlement {
        driver: ExecutorDriver::new(
            FilesystemTurnHost::new(
                stream.clone(),
                within(),
                filesystem.clone(),
                issuer,
                TurnAdmission::LocalRoot,
                executors(model.clone())?,
                LIMITS,
                Default::default(),
            )
            .with_actor_store(actor_store(&stream)?.root().clone()),
        ),
        stream: stream.clone(),
        once: std::sync::atomic::AtomicBool::new(true),
    };
    let host = committing(&stream, &filesystem, driver)?;
    let mut adoption = adopted(path.id());
    adoption.volumes = volumes;
    host.create(&intent("settlement-race"), adoption).await?;
    let sender = super::super::user();
    host.send(
        &path,
        &sender,
        &InboxRecord::Message(Message {
            from: sender.clone(),
            id: intent(&operation.to_string()),
            body: Box::new(body),
            re: None,
            attachments: None,
        }),
    )
    .await?;
    host.drain(&path).await?;
    assert_eq!(
        model.requests().len(),
        1,
        "foreign revalidation redispatched accepted work"
    );
    let state = host.state(&path).await?;
    assert_eq!(state.conversation_tail, 1, "completed reply was stranded");
    let store = actor_store(&stream)?;
    let snapshot = store.load(&host.actor(&path)?).await?;
    assert!(snapshot.state().ok_or("actor missing")?.paused());
    let events = stream
        .read(ReadRequest {
            path: acyclic_harness_core::supervision::streams::path(&path)?,
            key: None,
            from: 0,
            limit: 32,
            at_least: Default::default(),
        })
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let events = events
        .into_iter()
        .map(|record| {
            serde_json::from_slice::<acyclic_harness_core::supervision::Event>(&record.value)
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.event, Lifecycle::ControlRejected { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.event, Lifecycle::TurnDone { .. }))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.event, Lifecycle::Paused { .. }))
            .count(),
        1
    );
    Ok(())
}

async fn admitted_files(
    stream: &Arc<MemoryStream>,
    path: &AgentPath,
    operation: OperationId,
    policy: &Policy,
) -> Result<(
    Host,
    Vec<WorkspaceEntry>,
    acyclic_harness_core::core::RuntimeAuthority,
    Arc<dyn ContentPublisher>,
)> {
    let (filesystem, private) = private_in(stream, path).await?;
    let home = VolumeRef::new(
        private.volume()?.provider().clone(),
        "home".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes(
            path.id().to_bytes(),
        )),
        &mut None,
    )?;
    filesystem.create_volume(&home).await?;
    let volumes = vec![
        private,
        WorkspaceEntry::initial_volume(WorkspaceRole::Home, HOME_VOLUME.into(), home.clone()),
    ];
    let issuer = AuthorityRoot::new(
        "agents",
        [7; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "agents".into(),
        },
    )
    .into();
    let authority =
        agent_authority(&issuer, &TurnAdmission::LocalRoot, path, &volumes, &[]).await?;
    let publisher = FilesystemContentPublisher::new(
        filesystem.clone(),
        &home,
        &authority.verifier,
        &authority.scope,
        LIMITS.maximum_bytes,
        None,
    )?;
    let goal = b"Finish this task\n";
    let policy_bytes = serde_json::to_vec(&policy)?;
    let declaration = serde_json::to_vec(&RunAdmission::new(operation, goal, &policy_bytes))?;
    let declaration_path = format!("runs/{operation}.json");
    let changes = [
        ContentChange::CreateAbsent {
            path: "GOAL.md",
            bytes: goal,
            media_type: "text/markdown",
            display_name: "GOAL.md",
        },
        ContentChange::CreateAbsent {
            path: "control/supervision.json",
            bytes: &policy_bytes,
            media_type: "application/json",
            display_name: "supervision.json",
        },
        ContentChange::CreateAbsent {
            path: &declaration_path,
            bytes: &declaration,
            media_type: "application/json",
            display_name: "run.json",
        },
    ];
    publisher
        .stage_changes(&home, operation, &changes, ContentAccess::default())?
        .await?;
    Ok((filesystem, volumes, issuer, publisher))
}

#[tokio::test]
async fn spending_turn_settlement_commits_signal_and_terminal_action_once() -> Result {
    for action in [Action::Pause, Action::Stop] {
        for spending in [false, true] {
            let stream = Arc::new(MemoryStream::default());
            let path = super::super::create(stream.as_ref(), id(218)).await?;
            let operation = OperationId::from_bytes([218; 16]);
            let mut policy = Policy {
                no_progress_turns: Some(1),
                ..Default::default()
            };
            policy.actions.insert(Kind::NoProgress, action);
            let (filesystem, volumes, issuer, publisher) =
                admitted_files(&stream, &path, operation, &policy).await?;
            let (body, _) = publisher
                .stage_once(
                    OperationId::new(),
                    "input.txt",
                    b"Finish this task",
                    "text/plain",
                    "input.txt",
                    ContentAccess::default(),
                )?
                .await?;
            let model = Arc::new(ScriptedModelProvider::new([ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::Content {
                    delta: "finished response".into(),
                }),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: serde_json::json!({"usage":{"input_tokens":u64::from(spending),"output_tokens":0}}),
                }),
            ])]));
            let driver = ExecutorDriver::new(FilesystemTurnHost::new(
                stream.clone(),
                within(),
                filesystem,
                issuer,
                TurnAdmission::LocalRoot,
                executors(model.clone())?,
                LIMITS,
                Default::default(),
            ));
            let mut adoption = adopted(path.id());
            adoption.volumes = volumes;
            let mut state = AgentState::adopted(path.clone(), &adoption)?;
            let wake = || Wake::Inbox {
                sequence: 1,
                record: InboxRecord::Message(Message {
                    from: super::super::user(),
                    id: intent(&operation.to_string()),
                    body: Box::new(body.clone()),
                    re: None,
                    attachments: None,
                }),
            };
            let first = step(
                &driver,
                &path,
                Some(state.clone()),
                (2, 0),
                wake(),
                acyclic_actors::guest::CancellationToken::new(),
            )
            .await?;
            let run = first
                .outs
                .iter()
                .find_map(|out| match out {
                    Out::TurnDone { run, .. } => Some(*run),
                    _ => None,
                })
                .ok_or("turn did not settle")?;
            let queries = Queries::new(&*stream, &path, run);
            assert!(
                queries.read_signal(Kind::NoProgress).await?.is_none(),
                "an uncommitted terminal decision escaped"
            );
            let replay = step(
                &driver,
                &path,
                Some(state),
                (2, 0),
                wake(),
                acyclic_actors::guest::CancellationToken::new(),
            )
            .await?;
            assert_eq!(first.outs, replay.outs);
            assert_eq!(model.requests().len(), 1);
            let request = lower(&first, 2, delivery_key(&first.state, "settlement")?)?;
            assert!(matches!(
                stream.commit(request.clone()).await?,
                CommitOutcome::Committed(_)
            ));
            assert!(matches!(
                stream.commit(request).await?,
                CommitOutcome::Committed(_)
            ));
            let signal = queries.read_signal(Kind::NoProgress).await?;
            assert_eq!(signal.is_some(), spending);
            let records: Vec<_> = stream
                .read(ReadRequest {
                    path: acyclic_harness_core::supervision::streams::path(&path)?,
                    key: None,
                    from: 0,
                    limit: 32,
                    at_least: Default::default(),
                })
                .await?
                .try_collect()
                .await?;
            let events: Vec<acyclic_harness_core::supervision::Event> = records
                .iter()
                .map(|record| serde_json::from_slice(&record.value))
                .collect::<std::result::Result<_, _>>()?;
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(event.event, Lifecycle::TurnDone { .. }))
                    .count(),
                1
            );
            let terminal = events
                .iter()
                .filter(|event| {
                    matches!(
                        event.event,
                        Lifecycle::Paused { .. } | Lifecycle::Stopped { .. }
                    )
                })
                .count();
            assert_eq!(terminal, usize::from(spending));
            if spending {
                assert_eq!(
                    queries
                        .checkpoint()
                        .await?
                        .ok_or("missing checkpoint")?
                        .control()
                        .ok_or("missing control")?
                        .action,
                    action
                );
                let conversation = stream
                    .read(ReadRequest {
                        path: path.conversation()?,
                        key: None,
                        from: 0,
                        limit: 1,
                        at_least: Default::default(),
                    })
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?;
                let terminal_commit = conversation
                    .first()
                    .ok_or("missing terminal conversation")?
                    .commit_id;
                for (event, record) in events.iter().zip(&records) {
                    if matches!(
                        event.event,
                        Lifecycle::Signal { .. }
                            | Lifecycle::TurnDone { .. }
                            | Lifecycle::Paused { .. }
                            | Lifecycle::Stopped { .. }
                    ) {
                        assert_eq!(record.commit_id, terminal_commit);
                    }
                }
                assert_eq!(
                    matches!(first.state.phase, Phase::Done { .. }),
                    action == Action::Stop
                );
            }
        }
    }
    Ok(())
}
