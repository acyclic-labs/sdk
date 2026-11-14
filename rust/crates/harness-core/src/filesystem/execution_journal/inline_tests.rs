use super::*;
use crate::executor::ModelPurpose;
use crate::model::{Model, ModelMessage, ModelRequest, ModelRole, PreparedModelRequest};
use acyclic_stream::StreamProviderExt as _;

type Journal = FilesystemExecutionJournal<
    acyclic_stream::MemoryStream,
    acyclic_fs::MemoryAuthorityBackend,
    acyclic_fs::MemoryObjectBackend,
>;

async fn initialize(journal: &Journal, operation: OperationId) -> Result<()> {
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;
    let prepared = request()?;
    let reference =
        crate::test_support::stage_model_request(journal, operation, "request", &prepared).await?;
    journal
        .append(
            operation,
            "model:0:started",
            &ExecutionEvent::ModelStarted {
                step: 0,
                purpose: ModelPurpose::Response,
                request_digest: prepared.request_digest(),
                request: reference,
                context: None,
            },
            None,
        )?
        .await
}

fn request() -> Result<PreparedModelRequest> {
    PreparedModelRequest::prepare(
        ModelRequest {
            summary: false,
            compose_tools: false,
            model: Model::new("test".into(), "inline".into(), "1".into())?,
            messages: vec![ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text("input".into()),
            }],
            tools: Vec::new(),
            sampling: Default::default(),
        },
        Limits::default(),
        None,
    )
}

fn events() -> Vec<ModelEvent> {
    vec![
        ModelEvent::Content {
            delta: "hello π\n".into(),
        },
        ModelEvent::Reasoning {
            delta: "thought".into(),
        },
        ModelEvent::ReasoningBlock {
            block: crate::model::ReasoningBlock {
                kind: crate::model::ReasoningKind::Readable,
                text: Some("reason".into()),
                origin: crate::model::ReasoningOrigin {
                    provider: "test".into(),
                    model: "inline".into(),
                    credential_scope: None,
                },
                continuity: Vec::new(),
                outcome: crate::model::ReasoningOutcome::PreservedReadable,
            },
        },
        ModelEvent::ToolCall(crate::model::ToolCall::new(
            "call".into(),
            "example.echo".into(),
            serde_json::json!({"sparse": ["value"]}),
        )),
        ModelEvent::Content {
            delta: "large".repeat(256),
        },
        ModelEvent::Completed {
            metadata: serde_json::json!({"usage": {"input_tokens": 1}}),
        },
    ]
}

#[tokio::test]
async fn count_transport_traces_survive_reload_without_admitting_generation() -> Result<()> {
    use acyclic_stream::trace::{Mechanism, Outcome, SpanEvent, SpanId, TraceRecord};

    let (mut journal, _issuer) = test_support::journal().await?;
    let operation = OperationId::new();
    let id = SpanId {
        process: [2; 16],
        sequence: 0,
    };
    let traces = [
        ModelEvent::Trace {
            record: TraceRecord::V1(SpanEvent::Started {
                id,
                parent: None,
                name: "model.response_headers".into(),
                mechanism: Mechanism::RoundTrip,
                at_ns: 10,
                guarantee: Some("receive_response_headers".into()),
            }),
        },
        ModelEvent::Trace {
            record: TraceRecord::V1(SpanEvent::Finished {
                id,
                at_ns: 20,
                bytes: None,
                result: Outcome::Succeeded,
            }),
        },
    ];
    assert!(
        crate::executor::append_model_event(
            &journal,
            operation,
            "unbound",
            (0, ModelPurpose::Count),
            &traces[0],
            None,
            None,
        )
        .await
        .is_err(),
        "count transport requires an initial request binding"
    );
    journal
        .append(
            operation,
            "started",
            &ExecutionEvent::Started {
                request_digest: [1; 32],
            },
            None,
        )?
        .await?;

    for (index, trace) in traces.iter().enumerate() {
        // Drop the authenticated cache before both new appends and exact retries.
        // The next append must recover its state from the durable journal.
        for _ in 0..2 {
            *journal.verified.get_mut() = SummaryCache::default();
            crate::executor::append_model_event(
                &journal,
                operation,
                &format!("count:{index}"),
                (0, ModelPurpose::Count),
                trace,
                None,
                None,
            )
            .await?;
        }
    }
    for purpose in [
        ModelPurpose::Count,
        ModelPurpose::Response,
        ModelPurpose::Summary,
    ] {
        for invalid in [
            ModelEvent::Content {
                delta: "output".into(),
            },
            ModelEvent::Completed {
                metadata: serde_json::json!({}),
            },
        ] {
            assert!(
                crate::executor::append_model_event(
                    &journal,
                    operation,
                    "generation",
                    (0, purpose),
                    &invalid,
                    None,
                    None,
                )
                .await
                .is_err(),
                "an observation cannot invent a generation dispatch"
            );
        }
    }
    let records = journal.records(operation, 0, 64, None).await?;
    assert_eq!(records.len(), 3, "reload/retry duplicated transport work");
    for (record, expected) in records[1..].iter().zip(&traces) {
        let ExecutionEvent::Model {
            event,
            purpose: ModelPurpose::Count,
            usage: None,
            ..
        } = &record.event
        else {
            panic!("count trace lost its original private payload");
        };
        let bytes = journal.load(event, None)?.await?;
        assert_eq!(
            &crate::executor::decode_model_payload(event.descriptor().media_type(), &bytes, None,)?,
            expected
        );
    }
    // Counting never claims the response slot at this step.
    let prepared = request()?;
    let reference =
        crate::test_support::stage_model_request(&journal, operation, "request", &prepared).await?;
    journal
        .append(
            operation,
            "response",
            &ExecutionEvent::ModelStarted {
                step: 0,
                purpose: ModelPurpose::Response,
                request_digest: prepared.request_digest(),
                request: reference,
                context: None,
            },
            None,
        )?
        .await?;
    Ok(())
}

#[tokio::test]
async fn committed_archive_fences_dispatch_before_its_actor_consumes_the_control() -> Result<()> {
    use crate::agent::{ForkAdopted, IntentId, Sender, Submitted};
    use acyclic_actors::{
        addressing::ActorReference,
        local::{Creation, Store},
    };
    for (archive_depth, archive_first) in
        (0..4).flat_map(|depth| [false, true].map(move |first| (depth, first)))
    {
        let (journal, _issuer) = test_support::journal().await?;
        let operation = OperationId::new();
        let id = crate::agent::AgentId::from_bytes([81; 16]);
        let agent = crate::agent::AgentPath::root(&id);
        let adopted = ForkAdopted {
            agent: id,
            parent: None,
            profile: None,
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            volumes: Vec::new(),
            brief: None,
            policy: None,
        };
        let creation = crate::agent::create_root(
            &IntentId::try_from("create").map_err(|error| Error::Invalid(error.to_string()))?,
            adopted.clone(),
        )
        .map_err(|error| Error::Invalid(error.to_string()))?
        .request;
        assert!(matches!(
            crate::agent::submit(journal.stream.as_ref(), creation)
                .await
                .map_err(|error| Error::Invalid(error.to_string()))?,
            Submitted::Committed(_)
        ));
        let mut ancestors = vec![agent];
        for intent in ["parent", "leaf"] {
            let parent = ancestors.last().expect("original root");
            let mut spawn = crate::agent::spawn(
                parent,
                &IntentId::try_from(intent).map_err(|error| Error::Invalid(error.to_string()))?,
                journal
                    .stream
                    .tail(
                        parent
                            .inbox()
                            .map_err(|error| Error::Invalid(error.to_string()))?,
                    )
                    .await?,
                adopted.clone(),
            )
            .map_err(|error| Error::Invalid(error.to_string()))?;
            if intent == "leaf" {
                let actor = ActorReference::new(
                    b"account",
                    b"agents",
                    spawn.child.id().as_str().as_bytes(),
                )
                .map_err(|error| Error::Invalid(error.to_string()))?;
                let store = Store::new(
                    journal.stream.clone(),
                    acyclic_stream::StreamPath::new("archive-test/actors")?,
                );
                spawn.request.effects.push(
                    store
                        .create(None, &actor, Creation::default())
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                );
            }
            assert!(matches!(
                crate::agent::submit(journal.stream.as_ref(), spawn.request)
                    .await
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                Submitted::Committed(_)
            ));
            ancestors.push(spawn.child);
        }
        // Archiving a sibling must leave this branch's paid admission open.
        let sibling = crate::agent::spawn(
            &ancestors[0],
            &IntentId::try_from("sibling").map_err(|error| Error::Invalid(error.to_string()))?,
            1,
            adopted,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(matches!(
            crate::agent::submit(journal.stream.as_ref(), sibling.request)
                .await
                .map_err(|error| Error::Invalid(error.to_string()))?,
            Submitted::Committed(_)
        ));
        let stops_dispatch = archive_depth < ancestors.len();
        let target = if stops_dispatch {
            ancestors[archive_depth].clone()
        } else {
            sibling.child
        };
        let closed = target
            .closed()
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let agent = ancestors.last().expect("original leaf").clone();
        let actor = ActorReference::new(b"account", b"agents", agent.id().as_str().as_bytes())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let actor_path = Store::new(
            journal.stream.clone(),
            acyclic_stream::StreamPath::new("archive-test/actors")?,
        )
        .path(&actor)
        .map_err(|error| Error::Invalid(error.to_string()))?;
        let journal = journal
            .with_agent_events(agent.clone(), operation, 0, true)
            .with_actor_admissions(vec![actor_path.clone()]);
        journal
            .append(
                operation,
                "started",
                &ExecutionEvent::Started {
                    request_digest: [1; 32],
                },
                None,
            )?
            .await?;
        let prepared = request()?;
        let request =
            crate::test_support::stage_model_request(&journal, operation, "request", &prepared)
                .await?;
        let dispatch = ExecutionEvent::ModelStarted {
            step: 0,
            purpose: ModelPurpose::Response,
            request_digest: prepared.request_digest(),
            request: request.clone(),
            context: None,
        };
        let archive = async || -> Result<()> {
            let commit = crate::agent::archive(
                &target,
                &Sender::User {
                    principal: "owner".into(),
                },
                &IntentId::try_from("archive")
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            )
            .map_err(|error| Error::Invalid(error.to_string()))?;
            assert!(matches!(
                crate::agent::submit(journal.stream.as_ref(), commit)
                    .await
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                Submitted::Committed(_)
            ));
            Ok(())
        };
        if archive_first {
            archive().await?;
        }
        let first = journal
            .append(operation, "model:0:started", &dispatch, None)?
            .await;
        if archive_first && stops_dispatch {
            assert!(
                matches!(first, Err(Error::ExecutionControlled(control))
                if control.action == crate::supervision::Action::Stop && control.source.stream == closed),
                "ancestor archive admitted a fresh model dispatch"
            );
        } else {
            first?;
            if !archive_first {
                archive().await?;
            }
            // An accepted dispatch can replay after closure without new work.
            journal
                .append(operation, "model:0:started", &dispatch, None)?
                .await?;
        }
        // The actor has not consumed even its adoption. Archive must fence paid
        // admission without depending on a later runtime delivery/checkpoint.
        assert_eq!(journal.stream.tail(actor_path).await?, 1);
        let tool = ExecutionEvent::ToolStarted {
            step: 0,
            call_id: "after-archive".into(),
            invocation: request,
            publication: None,
        };
        let tool_result = journal
            .append(operation, "tool:after-archive", &tool, None)?
            .await;
        if stops_dispatch {
            assert!(
                matches!(tool_result, Err(Error::ExecutionControlled(control))
                if control.action == crate::supervision::Action::Stop && control.source.stream == closed),
                "ancestor archive admitted a fresh tool dispatch"
            );
        } else {
            tool_result?;
        }
        assert_eq!(
            journal.records(operation, 0, 8, None).await?.len(),
            1 + usize::from(!archive_first || !stops_dispatch) + usize::from(!stops_dispatch)
        );
    }
    Ok(())
}

#[tokio::test]
async fn ancestor_pause_fences_original_dispatch_and_resume_without_changing_child_admission()
-> Result<()> {
    use acyclic_actors::{
        addressing::ActorReference,
        local::{Creation, Store},
    };
    use acyclic_stream::{
        CommitEffect, CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey,
    };

    for (depth, pause_first) in
        (0..4).flat_map(|depth| [false, true].map(move |first| (depth, first)))
    {
        let (journal, _issuer) = test_support::journal().await?;
        let root = crate::agent::AgentPath::root(&crate::agent::AgentId::from_bytes([182; 16]));
        let parent = root.child(&crate::agent::AgentId::from_bytes([183; 16]))?;
        let leaf = parent.child(&crate::agent::AgentId::from_bytes([184; 16]))?;
        let sibling = root.child(&crate::agent::AgentId::from_bytes([185; 16]))?;
        let store = Store::new(
            journal.stream.clone(),
            acyclic_stream::StreamPath::new("pause-test/actors")?,
        );
        let mut actors = Vec::new();
        let mut paths = Vec::new();
        let mut effects = Vec::new();
        for agent in [&root, &parent, &leaf, &sibling] {
            let actor = ActorReference::new(b"account", b"agents", agent.id().as_str().as_bytes())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            paths.push(
                store
                    .path(&actor)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            );
            effects.push(
                store
                    .create(None, &actor, Creation::default())
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            );
            actors.push(actor);
        }
        assert!(matches!(
            journal
                .stream
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    effects,
                    idempotency_key: IdempotencyKey::unique(),
                })
                .await?,
            CommitOutcome::Committed(_)
        ));
        let operation = OperationId::new();
        let journal = journal
            .with_agent_events(leaf.clone(), operation, 0, true)
            .with_actor_admissions(paths[..3].to_vec());
        journal
            .append(
                operation,
                "started",
                &ExecutionEvent::Started {
                    request_digest: [1; 32],
                },
                None,
            )?
            .await?;
        let prepared = request()?;
        let request =
            crate::test_support::stage_model_request(&journal, operation, "request", &prepared)
                .await?;
        let model = ExecutionEvent::ModelStarted {
            step: 0,
            purpose: ModelPurpose::Response,
            request_digest: prepared.request_digest(),
            request: request.clone(),
            context: None,
        };
        if !pause_first {
            journal.append(operation, "model", &model, None)?.await?;
        }

        // Capture the exact producer fence while every ancestor is open. A
        // later pause must refuse this dispatch in the same atomic commit.
        let paid = acyclic_stream::StreamPath::new("pause-test/stale-paid-dispatch")?;
        let mut stale = CommitEffect::hard(
            vec![acyclic_stream::CommitCondition::Absent { path: paid.clone() }],
            vec![CommitMutation::Append {
                path: paid.clone(),
                records: vec![b"provider intent".as_slice().into()],
            }],
        );
        journal.fence_dispatch(&mut stale, None).await?;
        let previous = store
            .load(&actors[depth])
            .await
            .map_err(|error| Error::Invalid(error.to_string()))?;
        store
            .set_paused(&previous, 0, true, IdempotencyKey::unique())
            .await
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let stopped = depth < 3;
        let stale_result = journal
            .stream
            .commit(CommitRequest {
                deadline_unix_millis: None,
                effects: vec![stale],
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        assert_eq!(
            matches!(stale_result, CommitOutcome::Conflict(_)),
            stopped,
            "ancestor pause did not fence a captured dispatch (depth={depth})"
        );
        assert_eq!(journal.stream.tail(paid).await.is_ok(), !stopped);

        let model_result = journal.append(operation, "model", &model, None)?.await;
        if stopped && pause_first {
            assert!(
                matches!(model_result, Err(Error::ExecutionControlled(control))
                if control.action == crate::supervision::Action::Pause && control.source.stream == paths[depth]),
                "ancestor pause admitted a fresh model dispatch"
            );
        } else {
            // This includes exact replay of a dispatch accepted before pause.
            model_result?;
        }
        let tool = ExecutionEvent::ToolStarted {
            step: 0,
            call_id: "paused-tool".into(),
            invocation: request,
            publication: None,
        };
        let tool_result = journal.append(operation, "tool", &tool, None)?.await;
        if stopped {
            assert!(
                matches!(tool_result, Err(Error::ExecutionControlled(control))
                if control.action == crate::supervision::Action::Pause && control.source.stream == paths[depth]),
                "ancestor pause admitted a fresh tool dispatch"
            );
        } else {
            tool_result?;
        }
        if depth < 2 {
            let child = store
                .load(&actors[2])
                .await
                .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(
                child.state().map(|state| state.paused()),
                Some(false),
                "ancestor pause rewrote child admission"
            );
        }
        let previous = store
            .load(&actors[depth])
            .await
            .map_err(|error| Error::Invalid(error.to_string()))?;
        store
            .set_paused(&previous, 0, false, IdempotencyKey::unique())
            .await
            .map_err(|error| Error::Invalid(error.to_string()))?;
        journal.append(operation, "model", &model, None)?.await?;
        journal.append(operation, "tool", &tool, None)?.await?;
        assert_eq!(
            journal.records(operation, 0, 8, None).await?.len(),
            3,
            "resume or accepted replay duplicated a paid intent"
        );
    }
    Ok(())
}

#[tokio::test]
async fn resumed_exhausted_reservation_refuses_new_dispatch_but_keeps_original_replay() -> Result<()>
{
    use crate::supervision::{Action, Kind, Policy, Reservation, RunAdmission, RunAdmissionSource};
    let (journal, _issuer) = test_support::journal().await?;
    let operation = OperationId::new();
    let agent = crate::agent::AgentPath::root(&crate::agent::AgentId::from_bytes([70; 16]));
    let policy = Policy {
        no_progress_turns: Some(10),
        reservation: Reservation {
            tokens: Some(2),
            usd_micros: None,
        },
        ..Default::default()
    };
    let goal_bytes = b"Finish the task";
    let policy_bytes = crate::contract::canonical_json_bytes(&policy)?;
    let declaration = RunAdmission::new(operation, goal_bytes, &policy_bytes);
    let goal = journal
        .stage(operation, "goal", goal_bytes, "text/plain", None)?
        .await?;
    let policy_file =
        crate::executor::stage_json(&journal, operation, "policy", &policy, None).await?;
    let admission =
        crate::executor::stage_json(&journal, operation, "admission", &declaration, None).await?;
    let journal = journal
        .with_run_admission(Some(RunAdmissionSource {
            admission,
            goal,
            policy: policy_file,
            value: declaration.verify(operation, goal_bytes, &policy_bytes)?,
        }))
        .with_agent_events(agent.clone(), operation, 0, true);
    initialize(&journal, operation).await?;
    let completed = ModelEvent::Completed {
        metadata: serde_json::json!({"usage":{"input_tokens":1,"output_tokens":1}}),
    };
    crate::executor::append_model_event(
        &journal,
        operation,
        "terminal",
        (0, ModelPurpose::Response),
        &completed,
        None,
        None,
    )
    .await?;
    let queries =
        crate::supervision::streams::Queries::new(journal.stream.as_ref(), &agent, operation);
    let (spent, fence) = queries.spent_in_turn(0).await?;
    assert!(spent, "original usage did not publish its turn fact");
    assert!(!queries.spent_in_turn(1).await?.0);
    let marker = journal
        .stream
        .latest_fenced(fence.path().clone(), None, Default::default())
        .await?
        .0
        .ok_or_else(|| Error::Invalid("turn usage marker absent".into()))?;
    let mut original = journal
        .stream
        .read(acyclic_stream::ReadRequest {
            path: crate::distributed::execution_path(operation, &mut None)?,
            key: None,
            from: 2,
            limit: 1,
            at_least: Default::default(),
        })
        .await?;
    let original = futures::StreamExt::next(&mut original)
        .await
        .transpose()?
        .ok_or_else(|| Error::Invalid("original model terminal absent".into()))?;
    assert_eq!(marker.commit_id, original.commit_id);
    assert_eq!(
        queries
            .admission(&policy)
            .await?
            .control
            .map(|control| control.signal),
        Some(Some(Kind::Spend))
    );
    let prepared = request()?;
    let request_file = crate::executor::stage_json(
        &journal,
        operation,
        "next-request",
        prepared.request(),
        None,
    )
    .await?;
    let before = journal.records(operation, 0, 16, None).await?;
    for attempt in 0..2 {
        let owner = crate::supervision::Position {
            stream: acyclic_stream::StreamPath::new(format!("sources/resume-{attempt}"))?,
            sequence: 0,
        };
        if let Some(resume) = queries.resume(&owner).await? {
            let outcome = journal
                .stream
                .commit(acyclic_stream::CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: acyclic_stream::IdempotencyKey::unique(),
                    effects: vec![
                        acyclic_stream::CommitEffect::hard(
                            vec![acyclic_stream::CommitCondition::Absent {
                                path: owner.stream.clone(),
                            }],
                            vec![acyclic_stream::CommitMutation::Append {
                                path: owner.stream,
                                records: vec![b"owner resume".as_slice().into()],
                            }],
                        ),
                        resume,
                    ],
                })
                .await?;
            assert!(matches!(
                outcome,
                acyclic_stream::CommitOutcome::Committed(_)
            ));
        }
        // Accepted original intents/usage remain recoverable after resume.
        initialize(&journal, operation).await?;
        crate::executor::append_model_event(
            &journal,
            operation,
            "terminal",
            (0, ModelPurpose::Response),
            &completed,
            None,
            None,
        )
        .await?;
        let denied = journal
            .append(
                operation,
                &format!("next-dispatch-{attempt}"),
                &ExecutionEvent::ModelStarted {
                    step: 1,
                    purpose: ModelPurpose::Response,
                    request_digest: prepared.request_digest(),
                    request: request_file.clone(),
                    context: prepared.context_window(),
                },
                None,
            )?
            .await;
        assert!(matches!(denied, Err(Error::ExecutionControlled(control))
            if control.action == Action::Pause && control.signal == Some(Kind::Spend)));
        assert_eq!(journal.records(operation, 0, 16, None).await?, before);
        assert_eq!(
            queries
                .checkpoint()
                .await?
                .expect("retained usage")
                .state
                .tokens
                .measured,
            2
        );
    }
    Ok(())
}

#[cfg(feature = "research")]
#[tokio::test]
async fn append_receipts_preserve_original_sequence_after_later_publication() -> Result<()> {
    let (journal, _issuer) = test_support::journal().await?;
    let operation = OperationId::new();
    initialize(&journal, operation).await?;
    let event = ModelEvent::Completed {
        metadata: serde_json::Value::Null,
    };
    let terminal = crate::executor::append_model_event(
        &journal,
        operation,
        "terminal",
        (0, ModelPurpose::Response),
        &event,
        None,
        None,
    )
    .await?;
    let terminal_cut = terminal.journal_cut.expect("original terminal receipt");
    assert_eq!(terminal_cut.operation, operation);
    assert_eq!(terminal_cut.sequence, 3);
    let started = ExecutionEvent::Started {
        request_digest: [1; 32],
    };
    let sequenced = journal
        .sequenced_appends()
        .expect("original receipt capability");
    let original = sequenced
        .append_with_sequence(operation, "started", &started, None)?
        .await?;
    assert_eq!(original, 1);
    assert_ne!(
        original, terminal_cut.sequence,
        "current tail would certify a different observation"
    );
    let foreign = ExecutionEvent::Started {
        request_digest: [2; 32],
    };
    assert!(
        sequenced
            .append_with_sequence(operation, "started", &foreign, None)?
            .await
            .is_err()
    );
    assert_eq!(
        crate::executor::append_model_event(
            &journal,
            operation,
            "terminal",
            (0, ModelPurpose::Response),
            &event,
            None,
            None,
        )
        .await?
        .journal_cut,
        Some(terminal_cut)
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn private_payload_ranges_preserve_original_authority_and_bounded_residency() -> Result<()> {
    let (journal, _issuer) = test_support::journal().await?;
    let operation = OperationId::new();
    initialize(&journal, operation).await?;
    let payload = "x".repeat(4 * 1024 * 1024);
    let content = ModelEvent::Content { delta: payload };
    crate::executor::append_model_event(
        &journal,
        operation,
        &format!("model-live:{operation}:model:0:0"),
        (0, ModelPurpose::Response),
        &content,
        None,
        None,
    )
    .await?;
    let ModelEvent::Content { delta: payload } = content else {
        unreachable!()
    };
    let records = journal.records(operation, 0, 8, None).await?;
    let Some(ExecutionRecord {
        event: ExecutionEvent::Model { event: file, .. },
        ..
    }) = records.last()
    else {
        return Err(Error::Storage(
            "original model observation is absent".into(),
        ));
    };
    let file = file.clone();
    assert!(
        file.path()
            .starts_with(InternalContentClass::ModelText.prefix())
    );
    let stop = acyclic_fs::CancellationToken::new();
    let range = acyclic_fs::ByteRange {
        offset: 65530,
        length: 16,
    };
    let work = acyclic_fs::NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &stop)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let read = journal.load_range(&file, range, Some(&io))?;
    let setup_bytes = work.live_allocation_bytes();
    let bytes = read.await?;
    assert_eq!(bytes.as_ref(), b"xxxxxxxxxxxxxxxx");
    {
        // Fund the actual ledger and read frame, then leave less credit than
        // the output requires, independently of provider chunks or caching.
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget {
                peak_allocation_bytes: setup_bytes + range.length - 1,
                ..acyclic_fs::WorkBudget::UNBOUNDED
            },
            &stop,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let error = journal
            .load_range(&file, range, Some(&io))?
            .await
            .unwrap_err();
        let cause = match &error {
            Error::NativeWorkspace { source, .. } => match source.as_ref() {
                acyclic_fs::WorkspaceError::Work(cause) => Some(cause),
                _ => None,
            },
            Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(cause),
                ..
            } => Some(cause),
            _ => None,
        };
        assert!(
            matches!(
                cause,
                Some(acyclic_fs::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                })
            ),
            "{error:?}"
        );
    }
    let length = file.descriptor().byte_length();
    let receipt = work.receipt();
    // Authenticate the whole intersecting chunk even when storage lends its
    // backing. Reading the whole payload then slicing must also fail this
    // work bound when shared aliases do not allocate new chunk backing.
    assert!(receipt.bytes_hashed >= 1024 * 1024);
    assert!(receipt.object_bytes_read < length);
    assert!(receipt.peak_allocation_bytes < length);
    assert!(
        journal
            .load_range(
                &file,
                acyclic_fs::ByteRange {
                    offset: length,
                    length: 0
                },
                None
            )?
            .await?
            .is_empty()
    );
    for range in [
        acyclic_fs::ByteRange {
            offset: length,
            length: 1,
        },
        acyclic_fs::ByteRange {
            offset: u64::MAX,
            length: 1,
        },
    ] {
        assert!(journal.load_range(&file, range, None)?.await.is_err());
    }
    let foreign = FileRef::new(
        VolumeRef::new(
            journal.volume.provider().clone(),
            "foreign".into(),
            VolumeClass::AgentPrivate,
            crate::conversation::VolumeOwner::Agent(crate::AgentId::from_bytes([81; 16])),
            &mut None,
        )?,
        file.path().into(),
        file.version().into(),
        file.descriptor().clone(),
        file.display_name().into(),
        &mut None,
    )?;
    assert_eq!(
        journal
            .load_range(&foreign, range, None)?
            .await
            .unwrap_err()
            .code(),
        crate::wire::ErrorCode::Unauthorized
    );
    let mut digest = *file.descriptor().sha256();
    digest[0] ^= 1;
    let forged = FileRef::new(
        file.volume().clone(),
        file.path().into(),
        file.version().into(),
        crate::conversation::FileDescriptor::new(
            digest,
            length,
            file.descriptor().media_type().into(),
        )?,
        file.display_name().into(),
        &mut None,
    )?;
    assert!(journal.load_range(&forged, range, None)?.await.is_err());
    assert_eq!(
        journal.load_range(&file, range, None)?.await?.as_ref(),
        bytes.as_ref()
    );
    let reads = io.receipt().backend_read_operations;
    stop.cancel();
    let cancelled = match journal.load_range(&file, range, Some(&io)) {
        Ok(read) => read.await,
        Err(error) => Err(error),
    };
    assert_eq!(
        cancelled.unwrap_err().code(),
        crate::wire::ErrorCode::Cancelled
    );
    assert_eq!(io.receipt().backend_read_operations, reads);
    // Capture the real journal's committed frames, then omit their original
    // atomic proof publications. Reference replay must refuse rather than
    // reconstruct payload bodies to accept an unproven observation.
    use futures::TryStreamExt as _;
    let path = crate::distributed::execution_path(operation, &mut None)?;
    let mut captured = journal
        .stream
        .read(acyclic_stream::ReadRequest {
            key: None,
            path,
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
        .await?;
    let source = captured
        .try_next()
        .await?
        .ok_or_else(|| Error::Storage("original journal frame is absent".into()))?;
    let isolated = Arc::new(acyclic_stream::MemoryStream::default());
    let unproven = FilesystemExecutionJournal::new(
        isolated,
        journal.host.clone(),
        crate::filesystem::FilesystemJournalBinding {
            volume: &journal.volume,
            verifier: &journal.verifier,
            scope: &journal.scope,
            io: None,
        },
    )?;
    unproven
        .path(operation)?
        .append_batch(
            vec![source.value.into()],
            Some(0),
            crate::test_support::fresh_key()?,
        )
        .await?;
    assert!(matches!(unproven.replay(operation, 0, 1, None)?.await,
        Err(Error::Storage(reason)) if reason.contains("original publication proof")));

    // The host's immutable request owner resolves its original publication,
    // rather than reading a whole request or delegating its internal path.
    let write = ContentGrant::verify(
        &journal.verifier,
        &journal.scope,
        &journal.volume,
        VolumeOperation::Write,
        &mut None,
    )?;
    let read = ContentGrant::verify(
        &journal.verifier,
        &journal.scope,
        &journal.volume,
        VolumeOperation::Read,
        &mut None,
    )?;
    let path = ".system/interactions/approvals/approval-test/request.json";
    let original = journal
        .host
        .put_internal_content(
            &journal.volume,
            &write,
            path,
            payload.as_bytes(),
            "text/plain",
            "request.json",
            Some(u64::MAX),
            &crate::IdempotencyKey("internal-file:approval-test".into()),
            crate::filesystem::InternalContentClass::Interaction,
            None,
        )
        .await?;
    for offset in [range.offset, payload.len() as u64] {
        let (selected, bytes) = journal
            .host
            .read_internal_range(
                &journal.volume,
                &read,
                path,
                crate::filesystem::InternalContentClass::Interaction,
                acyclic_fs::ByteRange { offset, length: 16 },
                None,
            )
            .await?;
        assert_eq!(selected, original);
        assert_eq!(
            bytes.as_ref(),
            &payload.as_bytes()[offset as usize..][..bytes.len()]
        );
        assert_eq!(
            bytes.len(),
            if offset == payload.len() as u64 {
                0
            } else {
                16
            }
        );
    }
    for (class, offset) in [
        (crate::filesystem::InternalContentClass::Execution, 0),
        (
            crate::filesystem::InternalContentClass::Interaction,
            payload.len() as u64 + 1,
        ),
    ] {
        assert!(
            journal
                .host
                .read_internal_range(
                    &journal.volume,
                    &read,
                    path,
                    class,
                    acyclic_fs::ByteRange { offset, length: 16 },
                    None,
                )
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn terminal_usage_keeps_one_original_commit_and_refuses_foreign_or_partial_records()
-> Result<()> {
    for (metadata, complete_input, expected) in [
        (
            serde_json::json!({"usage":{"input_tokens":5,"cache_creation_input_tokens":7,"cache_read_input_tokens":11,"output_tokens":2}}),
            Some(23),
            Some(25),
        ),
        (
            serde_json::json!({"usage":{"prompt_tokens":23,"prompt_tokens_details":{"cached_tokens":18},"completion_tokens":2}}),
            Some(23),
            Some(25),
        ),
        (
            serde_json::json!({"usage":{"input_tokens":0,"cache_read_input_tokens":11,"output_tokens":0}}),
            Some(11),
            Some(11),
        ),
        (
            serde_json::json!({"usage":{"input_tokens":5,"output_tokens":2}}),
            None,
            None,
        ),
        (
            serde_json::json!({"usage":{"input_tokens":5,"output_tokens":2}}),
            Some(u64::MAX),
            None,
        ),
    ] {
        terminal_usage_case(metadata, complete_input, expected).await?;
    }
    Ok(())
}

async fn terminal_usage_case(
    metadata: serde_json::Value,
    complete_input_tokens: Option<u64>,
    expected_tokens: Option<u64>,
) -> Result<()> {
    let (journal, _issuer) = test_support::journal().await?;
    let operation = OperationId::new();
    let agent = crate::agent::AgentPath::root(&crate::agent::AgentId::from_bytes([69; 16]));
    let journal = journal.with_agent_events(agent.clone(), operation, 0, false);
    initialize(&journal, operation).await?;
    let completed = ModelEvent::Completed { metadata };
    let mut usage = crate::executor::model_usage::ModelCallUsage {
        version: crate::executor::ModelUsageRecord::VERSION,
        completed: true,
        dispatch: crate::model::ModelDispatch {
            operation_id: operation,
            step: 0,
            request_digest: request()?.request_digest(),
        },
        purpose: ModelPurpose::Response,
        provider: "test",
        model: "inline",
        agent_path: None,
        sampling: Default::default(),
        provider_request_id: None,
        #[cfg(feature = "research")]
        system_fingerprint: None,
        ttft_ns: None,
        #[cfg(feature = "research")]
        cache: None,
        abandoned: Default::default(),
        tokens: crate::executor::ModelTokenUsage::reported(match &completed {
            ModelEvent::Completed { metadata } => metadata,
            _ => unreachable!(),
        }),
        complete_input_tokens,
        started_at: None,
        completed_at: None,
        latency_ns: Some(123),
    };
    let partial = ModelEvent::Content {
        delta: "partial".into(),
    };
    // A staged usage file alone cannot settle or add a journal observation.
    assert!(
        crate::executor::append_model_event(
            &journal,
            operation,
            "partial",
            (0, ModelPurpose::Response),
            &partial,
            Some(&usage),
            None
        )
        .await
        .is_err()
    );
    assert_eq!(journal.records(operation, 0, 64, None).await?.len(), 2);
    let before = crate::executor::usage_page(&journal, operation, "agents/usage", 0).await?;
    assert_eq!(
        before.items.len(),
        1,
        "uncommitted usage must stay invisible"
    );
    assert!(!before.items[0].completed);
    assert_eq!(
        before.items[0].tokens,
        crate::executor::ModelTokenUsage::default()
    );
    assert_eq!(before.items[0].latency_ns, None);
    assert_eq!(before.end, 2);
    assert_eq!(before.next, None);
    let quiet =
        crate::executor::usage_page(&journal, operation, "agents/usage", before.end).await?;
    assert!(quiet.items.is_empty());
    assert_eq!(quiet.end, before.end);
    assert_eq!(quiet.next, None);
    let change = async {
        let updates = journal
            .stream
            .follow(journal.follow(operation, quiet.end)?)
            .await?;
        let record = futures::StreamExt::next(&mut acyclic_stream::records_only(updates)).await;
        assert!(matches!(record, Some(Ok(_))));
        Ok::<(), Error>(())
    };
    let mut changed = std::pin::pin!(change);
    assert!(
        futures::poll!(&mut changed).is_pending(),
        "the original producer is quiet"
    );
    usage.dispatch.operation_id = OperationId::new();
    assert!(
        crate::executor::append_model_event(
            &journal,
            operation,
            "foreign",
            (0, ModelPurpose::Response),
            &completed,
            Some(&usage),
            None
        )
        .await
        .is_err()
    );
    usage.dispatch.operation_id = operation;
    let original_input = usage.tokens.input;
    usage.tokens.input = Some(21);
    assert!(
        crate::executor::append_model_event(
            &journal,
            operation,
            "altered",
            (0, ModelPurpose::Response),
            &completed,
            Some(&usage),
            None
        )
        .await
        .is_err()
    );
    assert_eq!(journal.records(operation, 0, 64, None).await?.len(), 2);
    usage.tokens.input = original_input;
    let mut altered: crate::executor::ModelUsageRecord = serde_json::from_value(
        serde_json::to_value(&usage).map_err(|error| Error::Invalid(error.to_string()))?,
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    for version in [0, 1, 2, crate::executor::ModelUsageRecord::VERSION + 1] {
        altered.version = version;
        assert!(
            crate::executor::validate_model_usage(
                &altered,
                operation,
                0,
                ModelPurpose::Response,
                &completed,
                None
            )
            .is_err()
        );
    }
    altered.version = crate::executor::ModelUsageRecord::VERSION;
    altered.ttft_ns = Some(124);
    assert!(
        crate::executor::validate_model_usage(
            &altered,
            operation,
            0,
            ModelPurpose::Response,
            &completed,
            None
        )
        .is_err()
    );
    altered.ttft_ns = None;
    altered.provider_request_id = Some("generated-unreported-identity".into());
    assert!(
        crate::executor::validate_model_usage(
            &altered,
            operation,
            0,
            ModelPurpose::Response,
            &completed,
            None
        )
        .is_err()
    );
    for _ in 0..2 {
        crate::executor::append_model_event(
            &journal,
            operation,
            "terminal",
            (0, ModelPurpose::Response),
            &completed,
            Some(&usage),
            None,
        )
        .await?;
    }
    changed.await?;
    // Registration after the publication still starts at the same exclusive
    // cut, rather than resolving a new head and losing this terminal record.
    let updates = journal
        .stream
        .follow(journal.follow(operation, quiet.end)?)
        .await?;
    assert!(matches!(
        futures::StreamExt::next(&mut acyclic_stream::records_only(updates)).await,
        Some(Ok(_))
    ));
    let records = journal.records(operation, 0, 64, None).await?;
    assert_eq!(records.len(), 3);
    let queries = crate::supervision::streams::Queries::new(&*journal.stream, &agent, operation);
    let cut = queries
        .checkpoint()
        .await?
        .ok_or_else(|| Error::Invalid("usage query cut absent".into()))?;
    assert_eq!(cut.state.through, 1, "exact terminal replay charged twice");
    assert_eq!(cut.state.tokens.measured, expected_tokens.unwrap_or(0));
    assert_eq!(cut.state.tokens.complete, expected_tokens.is_some());
    assert_eq!(cut.action, crate::supervision::Action::Alert);
    assert!(
        queries
            .read_signal(crate::supervision::Kind::Spend)
            .await?
            .is_none()
    );
    assert_eq!(cut.source.sequence, 2);
    let query_record = journal
        .stream
        .latest(
            acyclic_stream::StreamPath::new(format!(
                "{agent}/supervision/runs/{operation}/checkpoint"
            ))?,
            acyclic_stream::OwnedBytes::from_static(b"state"),
            Default::default(),
        )
        .await?
        .ok_or_else(|| Error::Invalid("indexed query record absent".into()))?;
    let mut source = journal
        .stream
        .read(acyclic_stream::ReadRequest {
            key: None,
            path: cut.source.stream,
            from: cut.source.sequence,
            limit: 1,
            at_least: Default::default(),
        })
        .await?;
    let source = futures::StreamExt::next(&mut source)
        .await
        .transpose()?
        .ok_or_else(|| Error::Invalid("query source absent".into()))?;
    assert_eq!(query_record.commit_id, source.commit_id);
    let exported = crate::executor::event_page(&journal, operation, 0).await?;
    assert_eq!(exported.operation, Some(operation));
    assert_eq!(exported.items, records);
    let resumed_events = crate::executor::event_page(&journal, operation, quiet.end).await?;
    assert_eq!(resumed_events.items, records[2..]);
    assert_eq!(resumed_events.end, exported.end);
    assert_ne!(
        exported.items, resumed_events.items,
        "resume replayed the prefix"
    );
    let ExecutionEvent::Model {
        usage: Some(reference),
        ..
    } = &records[2].event
    else {
        panic!("original terminal commit lost its usage reference");
    };
    let bytes = journal.load(reference, None)?.await?;
    let record: crate::executor::ModelUsageRecord = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(record.dispatch.operation_id, operation);
    assert_eq!(record.tokens, usage.tokens);
    assert_eq!(record.complete_input_tokens, complete_input_tokens);
    assert_eq!(record.latency_ns, Some(123));
    assert_eq!(record.agent_path, None);
    let after = crate::executor::usage_page(&journal, operation, "agents/usage", 0).await?;
    assert_eq!(after.items.len(), 2);
    assert!(!after.items[0].completed);
    assert!(after.items[1].completed);
    assert_eq!(after.items[0].dispatch, after.items[1].dispatch);
    assert_eq!(after.items[1].tokens, usage.tokens);
    assert_eq!(after.end, 3);
    let resumed =
        crate::executor::usage_page(&journal, operation, "agents/usage", quiet.end).await?;
    assert_eq!(resumed.items.len(), 1);
    assert!(resumed.items[0].completed);
    assert_eq!(resumed.items[0].dispatch, before.items[0].dispatch);
    assert_eq!(resumed.end, after.end);
    assert_eq!(resumed.next, None);
    // Rescanning the prior prefix would duplicate the admitted contribution.
    assert!(after.items.len() > resumed.items.len());
    #[cfg(feature = "metrics-export")]
    {
        let projected =
            crate::metrics::journal_page(&journal, operation, "agents/usage", 0, |_| None).await?;
        let resumed =
            crate::metrics::journal_page(&journal, operation, "agents/usage", quiet.end, |_| None)
                .await?;
        assert_eq!(projected.end, after.end);
        assert_eq!(resumed.end, projected.end);
        assert_eq!(resumed.batches.len(), 1);
        let models = projected
            .batches
            .iter()
            .find(|batch| batch.table == crate::metrics::Table::ModelCalls)
            .unwrap();
        assert_eq!(models.data.num_rows(), 2);
        assert_eq!(
            resumed.batches[0].data,
            models.data.slice(1, 1),
            "resume must project the same committed observation without its prefix"
        );
    }
    let output = ExecutionEvent::ModelOutput {
        step: 0,
        digest: Some([81; 32]),
    };
    for _ in 0..2 {
        journal.append(operation, "output", &output, None)?.await?;
    }
    assert!(
        journal
            .append(operation, "duplicate-output", &output, None)?
            .await
            .is_err(),
        "a completed response acquired a second semantic-output receipt"
    );
    let cut = queries.checkpoint().await?.unwrap();
    assert_eq!(cut.state.through, 2, "output replay counted twice");
    assert_eq!(
        cut.state.tokens.measured,
        expected_tokens.unwrap_or(0),
        "output duplicated the original usage"
    );
    assert_eq!(cut.source.sequence, 3);
    assert!(
        queries
            .read_signal(crate::supervision::Kind::LoopOutput)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn private_payloads_reject_inline_bodies_at_every_append_entry_point() -> Result<()> {
    let (journal, _issuer) = test_support::journal().await?;
    let operation = OperationId::new();
    initialize(&journal, operation).await?;
    let valid = String::from_utf8(crate::contract::canonical_json_bytes(&events().remove(0))?)
        .map_err(|_| Error::Invalid("fixture JSON is not UTF-8".into()))?;
    assert_eq!(journal.inline_model_event_bytes(), 0);
    // Negative control: even valid, fully authorized small model content must
    // never be accepted as raw Stream data by either append entry point.
    let inline = ExecutionEvent::ModelInline {
        usage: None,
        step: 0,
        purpose: ModelPurpose::Response,
        event: valid.clone(),
    };
    assert!(
        journal
            .append_if_tail(operation, 2, "model:0:0", &inline, None)?
            .await
            .is_err()
    );
    for value in [
        valid.clone(),
        "{}".into(),
        format!(" {valid}"),
        "x".repeat(513),
    ] {
        assert!(
            journal
                .append(
                    operation,
                    "model:0:0",
                    &ExecutionEvent::ModelInline {
                        usage: None,
                        step: 0,
                        purpose: ModelPurpose::Response,
                        event: value,
                    },
                    None
                )?
                .await
                .is_err()
        );
    }
    for (index, (media, bytes)) in [
        ("text/vnd.acyclic.model-content.v1", b"\xff".as_slice()),
        (
            "text/vnd.acyclic.model-restarted.v1",
            b"not empty".as_slice(),
        ),
        ("text/plain", b"untyped event".as_slice()),
    ]
    .into_iter()
    .enumerate()
    {
        let file = journal
            .stage(operation, &format!("invalid:{index}"), bytes, media, None)?
            .await?;
        let invalid = ExecutionEvent::Model {
            step: 0,
            purpose: ModelPurpose::Response,
            event: file,
            usage: None,
        };
        assert!(
            journal
                .append_if_tail(operation, 2, &format!("invalid:{index}"), &invalid, None)?
                .await
                .is_err()
        );
    }
    assert_eq!(journal.records(operation, 0, 64, None).await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn private_payloads_require_original_read_and_write_grants() -> Result<()> {
    let (mut journal, original_issuer) = test_support::journal().await?;
    let operation = OperationId::new();
    initialize(&journal, operation).await?;
    let original = journal.scope.clone();
    let issuer = crate::core::AuthorityRoot::new(
        "append-cost",
        [81; 32],
        journal.verifier.audience().clone(),
    );
    for grant in [VolumeOperation::Read, VolumeOperation::Write] {
        journal.scope = issuer.bootstrap_scope(
            "owner",
            &crate::Capabilities::new([journal.volume.capability(grant, &mut None)?]),
            Some(crate::AgentId::from_bytes([81; 16])),
            &mut None,
        )?;
        assert!(
            crate::executor::append_model_event(
                &journal,
                operation,
                "model:0:0",
                (0, ModelPurpose::Response),
                &events().remove(0),
                None,
                None
            )
            .await
            .is_err(),
            "missing opposite grant for {grant:?}"
        );
    }
    journal.scope = original.clone();
    assert_eq!(journal.records(operation, 0, 64, None).await?.len(), 2);
    crate::executor::append_model_event(
        &journal,
        operation,
        "model:0:0",
        (0, ModelPurpose::Response),
        &events().remove(0),
        None,
        None,
    )
    .await?;
    for grant in [VolumeOperation::Read, VolumeOperation::Write] {
        journal.scope = issuer.bootstrap_scope(
            "owner",
            &crate::Capabilities::new([journal.volume.capability(grant, &mut None)?]),
            Some(crate::AgentId::from_bytes([81; 16])),
            &mut None,
        )?;
        assert_eq!(
            journal.records(operation, 0, 64, None).await.is_ok(),
            grant == VolumeOperation::Read,
            "cached replay requires read authority"
        );
    }
    journal.scope = original;
    assert!(journal.records(operation, 0, 64, None).await.is_ok());
    for (index, event) in [
        ModelEvent::Reasoning {
            delta: "thought π".into(),
        },
        ModelEvent::Restarted,
    ]
    .into_iter()
    .enumerate()
    {
        crate::executor::append_model_event(
            &journal,
            operation,
            &format!("model-live:{operation}:model:0:{}", index + 1),
            (0, ModelPurpose::Response),
            &event,
            None,
            None,
        )
        .await?;
        let records = journal.records(operation, 0, 64, None).await?;
        let Some(ExecutionRecord {
            event: ExecutionEvent::Model { event: file, .. },
            ..
        }) = records.last()
        else {
            return Err(Error::Storage(
                "original model observation is absent".into(),
            ));
        };
        let bytes = journal.load(file, None)?.await?;
        assert_eq!(
            crate::executor::decode_model_payload(file.descriptor().media_type(), &bytes, None)?,
            event,
        );
    }
    original_issuer.revoke_workload(&journal.scope)?;
    assert!(matches!(
        journal.records(operation, 0, 64, None).await,
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        crate::executor::append_model_event(
            &journal,
            operation,
            "model:0:3",
            (0, ModelPurpose::Response),
            &events().remove(0),
            None,
            None,
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}
