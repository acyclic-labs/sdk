//! The local agent host: a prompt is one staged file and one inbox commit, and
//! the root agent's actor runs its turn on the stock executor.

use super::{ObservedConversation, ScriptedModelProvider, text, volume, volume_config};
use crate::{
    Error, LocalHarness, Result,
    agent::{AgentPath, AgentRecord, ConversationEntry, Reply, StatusRecord, TurnInput},
    local::agent_conversation::Conversation,
};
use acyclic_stream::{
    ReadRequest, StreamError, StreamPath, StreamProvider, StreamProviderExt as _,
};
use futures::TryStreamExt;
use std::{path::Path, sync::Arc};

mod workspace_files;

/// The root conversation `harness` serves to its own in-process session.
async fn conversation(harness: &Arc<LocalHarness>) -> Result<Arc<Conversation>> {
    Ok(Conversation::open(harness.connect().await?))
}

#[tokio::test]
async fn cancelling_current_input_preserves_a_newer_queued_prompt() -> Result<()> {
    let directory = tempfile::tempdir().map_err(agent)?;
    let (dispatched, mut requests) = tokio::sync::mpsc::unbounded_channel();
    let provider = Arc::new(
        ScriptedModelProvider::new([
            super::ScriptedTurn::Events(vec![crate::test_support::ScriptedEvent::Pending {}]),
            super::ScriptedTurn::Text("queued prompt survived".into()),
        ])
        .with_context_capacity(super::capacity())
        .with_dispatch_observer(move |_| {
            let _ = dispatched.send(());
            Box::pin(async { Ok(()) })
        }),
    );
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), volume(directory.path()).await?, provider)?
                .without_default_tools()
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let mut observed = ObservedConversation::open(&harness).await?;
    let client = observed.conversation.clone();
    let running = client.send("running-input", "hold this turn").await?;
    model_dispatch_before_settlement(&harness, client.agent(), async {
        requests
            .recv()
            .await
            .ok_or_else(|| Error::Storage("dispatch producer closed".into()))
    })
    .await?;
    let queued = client
        .send("queued-input", "answer the next prompt")
        .await?;
    assert!(queued > running);
    assert_eq!(client.cancel().await?, Some(running));
    assert_eq!(
        observed.settled(running).await?.outcome,
        crate::agent::TurnOutcome::Cancelled
    );
    let next = observed.settled(queued).await?;
    assert_eq!(next.outcome, crate::agent::TurnOutcome::Completed);
    assert_eq!(client.output_text(&next).await?, "queued prompt survived");
    Ok(())
}

#[tokio::test]
async fn peer_pages_expose_members_without_private_titles_or_directory_authority() -> Result<()> {
    use crate::client::{AgentPageRequest, SessionsScope};
    use crate::local::sessions::{self, Filter};
    let directory = tempfile::tempdir().map_err(agent)?;
    let harness = Arc::new(
        super::open_scripted(
            volume_config(
                directory.path(),
                volume(directory.path()).await?,
                super::scripted([]),
            )?
            .without_default_tools(),
        )
        .await?,
    );
    let owner = harness.connect_sessions().await?;
    let visible = sessions::create(&owner, "visible-session", None).await?;
    let hidden = sessions::create_with_peer(&owner, "private-session", false, None).await?;
    sessions::rename(&visible, crate::OperationId::new(), "private title".into()).await?;
    let client = harness.connect().await?;
    assert!(
        sessions::page(&client, AgentPageRequest::default())
            .await
            .is_err()
    );
    let peers = sessions::page_scoped(
        &client,
        AgentPageRequest::default(),
        &Filter::default(),
        SessionsScope::Peers,
    )
    .await?;
    assert_eq!(peers.items.len(), 1);
    assert_eq!(peers.items[0].agent, *visible.root());
    assert_ne!(peers.items[0].agent, *hidden.root());
    assert!(peers.items[0].title.is_none());
    assert!(peers.items[0].status.is_some());
    assert!(
        sessions::page_scoped(
            &hidden,
            AgentPageRequest::default(),
            &Filter::default(),
            SessionsScope::Peers,
        )
        .await
        .is_err()
    );
    assert!(
        client
            .execute(&crate::client::AgentCommand::Sessions {
                scope: SessionsScope::Peers,
                requester: Some(visible.root().clone()),
                page: AgentPageRequest::default(),
            })
            .await
            .is_err()
    );
    sessions::set_peers(&client, "host-off", false).await?;
    let selected = sessions::page_scoped(
        &visible,
        AgentPageRequest::default(),
        &Filter::default(),
        SessionsScope::Peers,
    )
    .await?;
    assert!(
        selected.items.is_empty(),
        "the selected root excludes itself"
    );
    sessions::set_peers(&client, "host-on", true).await?;
    sessions::set_peers(&visible, "member-off", false).await?;
    let peers = sessions::page_scoped(
        &client,
        AgentPageRequest::default(),
        &Filter::default(),
        SessionsScope::Peers,
    )
    .await?;
    assert!(peers.items.is_empty());
    sessions::set_peers(&client, "requester-off", false).await?;
    assert!(
        sessions::page_scoped(
            &client,
            AgentPageRequest::default(),
            &Filter::default(),
            SessionsScope::Peers,
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn workspace_deltas_keep_local_turns_on_the_same_primary() -> Result<()> {
    use crate::agent::{
        InboxRecord, WorkspaceChanged, WorkspaceEntry, WorkspaceOp, WorkspaceRole, WorkspaceTable,
    };
    use crate::conversation::{
        ContentPublisher as _, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    };
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = super::scripted([
        super::ScriptedTurn::Text("before rename".into()),
        super::ScriptedTurn::Text("after rename".into()),
        super::ScriptedTurn::Text("after add".into()),
    ]);
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), project.clone(), provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let mut observed = ObservedConversation::open(&harness).await?;
    assert_eq!(
        observed.send("before", "before").await?.outcome,
        crate::agent::TurnOutcome::Completed
    );
    let (agents, root) = harness.agents().await?;
    let state = agents.state(root).await.map_err(agent)?;
    let mut entry = WorkspaceTable::primary_in(&state.volumes)
        .ok_or_else(|| Error::NotFound("primary workspace".into()))?
        .clone();
    let id = entry.id;
    entry.name = "renamed".into();
    let record = InboxRecord::WorkspaceChanged(Box::new(WorkspaceChanged {
        version: 1,
        op: WorkspaceOp::Rename,
        entry,
    }));
    let streams = harness
        .agent_stream
        .as_ref()
        .ok_or_else(|| Error::NotFound("agent stream".into()))?;
    let result = streams
        .commit(acyclic_stream::CommitRequest {
            idempotency_key: acyclic_stream::IdempotencyKey::new(b"rename-primary".to_vec())
                .map_err(agent)?,
            deadline_unix_millis: None,
            effects: vec![acyclic_stream::CommitEffect::hard(
                vec![acyclic_stream::CommitCondition::Absent {
                    path: root.closed().map_err(agent)?,
                }],
                vec![acyclic_stream::CommitMutation::Append {
                    path: root.inbox().map_err(agent)?,
                    records: vec![record.encode().map_err(agent)?.into()],
                }],
            )],
        })
        .await
        .map_err(agent)?;
    assert!(matches!(
        result,
        acyclic_stream::CommitOutcome::Committed(_)
    ));
    agents.drain(root).await.map_err(agent)?;
    let after = observed.send("after", "after").await?;
    assert_eq!(after.outcome, crate::agent::TurnOutcome::Completed);
    assert_eq!(provider.request_count(), 2);
    let state = agents.state(root).await.map_err(agent)?;
    let primary = WorkspaceTable::primary_in(&state.volumes)
        .ok_or_else(|| Error::NotFound("renamed workspace".into()))?;
    assert_eq!(primary.id, id);
    assert_eq!(primary.volume()?, &project);
    assert_eq!(primary.name, "renamed");
    let added = VolumeRef::new(
        project.provider().clone(),
        "added-repository".into(),
        VolumeClass::Project,
        VolumeOwner::Project("test".into()),
        &mut None,
    )?;
    harness.files.create_volume(&added).await?;
    let issuer = harness
        .root_authority
        .as_ref()
        .ok_or_else(|| Error::NotFound("test signing custody".into()))?;
    let grants = crate::Capabilities::new([added.capability(VolumeOperation::Write, &mut None)?]);
    let scope = issuer.bootstrap_scope("added-instructions", &grants, None, &mut None)?;
    let publisher = crate::filesystem::FilesystemContentPublisher::new(
        Arc::clone(&harness.files),
        &added,
        &harness.verifier,
        &scope,
        harness.limits.file_bytes,
        None,
    )?;
    let (instructions, _) = publisher
        .stage(
            crate::OperationId::new(),
            "repo/AGENTS.md",
            b"Instructions from the newly added repository.",
            "text/markdown",
            "AGENTS.md",
            Default::default(),
        )?
        .await?;
    let mut added = WorkspaceEntry::initial_volume(WorkspaceRole::User, "second".into(), added);
    added.root = "repo".into();
    let record = InboxRecord::WorkspaceChanged(Box::new(WorkspaceChanged {
        version: 1,
        op: WorkspaceOp::Add,
        entry: added,
    }));
    let result = streams
        .commit(acyclic_stream::CommitRequest {
            idempotency_key: acyclic_stream::IdempotencyKey::new(b"add-second-repository".to_vec())
                .map_err(agent)?,
            deadline_unix_millis: None,
            effects: vec![acyclic_stream::CommitEffect::hard(
                vec![acyclic_stream::CommitCondition::Absent {
                    path: root.closed().map_err(agent)?,
                }],
                vec![acyclic_stream::CommitMutation::Append {
                    path: root.inbox().map_err(agent)?,
                    records: vec![record.encode().map_err(agent)?.into()],
                }],
            )],
        })
        .await
        .map_err(agent)?;
    assert!(matches!(
        result,
        acyclic_stream::CommitOutcome::Committed(_)
    ));
    agents.drain(root).await.map_err(agent)?;
    let after = observed.send("added", "after add").await?;
    assert_eq!(after.outcome, crate::agent::TurnOutcome::Completed);
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[2].messages[..requests[1].messages.len()],
        requests[1].messages
    );
    assert!(
        !super::request_text(&harness, &requests[1])
            .await?
            .contains("Instructions from the newly added repository.")
    );
    let announced = after
        .announcements
        .iter()
        .find(|document| document.kind == crate::agent::AnnouncementKind::Workspaces)
        .ok_or_else(|| Error::NotFound("workspace delta".into()))?;
    let selection = announced
        .instructions
        .as_ref()
        .ok_or_else(|| Error::NotFound("instruction delta".into()))?;
    let proof_grants = crate::Capabilities::new([selection.manifest.read_capability(&mut None)?]);
    let proof_scope =
        issuer.bootstrap_scope("instruction-proof", &proof_grants, None, &mut None)?;
    let reader = crate::filesystem::FilesystemContentVerifier::new(
        Arc::clone(&harness.files),
        &harness.verifier,
        &proof_scope,
        crate::conversation::MAX_EXACT_JS_INTEGER,
        None,
    )?;
    assert!(
        selection
            .entries(reader.as_ref())
            .await?
            .iter()
            .flat_map(|entry| &entry.files)
            .any(|file| file == &instructions)
    );
    assert!(
        super::request_text(&harness, &requests[2])
            .await?
            .contains("Instructions from the newly added repository.")
    );
    Ok(())
}

#[tokio::test]
async fn approval_ranges_retain_the_original_status_until_every_chunk_flushes() -> Result<()> {
    use crate::model::{ModelContent, ModelEvent, ToolCall};
    use crate::test_support::{ScriptedEvent, ScriptedTurn};
    use acyclic_harness_client::{AgentCommand, AgentReply, EventProjection, MAX_READ_BYTES};

    let arguments = serde_json::json!({
        "path": "approval-request.txt",
        "content": "request".repeat(12_000),
    });
    let provider = super::scripted([ScriptedTurn::Text("ready".into())]);
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), project, provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled)
                .approval_policy(crate::approvals::ApprovalPolicy::default()),
        )
        .await?,
    );
    // The root orchestrator admits delegation, not file writes. Select an
    // actual worker before observing the original write approval.
    let connection = harness.connect().await?;
    let AgentReply::Spawned {
        agent: worker,
        delivery: crate::agent::Delivered::Committed { .. },
    } = connection
        .execute(&AgentCommand::Spawn {
            parent: connection.root().clone(),
            id: crate::agent::IntentId::try_from("approval-worker").map_err(agent)?,
            brief: ModelContent::Text("prepare the write".into()),
            role: "worker".into(),
        })
        .await?
    else {
        return Err(Error::Invalid("approval worker did not open".into()));
    };
    let client = Conversation::open(connection.select(worker.clone()).await?);
    // Spawn's receipt names the parent's inbox. Join the worker's own
    // committed input frontier rather than treating that receipt as its input.
    settled(&harness, &worker).await?;
    let opened_calls = provider.request_count();
    provider.extend([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                "write-request".into(),
                "write".into(),
                arguments.clone(),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::json!({}),
            }),
        ]),
        ScriptedTurn::Text("approved".into()),
    ]);
    let mut projection = EventProjection::new("approval-export".into());
    let mut observed = ObservedConversation {
        conversation: Arc::clone(&client),
        observer: client.replay(&projection)?,
    };
    let inbox = client.send("approval-input", "echo the request").await?;
    let (id, published_binding, delivery) = loop {
        let (event, delivery) = observed.observer.next_projected().await?;
        match event? {
            Some(crate::local::Event::Status(StatusRecord {
                phase: crate::agent::Phase::WaitingApproval { id, binding },
                ..
            })) => break (id, binding, delivery),
            Some(crate::local::Event::Turn(entry)) if entry.inbox == inbox => {
                let journal = client.execution_page(inbox, 0, false).await?;
                let failures: Vec<_> = journal
                    .page
                    .iter()
                    .flat_map(|page| &page.items)
                    .filter_map(|record| match &record.event {
                        crate::executor::ExecutionEvent::ToolFailed { reason, cause, .. } => {
                            Some((reason, cause))
                        }
                        _ => None,
                    })
                    .collect();
                return Err(Error::Invalid(format!(
                    "input settled before its expected approval: {:?}; {}; original tool failures: {failures:?}",
                    entry.outcome,
                    client.turn(&entry).await?.answer
                )));
            }
            Some(crate::local::Event::Status(StatusRecord {
                phase: crate::agent::Phase::Done { outcome },
                ..
            })) => {
                return Err(Error::Invalid(format!(
                    "agent ended before the required approval: {outcome:?}"
                )));
            }
            _ => {}
        }
        let after = projection.event(&delivery).after;
        projection.accepted(&delivery.resource, after);
    };
    let sequence = projection
        .event(&delivery)
        .sequence
        .ok_or_else(|| Error::Invalid("approval status has no sequence".into()))?;
    let pending = client.pending_approval(&id).await?.ok_or_else(|| {
        Error::Invalid("original approval binding is absent from the pending query".into())
    })?;
    let (_opening, phase) = client.observe_current().await?;
    assert!(matches!(
        phase,
        crate::agent::Phase::WaitingApproval { id: opening_id, binding }
            if opening_id == id && binding.as_ref() == Some(&pending.binding)
    ));
    assert_eq!(pending.status, sequence);
    assert_eq!(Some(&pending.binding), published_binding.as_ref());
    assert!(serde_json::to_vec(&pending).map_err(agent)?.len() < 1024);
    let preview = client.approval_prompt(&id).await?;
    let before = projection.cursor(&delivery.resource);
    let checkpoint = serde_json::to_vec(&projection).map_err(agent)?;
    let mut bytes = Vec::new();
    let (length, after) = loop {
        let offset = bytes.len() as u64;
        let (chunk, length) = client.approval_request(sequence, offset).await?;
        assert!(chunk.len() as u64 <= MAX_READ_BYTES);
        let event = projection
            .approval_payload_event(&delivery, offset, &chunk, length)
            .map_err(agent)?;
        assert_eq!(event.sequence, sequence);
        assert_eq!(projection.cursor(&delivery.resource), before);
        assert_eq!(serde_json::to_vec(&projection).map_err(agent)?, checkpoint);
        bytes.extend_from_slice(&chunk);
        if bytes.len() as u64 == length {
            assert_eq!(event.after.next, sequence + 1);
            break (length, event.after);
        }
        assert_eq!(
            event.after, before,
            "partial request acknowledged its status"
        );
        assert!(
            projection
                .approval_payload_event(&delivery, offset, &chunk[..chunk.len() - 1], length)
                .is_err()
        );
    };
    assert!(length > MAX_READ_BYTES * 2);
    let request: crate::interaction::Interaction = serde_json::from_slice(&bytes).map_err(agent)?;
    let crate::interaction::Interaction::Approval {
        prompt,
        operation_id,
        action_digest,
        subject:
            crate::interaction::ApprovalSubject::Tool {
                arguments: saved, ..
            },
        ..
    } = request
    else {
        return Err(Error::Invalid("request is not a tool approval".into()));
    };
    assert_eq!(preview.as_deref(), Some(prompt.as_str()));
    assert_eq!(pending.binding.operation_id, operation_id);
    assert_eq!(pending.binding.action_digest, action_digest);
    assert_eq!(saved, arguments);
    assert_eq!(
        client.approval_request(sequence, length).await?,
        (Vec::new(), length)
    );
    assert!(client.approval_request(sequence, length + 1).await.is_err());
    assert!(client.approval_request(0, 0).await.is_err());
    assert_eq!(provider.request_count(), opened_calls + 1);
    projection.accepted(&delivery.resource, Some(after));
    assert_eq!(projection.cursor(&delivery.resource).next, sequence + 1);
    let binding = crate::interaction::ApprovalBinding {
        operation_id,
        action_digest,
    };
    let mut different = binding.clone();
    different.action_digest[0] ^= 1;
    assert!(
        client
            .decide_with_binding(&id, different, true.into())
            .await
            .is_err()
    );
    assert_eq!(provider.request_count(), opened_calls + 1);
    client
        .decide_with_binding(&id, binding.clone(), true.into())
        .await?;
    let (agents, _) = harness.agents().await?;
    let root = &worker;
    observed.settled(inbox).await?;
    assert!(client.pending_approval(&id).await?.is_none());
    assert!(client.approval_prompt(&id).await?.is_none());
    let requests = provider.request_count();
    let (historical, saved_length) = client.approval_request(sequence, 0).await?;
    assert_eq!(saved_length, length);
    assert_eq!(historical, bytes[..MAX_READ_BYTES as usize]);
    assert_eq!(provider.request_count(), requests);
    client
        .decide_with_binding(&id, binding, true.into())
        .await?;
    assert_eq!(provider.request_count(), requests);
    agents
        .archive(
            root,
            &crate::agent::Sender::User {
                principal: crate::approvals::HUMAN_APPROVER.into(),
            },
            &crate::agent::IntentId::try_from("close-approval-export").map_err(agent)?,
        )
        .await
        .map_err(agent)?;
    assert_eq!(
        client.cancel().await?,
        None,
        "an archived agent has no current input to cancel"
    );
    assert_eq!(provider.request_count(), requests);
    Ok(())
}

fn agent(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

/// The control runs the same cut and model without an intervention. The
/// treatment must execute a real filesystem tool before free generation.
#[tokio::test]
async fn forced_continuation_retains_the_cut_and_applies_only_its_delta() -> Result<()> {
    use crate::{
        agent::{ContinuationOverride, ContinuationRequest, ForcedCall},
        client::{AgentCommand, AgentReply},
        model::ModelContent,
    };
    use acyclic_fs::{CancellationToken, WorkBudget};
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await
    .map_err(agent)?;
    let cancel = CancellationToken::new();
    let workspace = fs
        .open_workspace(
            project.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| agent(failure.error))?
        .value;
    workspace
        .write_text("/plan.md", "at cut")
        .await
        .map_err(agent)?;
    let (dispatched, mut requests) = tokio::sync::mpsc::unbounded_channel();
    let provider = Arc::new(
        ScriptedModelProvider::new(
            std::iter::once(super::ScriptedTurn::Events(vec![
                crate::test_support::ScriptedEvent::Pending {},
            ]))
            .chain(
                ["prefix", "later", "free", "free", "free", "free"]
                    .map(|text| super::ScriptedTurn::Text(text.into())),
            ),
        )
        .with_context_capacity(super::capacity())
        .with_dispatch_observer(move |_| {
            let _ = dispatched.send(());
            Box::pin(async { Ok(()) })
        }),
    );
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), project, provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let connection = harness.connect().await?;
    let AgentReply::Spawned {
        agent: original_child,
        ..
    } = connection
        .execute(&AgentCommand::Spawn {
            parent: root.clone(),
            id: crate::agent::IntentId::try_from("live-child").map_err(agent)?,
            brief: ModelContent::Text("Remain live at the cut".into()),
            role: "worker".into(),
        })
        .await?
    else {
        return Err(Error::Invalid("spawn returned another reply".into()));
    };
    model_dispatch_before_settlement(&harness, &original_child, async {
        requests
            .recv()
            .await
            .ok_or_else(|| Error::Storage("dispatch channel closed".into()))
    })
    .await?;
    let mut parent = ObservedConversation::open(&harness).await?;
    parent.send("prefix", "decision point").await?;
    let prefix = records(agents.provider(), root.conversation().map_err(agent)?).await?;
    let saved = crate::filesystem::history_checkpoint(agents.provider().as_ref(), &root, 0).await?;
    assert!(saved.live_children);
    workspace
        .write_text("/plan.md", "after cut")
        .await
        .map_err(agent)?;
    parent.send("later", "later decision").await?;

    let override_item = ContinuationOverride::Assistant {
        text: "FORCED_ACTION".into(),
        calls: vec![ForcedCall {
            name: "write".into(),
            arguments: serde_json::json!({"path":"plan.md","content":"forced files"}),
        }],
    };
    let source = format!("{root}@0");
    for (operation, intervention) in [
        ("treatment", Some(override_item.clone())),
        ("control", None),
        (
            "instruction",
            Some(ContinuationOverride::Instruction {
                text: "INJECTED_INSTRUCTION".into(),
            }),
        ),
    ] {
        let request = ContinuationRequest {
            version: 1,
            override_item: intervention.clone(),
        };
        let branch =
            crate::local::continuation::fork_from(&connection, &source, operation, request.clone())
                .await?;
        let child = branch.root().clone();
        let state = agents.state(&child).await.map_err(agent)?;
        assert!(state.open_children.is_empty());
        assert_eq!(
            records(agents.provider(), child.conversation().map_err(agent)?).await?,
            prefix
        );
        let creation = state.profile.as_ref().expect("decider creation");
        assert!(
            creation.template.is_none(),
            "continuation must retain the decider directive"
        );
        let metadata = creation
            .continuation
            .as_ref()
            .expect("continuation metadata");
        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.parent_run_id, root);
        assert_eq!(metadata.stream_offset, 0);
        assert!(metadata.live_children_at_cut);
        assert_eq!(
            metadata.workspace_generations[0].generation,
            saved.volumes[0].generation
        );
        assert_eq!(
            metadata.non_replay_manifest,
            [
                "wall_clocks",
                "web_network",
                "external_machine_state",
                "model_sampling"
            ]
        );
        assert_eq!(metadata.override_item.is_some(), intervention.is_some());
        let binding = state
            .volumes
            .iter()
            .find(|binding| binding.name == saved.volumes[0].binding.name)
            .expect("project binding");
        let forked = fs
            .open_workspace(
                binding.volume()?.storage_name(&mut None)?,
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await
            .map_err(|failure| agent(failure.error))?
            .value;
        assert_eq!(
            forked
                .read("/plan.md", 64, WorkBudget::UNBOUNDED, &cancel)
                .await
                .map_err(|failure| agent(failure.error))?
                .value
                .as_ref(),
            b"at cut"
        );
        let mut client = ObservedConversation::observe(Conversation::open(branch), Some(1)).await?;
        if let Some(file) = &metadata.override_item {
            let recorded: ContinuationRequest =
                serde_json::from_str(&client.conversation.text(file).await?).map_err(agent)?;
            assert_eq!(recorded, request);
        }
        let entry = client.send(operation, "continue").await?;
        let output: crate::executor::TurnOutput =
            serde_json::from_str(&client.conversation.text(&entry.output).await?).map_err(agent)?;
        let has_text = |message: &crate::model::ModelMessage, expected: &str| {
            matches!(&message.content, ModelContent::Text(text) if text == expected)
                || message.content.parts().iter().any(|part| {
                    matches!(part, crate::model::ModelContentPart::Text { text, .. } if text == expected)
                })
        };
        let applied = output
            .transcript
            .iter()
            .position(|message| has_text(message, "FORCED_ACTION"));
        // Negative control: the identical assertion would fail without the override.
        assert_eq!(applied.is_some(), operation == "treatment");
        if let Some(forced) = applied {
            let free = output
                .transcript
                .iter()
                .position(|message| has_text(message, "free"))
                .expect("freely sampled continuation");
            assert!(forced < free, "the override must precede continuation");
        }
        assert_eq!(output.text, "free");
        let context =
            super::request_text(&harness, &provider.last_request().expect("free request")).await?;
        assert!(context.contains("prefix"));
        assert!(!context.contains("later decision"));
        assert_eq!(
            context.contains("INJECTED_INSTRUCTION"),
            operation == "instruction"
        );
        assert_eq!(
            forked
                .read("/plan.md", 64, WorkBudget::UNBOUNDED, &cancel)
                .await
                .map_err(|failure| agent(failure.error))?
                .value
                .as_ref(),
            if operation == "treatment" {
                b"forced files".as_slice()
            } else {
                b"at cut".as_slice()
            }
        );
        let history = records(agents.provider(), child.conversation().map_err(agent)?).await?;
        assert_eq!(&history[..prefix.len()], prefix.as_slice());
        assert_eq!(history.len(), prefix.len() + 1);
        if operation == "treatment" {
            client.send("second", "continue freely again").await?;
            let context =
                super::request_text(&harness, &provider.last_request().expect("second request"))
                    .await?;
            assert_eq!(
                context.matches("FORCED_ACTION").count(),
                1,
                "override must apply exactly once"
            );
        }
    }
    assert_eq!(
        provider.request_count(),
        7,
        "forced action must not dispatch a model request"
    );
    assert!(
        agents
            .state(&root)
            .await
            .map_err(agent)?
            .open_children
            .contains(&original_child)
            .await
            .map_err(agent)?
    );
    assert_eq!(
        workspace
            .read("/plan.md", 64, WorkBudget::UNBOUNDED, &cancel)
            .await
            .map_err(|failure| agent(failure.error))?
            .value
            .as_ref(),
        b"after cut"
    );
    Ok(())
}

/// Locate the committed input by identity; notices may follow it in a request.
fn input_content<'a>(
    request: &'a crate::model::ModelRequest,
    body: &crate::conversation::FileRef,
) -> Result<&'a crate::model::ModelContent> {
    request
        .messages
        .iter()
        .find_map(|message| {
            (message.role == crate::model::ModelRole::User
                && message.content.file_parts().any(|(file, _)| file == body))
            .then_some(&message.content)
        })
        .ok_or_else(|| Error::NotFound("committed input absent from model request".into()))
}

async fn records<P: StreamProvider + ?Sized>(
    stream: &Arc<P>,
    path: StreamPath,
) -> Result<Vec<Vec<u8>>> {
    let tail = match stream.tail(path.clone()).await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => return Ok(Vec::new()),
        Err(error) => return Err(agent(error)),
    };
    let mut replay = acyclic_stream::Stream::new(Arc::clone(stream), path.as_str())
        .map_err(agent)?
        .replay(0)
        .until(tail);
    let mut values = Vec::new();
    while let Some(page) = replay.next_page().await.map_err(agent)? {
        values.extend(page.into_iter().map(|record| record.value.to_vec()));
    }
    Ok(values)
}

#[tokio::test]
async fn conversation_records_include_the_complete_committed_prefix() -> Result<()> {
    let provider = Arc::new(acyclic_stream::MemoryStream::default());
    let stream = acyclic_stream::Stream::new(Arc::clone(&provider), "captured-conversation")
        .map_err(agent)?;
    let expected = (0_u8..17).map(|value| vec![value]).collect::<Vec<_>>();
    stream
        .append_batch(
            expected.iter().cloned().map(Into::into).collect(),
            None,
            acyclic_stream::IdempotencyKey::unique(),
        )
        .await
        .map_err(agent)?;
    assert_eq!(records(&provider, stream.path().clone()).await?, expected);
    Ok(())
}

/// The root's conversation once it has settled every input sent before
/// this call, using the original actor checkpoint and runtime terminal event.
pub(super) async fn settled(harness: &Arc<LocalHarness>, root: &AgentPath) -> Result<Vec<Vec<u8>>> {
    let (agents, _) = harness.agents().await?;
    let completion = harness
        .agents
        .get()
        .expect("hosted agents")
        .runtime
        .completion();
    let observation = async {
        let inbox = crate::agent::inbox_tail(&**agents.provider(), root)
            .await
            .map_err(agent)?;
        if let Some(input) = inbox.checked_sub(1) {
            agents.wait_for_input(root, input).await.map_err(agent)?;
        }
        records(agents.provider(), root.conversation().map_err(agent)?).await
    };
    tokio::select! {
        biased;
        result = completion => {
            result?;
            Err(Error::Invalid("runtime completion returned success".into()))
        },
        result = observation => result,
    }
}

/// A retained test producer cannot outlive the original actor's progress.
async fn model_dispatch_before_settlement(
    harness: &Arc<LocalHarness>,
    original: &AgentPath,
    dispatch: impl Future<Output = Result<()>>,
) -> Result<()> {
    tokio::select! { biased;
        dispatched = dispatch => dispatched,
        completed = settled(harness, original) => {
            let entries = completed?;
            let last = entries.last().map(|entry| String::from_utf8_lossy(entry));
            Err(Error::Storage(format!(
                "original agent settled before model dispatch: {last:?}"
            )))
        },
    }
}

#[tokio::test]
async fn a_prompt_runs_one_root_agent_turn_and_replays_exactly() -> Result<()> {
    fn future_bytes<F>(_: fn(crate::LocalHarnessConfig) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    // The real open entry owns configuration and phase pointers, not their bodies.
    let opening_bytes = future_bytes(crate::local::open::open);
    assert!(
        opening_bytes <= 8192,
        "local open retained a {opening_bytes}-byte phase frame"
    );
    eprintln!("local opening future: {opening_bytes} bytes");

    use acyclic_harness_client::{AgentCommand, AgentReply, encode_agent_reply};

    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = super::scripted([super::ScriptedTurn::Events(vec![
        crate::test_support::ScriptedEvent::Event(crate::model::ModelEvent::Wait(
            crate::model::ModelWait::Started {
                cause: Some(crate::ModelTransportFailure::Status(429)),
                delay: std::time::Duration::from_secs(2),
                retry_after: true,
            },
        )),
        crate::test_support::ScriptedEvent::Event(crate::model::ModelEvent::Wait(
            crate::model::ModelWait::Finished {
                elapsed: std::time::Duration::from_millis(2007),
            },
        )),
        crate::test_support::ScriptedEvent::Event(crate::model::ModelEvent::Content {
            delta: "done".into(),
        }),
        crate::test_support::ScriptedEvent::Event(crate::model::ModelEvent::Completed {
            metadata: serde_json::Value::Null,
        }),
    ])]);
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), project, provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let hosting = harness.agents();
    // The caller carries the cell and its heap-owned initialization, not the
    // complete serving/publication future. Keep that frame within two pages.
    let hosting_bytes = std::mem::size_of_val(&hosting);
    assert!(
        hosting_bytes <= 8 * 1024,
        "agent hosting frame: {hosting_bytes} bytes"
    );
    eprintln!("agent hosting frame: {hosting_bytes} bytes");
    let (agents, root) = hosting.await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let connection = harness.connect().await?;
    let client = Conversation::open(connection.clone());
    let mut inspection = client.inspect().await?;
    while !inspection.snapshot().synchronized {
        inspection.advance().await?;
    }
    assert_eq!(inspection.snapshot().usage.calls, 0);
    let before = serde_json::to_value(inspection.snapshot()).map_err(agent)?;
    assert_eq!(before["agent"], root.as_str());
    assert_eq!(before["phase"], "idle");
    assert_eq!(before["open_children"], 0);
    assert!(matches!(
        client
            .children_page(acyclic_harness_client::AgentPageRequest::default())
            .await?,
        AgentReply::Children { items, next: None } if items.is_empty()
    ));
    let inbox = client.send("hello", "hello").await?;
    let conversation = root.conversation().map_err(agent)?;
    let entries = settled(&harness, &root).await?;
    assert_eq!(entries.len(), 1);
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    assert_eq!(entry.inbox, inbox);
    assert!(matches!(entry.input, TurnInput::Message { .. }));
    assert_eq!(
        provider.request_count(),
        1,
        "settled producer outcome: {:?}",
        entry.outcome
    );
    let turn = client.turn(&entry).await?;
    assert_eq!(
        (turn.prompt.as_deref(), turn.answer.as_str()),
        (Some("hello"), "done")
    );
    assert!(turn.tools.is_empty());
    // An output-only projection must not depend on the prompt's source. The
    // full turn still reads both, and the output keeps its original read fence.
    let absent = |file: &crate::conversation::FileRef| {
        crate::conversation::FileRef::new(
            file.volume().clone(),
            "missing-turn-text".into(),
            file.version().into(),
            file.descriptor().clone(),
            file.display_name().into(),
            &mut None,
        )
        .map_err(agent)
    };
    let mut output_only = entry.clone();
    let TurnInput::Message { body, .. } = &mut output_only.input else {
        return Err(Error::Invalid("the settled input is not a prompt".into()));
    };
    **body = absent(body)?;
    assert!(client.turn(&output_only).await.is_err());
    assert_eq!(client.output_text(&output_only).await?, "done");
    output_only.output = absent(&entry.output)?;
    assert!(client.output_text(&output_only).await.is_err());
    // The same live query started before this input. Its exact completion
    // replaces the original admission, rather than counting another call.
    while inspection.snapshot().usage.completed == 0 {
        inspection.advance().await?;
    }
    assert_eq!(inspection.snapshot().usage.calls, 1);
    assert_eq!(inspection.snapshot().usage.completed, 1);
    assert_eq!(inspection.snapshot().waits.started, 1);
    assert_eq!(inspection.snapshot().waits.completed, 1);
    assert_eq!(
        inspection.snapshot().waits.elapsed_ns(),
        Some(2_007_000_000)
    );
    assert_eq!(
        inspection.snapshot().model,
        Some(("morph", "morph-kimik31"))
    );
    assert_eq!(inspection.snapshot().open_children, Some(0));
    assert_eq!(
        inspection.snapshot().context,
        Some(crate::executor::ContextUsage::Unavailable),
        "ordinary disabled-compaction dispatch does not perform accounting"
    );
    assert!(inspection.snapshot().usage.currency.is_none());
    assert_eq!(inspection.snapshot().usage.token_nano_units.value(), None);
    // The file exporter must accept the same original completion, including
    // request settings when research diagnostics are disabled.
    let mut exported = Vec::new();
    client.write_usage_jsonl(&mut exported).await?;
    let mut rows = serde_json::Deserializer::from_slice(&exported)
        .into_iter::<crate::executor::ModelUsageProjection<'_>>();
    let completed = rows
        .next()
        .ok_or_else(|| Error::Storage("completed usage export is absent".into()))?
        .map_err(agent)?;
    assert!(completed.usage.completed);
    assert_eq!(
        inspection.snapshot().model_time_ns.value(),
        completed.usage.latency_ns,
        "inspection must retain the original monotonic duration"
    );
    assert_eq!(
        &inspection.snapshot().providers["morph"],
        inspection.snapshot().usage,
        "the admitted provider must receive the same original contribution"
    );
    assert_eq!(
        completed.version,
        crate::executor::ModelUsageProjection::VERSION
    );
    assert_eq!(
        completed.usage.version,
        crate::executor::ModelUsageRecord::VERSION
    );
    assert!(completed.usage.started_at.is_some());
    assert!(completed.usage.completed_at.is_some());
    assert_eq!(completed.usage.agent_path.as_deref(), Some(root.as_str()));
    assert_eq!(
        completed.usage.sampling,
        Some(crate::model::ModelSampling::default())
    );
    assert!(rows.next().is_none());
    // The real journal's current format crosses the same shared boundary in
    // both directions. No other version or agent can enter this usage view.
    let command = AgentCommand::Usage {
        agent: root.clone(),
        input: inbox,
        after: 0,
        wait: false,
        observed_children: Some(0),
    };
    let mut reply = Ok(connection.execute(&command).await?);
    let version = crate::executor::ModelUsageRecord::VERSION;
    for (version, agent_path) in [(version ^ 1, Some(root.as_str())), (version, None)] {
        let Ok(AgentReply::Usage {
            page: Some(page), ..
        }) = &mut reply
        else {
            return Err(Error::Invalid("the settled usage page is absent".into()));
        };
        assert!(!page.items.is_empty());
        for row in &mut page.items {
            row.version = version;
            row.agent_path = agent_path.map(str::to_owned);
        }
        assert!(encode_agent_reply(&reply, &command).is_err());
    }
    // A client reads the settled output from the root's private volume.
    let output = harness
        .client_reader()?
        .read(&entry.output, 1 << 20, None, None)?
        .await?;
    assert!(!output.is_empty());
    // The same authenticated connection queries an original status cut; a
    // caller cannot choose a foreign owner or invent a missing status record.
    let (samples, next) = client
        .child_wakes(0, acyclic_harness_client::AgentPageRequest::default())
        .await?;
    assert!(samples.is_empty() && next.is_none());
    assert!(
        client
            .child_wakes(
                u64::MAX,
                acyclic_harness_client::AgentPageRequest::default()
            )
            .await
            .is_err()
    );
    assert!(
        connection
            .execute(&AgentCommand::ChildWakes {
                agent: AgentPath::root(&crate::agent::AgentId::from_bytes([91; 16])),
                status: 0,
                page: acyclic_harness_client::AgentPageRequest::default(),
            })
            .await
            .is_err()
    );
    // Give this scenario two visible files, independent of internal layout.
    let cancellation = acyclic_fs::CancellationToken::new();
    let filesystem = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await
    .map_err(agent)?;
    let private = filesystem
        .open_workspace(
            entry.output.volume().storage_name(&mut None)?,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| agent(failure.error))?
        .value;
    let original = b"directory original";
    private
        .write_text(
            "/directory-a",
            std::str::from_utf8(original).map_err(agent)?,
        )
        .await
        .map_err(agent)?;
    private
        .write_text("/directory-b", "second")
        .await
        .map_err(agent)?;
    // The same authenticated session selects a directory and its original
    // generation, then continues only at that immutable generation.
    let page =
        |after| acyclic_harness_client::AgentPageRequest::new(1, 64 * 1024, after).map_err(agent);
    let first = client.directory_page("", None, page(None)?).await?;
    assert_eq!(first.entries.len(), 1);
    assert!(first.has_more);
    private
        .write_text("/directory-a", "changed after selection")
        .await
        .map_err(agent)?;
    // Credit includes the canonical frame, generation and continuation bit.
    // Give the host room for exactly one row while requesting many rows.
    let selected = AgentCommand::Directory {
        agent: root.clone(),
        target: crate::conversation::DirectoryTarget::Private {},
        path: String::new(),
        generation: Some(first.generation.clone()),
        after_generation: None,
        page: page(None)?,
    };
    let encoded = encode_agent_reply(
        &Ok(AgentReply::Directory {
            page: first.clone(),
        }),
        &selected,
    )
    .map_err(agent)?;
    let credit = u32::try_from(encoded.len()).map_err(agent)?;
    let bounded = client
        .directory_page(
            "",
            Some(&first.generation),
            acyclic_harness_client::AgentPageRequest::new(128, credit, None).map_err(agent)?,
        )
        .await?;
    assert_eq!(bounded, first);
    assert!(
        client
            .directory_page(
                "",
                Some(&first.generation),
                acyclic_harness_client::AgentPageRequest::new(128, credit - 1, None)
                    .map_err(agent)?,
            )
            .await
            .is_err()
    );
    let last = &first.entries[0].name;
    let second = client
        .directory_page("", Some(&first.generation), page(Some(last.clone()))?)
        .await?;
    assert_eq!(second.generation, first.generation);
    assert_eq!(second.entries.len(), 1);
    assert!(second.entries[0].name > *last);
    let (range, length) = client
        .read_private_range("directory-a", &first.generation, 0)
        .await?;
    assert_eq!(length, original.len() as u64);
    assert!(!range.is_empty());
    assert_eq!(range.as_slice(), &original[..range.len()]);
    let (end, end_length) = client
        .read_private_range("directory-a", &first.generation, length)
        .await?;
    assert!(end.is_empty());
    assert_eq!(end_length, length);
    // Reusing the preceding page cannot pass the shared continuation boundary.
    let continued = acyclic_harness_client::AgentCommand::Directory {
        agent: root.clone(),
        target: crate::conversation::DirectoryTarget::Private {},
        path: String::new(),
        generation: Some(first.generation.clone()),
        after_generation: None,
        page: page(Some(last.clone()))?,
    };
    assert!(
        acyclic_harness_client::encode_agent_reply(
            &Ok(acyclic_harness_client::AgentReply::Directory {
                page: first.clone()
            },),
            &continued
        )
        .is_err()
    );
    // A successful range must make progress or prove this exact file's EOF.
    let read = acyclic_harness_client::AgentCommand::Read {
        agent: root.clone(),
        offset: 0,
        target: acyclic_harness_client::ReadTarget::Path {
            target: crate::conversation::DirectoryTarget::Private {},
            path: "directory-a".to_owned(),
            generation: first.generation.clone(),
        },
    };
    assert!(
        acyclic_harness_client::encode_agent_reply(
            &Ok(acyclic_harness_client::AgentReply::read(&[], length),),
            &read
        )
        .is_err()
    );
    // A path cannot bypass the original private grant or its internal boundary.
    assert!(
        client
            .directory_page(".system", None, page(None)?)
            .await
            .is_err()
    );
    assert!(
        client
            .directory_page("", None, page(Some(last.clone()))?)
            .await
            .is_err()
    );
    assert!(
        client
            .read_private_range(".system/record", &first.generation, 0)
            .await
            .is_err()
    );
    assert!(
        client
            .read_private_range("directory-a", &first.generation, length + 1)
            .await
            .is_err()
    );
    let paths = vec!["directory-a".to_owned(), "absent-row".to_owned()];
    let mut visible =
        crate::local::agent_conversation::VisibleFiles::new(Arc::clone(&client), paths.clone());
    visible.retain([root.clone()], 128);
    futures::future::poll_fn(|cx| visible.poll_changed(cx)).await;
    let snapshot = client.agent_files(paths.clone(), None).await?;
    assert_eq!(
        visible
            .snapshot(&root)
            .ok_or_else(|| Error::NotFound("visible cut".into()))?
            .map_err(agent)?
            .generation,
        snapshot.cut.generation
    );
    assert!(snapshot.files["absent-row"].is_none());
    assert_eq!(
        snapshot.files["directory-a"]
            .as_ref()
            .ok_or_else(|| Error::NotFound("row".into()))?
            .text,
        "changed after selection"
    );
    let waiting = client.agent_files(paths.clone(), Some(snapshot.cut.clone()));
    futures::pin_mut!(waiting);
    assert!(
        futures::poll!(waiting.as_mut()).is_pending(),
        "unchanged files must wait on publication"
    );
    assert!(futures::poll!(futures::future::poll_fn(|cx| visible.poll_changed(cx))).is_pending());
    private
        .write_text("/directory-a", "authored row changed")
        .await
        .map_err(agent)?;
    let updated = waiting.await?;
    futures::future::poll_fn(|cx| visible.poll_changed(cx)).await;
    assert_eq!(
        visible
            .snapshot(&root)
            .ok_or_else(|| Error::NotFound("published visible cut".into()))?
            .map_err(agent)?
            .generation,
        updated.cut.generation
    );
    visible.retain(std::iter::empty(), 0);
    assert!(visible.snapshot(&root).is_none());
    assert!(futures::poll!(futures::future::poll_fn(|cx| visible.poll_changed(cx))).is_pending());
    assert_ne!(updated.cut.generation, snapshot.cut.generation);
    assert_eq!(
        updated.files["directory-a"]
            .as_ref()
            .ok_or_else(|| Error::NotFound("updated row".into()))?
            .text,
        "authored row changed"
    );
    assert!(
        connection
            .execute(&AgentCommand::AgentFiles {
                agent: AgentPath::root(&crate::agent::AgentId::from_bytes([91; 16])),
                paths,
                after: None,
            })
            .await
            .is_err()
    );
    // The same intent replays its commit at the same sequence; no second turn runs.
    assert_eq!(client.send("hello", "hello").await?, inbox);
    assert_eq!(records(agents.provider(), conversation).await?.len(), 1);
    // The status-only follow retains the opening frontier through later tool
    // publications and observes archive without a conversation or output reader.
    agents
        .archive(
            &root,
            &crate::agent::Sender::User {
                principal: crate::approvals::HUMAN_APPROVER.into(),
            },
            &crate::agent::IntentId::try_from("close-inspected-agent").map_err(agent)?,
        )
        .await
        .map_err(agent)?;
    while !matches!(
        inspection.snapshot().phase,
        crate::agent::Phase::Done { .. }
    ) {
        inspection.advance().await?;
    }
    Ok(())
}

/// The model's real channel owns progress; dropping it must close the same
/// Session that carries the local commands and follows, retaining queued input.
#[tokio::test]
async fn lost_model_producer_closes_local_session_without_settling_input() -> Result<()> {
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::sync::oneshot;

    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let (entered, entering) = oneshot::channel();
    let (producer, result) = oneshot::channel::<()>();
    let gate = Mutex::new(Some((entered, result)));
    let warming = AtomicBool::new(true);
    let provider = ScriptedModelProvider::new([
        super::ScriptedTurn::Text("ready".into()),
        super::ScriptedTurn::Events(Vec::new()),
    ])
    .with_context_capacity(super::capacity())
    .with_dispatch_observer(move |_| {
        let warming = warming.swap(false, Ordering::SeqCst);
        let gate = (!warming).then(|| gate.lock().unwrap().take()).flatten();
        Box::pin(async move {
            if warming {
                return Ok(());
            }
            let (entered, result) = gate.ok_or_else(|| Error::Invalid("second dispatch".into()))?;
            let _ = entered.send(());
            result
                .await
                .map_err(|_| Error::Storage("model producer closed".into()))?;
            Err(Error::Invalid("model gate unexpectedly released".into()))
        })
    });
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), project, Arc::new(provider))?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let connection = harness.connect().await?;
    let client = Conversation::open(connection.clone());
    // Create the conversation through its actual first turn before testing
    // out-of-range follows: an absent stream intentionally waits for creation.
    let mut warmup_events = client.observe(None).await?;
    let warmup = client.send("warmup", "hello").await?;
    assert!(matches!(
        client.settle("warmup", warmup, &mut warmup_events).await?,
        crate::local::agent_conversation::Settlement::Reply(crate::agent::Reply::Answered { .. })
    ));
    let mut events = client.observe(Some(0)).await?;
    let inbox = client.send("lost-producer", "hello").await?;
    model_dispatch_before_settlement(&harness, connection.root(), async {
        entering.await.map_err(agent)
    })
    .await?;
    // Same canonical agent, different original authenticated Session. Refusal
    // must precede even the reply query, without consuming the other follow.
    let other = conversation(&harness).await?;
    let mut unrelated = other.observe(Some(0)).await?;
    {
        let invalid = client.settle("lost-producer", inbox, &mut unrelated);
        tokio::pin!(invalid);
        let observed = futures::poll!(&mut invalid);
        assert!(
            matches!(&observed, std::task::Poll::Ready(Err(Error::Invalid(reason))) if reason.contains("observer from the original connection")),
            "settlement admitted an unrelated progress producer or performed I/O before refusal: {observed:?}"
        );
    }
    // Retain another observer on the original connection. Its first failed
    // opening retires only that follow; the other selected follows remain.
    // A future live cursor is valid and waits, so it cannot provoke retirement.
    let mut retired = client.replay(&acyclic_harness_client::EventProjection::new(
        "retired-source".into(),
    ))?;
    let root = connection.root().clone();
    let settling = settled(&harness, &root);
    tokio::pin!(settling);
    assert!(
        futures::poll!(&mut settling).is_pending(),
        "live runtime must retain settlement"
    );
    let mut usage = client.usage_query();
    while !usage.synchronized() {
        usage.advance().await?;
    }
    assert_eq!(usage.totals().calls, 2);
    assert_eq!(usage.totals().completed, 1);
    let before = usage.totals().clone();
    let fenced = client.wait_current_input();
    tokio::pin!(fenced);
    assert!(
        futures::poll!(&mut fenced).is_pending(),
        "captured input must remain pending while its producer is live"
    );
    let waiting = client.settle("lost-producer", inbox, &mut events);
    tokio::pin!(waiting);
    assert!(
        futures::poll!(&mut waiting).is_pending(),
        "live producer must retain the wait"
    );
    let runtime = &harness.agents.get().unwrap().runtime;
    let admission = runtime.admit(std::future::pending::<Result<()>>());
    tokio::pin!(admission);
    assert!(
        futures::poll!(&mut admission).is_pending(),
        "live runtime must retain in-flight admission"
    );
    let usage_result = {
        let advancing = usage.advance();
        tokio::pin!(advancing);
        assert!(
            futures::poll!(&mut advancing).is_pending(),
            "quiet live usage must wait at its retained frontier"
        );
        drop(producer);
        advancing.await
    };
    assert!(
        matches!(&usage_result, Err(Error::Storage(reason)) if reason.contains("model producer closed")),
        "usage lost the runtime cause: {usage_result:?}"
    );
    let fenced = fenced.await;
    assert!(
        matches!(&fenced, Err(Error::Storage(reason)) if reason.contains("model producer closed")),
        "input fence lost the runtime cause: {fenced:?}"
    );
    let admitted = futures::poll!(&mut admission);
    assert!(
        matches!(&admitted, std::task::Poll::Ready(Err(Error::Storage(reason))) if reason.contains("model producer closed")),
        "in-flight admission lost the runtime cause: {admitted:?}"
    );
    assert_eq!(
        usage.totals(),
        &before,
        "source loss changed the admitted projection"
    );
    // The session closed by the failed runtime reports that failure, not a bare `unavailable`.
    let observed = futures::poll!(&mut waiting);
    assert!(
        matches!(&observed, std::task::Poll::Ready(Err(Error::Storage(reason))) if reason.contains("model producer closed")),
        "public settlement retained its lost model producer: {observed:?}"
    );
    let hosted = harness.agents.get().unwrap();
    assert!(
        matches!(hosted.runtime.completion().await, Err(Error::Storage(reason)) if reason.contains("model producer closed"))
    );
    loop {
        match retired.next_projected().await {
            Err(Error::Storage(reason)) => {
                assert!(
                    reason.contains("model producer closed"),
                    "retired follow lost the runtime cause: {reason}"
                );
                break;
            }
            Err(error) => return Err(error),
            Ok((event, _)) => {
                event?;
            }
        }
    }
    let invalid = client.settle("lost-producer", inbox, &mut retired);
    tokio::pin!(invalid);
    assert!(
        matches!(
            futures::poll!(&mut invalid),
            std::task::Poll::Ready(Err(Error::Invalid(_)))
        ),
        "settlement admitted a retired original follow or performed I/O before refusal"
    );
    let settled = futures::poll!(&mut settling);
    assert!(
        matches!(&settled, std::task::Poll::Ready(Err(Error::Storage(reason))) if reason.contains("model producer closed")),
        "settlement bypassed the original runtime terminal event: {settled:?}"
    );
    assert!(
        harness.agent_stream.as_ref().unwrap().failure().is_none(),
        "the Session's runtime failure must not be invented as a provider failure"
    );
    assert!(
        matches!(connection.execute(&acyclic_harness_client::AgentCommand::Root {}).await, Err(Error::Storage(reason)) if reason.contains("model producer closed")),
        "commands must retain the same runtime cause"
    );
    assert_eq!(
        hosted
            .agents()
            .reply(
                hosted.root(),
                &crate::agent::IntentId::try_from("lost-producer").map_err(agent)?,
                inbox
            )
            .await
            .map_err(agent)?,
        crate::agent::Reply::Pending,
        "failed input stays recoverable"
    );
    assert_eq!(
        records(
            hosted.agents().provider(),
            hosted.root().conversation().map_err(agent)?
        )
        .await?
        .len(),
        1,
        "failed input appended another conversation entry"
    );
    assert!(
        matches!(harness.connect().await, Err(Error::Storage(reason)) if reason.contains("model producer closed")),
        "late clients must retain the original runtime cause"
    );
    // Both admitted calls outlive the runtime. Only the warm-up completed;
    // losing the second producer must preserve its distinct unfinished call.
    let mut exported = Vec::new();
    client
        .observation()
        .await?
        .write_usage_jsonl(&mut exported)
        .await?;
    let rows = serde_json::Deserializer::from_slice(&exported)
        .into_iter::<crate::executor::ModelUsageProjection<'_>>()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(agent)?;
    assert_eq!(rows.len(), 2, "an admitted call was lost or duplicated");
    assert_ne!(rows[0].usage.dispatch, rows[1].usage.dispatch);
    assert_eq!(rows.iter().filter(|row| row.usage.completed).count(), 1);
    Ok(())
}

#[tokio::test]
async fn idle_usage_follows_child_lifecycle_without_a_parent_model_call() -> Result<()> {
    use acyclic_harness_client::{AgentCommand, AgentReply};
    let directory = tempfile::tempdir().map_err(agent)?;
    let provider = super::scripted([super::ScriptedTurn::Text("child done".into())]);
    let harness = Arc::new(
        super::open_scripted(
            volume_config(
                directory.path(),
                volume(directory.path()).await?,
                provider.clone(),
            )?
            .without_default_tools()
            .workspace_mounting(crate::local::WorkspaceMounting::Physical)
            .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let connection = harness.connect().await?;
    let root = connection.root().clone();
    let client = Conversation::open(connection.clone());
    let mut usage = client.usage_query();
    while !usage.synchronized() {
        usage.advance().await?;
    }
    assert_eq!(usage.open_children(), Some(0));
    // Even a waiting selection can establish its initial metadata at an empty
    // tail. No journal record is invented to transport an actor observation.
    let (agents, _) = harness.agents().await?;
    let tail = crate::agent::inbox_tail(&**agents.provider(), &root)
        .await
        .map_err(agent)?;
    assert!(matches!(
        connection
            .execute(&AgentCommand::Usage {
                agent: root.clone(),
                input: tail,
                after: 0,
                wait: true,
                observed_children: None,
            })
            .await?,
        AgentReply::Usage {
            page: None,
            open_children: 0,
            ..
        }
    ));
    let spawning = async {
        match connection
            .execute(&AgentCommand::Spawn {
                parent: root.clone(),
                id: crate::agent::IntentId::try_from("counted-child").map_err(agent)?,
                brief: crate::model::ModelContent::Text("child".into()),
                role: "worker".into(),
            })
            .await?
        {
            AgentReply::Spawned { agent, .. } => Ok(agent),
            _ => Err(Error::Invalid("spawn did not return its child".into())),
        }
    };
    let observing = async {
        while usage.open_children() != Some(1) {
            usage.advance().await?;
        }
        Ok::<_, Error>(())
    };
    let (child, ()) = tokio::try_join!(spawning, observing)?;
    settled(&harness, &child).await?;
    // Consume the already committed parent control input before parking at
    // its quiet tail again. Both queries retain their original producer cuts.
    let closing = async {
        agents
            .archive(
                &child,
                &crate::agent::Sender::User {
                    principal: crate::approvals::HUMAN_APPROVER.into(),
                },
                &crate::agent::IntentId::try_from("close-counted-child").map_err(agent)?,
            )
            .await
            .map_err(agent)?;
        settled(&harness, &child).await?;
        Ok::<_, Error>(())
    };
    let observing = async {
        while usage.open_children() != Some(0) {
            usage.advance().await?;
        }
        Ok::<_, Error>(())
    };
    tokio::try_join!(closing, observing)?;
    assert_eq!(usage.totals().calls, 0);
    assert_eq!(
        provider.request_count(),
        1,
        "only the child requested a model"
    );
    Ok(())
}

#[tokio::test]
async fn session_and_subtree_prices_replay_and_preserve_unfinished_provider_work() -> Result<()> {
    fn completed_turn(entries: Vec<Vec<u8>>) -> Result<()> {
        let entry = entries
            .last()
            .ok_or_else(|| Error::Invalid("pricing turn is absent".into()))?;
        let entry = ConversationEntry::decode(entry).map_err(agent)?;
        if entry.outcome != crate::agent::TurnOutcome::Completed {
            return Err(Error::Invalid(format!(
                "pricing turn did not complete: {:?}",
                entry.outcome
            )));
        }
        Ok(())
    }
    use crate::{
        executor::{ModelPriceRecord, ModelPrices, TokenPrice},
        local::agent_conversation::UsageScope,
        model::{ModelContent, ModelEvent},
        test_support::{ScriptedEvent, ScriptedTurn},
    };
    use acyclic_harness_client::{AgentCommand, AgentReply};
    let completed = |input, output| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::Content {
                delta: "done".into(),
            }),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::json!({
                    "usage": {"input_tokens": input, "output_tokens": output}
                }),
            }),
        ])
    };
    let prices = Arc::new(ModelPrices::from_jsonl(
        &["morph-kimik31", "morph-dsv41flash"]
            .into_iter()
            .map(|name| {
                serde_json::to_string(&ModelPriceRecord {
                    version: 1,
                    revision: "chosen-price-1".into(),
                    provider: "morph".into(),
                    model: name.into(),
                    effective_at: 1,
                    currency: "USD".into(),
                    input_nano_units_per_million: Some(1_000_000),
                    output_nano_units_per_million: Some(2_000_000),
                    cached: TokenPrice::Included,
                    cache_write: TokenPrice::Included,
                    reasoning: TokenPrice::Included,
                })
                .map_err(agent)
            })
            .collect::<Result<Vec<_>>>()?
            .join("\n")
            .into_bytes(),
    )?);
    let provider = super::scripted([
        completed(100, 20),
        completed(200, 30),
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::Wait(crate::model::ModelWait::Started {
                cause: Some(crate::ModelTransportFailure::Connect),
                delay: std::time::Duration::from_secs(2),
                retry_after: false,
            })),
            ScriptedEvent::Interrupted {
                error: "priced provider failure".into(),
            },
        ]),
    ]);
    let directory = tempfile::tempdir().map_err(agent)?;
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), volume(directory.path()).await?, provider)?
                .without_default_tools()
                .workspace_mounting(crate::local::WorkspaceMounting::Physical)
                .compaction(crate::context::CompactionPolicy::Capacity(
                    crate::context::CapacityCompaction {},
                )),
        )
        .await?,
    );
    let connection = harness.connect().await?;
    let root = connection.root().clone();
    let client = Conversation::open(connection.clone());
    client.send("priced-root", "root").await?;
    completed_turn(settled(&harness, &root).await?)?;
    let AgentReply::Spawned { agent: child, .. } = connection
        .execute(&AgentCommand::Spawn {
            parent: root.clone(),
            id: crate::agent::IntentId::try_from("priced-child").map_err(agent)?,
            brief: ModelContent::Text("child".into()),
            role: "worker".into(),
        })
        .await?
    else {
        return Err(Error::Invalid("spawn did not return its child".into()));
    };
    completed_turn(settled(&harness, &child).await?)?;
    let root_cost = client.usage_totals(UsageScope::Agent, &prices).await?;
    assert_eq!(
        (
            root_cost.calls,
            root_cost.completed,
            root_cost.token_nano_units.value()
        ),
        (1, 1, Some(140))
    );
    let tree_cost = client
        .usage_totals(UsageScope::Tree(&|_| false), &prices)
        .await?;
    assert_eq!(
        (
            tree_cost.calls,
            tree_cost.completed,
            tree_cost.token_nano_units.value()
        ),
        (2, 2, Some(400))
    );
    assert_eq!(
        client
            .usage_totals(UsageScope::Tree(&|agent| agent == &child), &prices)
            .await?,
        root_cost
    );
    // Fresh authenticated selection and the live fold agree with the visitor;
    // the child's inherited root context is not charged a second time.
    let reopened = conversation(&harness).await?;
    let mut live = reopened.usage_query_with_prices(Arc::clone(&prices));
    while !live.synchronized() {
        live.advance().await?;
    }
    assert_eq!(live.totals(), &root_cost);
    assert!(matches!(
        live.context(),
        Some(crate::executor::ContextUsage::Counted { input_tokens, capacity_tokens: 32_768 })
            if input_tokens > 0
    ));
    assert_eq!(
        reopened
            .usage_totals(UsageScope::Tree(&|_| false), &prices)
            .await?,
        tree_cost
    );
    let mut events = client.observe(Some(client.settled().await?)).await?;
    let inbox = client.send("priced-failure", "fail").await?;
    let result = client.settle("priced-failure", inbox, &mut events).await;
    assert!(
        format!("{result:?}").contains("priced provider failure"),
        "failure lost its original cause: {result:?}"
    );
    let durable = client
        .observation()
        .await?
        .usage_totals(UsageScope::Tree(&|_| false), &prices)
        .await?;
    assert_eq!((durable.calls, durable.completed), (3, 2));
    assert_eq!(durable.token_nano_units.known, 400);
    assert_eq!(durable.token_nano_units.unknown, 1);
    assert_eq!(durable.token_nano_units.value(), None);
    while live.waits().started == 0 {
        live.advance().await?;
    }
    assert_eq!((live.waits().started, live.waits().completed), (1, 0));
    assert_eq!(live.waits().elapsed_ns(), None);
    Ok(())
}

/// The names directly under `path` on `volume`.
async fn names(
    harness: &LocalHarness,
    volume: &crate::conversation::VolumeRef,
    path: &str,
) -> Result<Vec<String>> {
    let grant = crate::conversation::ContentGrant::verify_directory_read(
        &harness.verifier,
        &harness.signed,
        volume,
        "",
        &mut None,
    )?;
    let (_, page) = harness
        .files
        .list_private_directory(
            volume,
            &grant,
            path,
            crate::filesystem::DirectoryOptions {
                generation: None,
                after: None,
                maximum_entries: 16,
                ceiling: None,
            },
        )
        .await?
        .expect("fixture directory exists");
    Ok(page
        .entries
        .iter()
        .map(|entry| String::from_utf8_lossy(entry.name.as_bytes()).into_owned())
        .collect())
}

#[tokio::test]
async fn a_shell_call_stages_its_output_on_the_journal_volume() -> Result<()> {
    use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};
    let waits = acyclic_native_runtime::obs::WaitReport::default();
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(waits.clone()).with(
            tracing_subscriber::fmt::layer()
                .with_filter(tracing_subscriber::filter::LevelFilter::WARN),
        ),
    );
    // Retain the exact admission the served tree derives its credentials from.
    // Minting another scope with the same grants does not identify that parent.
    struct Admission(std::sync::Mutex<Option<crate::core::Scope>>);
    impl crate::local::Verifier for Admission {
        fn caller(
            &self,
            custody: &crate::local::Custody<'_>,
            owner: &crate::core::Scope,
        ) -> Result<crate::hosting::VerifiedCaller> {
            let caller =
                crate::local::Verifier::caller(&crate::local::KeyFileVerifier, custody, owner)?;
            *self.0.lock().map_err(agent)? = Some(caller.scope.clone());
            Ok(caller)
        }
    }
    let admission = Arc::new(Admission(std::sync::Mutex::new(None)));
    let directory = tempfile::tempdir().map_err(agent)?;
    let provider = super::scripted([
        super::checked_commands::call(0, crate::tools::shell::PLATFORM.name, "echo staged"),
        crate::test_support::ScriptedTurn::Text("done".into()),
    ]);
    let mut config = super::checkout_config(directory.path(), (super::model()?, provider.clone()))?;
    config.served = Some(admission.clone());
    let harness = Arc::new(super::open_scripted(config).await?);
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let completion = harness.agents.get().unwrap().runtime.completion();
    let mut client = ObservedConversation::open(&harness).await?;
    let inbox = client.conversation.send("shell", "run one command").await?;
    let entry = tokio::select! {
        result = client.settled(inbox) => result,
        failure = completion => Err(Error::Conflict(format!("shell turn failed before settlement: {failure:?}"))),
    };
    let entry = entry.inspect_err(|_| {
        let _ = waits.report(&mut std::io::stderr());
    })?;
    assert!(
        entry.outcome.is_completed(),
        "shell turn did not complete: {:?}; model requests: {}",
        entry.outcome,
        provider.request_count()
    );
    let request = provider
        .request(1)
        .ok_or_else(|| Error::NotFound("shell result request".into()))?;
    let result = request
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id, content, ..
            } if call_id == "checkout-0" => Some(content),
            _ => None,
        })
        .ok_or_else(|| Error::NotFound("shell result".into()))?;
    assert!(
        !matches!(result, crate::model::ToolResultContent::Error { .. }),
        "shell failed: {result:?}"
    );
    let state = agents.state(&root).await.map_err(agent)?;
    // Each turn's calls share one directory, named by the turn.
    let turn = crate::agent::turn_id(&state, &entry.input).map_err(agent)?;
    let private = state
        .volumes
        .iter()
        .find(|binding| binding.name == crate::filesystem::PRIVATE_VOLUME)
        .ok_or_else(|| Error::NotFound("private volume".into()))?
        .volume()?;
    assert!(
        !names(&harness, private, "")
            .await?
            .iter()
            .any(|name| name == "tools")
    );
    let journal =
        crate::conversation::journal_volume(private.provider().clone(), &state.path.id())?;
    assert_eq!(entry.output.volume(), &journal);
    // Observe the published call directory at the settled cut rather than
    // reconstructing the executor's internal model-step identity.
    let issuer = harness
        .root_authority
        .as_ref()
        .ok_or_else(|| Error::NotFound("issuer".into()))?;
    let scope = issuer.bootstrap_scope(
        "output-inspection",
        &crate::Capabilities::new([
            journal.capability(crate::conversation::VolumeOperation::Read, &mut None)?
        ]),
        Some(crate::AgentId::from_bytes(state.path.id_bytes())),
        &mut None,
    )?;
    let directory = format!("tools/{turn}");
    let grant = crate::conversation::ContentGrant::verify_directory_read(
        &harness.verifier,
        &scope,
        &journal,
        &directory,
        &mut None,
    )?;
    let generation = harness.files.file_generation(&entry.output, None)?;
    let (_, page) = harness
        .files
        .list_private_directory(
            &journal,
            &grant,
            &directory,
            crate::filesystem::DirectoryOptions {
                generation: Some(&generation),
                after: None,
                maximum_entries: 2,
                ceiling: None,
            },
        )
        .await?
        .ok_or_else(|| {
            Error::NotFound(format!(
                "shell output directory {directory}; result: {result:?}"
            ))
        })?;
    assert!(!page.has_more, "unexpected additional model calls");
    let mut calls = Vec::new();
    for entry in &page.entries {
        let name = std::str::from_utf8(entry.name.as_bytes()).map_err(agent)?;
        let (_, children) = harness
            .files
            .list_private_directory(
                &journal,
                &grant,
                &format!("{directory}/{name}"),
                crate::filesystem::DirectoryOptions {
                    generation: Some(&generation),
                    after: None,
                    maximum_entries: 8,
                    ceiling: None,
                },
            )
            .await?
            .ok_or_else(|| Error::NotFound("published output directory".into()))?;
        assert!(!children.has_more, "unexpected additional call files");
        if children
            .entries
            .iter()
            .any(|entry| entry.name.as_bytes() == b"request.json")
        {
            calls.push(name);
        }
    }
    assert_eq!(calls.len(), 1, "one real shell call: {result:?}");
    let call = calls[0];
    let bound = harness.agents.get().unwrap().server().bound();
    for name in ["request.json", "result.json", "stdout/0000000000000000"] {
        let path = format!("tools/{turn}/{call}/{name}");
        let (bytes, _) = bound
            .read_tool_output(&state.path, &path, Some(&entry.output), 0, 1 << 20)
            .await
            .map_err(|error| agent(format!("read {path}: {error}; shell result: {result:?}")))?;
        assert!(!bytes.is_empty(), "empty {name}");
        if name.starts_with("stdout/") {
            assert!(String::from_utf8_lossy(&bytes).contains("staged"));
        }
    }
    let scope = admission
        .0
        .lock()
        .map_err(agent)?
        .clone()
        .ok_or_else(|| Error::NotFound("served admission".into()))?;
    follow_tool_output(&harness, &state.path, &scope).await
}

async fn observation(
    harness: &LocalHarness,
    authority: &crate::filesystem::AgentAuthority<'_>,
    path: &AgentPath,
) -> Result<()> {
    harness
        .files
        .agent_observation(
            authority,
            path,
            authority.private.volume()?,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            None,
        )
        .await
        .map(drop)
}

/// The agent's own scope yields a live observation of its tool output, and
/// none from a scope that cannot list and read all of `tools/`; revoking the
/// scope ends an open observation.
async fn follow_tool_output(
    harness: &Arc<LocalHarness>,
    root: &AgentPath,
    admitted: &crate::core::Scope,
) -> Result<()> {
    use acyclic_fs::access::{Action, Capability, Generations, Grant, ResourcePattern};
    let (agents, _) = harness.agents().await?;
    let state = agents.state(root).await.map_err(agent)?;
    let issuer = harness
        .root_authority
        .as_ref()
        .ok_or_else(|| Error::NotFound("issuer".into()))?;
    let (path, volumes) = (&state.path, &state.volumes);
    let runtime = &(*issuer).clone().into();
    let authority = move || {
        crate::filesystem::agent_authority(
            runtime,
            &crate::filesystem::TurnAdmission::LocalRoot,
            path,
            volumes,
            &[],
        )
    };
    // A structured scope of `actions` on `paths`, for the same agent.
    let structured = async |actions: &[Action], paths: &[&str]| -> Result<_> {
        let mut scoped = authority().await?;
        let capability = Capability {
            layers: vec![
                paths
                    .iter()
                    .map(|path| Grant {
                        actions: actions.iter().copied().collect(),
                        resource: ResourcePattern {
                            lineage_root: None,
                            volume: None,
                            generations: Generations::All,
                            path: (*path).into(),
                        },
                    })
                    .collect(),
            ],
            expires_at: None,
        };
        scoped.scope = scoped
            .issuer
            .native()
            .ok_or_else(|| Error::Invalid("native fixture custody required".into()))?
            .bootstrap_scope(
                "observer",
                &crate::Capabilities::new([capability.to_scope_label().map_err(agent)?]),
                scoped.scope.agent(),
                &mut None,
            )?;
        Ok(scoped)
    };
    // Negative control: the same agent, attenuated to no grants, gets none.
    let mut denied = authority().await?;
    denied.scope = denied.scope.attenuate_workload(
        "denied",
        &crate::Capabilities::new(Vec::<String>::new()),
        denied.scope.agent(),
        &issuer.public_workload_trust()?,
        &crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: state.path.id().as_str().to_owned(),
        },
        acyclic_stream::UnixMillisClock::now_unix_millis(&acyclic_stream::SystemUnixMillisClock)
            / 1000,
    )?;
    assert!(
        observation(harness, &denied, path).await.is_err(),
        "a scope without read on tools/ yields no credential"
    );
    // A structured scope is held to its compiled view: writing everywhere
    // reads nothing, and reading only `tools/` is enough.
    let writer = structured(&[Action::Create, Action::Write], &["**"]).await?;
    assert!(
        observation(harness, &writer, path).await.is_err(),
        "a write-only structured scope yields no credential"
    );
    let reader = structured(&[Action::List, Action::Read], &["tools", "tools/**"]).await?;
    observation(harness, &reader, path).await?;
    // From after the settled turn: its output stays the newest call's.
    let mut observer = conversation(harness).await?.observe(Some(1)).await?;
    loop {
        if matches!(observer.next().await?, super::super::Event::Delta) {
            observer.absorb().await?;
            if observer.live().1.contains("staged") {
                break;
            }
        }
    }
    // Revoking the agent scope the connection's credential derives from
    // ends its observation.
    // Negative control: an unrelated root with identical grants cannot close it.
    let unrelated = authority().await?;
    unrelated
        .issuer
        .native()
        .ok_or_else(|| Error::Invalid("native fixture custody required".into()))?
        .revoke_workload(&unrelated.scope)?;
    issuer.verifier(&mut None)?.verify(admitted, &mut None)?;
    issuer.revoke_workload(admitted)?;
    loop {
        if observer.next().await.is_err() {
            return Ok(());
        }
    }
}

/// A turn whose `UserPromptSubmit` hook updated its project counter is interrupted
/// after the hook's outcome was recorded; the reopened agent replays the
/// turn from `hooks/{turn}/{op}.json`, without updating the counter again,
/// and the model sees the identical recorded context.
#[cfg(unix)]
#[tokio::test]
async fn a_reopened_turn_replays_its_recorded_hook_outcome() -> Result<()> {
    let directory = tempfile::tempdir().map_err(agent)?;
    let home = directory.path().join("home");
    std::fs::create_dir_all(home.join(".claude")).map_err(agent)?;
    // The original workspace-write sandbox intentionally isolates networking.
    let hook = r#"cat >/dev/null; n=0; if test -f hook-count; then read -r n < hook-count; fi; n=$((n + 1)); printf '%s\n' "$n" > hook-count; printf 'HOOK-RUN-%s\n' "$n""#;
    std::fs::write(
        home.join(".claude/settings.json"),
        serde_json::to_vec(
            &serde_json::json!({"hooks": {"UserPromptSubmit": [{"hooks": [
                {"type": "command", "shell": "sh", "command": hook}
            ]}]}}),
        )
        .map_err(agent)?,
    )
    .map_err(agent)?;
    let workspace = directory.path().join("work");
    std::fs::create_dir_all(&workspace).map_err(agent)?;
    let open = |provider: Arc<ScriptedModelProvider>| {
        let (workspace, home) = (workspace.clone(), home.clone());
        let state = directory.path().join("state");
        async move {
            let config = super::checkout_config(&workspace, (super::model()?, provider))?
                .state_directory(state)
                .settings(
                    crate::native_settings::SettingsConfig::new(&workspace, &home)
                        .codex_home(home.join(".codex")),
                );
            Ok::<_, Error>(Arc::new(super::open_scripted(config).await?))
        }
    };
    let (dispatched, mut requests) = tokio::sync::mpsc::channel(1);
    let pending = Arc::new(
        ScriptedModelProvider::new([crate::test_support::ScriptedTurn::Events(vec![
            crate::test_support::ScriptedEvent::Pending {},
        ])])
        .with_context_capacity(super::capacity())
        .with_dispatch_observer(move |_| {
            let _ = dispatched.try_send(());
            Box::pin(async { Ok(()) })
        }),
    );
    let harness = open(pending.clone()).await?;
    let mut observed = ObservedConversation::open(&harness).await?;
    let completion = harness.agents.get().unwrap().runtime.completion();
    let inbox = observed.conversation.send("hooked", "hello").await?;
    // A turn that settles before model dispatch has already failed.
    tokio::select! {
        request = requests.recv() => request.ok_or_else(|| Error::Storage("model dispatch producer closed".into())),
        entry = observed.settled(inbox) => Err(Error::Conflict(format!("hooked turn settled before model dispatch: {entry:?}"))),
        failure = completion => Err(Error::Conflict(format!("hooked turn failed before model dispatch: {failure:?}"))),
    }?;
    let request = pending
        .request(0)
        .ok_or_else(|| Error::NotFound("hook model request".into()))?;
    let recorded = super::request_text(&harness, &request).await?;
    assert!(recorded.contains("HOOK-RUN-1"), "{recorded}");
    let project = crate::filesystem::workspace_ref(
        harness.project.provider().clone(),
        &harness.project.storage_name(&mut None)?,
    )?;
    assert_eq!(
        harness
            .files
            .read(&project, None, "/hook-count", 16, None)
            .await?
            .as_ref(),
        b"1\n",
    );
    // Crash: the turn is still waiting on the model when the harness drops.
    drop(observed);
    drop(harness);
    // The interrupted turn keeps its pinned configuration even when the live
    // settings remove the hook before its owner reopens.
    std::fs::write(home.join(".claude/settings.json"), br#"{"hooks":{}}"#).map_err(agent)?;
    let resumed = Arc::new(
        ScriptedModelProvider::new([crate::test_support::ScriptedTurn::Text("done".into())])
            .with_context_capacity(super::capacity())
            .with_reconciliations([crate::test_support::ScriptedReconciliation {
                orphan: true,
                observed: Vec::new(),
                continuation: Some(vec![
                    serde_json::from_value(serde_json::json!({"kind": "content", "delta": "done"}))
                        .map_err(agent)?,
                    serde_json::from_value(
                        serde_json::json!({"kind": "completed", "metadata": {"finish": "stop"}}),
                    )
                    .map_err(agent)?,
                ]),
            }]),
    );
    let harness = open(resumed.clone()).await?;
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let entries = settled(&harness, &root).await?;
    assert_eq!(entries.len(), 1);
    // The replay ran no hook: the retained project counter is unchanged.
    assert_eq!(
        harness
            .files
            .read(&project, None, "/hook-count", 16, None)
            .await?
            .as_ref(),
        b"1\n",
    );
    // A re-dispatched request carries the identical recorded context.
    if let Some(request) = resumed.request(0) {
        let replayed = super::request_text(&harness, &request).await?;
        assert!(
            replayed.contains("HOOK-RUN-1") && !replayed.contains("HOOK-RUN-2"),
            "{replayed}"
        );
    }
    // The outcome is the one file recorded under the turn, beside its marker.
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let state = agents.state(&root).await.map_err(agent)?;
    let turn = crate::agent::turn_id(&state, &entry.input).map_err(agent)?;
    let private = state
        .volumes
        .iter()
        .find(|binding| binding.name == crate::filesystem::PRIVATE_VOLUME)
        .ok_or_else(|| Error::NotFound("private volume".into()))?
        .volume()?;
    // Configuration is a host publication on the journal; replay selects the
    // original receipt, not the now-empty live settings or the private volume.
    let bound = harness.agents.get().unwrap().server().bound();
    let journal = crate::conversation::journal_volume(private.provider().clone(), &root.id())?;
    let scope = harness
        .root_authority
        .as_ref()
        .expect("hook fixture owns its signing custody")
        .bootstrap_scope(
            "hook-snapshot-inspection",
            &crate::Capabilities::new([
                crate::contract::capability::HARNESS_HOST.to_owned(),
                journal.capability(crate::conversation::VolumeOperation::Read, &mut None)?,
                journal.capability(crate::conversation::VolumeOperation::Write, &mut None)?,
            ]),
            Some(crate::AgentId::from_bytes(root.id_bytes())),
            &mut None,
        )?;
    let content = crate::runtime::ContentBindings {
        reader: crate::filesystem::FilesystemContentVerifier::new(
            harness.files.clone(),
            &harness.verifier,
            &scope,
            harness.limits.file_bytes,
            None,
        )?,
        writer: Some(crate::filesystem::FilesystemContentPublisher::new(
            harness.files.clone(),
            &journal,
            &harness.verifier,
            &scope,
            harness.limits.file_bytes,
            None,
        )?),
        volume: journal,
    };
    let configuration_operation = crate::local::context::publication_operation(
        b"acyclic.local.turn-hooks/v1\0",
        &turn.into_bytes(),
    );
    let (configuration, _) = content
        .writer
        .as_ref()
        .ok_or_else(|| Error::NotFound("hook journal publisher".into()))?
        .observe_staged_once(
            configuration_operation,
            None,
            harness.limits.file_bytes,
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
    let configuration =
        configuration.ok_or_else(|| Error::NotFound("original hook configuration".into()))?;
    assert_eq!(configuration.volume(), &content.volume);
    assert_ne!(configuration.volume(), private);
    assert_eq!(configuration.path(), format!("config/hooks/{turn}.json"));
    let bytes = content
        .reader
        .read(
            &configuration,
            configuration.descriptor().byte_length(),
            None,
            None,
        )?
        .await?;
    assert!(
        !serde_json::from_slice::<crate::hooks::Hooks>(&bytes)
            .map_err(agent)?
            .is_empty()
    );
    assert!(matches!(
        bound.read_file(&root, &configuration, 0, configuration.descriptor().byte_length()).await,
        Err(crate::agent::Error::Turn(error)) if matches!(*error, Error::Unauthorized(_))
    ));
    assert_eq!(names(&harness, private, "hooks").await?, [turn.to_string()]);
    let recorded = names(&harness, private, &format!("hooks/{turn}")).await?;
    let [outcome, marker] = recorded.as_slice() else {
        return Err(Error::Conflict(format!(
            "one hook outcome, got {recorded:?}"
        )));
    };
    assert_eq!(
        outcome.strip_suffix(".json"),
        marker.strip_suffix(".started")
    );
    Ok(())
}

/// The files an attachment manifest lists, read through `harness`'s reader.
async fn listed(
    harness: &LocalHarness,
    manifest: &crate::conversation::FileRef,
) -> Result<Vec<crate::conversation::FileRef>> {
    let reader = harness.client_reader()?;
    let bytes = reader
        .read(manifest, manifest.descriptor().byte_length(), None, None)?
        .await?;
    Ok(
        crate::conversation::decode_attachment_manifest(manifest, bytes.as_ref(), None, None)?
            .into_iter()
            .map(|item| item.file)
            .collect(),
    )
}

/// A harness over a fresh checkout holding `files`, written after it opens.
async fn checkout_with(
    files: &[(String, Vec<u8>)],
) -> Result<(
    tempfile::TempDir,
    Arc<LocalHarness>,
    Arc<ScriptedModelProvider>,
)> {
    checkout_scripted(files, text()).await
}

async fn checkout_scripted(
    files: &[(String, Vec<u8>)],
    provider: Arc<ScriptedModelProvider>,
) -> Result<(
    tempfile::TempDir,
    Arc<LocalHarness>,
    Arc<ScriptedModelProvider>,
)> {
    let directory = tempfile::tempdir().map_err(agent)?;
    std::fs::create_dir(directory.path().join(".git")).map_err(agent)?;
    let harness = Arc::new(
        super::open_scripted(super::checkout_config(
            directory.path(),
            (super::model()?, provider.clone()),
        )?)
        .await?,
    );
    let checkout = &harness.workspace.as_ref().expect("checkout").path;
    let mut previous_parent = Some(Path::new(""));
    for (name, bytes) in files {
        let parent = Path::new(name).parent();
        let path = checkout.join(name);
        if parent != previous_parent {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(agent)?;
            }
            previous_parent = parent;
        }
        std::fs::write(path, bytes).map_err(agent)?;
    }
    Ok((directory, harness, provider))
}

/// Archive releases a physical view through the original closing composition,
/// while an idle sibling, root checkout and retained child history survive.
#[tokio::test]
async fn physical_archive_releases_only_the_closed_binding_and_retains_history() -> Result<()> {
    use acyclic_harness_client::{AgentCommand, AgentReply, ReadTarget};
    let directory = tempfile::tempdir().map_err(agent)?;
    std::fs::create_dir(directory.path().join(".git")).map_err(agent)?;
    let provider = super::scripted(
        ["first", "second"].map(|text| crate::test_support::ScriptedTurn::Text(text.into())),
    );
    let harness = Arc::new(
        super::open_scripted(
            super::checkout_config(directory.path(), (super::model()?, provider))?
                // This control exercises physical ownership, without invoking tools.
                .without_default_tools()
                .workspace_mounting(crate::local::WorkspaceMounting::Physical),
        )
        .await?,
    );
    let workspace = harness.workspace.as_ref().expect("physical checkout");
    std::fs::write(workspace.path.join("kept.txt"), b"inherited").map_err(agent)?;
    let (agents, root) = harness.agents().await?;
    let root = root.clone();
    let connection = harness.connect().await?;
    let mut children = Vec::new();
    for name in ["archived", "idle"] {
        let AgentReply::Spawned { agent: child, .. } = connection
            .execute(&AgentCommand::Spawn {
                parent: root.clone(),
                id: crate::agent::IntentId::try_from(name).map_err(agent)?,
                brief: crate::model::ModelContent::Text(name.into()),
                role: "worker".into(),
            })
            .await?
        else {
            return Err(Error::Invalid("spawn did not return its child".into()));
        };
        let history = settled(&harness, &child).await?;
        let state = agents.state(&child).await.map_err(agent)?;
        let first = usize::try_from(state.conversation_base).map_err(agent)?;
        let entry =
            ConversationEntry::decode(history.get(first).expect("child's original brief turn"))
                .map_err(agent)?;
        assert!(matches!(&entry.input, TurnInput::Brief { .. }), "{entry:?}");
        assert_eq!(
            entry.outcome,
            crate::agent::TurnOutcome::Completed,
            "child {name} must complete its original model turn before archive: {entry:?}"
        );
        let project = state
            .volumes
            .iter()
            .find(|binding| binding.name == "project")
            .expect("child project");
        let id = workspace
            .filesystem
            .workspace_id(project.volume()?.storage_name(&mut None)?)
            .map_err(agent)?;
        let path = workspace
            .held_task_mount_root()?
            .path()
            .join(".physical")
            .join(hex::encode(id.into_bytes()));
        assert_eq!(
            std::fs::read(path.join("kept.txt")).map_err(agent)?,
            b"inherited"
        );
        children.push((child, path, history, state.volumes, entry));
    }
    let (child, path, history, bindings, entry) = &children[0];
    agents
        .archive(
            child,
            &crate::agent::Sender::User {
                principal: crate::approvals::HUMAN_APPROVER.into(),
            },
            &crate::agent::IntentId::try_from("close-physical").map_err(agent)?,
        )
        .await
        .map_err(agent)?;
    settled(&harness, child).await?;
    assert!(agents.state(child).await.map_err(agent)?.closed);
    assert!(
        !path.try_exists().map_err(agent)?,
        "archived physical binding survived its committed close"
    );
    assert_eq!(
        records(agents.provider(), child.conversation().map_err(agent)?).await?,
        *history
    );
    let output = Conversation::read(
        &connection,
        child,
        ReadTarget::File {
            file: entry.output.clone(),
        },
    )
    .await?;
    let output: crate::executor::TurnOutput = serde_json::from_slice(&output).map_err(agent)?;
    assert_eq!(output.text, "first", "{entry:?}; {output:?}");
    assert_eq!(
        std::fs::read(children[1].1.join("kept.txt")).map_err(agent)?,
        b"inherited"
    );
    assert_eq!(
        std::fs::read(workspace.path.join("kept.txt")).map_err(agent)?,
        b"inherited"
    );
    // The same volume owner completes crash/retry cleanup without reopening a
    // logical volume or taking custody of the idle sibling's directory.
    workspace.release_physical_bindings(bindings)?;
    assert!(children[1].1.is_dir());
    Ok(())
}

#[tokio::test]
async fn a_prompt_attaches_the_checkout_files_it_mentions_once_each() -> Result<()> {
    use crate::model::{FileProjectionPolicy, ModelContent, ModelContentPart};
    const PIXEL: &[u8] = &[
        0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 0x0d, 0x49, 0x48, 0x44, 0x52, 0,
        0, 0, 1, 0, 0, 0, 1, 8, 4, 0, 0, 0, 0xb5, 0x1c, 0x0c, 0x02, 0, 0, 0, 0x0b, 0x49, 0x44,
        0x41, 0x54, 0x78, 0xda, 0x63, 0xfc, 0xff, 0x1f, 0, 0x03, 0x03, 0x02, 0, 0xef, 0x9a, 0x5e,
        0x4f, 0, 0, 0, 0, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
    ];
    let (_directory, harness, provider) = checkout_with(&[
        ("mention.txt".into(), b"MENTIONED\n".to_vec()),
        ("pixel.png".into(), PIXEL.to_vec()),
    ])
    .await?;
    let checkout = harness.workspace.as_ref().expect("checkout").path.clone();
    let mut observed = ObservedConversation::open(&harness).await?;
    let prompt = "inspect @mention.txt and @pixel.png then @mention.txt again";
    // A capture for one message replays its pinned manifest exactly.
    let first = harness.prompt_attachments(b"direct", Some(prompt)).await?;
    assert_eq!(
        harness.prompt_attachments(b"direct", Some(prompt)).await?,
        first
    );
    let inbox = observed.conversation.send("mention", prompt).await?;
    let entry = observed.settled(inbox).await?;
    assert_eq!(
        entry.outcome,
        crate::agent::TurnOutcome::Completed,
        "the real mention must reach and settle its model request"
    );
    let client = observed.conversation;
    // Later publications on the private volume leave a replay unchanged.
    assert_eq!(
        harness.prompt_attachments(b"direct", Some(prompt)).await?,
        first
    );
    let TurnInput::Message {
        attachments: Some(manifest),
        body,
        ..
    } = &entry.input
    else {
        return Err(Error::Conflict(
            "the prompt started no attached message turn".into(),
        ));
    };
    // Each mentioned file attaches once, in first-mention order, pinned.
    let reader = harness.client_reader()?;
    let mut read = Vec::new();
    for file in listed(&harness, manifest).await? {
        read.push((
            file.path()
                .rsplit('/')
                .next()
                .unwrap_or_default()
                .to_owned(),
            file.descriptor().media_type().to_owned(),
            reader.read(&file, 1 << 20, None, None)?.await?.to_vec(),
        ));
    }
    assert_eq!(
        read,
        [
            (
                super::super::context::copy_name("mention.txt"),
                "text/plain".into(),
                b"MENTIONED\n".to_vec()
            ),
            (
                super::super::context::copy_name("pixel.png"),
                "image/png".into(),
                PIXEL.to_vec()
            ),
        ]
    );
    // The model sees the prompt, then the text file as text and the image natively.
    let request = provider.last_request().expect("one model request");
    let ModelContent::Parts(parts) = input_content(&request, body)? else {
        return Err(Error::Conflict("the turn input has no attachments".into()));
    };
    let policies = parts
        .iter()
        .map(|part| match part {
            ModelContentPart::File { policy, .. } => {
                Some(matches!(policy, FileProjectionPolicy::Native(_)))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(policies, [Some(false), Some(false), Some(true)]);
    // A resend after its files changed or vanished replays the pinned
    // manifest, so it still matches the delivered message exactly.
    std::fs::write(checkout.join("mention.txt"), "CHANGED\n").map_err(agent)?;
    std::fs::remove_file(checkout.join("pixel.png")).map_err(agent)?;
    assert_eq!(
        harness.prompt_attachments(b"direct", Some(prompt)).await?,
        first
    );
    assert_eq!(client.send("mention", prompt).await?, inbox);
    // A new message sees the files as they are now.
    let fresh = harness
        .prompt_attachments(b"fresh", Some("inspect @mention.txt"))
        .await?
        .expect("a manifest");
    let fresh = listed(&harness, &fresh).await?;
    assert_eq!(
        reader.read(&fresh[0], 1 << 20, None, None)?.await?.to_vec(),
        b"CHANGED\n"
    );
    Ok(())
}

#[tokio::test]
async fn a_replayed_command_answers_from_its_first_delivery_after_the_command_is_gone() -> Result<()>
{
    let (_directory, harness, provider) = checkout_with(&[
        (
            ".claude/commands/marker.md".into(),
            b"MARKER $ARGUMENTS with @note.txt
"
            .to_vec(),
        ),
        (
            "note.txt".into(),
            b"NOTE
"
            .to_vec(),
        ),
    ])
    .await?;
    let checkout = harness.workspace.as_ref().expect("checkout").path.clone();
    let mut observed = ObservedConversation::open(&harness).await?;
    // A client-removed builtin must fail prompt admission, just like any
    // unknown slash command, without spending a model request.
    let before = provider.request_count();
    for command in ["/view", "/help", "/no-such-command"] {
        assert!(observed.conversation.send(command, command).await.is_err());
    }
    assert_eq!(provider.request_count(), before);
    let inbox = observed.conversation.send("marker", "/marker now").await?;
    let entry = observed.settled(inbox).await?;
    let client = observed.conversation;
    // The command, the skill catalog and the mentioned file are all gone.
    std::fs::remove_dir_all(checkout.join(".claude")).map_err(agent)?;
    std::fs::remove_file(checkout.join("note.txt")).map_err(agent)?;
    // Negative control: a new delivery reads the project as it is now.
    assert!(client.send("fresh", "/marker now").await.is_err());
    // The replay is the first delivery, answered from its committed record.
    assert_eq!(client.send("marker", "/marker now").await?, inbox);
    let Reply::Answered {
        entry: replayed, ..
    } = client.reply("marker", inbox).await?
    else {
        return Err(Error::Conflict("the replay lost its saved answer".into()));
    };
    assert_eq!(replayed, entry);
    Ok(())
}

#[tokio::test]
async fn a_prompt_past_two_frames_is_accepted_and_reads_back_exactly() -> Result<()> {
    use acyclic_harness_client::{AgentCommand, MAX_AGENT_COMMAND_BYTES};
    let (_directory, harness, _provider) = checkout_with(&[]).await?;
    let root = harness.agents().await?.1.clone();
    let mut observed = ObservedConversation::open(&harness).await?;
    let client = Arc::clone(&observed.conversation);
    // Cross two complete frames: staging needs both continuation and final
    // parts, and the command names them by digest.
    let prompt = "0123456789abcdef\n".repeat(2 * MAX_AGENT_COMMAND_BYTES / 17 + 1);
    assert!(prompt.len() > 2 * MAX_AGENT_COMMAND_BYTES);
    let inbox = client.send("large", &prompt).await?;
    let entry = observed.settled(inbox).await?;
    let read = client.turn(&entry).await?.prompt;
    assert!(read.as_deref() == Some(prompt.as_str()));
    // A resend is the same delivery.
    assert_eq!(client.send("large", &prompt).await?, inbox);
    // Negative controls: a head is refused when its parts were never staged,
    // and when the staged parts do not match its digest.
    let connection = harness.connect().await?;
    let digest = blake3::hash(b"{}").to_hex().to_string();
    let head = |digest: &str| AgentCommand::Staged {
        agent: root.clone(),
        command: "prompt".into(),
        digest: digest.to_owned(),
        parts: 1,
        bytes: 2,
    };
    assert!(connection.execute(&head(&digest)).await.is_err());
    connection
        .execute(&AgentCommand::Stage {
            agent: root.clone(),
            digest: digest.clone(),
            part: 0,
            text: "[]".into(),
        })
        .await?;
    assert!(connection.execute(&head(&digest)).await.is_err());
    Ok(())
}

#[tokio::test]
async fn physical_siblings_use_the_original_shared_file_binding() -> Result<()> {
    use crate::model::{ModelContent, ModelEvent, ToolCall};
    use crate::test_support::{ScriptedEvent, ScriptedTurn};
    use acyclic_harness_client::{AgentCommand, AgentReply};
    let call = |name: &str, arguments| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                name.into(),
                name.into(),
                arguments,
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::json!({}),
            }),
        ])
    };
    let provider = super::scripted([
        call(
            "write",
            serde_json::json!({"path":"volume:///shared/board.txt", "content":"shared-board-sentinel-73dae"}),
        ),
        ScriptedTurn::Text("writer settled".into()),
        call(
            "shell",
            serde_json::json!({"command":"pwd", "cwd":"missing-cwd"}),
        ),
        call(
            "read",
            serde_json::json!({"path":"volume:///shared/board.txt"}),
        ),
        ScriptedTurn::Text("reader settled".into()),
    ]);
    let directory = tempfile::tempdir().map_err(agent)?;
    let harness = Arc::new(
        super::open_scripted(
            super::checkout_config(directory.path(), (super::model()?, provider.clone()))?
                .workspace_mounting(crate::local::WorkspaceMounting::Physical)
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let connection = harness.connect().await?;
    let root = connection.root().clone();
    for (id, brief) in [
        ("writer", "write the shared board"),
        ("reader", "read the shared board"),
    ] {
        let AgentReply::Spawned { agent: child, .. } = connection
            .execute(&AgentCommand::Spawn {
                parent: root.clone(),
                id: crate::agent::IntentId::try_from(id).map_err(agent)?,
                brief: ModelContent::Text(brief.into()),
                role: "worker".into(),
            })
            .await?
        else {
            return Err(Error::Invalid("shared-board spawn refused".into()));
        };
        // Creation's initial idle status is not the brief's completion.
        // Wait for the child's actual turn record through its served follow.
        let conversation = Conversation::open(harness.attach(Some(child.clone()), true).await?);
        let mut events = conversation.observe(Some(0)).await?;
        let entry = loop {
            match events.next().await? {
                crate::local::Event::Turn(entry) => break entry,
                crate::local::Event::Status(status)
                    if matches!(status.phase, crate::agent::Phase::Done { .. }) =>
                {
                    return Err(Error::Conflict(format!(
                        "shared-board child ended before its turn: {status:?}; model_requests={}",
                        provider.request_count()
                    )));
                }
                _ => {}
            }
        };
        assert!(
            matches!(entry.outcome, crate::agent::TurnOutcome::Completed),
            "{id}: {:?}; model_requests={}",
            entry.outcome,
            provider.request_count()
        );
        let mut inspection = conversation.inspect().await?;
        while !inspection.snapshot().synchronized {
            inspection.advance().await?;
        }
        assert_eq!(inspection.snapshot().tools.completed, 1);
        assert_eq!(
            inspection.snapshot().tools.failures.values().sum::<u64>(),
            u64::from(id == "reader"),
            "inspection must count the original missing-cwd refusal once"
        );
    }
    assert_eq!(provider.request_count(), 5);
    let request = provider
        .request(4)
        .ok_or_else(|| Error::NotFound("shared-board read request".into()))?;
    let refused = request
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id, content, ..
            } if call_id == "shell" => Some(content),
            _ => None,
        })
        .ok_or_else(|| Error::NotFound("missing-cwd tool result".into()))?;
    assert!(matches!(
        refused,
        crate::model::ToolResultContent::Error { .. }
    ));
    assert!(
        matches!(refused, crate::model::ToolResultContent::Error { message }
        if message == "tool execution failed: cwd missing-cwd does not exist")
    );
    let result = request
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            crate::model::ModelContentPart::ToolResult {
                call_id, content, ..
            } if call_id == "read" => Some(content),
            _ => None,
        })
        .ok_or_else(|| Error::NotFound("shared-board read result".into()))?;
    assert!(
        !matches!(result, crate::model::ToolResultContent::Error { .. }),
        "shared-board read failed: {result:?}"
    );
    let observed = serde_json::to_string(result).map_err(agent)?;
    assert!(
        observed.contains("shared-board-sentinel-73dae"),
        "the sibling did not read the original shared write: {observed}"
    );
    Ok(())
}

#[tokio::test]
async fn parent_and_child_context_and_writes_retain_their_own_volumes() -> Result<()> {
    use crate::model::{ModelContent, ModelContentPart, ModelEvent, ToolCall};
    use crate::test_support::{ScriptedEvent, ScriptedTurn};
    use acyclic_harness_client::{AgentCommand, AgentReply, ReadTarget};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};

    // Count the original native provider boundary, including the real command
    // admission. A settlement must add only its one actor observation.
    struct HeadReads(Arc<AtomicUsize>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for HeadReads {
        fn on_new_span(
            &self,
            attributes: &tracing::span::Attributes<'_>,
            _: &tracing::span::Id,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if attributes.metadata().name() == "acyclic.stream.heads" {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    let head_reads = Arc::new(AtomicUsize::new(0));
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::registry()
            .with(HeadReads(Arc::clone(&head_reads)))
            .with(
                tracing_subscriber::fmt::layer()
                    .with_test_writer()
                    .with_filter(tracing_subscriber::filter::LevelFilter::WARN),
            ),
    );

    let write = |text: &str| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                "write-seed".into(),
                "write".into(),
                serde_json::json!({
                    "path": "seed.txt", "content": text,
                }),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::json!({}),
            }),
        ])
    };
    let provider = super::scripted([
        write("child"),
        ScriptedTurn::Text("child done".into()),
        write("parent"),
        ScriptedTurn::Text("parent done".into()),
    ]);
    // Model projections carry a path and opaque version. The authoritative
    // FileRef belongs to the original ToolCompleted journal observation.
    async fn publication(
        connection: &crate::local::Connection,
        owner: &AgentPath,
        mut input: u64,
    ) -> Result<crate::conversation::FileRef> {
        let mut after = 0;
        loop {
            let AgentReply::Events {
                input: selected,
                page: Some(page),
                ..
            } = connection
                .execute(&AgentCommand::Events {
                    agent: owner.clone(),
                    input,
                    after,
                    wait: false,
                })
                .await?
            else {
                return Err(Error::NotFound("write observation".into()));
            };
            for record in page.items {
                if matches!(record.event, crate::executor::ExecutionEvent::ToolCompleted { ref call_id, .. } if call_id == "write-seed")
                {
                    let bytes = Conversation::read(
                        connection,
                        owner,
                        ReadTarget::Execution {
                            input: selected,
                            sequence: record.sequence,
                            field: crate::executor::ExecutionPayloadField::Result,
                        },
                    )
                    .await?;
                    let result: crate::tool::ToolResult =
                        serde_json::from_slice(&bytes).map_err(agent)?;
                    return Ok(serde_json::from_value::<crate::tool::files::FileResult>(
                        result.value,
                    )
                    .map_err(agent)?
                    .file);
                }
            }
            let Some(next) = page.next else {
                return Err(Error::NotFound("write completion".into()));
            };
            assert!(next > after, "execution page did not advance");
            (input, after) = (selected, next);
        }
    }
    let catalog = |index| -> Result<Vec<crate::context::SkillMetadata>> {
        let request = provider
            .request(index)
            .ok_or_else(|| Error::NotFound("context request".into()))?;
        request
            .messages
            .iter()
            .flat_map(|message| {
                let text = match &message.content {
                    ModelContent::Text(text) => Some(text.as_str()),
                    _ => None,
                };
                text.into_iter()
                    .chain(
                        message
                            .content
                            .parts()
                            .iter()
                            .filter_map(|part| match part {
                                ModelContentPart::Text { text, .. } => Some(text.as_str()),
                                _ => None,
                            }),
                    )
            })
            .find_map(|text| {
                serde_json::from_str::<Vec<crate::context::SkillMetadata>>(text)
                    .ok()
                    .filter(|skills| !skills.is_empty())
            })
            .ok_or_else(|| Error::NotFound("model-visible skill catalog".into()))
    };
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), project.clone(), provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let project_workspace = crate::filesystem::workspace_ref(
        project.provider().clone(),
        &project.storage_name(&mut None)?,
    )?;
    harness
        .files
        .apply(
            &project_workspace,
            None,
            &[
                crate::filesystem::WorkspaceMutation::CreateDirectory {
                    path: "/.agents/skills/context-checkpoint".into(),
                },
                crate::filesystem::WorkspaceMutation::PutFile {
                    path: "/.agents/skills/context-checkpoint/SKILL.md".into(),
                    bytes: acyclic_stream::OwnedBytes::from_static(
                        b"---\nname: context-checkpoint\ndescription: Project-specific context\n---\nproject procedure",
                    ),
                },
            ],
            &crate::IdempotencyKey::new("project-skill")?,
        )
        .await?;
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let connection = harness.connect().await?;
    let AgentReply::Spawned { agent: child, .. } = connection
        .execute(&AgentCommand::Spawn {
            parent: root.clone(),
            id: crate::agent::IntentId::try_from("isolated-write").map_err(agent)?,
            brief: ModelContent::Text("write child seed".into()),
            role: "worker".into(),
        })
        .await?
    else {
        return Err(Error::Invalid("spawn did not return its child".into()));
    };
    let settlement = settled(&harness, &child).await.map(|_| ());
    if let Err(error) = settlement {
        return Err(Error::Conflict(format!(
            "child write did not settle: {error}; model_requests={}; child_state={:?}",
            provider.request_count(),
            agents.state(&child).await,
        )));
    }
    assert_eq!(provider.request_count(), 2);
    let child_file = publication(&connection, &child, 0).await?;
    let child_state = agents.state(&child).await.map_err(agent)?;
    let child_project = child_state
        .volumes
        .iter()
        .find(|binding| binding.name == "project")
        .ok_or_else(|| Error::NotFound("child project volume".into()))?
        .volume()?;
    assert_ne!(child_project, &project);
    assert_eq!(child_file.volume(), child_project);
    let child_scratch = child_state
        .volumes
        .iter()
        .find(|binding| binding.name == "home")
        .ok_or_else(|| Error::NotFound("child scratch volume".into()))?
        .volume()?;
    assert_ne!(child_scratch, &harness.scratch);
    let child_skills = catalog(0)?;
    let project_skill = child_skills
        .iter()
        .find(|skill| {
            skill.name == "context-checkpoint" && &skill.source.root.volume == child_project
        })
        .ok_or_else(|| Error::NotFound("child project skill".into()))?;
    assert_eq!(&project_skill.source.root.volume, child_project);
    assert_eq!(
        child_skills
            .iter()
            .filter(|skill| skill.name == "context-checkpoint")
            .count(),
        2,
        "configured and built-in sources remain separate pinned entries",
    );
    // Built-in skills are read in place from the one shared seed volume.
    let seed = &harness.seed.0;
    assert!(
        child_skills.iter().any(|skill| {
            skill.name == "context-checkpoint" && &skill.source.root.volume == seed
        })
    );
    assert!(
        child_skills
            .iter()
            .filter(|skill| skill.name != "context-checkpoint")
            .all(|skill| &skill.source.root.volume == seed)
    );

    let mut parent = ObservedConversation::open(&harness).await?;
    let parent_input = parent
        .conversation
        .send("parent-write", "write parent seed")
        .await?;
    parent.settled(parent_input).await?;
    assert_eq!(provider.request_count(), 4);
    let parent_file = publication(&connection, &root, parent_input).await?;
    assert_eq!(parent_file.volume(), &project);
    // The second agent must not reuse the first agent's cached private pins.
    let parent_skills = catalog(2)?;
    let project_skill = parent_skills
        .iter()
        .find(|skill| skill.name == "context-checkpoint" && skill.source.root.volume == project)
        .ok_or_else(|| Error::NotFound("parent project skill".into()))?;
    assert_eq!(&project_skill.source.root.volume, &project);
    assert!(
        parent_skills.iter().any(|skill| {
            skill.name == "context-checkpoint" && &skill.source.root.volume == seed
        })
    );
    assert!(
        parent_skills
            .iter()
            .filter(|skill| skill.name != "context-checkpoint")
            .all(|skill| &skill.source.root.volume == seed)
    );
    // Project skills differ: the child reads its own forked project.
    assert_ne!(child_skills, parent_skills);
    for command in ["/context-checkpoint", "/CONTEXT-CHECKPOINT"] {
        assert!(matches!(
            harness.resolve_prompt_command(command).await,
            Err(Error::Conflict(_))
        ));
    }
    // An unambiguous name must still expand in the same catalog.
    assert!(
        harness
            .resolve_prompt_command("/candidate-search")
            .await?
            .is_some()
    );
    for (owner, file, expected) in [
        (&child, &child_file, b"child".as_slice()),
        (&root, &parent_file, b"parent".as_slice()),
    ] {
        assert_eq!(
            Conversation::read(&connection, owner, ReadTarget::File { file: file.clone() }).await?,
            expected,
        );
    }
    // The parent's later write cannot retarget the child's retained result.
    assert!(
        Conversation::read(&connection, &child, ReadTarget::File { file: parent_file })
            .await
            .is_err()
    );
    // Open the root observer after both command-cost checks, so registering its
    // follows cannot contribute to the child's provider count.
    for owner in [&child, &root] {
        let before = head_reads.load(Ordering::Relaxed);
        connection.execute(&AgentCommand::Root {}).await?;
        let admission_heads = head_reads.load(Ordering::Relaxed) - before;
        let before = head_reads.load(Ordering::Relaxed);
        let state = agents.state(owner).await.map_err(agent)?;
        let state_heads = head_reads.load(Ordering::Relaxed) - before;
        assert!(
            state_heads > 0,
            "native checkpoint observation was not counted"
        );
        let before = head_reads.load(Ordering::Relaxed);
        let observed = connection
            .execute(&AgentCommand::Settlement {
                agent: owner.clone(),
                wait: false,
            })
            .await?;
        let observed_heads = head_reads.load(Ordering::Relaxed) - before;
        eprintln!(
            "settlement head reads: admission={admission_heads}, checkpoint={state_heads}, observed={observed_heads}"
        );
        assert_eq!(
            observed_heads,
            admission_heads + state_heads,
            "settlement reread stream tails outside its actor observation"
        );
        let AgentReply::Settlement {
            settled,
            statuses,
            phase,
            ..
        } = observed
        else {
            return Err(Error::Invalid("settlement returned another reply".into()));
        };
        assert_eq!(
            (settled, statuses, phase),
            (state.conversation_tail, state.status_tail, state.phase),
            "settlement mixed committed actor cuts"
        );
        if owner == &root {
            let conversation = Conversation::open(connection.clone());
            let before = head_reads.load(Ordering::Relaxed);
            let observer = conversation.observe(None).await?;
            assert_eq!(
                head_reads.load(Ordering::Relaxed) - before,
                observed_heads,
                "default observation repeated its settlement query"
            );
            assert_eq!(
                observer.seen(),
                settled,
                "observation missed the settled cut"
            );
        }
    }
    Ok(())
}

/// Each agent of a chain runs a command, which publishes its task view and so
/// the volume alias it authored, then spawns the next below it. Every fork
/// inherits its parent's alias to that parent's own binding; each depth must
/// rebind it. Negative control: accepting only the root's binding as the
/// alias to replace fails the second child with "volume alias names another
/// binding".
#[tokio::test]
async fn a_spawn_chain_binds_its_volumes_at_every_depth() -> Result<()> {
    use acyclic_harness_client::{AgentCommand, AgentReply};
    const DEPTH: usize = 6;
    let provider = super::scripted((0..DEPTH).flat_map(|depth| {
        [
            super::checked_commands::call(depth, crate::tools::shell::PLATFORM.name, "echo depth"),
            super::ScriptedTurn::Text(format!("depth {depth} done")),
        ]
    }));
    let (_directory, harness, provider) = checkout_scripted(&[], provider).await?;
    let (agents, root) = harness.agents().await?;
    let (agents, mut parent) = (Arc::clone(agents), root.clone());
    let connection = harness.connect().await?;
    for depth in 0..DEPTH {
        let AgentReply::Spawned { agent: child, .. } = connection
            .execute(&AgentCommand::Spawn {
                parent: parent.clone(),
                id: crate::agent::IntentId::try_from(format!("depth-{depth}").as_str())
                    .map_err(agent)?,
                brief: crate::model::ModelContent::Text("run one command".into()),
                role: "worker".into(),
            })
            .await?
        else {
            return Err(Error::Invalid("spawn was not delivered".into()));
        };
        settled(&harness, &child).await?;
        let state = agents.state(&child).await.map_err(agent)?;
        assert!(
            !matches!(state.phase, crate::agent::Phase::Done { .. }),
            "depth {} settled {:?}",
            depth + 1,
            state.phase
        );
        parent = child;
    }
    assert_eq!(provider.request_count(), 2 * DEPTH);
    Ok(())
}

#[tokio::test]
async fn a_spawn_brief_past_one_frame_is_the_childs_first_input_exactly() -> Result<()> {
    use acyclic_harness_client::{AgentCommand, AgentReply, ReadTarget};
    // The child's brief turn, then the parent's own.
    let turns = ["child", "parent"].map(|text| super::ScriptedTurn::Text(text.into()));
    let recorded = Arc::new(std::sync::OnceLock::<std::sync::Weak<ScriptedModelProvider>>::new());
    let parent_cwd = Arc::new(std::sync::OnceLock::<std::path::PathBuf>::new());
    let checked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (source, parent, observed) = (recorded.clone(), parent_cwd.clone(), checked.clone());
    let provider = Arc::new(
        ScriptedModelProvider::new(turns)
            .with_context_capacity(super::capacity())
            .with_dispatch_observer(move |_| {
                let provider = source
                    .get()
                    .and_then(std::sync::Weak::upgrade)
                    .expect("live provider");
                if provider.request_count() != 1 {
                    return Box::pin(async { Ok(()) });
                }
                let request = provider.last_request().expect("child request");
                let cwd = request
                    .messages
                    .iter()
                    .find_map(|message| match &message.content {
                        crate::model::ModelContent::Text(text) => text
                            .split_once("<cwd>")
                            .and_then(|(_, tail)| tail.split_once("</cwd>"))
                            .map(|(cwd, _)| cwd.to_owned()),
                        content => content.parts().iter().find_map(|part| match part {
                            crate::model::ModelContentPart::Text { text, .. } => text
                                .split_once("<cwd>")
                                .and_then(|(_, tail)| tail.split_once("</cwd>"))
                                .map(|(cwd, _)| cwd.to_owned()),
                            _ => None,
                        }),
                    })
                    .expect("child environment cwd");
                let parent = parent.get().expect("parent cwd").clone();
                let observed = observed.clone();
                Box::pin(async move {
                    // Native projection must keep running while the actual
                    // turn retains its mount and inspects the inherited file.
                    let cwd = tokio::fs::canonicalize(&cwd).await.map_err(agent)?;
                    assert_ne!(cwd, tokio::fs::canonicalize(parent).await.map_err(agent)?);
                    assert_eq!(
                        tokio::fs::read(cwd.join("context-sentinel"))
                            .await
                            .map_err(agent)?,
                        b"inherited"
                    );
                    observed.store(true, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                })
            }),
    );
    recorded
        .set(Arc::downgrade(&provider))
        .expect("one provider binding");
    let (_directory, harness, provider) = checkout_scripted(
        &[("context-sentinel".into(), b"inherited".to_vec())],
        provider,
    )
    .await?;
    parent_cwd
        .set(
            harness
                .workspace
                .as_ref()
                .expect("root workspace")
                .path
                .clone(),
        )
        .expect("one parent binding");
    let (_, root) = harness.agents().await?;
    let root = root.clone();
    let connection = harness.connect().await?;
    // Two frames and more: the client stages the command, and the host stages
    // the brief once on the parent, where the child's adoption names it.
    let brief = "brief \u{e9}\u{1f600}\n".repeat(2 * 1024 * 1024 / 12);
    let spawn = AgentCommand::Spawn {
        parent: root.clone(),
        id: crate::agent::IntentId::try_from("large").map_err(agent)?,
        brief: crate::model::ModelContent::Text(brief.clone()),
        role: "worker".into(),
    };
    let AgentReply::Spawned { agent: child, .. } = connection.execute(&spawn).await? else {
        return Err(Error::Invalid("spawn was not delivered".into()));
    };
    let entries = settled(&harness, &child).await?;
    let request = provider
        .request(0)
        .ok_or_else(|| Error::NotFound("child request".into()))?;
    let context = serde_json::to_string(&request).map_err(agent)?;
    assert!(context.contains(&format!("working for parent {root}")));
    assert!(context.contains("final message is your report"));
    assert!(context.contains(&format!("Use send with agent {root}")));
    assert!(checked.load(std::sync::atomic::Ordering::SeqCst));
    assert!(context.contains("<environment_context>"));
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let TurnInput::Brief { brief: file } = entry.input else {
        return Err(Error::Invalid(
            "the child's first input is not its brief".into(),
        ));
    };
    let read = Conversation::read(&connection, &child, ReadTarget::File { file: *file }).await?;
    assert!(read == brief.as_bytes());
    // Negative control: the child reads no other file of its parent's.
    let client = conversation(&harness).await?;
    client.send("other", "a parent's own prompt").await?;
    let entries = settled(&harness, &root).await?;
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let TurnInput::Message { body, .. } = entry.input else {
        return Err(Error::Invalid(
            "the parent's input is not its prompt".into(),
        ));
    };
    assert!(
        Conversation::read(
            &connection,
            &child,
            ReadTarget::File {
                file: (*body).clone()
            }
        )
        .await
        .is_err()
    );
    // The parent reads it, so the refusal is the grant's.
    Conversation::read(&connection, &root, ReadTarget::File { file: *body }).await?;
    // A retried spawn names the same child.
    assert!(matches!(
        connection.execute(&spawn).await?,
        AgentReply::Spawned { agent, .. } if agent == child
    ));
    Ok(())
}

#[tokio::test]
async fn a_prompt_carries_a_thousand_mentions_as_one_reference() -> Result<()> {
    use crate::model::{ModelContent, ModelContentPart};
    const MENTIONS: usize = 1_000;
    let files = (0..MENTIONS)
        .map(|index| {
            (
                format!("m/f{index}.txt"),
                format!("F{index}\n").into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let (_directory, harness, provider) = checkout_with(&files).await?;
    let mut client = ObservedConversation::open(&harness).await?;
    let prompt = files
        .iter()
        .map(|(path, _)| format!("@{path}"))
        .collect::<Vec<_>>()
        .join(" ");
    let entry = client.send("many", &prompt).await?;
    let TurnInput::Message {
        attachments: Some(manifest),
        body,
        ..
    } = &entry.input
    else {
        return Err(Error::Conflict(
            "the prompt started no attached message turn".into(),
        ));
    };
    let listed = listed(&harness, manifest).await?;
    // Compare reference publication with the full manifest independently
    // of any stream record-size policy.
    let inline = serde_json::to_vec(&listed).map_err(agent)?;
    // The records carry one reference rather than the full inline manifest.
    let (agents, root) = harness.agents().await?;
    let entries = records(agents.provider(), root.conversation().map_err(agent)?).await?;
    let inbox = records(agents.provider(), root.inbox().map_err(agent)?).await?;
    for record in inbox.iter().chain(&entries) {
        assert!(record.len() < inline.len());
    }
    assert_eq!(listed.len(), MENTIONS);
    // The model sees the prompt and every file, in mention order.
    let request = provider.last_request().expect("one model request");
    let ModelContent::Parts(parts) = input_content(&request, body)? else {
        return Err(Error::Conflict("the turn input has no attachments".into()));
    };
    assert_eq!(parts.len(), MENTIONS + 1);
    for (index, part) in parts[1..].iter().enumerate() {
        let ModelContentPart::File { file, .. } = part else {
            return Err(Error::Conflict("an attachment is not a file".into()));
        };
        assert_eq!(
            file.path().rsplit('/').next(),
            Some(super::super::context::copy_name(&format!("m/f{index}.txt")).as_str())
        );
    }
    Ok(())
}

#[tokio::test]
async fn project_mentions_keep_their_volume_and_bounded_copy_paths() -> Result<()> {
    use crate::model::ModelContent;
    const MENTIONS: usize = 32;
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = super::scripted([
        super::ScriptedTurn::Text("done".into()),
        super::ScriptedTurn::Text("done".into()),
    ]);
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), project.clone(), provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let workspace = crate::filesystem::workspace_ref(
        project.provider().clone(),
        &project.storage_name(&mut None)?,
    )?;
    let mutations = std::iter::once(crate::filesystem::WorkspaceMutation::CreateDirectory {
        path: "/d".into(),
    })
    .chain(
        (0..MENTIONS).map(|index| crate::filesystem::WorkspaceMutation::PutFile {
            path: format!("/d/n{index}.txt"),
            bytes: acyclic_stream::OwnedBytes::from(format!("{index}\n").into_bytes()),
        }),
    )
    .collect::<Vec<_>>();
    harness
        .files
        .apply(
            &workspace,
            None,
            &mutations,
            &crate::contract::IdempotencyKey::new("project-files")?,
        )
        .await?;
    let mut client = ObservedConversation::open(&harness).await?;
    let prompt = (0..MENTIONS)
        .map(|index| format!("@d/n{index}.txt"))
        .collect::<Vec<_>>()
        .join(" ");
    let entry = client.send("project", &prompt).await?;
    let TurnInput::Message {
        attachments: Some(manifest),
        body,
        ..
    } = &entry.input
    else {
        return Err(Error::Conflict(
            "the prompt started no attached message turn".into(),
        ));
    };
    // Every member lives beside its manifest on the private volume, inside
    // the directory a forked child is granted with the manifest; none stays
    // on the project volume a child does not inherit.
    let (base, _) = manifest
        .path()
        .rsplit_once('/')
        .expect("manifest directory");
    let project_files = listed(&harness, manifest).await?;
    assert_eq!(project_files.len(), MENTIONS);
    for file in &project_files {
        assert_eq!(file.volume(), manifest.volume());
        assert_ne!(file.volume(), &project);
        assert!(
            file.path().starts_with(&format!("{base}/")),
            "{}",
            file.path()
        );
    }
    // The turn reached the model with every file.
    let request = provider.last_request().expect("one model request");
    let ModelContent::Parts(parts) = input_content(&request, body)? else {
        return Err(Error::Conflict("the turn input has no attachments".into()));
    };
    assert_eq!(parts.len(), MENTIONS + 1);

    // Copy names use their admitted identity instead of repeating a long source path.
    const FIXTURE_PATH_BYTES: usize = 8193;
    // Cross the former logical path ceiling with ordinary-sized components.
    let mut parents = Vec::new();
    let mut long = String::new();
    while FIXTURE_PATH_BYTES - 1 - long.len() > 200 {
        long.push_str(&format!("{}{}/", parents.len() % 10, "d".repeat(199)));
        parents.push(format!("/{}", long.trim_end_matches('/')));
    }
    long.push_str(&"f".repeat(FIXTURE_PATH_BYTES - 1 - long.len()));
    assert_eq!(long.len() + 1, FIXTURE_PATH_BYTES);
    let mutations = parents
        .into_iter()
        .map(|path| crate::filesystem::WorkspaceMutation::CreateDirectory { path })
        .chain(
            [("short.txt", "short\n"), (long.as_str(), "long\n")].map(|(path, text)| {
                crate::filesystem::WorkspaceMutation::PutFile {
                    path: format!("/{path}"),
                    bytes: acyclic_stream::OwnedBytes::from(text.as_bytes().to_vec()),
                }
            }),
        )
        .collect::<Vec<_>>();
    harness
        .files
        .apply(
            &workspace,
            None,
            &mutations,
            &crate::contract::IdempotencyKey::new("long-path-files")?,
        )
        .await?;
    let entry = client.send("long", &format!("@short.txt @{long}")).await?;
    let TurnInput::Message {
        attachments: Some(manifest),
        ..
    } = &entry.input
    else {
        return Err(Error::Conflict(
            "the prompt started no attached message turn".into(),
        ));
    };
    let listed = listed(&harness, manifest).await?;
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].path().len(), listed[1].path().len());
    let reader = harness.client_reader()?;
    for (file, text) in listed.iter().zip(["short\n", "long\n"]) {
        let bytes = reader
            .read(file, file.descriptor().byte_length(), None, None)?
            .await?;
        assert_eq!(bytes.as_ref(), text.as_bytes());
    }
    let (directory, _) = listed[1].path().rsplit_once('/').expect("copy directory");
    assert!(format!("{directory}/{long}").len() > FIXTURE_PATH_BYTES);
    Ok(())
}

#[tokio::test]
async fn a_small_file_bound_never_fails_a_turn_on_its_lists() -> Result<()> {
    use crate::model::ModelContent;
    const MENTIONS: usize = 32;
    // Each mentioned file and all of them together fit this bound; the
    // attachment manifest, the turn's context manifest and its journaled
    // input do not.
    const FILE_BYTES: u64 = 1024;
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = super::scripted([
        crate::test_support::ScriptedTurn::Text("done".into()),
        crate::test_support::ScriptedTurn::Text("done".into()),
    ]);
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), project.clone(), provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled)
                .limits(crate::conversation::Limits {
                    file_bytes: Some(FILE_BYTES),
                    ..Default::default()
                }),
        )
        .await?,
    );
    let workspace = crate::filesystem::workspace_ref(
        project.provider().clone(),
        &project.storage_name(&mut None)?,
    )?;
    let mutations = std::iter::once(crate::filesystem::WorkspaceMutation::CreateDirectory {
        path: "/d".into(),
    })
    .chain(
        (0..MENTIONS).map(|index| crate::filesystem::WorkspaceMutation::PutFile {
            path: format!("/d/n{index}.txt"),
            bytes: acyclic_stream::OwnedBytes::from(format!("{index}\n").into_bytes()),
        }),
    )
    .collect::<Vec<_>>();
    harness
        .files
        .apply(
            &workspace,
            None,
            &mutations,
            &crate::contract::IdempotencyKey::new("project-files")?,
        )
        .await?;
    let mut client = ObservedConversation::open(&harness).await?;
    let prompt = (0..MENTIONS)
        .map(|index| format!("@d/n{index}.txt"))
        .collect::<Vec<_>>()
        .join(" ");
    let entry = client.send("small", &prompt).await?;
    let TurnInput::Message {
        attachments: Some(manifest),
        body,
        ..
    } = &entry.input
    else {
        return Err(Error::Conflict(
            "the prompt started no attached message turn".into(),
        ));
    };
    // Control: the list alone exceeds the configured file bound.
    assert!(manifest.descriptor().byte_length() > FILE_BYTES);
    let attached = |content: &ModelContent| match content {
        ModelContent::Parts(parts) => parts.len(),
        _ => 1,
    };
    let request = provider.last_request().expect("one model request");
    assert_eq!(attached(input_content(&request, body)?), MENTIONS + 1);
    // The next turn replays that message, attachments expanded, under the
    // same bound.
    let next = client.send("next", "next").await?;
    let TurnInput::Message {
        body: next_body, ..
    } = &next.input
    else {
        return Err(Error::Invalid("the next turn has no message input".into()));
    };
    let request = provider.last_request().expect("a second model request");
    let replayed = request
        .messages
        .iter()
        .filter(|message| {
            message
                .content
                .file_parts()
                .any(|(file, _)| file == body.as_ref() || file == next_body.as_ref())
        })
        .map(|message| attached(&message.content))
        .collect::<Vec<_>>();
    assert_eq!(replayed, vec![MENTIONS + 1, 1]);
    Ok(())
}

/// Exercise file generations, selection refusal, immutable outputs and indexed
/// history through the actual local server/actor path. The existing text fixture
/// supplies only the first turn; restore is model-free.
#[tokio::test]
async fn selected_restore_retains_files_and_settles_stale_heads_without_model_work() -> Result<()> {
    use crate::agent::{
        Delivered, InboxRecord, IntentId, NoticePayload, RestoreMode, Sender, TurnOutcome,
    };
    use acyclic_fs::{CancellationToken, WorkBudget};
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await
    .map_err(agent)?;
    let cancel = CancellationToken::new();
    let workspace = fs
        .open_workspace(
            project.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| agent(failure.error))?
        .value;
    workspace
        .write_text("/plan.md", "saved")
        .await
        .map_err(agent)?;
    let provider = text();
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), project, provider.clone())?
                .without_default_tools()
                .compaction(crate::context::CompactionPolicy::default()),
        )
        .await?,
    );
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let connection = harness.connect().await?;
    let client = Conversation::open(connection.clone());
    let mut observed = ObservedConversation::observe(Arc::clone(&client), Some(0)).await?;
    let runtime = &harness
        .agents
        .get()
        .ok_or_else(|| Error::Invalid("agent runtime missing".into()))?
        .runtime;
    let settlement = async |observed: &mut ObservedConversation, inbox| {
        tokio::select! {
            result = observed.settled(inbox) => result,
            result = runtime.completion() => {
                result?;
                Err(Error::Storage("agent runtime ended before settlement".into()))
            }
        }
    };
    let inbox = client.send("first", "hello").await?;
    let first = settlement(&mut observed, inbox).await?;
    assert!(first.outcome.is_completed(), "{:?}", first.outcome);
    settled(&harness, &root).await?;
    let mut usage = client.usage_query();
    while !usage.synchronized() {
        usage.advance().await?;
    }
    assert_eq!(usage.totals().completed, 1);
    let original_window = usage.context();
    assert!(original_window.is_some());
    let store = crate::agent::actor_store(agents.provider()).map_err(agent)?;
    let actor = agents.actor(&root).map_err(agent)?;
    let before = store.load(&actor).await.map_err(agent)?.follow_request();
    let Delivered::Committed { inbox: notice } = agents
        .send(
            &root,
            &Sender::Agent { path: root.clone() },
            &InboxRecord::Notice {
                id: IntentId::try_from("selection-notice").map_err(agent)?,
                notice: NoticePayload::Quiet(crate::conversation::wire::Notice {
                    source: "files".into(),
                    kind: "files.changed".into(),
                    body: serde_json::json!({"path":"/plan.md"}),
                    wake: false,
                    coalesce_key: None,
                }),
            },
        )
        .await
        .map_err(agent)?
    else {
        return Err(Error::Invalid("notice recipient closed".into()));
    };
    let saved = crate::filesystem::history_checkpoint(agents.provider().as_ref(), &root, 0).await?;
    // A display rename after the saved cut cannot change its restore identity.
    let state = agents.state(&root).await.map_err(agent)?;
    let mut primary = crate::agent::WorkspaceTable::primary_in(&state.volumes)
        .ok_or_else(|| Error::NotFound("primary workspace".into()))?
        .clone();
    let original_id = primary.id;
    primary.name = "renamed-before-restore".into();
    let record = InboxRecord::WorkspaceChanged(Box::new(crate::agent::WorkspaceChanged {
        version: 1,
        op: crate::agent::WorkspaceOp::Rename,
        entry: primary,
    }));
    let request = acyclic_stream::CommitRequest {
        idempotency_key: acyclic_stream::IdempotencyKey::new(b"rename-saved-workspace".to_vec())
            .map_err(agent)?,
        deadline_unix_millis: None,
        effects: vec![acyclic_stream::CommitEffect::hard(
            vec![
                acyclic_stream::CommitCondition::Present {
                    path: root.inbox().map_err(agent)?,
                },
                acyclic_stream::CommitCondition::Absent {
                    path: root.closed().map_err(agent)?,
                },
            ],
            vec![acyclic_stream::CommitMutation::Append {
                path: root.inbox().map_err(agent)?,
                records: vec![record.encode().map_err(agent)?.into()],
            }],
        )],
    };
    assert!(matches!(
        agents.provider().commit(request).await.map_err(agent)?,
        acyclic_stream::CommitOutcome::Committed(_)
    ));
    agents.drain(&root).await.map_err(agent)?;
    assert_eq!(
        crate::agent::WorkspaceTable::primary_in(
            &agents.state(&root).await.map_err(agent)?.volumes
        )
        .ok_or_else(|| Error::NotFound("renamed workspace".into()))?
        .id,
        original_id
    );
    let snapshot = saved
        .volumes
        .first()
        .ok_or_else(|| Error::Invalid("project snapshot missing".into()))?;
    let expected = client.project_heads().await?;
    assert_eq!(expected[0].generation, snapshot.generation);
    workspace
        .write_text("/plan.md", "user edit")
        .await
        .map_err(agent)?;
    let inbox = client
        .restore("refuse", 0, RestoreMode::Both, expected)
        .await?;
    let refusal = settlement(&mut observed, inbox).await?;
    assert_eq!(refusal.outcome, TurnOutcome::RestoreConflict);
    settled(&harness, &root).await?;
    for _ in notice..=inbox {
        usage.advance().await?;
    }
    assert_eq!(
        usage.context(),
        original_window,
        "refused restore changed model occupancy"
    );
    assert!(
        client
            .turn(&refusal)
            .await?
            .answer
            .contains("expected project heads changed")
    );
    assert_eq!(
        workspace
            .read("/plan.md", 32, WorkBudget::UNBOUNDED, &cancel)
            .await
            .map_err(|failure| agent(failure.error))?
            .value
            .as_ref(),
        b"user edit"
    );
    let refused =
        crate::filesystem::history_checkpoint(agents.provider().as_ref(), &root, 1).await?;
    assert_eq!(refused.context.tail, saved.context.tail);
    assert_ne!(
        refused.volumes[0].retained_workspace,
        snapshot.retained_workspace
    );
    let crate::local::agent_conversation::HistoryPage { items, next } = client
        .history_page(crate::client::AgentPageRequest::new(1, 4096, None).map_err(agent)?)
        .await?;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].conversation, 2);
    assert_eq!(items[0].context_tail, saved.context.tail);
    assert_eq!(items[0].volume_count, saved.volumes.len() as u64);
    let measure = crate::client::AgentCommand::History {
        agent: root.clone(),
        page: crate::client::AgentPageRequest::new(1, 4096, None).map_err(agent)?,
    };
    let exact_credit = acyclic_harness_client::encode_agent_reply(
        &Ok(crate::client::AgentReply::History {
            items: items.clone(),
            next: next.clone(),
        }),
        &measure,
    )
    .map_err(agent)?
    .len() as u32;
    let exact_page = connection
        .execute(&crate::client::AgentCommand::History {
            agent: root.clone(),
            page: crate::client::AgentPageRequest::new(1, exact_credit, None).map_err(agent)?,
        })
        .await?;
    assert_eq!(
        acyclic_harness_client::encode_agent_reply(&Ok(exact_page.clone()), &measure)
            .map_err(agent)?
            .len(),
        exact_credit as usize,
        "the newest row and its exact continuation must fit their byte credit"
    );
    let crate::client::AgentReply::History {
        items: exact_items,
        next: exact_next,
    } = exact_page
    else {
        return Err(Error::Invalid(
            "history query returned another reply".into(),
        ));
    };
    assert_eq!(exact_items, items);
    assert_eq!(exact_next, next);
    assert!(
        connection
            .execute(&crate::client::AgentCommand::History {
                agent: root.clone(),
                page: crate::client::AgentPageRequest::new(1, exact_credit - 1, None)
                    .map_err(agent)?,
            })
            .await
            .is_err(),
        "one missing credit byte must refuse rather than exceed the page"
    );
    let older = next.ok_or_else(|| Error::Invalid("older continuation missing".into()))?;
    let expected = client.project_heads().await?;
    let mut prepared = client.prepare_restore("apply", 0, RestoreMode::Both, expected)?;
    let receipt = prepared.execute().await.map_err(agent)?;
    let crate::client::AgentReply::Delivered {
        delivery: Delivered::Committed { inbox },
    } = receipt
    else {
        return Err(Error::Invalid("prepared restore did not commit".into()));
    };
    let replay = prepared.execute().await.map_err(agent)?;
    assert!(
        matches!(replay, crate::client::AgentReply::Delivered { delivery: Delivered::Committed { inbox: repeated } } if repeated == inbox)
    );
    settlement(&mut observed, inbox).await?;
    let entries = records(agents.provider(), root.conversation().map_err(agent)?).await?;
    assert_eq!(entries.len(), 3);
    let restored =
        crate::filesystem::history_checkpoint(agents.provider().as_ref(), &root, 2).await?;
    assert_eq!(restored.context.tail, saved.context.tail);
    assert_eq!(
        restored.volumes[0].retained_workspace,
        snapshot.retained_workspace
    );
    // The saved continuation stays at its original cut after another turn.
    // A fresh selection seeks directly to the new tail.
    for (cursor, expected) in [(Some(older), 1), (None, 3)] {
        let page = client
            .history_page(crate::client::AgentPageRequest::new(1, 4096, cursor).map_err(agent)?)
            .await?;
        let items = page.items;
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].conversation, expected);
    }
    assert_eq!(
        workspace
            .read("/plan.md", 32, WorkBudget::UNBOUNDED, &cancel)
            .await
            .map_err(|failure| agent(failure.error))?
            .value
            .as_ref(),
        b"saved"
    );
    // A content restore cannot reinstate an earlier authority policy, even
    // when the changed policy happens to grant the same content actions.
    let mut policy = acyclic_fs::access::INITIAL_POLICY.to_vec();
    policy.push(b'\n');
    let mut writer = workspace
        .checkout(
            acyclic_fs::model::GenerationSelector::Head,
            acyclic_fs::model::CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| agent(failure.error))?
        .value;
    writer
        .write_access_policy(
            &policy,
            acyclic_fs::NamedAttributeWriteMode::Replace,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| agent(failure.error))?;
    writer
        .commit(
            acyclic_fs::OperationId::new(),
            None,
            None,
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancel,
            |_| {
                std::future::ready(Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                    },
                ))
            },
        )
        .await
        .map_err(|failure| agent(failure.error))?;
    let expected = client.project_heads().await?;
    let inbox = client
        .restore("policy-refusal", 0, RestoreMode::Both, expected.clone())
        .await?;
    let refusal = settlement(&mut observed, inbox).await?;
    assert_eq!(refusal.outcome, TurnOutcome::RestoreConflict);
    assert!(
        client
            .turn(&refusal)
            .await?
            .answer
            .contains("cannot replace the current access policy")
    );
    assert_eq!(client.project_heads().await?, expected);
    let refused =
        crate::filesystem::history_checkpoint(agents.provider().as_ref(), &root, 3).await?;
    assert_eq!(refused.context, restored.context);
    let inbox = client
        .restore("context-only", 0, RestoreMode::Context, Vec::new())
        .await?;
    settlement(&mut observed, inbox).await?;
    let context_only =
        crate::filesystem::history_checkpoint(agents.provider().as_ref(), &root, 4).await?;
    assert_eq!(context_only.context, saved.context);
    assert_eq!(context_only.volumes, refused.volumes);
    assert_eq!(client.project_heads().await?, expected);
    let inbox = client.rewind("rewind").await?;
    settlement(&mut observed, inbox).await?;
    let inbox = client.clear_all("clear").await?;
    settlement(&mut observed, inbox).await?;
    assert_eq!(
        agents.state(&root).await.map_err(agent)?.pending_notices,
        Some(notice..notice + 1),
        "model-free controls must retain the original pending Notice range"
    );
    // Read only this control sequence's original actor publications. The
    // former freeze-before-restore path left an armed continuation here.
    let end = store
        .load(&actor)
        .await
        .map_err(agent)?
        .follow_request()
        .from;
    let limit = u32::try_from(end - before.from).map_err(agent)?;
    assert!(limit > 0);
    let mut publications = agents
        .provider()
        .read(ReadRequest {
            key: None,
            path: before.path,
            from: before.from,
            limit,
            at_least: Default::default(),
        })
        .await
        .map_err(agent)?;
    while let Some(record) = publications.try_next().await.map_err(agent)? {
        let state = acyclic_actors::local::State::decode(&record.value).map_err(agent)?;
        assert!(
            state.alarm().deadline_unix_ms().is_none(),
            "model-free control scheduled an avoidable continuation"
        );
    }
    assert_eq!(
        provider.request_count(),
        1,
        "selection ran another model request"
    );
    Ok(())
}

#[tokio::test]
async fn historical_child_settles_after_its_parent_has_later_context_segments() -> Result<()> {
    use crate::filesystem::VolumeResources;
    use acyclic_fs::{CancellationToken, WorkBudget};
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await
    .map_err(agent)?;
    let cancel = CancellationToken::new();
    let source = project.storage_name(&mut None)?;
    let workspace = fs
        .open_workspace(&source, WorkBudget::UNBOUNDED, &cancel)
        .await
        .map_err(|failure| agent(failure.error))?
        .value;
    workspace
        .write_text("/plan.md", "selected files")
        .await
        .map_err(agent)?;
    let provider = super::scripted(
        (0..4).map(|index| crate::test_support::ScriptedTurn::Text(format!("answer {index}"))),
    );
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), project, provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let connection = harness.connect().await?;
    let client = Conversation::open(connection.clone());
    for id in ["first", "second", "third"] {
        client.send(id, id).await?;
        settled(&harness, &root).await?;
        if id == "first" {
            let cuts = crate::local::workspace::LocalVolumeResources(Arc::clone(
                harness.workspace.as_ref().ok_or_else(|| {
                    Error::Invalid("historical fork has no native workspace".into())
                })?,
            ));
            let destination = format!("{source}-selected-cut");
            let operation = acyclic_fs::OperationId::from_bytes([44; 16]);
            let original = cuts
                .cut(&source, &destination, operation, None)
                .await
                .map_err(agent)?
                .ok_or_else(|| Error::Invalid("native fork did not retain its cut".into()))?;
            workspace
                .write_text("/plan.md", "later files")
                .await
                .map_err(agent)?;
            let later = workspace
                .head(WorkBudget::UNBOUNDED, &cancel)
                .await
                .map_err(|failure| agent(failure.error))?
                .value
                .id();
            assert_ne!(original, later, "parent edit must advance the original cut");
            assert_eq!(
                cuts.cut(&source, &destination, operation, Some(original))
                    .await
                    .map_err(agent)?,
                Some(original),
                "same operation must reuse its retained cut after its source advances"
            );
            for (destination, selected) in [
                (destination, later),
                (format!("{source}-changed-cut"), original),
            ] {
                assert!(
                    matches!(
                        cuts.cut(&source, &destination, operation, Some(selected))
                            .await,
                        Err(crate::filesystem::ForkCutError::SourceChanged)
                    ),
                    "selected generation mismatch must refuse both retained and fresh cuts"
                );
            }
        }
    }
    let crate::client::AgentReply::Spawned { agent: child, .. } = connection
        .execute(&crate::client::AgentCommand::Fork {
            continuation: None,
            parent: root.clone(),
            id: crate::agent::IntentId::try_from("historical-child").map_err(agent)?,
            at: 0,
            brief: crate::model::ModelContent::Text("continue the first turn".into()),
            role: "worker".into(),
        })
        .await?
    else {
        return Err(Error::Invalid(
            "historical fork returned another reply".into(),
        ));
    };
    let entries = settled(&harness, &child).await?;
    let child_state = agents.state(&child).await.map_err(agent)?;
    assert_eq!(
        entries.len(),
        2,
        "historical fork: base={}, tail={}, phase={:?}, first input={:?}, requests={}",
        child_state.conversation_base,
        child_state.conversation_tail,
        child_state.phase,
        entries
            .first()
            .map(|bytes| ConversationEntry::decode(bytes).map(|entry| entry.input)),
        provider.request_count(),
    );
    let inherited = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let branched = ConversationEntry::decode(&entries[1]).map_err(agent)?;
    assert!(matches!(branched.input, TurnInput::Brief { .. }));
    assert_eq!(client.turn(&inherited).await?.answer, "answer 0");
    let saved =
        crate::filesystem::history_checkpoint(agents.provider().as_ref(), &child, 1).await?;
    assert_eq!(saved.context.tail, 2);
    assert_eq!(saved.context.path.as_str(), format!("{child}/context/1"));
    assert_eq!(
        records(agents.provider(), root.conversation().map_err(agent)?)
            .await?
            .len(),
        3
    );
    assert_eq!(provider.request_count(), 4);
    let request = provider
        .last_request()
        .ok_or_else(|| Error::Invalid("historical child made no model request".into()))?;
    let context = super::request_text(&harness, &request).await?;
    assert!(context.contains("answer 0"));
    assert!(!context.contains("answer 1") && !context.contains("answer 2"));
    let project = child_state
        .volumes
        .iter()
        .find(|binding| {
            binding
                .as_volume()
                .is_some_and(|volume| volume.class() == crate::conversation::VolumeClass::Project)
        })
        .ok_or_else(|| Error::Invalid("historical child has no project volume".into()))?;
    let child_workspace = fs
        .open_workspace(
            project.volume()?.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| agent(failure.error))?
        .value;
    for (selected, expected) in [
        (&child_workspace, b"selected files".as_slice()),
        (&workspace, b"later files".as_slice()),
    ] {
        assert_eq!(
            selected
                .read("/plan.md", 32, WorkBudget::UNBOUNDED, &cancel)
                .await
                .map_err(|failure| agent(failure.error))?
                .value
                .as_ref(),
            expected,
            "fork must retain the selected files while its parent keeps later edits"
        );
    }
    Ok(())
}

#[tokio::test]
async fn tool_free_checkout_sessions_read_their_own_logical_context_without_mounting() -> Result<()>
{
    use acyclic_fs::facade::FsWork as _;
    let checkout = tempfile::tempdir().map_err(agent)?;
    let state = tempfile::tempdir().map_err(agent)?;
    std::fs::write(
        checkout.path().join("AGENTS.md"),
        "ORIGINAL_CHECKOUT_INSTRUCTIONS",
    )
    .map_err(agent)?;
    let provider = super::scripted([super::ScriptedTurn::Text("own context".into())]);
    let harness = Arc::new(
        super::open_scripted(
            crate::test_support::local_harness_config(
                checkout.path(),
                (
                    crate::model::Model::new("scripted".into(), "test".into(), "1".into())?,
                    provider.clone(),
                ),
            )?
            .state_directory(state.path())
            .without_default_tools()
            .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let directory = harness.connect_sessions().await?;
    let session = crate::local::sessions::create(&directory, "tool-free-session", None).await?;
    let (agents, _) = harness.agents().await?;
    let root = agents.state(session.root()).await.map_err(agent)?;
    let project = root
        .volumes
        .iter()
        .find(|binding| binding.name == "project")
        .ok_or_else(|| Error::Invalid("session has no project".into()))?;
    let view = harness
        .workspace
        .as_ref()
        .unwrap()
        .spawned_project_view(project.volume()?)
        .await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    view.write(
        "/AGENTS.md",
        acyclic_stream::OwnedBytes::from_static(b"SESSION_ONLY_INSTRUCTIONS"),
        acyclic_fs::IdempotencyKey::from_bytes(crate::OperationId::new().into_bytes()),
        acyclic_fs::PublicationPermit::Unrestricted,
        &mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation),
    )
    .await
    .map_err(agent)?;
    let mut observed = ObservedConversation::observe(Conversation::open(session), None).await?;
    observed
        .send("context", "read the selected instructions")
        .await?;
    let request = super::request_text(
        &harness,
        &provider.last_request().expect("session model turn"),
    )
    .await?;
    assert!(request.contains("SESSION_ONLY_INSTRUCTIONS"));
    assert!(!request.contains("ORIGINAL_CHECKOUT_INSTRUCTIONS"));
    assert_eq!(provider.requests().len(), 1);
    Ok(())
}

#[tokio::test]
async fn historical_input_previews_do_not_admit_reserved_follow_ups_inside_a_steering_cut()
-> Result<()> {
    historical_input_preview_case(false).await
}

#[tokio::test]
async fn historical_input_previews_retain_newer_steering_across_later_hook_turns() -> Result<()> {
    historical_input_preview_case(true).await
}

async fn historical_input_preview_case(later_hook: bool) -> Result<()> {
    use crate::{
        agent::{Delivered, InboxRecord, InputPreviewStep, IntentId, Message, Sender},
        model::{ModelEvent, ToolCall},
        test_support::{ScriptedEvent, ScriptedTurn},
    };
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let boundary = |id: &str| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                id.into(),
                "example.echo".into(),
                serde_json::json!({"text":"boundary"}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::json!({}),
            }),
        ])
    };
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let gate = std::sync::Mutex::new(Some((entered, released)));
    let calls = std::sync::atomic::AtomicUsize::new(0);
    let provider = Arc::new(
        ScriptedModelProvider::new([
            boundary("boundary"),
            ScriptedTurn::Text("at the selected cut".into()),
            boundary("reserved-boundary"),
            ScriptedTurn::Text("reserved turn".into()),
        ])
        .with_context_capacity(super::capacity())
        .with_dispatch_observer(move |_| {
            let gate = if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 3 {
                gate.lock().unwrap().take()
            } else {
                None
            };
            Box::pin(async move {
                if let Some((entered, released)) = gate {
                    let _ = entered.send(());
                    released.await.map_err(agent)?;
                }
                Ok(())
            })
        }),
    );
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), project, provider)?
                .without_default_tools()
                .tool(super::tool("example.echo"))
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let connection = harness.connect_sessions().await?;
    let (agents, root) = harness.agents().await?;
    agents.pause(root, true).await.map_err(agent)?;
    let mut observed =
        ObservedConversation::observe(Conversation::open(connection.clone()), None).await?;
    let bound = harness
        .agents
        .get()
        .ok_or_else(|| Error::Invalid("missing hosted agents".into()))?
        .server()
        .bound();
    let by = Sender::Host {
        host: "preview-cut".into(),
    };
    let mut sequences = Vec::new();
    let mut reserved = None;
    let inputs = if later_hook {
        [
            ("first", "INITIAL_INPUT", 0),
            ("hook", "HOOK_INPUT", 2),
            ("steer", "ADMITTED_STEERING_INPUT", 0),
        ]
    } else {
        [
            ("first", "INITIAL_INPUT", 0),
            ("steer", "ADMITTED_STEERING_INPUT", 0),
            ("reserved", "UNADMITTED_FOLLOW_UP", 1),
        ]
    };
    for (name, text, kind) in inputs {
        let id = IntentId::try_from(name).map_err(agent)?;
        let body = bound
            .stage_text(root, "message", &by, &id, (text, text))
            .await
            .map_err(agent)?;
        if kind == 1 {
            reserved = Some(body.clone());
        }
        let message = Message {
            from: by.clone(),
            id,
            re: None,
            body: Box::new(body),
            attachments: None,
        };
        let input = match kind {
            1 => InboxRecord::FollowUp(message),
            2 => InboxRecord::Hook(crate::agent::Hook {
                host: "preview-cut".into(),
                id: message.id,
                event: message.body,
            }),
            _ => InboxRecord::Message(message),
        };
        let Delivered::Committed { inbox } = agents.send(root, &by, &input).await.map_err(agent)?
        else {
            return Err(Error::Invalid("preview fixture input was closed".into()));
        };
        sequences.push(inbox);
    }
    agents.pause(root, false).await.map_err(agent)?;
    let entry = observed.settled(sequences[0]).await?;
    assert!(
        entry
            .steering
            .as_ref()
            .is_some_and(|cut| cut.through > sequences[2]),
        "the selected prefix must include the reserved gap for this control to prove admission"
    );
    entering.await.map_err(agent)?;
    let pending = observed.conversation.pending_inputs().await?;
    if later_hook {
        let (state, cursor) = agents.state_snapshot(root).await.map_err(agent)?;
        assert!(state.settled_before(cursor) <= sequences[1]);
        assert!(
            state.input_pending(cursor, sequences[2]),
            "this control must retain the ambiguous earlier steering interval"
        );
        assert!(matches!(
            observed.conversation.reply("steer", sequences[2]).await?,
            Reply::Answered { .. }
        ));
        assert!(
            pending.is_empty(),
            "answered steering returned as pending: {pending:?}"
        );
    } else {
        assert_eq!(pending, vec![(sequences[2], "UNADMITTED_FOLLOW_UP".into())]);
    }
    release
        .send(())
        .map_err(|_| Error::Invalid("reserved dispatch gate closed".into()))?;
    if later_hook {
        observed.conversation.wait_current_input().await?;
        let mut entries = agents
            .provider()
            .read(ReadRequest {
                key: None,
                path: root.conversation().map_err(agent)?,
                from: 1,
                limit: 1,
                at_least: Default::default(),
            })
            .await
            .map_err(agent)?;
        let record = entries.try_next().await.map_err(agent)?.ok_or_else(|| {
            Error::Invalid("the settled hook published no conversation entry".into())
        })?;
        let hook = ConversationEntry::decode(&record.value).map_err(agent)?;
        assert_eq!(hook.inbox, sequences[1]);
        assert!(matches!(hook.input, TurnInput::Hook { .. }));
        assert!(
            hook.steering.is_none() && hook.observed_through > sequences[2],
            "the hook must retain the earlier admission cursor without its own steering proof"
        );
    }
    let branch = crate::local::sessions::fork(
        &connection,
        root.clone(),
        u64::from(later_hook),
        "preview-historical-cut",
    )
    .await?;
    let branch = Conversation::open(branch);
    let mut query = branch
        .input_query(crate::client::AgentPageRequest::new(1, 128, None).map_err(agent)?)
        .await
        .map_err(agent)?;
    let preview = loop {
        match query.next().await.map_err(agent)? {
            Some(InputPreviewStep::Progress) => {}
            Some(InputPreviewStep::Pending { .. }) => {
                return Err(Error::Invalid("preview returned pending ranges".into()));
            }
            Some(InputPreviewStep::Selected(preview)) => break preview,
            None => {
                return Err(Error::Invalid(
                    "input query ended before its selected cut".into(),
                ));
            }
        }
    };
    assert_eq!(
        preview.as_ref().map(|value| value.text.as_str()),
        Some("ADMITTED_STEERING_INPUT"),
        "{}",
        if later_hook {
            "a later hook must retain the newest admitted steering message"
        } else {
            "an inherited preview must not disclose an unadmitted follow-up"
        }
    );
    if !later_hook {
        assert!(
            branch
                .text(&reserved.ok_or_else(|| Error::Invalid("missing reserved body".into()))?)
                .await
                .is_err()
        );
    }
    let mut own = observed
        .conversation
        .input_query(crate::client::AgentPageRequest::new(1, 128, None).map_err(agent)?)
        .await
        .map_err(agent)?;
    let expected = if later_hook {
        "ADMITTED_STEERING_INPUT"
    } else {
        "UNADMITTED_FOLLOW_UP"
    };
    assert!(
        matches!(own.next().await.map_err(agent)?, Some(InputPreviewStep::Selected(Some(preview))) if preview.text == expected),
        "the originating session still owns its accepted follow-up"
    );
    Ok(())
}

#[tokio::test]
async fn historical_sessions_preserve_the_source_and_fork_files_at_the_conversation_cut()
-> Result<()> {
    use crate::client::{AgentCommand, AgentReply};
    use crate::filesystem::{WorkspaceMutation, workspace_ref};
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = super::scripted(
        ["at cut", "later", "branch answer"].map(|text| super::ScriptedTurn::Text(text.into())),
    );
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), project.clone(), provider.clone())?
                .without_default_tools()
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let workspace = workspace_ref(
        project.provider().clone(),
        &project.storage_name(&mut None)?,
    )?;
    for (key, text) in [
        ("at-cut", b"at cut".as_slice()),
        ("after-cut", b"after cut".as_slice()),
    ] {
        harness
            .files
            .apply(
                &workspace,
                None,
                &[WorkspaceMutation::PutFile {
                    path: "/plan.md".into(),
                    bytes: acyclic_stream::OwnedBytes::copy_from_slice(text),
                }],
                &crate::IdempotencyKey::new(key)?,
            )
            .await?;
        if key == "at-cut" {
            let mut conversation = ObservedConversation::open(&harness).await?;
            conversation.send("prefix", "first decision").await?;
        } else {
            let mut conversation = ObservedConversation::open(&harness).await?;
            conversation.send("later", "later decision").await?;
        }
    }
    let (agents, root) = harness.agents().await?;
    let source = records(agents.provider(), root.conversation().map_err(agent)?).await?;
    assert_eq!(source.len(), 2);
    let connection = harness.connect_sessions().await?;
    let request = AgentCommand::ForkSession {
        peer: true,
        agent: root.clone(),
        id: crate::agent::IntentId::try_from("rewind").map_err(agent)?,
        at: 0,
    };
    let AgentReply::Root { agent: branch } = connection.execute(&request).await? else {
        return Err(Error::Invalid(
            "historical root returned another reply".into(),
        ));
    };
    assert!(branch.parent().is_none());
    assert_ne!(&branch, root);
    let state = agents.state(&branch).await.map_err(agent)?;
    assert_eq!(state.conversation_tail, 1);
    assert!(state.open_children.is_empty());
    let actor = crate::agent::actor_store(agents.provider())
        .map_err(agent)?
        .load(&agents.actor(&branch).map_err(agent)?)
        .await
        .map_err(agent)?;
    assert!(
        actor
            .state()
            .ok_or_else(|| Error::Invalid("missing branch actor".into()))?
            .paused()
    );
    assert_eq!(
        records(agents.provider(), branch.conversation().map_err(agent)?).await?,
        source[..1]
    );
    let selected_branch = Conversation::open(connection.select(branch.clone()).await?);
    let mut input = selected_branch
        .input_query(crate::client::AgentPageRequest::new(1, 64, None).map_err(agent)?)
        .await
        .map_err(agent)?;
    let preview = loop {
        match input.next().await.map_err(agent)? {
            Some(crate::client::InputPreviewStep::Progress) => {}
            Some(crate::client::InputPreviewStep::Pending { .. }) => {
                return Err(Error::Invalid("preview returned pending ranges".into()));
            }
            Some(crate::client::InputPreviewStep::Selected(preview)) => break preview,
            None => return Err(Error::Invalid("historical input query ended early".into())),
        }
    };
    assert_eq!(
        preview.as_ref().map(|value| value.text.as_str()),
        Some("first decision")
    );
    assert!(input.next().await.map_err(agent)?.is_none());
    let mut baseline = selected_branch
        .diff_query(
            crate::client::AgentPageRequest::new(2, 2048, None).map_err(agent)?,
            crate::client::DiffProjection::Summary,
        )
        .await
        .map_err(agent)?;
    let Some(crate::client::DiffQueryEvent::Snapshot(snapshot)) =
        baseline.next().await.map_err(agent)?
    else {
        return Err(Error::Invalid(
            "missing historical fork diff snapshot".into(),
        ));
    };
    assert_eq!(
        snapshot.base, snapshot.current,
        "a historical fork starts against its own creation cut, not the later checkout"
    );
    while let Some(event) = baseline.next().await.map_err(agent)? {
        assert!(
            matches!(event, crate::client::DiffQueryEvent::Progress),
            "a freshly forked historical volume has no changes"
        );
    }
    let inherited = ConversationEntry::decode(&source[0]).map_err(agent)?;
    let shown = selected_branch.turn_at(0, &inherited).await?;
    assert_eq!(shown.prompt.as_deref(), Some("first decision"));
    assert_eq!(shown.answer, "at cut");
    let future = ConversationEntry::decode(&source[1]).map_err(agent)?;
    assert!(
        selected_branch.turn_at(0, &future).await.is_err(),
        "an exact record proof cannot authorize another turn's files"
    );
    assert!(
        selected_branch.turn_at(1, &future).await.is_err(),
        "a proof beyond the inherited prefix cannot authorize future source files"
    );
    assert!(
        selected_branch.text(&future.output).await.is_err(),
        "inheritance never grants the source private volume"
    );
    let page =
        crate::local::sessions::page(&connection, crate::client::AgentPageRequest::default())
            .await?;
    assert_eq!(
        page.items
            .iter()
            .find(|item| item.agent == branch)
            .and_then(|item| item.title.as_deref()),
        Some("first decision")
    );
    let project = state
        .volumes
        .iter()
        .find(|binding| binding.name == "project")
        .ok_or_else(|| Error::Invalid("missing branch project".into()))?;
    let branched_workspace = workspace_ref(
        project.volume()?.provider().clone(),
        &project.volume()?.storage_name(&mut None)?,
    )?;
    assert_eq!(
        harness
            .files
            .read(&branched_workspace, None, "/plan.md", 64, None)
            .await?
            .as_ref(),
        b"at cut"
    );
    assert_eq!(
        harness
            .files
            .read(&workspace, None, "/plan.md", 64, None)
            .await?
            .as_ref(),
        b"after cut"
    );
    assert_eq!(
        records(agents.provider(), root.conversation().map_err(agent)?).await?,
        source
    );
    let AgentReply::Root { agent: replay } = connection.execute(&request).await? else {
        return Err(Error::Invalid(
            "historical replay returned another reply".into(),
        ));
    };
    assert_eq!(replay, branch);
    let mut changed = request;
    if let AgentCommand::ForkSession { at, .. } = &mut changed {
        *at = 1;
    }
    assert!(
        connection.execute(&changed).await.is_err(),
        "one intent cannot select another cut"
    );
    let AgentReply::Root { agent: repeated } = connection
        .execute(&AgentCommand::ForkSession {
            peer: true,
            agent: branch,
            id: crate::agent::IntentId::try_from("repeated-rewind").map_err(agent)?,
            at: 0,
        })
        .await?
    else {
        return Err(Error::Invalid(
            "repeated rewind returned another reply".into(),
        ));
    };
    assert_eq!(
        records(agents.provider(), repeated.conversation().map_err(agent)?).await?,
        source[..1]
    );
    assert_eq!(
        provider.requests().len(),
        2,
        "creating a branch must never dispatch the model"
    );
    let selected = connection.select(repeated.clone()).await?;
    let queued = Conversation::open(selected);
    assert!(
        !queued.phase().await?.in_turn(),
        "adoption alone stays idle"
    );
    let inbox = queued
        .send("cancel-before-resume", "do not run this queued prompt")
        .await?;
    let (mut queued_events, phase) = queued.observe_with_phase(Some(1)).await?;
    assert!(
        phase.in_turn(),
        "unread adoption must not hide accepted work from cancellation"
    );
    assert_eq!(queued.cancel().await?, Some(inbox));
    crate::local::sessions::pause_subtree(&connection, repeated.clone(), false).await?;
    queued.wait_current_input().await?;
    assert_eq!(queued.settled().await?, 1);
    assert!(matches!(
        queued.reply("cancel-before-resume", inbox).await?,
        crate::agent::Reply::Dropped
    ));
    assert_eq!(
        provider.requests().len(),
        2,
        "cancelled queued work must never dispatch"
    );
    loop {
        if let crate::local::Event::Status(status) = queued_events.next().await? {
            assert_eq!(status.phase, crate::agent::Phase::Idle);
            break;
        }
    }
    drop(queued_events);
    let mut observed = ObservedConversation::observe(queued, Some(1)).await?;
    observed
        .send("branch-input", "continue the selected decision")
        .await?;
    assert_eq!(provider.requests().len(), 3);
    assert_eq!(
        records(agents.provider(), root.conversation().map_err(agent)?).await?,
        source
    );
    let source_conversation = Conversation::open(connection.select(root.clone()).await?);
    let mut closing = source_conversation.observe(None).await?;
    crate::local::sessions::archive(&connection.select(root.clone()).await?, "archive-source")
        .await?;
    loop {
        if let crate::local::Event::Status(status) = closing.next().await?
            && matches!(
                status.phase,
                crate::agent::Phase::Done {
                    outcome: crate::agent::AgentOutcome::Archived
                }
            )
        {
            break;
        }
    }
    let archived_branch =
        crate::local::sessions::fork(&connection, root.clone(), 1, "fork-archived-source").await?;
    assert_eq!(
        Conversation::open(archived_branch)
            .turn_at(1, &future)
            .await?
            .answer,
        "later"
    );
    assert!(agents.state(root).await.map_err(agent)?.closed);
    assert_eq!(
        provider.requests().len(),
        3,
        "forking retained archived history cannot dispatch a model"
    );
    Ok(())
}

#[tokio::test]
async fn session_diff_queries_pin_endpoints_and_keep_client_lifetimes_independent() -> Result<()> {
    use crate::client::{AgentPageRequest, DiffProjection, DiffQueryEvent};
    use crate::filesystem::{WorkspaceMutation, workspace_ref};
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = super::scripted([super::ScriptedTurn::Text("unused".into())]);
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project.clone(), provider.clone())?
                .without_default_tools()
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let original = workspace_ref(
        project.provider().clone(),
        &project.storage_name(&mut None)?,
    )?;
    harness
        .files
        .apply(
            &original,
            None,
            &[WorkspaceMutation::PutFile {
                path: "/plan.md".into(),
                bytes: acyclic_stream::OwnedBytes::copy_from_slice(b"base\n"),
            }],
            &crate::IdempotencyKey::new("diff-base")?,
        )
        .await?;
    let directory_connection = harness.connect_sessions().await?;
    let selected =
        crate::local::sessions::create(&directory_connection, "diff-session", None).await?;
    let conversation = Conversation::open(selected.clone());
    let (agents, _) = harness.agents().await?;
    let state = agents.state(selected.root()).await.map_err(agent)?;
    let project = state
        .volumes
        .iter()
        .find(|binding| binding.name == "project")
        .ok_or_else(|| Error::Invalid("missing selected project".into()))?;
    let workspace = workspace_ref(
        project.volume()?.provider().clone(),
        &project.volume()?.storage_name(&mut None)?,
    )?;
    harness
        .files
        .apply(
            &workspace,
            None,
            &[WorkspaceMutation::PutFile {
                path: "/plan.md".into(),
                bytes: acyclic_stream::OwnedBytes::from("changed\n".repeat(32).into_bytes()),
            }],
            &crate::IdempotencyKey::new("diff-change")?,
        )
        .await?;
    let page = || AgentPageRequest::new(2, 2048, None).map_err(agent);
    let mut patches = conversation
        .diff_query(page()?, DiffProjection::Patch)
        .await
        .map_err(agent)?;
    let Some(DiffQueryEvent::Snapshot(first)) = patches.next().await.map_err(agent)? else {
        return Err(Error::Invalid("missing patch snapshot".into()));
    };
    let first_page = loop {
        match patches.next().await.map_err(agent)? {
            Some(DiffQueryEvent::Page(items)) => break items,
            Some(DiffQueryEvent::Progress) => {}
            _ => return Err(Error::Invalid("missing first patch page".into())),
        }
    };
    assert!(
        first_page.last().is_some_and(|item| item.next.is_some()),
        "two rows must leave a real file continuation"
    );
    let mut summary = conversation
        .diff_query(page()?, DiffProjection::Summary)
        .await
        .map_err(agent)?;
    let Some(DiffQueryEvent::Snapshot(second)) = summary.next().await.map_err(agent)? else {
        return Err(Error::Invalid("missing summary snapshot".into()));
    };
    assert_eq!(first.current, second.current);
    // Replacing one client releases only that original query, including when
    // it has an unfinished file. The other client's source stays usable.
    let mut partial = conversation
        .diff_query(page()?, DiffProjection::Patch)
        .await
        .map_err(agent)?;
    assert!(matches!(
        partial.next().await.map_err(agent)?,
        Some(DiffQueryEvent::Snapshot(_))
    ));
    loop {
        match partial.next().await.map_err(agent)? {
            Some(DiffQueryEvent::Page(items)) => {
                assert!(items.last().is_some_and(|item| item.next.is_some()));
                break;
            }
            Some(DiffQueryEvent::Progress) => {}
            _ => return Err(Error::Invalid("missing partial file".into())),
        }
    }
    drop(partial);
    harness
        .files
        .apply(
            &workspace,
            None,
            &[WorkspaceMutation::PutFile {
                path: "/later.md".into(),
                bytes: acyclic_stream::OwnedBytes::copy_from_slice(b"later\n"),
            }],
            &crate::IdempotencyKey::new("diff-later")?,
        )
        .await?;
    let mut patch = String::new();
    for item in first_page {
        patch.push_str(&item.patch);
    }
    while let Some(event) = patches.next().await.map_err(agent)? {
        match event {
            DiffQueryEvent::Page(items) => {
                for item in items {
                    assert_eq!(
                        item.path, "plan.md",
                        "later generations cannot enter a pinned query"
                    );
                    patch.push_str(&item.patch);
                }
            }
            DiffQueryEvent::Progress => {}
            _ => return Err(Error::Invalid("unexpected patch query event".into())),
        }
    }
    assert!(patch.contains("-base\n"));
    assert_eq!(patch.matches("+changed\n").count(), 32);
    let mut totals = crate::client::DiffSummary::default();
    while let Some(event) = summary.next().await.map_err(agent)? {
        match event {
            DiffQueryEvent::Summary(value) => totals = value,
            DiffQueryEvent::Progress => {}
            _ => return Err(Error::Invalid("summary transferred patch bytes".into())),
        }
    }
    assert_eq!(
        (totals.files, totals.added, totals.removed, totals.omitted),
        (1, 32, 1, 0)
    );
    let mut fresh = conversation
        .diff_query(page()?, DiffProjection::Summary)
        .await
        .map_err(agent)?;
    let mut fresh_totals = crate::client::DiffSummary::default();
    while let Some(event) = fresh.next().await.map_err(agent)? {
        match event {
            DiffQueryEvent::Snapshot(snapshot) => assert_ne!(snapshot.current, first.current),
            DiffQueryEvent::Summary(value) => fresh_totals = value,
            DiffQueryEvent::Progress => {}
            _ => return Err(Error::Invalid("summary transferred patch bytes".into())),
        }
    }
    assert_eq!(
        (fresh_totals.files, fresh_totals.added, fresh_totals.removed),
        (2, 33, 1)
    );
    assert_eq!(
        provider.request_count(),
        0,
        "query lifetime never starts a model"
    );
    let tree_only = harness.connect().await?;
    let refused = tree_only
        .controls
        .diff_query(selected.root().clone(), page()?, DiffProjection::Summary)
        .await;
    match refused {
        Err(acyclic_harness_client::AgentExchangeError::Session(StreamError::AccessDenied)) => {}
        Ok(mut query) => assert!(matches!(
            query.next().await,
            Err(acyclic_harness_client::AgentExchangeError::Session(
                StreamError::AccessDenied
            ))
        )),
        Err(error) => return Err(agent(error)),
    }
    Ok(())
}

#[tokio::test]
async fn archive_reactivates_paused_roots_and_open_descendants_for_closing() -> Result<()> {
    use crate::agent::{Delivered, InboxRecord, IntentId, Sender, SpawnOutcome, SpawnRequest};
    use crate::client::{AgentCommand, AgentReply};
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = super::scripted(std::iter::empty::<super::ScriptedTurn>());
    let harness = Arc::new(
        super::open_scripted(
            volume_config(directory.path(), project, provider.clone())?.without_default_tools(),
        )
        .await?,
    );
    let (agents, root) = harness.agents().await?;
    let connection = harness.connect().await?;
    let profile = harness
        .agents
        .get()
        .ok_or_else(|| Error::Invalid("missing host".into()))?
        .server()
        .bound()
        .child_profile(root, "worker")
        .await
        .map_err(agent)?;
    let by = Sender::User {
        principal: "archive-control".into(),
    };
    let record = InboxRecord::Spawn(SpawnRequest {
        by: by.clone(),
        id: IntentId::try_from("idle-child").map_err(agent)?,
        brief: None,
        profile,
        history: None,
    });
    let Delivered::Committed { inbox } = agents.send(root, &by, &record).await.map_err(agent)?
    else {
        return Err(Error::Invalid("parent was closed".into()));
    };
    let (intent, _) = crate::agent::requested_child(root, inbox).map_err(agent)?;
    let SpawnOutcome::Spawned { child } =
        agents.spawned(root, inbox, &intent).await.map_err(agent)?
    else {
        return Err(Error::Invalid("child was refused".into()));
    };
    agents.pause(&child, true).await.map_err(agent)?;
    agents.pause(root, true).await.map_err(agent)?;
    let parent = Conversation::open(connection.clone());
    let descendant = Conversation::open(connection.select(child.clone()).await?);
    let mut parent_events = parent.observe(None).await?;
    let mut child_events = descendant.observe(None).await?;
    assert!(matches!(
        connection
            .execute(&AgentCommand::Archive {
                agent: root.clone(),
                id: IntentId::try_from("archive-paused-tree").map_err(agent)?
            })
            .await?,
        AgentReply::Delivered { .. }
    ));
    for events in [&mut parent_events, &mut child_events] {
        loop {
            if let crate::local::Event::Status(status) = events.next().await?
                && let crate::agent::Phase::Done { outcome } = status.phase
            {
                assert_eq!(outcome, crate::agent::AgentOutcome::Archived);
                break;
            }
        }
    }
    assert!(agents.state(root).await.map_err(agent)?.closed);
    assert!(agents.state(&child).await.map_err(agent)?.closed);
    assert_eq!(
        provider.request_count(),
        0,
        "closing a paused tree must never start its model"
    );
    Ok(())
}

/// Editing AGENTS.md between turns keeps the cached span (system block and
/// tools) of the first request byte-identical in the second, and appends the
/// edit as a notice the second request carries. Its host-only delivery
/// frontier survives reopen and prevents losing or repeating transitions.
#[tokio::test]
async fn an_instruction_edit_appends_a_notice_and_keeps_the_cached_prefix() -> Result<()> {
    use crate::test_support::ScriptedTurn;
    let provider = super::scripted(
        ["one", "two", "three", "four", "five"].map(|text| ScriptedTurn::Text(text.into())),
    );
    let directory = tempfile::tempdir().map_err(agent)?;
    let workspace = directory.path().join("work");
    std::fs::create_dir_all(workspace.join(".git")).map_err(agent)?;
    let open = || {
        Ok::<_, Error>(super::open_scripted(
            super::checkout_config(&workspace, (super::model()?, provider.clone()))?
                .state_directory(directory.path().join("state")),
        ))
    };
    let harness = Arc::new(open()?.await?);
    let checkout = harness.workspace.as_ref().expect("checkout").path.clone();
    std::fs::write(checkout.join("AGENTS.md"), b"RULE_ONE\n").map_err(agent)?;
    let mut client = ObservedConversation::open(&harness).await?;
    assert_eq!(
        client.send("one", "FIRST_PROMPT").await?.outcome,
        crate::agent::TurnOutcome::Completed,
        "the instruction proof must reach model dispatch"
    );
    std::fs::write(checkout.join("AGENTS.md"), b"RULE_TWO\n").map_err(agent)?;
    client.send("two", "SECOND_PROMPT").await?;
    let (agents, root) = harness.agents().await?;
    let root = root.clone();
    let state = agents.state(&root).await.map_err(agent)?;
    let operation = crate::context::ContextEpoch::operation(root.as_str(), state.checkpoint)?;
    let frontier_path = format!("context/told/{operation}.json");
    let content = super::journal_content(&harness, &root)?;
    let (frontier, bytes, _) = content
        .reader
        .read_private_path(
            &content.volume,
            "context/told",
            &frontier_path,
            None,
            None,
            crate::conversation::ContentAccess::default(),
        )?
        .await?
        .ok_or_else(|| Error::NotFound("journal context delivery frontier".into()))?;
    assert_eq!(frontier.volume(), &content.volume);
    let private = state
        .volumes
        .iter()
        .find(|binding| binding.role == crate::agent::WorkspaceRole::Private)
        .ok_or_else(|| Error::NotFound("private volume".into()))?
        .volume()?;
    assert_ne!(frontier.volume(), private);
    let delivered: serde_json::Value = serde_json::from_slice(&bytes).map_err(agent)?;
    assert_eq!(delivered["sent"], 1);
    assert!(delivered["pending"].is_object());
    assert!(matches!(
        harness.agents.get().unwrap().server().bound()
            .read_file(&root, &frontier, 0, frontier.descriptor().byte_length()).await,
        Err(crate::agent::Error::Turn(error)) if matches!(*error, Error::Unauthorized(_))
    ));
    drop(content);
    // Its lazy child readers retain the original stream provider across reopen.
    drop(state);
    let request = |index| {
        provider
            .request(index)
            .ok_or_else(|| Error::NotFound(format!("model request {index}")))
    };
    let (first, second) = (request(0)?, request(1)?);
    let prefix = |request: &crate::model::ModelRequest| {
        let system = request
            .messages
            .iter()
            .take_while(|message| matches!(message.role, crate::model::ModelRole::System))
            .collect::<Vec<_>>();
        serde_json::to_value((system, &request.tools)).map_err(agent)
    };
    assert!(
        super::request_text(&harness, &first)
            .await?
            .contains("RULE_ONE")
    );
    assert_eq!(prefix(&second)?, prefix(&first)?);
    let appended = super::request_text(&harness, &second).await?;
    assert!(appended.contains("RULE_TWO"), "{appended}");

    // Return to the frozen value while closed, then repeat the same transition.
    // The durable frontier must correct B -> A and give A -> B a new intent.
    settled(&harness, &root).await?;
    drop(client);
    Arc::try_unwrap(harness)
        .map_err(|_| Error::Conflict("instruction owner is still borrowed".into()))?
        .disconnect()
        .await?;
    std::fs::write(checkout.join("AGENTS.md"), b"RULE_ONE\n").map_err(agent)?;
    let harness = Arc::new(open()?.await?);
    let mut client = ObservedConversation::open(&harness).await?;
    client.send("three", "AFTER_REOPEN").await?;
    let checkout = harness.workspace.as_ref().expect("checkout").path.clone();
    std::fs::write(checkout.join("AGENTS.md"), b"RULE_TWO\n").map_err(agent)?;
    client.send("four", "REPEAT_EDIT").await?;
    client.send("five", "UNCHANGED").await?;
    for (index, expected) in [
        (2, vec!["RULE_TWO\n", "RULE_ONE\n"]),
        (3, vec!["RULE_TWO\n", "RULE_ONE\n", "RULE_TWO\n"]),
        (4, vec!["RULE_TWO\n", "RULE_ONE\n", "RULE_TWO\n"]),
    ] {
        let current = request(index)?;
        assert_eq!(prefix(&current)?, prefix(&first)?);
        let edits = current
            .messages
            .iter()
            .filter_map(|message| message.content.notice_parts())
            .flat_map(|(notices, _)| notices)
            .filter(|notice| notice.kind == crate::context::CONTEXT_CHANGED)
            .filter_map(|notice| notice.body.get("text").and_then(serde_json::Value::as_str))
            .collect::<Vec<_>>();
        assert_eq!(
            edits, expected,
            "request {index} must deliver each transition once"
        );
    }
    let content = super::journal_content(&harness, &root)?;
    let (latest, bytes, _) = content
        .reader
        .read_private_path(
            &content.volume,
            "context/told",
            &frontier_path,
            None,
            None,
            crate::conversation::ContentAccess::default(),
        )?
        .await?
        .ok_or_else(|| Error::NotFound("reopened context delivery frontier".into()))?;
    assert_eq!(latest.volume(), frontier.volume());
    assert_ne!(latest, frontier);
    let delivered: serde_json::Value = serde_json::from_slice(&bytes).map_err(agent)?;
    assert_eq!(delivered["sent"], 3);
    Ok(())
}
