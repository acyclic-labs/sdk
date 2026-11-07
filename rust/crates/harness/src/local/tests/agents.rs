//! The local agent host: a prompt is one staged file and one inbox commit, and
//! the root agent's actor runs its turn on the stock executor.

use super::{ObservedConversation, ScriptedModelProvider, text, volume, volume_config};
use crate::{
    Error, LocalHarness, Result,
    agent::{AgentPath, AgentRecord, ConversationEntry, Reply, StatusRecord, TurnInput},
    local::agent_conversation::Conversation,
};
use acyclic_stream::{ReadRequest, StreamError, StreamPath, StreamProvider};
use futures::TryStreamExt;
use std::{path::Path, sync::Arc};

/// The root conversation `harness` serves to its own in-process session.
async fn conversation(harness: &Arc<LocalHarness>) -> Result<Arc<Conversation>> {
    Ok(Conversation::open(harness.connect().await?))
}

#[tokio::test]
async fn approval_ranges_retain_the_original_status_until_every_chunk_flushes() -> Result<()> {
    use crate::model::{ModelEvent, ToolCall};
    use crate::test_support::{ScriptedEvent, ScriptedTurn};
    use acyclic_harness_client::{EventProjection, MAX_READ_BYTES};

    let arguments = serde_json::json!({"text": "request".repeat(12_000)});
    let provider = super::scripted([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                "echo-request".into(),
                "example.echo".into(),
                arguments.clone(),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: serde_json::json!({}),
            }),
        ]),
        ScriptedTurn::Text("approved".into()),
    ]);
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project, provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled)
                .tool(super::tool("example.echo"))
                .approval_policy(crate::approvals::ApprovalPolicy::default()),
        )
        .await?,
    );
    let client = conversation(&harness).await?;
    let mut projection = EventProjection::new("approval-export".into());
    let mut observed = ObservedConversation {
        conversation: Arc::clone(&client),
        observer: client.replay(&projection)?,
    };
    let inbox = client.send("approval-input", "echo the request").await?;
    let (id, published_binding, delivery) = loop {
        let (event, delivery) = observed.observer.next_projected().await?;
        if let Some(crate::local::Event::Status(StatusRecord {
            phase: crate::agent::Phase::WaitingApproval { id, binding },
            ..
        })) = event?
        {
            break (id, binding, delivery);
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
    assert_eq!(provider.request_count(), 1);
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
            .decide_with_binding(&id, different, true)
            .await
            .is_err()
    );
    assert_eq!(provider.request_count(), 1);
    client
        .decide_with_binding(&id, binding.clone(), true)
        .await?;
    let (agents, root) = harness.agents().await?;
    observed.settled(inbox).await?;
    assert!(client.pending_approval(&id).await?.is_none());
    assert!(client.approval_prompt(&id).await?.is_none());
    let requests = provider.request_count();
    let (historical, saved_length) = client.approval_request(sequence, 0).await?;
    assert_eq!(saved_length, length);
    assert_eq!(historical, bytes[..MAX_READ_BYTES as usize]);
    assert_eq!(provider.request_count(), requests);
    client.decide_with_binding(&id, binding, true).await?;
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
    assert!(matches!(
        client.cancel().await,
        Err(Error::Conflict(reason)) if reason.contains("archived")
    ));
    assert_eq!(provider.request_count(), requests);
    Ok(())
}

fn agent(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
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

async fn records(stream: &impl StreamProvider, path: StreamPath) -> Result<Vec<Vec<u8>>> {
    match stream
        .read(ReadRequest {
            key: None,
            path,
            from: 0,
            limit: 16,
            at_least: Default::default(),
        })
        .await
    {
        Ok(records) => Ok(records
            .map_ok(|record| record.value.to_vec())
            .try_collect()
            .await
            .map_err(agent)?),
        Err(StreamError::NotFound) => Ok(Vec::new()),
        Err(error) => Err(agent(error)),
    }
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
        loop {
            let (settled, cut) = agents
                .input_progress(root, inbox.saturating_sub(1))
                .await
                .map_err(agent)?;
            if settled {
                return records(&**agents.provider(), root.conversation().map_err(agent)?).await;
            }
            let mut changes =
                acyclic_stream::records_only(agents.provider().follow(cut).await.map_err(agent)?);
            changes.try_next().await.map_err(agent)?.ok_or_else(|| {
                Error::Storage("agent state follow ended before settlement".into())
            })?;
        }
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

#[tokio::test]
async fn a_prompt_runs_one_root_agent_turn_and_replays_exactly() -> Result<()> {
    use acyclic_harness_client::{AgentCommand, AgentReply, encode_agent_reply};

    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = text();
    let harness = Arc::new(
        LocalHarness::open(
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
    let mut usage = client.usage_query();
    while !usage.synchronized() {
        usage.advance().await?;
    }
    assert_eq!(usage.totals().calls, 0);
    let inbox = client.send("hello", "hello").await?;
    let conversation = root.conversation().map_err(agent)?;
    let entries = settled(&harness, &root).await?;
    assert_eq!(entries.len(), 1);
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    assert_eq!(entry.inbox, inbox);
    assert!(matches!(entry.input, TurnInput::Message { .. }));
    assert_eq!(provider.request_count(), 1);
    assert_eq!(
        client.turn(&entry).await?,
        (Some("hello".into()), "done".into())
    );
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
    while usage.totals().completed == 0 {
        usage.advance().await?;
    }
    assert_eq!(usage.totals().calls, 1);
    assert_eq!(usage.totals().completed, 1);
    assert!(usage.totals().currency.is_none());
    assert_eq!(usage.totals().token_nano_units.value(), None);
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
    assert_eq!(completed.usage.agent_path.as_deref(), Some(root.as_str()));
    assert_eq!(
        completed.usage.sampling,
        Some(crate::model::ModelSampling::default())
    );
    assert_eq!(completed.usage.max_output_tokens, None);
    assert!(rows.next().is_none());
    // The real journal's current format crosses the same shared boundary in
    // both directions. No other version or agent can enter this usage view.
    let command = AgentCommand::Usage {
        agent: root.clone(),
        input: inbox,
        after: 0,
        wait: false,
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
    // The same intent replays its commit at the same sequence; no second turn runs.
    assert_eq!(client.send("hello", "hello").await?, inbox);
    assert_eq!(records(&**agents.provider(), conversation).await?.len(), 1);
    Ok(())
}

/// The model's real channel owns progress; dropping it must close the same
/// Session that carries the local commands and follows, retaining queued input.
#[tokio::test]
async fn lost_model_producer_closes_local_session_without_settling_input() -> Result<()> {
    use std::sync::Mutex;
    use tokio::sync::oneshot;

    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let (entered, entering) = oneshot::channel();
    let (producer, result) = oneshot::channel::<()>();
    let gate = Mutex::new(Some((entered, result)));
    let provider = ScriptedModelProvider::new([super::ScriptedTurn::Events(Vec::new())])
        .with_context_capacity(super::capacity())
        .with_dispatch_observer(move |_| {
            let gate = gate.lock().unwrap().take();
            Box::pin(async move {
                let (entered, result) =
                    gate.ok_or_else(|| Error::Invalid("second dispatch".into()))?;
                let _ = entered.send(());
                result
                    .await
                    .map_err(|_| Error::Storage("model producer closed".into()))?;
                Err(Error::Invalid("model gate unexpectedly released".into()))
            })
        });
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project, Arc::new(provider))?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let connection = harness.connect().await?;
    let client = Conversation::open(connection.clone());
    let mut events = client.observe(Some(0)).await?;
    let inbox = client.send("lost-producer", "hello").await?;
    entering.await.map_err(agent)?;
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
    assert_eq!(usage.totals().calls, 1);
    assert_eq!(usage.totals().completed, 0);
    let before = usage.totals().clone();
    let waiting = async {
        loop {
            match events.next().await? {
                super::super::Event::Turn(_) => {
                    return Err::<(), _>(Error::Conflict("unsettled model produced a turn".into()));
                }
                super::super::Event::Status(_) | super::super::Event::Output => {}
            }
        }
    };
    tokio::pin!(waiting);
    assert!(
        futures::poll!(&mut waiting).is_pending(),
        "live producer must retain the wait"
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
    assert_eq!(
        usage.totals(),
        &before,
        "source loss changed the admitted projection"
    );
    // The session closed by the failed runtime reports that failure, not a bare `unavailable`.
    let observed = waiting.await;
    assert!(
        matches!(&observed, Err(Error::Storage(reason)) if reason.contains("model producer closed")),
        "observation lost the runtime cause: {observed:?}"
    );
    let hosted = harness.agents.get().unwrap();
    assert!(
        matches!(hosted.runtime.completion().await, Err(Error::Storage(reason)) if reason.contains("model producer closed"))
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
    assert!(
        records(
            &**hosted.agents().provider(),
            hosted.root().conversation().map_err(agent)?
        )
        .await?
        .is_empty()
    );
    assert!(
        matches!(harness.connect().await, Err(Error::Storage(_))),
        "late clients observe the already closed runtime"
    );
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
        .await?;
    Ok(page
        .entries
        .iter()
        .map(|entry| String::from_utf8_lossy(entry.name.as_bytes()).into_owned())
        .collect())
}

#[tokio::test]
async fn a_shell_call_stages_its_output_on_the_private_volume() -> Result<()> {
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
    let mut config = super::checkout_config(directory.path(), (super::model()?, provider))?;
    config.served = Some(admission.clone());
    let harness = Arc::new(LocalHarness::open(config).await?);
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
    let state = agents.state(&root).await.map_err(agent)?;
    // Each turn's calls share one directory, named by the turn.
    let turn = crate::agent::turn_id(&state, &entry.input).map_err(agent)?;
    let private = &state
        .volumes
        .iter()
        .find(|binding| binding.name == crate::filesystem::PRIVATE_VOLUME)
        .ok_or_else(|| Error::NotFound("private volume".into()))?
        .volume;
    let output = harness
        .client_reader()?
        .read(&entry.output, 1 << 20, None, None)?
        .await?;
    let root = names(&harness, private, "").await?;
    assert!(
        root.iter().any(|name| name == "tools"),
        "no tools directory in {root:?}; turn output {}",
        String::from_utf8_lossy(&output)
    );
    let turn = format!("tools/{turn}");
    let calls = names(&harness, private, &turn).await?;
    let [call] = calls.as_slice() else {
        return Err(Error::Conflict(format!(
            "one shell call staged, got {calls:?}"
        )));
    };
    assert_eq!(
        names(&harness, private, &format!("{turn}/{call}")).await?,
        ["request.json", "result.json", "stdout"]
    );
    assert_eq!(
        names(&harness, private, &format!("{turn}/{call}/stdout")).await?,
        ["0000000000000000"]
    );
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
            &authority.private.volume,
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
        if matches!(observer.next().await?, super::super::Event::Output) {
            observer.absorb().await?;
            if observer.output().contains("staged") {
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

/// A local HTTP peer that answers each request with its own ordinal, so a
/// hook that ran again would add different context.
#[cfg(unix)]
struct CountingServer {
    port: u16,
    calls: Arc<std::sync::atomic::AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

#[cfg(unix)]
impl Drop for CountingServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(unix)]
async fn counting_server() -> Result<CountingServer> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(agent)?;
    let port = listener.local_addr().map_err(agent)?.port();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let served = Arc::clone(&count);
    let task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let ordinal = served.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            let mut request = [0; 8192];
            let mut used: usize = 0;
            loop {
                let start = used.saturating_sub(3);
                match stream.read(&mut request[used..]).await {
                    Ok(0) | Err(_) => return,
                    Ok(read) => used += read,
                }
                if request[start..used]
                    .windows(4)
                    .any(|window| window == b"\r\n\r\n")
                {
                    break;
                }
            }
            let body = format!("HOOK-HTTP-{ordinal}");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            if stream.write_all(response.as_bytes()).await.is_err() {
                return;
            }
        }
    });
    Ok(CountingServer {
        port,
        calls: count,
        task,
    })
}

/// A turn whose `UserPromptSubmit` hook called an HTTP peer is interrupted
/// after the hook's outcome was recorded; the reopened agent replays the
/// turn from `hooks/{turn}/{op}.json`, with no new hook run or HTTP call,
/// and the model sees the identical recorded context.
#[cfg(unix)]
#[tokio::test]
async fn a_reopened_turn_replays_its_recorded_hook_outcome() -> Result<()> {
    let directory = tempfile::tempdir().map_err(agent)?;
    let peer = counting_server().await?;
    let (port, calls) = (peer.port, &peer.calls);
    let home = directory.path().join("home");
    std::fs::create_dir_all(home.join(".claude")).map_err(agent)?;
    let hook = format!("cat >/dev/null; curl -sS http://127.0.0.1:{port}/");
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
            Ok::<_, Error>(Arc::new(LocalHarness::open(config).await?))
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
    assert!(recorded.contains("HOOK-HTTP-1"), "{recorded}");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    // Crash: the turn is still waiting on the model when the harness drops.
    drop(observed);
    drop(harness);
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
    // The replay ran no hook and made no HTTP call.
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    // A re-dispatched request carries the identical recorded context.
    if let Some(request) = resumed.request(0) {
        let replayed = super::request_text(&harness, &request).await?;
        assert!(
            replayed.contains("HOOK-HTTP-1") && !replayed.contains("HOOK-HTTP-2"),
            "{replayed}"
        );
    }
    // The outcome is the one file recorded under the turn, beside its marker.
    let entry = ConversationEntry::decode(&entries[0]).map_err(agent)?;
    let state = agents.state(&root).await.map_err(agent)?;
    let turn = crate::agent::turn_id(&state, &entry.input).map_err(agent)?;
    let private = &state
        .volumes
        .iter()
        .find(|binding| binding.name == crate::filesystem::PRIVATE_VOLUME)
        .ok_or_else(|| Error::NotFound("private volume".into()))?
        .volume;
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
        LocalHarness::open(super::checkout_config(
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
    let (_directory, harness, _provider) = checkout_with(&[
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
    let (read, _) = client.turn(&entry).await?;
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
        LocalHarness::open(
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
    let child_project = &child_state
        .volumes
        .iter()
        .find(|binding| binding.name == "project")
        .ok_or_else(|| Error::NotFound("child project volume".into()))?
        .volume;
    assert_ne!(child_project, &project);
    assert_eq!(child_file.volume(), child_project);
    let child_scratch = &child_state
        .volumes
        .iter()
        .find(|binding| binding.name == "home")
        .ok_or_else(|| Error::NotFound("child scratch volume".into()))?
        .volume;
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
    assert!(context.contains("git_status"));
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
    // Negative control: the same files listed inline in the record, as the
    // message carried them before, exceed the stream's record bound.
    let inline = serde_json::to_vec(&listed).map_err(agent)?;
    assert!(
        inline.len() > crate::agent::MAX_RECORD_BYTES,
        "{} inline bytes fit a record",
        inline.len()
    );
    // The records stay one reference: as small as a one-file prompt's.
    let (agents, root) = harness.agents().await?;
    let entries = records(&**agents.provider(), root.conversation().map_err(agent)?).await?;
    let inbox = records(&**agents.provider(), root.inbox().map_err(agent)?).await?;
    for record in inbox.iter().chain(&entries) {
        assert!(record.len() < 4096, "{} record bytes", record.len());
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
        LocalHarness::open(
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

    // The next turn keeps copy names bounded even at the source path bound.
    // Naming a copy by that full path would exceed the same bound.
    use crate::conversation::MAX_PATH_BYTES;
    // Nested 200-byte components, then a file name, to one byte under the bound.
    let mut parents = Vec::new();
    let mut long = String::new();
    while MAX_PATH_BYTES - 1 - long.len() > 200 {
        long.push_str(&format!("{}{}/", parents.len() % 10, "d".repeat(199)));
        parents.push(format!("/{}", long.trim_end_matches('/')));
    }
    long.push_str(&"f".repeat(MAX_PATH_BYTES - 1 - long.len()));
    assert_eq!(long.len() + 1, MAX_PATH_BYTES);
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
    assert!(format!("{directory}/{long}").len() > MAX_PATH_BYTES);
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
        LocalHarness::open(
            volume_config(directory.path(), project.clone(), provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled)
                .limits(crate::conversation::Limits {
                    file_bytes: FILE_BYTES,
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
        LocalHarness::open(
            volume_config(directory.path(), project, provider.clone())?
                .compaction(crate::context::CompactionPolicy::Disabled),
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
    settlement(&mut observed, inbox).await?;
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
    assert!(
        client
            .turn(&refusal)
            .await?
            .1
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
    let inbox = client
        .restore("apply", 0, RestoreMode::Both, expected)
        .await?;
    settlement(&mut observed, inbox).await?;
    let entries = records(&**agents.provider(), root.conversation().map_err(agent)?).await?;
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
            .1
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
    use crate::filesystem::ForkCuts;
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
        LocalHarness::open(
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
            let cuts = crate::local::workspace::LazyForkCuts(Arc::clone(
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
    assert_eq!(client.turn(&inherited).await?.1, "answer 0");
    let saved =
        crate::filesystem::history_checkpoint(agents.provider().as_ref(), &child, 1).await?;
    assert_eq!(saved.context.tail, 2);
    assert_eq!(saved.context.path.as_str(), format!("{child}/context/1"));
    assert_eq!(
        records(&**agents.provider(), root.conversation().map_err(agent)?)
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
        .find(|binding| binding.volume.class() == crate::conversation::VolumeClass::Project)
        .ok_or_else(|| Error::Invalid("historical child has no project volume".into()))?;
    let child_workspace = fs
        .open_workspace(
            project.volume.storage_name(&mut None)?,
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
