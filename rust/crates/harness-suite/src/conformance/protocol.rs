//! Hosted Spawn, Approvals and reply vectors. Endpoint and in-process drivers
//! use these identical commands, fixtures and assertions.
use crate::conformance::{Backend, Result};
use acyclic_harness_client::{
    AgentBinding, AgentCommand, AgentControlFailure, AgentPageRequest, AgentReply, PreparedCommand,
    ReadTarget, encode_agent_reply,
};
use acyclic_harness_core::{
    agent::{
        AgentPath, AgentRecord as _, ConversationEntry, InboxRecord, IntentId, Phase, Reply,
        StatusRecord,
    },
    approvals::ApprovalPolicy,
    interaction::{ApprovalBinding, ApprovalSubject, Interaction},
    model::{ModelContent, ModelEvent, ToolCall},
    tool::ToolDefinition,
};
use acyclic_stream::{
    BoxProviderFuture, ProviderOwner, ProviderPlatform, ReadRequest, StreamCategory, StreamError,
    StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde_json::json;
use std::collections::BTreeSet;

pub mod embedded;
mod observations;

/// Fixture tool name, shared with protocol clients and actual guest artifacts.
pub const TOOL: &str = "conformance.echo";
/// Fixture's settled model reply.
pub const REPLY: &str = "conformance hosted reply";

/// The one deterministic model script; every backend supplies these events.
pub fn model_events(step: u32) -> Vec<ModelEvent> {
    vec![
        if step == 0 {
            ModelEvent::ToolCall(
                ToolCall::new("call-0".into(), TOOL.into(), json!({}))
                    .with_metadata(invocation_metadata()),
            )
        } else {
            ModelEvent::Content {
                delta: REPLY.into(),
            }
        },
        ModelEvent::Completed {
            metadata: serde_json::Value::Null,
        },
    ]
}

fn invocation_metadata() -> serde_json::Value {
    json!({"continuation": {"signature": "original provider continuation"}})
}

/// The shared internal fixture tool, whose actual executions are counted.
pub fn tool_definition() -> ToolDefinition {
    ToolDefinition {
        name: TOOL.into(),
        revision: "1".into(),
        description: "Echo".into(),
        input_schema: json!(true),
        output_schema: json!(true),
        projection_schema: json!(true),
    }
}

/// Only transport, explicit delivery and observations belong in a backend.
pub trait Endpoint: ProviderPlatform {
    /// Canonical host-published Actors address, without a command body.
    fn binding(&self) -> &[u8];
    /// Invoke through the production hosting entry with its verified credential.
    fn exchange(&self, invocation: Bytes) -> BoxProviderFuture<'_, Result<Bytes>>;
    /// Deliver this explicitly placed actor, without activating its children.
    fn drain<'a>(&'a self, agent: &'a AgentPath) -> BoxProviderFuture<'a, Result>;
    /// The same storage the real host reads and publishes to.
    fn provider(&self) -> ProviderOwner<dyn StreamProvider>;
    /// Actual executions of the shared internal fixture tool.
    fn effects(&self) -> BoxProviderFuture<'_, Result<u64>>;
}

/// Send one canonical command through an endpoint and decode its typed reply.
pub async fn call(
    endpoint: &dyn Endpoint,
    command: &AgentCommand,
) -> Result<std::result::Result<AgentReply, AgentControlFailure>> {
    let prepared = PreparedCommand::new(command)?;
    let binding = AgentBinding::new(endpoint.binding())?;
    let invocation = binding.prepare(&prepared)?;
    invocation
        .send(|invocation| endpoint.exchange(invocation))
        .await
}

async fn accepted(endpoint: &dyn Endpoint, command: &AgentCommand) -> Result<AgentReply> {
    call(endpoint, command)
        .await
        .and_then(|reply| reply.map_err(Into::into))
        .map_err(|error| format!("{} command: {error}", command.kind()).into())
}

async fn read(endpoint: &dyn Endpoint, agent: &AgentPath, target: ReadTarget) -> Result<Vec<u8>> {
    let reply = accepted(
        endpoint,
        &AgentCommand::Read {
            agent: agent.clone(),
            target,
            offset: 0,
        },
    )
    .await?;
    let (bytes, length) = reply.content()?;
    if bytes.len() as u64 != length {
        return Err("the shared protocol payload was truncated".into());
    }
    Ok(bytes)
}

async fn records(
    endpoint: &dyn Endpoint,
    path: acyclic_stream::StreamPath,
) -> Result<Vec<acyclic_stream::Record>> {
    match endpoint
        .provider()
        .read(ReadRequest {
            key: None,
            path,
            from: 0,
            limit: 64,
            at_least: Default::default(),
        })
        .await
    {
        Ok(records) => Ok(records.try_collect().await?),
        Err(error) if error.category() == StreamCategory::NotFound => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

/// All hosted protocol vectors, unchanged for every registered backend.
pub async fn verify(backend: &dyn Backend) -> Result {
    for human in [false, true] {
        let policy = if human {
            ApprovalPolicy::default()
        } else {
            ApprovalPolicy::accept_all("conformance")
        };
        let endpoint = backend.serving(policy).await.map_err(|error| {
            format!("opening hosted endpoint (manual approval {human}): {error}")
        })?;
        let endpoint = endpoint.as_ref();
        let AgentReply::Root { agent: root } = accepted(endpoint, &AgentCommand::Root {}).await?
        else {
            return Err("Root did not reply with the admitted root".into());
        };
        let AgentReply::Children { items, next } = accepted(
            endpoint,
            &AgentCommand::Children {
                agent: root.clone(),
                page: Default::default(),
            },
        )
        .await?
        else {
            return Err("Children reply shape differs".into());
        };
        if !items.is_empty() || next.is_some() {
            return Err("a fresh root has children".into());
        }

        let prompt = AgentCommand::Prompt {
            peer: None,
            agent: root.clone(),
            id: IntentId::try_from("prompt")?,
            body: ModelContent::Text("Call the shared tool, then reply".into()),
        };
        let (first, concurrent) =
            futures::join!(accepted(endpoint, &prompt), accepted(endpoint, &prompt));
        let (first, concurrent) = match (first, concurrent) {
            (Ok(first), Ok(concurrent)) => (first, concurrent),
            (first, concurrent) => {
                let describe = |reply: Result<AgentReply>| {
                    reply
                        .err()
                        .map_or_else(|| "committed".to_owned(), |error| error.to_string())
                };
                return Err(format!(
                    "concurrent prompt submissions (manual approval {human}) returned {} and {}",
                    describe(first),
                    describe(concurrent),
                )
                .into());
            }
        };
        if serde_json::to_value(&first)? != serde_json::to_value(concurrent)? {
            return Err("concurrent prompt retries changed their delivery receipt".into());
        }
        endpoint
            .drain(&root)
            .await
            .map_err(|error| format!("delivering prompt (manual approval {human}): {error}"))?;
        let replay = accepted(endpoint, &prompt)
            .await
            .map_err(|error| format!("replaying the prompt after delivery: {error}"))?;
        if serde_json::to_value(&first)? != serde_json::to_value(replay)? {
            return Err("an exact prompt retry changed its delivery receipt".into());
        }
        let approvals = AgentCommand::Approvals {
            agent: root.clone(),
            page: Default::default(),
        };
        let AgentReply::Approvals { mut items, next } = accepted(endpoint, &approvals).await?
        else {
            return Err("Approvals reply shape differs".into());
        };
        if next.is_some() || items.len() != usize::from(human) {
            return Err("the host did not apply its preset approval policy".into());
        }
        if human {
            if endpoint.effects().await? != 0 {
                return Err("an unapproved tool ran".into());
            }
            let pending = items.pop().ok_or("approval missing")?;
            let request_bytes = read(
                endpoint,
                &root,
                ReadTarget::Approval {
                    status: pending.status,
                },
            )
            .await?;
            if !matches!(
                serde_json::from_slice::<Interaction>(&request_bytes)?,
                Interaction::Approval {
                    operation_id,
                    action_digest,
                    subject: ApprovalSubject::Tool { tool, call_id, arguments, .. },
                    ..
                } if operation_id == pending.binding.operation_id
                    && action_digest == pending.binding.action_digest
                    && tool == TOOL && call_id == "call-0" && arguments == json!({})
            ) {
                return Err("the original approval request differs from its pending action".into());
            }
            let ApprovalBinding {
                operation_id,
                action_digest,
            } = pending.binding;
            let mut wrong = action_digest;
            let byte = wrong.first_mut().ok_or("empty action digest")?;
            *byte ^= 1;
            let decision = |digest| AgentCommand::Decide {
                agent: root.clone(),
                id: pending.id.clone(),
                binding: ApprovalBinding {
                    operation_id,
                    action_digest: digest,
                },
                verdict: true.into(),
                reason: None,
            };
            if call(endpoint, &decision(wrong)).await?.is_ok() || endpoint.effects().await? != 0 {
                return Err("a changed approval binding had an effect".into());
            }
            let exact = decision(action_digest);
            let receipt = accepted(endpoint, &exact).await?;
            endpoint
                .drain(&root)
                .await
                .map_err(|error| format!("delivering approval: {error}"))?;
            if serde_json::to_value(receipt)?
                != serde_json::to_value(accepted(endpoint, &exact).await?)?
            {
                return Err("a settled decision did not replay its exact receipt".into());
            }
            if request_bytes
                != read(
                    endpoint,
                    &root,
                    ReadTarget::Approval {
                        status: pending.status,
                    },
                )
                .await?
            {
                return Err("settlement changed the original approval request bytes".into());
            }
        }
        endpoint
            .drain(&root)
            .await
            .map_err(|error| format!("finishing hosted turn (manual approval {human}): {error}"))?;
        if endpoint.effects().await? != 1 {
            return Err("a turn's tool did not run exactly once".into());
        }
        let conversation = records(endpoint, root.conversation()?).await?;
        if conversation.len() != 1 {
            return Err("a prompt retry produced another turn".into());
        }
        let record = conversation.first().ok_or("missing turn")?;
        let entry = ConversationEntry::decode(&record.value)?;
        let request_reply = AgentCommand::Reply {
            agent: root.clone(),
            id: IntentId::try_from("prompt")?,
            inbox: entry.inbox,
        };
        let answered = accepted(endpoint, &request_reply).await?;
        let AgentReply::Reply {
            reply:
                Reply::Answered {
                    sequence,
                    entry: settled,
                    ..
                },
        } = &answered
        else {
            return Err("the settled prompt has no correlated reply".into());
        };
        if *sequence != record.sequence
            || settled != &entry
            || serde_json::to_value(&answered)?
                != serde_json::to_value(accepted(endpoint, &request_reply).await?)?
        {
            return Err("the committed prompt reply changed".into());
        }
        let wrong_reply = AgentCommand::Reply {
            agent: root.clone(),
            id: IntentId::try_from("another-prompt")?,
            inbox: entry.inbox,
        };
        if !matches!(call(endpoint, &wrong_reply).await?, Err(error) if error.code() == "agent_conflict")
        {
            return Err("wrong-prompt reply did not report agent_conflict".into());
        }
        let output_bytes = read(
            endpoint,
            &root,
            ReadTarget::Conversation {
                sequence: record.sequence,
                file: entry.output.clone(),
            },
        )
        .await?;
        entry.output.descriptor().verify(&output_bytes, &mut None)?;
        let output: serde_json::Value = serde_json::from_slice(&output_bytes)?;
        if output.get("text").and_then(serde_json::Value::as_str) != Some(REPLY) {
            return Err("the guest's immutable reply differs".into());
        }
        let uncommitted_output = AgentCommand::Read {
            agent: root.clone(),
            target: ReadTarget::Conversation {
                sequence: record
                    .sequence
                    .checked_add(1)
                    .ok_or("conversation cursor overflow")?,
                file: entry.output.clone(),
            },
            offset: 0,
        };
        let expected_refusal = acyclic_harness_core::agent::Error::Record(
            "conversation file proof is outside the committed prefix",
        );
        if !matches!(
            call(endpoint, &uncommitted_output).await?,
            Err(AgentControlFailure::Refused { code, reason })
                if code == expected_refusal.code() && reason == expected_refusal.to_string()
        ) {
            return Err(
                "an uncommitted conversation did not report its committed-prefix refusal".into(),
            );
        }
        // The owning session can also read its published conversation files.
        if read(
            endpoint,
            &root,
            ReadTarget::File {
                file: entry.output.clone(),
            },
        )
        .await?
            != output_bytes
        {
            return Err("the owning session read changed its published conversation output".into());
        }
        observations::verify(endpoint, &root, entry.inbox)
            .await
            .map_err(|error| format!("observing hosted turn: {error}"))?;
        let AgentReply::Approvals { items, next } = accepted(endpoint, &approvals).await? else {
            return Err("Approvals reply shape differs after settlement".into());
        };
        if !items.is_empty() || next.is_some() {
            return Err("a settled approval is still pending".into());
        }

        let spawn = AgentCommand::Spawn {
            parent: root.clone(),
            id: IntentId::try_from("spawn")?,
            brief: ModelContent::Text("Run only after explicit placement".into()),
            role: "worker".into(),
        };
        let (spawned, concurrent) =
            futures::try_join!(accepted(endpoint, &spawn), accepted(endpoint, &spawn))?;
        if serde_json::to_value(&spawned)? != serde_json::to_value(concurrent)? {
            return Err("concurrent spawn retries changed their delivery receipt".into());
        }
        let AgentReply::Spawned { agent: child, .. } = &spawned else {
            return Err("Spawn reply shape differs".into());
        };
        if serde_json::to_value(&spawned)?
            != serde_json::to_value(accepted(endpoint, &spawn).await?)?
        {
            return Err("an exact spawn retry changed its receipt".into());
        }
        let changed = AgentCommand::Spawn {
            parent: root.clone(),
            id: IntentId::try_from("spawn")?,
            brief: ModelContent::Text("A conflicting prompt".into()),
            role: "worker".into(),
        };
        if call(endpoint, &changed).await?.is_ok() {
            return Err("changed spawn prompt was accepted".into());
        }
        let inbox = records(endpoint, child.inbox()?).await?;
        let adoption = inbox.last().ok_or("spawn omitted child adoption")?;
        let InboxRecord::ForkAdopted(adopted) = InboxRecord::decode(&adoption.value)? else {
            return Err("spawn did not end at its adoption record".into());
        };
        let parent_inbox = records(endpoint, root.inbox()?).await?;
        let cut = usize::try_from(adopted.forked_at)?;
        if adopted.parent.as_ref() != Some(&root)
            || adopted.conversation_tail != 1
            || adoption.sequence != adopted.forked_at
            || parent_inbox
                .get(..cut)
                .map(|rows| rows.iter().map(|row| &row.value).collect::<Vec<_>>())
                != inbox
                    .get(..cut)
                    .map(|rows| rows.iter().map(|row| &row.value).collect::<Vec<_>>())
        {
            return Err("spawn did not preserve exactly the saved cut".into());
        }
        let status = records(endpoint, child.status()?).await?;
        let initial =
            StatusRecord::decode(&status.first().ok_or("child omitted initial status")?.value)?;
        if status.len() != 1
            || initial.phase != Phase::Idle
            || initial.cursor != adoption.sequence
            || endpoint.effects().await? != 1
        {
            return Err("a child ran before explicit placement".into());
        }
        let AgentReply::Children { items, next } = accepted(
            endpoint,
            &AgentCommand::Children {
                agent: root.clone(),
                page: Default::default(),
            },
        )
        .await?
        else {
            return Err("Children reply shape differs after spawn".into());
        };
        if items.len() != 1
            || items.first().map(|item| &item.agent) != Some(child)
            || items.first().and_then(|item| item.status.as_ref()) != Some(&initial)
            || next.is_some()
        {
            return Err("the committed child was not listed exactly once".into());
        }
        // Both candidate prefixes must carry continuations. A terminal
        // two-row page can be smaller than one row with an encrypted cursor.
        let mut remaining = BTreeSet::from([child.clone()]);
        for id in ["spawn-page", "spawn-page-tail"] {
            let AgentReply::Spawned { agent, .. } = accepted(
                endpoint,
                &AgentCommand::Spawn {
                    parent: root.clone(),
                    id: IntentId::try_from(id)?,
                    brief: ModelContent::Text("Also wait for explicit placement".into()),
                    role: "worker".into(),
                },
            )
            .await?
            else {
                return Err("pagination fixture spawn reply shape differs".into());
            };
            if !remaining.insert(agent) {
                return Err("pagination fixture reused a child identity".into());
            }
        }
        let children = |items, bytes, cursor| {
            Ok::<_, acyclic_harness_client::CommandError>(AgentCommand::Children {
                agent: root.clone(),
                page: AgentPageRequest::new(items, bytes, cursor)?,
            })
        };
        let default_credit = AgentPageRequest::<String>::default().bytes();
        let first_command = children(1, default_credit, None)?;
        let first = Ok(accepted(endpoint, &first_command).await?);
        // The actual serving reply defines credit, including its opaque cursor
        // and canonical wire framing; the fixture invents neither encoding.
        let credit = u32::try_from(encode_agent_reply(&first, &first_command)?.len())?;
        let AgentReply::Children {
            items: first_items,
            next: Some(item_cursor),
        } = first?
        else {
            return Err("one-row discovery omitted its continuation".into());
        };
        let first_child = first_items.first().ok_or("first children page is empty")?;
        if !remaining.remove(&first_child.agent) {
            return Err("first children page included an unknown child".into());
        }
        let byte_command = children(2, credit, None)?;
        let bounded = Ok(accepted(endpoint, &byte_command).await?);
        let size = u32::try_from(encode_agent_reply(&bounded, &byte_command)?.len())?;
        if size != credit {
            return Err(format!("children reply used {size} of its {credit} bytes").into());
        }
        let AgentReply::Children {
            items,
            next: Some(byte_cursor),
        } = bounded?
        else {
            return Err("byte-credit children reply shape differs".into());
        };
        if items.len() != 1
            || items.first().map(|row| (&row.agent, &row.status))
                != Some((&first_child.agent, &first_child.status))
        {
            return Err("children byte credit did not retain exactly its fitting prefix".into());
        }
        // Sealed cursors use independent nonces. Compare the resumed rows at
        // the public serving boundary, where the continuation is observable.
        for cursor in [item_cursor, byte_cursor] {
            let AgentReply::Children { items, next } =
                accepted(endpoint, &children(2, default_credit, Some(cursor))?).await?
            else {
                return Err("continued children reply shape differs".into());
            };
            let agents: BTreeSet<_> = items.iter().map(|item| &item.agent).collect();
            if items.len() != remaining.len()
                || next.is_some()
                || !agents.into_iter().eq(remaining.iter())
                || items.iter().any(|item| {
                    item.status.as_ref().map(|status| &status.phase) != Some(&Phase::Idle)
                })
            {
                return Err("children continuation skipped, repeated or activated a child".into());
            }
        }
        if endpoint.effects().await? != 1 {
            return Err("children continuation activated a child".into());
        }
        for bytes in [1, credit - 1] {
            if !matches!(
                call(endpoint, &children(1, bytes, None)?).await?,
                Err(AgentControlFailure::Refused { code, .. })
                    if code == acyclic_harness_core::agent::Error::Stream(StreamError::Capacity).code()
            ) {
                return Err(
                    "insufficient child credit did not refuse before an empty continuation".into(),
                );
            }
        }
        // A missing descendant is within this credential's admitted tree. A
        // foreign root instead requires its own credential before agent lookup.
        // Reply checks existence before interpreting its original inbox cursor;
        // Cancel validates the input first and cannot prove this refusal.
        let missing = root.child(&acyclic_harness_core::agent::AgentId::from_bytes([255; 16]))?;
        let refused = call(
            endpoint,
            &AgentCommand::Reply {
                inbox: 1,
                agent: missing,
                id: IntentId::try_from("missing")?,
            },
        )
        .await?;
        if !matches!(refused, Err(AgentControlFailure::Refused { code, .. }) if code == "no_such_agent")
        {
            return Err("a missing descendant did not report no_such_agent".into());
        }
        let outside = AgentPath::root(&acyclic_harness_core::agent::AgentId::from_bytes([255; 16]));
        // This credential names only the served tree. An outside-tree request
        // requires independent session-directory authority before dispatch.
        let refused = call(
            endpoint,
            &AgentCommand::Cancel {
                conversation: 0,
                input: 1,
                agent: outside,
                id: IntentId::try_from("outside")?,
            },
        )
        .await;
        if !matches!(refused, Err(ref error)
            if error
                .downcast_ref::<StreamError>()
                .is_some_and(|error| error.category() == StreamCategory::AccessDenied))
        {
            return Err("an outside-tree command did not return typed AccessDenied".into());
        }
        if endpoint.effects().await? != 1 {
            return Err("an outside-tree command caused a tool effect".into());
        }
    }
    Ok(())
}
