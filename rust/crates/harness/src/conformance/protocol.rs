//! Hosted Spawn, Approvals and reply vectors. Endpoint and in-process drivers
//! use these identical commands, fixtures and assertions.
use crate::conformance::{Backend, Result};
use acyclic_harness_client::{
    AgentBinding, AgentCommand, AgentControlFailure, AgentPageRequest, AgentReply, PreparedCommand,
    encode_agent_reply,
};
use acyclic_harness_core::{
    agent::{
        AgentPath, AgentRecord as _, ConversationEntry, InboxRecord, IntentId, Phase, Reply,
        StatusRecord,
    },
    approvals::ApprovalPolicy,
    conversation::FileRef,
    interaction::ApprovalBinding,
    model::{ModelContent, ModelEvent, ToolCall},
    tool::ToolDefinition,
};
use acyclic_stream::{
    BoxProviderFuture, ProviderPlatform, ReadRequest, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde_json::json;
use std::sync::Arc;

pub mod embedded;

/// Fixture tool name, shared with protocol clients and actual guest artifacts.
pub const TOOL: &str = "conformance.echo";
/// Fixture's settled model reply.
pub const REPLY: &str = "conformance hosted reply";

/// The one deterministic model script; every backend supplies these events.
pub fn model_events(step: u32) -> Vec<ModelEvent> {
    vec![
        if step == 0 {
            ModelEvent::ToolCall(ToolCall::new("call-0".into(), TOOL.into(), json!({})))
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
    fn provider(&self) -> Arc<dyn StreamProvider>;
    /// Read a settled output through the host's admitted content reader.
    fn read_file<'a>(
        &'a self,
        agent: &'a AgentPath,
        file: &'a FileRef,
    ) -> BoxProviderFuture<'a, Result<Vec<u8>>>;
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
        Err(StreamError::NotFound) => Ok(Vec::new()),
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
        let endpoint = backend.serving(policy).await?;
        let endpoint = endpoint.as_ref();
        let AgentReply::Root { agent: root } = call(endpoint, &AgentCommand::Root {}).await??
        else {
            return Err("Root did not reply with the admitted root".into());
        };
        let AgentReply::Children { items, next } = call(
            endpoint,
            &AgentCommand::Children {
                agent: root.clone(),
                page: Default::default(),
            },
        )
        .await??
        else {
            return Err("Children reply shape differs".into());
        };
        if !items.is_empty() || next.is_some() {
            return Err("a fresh root has children".into());
        }

        let prompt = AgentCommand::Prompt {
            agent: root.clone(),
            id: IntentId::try_from("prompt")?,
            body: ModelContent::Text("Call the shared tool, then reply".into()),
        };
        let first = call(endpoint, &prompt).await??;
        endpoint.drain(&root).await?;
        if serde_json::to_value(&first)? != serde_json::to_value(call(endpoint, &prompt).await??)? {
            return Err("an exact prompt retry changed its delivery receipt".into());
        }
        let approvals = AgentCommand::Approvals {
            agent: root.clone(),
            page: Default::default(),
        };
        let AgentReply::Approvals { mut items, next } = call(endpoint, &approvals).await?? else {
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
                approved: true,
                reason: None,
            };
            if call(endpoint, &decision(wrong)).await?.is_ok() || endpoint.effects().await? != 0 {
                return Err("a changed approval binding had an effect".into());
            }
            let exact = decision(action_digest);
            let receipt = call(endpoint, &exact).await??;
            endpoint.drain(&root).await?;
            if serde_json::to_value(receipt)?
                != serde_json::to_value(call(endpoint, &exact).await??)?
            {
                return Err("a settled decision did not replay its exact receipt".into());
            }
        }
        endpoint.drain(&root).await?;
        if endpoint.effects().await? != 1 {
            return Err("a turn's tool did not run exactly once".into());
        }
        let conversation = records(endpoint, root.conversation()?).await?;
        if conversation.len() != 1 {
            return Err("a prompt retry produced another turn".into());
        }
        let entry = ConversationEntry::decode(&conversation.first().ok_or("missing turn")?.value)?;
        let request_reply = AgentCommand::Reply {
            agent: root.clone(),
            id: IntentId::try_from("prompt")?,
            inbox: entry.inbox,
        };
        let answered = call(endpoint, &request_reply).await??;
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
        if *sequence != conversation[0].sequence
            || settled != &entry
            || serde_json::to_value(&answered)?
                != serde_json::to_value(call(endpoint, &request_reply).await??)?
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
        let output: serde_json::Value =
            serde_json::from_slice(&endpoint.read_file(&root, &entry.output).await?)?;
        if output.get("text").and_then(serde_json::Value::as_str) != Some(REPLY) {
            return Err("the guest's immutable reply differs".into());
        }
        let AgentReply::Approvals { items, next } = call(endpoint, &approvals).await?? else {
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
        let spawned = call(endpoint, &spawn).await??;
        let AgentReply::Spawned { agent: child, .. } = &spawned else {
            return Err("Spawn reply shape differs".into());
        };
        if serde_json::to_value(&spawned)? != serde_json::to_value(call(endpoint, &spawn).await??)?
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
        let AgentReply::Children { items, next } = call(
            endpoint,
            &AgentCommand::Children {
                agent: root.clone(),
                page: Default::default(),
            },
        )
        .await??
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
        let AgentReply::Spawned { agent: other, .. } = call(
            endpoint,
            &AgentCommand::Spawn {
                parent: root.clone(),
                id: IntentId::try_from("spawn-page")?,
                brief: ModelContent::Text("Also wait for explicit placement".into()),
                role: "worker".into(),
            },
        )
        .await??
        else {
            return Err("second spawn reply shape differs".into());
        };
        let children = |items, bytes, cursor| {
            Ok::<_, acyclic_harness_client::CommandError>(AgentCommand::Children {
                agent: root.clone(),
                page: AgentPageRequest::new(items, bytes, cursor)?,
            })
        };
        let first_command = children(1, AgentPageRequest::default().bytes(), None)?;
        let first = Ok(call(endpoint, &first_command).await??);
        // The actual serving reply defines credit, including its opaque cursor
        // and canonical wire framing; the fixture invents neither encoding.
        let credit = u32::try_from(encode_agent_reply(&first, &first_command)?.len())?;
        let AgentReply::Children {
            items: first_items,
            next: Some(cursor),
        } = first?
        else {
            return Err("one-row discovery omitted its continuation".into());
        };
        let first_child = first_items.first().ok_or("first children page is empty")?;
        let AgentReply::Children { items, next } =
            call(endpoint, &children(2, credit, None)?).await??
        else {
            return Err("byte-credit children reply shape differs".into());
        };
        if items.len() != 1
            || items.first().map(|row| (&row.agent, &row.status))
                != Some((&first_child.agent, &first_child.status))
            || next.as_ref() != Some(&cursor)
        {
            return Err("children byte credit did not retain exactly its fitting prefix".into());
        }
        let AgentReply::Children { items, next } =
            call(endpoint, &children(2, credit, Some(cursor))?).await??
        else {
            return Err("continued children reply shape differs".into());
        };
        let last_child = items.first().ok_or("continued children page is empty")?;
        if items.len() != 1
            || next.is_some()
            || first_child.agent == last_child.agent
            || [child, &other]
                .into_iter()
                .any(|agent| agent != &first_child.agent && agent != &last_child.agent)
            || last_child.status.as_ref().map(|status| &status.phase) != Some(&Phase::Idle)
            || endpoint.effects().await? != 1
        {
            return Err("children continuation skipped, repeated or activated a child".into());
        }
        if !matches!(
            call(endpoint, &children(1, credit - 1, None)?).await?,
            Err(AgentControlFailure::Refused { code, .. })
                if code == acyclic_harness_core::agent::Error::Stream(StreamError::Capacity).code()
        ) {
            return Err(
                "insufficient child credit did not refuse before an empty continuation".into(),
            );
        }
        let outside = AgentPath::root(&acyclic_harness_core::agent::AgentId::from_bytes([255; 16]));
        let refused = call(
            endpoint,
            &AgentCommand::Cancel {
                agent: outside,
                id: IntentId::try_from("outside")?,
            },
        )
        .await?;
        if !matches!(refused, Err(AgentControlFailure::Refused { code, .. }) if code == "no_such_agent")
        {
            return Err("an outside-tree command was not a typed refusal".into());
        }
    }
    Ok(())
}
