//! A prompt's reply is read from committed state alone, on every host: pending
//! until its turn settles, then the entry at its own inbox sequence, a
//! conflict for another prompt's sequence, and dropped once an archive clears
//! it unanswered. It holds both for a turn that settles in one step and for a
//! native-style turn that a host process settles later through hooks.
use crate::conformance::{
    Backend, Result,
    fixtures::{adopted, sender},
    observe::Observed,
    vectors::{drain, hosting, intent, limits, prompt},
};
use acyclic_harness_core::agent::*;
use std::sync::Arc;

/// Host whose hooks settle `native-*` messages.
const HOST: &str = "conformance";

/// Settles each message as the shared conversation entry at its inbox
/// sequence. `hold` keeps its turn open until the agent is archived; a
/// `native-*` message is consumed in one step and settles when its host's
/// next hook arrives, like a native process completing it.
struct Replying;

impl TurnDriver for Replying {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        _cursor: u64,
        input: Option<Queued>,
        _cancellation: acyclic_actors::guest::CancellationToken,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let (inbox, input) = match (input, &agent.turn) {
                (
                    Some(Queued {
                        input: TurnInput::Hook { host, .. },
                        ..
                    }),
                    Some(turn),
                ) if host == HOST => {
                    let Queued { inbox, input } = serde_json::from_slice(&turn.checkpoint)
                        .map_err(|_| Error::Record("unexpected conformance checkpoint"))?;
                    return settle(inbox, input);
                }
                (
                    Some(Queued {
                        inbox,
                        input: input @ TurnInput::Message { .. },
                    }),
                    None,
                ) => (inbox, input),
                _ => return Err(Error::Record("unexpected conformance input")),
            };
            let TurnInput::Message { id, .. } = &input else {
                return Err(Error::Record("unexpected conformance input"));
            };
            if id.as_str() == "hold" {
                return Ok(TurnStep {
                    reserved_children: 0,
                    checkpoint: Some(Vec::new()),
                    outs: Vec::new(),
                    wait: Wait::Approval(id.clone()),
                    drops: None,
                });
            }
            if id.as_str().starts_with("native-") {
                let checkpoint = serde_json::to_vec(&Queued { inbox, input })
                    .map_err(|_| Error::Record("conformance checkpoint does not encode"))?;
                return Ok(TurnStep {
                    reserved_children: 0,
                    checkpoint: Some(checkpoint),
                    outs: Vec::new(),
                    wait: Wait::Host(HOST.into()),
                    drops: None,
                });
            }
            settle(inbox, input)
        })
    }
}

/// The turn that `input` at `inbox` started settles as its conversation entry.
fn settle(inbox: u64, input: TurnInput) -> std::result::Result<TurnStep, Error> {
    let TurnInput::Message { body, .. } = &input else {
        return Err(Error::Record("unexpected conformance input"));
    };
    let output = (**body).clone();
    let entry = ConversationEntry {
        inbox,
        input,
        output,
        outcome: Default::default(),
        goal: None,
    };
    Ok(TurnStep {
        reserved_children: 0,
        checkpoint: None,
        outs: vec![Out::Conversation(entry.encode()?)],
        wait: Wait::Input,
        drops: None,
    })
}

fn message(id: &str) -> Result<InboxRecord> {
    Ok(InboxRecord::Message(Message {
        from: sender(),
        id: intent(id)?,
        body: Box::new(prompt()?),
        attachments: None,
    }))
}

async fn sent(host: &AgentHost<Observed>, root: &AgentPath, id: &str) -> Result<(IntentId, u64)> {
    match host.send(root, &sender(), &message(id)?).await? {
        Delivered::Committed { inbox } => Ok((intent(id)?, inbox)),
        Delivered::Closed => Err("open recipient refused a message".into()),
    }
}

fn hook(id: &str) -> Result<InboxRecord> {
    Ok(InboxRecord::Hook(Hook {
        host: HOST.into(),
        id: intent(id)?,
        event: Box::new(prompt()?),
    }))
}

async fn answered(
    host: &AgentHost<Observed>,
    root: &AgentPath,
    prompt: &(IntentId, u64),
) -> Result {
    match host.reply(root, &prompt.0, prompt.1).await? {
        Reply::Answered { entry, .. }
            if entry.inbox == prompt.1
                && matches!(&entry.input, TurnInput::Message { id, .. } if *id == prompt.0) =>
        {
            Ok(())
        }
        other => Err(format!("a settled prompt replied {other:?}").into()),
    }
}

async fn pending(host: &AgentHost<Observed>, root: &AgentPath, prompt: &(IntentId, u64)) -> Result {
    match host.reply(root, &prompt.0, prompt.1).await? {
        Reply::Pending => Ok(()),
        other => Err(format!("an unsettled prompt replied {other:?}").into()),
    }
}

pub(crate) async fn verify(backend: &dyn Backend) -> Result {
    let storage = backend.fresh().await?;
    let provider = Arc::new(Observed::new(storage.open().await?));
    let handler = backend.handler(Arc::new(AgentHandler::new(
        actor_store(&provider)?,
        Replying,
        limits()?,
    )?))?;
    let host = hosting(provider, handler, None)?;
    let root = host.create(&intent("create")?, adopted()).await?;
    drain(&host).await?;
    let (first, at) = sent(&host, &root, "first").await?;
    if host.reply(&root, &first, at).await? != Reply::Pending {
        return Err("an unread prompt was not pending".into());
    }
    drain(&host).await?;
    match host.reply(&root, &first, at).await? {
        Reply::Answered { entry, .. }
            if entry.inbox == at
                && matches!(&entry.input, TurnInput::Message { id, .. } if *id == first) => {}
        other => return Err(format!("a settled prompt replied {other:?}").into()),
    }
    if !matches!(
        host.reply(&root, &intent("other")?, at).await,
        Err(Error::Conflict(_))
    ) {
        return Err("another prompt's entry was not a conflict".into());
    }
    let held = sent(&host, &root, "hold").await?;
    drain(&host).await?;
    let queued = sent(&host, &root, "queued").await?;
    drain(&host).await?;
    for (id, inbox) in [&held, &queued] {
        if host.reply(&root, id, *inbox).await? != Reply::Pending {
            return Err("a running or queued prompt was not pending".into());
        }
    }
    host.archive(&root, &sender(), &intent("archive")?).await?;
    drain(&host).await?;
    for (id, inbox) in [&held, &queued] {
        if host.reply(&root, id, *inbox).await? != Reply::Dropped {
            return Err("an archived prompt was not dropped".into());
        }
    }
    native(backend).await
}

/// A native-style host consumes each message in one step and settles it
/// later through its hooks, which the open turn takes ahead of queued input.
async fn native(backend: &dyn Backend) -> Result {
    let storage = backend.fresh().await?;
    let provider = Arc::new(Observed::new(storage.open().await?));
    let handler = backend.handler(Arc::new(AgentHandler::new(
        actor_store(&provider)?,
        Replying,
        limits()?,
    )?))?;
    let host = hosting(provider, handler, None)?;
    let root = host.create(&intent("create")?, adopted()).await?;
    drain(&host).await?;
    let first = sent(&host, &root, "native-1").await?;
    drain(&host).await?;
    let second = sent(&host, &root, "native-2").await?;
    drain(&host).await?;
    pending(&host, &root, &first).await?;
    pending(&host, &root, &second).await?;
    host.send(&root, &sender(), &hook("settle-1")?).await?;
    drain(&host).await?;
    answered(&host, &root, &first).await?;
    pending(&host, &root, &second).await?;
    host.send(&root, &sender(), &hook("settle-2")?).await?;
    drain(&host).await?;
    answered(&host, &root, &first).await?;
    answered(&host, &root, &second).await?;
    if !matches!(
        host.reply(&root, &second.0, first.1).await,
        Err(Error::Conflict(_))
    ) {
        return Err("another prompt's native entry was not a conflict".into());
    }
    Ok(())
}
