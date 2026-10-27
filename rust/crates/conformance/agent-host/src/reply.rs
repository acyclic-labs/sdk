//! A prompt's reply is read from committed state alone, on every host: pending
//! until its turn settles, then the entry at its own inbox sequence, a
//! conflict for another prompt's sequence, and dropped once an archive clears
//! it unanswered.
use crate::{
    Backend, Result,
    fixtures::{adopted, sender},
    observe::Observed,
    vectors::{drain, hosting, intent, limits, prompt},
};
use acyclic_harness_core::agent::*;
use std::sync::Arc;

/// Settles each message as the shared conversation entry at its inbox
/// sequence, except `hold`, whose turn stays open until the agent is archived.
struct Replying;

impl TurnDriver for Replying {
    fn step<'a>(
        &'a self,
        _agent: &'a AgentState,
        _cursor: u64,
        input: Option<Queued>,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            let Some(Queued {
                inbox,
                input: input @ TurnInput::Message { .. },
            }) = input
            else {
                return Err(Error::Record("unexpected conformance input"));
            };
            let TurnInput::Message { id, body, .. } = &input else {
                return Err(Error::Record("unexpected conformance input"));
            };
            if id.as_str() == "hold" {
                return Ok(TurnStep {
                    reserved_children: 0,
                    checkpoint: Some(Vec::new()),
                    outs: Vec::new(),
                    wait: Wait::Approval(id.clone()),
                });
            }
            let output = (**body).clone();
            let entry = ConversationEntry {
                inbox,
                input,
                output,
            };
            Ok(TurnStep {
                reserved_children: 0,
                checkpoint: None,
                outs: vec![Out::Conversation(entry.encode()?)],
                wait: Wait::Input,
            })
        })
    }
}

fn message(id: &str) -> Result<InboxRecord> {
    Ok(InboxRecord::Message(Message {
        from: sender(),
        id: intent(id)?,
        body: Box::new(prompt()?),
    }))
}

async fn sent(host: &AgentHost<Observed>, root: &AgentPath, id: &str) -> Result<(IntentId, u64)> {
    match host.send(root, &sender(), &message(id)?).await? {
        Delivered::Committed { inbox } => Ok((intent(id)?, inbox)),
        Delivered::Closed => Err("open recipient refused a message".into()),
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
    Ok(())
}
