//! Whether an agent has settled its input, read once from its own streams.
//!
//! An agent has settled the input committed to it before some point once its
//! status says so: it is done, or idle having consumed every one of those inbox
//! records. An idle agent still holding input publishes `Running`, and every
//! idle step publishes its cursor, so the one status record a step commits
//! decides this alone, with no read of the agent's checkpoint. Nothing here
//! waits: a turn waiting for a child commits [`super::Phase::WaitingChildren`]
//! and the child's [`super::ChildSettled`] wakes it (see [`super::Wait::Children`]).

use super::{
    AgentPath, AgentRecord as _, ConversationEntry, Error, Phase, StatusRecord, inbox_tail,
};
use acyclic_stream::{ReadRequest, StreamError, StreamPath, StreamProviderExt as _};
use futures::TryStreamExt as _;

/// `agent`'s phase if it has settled every inbox record committed before this
/// call ([`Phase::Idle`] or [`Phase::Done`]), else `None`: one read of its
/// inbox tail and one of its last status, never a wait.
///
/// # Errors
/// [`Error::NoSuchAgent`] when `agent` was never created; stream failures and
/// malformed records as themselves.
pub async fn settled<P: crate::store::JournalProvider + ?Sized>(
    provider: &P,
    agent: &AgentPath,
) -> Result<Option<Phase>, Error> {
    let inbox = inbox_tail(provider, agent).await?;
    let status = agent.status()?;
    let tail = match provider.tail(status.clone()).await {
        Ok(0) | Err(StreamError::NotFound) => return Err(Error::NoSuchAgent),
        Ok(tail) => tail,
        Err(error) => return Err(Error::Stream(error)),
    };
    let last = record(provider, &status, tail - 1).await?;
    Ok(match last.phase {
        Phase::Done { .. } => Some(last.phase),
        Phase::Idle if last.cursor >= inbox => Some(last.phase),
        _ => None,
    })
}

/// `agent`'s last own settled turn, if it has run one; inherited history is
/// never its answer.
///
/// # Errors
/// Stream failures and malformed records as themselves.
pub async fn answer<P: crate::store::JournalProvider + ?Sized>(
    provider: &P,
    agent: &AgentPath,
) -> Result<Option<ConversationEntry>, Error> {
    // The first status is the adoption's: `Idle` at its `ForkAdopted` sequence,
    // the first inbox sequence of the agent's own turns.
    let adopted = record(provider, &agent.status()?, 0).await?.cursor;
    let conversation = agent.conversation()?;
    match provider.tail(conversation.clone()).await {
        Ok(0) | Err(StreamError::NotFound) => Ok(None),
        Ok(tail) => Ok(
            Some(super::reply::entry(provider, &conversation, tail - 1).await?)
                .filter(|entry| entry.inbox >= adopted),
        ),
        Err(error) => Err(Error::Stream(error)),
    }
}

async fn record<P: crate::store::JournalProvider + ?Sized>(
    provider: &P,
    status: &StreamPath,
    sequence: u64,
) -> Result<StatusRecord, Error> {
    let mut records = provider
        .read(ReadRequest {
            key: None,
            path: status.clone(),
            from: sequence,
            limit: 1,
            at_least: Default::default(),
        })
        .await
        .map_err(Error::Stream)?;
    match records.try_next().await.map_err(Error::Stream)? {
        Some(record) if record.sequence == sequence => StatusRecord::decode(&record.value),
        _ => Err(Error::Record("committed status record is missing")),
    }
}

/// What one turn's `wait` observes of the agent's children, each read once.
pub trait TurnChildren: acyclic_stream::ProviderPlatform {
    /// The agent whose turn observes its children.
    fn agent(&self) -> &AgentPath;

    /// `child`'s phase once it settled (see [`settled`]), else `None`.
    fn settled<'a>(
        &'a self,
        child: &'a AgentPath,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Option<Phase>>>;

    /// The text `child`'s last own turn answered (see [`answer`]), read under
    /// the turn's authority.
    fn answer<'a>(
        &'a self,
        child: &'a AgentPath,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Option<String>>>;
}
