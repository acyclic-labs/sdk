//! Settlement from the original actor checkpoint and consumed inbox cursor.
//! Status records announce phase changes; conversation entries announce replies.

use super::{
    AgentPath, AgentRecord as _, ConversationEntry, Error, Phase, StatusRecord, inbox_tail,
};
use acyclic_stream::{ReadRequest, StreamError, StreamPath, StreamProviderExt as _};
use futures::TryStreamExt as _;
use std::sync::Arc;

/// `agent`'s phase, and whether it has settled every inbox record committed
/// before this call ([`Phase::Idle`] or [`Phase::Done`]): one read of its
/// inbox tail and its original actor state, never a wait. `within` selects
/// the original account and namespace; no actor discovery or history scan.
///
/// # Errors
/// [`Error::NoSuchAgent`] when `agent` was never created; stream failures and
/// malformed records as themselves.
pub async fn settled<P: crate::store::JournalProvider + ?Sized + 'static>(
    provider: &Arc<P>,
    within: &acyclic_actors::addressing::ActorReference,
    agent: &AgentPath,
) -> Result<(Phase, bool), Error> {
    let inbox = inbox_tail(&**provider, agent).await?;
    let actor = super::handler::Agents::<P>::actor(within, &agent.id())?;
    let (state, cursor) = super::handler::Agents::new(super::actor_store(provider)?)
        .state(&actor, agent)
        .await?;
    let settled = match &state.phase {
        Phase::Done { .. } => true,
        Phase::Idle => state.settled_before(cursor) >= inbox,
        _ => false,
    };
    Ok((state.phase, settled))
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

    /// `child`'s phase and whether it settled (see [`settled`]).
    fn settled<'a>(
        &'a self,
        child: &'a AgentPath,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<(Phase, bool)>>;

    /// The text `child`'s last own turn answered (see [`answer`]), read under
    /// the turn's authority.
    fn answer<'a>(
        &'a self,
        child: &'a AgentPath,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Option<String>>>;
}
