//! Waiting for an agent to settle its input, observed on its own streams.
//!
//! An agent has settled the input committed to it before some point once its
//! status says so: it is done, or idle having consumed every one of those inbox
//! records. An idle agent still holding input publishes `Running`, and every
//! idle step publishes its cursor, so the one status record a step commits
//! decides this alone, with no read of the agent's checkpoint and no polling:
//! the wait follows the status stream.

use super::{
    AgentPath, AgentRecord as _, ConversationEntry, Error, Phase, StatusRecord, inbox_tail,
};
use acyclic_stream::{FollowRequest, ReadRequest, StreamError, StreamPath, StreamProvider};
use futures::{StreamExt as _, TryStreamExt as _};

/// How an agent stood once it settled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settled {
    /// Its phase: [`Phase::Idle`] or [`Phase::Done`].
    pub phase: Phase,
    /// Its last own settled turn, if it has run one; inherited history is
    /// never its answer.
    pub entry: Option<ConversationEntry>,
}

/// Waits until `agent` settles every inbox record committed before this call,
/// following its status stream from its committed tail.
///
/// # Errors
/// [`Error::NoSuchAgent`] when `agent` was never created; stream failures and
/// malformed records as themselves.
pub async fn settled<P: StreamProvider + ?Sized>(
    provider: &P,
    agent: &AgentPath,
) -> Result<Settled, Error> {
    let inbox = inbox_tail(provider, agent).await?;
    let status = agent.status()?;
    // The first status is the adoption's: `Idle` at its `ForkAdopted` sequence,
    // the first inbox sequence of the agent's own turns.
    let adopted = record(provider, &status, 0).await?.cursor;
    let tail = match provider.tail(status.clone()).await {
        Ok(tail) => tail,
        Err(StreamError::NotFound) => return Err(Error::NoSuchAgent),
        Err(error) => return Err(Error::Stream(error)),
    };
    let settles = |record: &StatusRecord| match record.phase {
        Phase::Done { .. } => true,
        Phase::Idle => record.cursor >= inbox,
        _ => false,
    };
    let mut last = record(provider, &status, tail.saturating_sub(1)).await?;
    if !settles(&last) {
        // Follow from the tail read before the last record, so a step
        // committing after that read is never missed.
        let mut committed = acyclic_stream::records_only(
            provider
                .follow(FollowRequest::new(status, tail))
                .await
                .map_err(Error::Stream)?,
        );
        loop {
            match committed.next().await {
                Some(Ok(next)) => {
                    last = StatusRecord::decode(&next.value)?;
                    if settles(&last) {
                        break;
                    }
                }
                Some(Err(error)) => return Err(Error::Stream(error)),
                None => return Err(Error::Stream(StreamError::Unavailable)),
            }
        }
    }
    let conversation = agent.conversation()?;
    let entry = match provider.tail(conversation.clone()).await {
        Ok(0) | Err(StreamError::NotFound) => None,
        Ok(tail) => Some(super::reply::entry(provider, &conversation, tail - 1).await?)
            .filter(|entry| entry.inbox >= adopted),
        Err(error) => return Err(Error::Stream(error)),
    };
    Ok(Settled {
        phase: last.phase,
        entry,
    })
}

async fn record<P: StreamProvider + ?Sized>(
    provider: &P,
    status: &StreamPath,
    sequence: u64,
) -> Result<StatusRecord, Error> {
    let mut records = provider
        .read(ReadRequest {
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

/// What one turn's `wait` observes of the agent's children.
pub trait TurnChildren: acyclic_stream::ProviderPlatform {
    /// Waits until `child` settles (see [`settled`]) and returns its phase
    /// with the text its last own turn answered, read under the turn's
    /// authority.
    fn settle<'a>(
        &'a self,
        child: &'a AgentPath,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<(Phase, Option<String>)>>;
}
