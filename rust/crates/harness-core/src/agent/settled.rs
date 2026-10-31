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

#[cfg(feature = "filesystem")]
pub(crate) async fn settled_with_io<P: crate::store::JournalProvider + ?Sized>(
    provider: &std::sync::Arc<P>,
    agent: &AgentPath,
    io: &crate::filesystem::PreparationIo,
) -> crate::Result<Option<Phase>> {
    let inbox = super::stream_parts_with_io(&[agent.as_str()], io)?;
    let inbox = crate::store::operations::tail_optional_with_work(
        provider,
        &inbox,
        &mut Some(&mut io.history_io()),
    )
    .await?
    .ok_or_else(|| io.invalid("child agent does not exist").tool_input())?;
    let id = agent
        .as_str()
        .rsplit('/')
        .next()
        .ok_or_else(|| io.invalid("child agent has no identity"))?;
    let status = super::stream_parts_with_io(&[super::STATUSES, "/", id], io)?;
    let tail = crate::store::operations::tail_optional_with_work(
        provider,
        &status,
        &mut Some(&mut io.history_io()),
    )
    .await?
    .filter(|tail| *tail != 0)
    .ok_or_else(|| io.invalid("child agent status does not exist").tool_input())?;
    let last: StatusRecord = record_with_io(provider, &status, tail - 1, io).await?;
    Ok(match last.phase {
        Phase::Done { .. } => Some(last.phase),
        Phase::Idle if last.cursor >= inbox => Some(last.phase),
        _ => None,
    })
}

#[cfg(feature = "filesystem")]
pub(crate) async fn answer_with_io<P: crate::store::JournalProvider + ?Sized>(
    provider: &std::sync::Arc<P>,
    agent: &AgentPath,
    io: &crate::filesystem::PreparationIo,
) -> crate::Result<Option<ConversationEntry>> {
    let id = agent
        .as_str()
        .rsplit('/')
        .next()
        .ok_or_else(|| io.invalid("child agent has no identity"))?;
    let status = super::stream_parts_with_io(&[super::STATUSES, "/", id], io)?;
    let adopted: StatusRecord = record_with_io(provider, &status, 0, io).await?;
    let conversation = super::stream_parts_with_io(&[agent.as_str(), "/conversation"], io)?;
    let tail = crate::store::operations::tail_optional_with_work(
        provider,
        &conversation,
        &mut Some(&mut io.history_io()),
    )
    .await?;
    match tail {
        Some(tail) if tail != 0 => {
            let entry: ConversationEntry =
                record_with_io(provider, &conversation, tail - 1, io).await?;
            Ok((entry.inbox >= adopted.cursor).then_some(entry))
        }
        _ => Ok(None),
    }
}

#[cfg(feature = "filesystem")]
async fn record_with_io<P, T>(
    provider: &std::sync::Arc<P>,
    path: &StreamPath,
    sequence: u64,
    io: &crate::filesystem::PreparationIo,
) -> crate::Result<T>
where
    P: crate::store::JournalProvider + ?Sized,
    T: crate::filesystem::PreparationRecord + super::AgentRecord,
{
    let record = crate::store::operations::one_record(
        provider,
        path,
        sequence,
        &mut Some(&mut io.history_io()),
    )
    .await?
    .ok_or_else(|| io.invalid("committed child record is missing"))?;
    if record.value.len() > super::MAX_RECORD_BYTES {
        return Err(io.invalid("child record exceeds its frame"));
    }
    let value: T = super::records::open_with_io(&record.value, io)?;
    value
        .check()
        .map_err(|_| io.invalid("child record fails its format contract"))?;
    Ok(value)
}

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
    /// The original-work projection of the same settled-status observation.
    fn settled_with_io<'a>(
        &'a self,
        child: &'a AgentPath,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> crate::Result<crate::conversation::BoundedContentFuture<'a, Option<Phase>>> {
        if io.is_some() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "original-work child status is unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || self.settled(child))
    }
    /// The original-work projection of the same immutable answer read.
    fn answer_with_io<'a>(
        &'a self,
        child: &'a AgentPath,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> crate::Result<crate::conversation::BoundedContentFuture<'a, Option<String>>> {
        if io.is_some() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "original-work child answer is unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || self.answer(child))
    }
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
