//! Settlement from the original actor checkpoint and consumed inbox cursor.
//! Status records announce phase changes; conversation entries announce replies.

use super::{AgentPath, ConversationEntry, Error, Phase, inbox_tail};
use std::sync::Arc;

/// A settled phase and its answer position from the same actor checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settlement {
    /// The observed settled phase.
    pub phase: Phase,
    /// Exact conversation position of the agent's last own turn, if any.
    /// Inherited history is never an answer. Read this position with [`answer`].
    pub answer: Option<u64>,
}

/// `agent`'s settlement if it has settled every inbox record committed before this
/// call ([`Phase::Idle`] or [`Phase::Done`]), else `None`: one read of its
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
) -> Result<Option<Settlement>, Error> {
    let inbox = inbox_tail(&**provider, agent).await?;
    let actor = super::handler::Agents::<P>::actor(within, &agent.id())?;
    let (state, cursor) = super::handler::Agents::new(super::actor_store(provider)?)
        .state(&actor, agent)
        .await?;
    if matches!(state.phase, Phase::Done { .. })
        || state.phase == Phase::Idle && state.settled_before(cursor) >= inbox
    {
        Ok(Some(Settlement {
            phase: state.phase,
            answer: (state.conversation_tail > state.conversation_base)
                .then(|| state.conversation_tail - 1),
        }))
    } else {
        Ok(None)
    }
}

/// `agent`'s immutable answer at the exact position selected by [`settled`].
/// Later turns cannot change this selection; no status or live tail is read.
///
/// # Errors
/// Stream failures and malformed records as themselves.
pub async fn answer<P: crate::store::JournalProvider + ?Sized>(
    provider: &P,
    agent: &AgentPath,
    sequence: u64,
) -> Result<ConversationEntry, Error> {
    super::reply::entry(provider, &agent.conversation()?, sequence).await
}

/// What one turn's `wait` observes of the agent's children, each read once.
pub trait TurnChildren: acyclic_stream::ProviderPlatform {
    /// The agent whose turn observes its children.
    fn agent(&self) -> &AgentPath;

    /// `child`'s phase and exact answer position once it settled, else `None`.
    fn settled<'a>(
        &'a self,
        child: &'a AgentPath,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Option<Settlement>>>;

    /// The text at `child`'s selected answer position, read under the turn's
    /// authority. The caller retains the position from the same settlement.
    fn answer<'a>(
        &'a self,
        child: &'a AgentPath,
        sequence: u64,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<String>>;
}
