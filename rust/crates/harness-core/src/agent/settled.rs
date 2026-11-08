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
/// call (idle, waiting for a user, or done), else `None`: one read of its
/// inbox tail and its original actor state, read concurrently. `within` selects
/// the original account and namespace; no actor discovery or history scan.
/// An inbox cut newer than the actor checkpoint can only defer settlement:
/// the checkpoint must cover that cut before an idle agent is returned.
///
/// # Errors
/// [`Error::NoSuchAgent`] when `agent` was never created; stream failures and
/// malformed records as themselves.
pub async fn settled<P: crate::store::JournalProvider + ?Sized + 'static>(
    provider: &Arc<P>,
    within: &acyclic_actors::addressing::ActorReference,
    agent: &AgentPath,
) -> Result<Option<Settlement>, Error> {
    let actor = super::handler::Agents::<P>::actor(within, &agent.id())?;
    let agents = super::handler::Agents::new(super::actor_store(provider)?);
    let (inbox, (state, cursor)) =
        futures::try_join!(inbox_tail(&**provider, agent), agents.state(&actor, agent))?;
    if matches!(state.phase, Phase::Done { .. })
        || matches!(state.phase, Phase::Idle | Phase::WaitingUser { .. })
            && state.settled_before(cursor) >= inbox
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

    /// Resolves a child's or sibling's spawn name from committed state.
    fn resolve<'a>(
        &'a self,
        parent: &'a AgentPath,
        name: &'a str,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<super::SpawnResolution>>;

    /// Pins a wait call's references to child identities before it suspends.
    /// Replays of `operation` return the original selection after name reuse.
    fn select<'a>(
        &'a self,
        operation: crate::OperationId,
        references: &'a [String],
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Vec<(String, AgentPath)>>>;

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

/// Selects the current child, or the latest released generation when the name
/// has no replacement. The caller persists this selection before waiting.
pub(crate) async fn select_children(
    observer: &dyn TurnChildren,
    references: &[String],
) -> crate::Result<Vec<(String, AgentPath)>> {
    let parent = observer.agent();
    let mut children = Vec::with_capacity(references.len());
    for reference in references {
        let child = named_child(Some(observer), parent, reference, true)
            .await?
            .ok_or_else(|| {
                crate::Error::Invalid(format!("{reference} names no child of this agent"))
            })?;
        if !children.iter().any(|(_, known)| *known == child) {
            children.push((reference.clone(), child));
        }
    }
    Ok(children)
}

/// Resolves a direct child's path or spawn name. Waits may select the last
/// released generation; commands requiring ownership select only a live name.
pub(crate) async fn named_child(
    observer: Option<&dyn TurnChildren>,
    parent: &AgentPath,
    reference: &str,
    released: bool,
) -> crate::Result<Option<AgentPath>> {
    if let Ok(path) = AgentPath::try_from(reference.to_owned()) {
        return Ok(path.is_child_of(parent).then_some(path));
    }
    if parent.depth() == super::MAX_DEPTH || super::spawn_names::spawn_intent(reference, 0).is_err()
    {
        return Ok(None);
    }
    let Some(observer) = observer else {
        return Ok(None);
    };
    match observer.resolve(parent, reference).await? {
        super::SpawnResolution::Live(_, child) => Ok(Some(child)),
        super::SpawnResolution::Free(generation) if released && generation > 0 => {
            let intent = super::spawn_names::spawn_intent(reference, generation - 1)
                .map_err(|error| crate::Error::Invalid(error.to_string()))?;
            Ok(Some(super::TurnEffect::child(parent, &intent).map_err(
                |error| crate::Error::Invalid(error.to_string()),
            )?))
        }
        super::SpawnResolution::Free(_) => Ok(None),
    }
}
