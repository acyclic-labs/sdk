//! Settlement from the original actor checkpoint and consumed inbox cursor.
//! Status records announce phase changes and conversation creation; entries
//! announce replies.

use super::{AgentPath, AgentRecord as _, ConversationEntry, Error, Phase, inbox_tail};
use futures::TryStreamExt as _;
use std::{collections::HashSet, sync::Arc};

/// An original checkpoint's phase, readiness and answer against its accepted
/// inbox cut, including a delivery whose next checkpoint is not committed yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settlement {
    /// Exact current input, including accepted work before its first checkpoint.
    pub input: Option<u64>,
    /// Whether the checkpoint covers the observed inbox cut.
    pub settled: bool,
    /// The observed phase, including when the child has not settled.
    pub phase: Phase,
    /// Exact conversation position of the agent's last own turn, if any.
    /// Inherited history is never an answer. Read this position with [`answer`].
    pub answer: Option<u64>,
}

/// `agent`'s phase and whether it has settled every inbox record committed before
/// this call (idle, waiting for a user, or done): one read of its
/// inbox tail and its original actor state, read concurrently. `within` selects
/// the original account and namespace; no actor discovery or history scan.
/// An inbox cut newer than the actor checkpoint can only defer settlement.
/// If an idle checkpoint has pending input, one lazy read of the accepted
/// inbox range finds its first waking input; non-waking controls never invent
/// running work. The fixed observed cut excludes later concurrent deliveries.
///
/// # Errors
/// [`Error::NoSuchAgent`] when `agent` was never created; stream failures and
/// malformed records as themselves.
pub async fn settled<P: crate::store::JournalProvider + ?Sized + 'static>(
    provider: &Arc<P>,
    within: &acyclic_actors::addressing::ActorReference,
    agent: &AgentPath,
) -> Result<Settlement, Error> {
    Ok(observe(provider, within, agent).await?.1)
}

pub(super) async fn snapshot<P: crate::store::JournalProvider + ?Sized + 'static>(
    provider: &Arc<P>,
    within: &acyclic_actors::addressing::ActorReference,
    agent: &AgentPath,
) -> Result<(super::AgentState, Settlement), Error> {
    let (head, settlement) = observe(provider, within, agent).await?;
    Ok((head.load(provider.as_ref()).await?, settlement))
}

async fn observe<P: crate::store::JournalProvider + ?Sized + 'static>(
    provider: &Arc<P>,
    within: &acyclic_actors::addressing::ActorReference,
    agent: &AgentPath,
) -> Result<(super::checkpoint::Head, Settlement), Error> {
    let actor = super::handler::Agents::<P>::actor(within, &agent.id())?;
    let agents = super::handler::Agents::new(super::actor_store(provider)?);
    let (inbox, (head, cursor)) =
        futures::try_join!(inbox_tail(&**provider, agent), agents.head(&actor, agent))?;
    let state = &head.state;
    let settled = matches!(state.phase, Phase::Done { .. })
        || matches!(state.phase, Phase::Idle | Phase::WaitingUser { .. })
            && state.settled_before(cursor) >= inbox;
    let mut phase = super::machine::published(state);
    let mut input = state
        .turn
        .as_ref()
        .map(|turn| turn.inbox)
        .or_else(|| state.queued.front().map(|queued| queued.inbox));
    if let Some(control) = &state.approval_control {
        input = Some(input.map_or(control.range.start, |input| input.min(control.range.start)));
    }
    let pending_from = state.overflow.unwrap_or(cursor);
    if input.is_none()
        && !settled
        && !state.closed
        && pending_from < inbox
        && matches!(&state.phase, Phase::Idle | Phase::WaitingUser { .. })
    {
        let mut sequence = pending_from;
        'accepted: while sequence < inbox {
            // Chunking bounds each read, never the accepted pending range.
            let limit = (inbox - sequence).min(64) as u32;
            let end = sequence + u64::from(limit);
            let mut records = provider
                .read(acyclic_stream::ReadRequest {
                    key: None,
                    path: agent.inbox()?,
                    from: sequence,
                    limit,
                    at_least: Default::default(),
                })
                .await
                .map_err(Error::Stream)?;
            while sequence < end {
                let original = records
                    .try_next()
                    .await
                    .map_err(Error::Stream)?
                    .filter(|record| record.sequence == sequence)
                    .ok_or(Error::Record("accepted inbox input is unavailable"))?;
                let record = super::InboxRecord::decode(&original.value)?;
                let wakes = match record {
                    _ if !state.input_pending(cursor, sequence) => false,
                    record if state.already_steered(sequence, &record) => false,
                    super::InboxRecord::ForkAdopted(adopted) => adopted.brief.is_some(),
                    record => super::input::input(sequence, record).is_some(),
                };
                if wakes {
                    input = Some(sequence);
                    phase = Phase::Running;
                    break 'accepted;
                }
                sequence += 1;
            }
        }
    }
    let settlement = Settlement {
        input,
        settled,
        phase,
        answer: (settled && state.conversation_tail > state.conversation_base)
            .then(|| state.conversation_tail - 1),
    };
    Ok((head, settlement))
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

    /// Observe the reply to this agent's original send. A missing intent
    /// grants no observation of another agent or its inputs.
    fn sent_reply<'a>(
        &'a self,
        _id: &'a super::IntentId,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Option<super::SentReply>>> {
        Box::pin(async {
            Err(crate::Error::Unsupported(
                "sent reply observation is not bound".into(),
            ))
        })
    }

    /// Enabled membership versions retained through the eventual inbox commit.
    fn peer_admission<'a>(
        &'a self,
        _grants: &'a crate::Capabilities,
        _recipient: &'a AgentPath,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Option<super::PeerAdmission>>> {
        Box::pin(async { Ok(None) })
    }

    /// Waits for this agent's committed close. Accepted background work uses
    /// this lifetime, independently of the turn which admitted it. Hosts with
    /// no agent lifecycle retain their enclosing runtime's lifetime.
    fn closed(&self) -> acyclic_stream::BoxProviderFuture<'_, crate::Result<()>> {
        Box::pin(std::future::pending())
    }

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

    /// `child`'s phase, readiness and exact settled answer position.
    fn settled<'a>(
        &'a self,
        child: &'a AgentPath,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Settlement>>;

    /// One published diagnostic file page, under this turn's original child
    /// relationship and work owner. The returned hold outlives its text.
    fn diagnostic<'a>(
        &'a self,
        _agent: &'a AgentPath,
        _digest: [u8; 32],
        _from: u64,
        _maximum_bytes: u64,
        _io: Option<&'a crate::conversation::ContentIo>,
    ) -> acyclic_stream::BoxProviderFuture<
        'a,
        crate::Result<(
            super::Diagnostic,
            super::DiagnosticRead,
            Option<crate::conversation::ContentHold<'a>>,
        )>,
    > {
        Box::pin(async {
            Err(crate::Error::Unsupported(
                "diagnostic file queries are not bound".into(),
            ))
        })
    }

    /// The text at `child`'s selected answer position, read under the turn's
    /// authority. The caller retains the position from the same settlement.
    fn answer<'a>(
        &'a self,
        child: &'a AgentPath,
        sequence: u64,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<(String, super::TurnOutcome)>>;
}

/// Selects the current child, or the latest released generation when the name
/// has no replacement. The caller persists this selection before waiting.
pub(crate) async fn select_children(
    observer: &dyn TurnChildren,
    references: &[String],
) -> crate::Result<Vec<(String, AgentPath)>> {
    let parent = observer.agent();
    let mut children = Vec::with_capacity(references.len());
    let mut seen = HashSet::with_capacity(references.len());
    for reference in references {
        let child = named_child(Some(observer), parent, reference, true)
            .await?
            .ok_or_else(|| {
                crate::Error::Invalid(format!("{reference} names no child of this agent"))
            })?;
        if seen.insert(wait_identity(&child)) {
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
    if super::spawn_names::spawn_intent(reference, 0).is_err() {
        return Ok(None);
    }
    let Some(observer) = observer else {
        return Ok(None);
    };
    match observer.resolve(parent, reference).await? {
        super::SpawnResolution::Live(_, child) => Ok(Some(child)),
        super::SpawnResolution::Free(generation) if released && generation > 0 => {
            let intent = super::spawn_names::spawn_intent(reference, generation - 1)
                .map_err(crate::Error::from)?;
            Ok(Some(
                super::TurnEffect::child(parent, &intent).map_err(crate::Error::from)?,
            ))
        }
        super::SpawnResolution::Free(_) => Ok(None),
    }
}

#[inline]
fn wait_identity(child: &AgentPath) -> [u8; 16] {
    #[cfg(any(test, feature = "test-support"))]
    SELECTED_IDENTITIES.with(|count| count.set(count.get() + 1));
    child.id_bytes()
}

#[cfg(any(test, feature = "test-support"))]
thread_local! {
    pub(crate) static SELECTED_IDENTITIES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
