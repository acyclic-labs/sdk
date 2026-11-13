//! Targeted cancellation, published with its source inbox control.
use super::{AgentRecord as _, Error};
use acyclic_stream::{ReadRequest, StreamPath, StreamProviderExt as _};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};
mod continuing;
mod retirement;
pub(super) use retirement::{RETIRE, open_retirement, retirement_participant};
/// Terminal input facts share one stream's atomic order. The first indexed
/// fact for an input wins, including cancellation racing with settlement.
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum InputFact {
    Cancelled {
        conversation: u64,
    },
    Completed,
    /// Physical inputs consumed by this goal; its terminal is published later.
    Goal {
        run: crate::OperationId,
    },
}

impl super::AgentRecord for InputFact {}

#[derive(Default)]
pub(super) struct Retirement {
    pub steered: Option<super::BoundaryCut>,
    pub dropped: Option<Dropped>,
}

/// The original folded input window, before a control clears it.
pub(super) struct Dropped {
    pub previous: super::AgentState,
    pub cursor: u64,
    pub through: u64,
    pub terminal: bool,
}

/// Validate the immutable accepted input before publishing a cancellation fact.
pub(super) async fn validate<P: crate::store::JournalProvider + ?Sized>(
    provider: &P,
    agent: &super::AgentPath,
    cancel: &super::Cancellation,
) -> Result<(), Error> {
    let mut records = provider
        .read(ReadRequest {
            key: None,
            path: agent.inbox()?,
            from: cancel.input,
            limit: 1,
            at_least: Default::default(),
        })
        .await
        .map_err(Error::Stream)?;
    let record = records
        .try_next()
        .await
        .map_err(Error::Stream)?
        .filter(|record| record.sequence == cancel.input)
        .ok_or(Error::Record("cancellation must name an accepted input"))?;
    let record = super::InboxRecord::decode(&record.value)?;
    if super::input::input(cancel.input, record).is_none() {
        return Err(Error::Record("cancellation must name an accepted input"));
    }
    Ok(())
}

pub(super) fn input_events(agent: &super::AgentPath) -> Result<StreamPath, Error> {
    StreamPath::new(format!("{agent}/context/input-events")).map_err(Error::Stream)
}

pub(super) fn cancellation(
    agent: &super::AgentPath,
    input: u64,
    conversation: u64,
) -> Result<(StreamPath, acyclic_stream::AppendRecord), Error> {
    Ok((
        input_events(agent)?,
        acyclic_stream::AppendRecord {
            value: InputFact::Cancelled { conversation }.encode()?.into(),
            headers: acyclic_stream::RecordHeaders::new(
                vec![input.to_be_bytes().to_vec().into()],
                Vec::new(),
            )
            .map_err(Error::Stream)?,
        },
    ))
}

/// Fence cancellation against exactly the inputs this same commit settles.
/// Steering inputs come from the immutable admitted interval; reserved follow-ups
/// keep their independent turns. Each source window is read once, without a scan
/// of conversation history or a separate publication commit. Indexed keys share
/// one append mutation; record chunking follows the existing byte frame bound.
pub(super) async fn complete<P: acyclic_stream::StreamProvider + ?Sized>(
    provider: &P,
    agent: &super::AgentPath,
    outs: &[super::Out],
    retirement: Retirement,
    conversation: u64,
    routing: Option<(&super::Routing, &acyclic_actors::addressing::ActorReference)>,
    request: &mut acyclic_stream::CommitRequest,
) -> Result<bool, Error> {
    let mut completed = std::collections::BTreeMap::new();
    let mut records = Vec::new();
    let mut ranges = Vec::new();
    let mut fence = None;
    let mut results = std::collections::BTreeMap::new();
    let mut deferred =
        std::collections::BTreeMap::<crate::OperationId, std::collections::BTreeSet<u64>>::new();
    let mut goals = std::collections::BTreeMap::new();
    let mut sequence = conversation
        .checked_sub(
            outs.iter()
                .filter(|out| matches!(out, super::Out::Conversation(_)))
                .count() as u64,
        )
        .ok_or(Error::Record("conversation tail precedes its appends"))?;
    let prior_conversation = sequence;
    let mut committed_frontier = None;
    let mut preceding_frontier = None;
    if let Some(retired) = retirement.steered {
        ranges.push((retired, None));
    }
    for out in outs {
        if let super::Out::InputCancelled {
            input,
            conversation,
        } = out
        {
            let record = super::reply::source_record(provider, agent.inbox()?, *input)
                .await?
                .ok_or(Error::Record("cancelled input is unavailable"))?;
            let source = super::InboxRecord::decode(&record.value)?;
            if let Some((routing, within)) = routing
                && let Some(peer) =
                    super::routing::notices::PeerInput::parse(agent, &source, &record)?
            {
                results.insert(
                    *input,
                    peer.terminal(routing, within, super::SentReplyStatus::Dropped)?,
                );
            }
            if let Some(input) = super::input::input(*input, source)
                && let Some(run) = super::goal::continuing(agent, &input.input)
            {
                // A continuation can retire without a conversation entry.
                // Index its run in the same terminal-fact append and commit
                // as its physical input's cancellation, never a global floor.
                records.push(goal_cancellation(run, *conversation)?);
                goals.entry(run).or_insert(super::SentReplyStatus::Dropped);
            }
        }
        if let super::Out::GoalCompleted { run } = out {
            goals.insert(
                *run,
                super::SentReplyStatus::Answered {
                    sequence: sequence
                        .checked_sub(1)
                        .ok_or(Error::Record("goal completion has no conversation entry"))?,
                },
            );
        }
        let super::Out::Conversation(record) = out else {
            continue;
        };
        let entry = super::ConversationEntry::decode(record)?;
        let observed_before = preceding_frontier;
        preceding_frontier = Some(entry.observed_through);
        let settled_at = sequence;
        sequence = sequence
            .checked_add(1)
            .ok_or(Error::Record("agent conversation tail overflow"))?;
        // An initial goal entry can commit while its run still continues. It
        // is not an answer until the goal's terminal publication commits.
        if agent.depth() == 0
            && matches!(&entry.input, super::TurnInput::Message {
                from: super::Sender::Agent { path }, ..
            } if path.depth() == 0 && path != agent)
            && let Some((routing, within)) = routing
        {
            let record = super::reply::source_record(provider, agent.inbox()?, entry.inbox)
                .await?
                .ok_or(Error::Record("answered peer input is unavailable"))?;
            let source = super::InboxRecord::decode(&record.value)?;
            if entry.admitted_message(entry.inbox, &source).is_none() {
                return Err(Error::Record("conversation answered another peer input"));
            }
            if let Some(peer) = super::routing::notices::PeerInput::parse(agent, &source, &record)?
            {
                if let Some(run) = entry.goal {
                    deferred.entry(run).or_default().insert(entry.inbox);
                } else {
                    results.insert(
                        entry.inbox,
                        peer.terminal(
                            routing,
                            within,
                            super::SentReplyStatus::Answered {
                                sequence: settled_at,
                            },
                        )?,
                    );
                }
            }
        }
        completed.insert(entry.inbox, true);
        let Some(cut) = entry.steering else { continue };
        // Settlement carries its complete admitted interval. A parked turn's
        // frontier is supplied separately, so neither path depends on a later
        // cancellation delivery finding the retired input still queued.
        ranges.push((
            super::BoundaryCut {
                from: entry.inbox.saturating_add(1),
                through: cut.through,
            },
            Some((settled_at, observed_before, entry.goal)),
        ));
    }
    for (cut, settled_at) in ranges {
        let mut from = cut.from;
        while from < cut.through {
            let limit = (cut.through - from).min(super::MAX_QUEUED as u64) as u32;
            let end = from + u64::from(limit);
            let mut sources = provider
                .read(ReadRequest {
                    key: None,
                    path: agent.inbox()?,
                    from,
                    limit,
                    at_least: Default::default(),
                })
                .await
                .map_err(Error::Stream)?;
            while from < end {
                let record = sources
                    .try_next()
                    .await
                    .map_err(Error::Stream)?
                    .filter(|record| record.sequence == from)
                    .ok_or(Error::Record("settled steering input is unavailable"))?;
                let source = super::InboxRecord::decode(&record.value)?;
                if matches!(source, super::InboxRecord::Message(_)) {
                    completed
                        .entry(from)
                        .and_modify(|settled| *settled |= settled_at.is_some())
                        .or_insert(settled_at.is_some());
                    if let Some((settled_at, observed_before, goal)) = settled_at
                        && let Some((routing, within)) = routing
                        && let std::collections::btree_map::Entry::Vacant(result) =
                            results.entry(from)
                        && let Some(peer) =
                            super::routing::notices::PeerInput::parse(agent, &source, &record)?
                    {
                        let observed_before = match observed_before {
                            Some(frontier) => frontier,
                            None => match committed_frontier {
                                Some(frontier) => frontier,
                                None => {
                                    let frontier = match prior_conversation.checked_sub(1) {
                                        None => 0,
                                        Some(sequence) => {
                                            let record = super::reply::source_record(
                                                provider,
                                                agent.conversation()?,
                                                sequence,
                                            )
                                            .await?
                                            .ok_or(Error::Record(
                                                "preceding conversation is unavailable",
                                            ))?;
                                            super::ConversationEntry::decode(&record.value)?
                                                .observed_through
                                        }
                                    };
                                    committed_frontier = Some(frontier);
                                    frontier
                                }
                            },
                        };
                        if from >= observed_before {
                            observe_fence(provider, agent, &mut fence).await?;
                            if cancelled(provider, agent, from)
                                .await?
                                .is_some_and(|frontier| frontier <= settled_at)
                            {
                                result.insert(peer.terminal(
                                    routing,
                                    within,
                                    super::SentReplyStatus::Dropped,
                                )?);
                            } else if let Some(run) = goal {
                                deferred.entry(run).or_default().insert(from);
                            } else {
                                result.insert(peer.terminal(
                                    routing,
                                    within,
                                    super::SentReplyStatus::Answered {
                                        sequence: settled_at,
                                    },
                                )?);
                            }
                        }
                    }
                    if let Some(input) = super::input::input(from, source)
                        && let Some(run) = super::goal::continuing(agent, &input.input)
                    {
                        // The first terminal fact and this retirement must
                        // observe the same order. A changed tail is a source
                        // event that revalidates this actor publication.
                        observe_fence(provider, agent, &mut fence).await?;
                        if cancelled(provider, agent, from).await?.is_some() {
                            // A caller's requested frontier is not evidence of
                            // retirement; this commit's conversation cut is.
                            records.push(goal_cancellation(run, conversation)?);
                            goals.entry(run).or_insert(super::SentReplyStatus::Dropped);
                        }
                    }
                }
                from += 1;
            }
        }
    }
    if let Some((routing, within)) = routing
        && let Some(dropped) = retirement.dropped
    {
        if dropped.terminal {
            for run in deferred.keys() {
                goals.entry(*run).or_insert(super::SentReplyStatus::Dropped);
            }
        }
        let mut cancelled = std::collections::BTreeSet::new();
        let previous = dropped.previous;
        let mut from = previous
            .settled_before(dropped.cursor)
            .max(previous.dropped_before);
        while from < dropped.through {
            let limit = (dropped.through - from).min(super::MAX_QUEUED as u64) as u32;
            let end = from + u64::from(limit);
            let mut sources = provider
                .read(ReadRequest {
                    path: agent.inbox()?,
                    from,
                    limit,
                    key: None,
                    at_least: Default::default(),
                })
                .await
                .map_err(Error::Stream)?;
            while from < end {
                let record = sources
                    .try_next()
                    .await
                    .map_err(Error::Stream)?
                    .filter(|record| record.sequence == from)
                    .ok_or(Error::Record("retired input is unavailable"))?;
                let source = super::InboxRecord::decode(&record.value)?;
                let active = previous.turn.as_ref().is_some_and(|turn| {
                    turn.inbox == from
                        || (previous.dropped_before <= from
                            && turn.inbox < from
                            && from < previous.steered_through)
                });
                if completed.get(&from) != Some(&true)
                    && previous.input_pending(dropped.cursor, from)
                    && (!previous.already_steered(from, &source) || active)
                {
                    if let Some(peer) =
                        super::routing::notices::PeerInput::parse(agent, &source, &record)?
                        && let std::collections::btree_map::Entry::Vacant(result) =
                            results.entry(from)
                    {
                        result.insert(peer.terminal(
                            routing,
                            within,
                            super::SentReplyStatus::Dropped,
                        )?);
                        cancelled.insert(from);
                    }
                    if let Some(input) = super::input::input(from, source)
                        && let Some(run) = super::goal::continuing(agent, &input.input)
                    {
                        records.push(goal_cancellation(run, conversation)?);
                        goals.entry(run).or_insert(super::SentReplyStatus::Dropped);
                        cancelled.insert(from);
                    }
                }
                from += 1;
            }
        }
        records.extend(completion_records(
            &InputFact::Cancelled { conversation },
            cancelled,
            None,
        )?);
    }
    if let Some((routing, within)) = routing {
        for (run, status) in goals {
            let mut inputs = continuing::inputs(provider, agent, run).await?;
            if let Some(current) = deferred.get(&run) {
                inputs.extend(current);
            }
            for input in inputs {
                if let std::collections::btree_map::Entry::Vacant(result) = results.entry(input) {
                    let record = super::reply::source_record(provider, agent.inbox()?, input)
                        .await?
                        .ok_or(Error::Record("goal peer input is unavailable"))?;
                    let source = super::InboxRecord::decode(&record.value)?;
                    let peer = super::routing::notices::PeerInput::parse(agent, &source, &record)?
                        .ok_or(Error::Record("goal input is not an owned routed peer"))?;
                    result.insert(peer.terminal(routing, within, status.clone())?);
                }
            }
        }
    }
    for (run, inputs) in deferred {
        for input in &inputs {
            completed.remove(input);
        }
        records.extend(completion_records(
            &InputFact::Goal { run },
            inputs,
            Some(continuing::key(agent, run)?),
        )?);
    }
    if !completed.is_empty() {
        records.extend(completion_records(
            &InputFact::Completed,
            completed.into_keys(),
            None,
        )?);
    }
    if !records.is_empty() || !results.is_empty() {
        let index = request
            .effects
            .iter()
            .position(|effect| !effect.soft)
            .unwrap_or_else(|| {
                request
                    .effects
                    .push(acyclic_stream::CommitEffect::hard(Vec::new(), Vec::new()));
                request.effects.len() - 1
            });
        let effect = &mut request.effects[index];
        if let Some(condition) = &fence {
            effect.conditions.push(condition.clone());
        }
        if !results.is_empty()
            && let Some((routing, _)) = routing
        {
            let results = results.into_values().collect::<Vec<_>>();
            let path = routing.paths.outbox(&routing.node).map_err(Error::Stream)?;
            if let Some(acyclic_stream::CommitMutation::Append { records, .. }) = effect
                .mutations
                .iter_mut()
                .find(|mutation| matches!(mutation, acyclic_stream::CommitMutation::Append {path: candidate, ..} if candidate == &path))
            {
                records.extend(results);
            } else {
                effect.mutations.push(acyclic_stream::CommitMutation::Append {
                    path,
                    records: results,
                });
            }
        }
        if records.is_empty() {
            return Ok(fence.is_some());
        }
        let path = input_events(agent)?;
        if let Some(acyclic_stream::CommitMutation::Append { records: original, .. }) = effect.mutations.iter_mut()
            .find(|mutation| matches!(mutation, acyclic_stream::CommitMutation::Append {path: candidate, ..} if candidate == &path)) {
            original.extend(records);
        } else {
            effect.mutations.push(acyclic_stream::CommitMutation::Append { path, records });
        }
    }
    Ok(fence.is_some())
}

/// Share terminal-fact batching; a goal key indexes the existing physical
/// completion records instead of introducing a second pending-message log.
fn completion_records(
    fact: &InputFact,
    inputs: impl IntoIterator<Item = u64>,
    index: Option<Vec<u8>>,
) -> Result<Vec<acyclic_stream::AppendRecord>, Error> {
    let value = fact.encode()?;
    let per_record =
        (acyclic_stream::MAX_RECORD_BYTES - value.len() - index.as_ref().map_or(0, Vec::len))
            / std::mem::size_of::<u64>();
    let keys = inputs
        .into_iter()
        .map(|input| input.to_be_bytes().to_vec().into())
        .collect::<Vec<acyclic_stream::OwnedBytes>>();
    keys.chunks(per_record)
        .map(|keys| {
            let mut keys = keys.to_vec();
            if let Some(index) = &index {
                let at = keys.partition_point(|key| key.as_ref() < index.as_slice());
                keys.insert(at, index.clone().into());
            }
            Ok(acyclic_stream::AppendRecord {
                value: value.clone().into(),
                headers: acyclic_stream::RecordHeaders::new(keys, Vec::new())
                    .map_err(Error::Stream)?,
            })
        })
        .collect()
}

/// Share the one terminal-fact cut across all decisions in this publication.
async fn observe_fence<P: acyclic_stream::StreamProvider + ?Sized>(
    provider: &P,
    agent: &super::AgentPath,
    fence: &mut Option<acyclic_stream::CommitCondition>,
) -> Result<(), Error> {
    if fence.is_none() {
        let path = input_events(agent)?;
        *fence = Some(match provider.head(path.clone()).await {
            Ok(head) => acyclic_stream::CommitCondition::Tail {
                path,
                expected: head.tail,
            },
            Err(error) if error.category() == acyclic_stream::StreamError::NotFound => {
                acyclic_stream::CommitCondition::Absent { path }
            }
            Err(error) => return Err(Error::Stream(error)),
        });
    }
    Ok(())
}

fn goal_cancellation(
    run: crate::OperationId,
    conversation: u64,
) -> Result<acyclic_stream::AppendRecord, Error> {
    Ok(acyclic_stream::AppendRecord {
        value: InputFact::Cancelled { conversation }.encode()?.into(),
        headers: acyclic_stream::RecordHeaders::new(
            vec![run.into_bytes().to_vec().into()],
            Vec::new(),
        )
        .map_err(Error::Stream)?,
    })
}

pub(super) async fn cancelled<P: acyclic_stream::StreamProvider + ?Sized>(
    provider: &P,
    agent: &super::AgentPath,
    input: u64,
) -> Result<Option<u64>, Error> {
    cancelled_key(provider, agent, input.to_be_bytes().to_vec()).await
}

/// Run keys are 16 bytes; source-input keys are 8 bytes, so their domains do not overlap.
pub(super) async fn cancelled_goal<P: acyclic_stream::StreamProvider + ?Sized>(
    provider: &P,
    agent: &super::AgentPath,
    run: crate::OperationId,
) -> Result<Option<u64>, Error> {
    cancelled_key(provider, agent, run.into_bytes().to_vec()).await
}

async fn cancelled_key<P: acyclic_stream::StreamProvider + ?Sized>(
    provider: &P,
    agent: &super::AgentPath,
    key: Vec<u8>,
) -> Result<Option<u64>, Error> {
    let records = provider
        .read(ReadRequest {
            key: Some(acyclic_stream::ReadKeySeek::exact(key, u64::MAX)),
            path: input_events(agent)?,
            from: 0,
            limit: 1,
            at_least: Default::default(),
        })
        .await;
    let mut records = match records {
        Ok(records) => records,
        Err(error) if error.category() == acyclic_stream::StreamError::NotFound => return Ok(None),
        Err(error) => return Err(Error::Stream(error)),
    };
    let Some(record) = records.try_next().await.map_err(Error::Stream)? else {
        return Ok(None);
    };
    Ok(match InputFact::decode(&record.value)? {
        InputFact::Cancelled { conversation } => Some(conversation),
        InputFact::Completed | InputFact::Goal { .. } => None,
    })
}

pub(super) async fn accepted<P: acyclic_stream::StreamProvider + ?Sized>(
    provider: &P,
    agent: &super::AgentPath,
    cancel: &super::Cancellation,
) -> Result<bool, Error> {
    Ok(cancelled(provider, agent, cancel.input).await? == Some(cancel.conversation))
}

pub(super) fn cancel_effect(
    agent: &super::AgentPath,
    input: u64,
    conversation: u64,
) -> Result<acyclic_stream::CommitEffect, Error> {
    let (path, record) = cancellation(agent, input, conversation)?;
    Ok(acyclic_stream::CommitEffect::hard(
        Vec::new(),
        vec![acyclic_stream::CommitMutation::Append {
            path,
            records: vec![record],
        }],
    ))
}

/// Selected-input admission shares the original driver and its custody. One
/// point read prevents cancelled queued work from starting; a resumed turn
/// performs no extra read and retains its original execution identity.
pub(super) struct Driver<'a, P: ?Sized, D: ?Sized> {
    pub provider: &'a P,
    pub inner: &'a D,
}

impl<P: crate::store::JournalProvider + ?Sized, D: super::TurnDriver + ?Sized> super::TurnDriver
    for Driver<'_, P, D>
{
    fn review<'a>(
        &'a self,
        agent: &'a super::AgentState,
        cursor: u64,
        cancellation: acyclic_actors::guest::CancellationToken,
    ) -> super::ReviewFuture<'a> {
        self.inner.review(agent, cursor, cancellation)
    }

    fn step<'a>(
        &'a self,
        agent: &'a super::AgentState,
        cursor: u64,
        input: Option<super::Queued>,
        cancellation: acyclic_actors::guest::CancellationToken,
    ) -> super::TurnFuture<'a> {
        Box::pin(async move {
            if let Some(selected) = &input
                && cancelled(self.provider, &agent.path, selected.inbox)
                    .await?
                    .is_some_and(|frontier| frontier <= agent.conversation_tail)
            {
                let retained = match (&agent.turn, &agent.phase) {
                    (None, _) => Some((None, super::Wait::Input)),
                    (Some(turn), super::Phase::WaitingApproval { id, binding }) => Some((
                        Some(turn.checkpoint.clone()),
                        super::Wait::Approval {
                            id: id.clone(),
                            binding: binding.clone(),
                        },
                    )),
                    (Some(turn), super::Phase::WaitingHost { host }) => Some((
                        Some(turn.checkpoint.clone()),
                        super::Wait::Host(host.clone()),
                    )),
                    _ => None,
                };
                if let Some((checkpoint, wait)) = retained {
                    return Ok(super::TurnStep {
                        checkpoint,
                        wait,
                        drops: None,
                        outs: vec![super::Out::InputCancelled {
                            input: selected.inbox,
                            conversation: agent.conversation_tail,
                        }],
                    });
                }
            }
            self.inner.step(agent, cursor, input, cancellation).await
        })
    }
}
