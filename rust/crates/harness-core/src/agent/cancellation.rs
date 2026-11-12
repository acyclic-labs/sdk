//! Targeted cancellation, published with its source inbox control.
use super::{AgentRecord as _, Error};
use acyclic_stream::{ReadRequest, StreamPath};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};
/// A cancellation and the original checkpoint frontiers that publish it.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CancelledInput {
    pub conversation: u64,
}

impl super::AgentRecord for CancelledInput {}

pub(super) fn cancelled_path(agent: &super::AgentPath, input: u64) -> Result<StreamPath, Error> {
    StreamPath::new(format!("{agent}/context/cancelled/{input}")).map_err(Error::Stream)
}

pub(super) async fn cancelled<P: crate::store::JournalProvider + ?Sized>(
    provider: &P,
    agent: &super::AgentPath,
    input: u64,
) -> Result<Option<u64>, Error> {
    let records = provider
        .read(ReadRequest {
            key: None,
            path: cancelled_path(agent, input)?,
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
    let fact = CancelledInput::decode(&record.value)?;
    Ok(Some(fact.conversation))
}

pub(super) fn cancel_effect(
    agent: &super::AgentPath,
    input: u64,
    conversation: u64,
) -> Result<acyclic_stream::CommitEffect, Error> {
    let path = cancelled_path(agent, input)?;
    Ok(acyclic_stream::CommitEffect {
        conditions: vec![
            acyclic_stream::CommitCondition::Absent { path: path.clone() },
            acyclic_stream::CommitCondition::Tail {
                path: agent.conversation()?,
                expected: conversation,
            },
        ],
        mutations: vec![acyclic_stream::CommitMutation::Append {
            path,
            records: vec![CancelledInput { conversation }.encode()?.into()],
        }],
        soft: true,
    })
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
