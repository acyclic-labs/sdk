//! Inbox observations at model boundaries. They do not race or drop tool futures.

use super::{AgentRecord as _, AgentState, Error, InboxRecord, turn_input};
use acyclic_stream::{ReadRequest, StreamProvider};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

/// One bounded inbox observation, frozen before the next request is admitted.
/// `through` is an exclusive source cursor, not a model-visibility claim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundarySelection {
    /// First examined source sequence.
    pub from: u64,
    /// First unexamined source sequence.
    pub through: u64,
    /// Original source sequences and records, in order; never an out-of-band queue.
    pub inputs: Vec<(u64, InboxRecord)>,
}

/// Immutable inbox range pinned with the executor's prepared request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct BoundaryCut {
    /// First examined source sequence.
    pub from: u64,
    /// First unexamined source sequence.
    pub through: u64,
}

/// Read one window at an actual model boundary. A later arrival belongs to a
/// later boundary. Cancel/archive are barriers and remain explicit interrupts.
/// `agent` and `cursor` are the original actor delivery's admitted state and
/// exclusive inbox cursor, after any awaited response was taken from its queue.
pub async fn select_boundary<P: StreamProvider + ?Sized>(
    provider: &P,
    agent: &AgentState,
    cursor: u64,
    from: u64,
    initial: u64,
    until: Option<u64>,
) -> Result<BoundarySelection, Error> {
    let limit = match until {
        Some(until) => u32::try_from(
            until
                .checked_sub(from)
                .ok_or(Error::Record("steering cursor precedes its source range"))?,
        )
        .ok()
        .filter(|count| *count <= super::MAX_QUEUED as u32)
        .ok_or(Error::Record("steering range exceeds one inbox window"))?,
        None => super::MAX_QUEUED as u32,
    };
    let mut selection = BoundarySelection {
        from,
        through: from,
        inputs: Vec::new(),
    };
    if limit == 0 {
        return Ok(selection);
    }
    let records = provider
        .read(ReadRequest {
            key: None,
            path: agent.path.inbox()?,
            from,
            limit,
            at_least: Default::default(),
        })
        .await;
    let mut records = match records {
        Ok(records) => records,
        Err(acyclic_stream::StreamError::NotFound) if until.is_none() => return Ok(selection),
        Err(error) => return Err(Error::Stream(error)),
    };
    while let Some(record) = records.try_next().await.map_err(Error::Stream)? {
        if record.sequence != selection.through {
            return Err(Error::Record("steering inbox is not contiguous"));
        }
        let input = InboxRecord::decode(&record.value)?;
        let queued = match input {
            InboxRecord::Cancel(_) | InboxRecord::Archive(_) => break,
            InboxRecord::Notice { .. } | InboxRecord::Message(_) => Some(input),
            other => {
                // Inputs that keep their own turn are queue barriers. Passing
                // one would let a later steering message settle before an
                // earlier turn, destroying the inbox/conversation ordering.
                if record.sequence > initial
                    && agent.input_pending(cursor, record.sequence)
                    && turn_input(record.sequence, other).is_some()
                {
                    break;
                }
                None
            }
        };
        selection.through = record
            .sequence
            .checked_add(1)
            .ok_or(Error::Record("steering inbox cursor overflow"))?;
        if let Some(input) = queued {
            selection.inputs.push((record.sequence, input));
        }
    }
    if until.is_some_and(|until| until != selection.through) {
        return Err(Error::Record("frozen steering range is unavailable"));
    }
    Ok(selection)
}

/// A journal's model-boundary binding. The executor freezes the selected range
/// with ContextPrepared; replay must reconstruct only that immutable range.
pub trait ModelBoundary: acyclic_stream::ProviderPlatform {
    /// Inputs at this safe boundary, after every prior tool result settled.
    fn messages(
        &self,
        operation: crate::OperationId,
        step: u32,
        frozen: Option<BoundaryCut>,
    ) -> acyclic_stream::BoxProviderFuture<'_, crate::Result<Vec<crate::model::ModelMessage>>>;

    /// Range selected for this request; published by the existing request journal.
    fn selection(&self, operation: crate::OperationId, step: u32) -> crate::Result<BoundaryCut>;

    /// Called only after ModelStarted is durable, including on replay. A frozen
    /// selection alone never claims recipient visibility or removes queue input.
    fn admitted(
        &self,
        operation: crate::OperationId,
        step: u32,
    ) -> acyclic_stream::BoxProviderFuture<'_, crate::Result<()>>;

    /// Retain this turn's admitted boundary messages in its inherited history.
    fn transcript(
        &self,
        operation: crate::OperationId,
    ) -> acyclic_stream::BoxProviderFuture<'_, crate::Result<Option<SteeringTranscript>>>;
}

/// One inherited transcript reference, independent of the number of inputs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct SteeringTranscript {
    /// Exclusive inbox cursor included in admitted model requests.
    pub through: u64,
    /// Original frozen messages on the agent's private conversation volume.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
    pub file: crate::conversation::FileRef,
}
