//! Inbox observations at model boundaries. They do not race or drop tool futures.

use super::{AgentPath, AgentRecord as _, Error, InboxRecord};
use acyclic_stream::{ReadRequest, StreamProvider, StreamProviderExt as _};
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

/// How an inbox observation ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundaryEnd {
    /// Observe the current bounded window.
    Open,
    /// Stop at this exclusive source cursor or the end of a window.
    Fence(u64),
    /// Replay exactly the admitted source range.
    Exact(u64),
}

/// Read one window at an actual model boundary. A later arrival belongs to a
/// later boundary. Cancel/archive are barriers and remain explicit interrupts.
/// `from` is the last admitted model frontier; a frozen range is replayed
/// independently of which unrelated queued inputs the actor has consumed.
pub async fn select_boundary<P: StreamProvider + ?Sized>(
    provider: &P,
    agent: &AgentPath,
    from: u64,
    boundary: BoundaryEnd,
) -> Result<BoundarySelection, Error> {
    let until = match boundary {
        BoundaryEnd::Open => None,
        BoundaryEnd::Fence(fence) | BoundaryEnd::Exact(fence) => Some(fence),
    };
    if until.is_some_and(|until| until < from) {
        return Err(Error::Record("steering cursor precedes its source range"));
    }
    let mut selection = BoundarySelection {
        from,
        through: from,
        inputs: Vec::new(),
    };
    let mut end = until;
    loop {
        let room = super::MAX_QUEUED - selection.inputs.len();
        let limit = end.map_or(room as u64, |until| {
            (until - selection.through).min(room as u64)
        }) as u32;
        if limit == 0 {
            break;
        }
        let records = provider
            .read(ReadRequest {
                key: None,
                path: agent.inbox()?,
                from: selection.through,
                limit,
                at_least: Default::default(),
            })
            .await;
        let mut records = match records {
            Ok(records) => records,
            Err(acyclic_stream::StreamError::NotFound)
                if !matches!(boundary, BoundaryEnd::Exact(_)) =>
            {
                break;
            }
            Err(error) => return Err(Error::Stream(error)),
        };
        let mut read = 0;
        let mut barrier = false;
        while let Some(record) = records.try_next().await.map_err(Error::Stream)? {
            if record.sequence != selection.through {
                return Err(Error::Record("steering inbox is not contiguous"));
            }
            let input = InboxRecord::decode(&record.value)?;
            if matches!(input, InboxRecord::Cancel(_) | InboxRecord::Archive(_)) {
                barrier = true;
                break;
            }
            selection.through = record
                .sequence
                .checked_add(1)
                .ok_or(Error::Record("steering inbox cursor overflow"))?;
            read += 1;
            // Independent records keep their ordered turns. Page past them
            // without retaining them or capping how far a reply can arrive.
            if matches!(input, InboxRecord::Notice { .. } | InboxRecord::Message(_)) {
                selection.inputs.push((record.sequence, input));
            }
        }
        if barrier || read < limit {
            break;
        }
        if end.is_none() && selection.inputs.len() < super::MAX_QUEUED {
            // Only a paged observation needs a head fence. It bounds this cut
            // even if independent producers keep appending during the reads;
            // the ordinary single-page path buys no extra control request.
            end = Some(
                provider
                    .head(agent.inbox()?)
                    .await
                    .map_err(Error::Stream)?
                    .tail,
            );
        }
    }
    if matches!(boundary, BoundaryEnd::Exact(until) if until != selection.through) {
        return Err(Error::Record("frozen steering range is unavailable"));
    }
    Ok(selection)
}

/// Exact cumulative steering retained in the existing prepared projection.
#[derive(Clone, Debug, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct SteeringSnapshot {
    /// Exclusive source cursor of the retained messages.
    pub through: u64,
    /// Messages as rendered at their original boundaries, before transformation.
    pub messages: Vec<crate::model::ModelMessage>,
}

/// A journal's model-boundary binding. The executor freezes the selected range
/// with ContextPrepared; replay must reconstruct only that immutable range.
pub trait ModelBoundary: acyclic_stream::ProviderPlatform {
    /// Include a source update published during turn preparation in the first
    /// request. Its committed exclusive cursor fences the ordered inbox window;
    /// unrelated later arrivals remain for their original next boundary.
    fn include_prepared_sources(
        &self,
        _operation: crate::OperationId,
        _through: u64,
    ) -> crate::Result<()> {
        Err(crate::Error::Unsupported(
            "model boundary cannot include prepared sources".into(),
        ))
    }

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

    /// Snapshot the candidate before ContextPrepared is published. Only a later
    /// durable ModelStarted admits it; preparation alone grants no visibility.
    fn prepared_snapshot(
        &self,
        _operation: crate::OperationId,
        _step: u32,
    ) -> crate::Result<Option<SteeringSnapshot>> {
        Ok(None)
    }

    /// Restore the original journal's admitted snapshot before cancellation settles.
    fn restore_snapshot(
        &self,
        _operation: crate::OperationId,
        _snapshot: Option<SteeringSnapshot>,
    ) -> crate::Result<()> {
        Ok(())
    }

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
