//! The epoch pointer and preparation identity are one record, never separate
//! writes. Reopening reads only the last record, independent of history length.

use super::{
    Epoch, FileRef, InvalidEpoch,
    protocol::{Completion, Pending, Refusal, State},
};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitId, CommitMutation, CommitRequest, Record, StreamPath,
};
use serde::{Deserialize, Serialize};

/// A checked epoch and the preparation custody retained with it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    epoch: Epoch,
    state: State,
    outcome: Option<SummaryOutcome>,
    anchor: Option<(u64, CommitId)>,
}

/// The immutable result associated with the prepared summary identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "body",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum SummaryOutcome {
    /// Exact immutable reference returned by the original summary producer.
    Ready(FileRef),
    /// The producer terminated without a summary, including loss of custody.
    Failed,
}

impl SummaryOutcome {
    const fn completion(&self) -> Completion {
        match self {
            Self::Ready(_) => Completion::Ready,
            Self::Failed => Completion::Failed,
        }
    }
}

/// Refusal before an epoch record or conditional publication is constructed.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// The summary does not cover this source prefix.
    #[error(transparent)]
    Epoch(#[from] InvalidEpoch),
    /// A transition violates the single-producer protocol.
    #[error("epoch transition refused: {0:?}")]
    Transition(Refusal),
    /// The stored preparation and its content address disagree.
    #[error("epoch preparation identity differs from its source range")]
    Identity,
    /// A swap supplied another body's reference for the prepared result.
    #[error("epoch summary differs from its committed producer result")]
    ResultReference,
    /// Unsupported or malformed durable bytes.
    #[error("epoch record encoding: {0}")]
    Encoding(#[from] serde_json::Error),
}

impl From<Refusal> for SnapshotError {
    fn from(value: Refusal) -> Self {
        Self::Transition(value)
    }
}

// These are wire adapters only; all transitions use the one protocol reducer.
#[derive(Serialize, Deserialize)]
#[serde(tag = "version", content = "snapshot", deny_unknown_fields)]
enum RecordWire<E, O> {
    #[serde(rename = "1")]
    V1(SnapshotWire<E, O>),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotWire<E, O> {
    epoch: E,
    generation: u64,
    pending: Option<PendingWire<O>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingWire<O> {
    summary: [u8; 32],
    generation: u64,
    through: u64,
    completion: Option<O>,
}

impl Snapshot {
    /// Starts a conversation without losing any prefix. A nonempty summary
    /// must arrive through the prepared swap, never this constructor.
    pub fn genesis(epoch: Epoch) -> Result<Self, SnapshotError> {
        epoch.validate(&epoch.key.source, 0)?;
        Ok(Self {
            epoch,
            state: State::default(),
            outcome: None,
            anchor: None,
        })
    }

    /// Admits the one versioned record against its inherited source and head.
    /// Decoding never scans previous snapshots or reconstructs a missing job.
    pub fn decode(record: &Record, source: &StreamPath, head: u64) -> Result<Self, SnapshotError> {
        let RecordWire::V1(wire): RecordWire<Epoch, SummaryOutcome> =
            serde_json::from_slice(&record.value)?;
        wire.epoch.validate(source, head)?;
        if (wire.generation == 0) != (wire.epoch.cursor() == 0) {
            return Err(Refusal::Stale.into());
        }
        let (pending, outcome) = match wire.pending {
            Some(pending) => (
                Some(Pending {
                    summary: pending.summary,
                    generation: pending.generation,
                    through: pending.through,
                    completion: pending.completion.as_ref().map(SummaryOutcome::completion),
                }),
                pending.completion,
            ),
            None => (None, None),
        };
        let state = State::restore(wire.generation, wire.epoch.cursor(), pending, head)?;
        if let Some(pending) = state.pending() {
            if wire.epoch.key.digest_through(pending.through) != pending.summary {
                return Err(SnapshotError::Identity);
            }
        }
        Ok(Self {
            epoch: wire.epoch,
            state,
            outcome,
            anchor: Some((record.sequence, record.commit_id)),
        })
    }

    /// Frozen request prefix and source coverage.
    #[must_use]
    pub const fn epoch(&self) -> &Epoch {
        &self.epoch
    }

    /// Constant-size protocol view. Updating the copy cannot change this record.
    #[must_use]
    pub const fn state(&self) -> State {
        self.state
    }

    /// The original producer's result, retained across reopen without rereading
    /// or reconstructing another result reference.
    #[must_use]
    pub const fn outcome(&self) -> Option<&SummaryOutcome> {
        self.outcome.as_ref()
    }

    /// Pins exactly the content address of this source range. The common
    /// ancestor's conditional publication arbitrates shared producer ownership.
    pub fn prepare(&mut self, through: u64, head: u64) -> Result<[u8; 32], SnapshotError> {
        let summary = self.epoch.key.digest_through(through);
        self.state.prepare(summary, through, head)?;
        Ok(summary)
    }

    /// Accepts only the shared producer's immutable terminal outcome.
    pub fn complete(
        &mut self,
        summary: [u8; 32],
        outcome: SummaryOutcome,
    ) -> Result<(), SnapshotError> {
        let mut state = self.state;
        state.complete(summary, outcome.completion())?;
        if self.outcome.as_ref().is_some_and(|prior| prior != &outcome) {
            return Err(Refusal::Contradiction.into());
        }
        self.state = state;
        self.outcome = Some(outcome);
        Ok(())
    }

    /// Installs the summary and frozen Header together. Refusal leaves both
    /// the epoch and its pending identity unchanged.
    pub fn swap(&mut self, expected: u64, next: Epoch, head: u64) -> Result<(), SnapshotError> {
        next.validate(&self.epoch.key.source, head)?;
        let pending = self.state.pending().ok_or(Refusal::Stale)?;
        if next.key.digest() != pending.summary {
            return Err(SnapshotError::Identity);
        }
        let mut state = self.state;
        state.swap(pending.summary, expected, head)?;
        if !matches!((&self.outcome, &next.summary),
            (Some(SummaryOutcome::Ready(body)), Some(summary)) if body == summary)
        {
            return Err(SnapshotError::ResultReference);
        }
        self.state = state;
        self.epoch = next;
        self.outcome = None;
        Ok(())
    }

    /// Adds the epoch, pending identity and result to the caller's existing
    /// conditional commit. Its actor checkpoint, request identity and deadline
    /// stay on that same request. Encoding failure leaves the request unchanged.
    /// There is no separate request, flush, head lookup or publication here.
    pub fn append_to(
        &self,
        path: StreamPath,
        request: &mut CommitRequest,
    ) -> Result<(), SnapshotError> {
        let condition = if let Some((sequence, commit_id)) = self.anchor {
            CommitCondition::Head {
                path: path.clone(),
                expected: sequence.checked_add(1).ok_or(Refusal::Exhausted)?,
                commit_id,
            }
        } else {
            CommitCondition::Absent { path: path.clone() }
        };
        let wire = RecordWire::V1(SnapshotWire {
            epoch: &self.epoch,
            generation: self.state.generation(),
            pending: self.state.pending().map(|pending| PendingWire {
                summary: pending.summary,
                generation: pending.generation,
                through: pending.through,
                completion: self.outcome.as_ref(),
            }),
        });
        let record = AppendRecord::from(serde_json::to_vec(&wire)?);
        request.conditions.push(condition);
        request.mutations.push(CommitMutation::Append {
            path,
            records: vec![record],
        });
        Ok(())
    }
}
