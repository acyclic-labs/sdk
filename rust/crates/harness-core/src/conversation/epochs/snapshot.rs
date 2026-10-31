//! The epoch pointer and preparation identity are one record, never separate
//! writes. Reopening reads only the last record, independent of history length.

use super::{
    Epoch, InvalidEpoch,
    protocol::{Completion, Pending, Refusal, State},
};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitId, CommitMutation, CommitRequest, IdempotencyKey, Record,
    StreamPath,
};
use serde::{Deserialize, Serialize};

/// A checked epoch and the preparation custody retained with it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    epoch: Epoch,
    state: State,
    anchor: Option<(u64, CommitId)>,
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
enum RecordWire<E> {
    #[serde(rename = "1")]
    V1(SnapshotWire<E>),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotWire<E> {
    epoch: E,
    generation: u64,
    pending: Option<PendingWire>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingWire {
    summary: [u8; 32],
    generation: u64,
    through: u64,
    completion: Option<CompletionWire>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CompletionWire {
    Ready,
    Failed,
}

impl From<Pending> for PendingWire {
    fn from(value: Pending) -> Self {
        Self {
            summary: value.summary,
            generation: value.generation,
            through: value.through,
            completion: value.completion.map(|outcome| match outcome {
                Completion::Ready => CompletionWire::Ready,
                Completion::Failed => CompletionWire::Failed,
            }),
        }
    }
}

impl From<PendingWire> for Pending {
    fn from(value: PendingWire) -> Self {
        Self {
            summary: value.summary,
            generation: value.generation,
            through: value.through,
            completion: value.completion.map(|outcome| match outcome {
                CompletionWire::Ready => Completion::Ready,
                CompletionWire::Failed => Completion::Failed,
            }),
        }
    }
}

impl Snapshot {
    /// Starts a conversation without losing any prefix. A nonempty summary
    /// must arrive through the prepared swap, never this constructor.
    pub fn genesis(epoch: Epoch) -> Result<Self, SnapshotError> {
        epoch.validate(&epoch.key.source, 0)?;
        Ok(Self {
            epoch,
            state: State::default(),
            anchor: None,
        })
    }

    /// Admits the one versioned record against its inherited source and head.
    /// Decoding never scans previous snapshots or reconstructs a missing job.
    pub fn decode(record: &Record, source: &StreamPath, head: u64) -> Result<Self, SnapshotError> {
        let RecordWire::V1(wire): RecordWire<Epoch> = serde_json::from_slice(&record.value)?;
        wire.epoch.validate(source, head)?;
        if (wire.generation == 0) != (wire.epoch.cursor() == 0) {
            return Err(Refusal::Stale.into());
        }
        let state = State::restore(
            wire.generation,
            wire.epoch.cursor(),
            wire.pending.map(Into::into),
            head,
        )?;
        if let Some(pending) = state.pending() {
            if wire.epoch.key.digest_through(pending.through) != pending.summary {
                return Err(SnapshotError::Identity);
            }
        }
        Ok(Self {
            epoch: wire.epoch,
            state,
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

    /// Claims exactly the content address of this source range. The common
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
        completion: Completion,
    ) -> Result<(), SnapshotError> {
        Ok(self.state.complete(summary, completion)?)
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
        self.state = state;
        self.epoch = next;
        Ok(())
    }

    /// Encodes the epoch and pending identity as one conditional append. The
    /// caller composes this request with its actor checkpoint before submitting
    /// once. There is no flush, head lookup or publication inside this method.
    pub fn publication(
        &self,
        path: StreamPath,
        idempotency_key: IdempotencyKey,
    ) -> Result<CommitRequest, SnapshotError> {
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
            pending: self.state.pending().map(Into::into),
        });
        Ok(CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![condition],
            mutations: vec![CommitMutation::Append {
                path,
                records: vec![AppendRecord::from(serde_json::to_vec(&wire)?)],
            }],
            idempotency_key,
        })
    }
}
