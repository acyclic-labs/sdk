//! The epoch pointer and preparation identity are one record, never separate
//! writes. Reopening reads only the last record, independent of history length.

use super::{
    Epoch, FileRef, InvalidEpoch,
    protocol::{Completion, Demand, Pending, Pressure, Refusal, State},
};
#[cfg(feature = "filesystem")]
use acyclic_stream::{CommitCondition, OwnedBytes};
use acyclic_stream::{CommitId, Record, StreamPath};
use serde::{Deserialize, Serialize};

/// A checked epoch and the preparation custody retained with it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    epoch: Epoch,
    state: State,
    outcome: Option<SummaryOutcome>,
    attempt: u64,
    anchor: Option<(u64, CommitId)>,
}

/// The executor's next action after verified input accounting. Preparation
/// changes only pending custody; the visible epoch stays fixed until swap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Admission {
    /// Dispatch the original request with its byte-identical epoch prefix.
    Unchanged,
    /// Publish the preparation conditionally, then drive the sole producer.
    Produce {
        /// Original identity retained on restore and producer observation.
        summary: [u8; 32],
        /// Hard demand drives the same producer before dispatching a response.
        foreground: bool,
    },
    /// Wait for the existing exact-generation event; do not start work.
    Await([u8; 32]),
    /// Conditionally install the existing successful summary before dispatch.
    Swap([u8; 32]),
}

/// One encoded contribution to the original conditional publication.
/// Keep this owner alive until that publication has retired its borrowed bytes.
#[cfg(feature = "filesystem")]
pub struct EncodedSnapshot<'io> {
    condition: CommitCondition,
    record: OwnedBytes,
    // Physical values retire before their original preparation reservation.
    _hold: Option<crate::conversation::ContentHold<'io>>,
}

#[cfg(feature = "filesystem")]
impl EncodedSnapshot<'_> {
    /// Original optimistic fence and already-encoded record. The shared
    /// publication builder admits its own request storage, without reencoding.
    #[must_use]
    pub const fn parts(&self) -> (&CommitCondition, &OwnedBytes) {
        (&self.condition, &self.record)
    }
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
    #[serde(rename = "2")]
    V2(SnapshotWire<E, O>),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotWire<E, O> {
    epoch: E,
    generation: u64,
    attempt: u64,
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
            attempt: 0,
            anchor: None,
        })
    }

    /// Admits the one versioned record against its inherited source and head.
    /// Decoding never scans previous snapshots or reconstructs a missing job.
    pub fn decode(record: &Record, source: &StreamPath, head: u64) -> Result<Self, SnapshotError> {
        let RecordWire::V2(wire): RecordWire<Epoch, SummaryOutcome> =
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
            if wire.epoch.key.attempt_digest(pending.through, wire.attempt) != pending.summary {
                return Err(SnapshotError::Identity);
            }
        } else if wire.attempt != 0 {
            return Err(SnapshotError::Identity);
        }
        Ok(Self {
            epoch: wire.epoch,
            state,
            outcome,
            attempt: wire.attempt,
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

    /// Advances the fence from this snapshot's single record in the original
    /// confirmed conditional publication. The publication owner must first
    /// validate that member's path, bytes and receipt; this is not admission.
    /// Exact receipt replay is idempotent. No record is reread or reencoded.
    pub fn confirm(&mut self, sequence: u64, commit_id: CommitId) -> Result<(), SnapshotError> {
        if self.anchor == Some((sequence, commit_id)) {
            return Ok(());
        }
        let expected = match self.anchor {
            None => Some(0),
            Some((previous, original)) if original != commit_id => previous.checked_add(1),
            Some(_) => None,
        };
        if expected != Some(sequence) {
            return Err(Refusal::Stale.into());
        }
        self.anchor = Some((sequence, commit_id));
        Ok(())
    }

    /// Pins exactly the content address of this source range. The common
    /// ancestor's conditional publication arbitrates shared producer ownership.
    pub fn prepare(&mut self, through: u64, head: u64) -> Result<[u8; 32], SnapshotError> {
        let summary = self.epoch.key.attempt_digest(through, self.attempt);
        self.state.prepare(summary, through, head)?;
        Ok(summary)
    }

    /// One decision per request admission, with no wait or history read. The
    /// owner publishes a Produce candidate before attaching its future. A
    /// foreground failure returns to that caller; it must not loop admission
    /// into another producer. A later independent admission may retry it.
    pub fn admit(
        &mut self,
        pressure: Pressure,
        closed: u64,
        head: u64,
    ) -> Result<Admission, SnapshotError> {
        match (pressure, self.state.demand()) {
            (Pressure::Below, _) => Ok(Admission::Unchanged),
            (Pressure::Soft, Demand::Unprepared) if closed > self.state.cursor() => {
                Ok(Admission::Produce {
                    summary: self.prepare(closed, head)?,
                    foreground: false,
                })
            }
            (Pressure::Soft, _) => Ok(Admission::Unchanged),
            (Pressure::Hard, Demand::Unprepared) => Ok(Admission::Produce {
                summary: self.prepare(closed, head)?,
                foreground: true,
            }),
            (Pressure::Hard, Demand::Await(summary)) => Ok(Admission::Await(summary)),
            (Pressure::Hard, Demand::Swap(summary)) => Ok(Admission::Swap(summary)),
            (Pressure::Hard, Demand::Failed(_)) => Ok(Admission::Produce {
                summary: self.retry_failed(closed, head)?,
                foreground: true,
            }),
        }
    }

    /// A foreground fallback is a fresh attempt on the same content-addressed
    /// source. The failed result remains immutable in the preceding snapshot.
    /// Publication uses the original snapshot fence and codec.
    pub fn retry_failed(&mut self, through: u64, head: u64) -> Result<[u8; 32], SnapshotError> {
        let pending = self.state.pending().ok_or(Refusal::Stale)?;
        let attempt = self.attempt.checked_add(1).ok_or(Refusal::Exhausted)?;
        let summary = self.epoch.key.attempt_digest(through, attempt);
        let mut state = self.state;
        state.retry_failed(pending.summary, summary, through, head)?;
        self.state = state;
        self.attempt = attempt;
        self.outcome = None;
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
        if next.key.digest() != self.epoch.key.digest_through(pending.through) {
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
        self.attempt = 0;
        Ok(())
    }

    /// Encodes the epoch, pending identity and result on the original operation's
    /// ledger. Native bytes retain their own custody; standalone bytes retain
    /// the original reservation here. The path is moved, never reconstructed.
    /// No request, submission, head lookup or durability barrier is created.
    #[cfg(feature = "filesystem")]
    pub fn encode<'io>(
        &self,
        path: StreamPath,
        io: &'io crate::filesystem::PreparationIo,
    ) -> crate::Result<EncodedSnapshot<'io>> {
        let condition = if let Some((sequence, commit_id)) = self.anchor {
            CommitCondition::Head {
                path,
                expected: sequence.checked_add(1).ok_or_else(|| {
                    crate::executor::stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Storage,
                        "epoch snapshot cursor overflows",
                    )
                })?,
                commit_id,
            }
        } else {
            CommitCondition::Absent { path }
        };
        let wire = RecordWire::V2(SnapshotWire {
            epoch: &self.epoch,
            generation: self.state.generation(),
            attempt: self.attempt,
            pending: self.state.pending().map(|pending| PendingWire {
                summary: pending.summary,
                generation: pending.generation,
                through: pending.through,
                completion: self.outcome.as_ref(),
            }),
        });
        let record = io.encode(&wire, acyclic_stream::MAX_RECORD_BYTES as u64, false)?;
        let hold = io.guard_bytes(&record)?;
        Ok(EncodedSnapshot {
            condition,
            record,
            _hold: hold,
        })
    }
}
