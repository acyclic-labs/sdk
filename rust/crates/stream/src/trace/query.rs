//! One pull-based projection for finite reads and live follows alike.
use super::{Mechanism, Outcome, SpanEvent, SpanId, TraceRecord};
use crate::{Record, RetainedRecord, StreamError};
use futures::{Stream, StreamExt as _};
use std::collections::{BTreeMap, btree_map::Entry};

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// Original start context, retained only until its terminal delta arrives.
#[derive(Debug, serde::Serialize)]
pub struct ActiveSpan {
    /// Original start publication, also usable after a failed run.
    pub evidence: RetainedRecord,
    /// Causal parent, whose clock domain may differ.
    pub parent: Option<SpanId>,
    /// Stable operation name.
    pub name: String,
    /// Mechanism doing the work.
    pub mechanism: Mechanism,
    /// Timestamp in this span's producer domain.
    pub at_ns: u64,
    /// Explicit public guarantee, absent when this hop is unaccounted for.
    pub guarantee: Option<String>,
}

/// Inclusive elapsed time, never wall-clock utilization or exclusive parent time.
/// Counts and sums cover only starts and finishes both observed by this query.
#[derive(Debug, Default, serde::Serialize)]
pub struct MechanismTotals {
    /// Number of paired terminal observations.
    pub completed: u128,
    /// Sum of same-domain elapsed nanoseconds; concurrent spans can overlap.
    pub elapsed_ns: u128,
    /// Sum of explicitly reported bytes; missing counts are not zero estimates.
    pub known_bytes: u128,
    /// Terminal deltas that did not report their byte count.
    pub unknown_bytes: u128,
    /// Failed paired spans; causes remain in their original stream records.
    pub failed: u128,
    /// Paired spans that observed cancellation.
    pub cancelled: u128,
    /// Paired spans that returned or dropped without observing settlement.
    pub unsettled: u128,
    /// Count above the caller's mechanism threshold, unknown without a policy.
    pub outliers: Option<u128>,
    /// Largest observed same-domain duration and its original start publication.
    pub longest: Option<(u64, RetainedRecord)>,
}

/// One new query observation, using the caller's explicit outlier policy.
#[derive(Debug)]
pub enum Observation {
    /// A start was admitted; its context is available from `active`.
    Started {
        /// Original producer identity.
        id: SpanId,
        /// This start names no nonempty public guarantee.
        missing_guarantee: bool,
    },
    /// A terminal delta joined its original start.
    Completed {
        /// Joined producer identity.
        id: SpanId,
        /// Original start context.
        start: ActiveSpan,
        /// Original terminal publication.
        finish: RetainedRecord,
        /// Difference of timestamps in this identity's single clock domain.
        elapsed_ns: u64,
        /// Whether elapsed time exceeds this mechanism's configured threshold;
        /// unknown when the caller supplied no threshold for this mechanism.
        outlier: Option<bool>,
        /// Reported terminal byte count, without estimation.
        bytes: Option<u64>,
        /// Original terminal outcome and any failure evidence.
        result: Outcome,
    },
    /// The selected read window did not include this span's start. No duration
    /// or mechanism is fabricated; a failure still retains its original cause.
    UnpairedFinish {
        /// Original producer identity.
        id: SpanId,
        /// Original terminal publication outside paired coverage.
        evidence: RetainedRecord,
        /// Original outcome, whose failure reference remains usable.
        result: Outcome,
    },
}

/// Invalid causal evidence, distinct from an unfinished or partially read span.
#[derive(Debug, thiserror::Error)]
pub enum QueryViolation {
    /// Two starts claim the same active identity.
    #[error("span identity already has an active start")]
    RepeatedStart,
    /// A terminal observation precedes its own same-domain start.
    #[error("span finish precedes its original start")]
    ReversedTime,
    /// The input cursor replayed or reordered a record.
    #[error("trace record sequence did not advance")]
    ReorderedRecord,
}

/// Original source or decode failure, or a violated invariant tied to evidence.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// Original storage, authorization or session failure.
    #[error(transparent)]
    Stream(#[from] StreamError),
    /// Original decoder error; the immutable publication remains inspectable.
    #[error("malformed trace record at {evidence:?}: {source}")]
    Decode {
        /// Immutable malformed publication.
        evidence: RetainedRecord,
        /// Original JSON decoder failure.
        #[source]
        source: serde_json::Error,
    },
    /// Original publication violates the span contract.
    #[error("invalid trace record at {evidence:?}: {source}")]
    Invalid {
        /// Immutable publication violating the contract.
        evidence: RetainedRecord,
        /// Specific violated invariant.
        #[source]
        source: QueryViolation,
    },
}

/// Lazy projection of one selected trace stream or window. Construct this over
/// an ordinary paged read or live follow; no collector, task or storage is added.
/// Memory retains active spans and one fixed-size aggregate per mechanism,
/// never completed history. Missing starts make incomplete coverage explicit.
pub struct TraceQuery<S> {
    records: S,
    active: BTreeMap<SpanId, ActiveSpan>,
    totals: BTreeMap<Mechanism, MechanismTotals>,
    thresholds_ns: BTreeMap<Mechanism, u64>,
    sequence: Option<u64>,
    active_waits: usize,
    missing_guarantees: u128,
    unpaired_finishes: u128,
}

impl<S> TraceQuery<S> {
    /// Select exactly the records the caller already authorized and opened.
    /// No history is read until `next` is requested.
    /// Outlier thresholds are caller policy, not implicit SDK constants. An
    /// absent mechanism threshold preserves unknown classification.
    pub fn new(records: S, thresholds_ns: BTreeMap<Mechanism, u64>) -> Self {
        Self {
            records,
            active: BTreeMap::new(),
            totals: BTreeMap::new(),
            thresholds_ns,
            sequence: None,
            active_waits: 0,
            missing_guarantees: 0,
            unpaired_finishes: 0,
        }
    }

    /// Active spans in this selected window; absence of a finish is not failure
    /// or proof of a stuck agent. Iteration is lazy and may be paged by the view.
    pub fn active(&self) -> impl Iterator<Item = (&SpanId, &ActiveSpan)> {
        self.active.iter()
    }

    /// Fixed-cardinality mechanism aggregates, borrowed without scanning history.
    pub fn totals(&self) -> &BTreeMap<Mechanism, MechanismTotals> {
        &self.totals
    }
    /// Starts still waiting for any terminal observation.
    pub fn active_waits(&self) -> usize {
        self.active_waits
    }
    /// Observed starts naming no nonempty guarantee.
    pub fn missing_guarantees(&self) -> u128 {
        self.missing_guarantees
    }
    /// Finishes whose starts are outside the selected coverage.
    pub fn unpaired_finishes(&self) -> u128 {
        self.unpaired_finishes
    }
}

impl<S: Stream<Item = Result<Record, StreamError>> + Unpin> TraceQuery<S> {
    /// Consume one original delta, or wait on the source's existing event. On
    /// malformed input the bad record is consumed but projection state is not
    /// changed; no successful settlement is synthesized.
    pub async fn next(&mut self) -> Result<Option<Observation>, QueryError> {
        let Some(record) = self.records.next().await else {
            return Ok(None);
        };
        let record = record?;
        let evidence = RetainedRecord {
            commit_id: record.commit_id,
            path: record.origin,
            sequence: record.sequence,
        };
        if self.sequence.is_some_and(|prior| record.sequence <= prior) {
            return Err(QueryError::Invalid {
                evidence,
                source: QueryViolation::ReorderedRecord,
            });
        }
        let TraceRecord::V1(event) =
            serde_json::from_slice(record.value.as_ref()).map_err(|source| QueryError::Decode {
                evidence: evidence.clone(),
                source,
            })?;
        let observation = match event {
            SpanEvent::Started {
                id,
                parent,
                name,
                mechanism,
                at_ns,
                guarantee,
            } => {
                let Entry::Vacant(entry) = self.active.entry(id) else {
                    return Err(QueryError::Invalid {
                        evidence,
                        source: QueryViolation::RepeatedStart,
                    });
                };
                let missing_guarantee = guarantee
                    .as_deref()
                    .is_none_or(|value| value.trim().is_empty());
                entry.insert(ActiveSpan {
                    evidence,
                    parent,
                    name,
                    mechanism,
                    at_ns,
                    guarantee,
                });
                self.missing_guarantees += u128::from(missing_guarantee);
                self.active_waits += usize::from(mechanism == Mechanism::Wait);
                Observation::Started {
                    id,
                    missing_guarantee,
                }
            }
            SpanEvent::Finished {
                id,
                at_ns,
                bytes,
                result,
            } => {
                if let Some(start) = self.active.get(&id) {
                    let elapsed_ns =
                        at_ns
                            .checked_sub(start.at_ns)
                            .ok_or_else(|| QueryError::Invalid {
                                evidence: evidence.clone(),
                                source: QueryViolation::ReversedTime,
                            })?;
                    let start = self
                        .active
                        .remove(&id)
                        .expect("start was observed without yielding");
                    self.active_waits -= usize::from(start.mechanism == Mechanism::Wait);
                    let outlier = self
                        .thresholds_ns
                        .get(&start.mechanism)
                        .map(|threshold| elapsed_ns > *threshold);
                    let total = self.totals.entry(start.mechanism).or_default();
                    total.completed += 1;
                    total.elapsed_ns += u128::from(elapsed_ns);
                    if let Some(outlier) = outlier {
                        total.outliers = Some(total.outliers.unwrap_or(0) + u128::from(outlier));
                    }
                    match bytes {
                        Some(bytes) => total.known_bytes += u128::from(bytes),
                        None => total.unknown_bytes += 1,
                    }
                    match &result {
                        Outcome::Failed { .. } => total.failed += 1,
                        Outcome::Cancelled => total.cancelled += 1,
                        Outcome::Unsettled => total.unsettled += 1,
                        Outcome::Succeeded => {}
                    }
                    if total
                        .longest
                        .as_ref()
                        .is_none_or(|(duration, _)| elapsed_ns > *duration)
                    {
                        total.longest = Some((elapsed_ns, start.evidence.clone()));
                    }
                    Observation::Completed {
                        id,
                        start,
                        finish: evidence,
                        elapsed_ns,
                        outlier,
                        bytes,
                        result,
                    }
                } else {
                    self.unpaired_finishes += 1;
                    Observation::UnpairedFinish {
                        id,
                        evidence,
                        result,
                    }
                }
            }
        };
        self.sequence = Some(record.sequence);
        Ok(Some(observation))
    }
}
