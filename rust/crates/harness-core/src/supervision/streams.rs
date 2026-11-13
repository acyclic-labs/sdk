//! Source-cut contributions and live signal files on the shared Streams index.
//!
//! The actor retains the fixed-size [`State`]. Each signature count is the tail
//! of a digest-addressed stream; no resident map or replay is needed. Signal
//! records are indexed by their public file name in the same event stream any
//! client follows. Reading a signal file is one LATEST lookup, never a copy of
//! authority in a separately maintained volume.

use super::{
    Cell, Event, Kind, Lifecycle, Observation, Policy, Position, Signal, State, Transition,
};
use crate::{Error, OperationId, Result, agent::AgentPath};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitEffect, CommitMutation, StreamError, StreamPath,
    StreamProvider, StreamProviderExt as _,
};
use bytes::Bytes;

mod checkpoint;
pub use checkpoint::{Admission, Checkpoint, Projection};

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::{NativeSignalFile, SignalFileRead, native_signal_file};

#[cfg(test)]
mod tests;

fn invalid(error: impl std::fmt::Display) -> Error {
    Error::Invalid(error.to_string())
}

/// One agent's own event stream, excluded from ordinary child inheritance.
pub fn path(agent: &AgentPath) -> Result<StreamPath> {
    StreamPath::new(format!("{agent}/supervision")).map_err(Error::from)
}

/// Point-select the current original run admission with its publication fence.
/// The caller joins this fence with any action on that run, including resume.
pub async fn active_run<P: StreamProvider + ?Sized>(
    provider: &P,
    agent: &AgentPath,
) -> Result<(Option<OperationId>, CommitCondition)> {
    let (record, condition) = provider
        .latest_fenced(
            path(agent)?,
            Some(Bytes::from_static(super::RUN_KEY).into()),
            Default::default(),
        )
        .await?;
    Ok((decode_run(record, agent)?, condition))
}

const PAUSED_RUN_KEY: &[u8] = b"paused-run";
pub(crate) const CURRENT_RUN_KEY: &[u8] = b"current-run";

/// Retain the run whose original lifecycle publication closed admission.
/// Owner file events have no run and never replace this source identity.
pub(crate) fn lifecycle_record(event: &Event) -> Result<AppendRecord> {
    let bytes = serde_json::to_vec(event).map_err(invalid)?;
    let mut keys = Vec::new();
    if event.run.is_some() {
        keys.push(Bytes::from_static(CURRENT_RUN_KEY).into());
        if matches!(event.event, Lifecycle::Paused { .. }) {
            keys.push(Bytes::from_static(PAUSED_RUN_KEY).into());
        }
    }
    Ok(AppendRecord {
        value: bytes.into(),
        headers: acyclic_stream::RecordHeaders::new(keys, Vec::new())?,
    })
}

/// Run owning the most recent run-bearing lifecycle or signal at this cut.
/// An independent start takes ownership; a continuing run's original signal,
/// pause or completion takes it back without inventing another Started event.
pub(crate) async fn current_run_before<P: StreamProvider + ?Sized>(
    provider: &P,
    agent: &AgentPath,
    before: u64,
    at_least: acyclic_stream::RevisionCut,
) -> Result<Option<OperationId>> {
    let record = match provider
        .latest_before(
            path(agent)?,
            Some(Bytes::from_static(CURRENT_RUN_KEY).into()),
            before,
            at_least,
        )
        .await
    {
        Ok(record) => record,
        Err(StreamError::NotFound) => None,
        Err(error) => return Err(error.into()),
    };
    record
        .map(|record| {
            let event: Event = serde_json::from_slice(&record.value).map_err(invalid)?;
            current_run_from(&event, agent).map_err(invalid)
        })
        .transpose()
}

fn current_run_from(
    event: &Event,
    agent: &AgentPath,
) -> std::result::Result<OperationId, &'static str> {
    if event.version != 1 || event.agent != *agent {
        return Err("invalid indexed current run");
    }
    event.run.ok_or("indexed current activity has no run")
}

fn signal_from<'a>(
    event: &'a Event,
    agent: &AgentPath,
    run: OperationId,
    kind: Kind,
) -> std::result::Result<&'a Signal, &'static str> {
    let Lifecycle::Signal { signal, .. } = &event.event else {
        return Err("indexed supervision source is not a signal");
    };
    if event.version != 1
        || event.agent != *agent
        || event.run != Some(run)
        || signal.version != 1
        || signal.kind != kind
    {
        return Err("invalid supervision signal source");
    }
    Ok(signal)
}

/// Point-select the run owning the latest pause, with its atomic action fence.
pub(crate) async fn paused_run<P: StreamProvider + ?Sized>(
    provider: &P,
    agent: &AgentPath,
) -> Result<(Option<OperationId>, CommitCondition)> {
    let (record, fence) = provider
        .latest_fenced(
            path(agent)?,
            Some(Bytes::from_static(PAUSED_RUN_KEY).into()),
            Default::default(),
        )
        .await?;
    let run = record
        .map(|record| -> Result<_> {
            let event: Event = serde_json::from_slice(&record.value).map_err(invalid)?;
            if event.version != 1
                || event.agent != *agent
                || !matches!(event.event, Lifecycle::Paused { .. })
            {
                return Err(invalid("invalid indexed pause owner"));
            }
            event
                .run
                .ok_or_else(|| invalid("indexed pause has no run identity"))
        })
        .transpose()?;
    Ok((run, fence))
}

fn decode_run(
    record: Option<acyclic_stream::Record>,
    agent: &AgentPath,
) -> Result<Option<OperationId>> {
    record
        .map(|record| -> Result<_> {
            let event: Event = serde_json::from_slice(&record.value).map_err(invalid)?;
            if event.version != 1
                || event.agent != *agent
                || !matches!(event.event, Lifecycle::Started { .. })
            {
                return Err(invalid("invalid indexed run admission"));
            }
            event
                .run
                .ok_or_else(|| invalid("indexed run has no identity"))
        })
        .transpose()
}

/// Select the originally admitted run at a previously captured exclusive cut.
/// Used by pure views; no new head observation or publication is performed.
pub async fn active_run_before<P: StreamProvider + ?Sized>(
    provider: &P,
    agent: &AgentPath,
    before: u64,
    at_least: acyclic_stream::RevisionCut,
) -> Result<Option<OperationId>> {
    let record = match provider
        .latest_before(
            path(agent)?,
            Some(Bytes::from_static(super::RUN_KEY).into()),
            before,
            at_least,
        )
        .await
    {
        Ok(record) => record,
        Err(StreamError::NotFound) => None,
        Err(error) => return Err(error.into()),
    };
    decode_run(record, agent)
}

fn cell_path(agent: &AgentPath, run: OperationId, key: &[u8; 32]) -> Result<StreamPath> {
    StreamPath::new(format!(
        "{agent}/supervision/runs/{run}/cells/{}",
        hex::encode(key)
    ))
    .map_err(Error::from)
}

fn signal_key_text(
    run: OperationId,
    kind: Kind,
) -> arrayvec::ArrayString<{ 5 + 36 + 19 + 15 + 5 }> {
    use std::fmt::Write as _;
    // Fixed schema: prefix, canonical UUID, namespace, longest Kind, suffix.
    // No user-provided component or variable-size history occupies this buffer.
    let mut text = arrayvec::ArrayString::new();
    write!(text, "runs/{run}/status/supervision/{}.json", kind.name())
        .expect("supervision key fits its fixed schema");
    text
}

fn signal_key(run: OperationId, kind: Kind) -> Bytes {
    Bytes::copy_from_slice(signal_key_text(run, kind).as_bytes())
}

async fn tail<P: StreamProvider + ?Sized>(provider: &P, path: StreamPath) -> Result<Option<u64>> {
    match provider.tail(path).await {
        Ok(tail) => Ok(Some(tail)),
        Err(StreamError::NotFound) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn condition(path: StreamPath, tail: Option<u64>) -> CommitCondition {
    match tail {
        Some(expected) => CommitCondition::Tail { path, expected },
        None => CommitCondition::Absent { path },
    }
}

/// Candidate query result and its original conditional stream contribution.
/// The source producer MUST publish this hard effect with the checkpoint and
/// original source event in its existing atomic commit. A rejected commit must
/// discard all three; an exact replay must reuse that original commit identity.
/// This preparation never independently submits or retries an operation.
pub struct Prepared {
    /// Candidate query checkpoint, signals and policy action.
    pub transition: Transition,
    /// Point-counter and indexed signal publications; absent on a quiet cut.
    pub effect: Option<CommitEffect>,
}

/// Original model usage attributable to one accepted inbox turn. Unknown
/// totals remain unknown even when a reported input or output proves spending.
pub struct TurnUsage {
    /// Original inbox cursor, shared by every segment of this turn.
    pub turn: u64,
    /// Complete input plus output token total, if reported.
    pub tokens: Option<u64>,
    /// Original pinned-price cost in billionths of a US dollar, if known.
    pub usd_nanos: Option<u64>,
    /// At least one original reported token counter is positive.
    pub spent_tokens: bool,
}

fn spent_key(turn: u64) -> [u8; 32] {
    super::signature(b"spent-turn", &[&turn.to_be_bytes()])
}

fn progress_key(turn: u64) -> [u8; 32] {
    super::signature(b"progress-turn", &[&turn.to_be_bytes()])
}

fn unmeasured_key(turn: u64) -> [u8; 32] {
    super::signature(b"unmeasured-progress-turn", &[&turn.to_be_bytes()])
}

fn pending_key(turn: u64) -> [u8; 32] {
    super::signature(b"pending-native-turn", &[&turn.to_be_bytes()])
}

fn capture_key(turn: u64, call: OperationId) -> [u8; 32] {
    super::signature(
        b"native-capture",
        &[&turn.to_be_bytes(), &call.into_bytes()],
    )
}

/// An original call's measurement phase. Completion retires only that call's
/// pending contribution; unavailable completed evidence remains unmeasured.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeMeasurement {
    /// Physical execution has not published its original content comparison.
    Pending,
    /// Original content changed, was unchanged, or could not be measured.
    Complete(Option<bool>),
}

impl NativeMeasurement {
    const fn code(self) -> u64 {
        match self {
            Self::Pending => 1,
            Self::Complete(Some(false)) => 2,
            Self::Complete(Some(true)) => 3,
            Self::Complete(None) => 4,
        }
    }
}

fn occurrence_bytes(sequence: u64) -> Vec<u8> {
    let mut bytes = 1u32.to_be_bytes().to_vec();
    bytes.extend_from_slice(&sequence.to_be_bytes());
    bytes
}

/// A run's lazy query binding. Construction performs no I/O or background work.
pub struct Queries<'a, P: ?Sized> {
    provider: &'a P,
    agent: &'a AgentPath,
    run: OperationId,
}

impl<'a, P: StreamProvider + ?Sized> Queries<'a, P> {
    /// Bind the already authorized provider, agent and original run identity.
    pub const fn new(provider: &'a P, agent: &'a AgentPath, run: OperationId) -> Self {
        Self {
            provider,
            agent,
            run,
        }
    }

    /// Point-select whether this exact turn has original reported spending.
    /// The returned fence MUST join the turn's original settlement decision;
    /// another turn's later usage cannot change the selected answer.
    pub async fn spent_in_turn(&self, turn: u64) -> Result<(bool, CommitCondition)> {
        self.turn_fact(spent_key(turn)).await
    }

    /// Point-select original progress in this turn. Derived output and model
    /// claims do not publish this fact. Its fence joins original settlement.
    pub async fn progress_in_turn(&self, turn: u64) -> Result<(bool, CommitCondition)> {
        self.turn_fact(progress_key(turn)).await
    }

    async fn turn_fact(&self, key: [u8; 32]) -> Result<(bool, CommitCondition)> {
        let (record, fence) = self
            .provider
            .latest_fenced(
                cell_path(self.agent, self.run, &key)?,
                None,
                Default::default(),
            )
            .await?;
        if let Some(record) = &record
            && (record.sequence != 0
                || record.value.len() != 12
                || record.value[..4] != 1u32.to_be_bytes())
        {
            return Err(invalid("turn fact differs from its original cut"));
        }
        Ok((record.is_some(), fence))
    }

    async fn join_turn_fact(&self, projection: &mut Projection, key: [u8; 32]) -> Result<()> {
        let (present, fence) = self.turn_fact(key).await?;
        if !present {
            projection.effect.mutations.push(CommitMutation::Append {
                path: fence.path().clone(),
                records: vec![occurrence_bytes(projection.checkpoint.state.through - 1).into()],
            });
            projection.effect.conditions.push(fence);
        }
        Ok(())
    }

    async fn indexed_number(&self, key: [u8; 32]) -> Result<(Option<u64>, CommitCondition)> {
        let (record, fence) = self
            .provider
            .latest_fenced(
                cell_path(self.agent, self.run, &key)?,
                None,
                Default::default(),
            )
            .await?;
        let value = record
            .map(|record| {
                if record.value.len() != 12 || record.value[..4] != 1u32.to_be_bytes() {
                    return Err(invalid(
                        "native measurement index has an invalid version or length",
                    ));
                }
                Ok(u64::from_be_bytes(
                    record.value[4..].try_into().map_err(invalid)?,
                ))
            })
            .transpose()?;
        Ok((value, fence))
    }

    fn write_number(projection: &mut Projection, fence: CommitCondition, value: u64) {
        projection.effect.mutations.push(CommitMutation::Append {
            path: fence.path().clone(),
            records: vec![occurrence_bytes(value).into()],
        });
        projection.effect.conditions.push(fence);
    }

    /// Project usage and, when no-progress observation is enabled, its first
    /// positive per-turn fact. Both belong to the usage producer's one atomic
    /// publication. This performs no independent submission or history scan.
    pub async fn project_usage(
        &self,
        source: &Position,
        usage: &TurnUsage,
        policy: &Policy,
    ) -> Result<Projection> {
        let mut projection = self
            .project(
                source,
                &Observation::Usage {
                    tokens: usage.tokens,
                    usd_nanos: usage.usd_nanos,
                },
                policy,
            )
            .await?;
        if policy.no_progress_turns.is_some() && usage.spent_tokens {
            self.join_turn_fact(&mut projection, spent_key(usage.turn))
                .await?;
        }
        Ok(projection)
    }

    /// The original publication producer joins this contribution to its event.
    /// Only the first positive fact writes a marker; no turn history is read.
    pub async fn project_progress(
        &self,
        source: &Position,
        turn: u64,
        policy: &Policy,
    ) -> Result<Projection> {
        let mut projection = self.project(source, &Observation::Progress, policy).await?;
        if policy.no_progress_turns.is_some() {
            self.join_turn_fact(&mut projection, progress_key(turn))
                .await?;
        }
        Ok(projection)
    }

    /// An original native capture contributes either a positive byte-change
    /// fact, a known unchanged measurement, or an explicit coverage gap.
    /// A coverage gap never writes a positive progress marker.
    pub async fn project_native_capture(
        &self,
        source: &Position,
        turn: u64,
        call: OperationId,
        measurement: NativeMeasurement,
        policy: &Policy,
    ) -> Result<Projection> {
        let mut projection = self.project(source, &Observation::Progress, policy).await?;
        if policy.no_progress_turns.is_some() {
            let (previous, call_fence) = self.indexed_number(capture_key(turn, call)).await?;
            if previous.is_some_and(|value| !(1..=4).contains(&value)) {
                return Err(invalid("native measurement index has an invalid phase"));
            }
            let current = measurement.code();
            if previous == Some(current) || previous.is_some_and(|value| value != 1) {
                // A delayed start cannot downgrade an accepted completion, and
                // exact replay cannot consume another call's pending credit.
                if current != 1 && previous != Some(current) {
                    return Err(invalid(
                        "native measurement changed its original terminal verdict",
                    ));
                }
                return Ok(projection);
            }
            Self::write_number(&mut projection, call_fence, current);
            if previous == Some(1) || current == 1 {
                let (pending, fence) = self.indexed_number(pending_key(turn)).await?;
                let pending = pending.unwrap_or(0);
                let pending = if current == 1 {
                    pending.checked_add(1)
                } else {
                    pending.checked_sub(1)
                }
                .ok_or_else(|| invalid("native pending measurement count overflow or underflow"))?;
                Self::write_number(&mut projection, fence, pending);
            }
            let key = match measurement {
                NativeMeasurement::Complete(Some(true)) => Some(progress_key(turn)),
                NativeMeasurement::Complete(None) => Some(unmeasured_key(turn)),
                _ => None,
            };
            if let Some(key) = key {
                self.join_turn_fact(&mut projection, key).await?;
            }
        }
        Ok(projection)
    }

    /// Join original progress receipts and this exact turn's reported spending
    /// to its terminal publication. The original settlement producer owns the
    /// returned effect; a concurrent usage publication refuses that same commit
    /// rather than allowing a settlement to miss spending. Disabled policies do
    /// no turn lookup and produce no query contribution.
    pub async fn project_settlement(
        &self,
        source: &Position,
        turn: u64,
        policy: &Policy,
    ) -> Result<Option<Projection>> {
        if policy.no_progress_turns.is_none() {
            return Ok(None);
        }
        let (
            (spent_tokens, spending_fence),
            (progress, progress_fence),
            (unmeasured, measurement_fence),
            (pending, pending_fence),
        ) = futures::try_join!(
            self.spent_in_turn(turn),
            self.progress_in_turn(turn),
            self.turn_fact(unmeasured_key(turn)),
            self.indexed_number(pending_key(turn)),
        )?;
        let mut projection = self
            .project(
                source,
                &Observation::TurnDone {
                    spent_tokens,
                    progress,
                    progress_measured: !unmeasured && pending.unwrap_or(0) == 0,
                },
                policy,
            )
            .await?;
        projection.effect.conditions.extend([
            spending_fence,
            progress_fence,
            measurement_fence,
            pending_fence,
        ]);
        Ok(Some(projection))
    }

    /// Prepare one incremental query at its original source cut. Reads one
    /// signature tail when needed, one distinct-turn marker for model output,
    /// and the signal stream's tail only when emitting
    /// a signal. Each read buys an optimistic fence for the one composed commit.
    pub async fn prepare(
        &self,
        state: &State,
        sequence: u64,
        source: &Position,
        observation: &Observation<'_>,
        policy: &Policy,
    ) -> Result<Prepared> {
        let Self {
            provider,
            agent,
            run,
        } = *self;
        // Refuse gaps/versions before I/O, and replay without even reading a cell.
        if sequence != state.through || state.version != 2 || policy.validate_queries().is_err() {
            return Ok(Prepared {
                transition: state
                    .apply(sequence, observation, None, policy)
                    .map_err(invalid)?,
                effect: None,
            });
        }
        // All calls in one original turn share this marker. Its conditional
        // publication and the output count are indivisible, so repeated calls
        // and concurrent producers cannot turn one turn into multiple votes.
        let occurrence = if let Some(key) = observation.output_occurrence() {
            let path = cell_path(agent, run, &key)?;
            let through = tail(provider, path.clone()).await?;
            Some((path, through))
        } else {
            None
        };
        let observation = if occurrence
            .as_ref()
            .is_some_and(|(_, through)| through.is_some_and(|through| through > 0))
        {
            observation.without_output()
        } else {
            *observation
        };
        let selected = if let Some(key) = observation.key() {
            let path = cell_path(agent, run, &key)?;
            let tail = tail(provider, path.clone()).await?;
            Some((
                path,
                tail,
                Cell {
                    key,
                    count: tail.unwrap_or(0),
                },
            ))
        } else {
            None
        };
        let mut transition = state
            .apply(
                sequence,
                &observation,
                selected.as_ref().map(|(_, _, cell)| *cell),
                policy,
            )
            .map_err(invalid)?;
        // The fold ordinal orders selected observations; a signal identifies
        // its actual original source position, which can be sparse.
        for signal in &mut transition.signals {
            signal.sequence = source.sequence;
        }
        let mut conditions = Vec::new();
        let mut mutations = Vec::new();
        if let Some((path, through)) = occurrence
            && observation.key().is_some()
        {
            conditions.push(condition(path.clone(), through));
            mutations.push(CommitMutation::Append {
                path,
                records: vec![occurrence_bytes(sequence).into()],
            });
        }
        if let Some((path, tail, _)) = selected {
            conditions.push(condition(path.clone(), tail));
            // Version-one occurrence record: big-endian u32 version followed by
            // the source-cut sequence. The indexed tail is the occurrence count.
            mutations.push(CommitMutation::Append {
                path,
                records: vec![occurrence_bytes(sequence).into()],
            });
        }
        if !transition.signals.is_empty() {
            let path = path(agent)?;
            conditions.push(condition(path.clone(), tail(provider, path.clone()).await?));
            let records = transition
                .signals
                .iter()
                .map(|signal| {
                    // Attention preserves every occurrence on follow, while LATEST
                    // serves the current file from this very same original record.
                    let event = Event {
                        version: 1,
                        agent: agent.clone(),
                        run: Some(run),
                        source: source.clone(),
                        event: Lifecycle::Signal {
                            signal: signal.clone(),
                            action: policy.action(signal),
                        },
                    };
                    Ok(AppendRecord {
                        value: serde_json::to_vec(&event).map_err(invalid)?.into(),
                        headers: acyclic_stream::RecordHeaders::new(
                            vec![
                                Bytes::from_static(CURRENT_RUN_KEY).into(),
                                signal_key(run, signal.kind).into(),
                            ],
                            vec![acyclic_stream::Attribute {
                                name: "attention".into(),
                                value: Bytes::from_static(b"true").into(),
                            }],
                        )?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            mutations.push(CommitMutation::Append { path, records });
        }
        let effect = (!mutations.is_empty()).then(|| CommitEffect::hard(conditions, mutations));
        Ok(Prepared { transition, effect })
    }

    /// Project `status/supervision/<kind>.json` from its one indexed source record.
    /// The caller supplies the same authorized provider as ordinary private files.
    /// An absent signal is an absent file; it never becomes an all-clear record.
    pub async fn read_signal(&self, kind: Kind) -> Result<Option<Signal>> {
        self.read_signal_source(kind, u64::MAX, Default::default())
            .await
            .map(|value| value.map(|(signal, _)| signal))
    }

    /// The same pure point query at a captured cut, retaining its immutable
    /// original input for file provenance and cancellation-safe following.
    pub async fn read_signal_source(
        &self,
        kind: Kind,
        before: u64,
        at_least: acyclic_stream::RevisionCut,
    ) -> Result<Option<(Signal, Position)>> {
        let Self {
            provider,
            agent,
            run,
        } = *self;
        let record = match provider
            .latest_before(
                path(agent)?,
                Some(signal_key(run, kind).into()),
                before,
                at_least,
            )
            .await
        {
            Ok(record) => record,
            Err(StreamError::NotFound) => None,
            Err(error) => return Err(error.into()),
        };
        record
            .map(|record| {
                let event: Event = serde_json::from_slice(&record.value).map_err(invalid)?;
                let signal = signal_from(&event, agent, run, kind)
                    .map_err(invalid)?
                    .clone();
                Ok((
                    signal,
                    Position {
                        stream: path(agent)?,
                        sequence: record.sequence,
                    },
                ))
            })
            .transpose()
    }
}
