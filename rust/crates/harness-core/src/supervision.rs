//! Incremental supervision queries over an agent's original stream subtree.
//!
//! A query reads one signature cell, an output's distinct-turn marker when
//! needed, and a fixed-size checkpoint. The
//! caller publishes the returned cell, checkpoint, signals and policy action
//! atomically at the source cut. Signature cells live in a durable keyed index,
//! not in an agent-resident map. No history scan, clock or task is owned here.
//! The quiet-tool actor owns alarm scheduling. Its delivery supplies the
//! original tool's latest activity and settlement from its source index.

use arrayvec::ArrayVec;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

mod admission;
mod events;
pub(crate) mod files;
pub(crate) mod tool_jobs;
pub use admission::{RunAdmission, RunAdmissionSource};
pub mod streams;
pub use events::{Event, Lifecycle, Position};
/// Exact index key of the current explicit owner admission decision.
pub const OWNER_CONTROL_KEY: &[u8] = b"owner-control";
/// Exact index key of the latest originally admitted run.
pub const RUN_KEY: &[u8] = b"run";
#[cfg(test)]
mod tests;

/// Stable file name under `status/supervision/`, with `.json` appended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// An identical normalized invocation failed with the identical error.
    RepeatFailure,
    /// One check was admitted at least three times in one turn.
    ValidationLoop,
    /// Consecutive spending turns have no original progress receipt.
    NoProgress,
    /// An unsettled tool has exceeded its configured output quiet period.
    Stuck,
    /// The same nonempty model output was settled in another turn.
    LoopOutput,
    /// A configured token or cost reservation is exhausted or unmeasured.
    Spend,
}

impl Kind {
    /// Public file/event spelling.
    pub const fn name(self) -> &'static str {
        match self {
            Self::RepeatFailure => "repeat_failure",
            Self::ValidationLoop => "validation_loop",
            Self::NoProgress => "no_progress",
            Self::Stuck => "stuck",
            Self::LoopOutput => "loop_output",
            Self::Spend => "spend",
        }
    }
}

/// Policy actions, ordered by strength when several queries fire at one cut.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// Publish the signal and keep running.
    #[default]
    Alert,
    /// Park execution until the owner resumes it.
    Pause,
    /// Cancel execution and close the run.
    Stop,
}

/// Original owner decision returned at an execution boundary. Alert-only
/// observations do not interrupt the executor and carry no control decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Control {
    /// Original publication that established this decision.
    pub source: Position,
    /// Pause or stop, selected by the original policy or control publication.
    pub action: Action,
    /// Original signal, absent for an explicit control-file decision.
    pub signal: Option<Kind>,
}

/// User-owned capacity. Missing dimensions are truly unbounded.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reservation {
    /// Total reported input plus output tokens, without double-counting cached input.
    pub tokens: Option<u64>,
    /// Cost in millionths of a US dollar from the original pinned model price.
    pub usd_micros: Option<u64>,
}

/// Occurrence thresholds, configured by the owner. Null disables the respective
/// query or repeat-failure escalation; there is no hidden fallback threshold.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Thresholds {
    /// Identical normalized tool failures per run.
    pub repeat_failure: Option<u64>,
    /// Matching failures at which the default action escalates to pause.
    pub repeat_failure_pause: Option<u64>,
    /// Admissions of the same validation command per turn.
    pub validation_loop: Option<u64>,
    /// Settled turns with the same nonempty model-output hash.
    pub loop_output: Option<u64>,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            repeat_failure: Some(2),
            repeat_failure_pause: Some(3),
            validation_loop: Some(3),
            loop_output: Some(2),
        }
    }
}

/// Version-one `control/supervision.json`. It is never inserted in model context.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// Must be one; other persisted formats are refused without migration.
    pub version: u32,
    /// Occurrence thresholds; omission selects the shared defaults.
    #[serde(default)]
    pub thresholds: Thresholds,
    /// Owner-selected shell checks, normalized once when the policy is admitted.
    /// An empty set selects no checks; no tool or build-system names are guessed.
    #[serde(default)]
    pub validation_commands: BTreeSet<String>,
    /// Consecutive spending turns without progress; null disables this query.
    pub no_progress_turns: Option<u64>,
    /// Tool output quiet period in milliseconds; null disables stuck detection.
    pub quiet_ms: Option<u64>,
    /// A run's capacity, configured by its owner.
    pub reservation: Reservation,
    /// Immutable owner-selected USD prices, indexed by exact provider then model.
    /// Missing selections remain unknown; no endpoint or price is inferred.
    #[serde(default)]
    pub prices: BTreeMap<String, BTreeMap<String, crate::executor::ModelPriceRecord>>,
    /// Explicit overrides. Absent entries use the shared defaults.
    pub actions: BTreeMap<Kind, Action>,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            version: 1,
            thresholds: Thresholds::default(),
            validation_commands: BTreeSet::new(),
            no_progress_turns: None,
            quiet_ms: None,
            reservation: Reservation::default(),
            prices: BTreeMap::new(),
            actions: BTreeMap::new(),
        }
    }
}

impl Policy {
    /// Validate and compile owner selections without changing their original
    /// file bytes or the publication that authenticates those bytes.
    pub fn admit(mut self) -> Result<Self, Error> {
        self.validate()?;
        self.validation_commands = self
            .validation_commands
            .into_iter()
            .map(|command| normalize_command(&command))
            .collect();
        Ok(self)
    }

    /// Validate before admitting a policy-file generation.
    pub fn validate(&self) -> Result<(), Error> {
        self.validate_queries()?;
        for (provider, models) in &self.prices {
            for (model, price) in models {
                if price.validate().is_err()
                    || price.currency != "USD"
                    || &price.provider != provider
                    || &price.model != model
                {
                    return Err(Error::Price);
                }
            }
        }
        Ok(())
    }

    // Catalog validation is an admission operation. Each source cut checks
    // only this fixed-size query header and its exact selected price record.
    fn validate_queries(&self) -> Result<(), Error> {
        if self.version != 1 {
            return Err(Error::Version);
        }
        if [
            self.no_progress_turns,
            self.quiet_ms,
            self.thresholds.repeat_failure,
            self.thresholds.repeat_failure_pause,
            self.thresholds.validation_loop,
            self.thresholds.loop_output,
        ]
        .contains(&Some(0))
        {
            return Err(Error::ZeroThreshold);
        }
        Ok(())
    }

    /// Shared default: alert on every signal, escalating the third identical
    /// failure, validation loops, repeated output and spending to pause.
    pub fn action(&self, signal: &Signal) -> Action {
        self.actions
            .get(&signal.kind)
            .copied()
            .unwrap_or_else(|| match signal.kind {
                Kind::RepeatFailure
                    if self
                        .thresholds
                        .repeat_failure_pause
                        .is_some_and(|threshold| signal.count >= threshold) =>
                {
                    Action::Pause
                }
                Kind::ValidationLoop | Kind::LoopOutput | Kind::Spend => Action::Pause,
                _ => Action::Alert,
            })
    }
}

/// Incomplete measurements remain incomplete; they never become a zero charge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Total {
    /// Sum of the measurements actually reported by the source.
    pub measured: u64,
    /// Every original measurement was reported.
    pub complete: bool,
}

impl Default for Total {
    fn default() -> Self {
        Self {
            measured: 0,
            complete: true,
        }
    }
}

impl Total {
    fn add(&mut self, amount: Option<u64>) -> Result<(), Error> {
        match amount {
            Some(amount) => {
                self.measured = self.measured.checked_add(amount).ok_or(Error::Overflow)?
            }
            None => self.complete = false,
        }
        Ok(())
    }

    fn exhausted(self, reservation: Option<u64>) -> bool {
        reservation.is_some_and(|limit| !self.complete || self.measured >= limit)
    }
}

/// Fixed-size fold checkpoint, independent of turns, tools and signature count.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    /// Must be two.
    pub version: u32,
    /// Exclusive processed source cursor. Replayed cuts emit no duplicate effects.
    pub through: u64,
    /// Consecutive turns spending tokens without progress.
    pub unproductive_turns: u64,
    /// Own-run input plus output token spending.
    pub tokens: Total,
    /// Own-run cost, in millionths of a US dollar.
    pub usd_micros: Total,
    /// Unrounded fractional microdollar cost, always less than 1000.
    pub usd_nano_remainder: u16,
}

impl Default for State {
    fn default() -> Self {
        Self {
            version: 2,
            through: 0,
            unproductive_turns: 0,
            tokens: Total::default(),
            usd_micros: Total::default(),
            usd_nano_remainder: 0,
        }
    }
}

/// Digest-addressed durable counter. The caller scopes the index to this run,
/// authenticates the lookup at the checkpoint's cut, and commits its replacement
/// with the checkpoint. A missing cell has count zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cell {
    /// Domain-separated signature, never a raw command, error or model output.
    pub key: [u8; 32],
    /// Original number of matching events.
    pub count: u64,
}

/// Facts selected lazily from the original execution, settlement and tool indexes.
/// Every delivery has one monotonically increasing source position in this run.
#[derive(Clone, Copy)]
pub enum Observation<'a> {
    /// Original check admission, before dispatch; `check` comes from the owner's
    /// validation-command selection, not a model's success/failure claim.
    ToolStarted {
        /// Turn identity, used to scope validation counts.
        turn: u64,
        /// Actual admitted shell command, before normalization.
        command: &'a str,
        /// This invocation is a validation check.
        check: bool,
    },
    /// Original terminal result; successful transport can still carry a failed
    /// shell exit. `error` is the canonical, sanitized tool error in that result.
    ToolFinished {
        /// Actual admitted shell source selected by the original tool index.
        command: &'a str,
        /// Canonical sanitized failure, including nonzero process exits.
        error: Option<&'a [u8]>,
    },
    /// An actor alarm delivered with the tool index at this same source cut.
    ToolAlarm {
        /// Original tool identity.
        call: [u8; 32],
        /// Deadline on the actual delivered alarm.
        deadline_ms: u64,
        /// Latest committed output/admission time from the original tool index.
        last_activity_ms: u64,
        /// False once the original call's terminal result committed.
        in_flight: bool,
        /// Actual actor alarm-delivery time.
        now_ms: u64,
    },
    /// One original model-call terminal. Never feed both an admission and its
    /// completion, or a replay and its original completion.
    Usage {
        /// Actual input plus output tokens; null means unreported.
        tokens: Option<u64>,
        /// Actual pinned-price cost in billionths of a US dollar; null is unknown.
        usd_nanos: Option<u64>,
    },
    /// Original semantic response, including tool-call-only responses. Provider
    /// identities and event chunking cannot change this output fingerprint.
    ModelOutput {
        /// Original turn cursor, shared by all model calls in that turn.
        turn: u64,
        /// Empty output is not counted as a repeated response.
        output: Option<[u8; 32]>,
    },
    /// An original producer accepted a progress publication. The source owner
    /// commits the positive per-turn fact with this observation.
    Progress,
    /// Original turn settlement. Model outputs are observed separately, once
    /// per original turn, through `ModelOutput`.
    TurnDone {
        /// Actual original token spending, not an estimate from text length.
        spent_tokens: bool,
        /// Original progress receipts for this turn only.
        progress: bool,
    },
    /// Re-evaluate capacity before dispatch or when a policy generation changes.
    PolicyChanged,
}

/// Only source fingerprints and counters are public; no command/error/model
/// content is duplicated into the supervision stream or derived signal files.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Evidence {
    /// Counter cell that proves the repeated signature.
    Signature {
        /// Original counter cell's domain-separated signature.
        key: [u8; 32],
    },
    /// Original consecutive-turn count and configured threshold.
    NoProgress {
        /// Owner-selected consecutive-turn threshold.
        threshold: u64,
    },
    /// Original tool and the deadline whose latest activity still matches.
    Quiet {
        /// Original tool identity.
        call: [u8; 32],
        /// Deadline still matching this tool's latest activity.
        deadline_ms: u64,
    },
    /// Reported spending at this source cut and the owner's capacity.
    Spend {
        /// Original token measurements accumulated through this cut.
        tokens: Total,
        /// Original cost measurements accumulated through this cut.
        usd_micros: Total,
        /// Owner-selected capacity in force at this cut.
        reservation: Reservation,
    },
}

/// Version-one `status/supervision/<kind>.json`, also published as the stream's
/// signal event. Its source sequence makes exact replay distinguishable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Signal {
    /// Must be one.
    pub version: u32,
    /// Actual source position, not wall-clock time.
    pub sequence: u64,
    /// Stable query identity and signal-file name.
    pub kind: Kind,
    /// Matching source occurrences; one for deadline/capacity signals.
    pub count: u64,
    /// Original facts establishing this signal.
    pub evidence: Evidence,
}

/// One source-cut transaction. The source cursor advances even when no signal
/// fires. At most loop-output and no-progress can fire on a single settlement.
pub struct Transition {
    /// Candidate fixed-size checkpoint.
    pub state: State,
    /// Candidate point-cell replacement, when this observation uses one.
    pub cell: Option<Cell>,
    /// Signals to publish in the same source-cut transaction.
    pub signals: ArrayVec<Signal, 2>,
    /// Strongest action of all signals at this cut.
    pub action: Action,
}

/// A source/cell/policy mismatch is refused before publishing any effects.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// Unsupported checkpoint, cursor or policy version.
    #[error("unsupported supervision format version")]
    Version,
    /// A configured observation threshold is zero.
    #[error("supervision threshold must be positive or null")]
    ZeroThreshold,
    /// A selected price has invalid provenance, binding or currency.
    #[error("supervision price must bind its exact provider and model in USD")]
    Price,
    /// A source delivery skipped unprocessed facts.
    #[error("supervision source cursor has a gap")]
    SourceGap,
    /// A point lookup selected another signature.
    #[error("supervision signature cell differs from this source query")]
    CellMismatch,
    /// A counter or deadline cannot be represented.
    #[error("supervision counter or actor deadline overflows")]
    Overflow,
}

fn signature(domain: &[u8], fields: &[&[u8]]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness:supervision:v1:");
    hash.update(domain);
    for field in fields {
        hash.update(&(field.len() as u64).to_be_bytes());
        hash.update(field);
    }
    *hash.finalize().as_bytes()
}

impl Observation<'_> {
    fn output_occurrence(&self) -> Option<[u8; 32]> {
        match self {
            Self::ModelOutput {
                turn,
                output: Some(output),
            } => Some(signature(b"output-turn", &[&turn.to_be_bytes(), output])),
            _ => None,
        }
    }

    fn without_output(self) -> Self {
        match self {
            Self::ModelOutput { turn, .. } => Self::ModelOutput { turn, output: None },
            other => other,
        }
    }

    /// Signature point lookup, or none. Validation cells are scoped to a turn;
    /// failure/output cells span this run and never span inherited history.
    pub fn key(&self) -> Option<[u8; 32]> {
        match self {
            Self::ToolStarted {
                turn,
                command,
                check: true,
                ..
            } => Some(signature(
                b"check",
                &[&turn.to_be_bytes(), normalize_command(command).as_bytes()],
            )),
            Self::ToolFinished {
                command,
                error: Some(error),
                ..
            } => Some(signature(
                b"failure",
                &[normalize_command(command).as_bytes(), error],
            )),
            Self::ModelOutput {
                output: Some(output),
                ..
            } => Some(signature(b"output", &[output])),
            _ => None,
        }
    }
}

impl State {
    /// Computes one transaction without changing this checkpoint or its index.
    /// The caller must commit the result conditionally on this exact source cut.
    /// Duplicate deliveries have no effects; gaps cannot silently hide spending.
    pub fn apply(
        &self,
        sequence: u64,
        observation: &Observation<'_>,
        cell: Option<Cell>,
        policy: &Policy,
    ) -> Result<Transition, Error> {
        policy.validate_queries()?;
        if self.version != 2 || self.usd_nano_remainder >= 1000 {
            return Err(Error::Version);
        }
        let mut next = Transition {
            state: self.clone(),
            cell: None,
            signals: ArrayVec::new(),
            action: Action::Alert,
        };
        if sequence < self.through {
            return Ok(next);
        }
        if sequence != self.through {
            return Err(Error::SourceGap);
        }
        next.state.through = sequence.checked_add(1).ok_or(Error::Overflow)?;
        let key = observation.key();
        if cell.is_some_and(|cell| Some(cell.key) != key) {
            return Err(Error::CellMismatch);
        }
        if let Some(key) = key {
            next.cell = Some(Cell {
                key,
                count: cell
                    .map_or(0, |cell| cell.count)
                    .checked_add(1)
                    .ok_or(Error::Overflow)?,
            });
        }
        next.observe(sequence, observation, policy)?;
        next.action = next
            .signals
            .iter()
            .map(|signal| policy.action(signal))
            .max()
            .unwrap_or_default();
        Ok(next)
    }
}

impl Transition {
    fn observe(
        &mut self,
        sequence: u64,
        observation: &Observation<'_>,
        policy: &Policy,
    ) -> Result<(), Error> {
        match observation {
            Observation::ToolStarted { .. } => {
                self.repeated(
                    Kind::ValidationLoop,
                    policy.thresholds.validation_loop,
                    sequence,
                );
            }
            Observation::ToolFinished { .. } => {
                self.repeated(
                    Kind::RepeatFailure,
                    policy.thresholds.repeat_failure,
                    sequence,
                );
            }
            Observation::ToolAlarm {
                call,
                deadline_ms,
                last_activity_ms,
                in_flight,
                now_ms,
            } => {
                if *in_flight
                    && *now_ms >= *deadline_ms
                    && policy
                        .quiet_ms
                        .and_then(|quiet| last_activity_ms.checked_add(quiet))
                        == Some(*deadline_ms)
                {
                    self.signal(
                        Kind::Stuck,
                        1,
                        sequence,
                        Evidence::Quiet {
                            call: *call,
                            deadline_ms: *deadline_ms,
                        },
                    );
                }
            }
            Observation::Usage { tokens, usd_nanos } => {
                self.state.tokens.add(*tokens)?;
                let cost = usd_nanos.map(|nanos| {
                    let fraction = (nanos % 1000) as u16 + self.state.usd_nano_remainder;
                    self.state.usd_nano_remainder = fraction % 1000;
                    nanos / 1000 + u64::from(fraction / 1000)
                });
                self.state.usd_micros.add(cost)?;
                self.spend(sequence, policy);
            }
            Observation::ModelOutput { .. } => {
                self.repeated(Kind::LoopOutput, policy.thresholds.loop_output, sequence);
            }
            Observation::TurnDone {
                spent_tokens,
                progress,
                ..
            } => {
                if *progress {
                    self.state.unproductive_turns = 0;
                } else if *spent_tokens {
                    self.state.unproductive_turns = self
                        .state
                        .unproductive_turns
                        .checked_add(1)
                        .ok_or(Error::Overflow)?;
                    if let Some(threshold) = policy.no_progress_turns
                        && self.state.unproductive_turns >= threshold
                    {
                        self.signal(
                            Kind::NoProgress,
                            self.state.unproductive_turns,
                            sequence,
                            Evidence::NoProgress { threshold },
                        );
                    }
                } else {
                    // No token spending breaks the consecutive spending-turn run.
                    self.state.unproductive_turns = 0;
                }
            }
            Observation::Progress => {}
            Observation::PolicyChanged => self.spend(sequence, policy),
        }
        Ok(())
    }
    fn signal(&mut self, kind: Kind, count: u64, sequence: u64, evidence: Evidence) {
        self.signals.push(Signal {
            version: 1,
            sequence,
            kind,
            count,
            evidence,
        });
    }

    fn repeated(&mut self, kind: Kind, threshold: Option<u64>, sequence: u64) {
        if let Some(Cell { key, count }) = self.cell
            && threshold.is_some_and(|threshold| count >= threshold)
        {
            self.signal(kind, count, sequence, Evidence::Signature { key });
        }
    }

    fn spend(&mut self, sequence: u64, policy: &Policy) {
        if self.state.tokens.exhausted(policy.reservation.tokens)
            || self
                .state
                .usd_micros
                .exhausted(policy.reservation.usd_micros)
        {
            self.signal(
                Kind::Spend,
                1,
                sequence,
                Evidence::Spend {
                    tokens: self.state.tokens,
                    usd_micros: self.state.usd_micros,
                    reservation: policy.reservation,
                },
            );
        }
    }
}

/// Collapse horizontal whitespace outside quotes, preserving escaped bytes and
/// quoted content. Multiline commands remain byte-exact: here-doc bodies and
/// PowerShell here-strings can make any whitespace significant. This deliberately
/// prefers a missed equivalence over identifying two different shell programs.
pub fn normalize_command(command: &str) -> String {
    if command.contains(['\n', '\r', '\\', '`']) {
        return command.to_owned();
    }
    let mut result = String::with_capacity(command.len());
    let mut quote = None;
    let mut space = false;
    let mut comment = false;
    for character in command.trim_matches([' ', '\t']).chars() {
        if comment {
            result.push(character);
            continue;
        }
        if quote.is_none() && matches!(character, ' ' | '\t') {
            space = true;
            continue;
        }
        if space {
            result.push(' ');
            space = false;
        }
        result.push(character);
        if Some(character) == quote {
            quote = None;
        } else if quote.is_none() && matches!(character, '\'' | '"') {
            quote = Some(character);
        } else if quote.is_none() && character == '#' {
            comment = true;
        }
    }
    result
}
