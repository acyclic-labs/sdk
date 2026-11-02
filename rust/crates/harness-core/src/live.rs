//! File records projected from the original committed agent and model sources.
//!
//! These types do not issue revisions, aggregate a subtree, reserve budget or
//! publish records. The original producer supplies those facts in its admitting
//! commit. A reader never substitutes a clock, a request digest or zero for a
//! missing fact. Public file values are versionless; the original source keeps
//! its own versioned encoding. No extra live record is published.

use crate::executor::{ModelPriceError, ModelPriceRecord, ModelUsageRecord};
use crate::model::ModelDispatch;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;

/// A projection cannot supply the requested original fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// Stored source layout is unsupported.
    #[error(
        "live source version is unsupported; recovery: use the original producer's current record format"
    )]
    Version,
    /// Usage and admission must name the same actual dispatch.
    #[error(
        "live usage differs from its context admission; recovery: supply the admission for this dispatch"
    )]
    CallMismatch,
    /// An admission is not a completed model call.
    #[error(
        "live usage has no committed model terminal; recovery: read the original completed call"
    )]
    IncompleteCall,
    /// Required provider counters were not reported.
    #[error(
        "live usage counters are incomplete; recovery: retain the original source error or use a provider reporting these counters"
    )]
    IncompleteCounters,
    /// No USD estimate can be established from the selected original price.
    #[error(
        "live usage USD cost is unknown; recovery: supply a matching USD price and complete billing counters"
    )]
    IncompleteCost,
    /// Capacity, thresholds or counters cannot describe a context admission.
    #[error(
        "live context admission is invalid; recovery: supply the original verified capacity, count and revision"
    )]
    Admission,
    /// The existing shared price projection rejected the supplied binding.
    #[error(transparent)]
    Price(#[from] ModelPriceError),
}

/// Fixed public phase vocabulary; no per-step status events are implied.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentState {
    /// Running a turn.
    Working,
    /// Waiting outside the team.
    Waiting,
    /// Unable to continue without intervention.
    Blocked,
    /// Explicitly paused.
    Paused,
    /// No current work.
    Idle,
    /// Completed successfully.
    Done,
    /// Completed with failure.
    Failed,
    /// Archived by the original lifecycle owner.
    Archived,
}

/// Counts retained by the original aggregate, never reconstructed by a scan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateCounts {
    /// Working agents.
    pub working: u64,
    /// Waiting agents.
    pub waiting: u64,
    /// Blocked agents.
    pub blocked: u64,
    /// Paused agents.
    pub paused: u64,
    /// Idle agents.
    pub idle: u64,
    /// Successful agents.
    pub done: u64,
    /// Failed agents.
    pub failed: u64,
    /// Archived agents.
    pub archived: u64,
}

/// `status.json`; the original phase-change commit supplies every field.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Status {
    /// Current published phase.
    pub state: AgentState,
    /// Original agent-supplied summary.
    pub summary: String,
    /// Original child aggregate at the publication cut.
    pub children: StateCounts,
    /// Own model-call spending and escrow limit, in USD.
    pub budget: StatusBudget,
    /// Original publication time, never a reader's current time.
    pub updated: String,
}

/// Own-call spending; a fork starts with its own original spending ledger.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusBudget {
    /// Settled USD spending.
    pub spent: f64,
    /// Admitted USD limit; null shares the parent's budget.
    pub limit: Option<f64>,
}

/// `counters.json`, supplied by the original subtree aggregate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Counters {
    /// Counts by phase at one original cut.
    pub state: StateCounts,
    /// Counts by original declared kind; not an enumeration of descendants.
    pub kind: BTreeMap<String, u64>,
}

/// `budget.json`, supplied by the original escrow owner in USD.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    /// Admitted escrow; null shares the parent's budget.
    pub escrow: Option<f64>,
    /// Settled spending.
    pub spent: f64,
    /// Accepted work whose original reservation has not settled.
    pub reserved: f64,
}

/// Facts retained by the original verified context admission. This carrier is
/// not authority, a new revision issuer or a diagnostic funding permit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ContextAdmission {
    /// The actual dispatched request, including operation and step.
    pub dispatch: ModelDispatch,
    /// Original context replacement revision, not a hash or journal position.
    pub revision: u64,
    /// Original verified model context capacity.
    pub capacity: u64,
    /// Original plugin notification fraction; null disables notifications.
    pub notify_at: Option<f64>,
    /// Original kernel backstop threshold.
    pub auto_trim_at_tokens: u64,
    /// Original verified request token count.
    pub total_tokens: u64,
    /// Public paged parts path backed by the same original context cut.
    pub parts: String,
}

impl ContextAdmission {
    fn validate(&self) -> Result<(), Error> {
        if self.dispatch.request_digest == [0; 32]
            || self.capacity == 0
            || self.total_tokens > self.capacity
            || self.auto_trim_at_tokens > self.capacity
            || self.parts.is_empty()
            || self
                .notify_at
                .is_some_and(|fraction| !fraction.is_finite() || !(0.0..=1.0).contains(&fraction))
        {
            return Err(Error::Admission);
        }
        Ok(())
    }

    fn completed(&self, usage: &ModelUsageRecord) -> Result<(), Error> {
        self.validate()?;
        if self.dispatch != usage.dispatch {
            return Err(Error::CallMismatch);
        }
        if usage.version != ModelUsageRecord::VERSION {
            return Err(Error::Version);
        }
        if !usage.completed {
            return Err(Error::IncompleteCall);
        }
        Ok(())
    }
}

/// `context.json`; each cache count stays paired with its own request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ContextInfo<'a> {
    /// Original replacement revision.
    pub revision: u64,
    /// Selected model capacity.
    pub capacity: u64,
    /// Configured notification fraction.
    pub notify_at: Option<f64>,
    /// Kernel backstop threshold.
    pub auto_trim_at_tokens: u64,
    /// Count for the current verified request.
    pub current: ContextRequest,
    /// Counts for the original last completed request, possibly an older revision.
    pub last_call: ContextCall,
    /// Paged context part files, borrowed while projecting and owned on decode.
    pub parts: Cow<'a, str>,
}

/// A verified request and its own count.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ContextRequest {
    /// Hex encoding of the actual request digest; never used as a revision.
    pub request: String,
    /// Original verified input count.
    pub total_tokens: u64,
}

/// Cache observations for one particular completed request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ContextCall {
    /// Actual request digest.
    pub request: String,
    /// Original verified count for this request, not today's request.
    pub total_tokens: u64,
    /// Original cache-read count; absent remains unknown.
    pub cached_read_tokens: Option<u64>,
    /// Original cache-write count; absent remains unknown.
    pub cache_write_tokens: Option<u64>,
}

impl<'a> ContextInfo<'a> {
    /// Join the current admission with an independently retained last call.
    /// There is no provider call, historical lookup or revision derivation.
    pub fn project(
        current: &'a ContextAdmission,
        last: &ContextAdmission,
        usage: &ModelUsageRecord,
    ) -> Result<Self, Error> {
        current.validate()?;
        last.completed(usage)?;
        if last.revision > current.revision {
            return Err(Error::Admission);
        }
        Ok(Self {
            revision: current.revision,
            capacity: current.capacity,
            notify_at: current.notify_at,
            auto_trim_at_tokens: current.auto_trim_at_tokens,
            current: ContextRequest {
                request: request_id(&current.dispatch),
                total_tokens: current.total_tokens,
            },
            last_call: ContextCall {
                request: request_id(&last.dispatch),
                total_tokens: last.total_tokens,
                cached_read_tokens: usage.tokens.cached,
                cache_write_tokens: usage.tokens.cache_write,
            },
            parts: Cow::Borrowed(&current.parts),
        })
    }
}

fn request_id(dispatch: &ModelDispatch) -> String {
    blake3::Hash::from_bytes(dispatch.request_digest)
        .to_hex()
        .to_string()
}

/// One `context/parts/<id>.json`; the original tree supplies ordered paging.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextPartInfo {
    /// Stable original part identifier.
    pub id: String,
    /// Whole dependency-group category.
    pub kind: ContextPartKind,
    /// Original verified part weight.
    pub tokens: u64,
    /// Original replaced range for a summary; no inferred coverage.
    pub covers: Option<[String; 2]>,
}

/// Parts are retained or evicted whole by the original context owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContextPartKind {
    /// Frozen instructions.
    Instructions,
    /// An exchange and its dependent tool calls.
    Exchange,
    /// An authenticated summary of its covered range.
    Summary,
    /// Explicitly pinned input.
    Pinned,
}

/// `usage.jsonl`, projected only from an original committed terminal and its
/// matching admission. Missing required counters/cost produce a typed error.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Usage<'a> {
    /// Revision at this call, even if the current context has since changed.
    pub revision: u64,
    /// Original admitted model, borrowed while projecting and owned on decode.
    pub model: Cow<'a, str>,
    /// Provider's original input counter, including its reported cache hits.
    pub input_tokens: u64,
    /// Provider's original cache-read counter.
    pub cached_tokens: u64,
    /// Provider's original output counter.
    pub output_tokens: u64,
    /// Capacity admitted for this call.
    pub capacity: u64,
    /// Selected original token-price estimate, in USD.
    pub cost: f64,
    /// Unknown until the original renderer supplies the counterfactual costs.
    pub compaction: CompactionCost,
}

/// Counterfactual USD costs require the original renderer and selected prices.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CompactionCost {
    /// One summary and resend; null without the original rendered witness.
    pub summarise_now: Option<f64>,
    /// One additional call without compaction; null without its witness.
    pub per_call_if_not: Option<f64>,
    /// Original kernel backstop threshold.
    pub auto_trim_at_tokens: u64,
}

impl<'a> Usage<'a> {
    /// Reuse the shared provider-price interpretation. This projection does not
    /// publish a second usage event or turn unknown cache/cost into zero.
    pub fn project(
        admission: &ContextAdmission,
        usage: &'a ModelUsageRecord,
        price: Option<&ModelPriceRecord>,
    ) -> Result<Self, Error> {
        admission.completed(usage)?;
        let tokens = usage.tokens;
        let price = price.ok_or(Error::IncompleteCost)?;
        let (projection, input_tokens) = usage.estimate_with_input(Some(price))?;
        let (Some(input_tokens), Some(cached_tokens), Some(output_tokens)) =
            (input_tokens, tokens.cached, tokens.output)
        else {
            return Err(Error::IncompleteCounters);
        };
        let estimate = projection.estimated_cost;
        if estimate.currency.as_deref() != Some("USD") {
            return Err(Error::IncompleteCost);
        }
        let cost = estimate.token_nano_units.ok_or(Error::IncompleteCost)?;
        Ok(Self {
            revision: admission.revision,
            model: Cow::Borrowed(&usage.model),
            input_tokens,
            cached_tokens,
            output_tokens,
            capacity: admission.capacity,
            cost: usd(cost),
            compaction: CompactionCost {
                summarise_now: None,
                per_call_if_not: None,
                auto_trim_at_tokens: admission.auto_trim_at_tokens,
            },
        })
    }
}

fn usd(nanos: u64) -> f64 {
    let [a, b, c, d, e, f, g, h] = nanos.to_be_bytes();
    (f64::from(u32::from_be_bytes([a, b, c, d])) * 4_294_967_296.0
        + f64::from(u32::from_be_bytes([e, f, g, h])))
        / 1_000_000_000.0
}

/// Original cache, placement, sharing and usage facts for research live files.
#[cfg(feature = "research")]
pub mod resources;
