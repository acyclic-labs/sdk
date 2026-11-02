//! File-driven paired populations. Admission is linear in the arms file; trial
//! enumeration and statistics retain no state proportional to the population.

use crate::agent::{AgentPath, IntentId};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, num::NonZeroU64};

#[cfg(feature = "filesystem")]
mod profiles;
pub mod stats;
#[cfg(all(test, feature = "filesystem"))]
mod tests;
#[cfg(feature = "filesystem")]
pub use profiles::Population;

/// Refusals occur before population execution or before a statistics update.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Malformed or unsupported arms file.
    #[error("admit arms: {0}; recovery: correct the version-1 arms file and admit it again")]
    Arms(&'static str),
    /// Original JSON decoding failure.
    #[error("decode arms: {0}; recovery: correct the JSON arms file and admit it again")]
    Json(#[from] serde_json::Error),
    /// A role does not select the declared cheap model and inherited context.
    #[error(
        "admit experiment roles: {0}; recovery: bind fork-context roles to the declared cheap models and admit again"
    )]
    Role(String),
    /// Undefined or nonfinite statistics.
    #[error(
        "compute paired statistics: {0}; recovery: supply finite paired observations from independent clusters"
    )]
    Statistics(&'static str),
    /// Checked counter exhaustion.
    #[error(
        "count experiment observations: overflow; recovery: split the analysis into independent reports"
    )]
    Overflow,
}

/// The entire population selects this saved turn, including project generations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cut {
    /// Existing source agent, with a committed history descriptor.
    pub agent: AgentPath,
    /// Zero-based settled turn, as in the ordinary history fork command.
    pub turn: u64,
}

/// Experiment models are an allow-list, without an expensive fallback.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CheapModel {
    /// OpenAI Luna.
    #[serde(rename = "openai/gpt-6-luna")]
    Luna,
    /// Anthropic Haiku.
    #[serde(rename = "anthropic/claude-haiku-5-5")]
    Haiku,
    /// Google Flash.
    #[serde(rename = "google/gemini-3.8-flash")]
    Flash,
}

impl CheapModel {
    /// Canonical role model key.
    pub const fn key(self) -> &'static str {
        match self {
            Self::Luna => "openai/gpt-6-luna",
            Self::Haiku => "anthropic/claude-haiku-5-5",
            Self::Flash => "google/gemini-3.8-flash",
        }
    }
}

/// Topologies implemented by the live population path. Other topology recipes
/// are admitted when the shared topology owner can bind them before execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Topology {
    /// Ordinary inherited-context forks, supervised by the source agent.
    ForkJoin,
}

/// One treatment; the first arm is the baseline for paired comparisons.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Arm {
    /// Unique identity within the arms file.
    pub name: String,
    /// Shared topology mechanism to bind.
    pub topology: Topology,
    /// Admitted inherited-context role, including its model selection.
    pub role: String,
    /// Must equal the role's actual model; never a model hint in a prompt.
    pub model: CheapModel,
    /// New brief. It is staged by the ordinary fork owner.
    pub prompt: String,
}

/// Version-one JSON file stored in a volume. No population is allocated here.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArmsFile {
    /// Exactly 1; incompatible versions are refused.
    pub version: u32,
    /// Caller-chosen experiment identity.
    pub experiment: String,
    /// Common immutable saved turn for every arm and replicate.
    pub cut: Cut,
    /// Explicit user reservation; there is no implicit population limit.
    pub replicates: NonZeroU64,
    /// Baseline followed by treatments.
    pub arms: Vec<Arm>,
}

impl ArmsFile {
    /// Decode and validate before any fork or provider call.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let file: Self = serde_json::from_slice(bytes)?;
        file.validate()?;
        Ok(file)
    }

    /// Validate caller-built values at the same admission boundary.
    pub fn validate(&self) -> Result<(), Error> {
        if self.version != 1 {
            return Err(Error::Arms("unsupported version"));
        }
        if self.experiment.trim().is_empty() || self.arms.len() < 2 {
            return Err(Error::Arms(
                "an identity and baseline plus treatment are required",
            ));
        }
        self.cut.turn.checked_add(1).ok_or(Error::Overflow)?;
        let mut names = HashSet::new();
        for arm in &self.arms {
            if arm.name.trim().is_empty()
                || arm.role.trim().is_empty()
                || arm.prompt.trim().is_empty()
                || !names.insert(&arm.name)
            {
                return Err(Error::Arms("arm names must be unique and fields nonempty"));
            }
        }
        Ok(())
    }
}

/// One borrowed trial; advancing to it takes constant work and no allocation.
#[derive(Clone, Copy, Debug)]
pub struct Trial<'a> {
    /// The admitted, immutable experiment file.
    pub file: &'a ArmsFile,
    /// Exact treatment selection.
    pub arm: &'a Arm,
    /// Zero-based paired replicate.
    pub replicate: u64,
    pub(crate) arm_index: usize,
    pub(crate) digest: &'a blake3::Hash,
}

impl Trial<'_> {
    /// Fixed-size intent, bound to all admitted configuration and role pins.
    /// Changing a file creates different children rather than reusing old work.
    pub fn intent(&self) -> Result<IntentId, crate::agent::Error> {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.experiments.trial.v1");
        hash.update(self.digest.as_bytes());
        hash.update(&self.replicate.to_le_bytes());
        hash.update(&(self.arm_index as u64).to_le_bytes());
        IntentId::try_from(hash.finalize().to_hex().as_str())
    }
}

/// Measured score and paid cost, joined by the caller to a completed trial.
/// Missing cost remains missing; it must never be presented as zero spend.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    /// Result row version, exactly 1.
    pub version: u32,
    /// Experiment file and role snapshot digest.
    pub population: String,
    /// Zero-based replicate shared with its baseline.
    pub replicate: u64,
    /// Arm name.
    pub arm: String,
    /// Scorer's measured outcome.
    pub score: f64,
    /// Actual provider cost in USD, or unknown.
    pub cost_usd: Option<f64>,
}

impl Observation {
    /// Treatment minus baseline. Pair identity and finiteness are checked
    /// before the result can enter a cluster accumulator.
    pub fn difference(&self, baseline: &Self) -> Result<f64, Error> {
        if self.version != 1
            || baseline.version != 1
            || self.population != baseline.population
            || self.replicate != baseline.replicate
            || self.arm == baseline.arm
        {
            return Err(Error::Statistics(
                "observations are not a matched treatment pair",
            ));
        }
        for row in [self, baseline] {
            if !row.score.is_finite()
                || row
                    .cost_usd
                    .is_some_and(|cost| !cost.is_finite() || cost < 0.0)
            {
                return Err(Error::Statistics("score or cost is invalid"));
            }
        }
        let difference = self.score - baseline.score;
        if !difference.is_finite() {
            return Err(Error::Statistics("paired difference overflow"));
        }
        Ok(difference)
    }
}
