use serde::{Deserialize, Serialize};
use std::fmt;

/// An SLO file in a volume. Its bytes use the explicitly versioned v1 format.
///
/// The owner reads this file through its ordinary authorized volume reader and
/// follows changes through that volume's existing event. No telemetry endpoint,
/// polling task, configuration registry or separate persistence is needed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Slo {
    /// Must be 1. Unsupported persisted formats are refused, never migrated.
    pub version: u32,
    /// The static operation span name, never a resource identifier.
    pub operation: String,
    /// Select a method when the operation uses a shared RPC span factory.
    pub rpc: Option<String>,
    /// Required successful completions per million observed operations.
    pub success_ppm: u32,
    /// Optional latency objective, evaluated from the same operation histogram.
    pub latency: Option<LatencySlo>,
}

/// A latency objective. A histogram bucket crossing the requested threshold
/// produces an honest unknown result when it can affect the verdict.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencySlo {
    /// Inclusive upper bound on the operation's latency, in nanoseconds.
    pub maximum_ns: u64,
    /// Required samples meeting the bound, per million observed operations.
    pub target_ppm: u32,
}

/// Live evaluation of an SLO file. An absent verdict means insufficient evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Verdict {
    /// Availability objective; no samples is unknown.
    pub success: Option<bool>,
    /// Latency objective; absent objective, no samples or ambiguous bucket is unknown.
    pub latency: Option<bool>,
}

/// Malformed or unsupported SLO file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SloError {
    /// The persisted format is not v1.
    Version,
    /// The file does not name an SDK operation.
    Operation,
    /// A fraction is outside [0, 1].
    Fraction,
    /// The query belongs to a different operation than the SLO file.
    WrongOperation,
}

impl fmt::Display for SloError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Version => "unsupported SLO file version",
            Self::Operation => "SLO file must name an acyclic operation",
            Self::Fraction => "SLO success fraction exceeds one million ppm",
            Self::WrongOperation => "SLO file and live query name different operations",
        })
    }
}
impl std::error::Error for SloError {}

impl Slo {
    /// Validate a decoded file before selecting its live query.
    pub fn validate(&self) -> Result<(), SloError> {
        if self.version != 1 {
            return Err(SloError::Version);
        }
        if !self.operation.starts_with("acyclic.")
            || self.operation == "acyclic."
            || self.operation.contains('\0')
            || self
                .rpc
                .as_ref()
                .is_some_and(|rpc| rpc.is_empty() || rpc.contains('\0'))
        {
            return Err(SloError::Operation);
        }
        if self.success_ppm > 1_000_000
            || self
                .latency
                .as_ref()
                .is_some_and(|latency| latency.target_ppm > 1_000_000)
        {
            return Err(SloError::Fraction);
        }
        Ok(())
    }

    /// Evaluate this file against its selected live query. No samples means
    /// unknown, never a successful empty cohort. Incomplete operations count
    /// against availability without claiming that the underlying work failed.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn evaluate(&self, snapshot: &crate::Snapshot) -> Result<Verdict, SloError> {
        self.validate()?;
        if self.operation != snapshot.operation || self.rpc != snapshot.rpc {
            return Err(SloError::WrongOperation);
        }
        let total =
            u128::from(snapshot.ok) + u128::from(snapshot.err) + u128::from(snapshot.incomplete);
        let success = (total != 0)
            .then(|| u128::from(snapshot.ok) * 1_000_000 >= total * u128::from(self.success_ppm));
        let latency = self.latency.as_ref().and_then(|objective| {
            let (mut certain, mut possible) = (0_u128, 0_u128);
            for (bucket, count) in snapshot.latency_ns.iter().enumerate() {
                let low = if bucket == 0 {
                    0
                } else {
                    1_u128.checked_shl((bucket - 1).try_into().ok()?)?
                };
                let high = 1_u128
                    .checked_shl(bucket.try_into().ok()?)
                    .map_or(u128::MAX, |n| n - 1);
                if high <= u128::from(objective.maximum_ns) {
                    certain += u128::from(*count);
                }
                if low <= u128::from(objective.maximum_ns) {
                    possible += u128::from(*count);
                }
            }
            let target = total * u128::from(objective.target_ppm);
            if total == 0 {
                None
            } else if certain * 1_000_000 >= target {
                Some(true)
            } else if possible * 1_000_000 < target {
                Some(false)
            } else {
                None
            }
        });
        Ok(Verdict { success, latency })
    }
}
