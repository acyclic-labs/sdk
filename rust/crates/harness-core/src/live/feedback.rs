//! Feedback authored by an agent; provenance belongs to the admitting harness.
use serde::{Deserialize, Serialize};

/// The public vocabulary for productivity feedback.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
    crate::HeapLayout,
)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum Kind {
    /// A tool or harness misbehaved.
    Defect,
    /// Time spent waiting on the system.
    Slow,
    /// An ambiguous result or surface.
    Unclear,
    /// Something prevented progress.
    Limit,
    /// Another source of lost productivity.
    Other,
}

/// The self-scoped file whose writes append ordinary execution observations.
pub const PATH: &str = "live/agents/self/feedback.jsonl";

/// Agent estimates, not measured usage or billing facts.
#[derive(
    Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
pub struct Cost {
    /// Estimated wall seconds lost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seconds: Option<f64>,
    /// Estimated tokens lost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens: Option<u64>,
}

/// One model-authored line. Unknown fields are refused, including provenance.
#[derive(
    Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
pub struct Report {
    /// Persisted schema version, currently one.
    pub version: u32,
    /// What kind of obstacle occurred.
    pub kind: Kind,
    /// One line describing the obstacle.
    pub what: String,
    /// Command, path, error text or step reference supporting the report.
    pub evidence: String,
    /// Optional estimate of productivity lost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<Cost>,
    /// Optional proposed improvement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
}

impl crate::filesystem::PreparationRecord for Report {}

impl Report {
    /// Validate before publication. No text length or record count policy is
    /// imposed here; the stream's protective frame bound remains authoritative.
    pub fn validate(&self) -> crate::Result<()> {
        if self.version != 1 {
            return Err(crate::Error::Invalid(
                "feedback requires schema version 1".into(),
            ));
        }
        if self.what.trim().is_empty() || self.what.contains(['\r', '\n']) {
            return Err(crate::Error::Invalid(
                "feedback what requires one nonempty line".into(),
            ));
        }
        if self.evidence.trim().is_empty() {
            return Err(crate::Error::Invalid("feedback requires evidence".into()));
        }
        if let Some(cost) = &self.cost
            && ((cost.seconds.is_none() && cost.tokens.is_none())
                || cost
                    .seconds
                    .is_some_and(|seconds| !seconds.is_finite() || seconds < 0.0))
        {
            return Err(crate::Error::Invalid(
                "feedback cost requires nonnegative seconds or tokens".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authored_reports_cannot_supply_provenance_or_invalid_estimates() -> crate::Result<()> {
        let value = serde_json::json!({
            "version": 1, "kind": "slow", "what": "Waited for a result",
            "evidence": "shell step 3", "cost": { "seconds": 1.5 }
        });
        let report: Report = serde_json::from_value(value.clone()).unwrap();
        report.validate()?;
        for key in ["agent", "step", "time_unix_ms"] {
            let mut forged = value.clone();
            forged[key] = serde_json::json!("invented");
            assert!(serde_json::from_value::<Report>(forged).is_err());
        }
        for seconds in [-1.0, f64::INFINITY, f64::NAN] {
            let mut invalid = report.clone();
            invalid.cost = Some(Cost {
                seconds: Some(seconds),
                tokens: None,
            });
            assert!(invalid.validate().is_err());
        }
        for what in ["", "two\nlines", "two\rlines"] {
            let mut invalid = report.clone();
            invalid.what = what.into();
            assert!(invalid.validate().is_err());
        }
        Ok(())
    }
}
