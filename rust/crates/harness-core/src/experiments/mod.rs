//! Offline experiment observations and analysis, independent of execution.
//! Each source problem/cut is one cluster, however many agents it contains.

use serde::{Deserialize, Serialize};

pub mod probes;
pub mod reports;
pub mod stats;

/// Refusals leave analysis accumulators unchanged.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An invalid pair, statistic, trial cost or planning input.
    #[error(
        "analyze experiment observations: {0}; recovery: correct inputs and repeat the failed call to Observation::new, Observation::decode, Observation::difference, Observation::cost_difference, AssignedTrials::push/report, Cluster::push, PairedStats::push/estimate or required_clusters"
    )]
    Statistics(&'static str),
    /// Original observation decoder failure.
    #[error(
        "decode observation: {0}; recovery: correct the version-2 JSON object and call Observation::decode"
    )]
    Json(#[from] serde_json::Error),
    /// Invalid scoring contrast or decision version.
    #[error(
        "admit probe decision: {0}; recovery: supply version-1 files with distinct expected/injected values and call Scorer::decode or Decision::decode"
    )]
    Probe(&'static str),
    /// Original probe-file decoding failure.
    #[error(
        "decode probe file: {0}; recovery: correct the JSON object and call Scorer::decode or Decision::decode"
    )]
    ProbeJson(serde_json::Error),
    /// Counter or power-planning result cannot be represented.
    #[error(
        "count experiment observations: overflow; recovery: split independent input groups before calling AssignedTrials::push, Cluster::push or PairedStats::push, or correct required_clusters inputs"
    )]
    Overflow,
}

/// Persisted analysis input, decoded only through the public admission boundary.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationFile {
    version: u32,
    population: [u8; 32],
    replicate: u64,
    arm: u64,
    score: Option<f64>,
    cost_usd: Option<f64>,
}

/// An admitted trial row joined offline to original output and accounting.
/// The fixed-width population identity binds one comparison and common cut;
/// arm numbers refer to its declared arm table. Score and complete trial cost
/// are independently optional; missing values remain unknown.
#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct Observation(ObservationFile);

impl Observation {
    /// Admit one complete version-2 JSON object before numerical analysis.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Self::admit(crate::executor::json::decode_object(bytes)?)
    }

    /// Admit a caller-built row without serializing its identity.
    pub fn new(
        population: [u8; 32],
        replicate: u64,
        arm: u64,
        score: Option<f64>,
        cost_usd: Option<f64>,
    ) -> Result<Self, Error> {
        Self::admit(ObservationFile {
            version: 2,
            population,
            replicate,
            arm,
            score,
            cost_usd,
        })
    }

    /// Score treatment minus baseline, or unknown if either score is missing.
    /// Pair identity is checked even for missing scores; admission is not repeated.
    pub fn difference(&self, baseline: &Self) -> Result<Option<f64>, Error> {
        self.check_pair(baseline)?;
        let (Some(score), Some(baseline)) = (self.0.score, baseline.0.score) else {
            return Ok(None);
        };
        let difference = score - baseline;
        if !difference.is_finite() {
            return Err(Error::Statistics("paired difference overflow"));
        }
        Ok(Some(difference))
    }

    /// Complete cost treatment minus baseline, independently of score validity.
    /// Unknown if either cost is missing. Admitted costs are finite/nonnegative,
    /// so their difference is finite without another numerical check.
    pub fn cost_difference(&self, baseline: &Self) -> Result<Option<f64>, Error> {
        self.check_pair(baseline)?;
        Ok(self
            .0
            .cost_usd
            .zip(baseline.0.cost_usd)
            .map(|(cost, baseline)| cost - baseline))
    }

    fn check_pair(&self, baseline: &Self) -> Result<(), Error> {
        let (row, baseline) = (&self.0, &baseline.0);
        if row.population != baseline.population
            || row.replicate != baseline.replicate
            || row.arm == baseline.arm
        {
            return Err(Error::Statistics("observations are not a matched pair"));
        }
        Ok(())
    }

    fn admit(row: ObservationFile) -> Result<Self, Error> {
        if row.version != 2
            || row.score.is_some_and(|score| !score.is_finite())
            || row
                .cost_usd
                .is_some_and(|cost| !cost.is_finite() || cost < 0.0)
        {
            return Err(Error::Statistics(
                "invalid observation version, score or cost",
            ));
        }
        Ok(Self(row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_rows_and_mismatched_pairs_never_reach_statistics()
    -> Result<(), Box<dyn std::error::Error>> {
        // Authored analysis inputs, not captured provider output.
        let baseline = Observation::new([7; 32], 0, 0, Some(1.0), None)?;
        let treatment = Observation::new([7; 32], 0, 1, Some(3.0), Some(0.01))?;
        let encoded = serde_json::to_vec(&treatment)?;
        assert_eq!(
            Observation::decode(&encoded)?.difference(&baseline)?,
            Some(2.0)
        );
        for row in [
            Observation::new([7; 32], 1, 1, None, None)?,
            Observation::new([8; 32], 0, 1, None, None)?,
            Observation::new([7; 32], 0, 0, None, None)?,
        ] {
            assert!(row.difference(&baseline).is_err());
            assert!(row.cost_difference(&baseline).is_err());
        }
        assert!(Observation::new([7; 32], 0, 1, Some(f64::INFINITY), None).is_err());
        for cost in [-1.0, f64::INFINITY, f64::NAN] {
            assert!(Observation::new([7; 32], 0, 1, None, Some(cost)).is_err());
        }
        let high = Observation::new([7; 32], 0, 1, Some(f64::MAX), None)?;
        let low = Observation::new([7; 32], 0, 0, Some(-f64::MAX), None)?;
        assert!(high.difference(&low).is_err());
        let mut invalid = serde_json::to_value(&baseline)?;
        invalid["version"] = 1.into();
        assert!(Observation::decode(&serde_json::to_vec(&invalid)?).is_err());
        let object = serde_json::to_string(&baseline)?;
        let duplicate = format!("{{\"version\":2,{}", &object[1..]);
        assert!(Observation::decode(duplicate.as_bytes()).is_err());
        assert!(Observation::decode(format!("{object} null").as_bytes()).is_err());
        let row = serde_json::to_value(&baseline)?;
        let sequence = serde_json::json!([
            row["version"],
            row["population"],
            row["replicate"],
            row["arm"],
            row["score"],
            row["cost_usd"]
        ]);
        assert!(Observation::decode(&serde_json::to_vec(&sequence)?).is_err());
        Ok(())
    }

    #[test]
    fn paired_cost_retains_unscored_trials_without_imputing_unknown_values()
    -> Result<(), Box<dyn std::error::Error>> {
        // Authored analysis inputs, not captured provider output.
        let baseline = Observation::new([7; 32], 0, 0, None, Some(1.0))?;
        let treatment = Observation::new([7; 32], 0, 1, None, Some(3.0))?;
        let treatment = Observation::decode(&serde_json::to_vec(&treatment)?)?;
        assert_eq!(treatment.difference(&baseline)?, None);
        assert_eq!(treatment.cost_difference(&baseline)?, Some(2.0));
        assert_eq!(baseline.cost_difference(&treatment)?, Some(-2.0));

        let known = Observation::new([7; 32], 0, 0, Some(0.0), Some(0.0))?;
        let unscored = Observation::new([7; 32], 0, 1, None, Some(0.0))?;
        assert_eq!(unscored.difference(&known)?, None);
        assert_eq!(known.difference(&unscored)?, None);
        assert_eq!(unscored.cost_difference(&known)?, Some(0.0));
        let unpriced = Observation::new([7; 32], 0, 1, Some(0.0), None)?;
        assert_eq!(unpriced.difference(&known)?, Some(0.0));
        assert_eq!(unpriced.cost_difference(&known)?, None);
        assert_eq!(known.cost_difference(&unpriced)?, None);
        let unknown = Observation::new([7; 32], 0, 0, None, None)?;
        assert_eq!(unpriced.cost_difference(&unknown)?, None);

        let maximum = Observation::new([7; 32], 0, 1, None, Some(f64::MAX))?;
        assert_eq!(maximum.cost_difference(&known)?, Some(f64::MAX));
        assert_eq!(known.cost_difference(&maximum)?, Some(-f64::MAX));
        Ok(())
    }
}

#[cfg(kani)]
mod proofs {
    #[kani::proof]
    fn admitted_cost_difference_is_finite_even_without_scores() {
        let baseline: f64 = kani::any();
        let treatment: f64 = kani::any();
        kani::assume(baseline.is_finite() && baseline >= 0.0);
        kani::assume(treatment.is_finite() && treatment >= 0.0);
        let baseline = super::Observation::new([0; 32], 0, 0, None, Some(baseline)).unwrap();
        let treatment = super::Observation::new([0; 32], 0, 1, None, Some(treatment)).unwrap();
        let difference = treatment.cost_difference(&baseline).unwrap().unwrap();
        assert!(difference.is_finite());
        assert_eq!(treatment.difference(&baseline).unwrap(), None);
    }
}
