//! Offline experiment observations and analysis, independent of execution.
//! Each source problem/cut is one cluster, however many agents it contains.

use serde::{Deserialize, Serialize};

pub mod probes;
pub mod stats;

/// Refusals leave analysis accumulators unchanged.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An invalid pair, statistic or planning input.
    #[error(
        "analyze paired observations: {0}; recovery: correct finite paired inputs and call Observation::difference, Cluster::push, PairedStats::push, PairedStats::estimate or required_clusters again"
    )]
    Statistics(&'static str),
    /// Original scored-row decoder failure.
    #[error(
        "decode observation: {0}; recovery: correct the version-1 JSON object and call Observation::decode"
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
        "count experiment observations: overflow; recovery: split independent input groups before calling Cluster::push or PairedStats::push, or correct required_clusters inputs"
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
    score: f64,
    cost_usd: Option<f64>,
}

/// An admitted scored row joined offline to original output and accounting.
/// The fixed-width population identity binds one comparison and common cut;
/// arm numbers refer to its declared arm table. Missing cost remains unknown.
#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct Observation(ObservationFile);

impl Observation {
    /// Admit one complete version-1 JSON object before numerical analysis.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Self::admit(crate::executor::json::decode_object(bytes)?)
    }

    /// Admit a caller-built row without serializing its identity.
    pub fn new(
        population: [u8; 32],
        replicate: u64,
        arm: u64,
        score: f64,
        cost_usd: Option<f64>,
    ) -> Result<Self, Error> {
        Self::admit(ObservationFile {
            version: 1,
            population,
            replicate,
            arm,
            score,
            cost_usd,
        })
    }

    /// Treatment minus baseline, with exact population/replicate pairing.
    /// Fixed-width comparisons and one subtraction; admission is not repeated.
    pub fn difference(&self, baseline: &Self) -> Result<f64, Error> {
        let (row, baseline) = (&self.0, &baseline.0);
        if row.population != baseline.population
            || row.replicate != baseline.replicate
            || row.arm == baseline.arm
        {
            return Err(Error::Statistics("observations are not a matched pair"));
        }
        let difference = row.score - baseline.score;
        if !difference.is_finite() {
            return Err(Error::Statistics("paired difference overflow"));
        }
        Ok(difference)
    }

    fn admit(row: ObservationFile) -> Result<Self, Error> {
        if row.version != 1
            || !row.score.is_finite()
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
        let baseline = Observation::new([7; 32], 0, 0, 1.0, None)?;
        let treatment = Observation::new([7; 32], 0, 1, 3.0, Some(0.01))?;
        let encoded = serde_json::to_vec(&treatment)?;
        assert_eq!(Observation::decode(&encoded)?.difference(&baseline)?, 2.0);
        for row in [
            Observation::new([7; 32], 1, 1, 3.0, None)?,
            Observation::new([8; 32], 0, 1, 3.0, None)?,
            Observation::new([7; 32], 0, 0, 3.0, None)?,
        ] {
            assert!(row.difference(&baseline).is_err());
        }
        assert!(Observation::new([7; 32], 0, 1, f64::INFINITY, None).is_err());
        assert!(Observation::new([7; 32], 0, 1, 3.0, Some(-1.0)).is_err());
        let high = Observation::new([7; 32], 0, 1, f64::MAX, None)?;
        let low = Observation::new([7; 32], 0, 0, -f64::MAX, None)?;
        assert!(high.difference(&low).is_err());
        let mut invalid = serde_json::to_value(&baseline)?;
        invalid["version"] = 2.into();
        assert!(Observation::decode(&serde_json::to_vec(&invalid)?).is_err());
        let object = serde_json::to_string(&baseline)?;
        let duplicate = format!("{{\"version\":1,{}", &object[1..]);
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
}
