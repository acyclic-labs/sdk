//! One-pass analysis of ordered, admitted observation pairs. No trial payloads
//! or cluster-key map are retained; absent values still count as assignments.

use super::{
    Error, Observation,
    plan::PairAssignment,
    reports::{CostReport, Costs},
    stats::{Cluster, Estimate, PairedStats},
};
use serde::Serialize;

/// Version-2 coverage, arm costs and estimates for one declared arm contrast.
#[derive(Debug, Serialize)]
pub struct AnalysisReport {
    /// Persisted output format.
    pub version: u32,
    /// Declared baseline arm, first in each input pair.
    pub baseline_arm: u64,
    /// Declared treatment arm, second in each input pair.
    pub treatment_arm: u64,
    /// All assigned pairs, including those with no known scores or costs.
    pub assigned_pairs: u64,
    /// All source cuts, including entirely unobserved cuts.
    pub assigned_clusters: u64,
    /// Score coverage and uncertainty over observed pairs only.
    pub score: MetricReport,
    /// Complete-cost coverage, independent of score validity.
    pub cost_usd: MetricReport,
    /// Baseline costs over all assignments, independently of matched coverage.
    pub baseline_cost: CostReport,
    /// Treatment costs over all assignments, independently of matched coverage.
    pub treatment_cost: CostReport,
}

/// A measured subset cannot establish uncertainty for excluded assignments.
#[derive(Debug, Serialize)]
pub struct MetricReport {
    /// Pairs with both values known.
    pub pairs: u64,
    /// Source cuts with at least one known pair for this metric.
    pub clusters: u64,
    /// Unknown with fewer than two contributing independent cuts.
    pub estimate: Option<Estimate>,
}

#[derive(Default)]
struct Metric {
    current: Cluster,
    stats: PairedStats,
}

impl Metric {
    fn push(&mut self, value: Option<f64>) -> Result<(), Error> {
        if let Some(value) = value {
            self.current.push(value)?;
        }
        Ok(())
    }

    fn finish_cluster(&mut self) -> Result<(), Error> {
        if !self.current.is_empty() {
            self.stats.push(self.current)?;
            self.current = Cluster::default();
        }
        Ok(())
    }

    fn report(self) -> Result<MetricReport, Error> {
        let (pairs, clusters) = self.stats.counts();
        Ok(MetricReport {
            pairs,
            clusters,
            estimate: if clusters >= 2 {
                Some(self.stats.estimate()?)
            } else {
                None
            },
        })
    }
}

fn advance_key(
    last: &mut Option<([u8; 32], u64)>,
    population: &[u8; 32],
    replicate: u64,
) -> Result<bool, Error> {
    match last {
        Some((previous_population, previous_replicate)) => {
            match population.cmp(previous_population) {
                std::cmp::Ordering::Less => Err(Error::Analysis("population keys decreased")),
                std::cmp::Ordering::Equal => {
                    if replicate <= *previous_replicate {
                        return Err(Error::Analysis("replicate key repeated or decreased"));
                    }
                    *previous_replicate = replicate;
                    Ok(false)
                }
                std::cmp::Ordering::Greater => {
                    *previous_population = *population;
                    *previous_replicate = replicate;
                    Ok(true)
                }
            }
        }
        None => {
            *last = Some((*population, replicate));
            Ok(true)
        }
    }
}

/// An admitted arm orientation, reusable without repeating configuration checks.
#[derive(Clone, Copy, Debug)]
pub struct Contrast {
    baseline_arm: u64,
    treatment_arm: u64,
}

impl Contrast {
    /// Admit distinct arms before opening or consuming an input file.
    pub fn new(baseline_arm: u64, treatment_arm: u64) -> Result<Self, Error> {
        if baseline_arm == treatment_arm {
            return Err(Error::Analysis("baseline and treatment must differ"));
        }
        Ok(Self {
            baseline_arm,
            treatment_arm,
        })
    }

    /// Consume baseline then treatment for every assigned population/replicate pair.
    /// Pair keys must strictly increase lexicographically, so duplicate pairs and
    /// repeated closed clusters are refused with one retained key, without a map.
    /// Original independent grouping, provider/cut evidence and numeric-score
    /// semantics remain caller-owned. No input row or partial report escapes on error.
    /// Supply the original, ordered assignment stream, declared before outcomes.
    /// Match every key exactly: equal row counts cannot hide a replaced pair.
    /// Failed trials still need rows with unknown values. Missing, surplus and
    /// substituted assignments are refused, including entirely omitted pairs.
    /// Work is O(1) per row and retained space is O(1) for the entire input.
    pub fn analyze(
        &self,
        rows: impl IntoIterator<Item = Result<Observation, Error>>,
        expected: impl IntoIterator<Item = Result<PairAssignment, Error>>,
    ) -> Result<AnalysisReport, Error> {
        let Self {
            baseline_arm,
            treatment_arm,
        } = *self;
        let mut rows = rows.into_iter();
        let mut expected = expected.into_iter();
        let mut last: Option<([u8; 32], u64)> = None;
        let (mut assigned_pairs, mut assigned_clusters) = (0u64, 0u64);
        let (mut score, mut cost) = (Metric::default(), Metric::default());
        let (mut baseline_cost, mut treatment_cost) = (Costs::default(), Costs::default());
        while let Some(baseline) = rows.next() {
            let baseline = baseline?;
            let assignment = expected
                .next()
                .ok_or(Error::Analysis("input exceeds the declared assignments"))??;
            if baseline.0.population != assignment.population
                || baseline.0.replicate != assignment.replicate
            {
                return Err(Error::Analysis(
                    "input differs from the declared assignment",
                ));
            }
            let next_assigned = assigned_pairs.checked_add(1).ok_or(Error::Overflow)?;
            let treatment = rows
                .next()
                .ok_or(Error::Analysis("unpaired baseline at end of input"))??;
            if baseline.0.arm != baseline_arm || treatment.0.arm != treatment_arm {
                return Err(Error::Analysis(
                    "input arms differ from declared orientation",
                ));
            }
            let pair = treatment.pair(&baseline)?;
            let new_cluster = advance_key(&mut last, &baseline.0.population, baseline.0.replicate)?;
            if new_cluster {
                score.finish_cluster()?;
                cost.finish_cluster()?;
                assigned_clusters = assigned_clusters.checked_add(1).ok_or(Error::Overflow)?;
            }
            score.push(pair.score()?)?;
            cost.push(pair.cost())?;
            baseline_cost.push_admitted(baseline.0.cost_usd)?;
            treatment_cost.push_admitted(treatment.0.cost_usd)?;
            assigned_pairs = next_assigned;
        }
        if expected.next().transpose()?.is_some() {
            return Err(Error::Analysis("input omits declared assignments"));
        }
        score.finish_cluster()?;
        cost.finish_cluster()?;
        Ok(AnalysisReport {
            version: 2,
            baseline_arm,
            treatment_arm,
            assigned_pairs,
            assigned_clusters,
            score: score.report()?,
            cost_usd: cost.report()?,
            baseline_cost: baseline_cost.report(assigned_pairs),
            treatment_cost: treatment_cost.report(assigned_pairs),
        })
    }
}

#[cfg(kani)]
mod proofs {
    #[kani::proof]
    #[kani::unwind(34)]
    fn a_complete_pair_is_accepted_only_for_its_declared_assignment() {
        let declared_cut: u8 = kani::any();
        let actual_cut: u8 = kani::any();
        let declared_replicate: u64 = kani::any();
        let actual_replicate: u64 = kani::any();
        let rows = [0, 1].map(|arm| {
            super::Observation::new([actual_cut; 32], actual_replicate, arm, None, None)
        });
        let expected = [Ok(super::PairAssignment::new(
            [declared_cut; 32],
            declared_replicate,
        ))];
        let result = super::Contrast::new(0, 1).unwrap().analyze(rows, expected);
        assert_eq!(
            result.is_ok(),
            declared_cut == actual_cut && declared_replicate == actual_replicate
        );
    }

    #[kani::proof]
    #[kani::unwind(33)]
    fn ordered_pair_admission_rejects_reuse_without_changing_the_last_key() {
        let first_cut: u8 = kani::any();
        let first_replicate: u64 = kani::any();
        let next_cut: u8 = kani::any();
        let next_replicate: u64 = kani::any();
        let mut last = None;
        assert!(super::advance_key(&mut last, &[first_cut; 32], first_replicate).unwrap());
        let before = last;
        let result = super::advance_key(&mut last, &[next_cut; 32], next_replicate);
        let advances =
            next_cut > first_cut || (next_cut == first_cut && next_replicate > first_replicate);
        assert_eq!(result.is_ok(), advances);
        match result {
            Ok(new_cluster) => {
                assert_eq!(new_cluster, next_cut > first_cut);
                assert_eq!(last, Some(([next_cut; 32], next_replicate)));
            }
            Err(_) => assert_eq!(last, before),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn analyze(
        rows: impl IntoIterator<Item = Result<Observation, Error>>,
        baseline: u64,
        treatment: u64,
        expected: impl IntoIterator<Item = (u8, u64)>,
    ) -> Result<AnalysisReport, Error> {
        Contrast::new(baseline, treatment)?.analyze(
            rows,
            expected
                .into_iter()
                .map(|(cut, replicate)| Ok(PairAssignment::new([cut; 32], replicate))),
        )
    }

    #[test]
    fn empty_cuts_and_unscored_paid_trials_keep_separate_coverage() -> Result<(), Error> {
        // Authored numerical inputs, not captured provider output or measured prices.
        let mut bytes = Vec::new();
        for (cut, replicate, scores, costs) in [
            (1, 0, [None, None], [None, None]),
            (2, 0, [Some(1.0), Some(2.0)], [Some(4.0), Some(5.0)]),
            (2, 1, [Some(0.0), None], [Some(3.0), Some(5.0)]),
            (3, 0, [Some(0.0), Some(2.0)], [Some(0.0), Some(0.0)]),
            (3, 1, [None, None], [None, Some(1.0)]),
        ] {
            for arm in 0..2 {
                let row =
                    Observation::new([cut; 32], replicate, arm as u64, scores[arm], costs[arm])?;
                serde_json::to_writer(&mut bytes, &row)?;
                bytes.push(b'\n');
            }
        }
        let rows = serde_json::Deserializer::from_slice(&bytes)
            .into_iter::<Observation>()
            .map(|row| row.map_err(Error::Json));
        let report = analyze(rows, 0, 1, [(1, 0), (2, 0), (2, 1), (3, 0), (3, 1)])?;
        assert_eq!((report.assigned_pairs, report.assigned_clusters), (5, 3));
        assert_eq!((report.score.pairs, report.score.clusters), (2, 2));
        assert_eq!(
            report
                .score
                .estimate
                .as_ref()
                .map(|estimate| estimate.difference),
            Some(1.5)
        );
        assert_eq!((report.cost_usd.pairs, report.cost_usd.clusters), (3, 2));
        assert_eq!(
            report
                .cost_usd
                .estimate
                .as_ref()
                .map(|estimate| estimate.difference),
            Some(1.0)
        );

        let report = analyze(
            [
                Observation::new([1; 32], 0, 0, None, None),
                Observation::new([1; 32], 0, 1, None, Some(1.0)),
                Observation::new([2; 32], 0, 0, None, Some(0.0)),
                Observation::new([2; 32], 0, 1, None, Some(1.0)),
            ],
            0,
            1,
            [(1, 0), (2, 0)],
        )?;
        assert_eq!((report.assigned_pairs, report.assigned_clusters), (2, 2));
        assert_eq!((report.score.pairs, report.score.clusters), (0, 0));
        assert!(report.score.estimate.is_none());
        assert_eq!((report.cost_usd.pairs, report.cost_usd.clusters), (1, 1));
        assert!(report.cost_usd.estimate.is_none());
        Ok(())
    }

    #[test]
    fn arm_costs_keep_unmatched_and_unscored_assignments_without_inventing_totals()
    -> Result<(), Error> {
        // Authored prices exercise accounting; none are measured provider costs.
        let report = analyze(
            [
                Observation::new([1; 32], 0, 0, None, Some(1.0)),
                Observation::new([1; 32], 0, 1, None, Some(0.0)),
                Observation::new([1; 32], 1, 0, None, Some(2.0)),
                Observation::new([1; 32], 1, 1, None, Some(4.0)),
                Observation::new([2; 32], 0, 0, None, None),
                Observation::new([2; 32], 0, 1, None, Some(2.0)),
            ],
            0,
            1,
            [(1, 0), (1, 1), (2, 0)],
        )?;
        assert_eq!((report.assigned_pairs, report.assigned_clusters), (3, 2));
        assert_eq!((report.score.pairs, report.cost_usd.pairs), (0, 2));
        assert_eq!(report.cost_usd.clusters, 1);
        assert!(report.cost_usd.estimate.is_none());
        assert_eq!(report.baseline_cost.assigned, 3);
        assert_eq!(report.baseline_cost.priced_trials, 2);
        assert_eq!(report.baseline_cost.known_cost_usd, 3.0);
        assert_eq!(report.baseline_cost.total_cost_usd, None);
        assert_eq!(report.baseline_cost.cost_per_assigned_usd, None);
        assert_eq!(report.treatment_cost.assigned, 3);
        assert_eq!(report.treatment_cost.priced_trials, 3);
        assert_eq!(report.treatment_cost.known_cost_usd, 6.0);
        assert_eq!(report.treatment_cost.total_cost_usd, Some(6.0));
        assert_eq!(report.treatment_cost.cost_per_assigned_usd, Some(2.0));

        let empty = analyze([], 0, 1, [])?;
        assert_eq!(empty.baseline_cost.total_cost_usd, Some(0.0));
        assert_eq!(empty.baseline_cost.cost_per_assigned_usd, None);
        Ok(())
    }

    #[test]
    fn incomplete_or_failed_input_never_returns_a_partial_report() -> Result<(), Error> {
        let row = |cut, replicate, arm| Observation::new([cut; 32], replicate, arm, None, None);
        assert!(analyze([row(1, 0, 0)], 0, 1, [(1, 0)]).is_err());
        assert!(analyze([row(1, 0, 1), row(1, 0, 0)], 0, 1, [(1, 0)]).is_err());
        assert!(analyze([row(1, 0, 0), row(2, 0, 1)], 0, 1, [(1, 0)]).is_err());
        assert!(analyze([row(1, 0, 0), row(1, 1, 1)], 0, 1, [(1, 0)]).is_err());
        assert!(analyze([row(1, 0, 0), row(1, 0, 1)], 0, 1, [(1, 0), (2, 0)]).is_err());
        assert!(analyze([row(1, 0, 0), row(1, 0, 1)], 0, 1, []).is_err());
        assert!(analyze([], 0, 0, []).is_err());
        let baseline = row(1, 0, 0)?;
        let mut bytes = serde_json::to_vec(&baseline)?;
        bytes.extend_from_slice(b"\n{\"version\":");
        let rows = serde_json::Deserializer::from_slice(&bytes)
            .into_iter::<Observation>()
            .map(|row| row.map_err(Error::Json));
        assert!(matches!(analyze(rows, 0, 1, [(1, 0)]), Err(Error::Json(_))));
        let rows = (0..2).flat_map(|replicate| {
            (0..2).map(move |arm| Observation::new([1; 32], replicate, arm, None, Some(f64::MAX)))
        });
        assert!(matches!(
            analyze(rows, 0, 1, [(1, 0), (1, 1)]),
            Err(Error::Statistics(_))
        ));
        Ok(())
    }

    #[test]
    fn streamed_pair_analysis_has_zero_allocations_independent_of_cut_count() -> Result<(), Error> {
        let rows = || {
            (0u8..32).flat_map(|cut| {
                (0u64..2).flat_map(move |replicate| {
                    (0u64..2).map(move |arm| {
                        Observation::new([cut; 32], replicate, arm, Some(arm as f64), Some(0.0))
                    })
                })
            })
        };
        let mut result = None;
        let measured = acyclic_test_support::measure(|| {
            let expected = (0u8..32).flat_map(|cut| (0..2).map(move |replicate| (cut, replicate)));
            result = Some(analyze(rows(), 0, 1, expected));
        });
        let report = result.ok_or(Error::Analysis("allocation control did not run"))??;
        assert_eq!((report.assigned_pairs, report.assigned_clusters), (64, 32));
        assert_eq!(measured.count_total, 0);
        assert_eq!(measured.bytes_max, 0);

        // Negative control: retaining even these scalar rows violates the same bound.
        let mut retained = Vec::new();
        let measured = acyclic_test_support::measure(|| retained.extend(rows()));
        assert!(measured.count_total > 0);
        assert!(measured.bytes_max > 0);
        Ok(())
    }
}
