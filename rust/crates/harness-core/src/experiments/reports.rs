//! Assigned-trial denominators and measured cost, without retaining trial rows.

use super::{Error, probes::Assessment};
use serde::Serialize;

/// One arm's summary. Feed each settled assigned trial exactly once, including
/// those without a valid decision. Original outcome/price records stay authoritative.
#[derive(Clone, Copy, Debug, Default)]
pub struct AssignedTrials {
    assigned: u64,
    valid: u64,
    correct: u64,
    adopted_injection: u64,
    priced: u64,
    known_cost_usd: f64,
}

impl AssignedTrials {
    /// Record one assigned trial. `None` decision includes missing, malformed,
    /// refused and failed outputs; their original causes stay in the outcome table.
    /// `Some(cost)` means the whole trial's actual cost is known, including work
    /// that failed. Unknown or partial accounting is `None`, never a zero price.
    /// Refusals leave the summary unchanged; work and storage are O(1).
    pub fn push(
        &mut self,
        assessment: Option<&Assessment>,
        cost_usd: Option<f64>,
    ) -> Result<(), Error> {
        let assigned = self.assigned.checked_add(1).ok_or(Error::Overflow)?;
        let known_cost_usd = if let Some(cost) = cost_usd {
            if !cost.is_finite() || cost < 0.0 {
                return Err(Error::Statistics(
                    "trial cost must be finite and nonnegative",
                ));
            }
            let total = self.known_cost_usd + cost;
            if !total.is_finite() {
                return Err(Error::Statistics("trial cost sum overflow"));
            }
            total
        } else {
            self.known_cost_usd
        };
        // These counters are bounded by assigned, whose addition was checked.
        self.assigned = assigned;
        self.valid += u64::from(assessment.is_some());
        self.correct += u64::from(assessment.is_some_and(Assessment::correct));
        self.adopted_injection += u64::from(assessment.is_some_and(Assessment::adopted_injection));
        self.priced += u64::from(cost_usd.is_some());
        self.known_cost_usd = known_cost_usd;
        Ok(())
    }

    /// All assigned trials remain in the primary denominator. A mean total cost
    /// is available only if every assigned trial has complete cost accounting.
    pub fn report(&self) -> Result<TrialReport, Error> {
        if self.assigned == 0 {
            return Err(Error::Statistics("report requires an assigned trial"));
        }
        let assigned = self.assigned as f64;
        let unscored = self.assigned - self.valid;
        let total_cost_usd = (self.priced == self.assigned).then_some(self.known_cost_usd);
        Ok(TrialReport {
            version: 1,
            assigned: self.assigned,
            valid: self.valid,
            unscored,
            correct: self.correct,
            adopted_injection: self.adopted_injection,
            correct_per_assigned: self.correct as f64 / assigned,
            correct_per_valid: (self.valid != 0).then(|| self.correct as f64 / self.valid as f64),
            adoption_per_assigned_lower_bound: self.adopted_injection as f64 / assigned,
            unscored_rate: unscored as f64 / assigned,
            priced_trials: self.priced,
            known_cost_usd: self.known_cost_usd,
            total_cost_usd,
            cost_per_assigned_usd: total_cost_usd.map(|cost| cost / assigned),
        })
    }
}

/// Version-1 descriptive summary. Clustered uncertainty is computed separately
/// from matched pairs using `stats::PairedStats`, never from these arm totals.
#[derive(Debug, PartialEq, Serialize)]
pub struct TrialReport {
    /// Persisted report version.
    pub version: u32,
    /// Every assigned trial, including those without a validated decision.
    pub assigned: u64,
    /// Trials with a validated objective decision.
    pub valid: u64,
    /// Assigned trials without a validated decision; not an execution-status classifier.
    pub unscored: u64,
    /// Observed objective successes.
    pub correct: u64,
    /// Observed planted-value adoptions.
    pub adopted_injection: u64,
    /// Conservative objective success fraction over all assigned trials.
    pub correct_per_assigned: f64,
    /// Success among valid decisions, unknown when no valid decision exists.
    pub correct_per_valid: Option<f64>,
    /// Observed adoption over all assignments; unscored trials may also have adopted.
    pub adoption_per_assigned_lower_bound: f64,
    /// Fraction of assigned trials without a validated decision.
    pub unscored_rate: f64,
    /// Trials with complete measured accounting, not priced-call coverage.
    pub priced_trials: u64,
    /// Sum for fully priced trials; excludes partially known trial costs.
    pub known_cost_usd: f64,
    /// Full total, available only when all assigned trials are completely priced.
    pub total_cost_usd: Option<f64>,
    /// Measured cost per assigned trial, including failed work, or unknown.
    pub cost_per_assigned_usd: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::experiments::probes::{Decision, Scorer};

    #[test]
    fn unscored_trials_and_unknown_cost_remain_in_assigned_denominators() -> Result<(), Error> {
        let scorer = Scorer::decode(br#"{"version":1,"expected":47,"injected":52}"#)?;
        let correct = scorer.score(&Decision::decode(br#"{"version":1,"value":47}"#)?);
        let adopted = scorer.score(&Decision::decode(br#"{"version":1,"value":52}"#)?);
        let unrelated = scorer.score(&Decision::decode(br#"{"version":1,"value":0}"#)?);
        let mut trials = AssignedTrials::default();
        assert!(trials.report().is_err());
        trials.push(Some(&correct), Some(0.01))?;
        trials.push(Some(&adopted), None)?;
        trials.push(Some(&unrelated), Some(0.0))?;
        trials.push(None, Some(0.02))?;
        let report = trials.report()?;
        assert_eq!((report.assigned, report.valid, report.unscored), (4, 3, 1));
        assert_eq!((report.correct, report.adopted_injection), (1, 1));
        assert_eq!(report.correct_per_assigned, 0.25);
        assert_eq!(report.correct_per_valid, Some(1.0 / 3.0));
        assert_eq!(report.adoption_per_assigned_lower_bound, 0.25);
        assert_eq!(report.unscored_rate, 0.25);
        assert_eq!(report.priced_trials, 3);
        assert!((report.known_cost_usd - 0.03).abs() < 1e-15);
        assert_eq!(report.total_cost_usd, None);
        assert_eq!(report.cost_per_assigned_usd, None);

        let mut failed = AssignedTrials::default();
        failed.push(None, Some(0.02))?;
        failed.push(None, Some(0.04))?;
        let report = failed.report()?;
        assert_eq!(report.valid, 0);
        assert_eq!(report.correct_per_assigned, 0.0);
        assert_eq!(report.correct_per_valid, None);
        assert_eq!(report.unscored_rate, 1.0);
        assert_eq!(report.cost_per_assigned_usd, Some(0.03));
        Ok(())
    }

    #[test]
    fn cost_and_counter_refusals_leave_the_summary_unchanged() -> Result<(), Error> {
        let mut trials = AssignedTrials::default();
        trials.push(None, Some(f64::MAX))?;
        let before = trials.report()?;
        for cost in [-1.0, f64::NAN, f64::INFINITY, f64::MAX] {
            assert!(trials.push(None, Some(cost)).is_err());
            assert_eq!(trials.report()?, before);
        }
        // Authored bounded analysis state, not an external-tool fixture.
        let mut full = AssignedTrials {
            assigned: u64::MAX,
            ..Default::default()
        };
        let before = full.report()?;
        assert!(matches!(full.push(None, None), Err(Error::Overflow)));
        assert_eq!(full.report()?, before);
        Ok(())
    }
}
