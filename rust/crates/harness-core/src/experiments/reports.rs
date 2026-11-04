//! Assigned-trial denominators and measured cost, without retaining trial rows.

use super::{Error, admit_cost, probes::Assessment};
use serde::Serialize;

/// One arm's summary. Feed each settled assigned trial exactly once, including
/// those without a valid decision. Original outcome/price records stay authoritative.
#[derive(Clone, Copy, Debug, Default)]
pub struct AssignedTrials {
    assigned: u64,
    valid: u64,
    correct: u64,
    adopted_injection: u64,
    costs: Costs,
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
        admit_cost(cost_usd)?;
        self.costs.push_admitted(cost_usd)?;
        // These counters are bounded by assigned, whose addition was checked.
        self.assigned = assigned;
        self.valid += u64::from(assessment.is_some());
        self.correct += u64::from(assessment.is_some_and(Assessment::correct));
        self.adopted_injection += u64::from(assessment.is_some_and(Assessment::adopted_injection));
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
        let costs = self.costs.report(self.assigned);
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
            priced_trials: costs.priced_trials,
            known_cost_usd: costs.known_cost_usd,
            total_cost_usd: costs.total_cost_usd,
            cost_per_assigned_usd: costs.cost_per_assigned_usd,
        })
    }
}

/// The shared cost accumulator. Callers admit each cost once and bound its
/// priced counter by their checked assigned-trial count before adding a trial.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Costs {
    priced: u64,
    known_cost_usd: f64,
}

impl Costs {
    /// Add an already admitted complete cost; sum refusals change no state.
    pub(super) fn push_admitted(&mut self, cost: Option<f64>) -> Result<(), Error> {
        if let Some(cost) = cost {
            let total = self.known_cost_usd + cost;
            if !total.is_finite() {
                return Err(Error::Statistics("trial cost sum overflow"));
            }
            self.priced += 1;
            self.known_cost_usd = total;
        }
        Ok(())
    }

    pub(super) fn report(&self, assigned: u64) -> CostReport {
        let total_cost_usd = (self.priced == assigned).then_some(self.known_cost_usd);
        CostReport {
            version: 1,
            assigned,
            priced_trials: self.priced,
            known_cost_usd: self.known_cost_usd,
            total_cost_usd,
            cost_per_assigned_usd: total_cost_usd
                .filter(|_| assigned != 0)
                .map(|cost| cost / assigned as f64),
        }
    }
}

/// Version-1 arm cost coverage over all assignments, independently of scoring.
/// Caller-supplied costs must include every paid participant and failed work.
#[derive(Debug, PartialEq, Serialize)]
pub struct CostReport {
    /// Persisted report version.
    pub version: u32,
    /// All assigned trials, whether scored or priced.
    pub assigned: u64,
    /// Trials with complete measured accounting, not priced-call coverage.
    pub priced_trials: u64,
    /// Sum for fully priced trials; excludes partially known trial costs.
    pub known_cost_usd: f64,
    /// Full total only when all assigned trials are priced; zero for no assignments.
    pub total_cost_usd: Option<f64>,
    /// Total divided by all assignments; unknown for incomplete or empty input.
    pub cost_per_assigned_usd: Option<f64>,
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

#[cfg(kani)]
mod proofs {
    #[kani::proof]
    fn incomplete_cost_coverage_never_claims_a_full_total_or_mean() {
        let first_known: bool = kani::any();
        let second_known: bool = kani::any();
        let mut trials = super::AssignedTrials::default();
        trials.push(None, first_known.then_some(0.0)).unwrap();
        trials.push(None, second_known.then_some(0.0)).unwrap();
        let report = trials.report().unwrap();
        let complete = first_known && second_known;
        assert_eq!(report.assigned, 2);
        assert_eq!(
            report.priced_trials,
            u64::from(first_known) + u64::from(second_known)
        );
        assert_eq!(report.total_cost_usd, complete.then_some(0.0));
        assert_eq!(report.cost_per_assigned_usd, complete.then_some(0.0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unscored_fractional_costs_keep_assigned_denominators() -> Result<(), Error> {
        let mut failed = AssignedTrials::default();
        assert!(failed.report().is_err());
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
    fn aggregate_cost_and_counter_overflow_preserve_the_summary() -> Result<(), Error> {
        let mut trials = AssignedTrials::default();
        trials.push(None, Some(f64::MAX))?;
        let before = trials.report()?;
        assert!(trials.push(None, Some(f64::MAX)).is_err());
        assert_eq!(trials.report()?, before);
        // Authored bounded analysis state, not an external-tool fixture.
        let mut full = AssignedTrials {
            assigned: u64::MAX - 1,
            costs: Costs {
                priced: u64::MAX - 1,
                ..Default::default()
            },
            ..Default::default()
        };
        full.push(None, Some(0.0))?;
        let before = full.report()?;
        assert!(matches!(full.push(None, Some(0.0)), Err(Error::Overflow)));
        assert_eq!(full.report()?, before);
        Ok(())
    }
}
