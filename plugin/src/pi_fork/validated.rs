//! Fork plans already validated, recorded by the digest of their published
//! bytes.
//!
//! A published fork plan is immutable: it is written once under its original
//! operation and never rewritten. Its validation is a pure function of those
//! bytes, the caller it is checked for and the owner it belongs to, so a
//! successful validation is recorded under a digest of all three, and every
//! later check of the same plan, from any request, is a lookup. A plan whose
//! bytes change has a different digest and is validated afresh; a plan built
//! in memory and not yet read back from its publication has no digest and is
//! validated every time. A failed validation is never recorded.
use acyclic_harness::Result;
use std::collections::{HashSet, VecDeque};
use std::sync::{Mutex, PoisonError};

/// Validations kept per service. A spawn checks its plan only while it is in
/// flight, so this bounds memory without turning a live plan back into
/// per-request revalidation; the oldest record is the first to go.
pub(crate) const VALIDATED_FORK_PLANS: usize = 4096;

/// One successful validation: the owner's keyed digest of the published plan
/// bytes and the caller they were checked for.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ValidatedPlanKey(pub(crate) [u8; 32]);

#[derive(Default)]
pub(crate) struct ValidatedForkPlans {
    entries: Mutex<Entries>,
}

#[derive(Default)]
struct Entries {
    recorded: HashSet<ValidatedPlanKey>,
    order: VecDeque<ValidatedPlanKey>,
}

impl ValidatedForkPlans {
    /// Runs `validate` unless this exact published plan was already validated
    /// for this caller and owner, and records it once it succeeds.
    pub(crate) fn validate_once(
        &self,
        key: Option<ValidatedPlanKey>,
        validate: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let Some(key) = key else {
            return validate();
        };
        if self.lock().recorded.contains(&key) {
            return Ok(());
        }
        validate()?;
        let mut entries = self.lock();
        if entries.recorded.insert(key) {
            entries.order.push_back(key);
            while entries.order.len() > VALIDATED_FORK_PLANS {
                if let Some(oldest) = entries.order.pop_front() {
                    entries.recorded.remove(&oldest);
                }
            }
        }
        Ok(())
    }

    // A record is inserted only after its validation succeeded, so a panic
    // elsewhere cannot leave a half-written entry behind.
    fn lock(&self) -> std::sync::MutexGuard<'_, Entries> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().recorded.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn key(byte: u8) -> Option<ValidatedPlanKey> {
        Some(ValidatedPlanKey([byte; 32]))
    }

    /// A spawn checks its plan at every step of every request that touches it:
    /// fork, begin, advance, the commit and the prepared observation.
    const CHECKS_PER_SPAWN: usize = 10;

    #[test]
    fn an_unchanged_plan_is_validated_once_across_spawns() {
        let plans = ValidatedForkPlans::default();
        let runs = Cell::new(0);
        for _spawn in 0..8 {
            for _check in 0..CHECKS_PER_SPAWN {
                plans
                    .validate_once(key(1), || {
                        runs.set(runs.get() + 1);
                        Ok(())
                    })
                    .unwrap();
            }
        }
        assert_eq!(runs.get(), 1);
    }

    /// Negative control: the per-request path validated at every check.
    #[test]
    fn per_request_revalidation_validates_at_every_check() {
        let plans = ValidatedForkPlans::default();
        let runs = Cell::new(0);
        for _spawn in 0..8 {
            for _check in 0..CHECKS_PER_SPAWN {
                plans
                    .validate_once(None, || {
                        runs.set(runs.get() + 1);
                        Ok(())
                    })
                    .unwrap();
            }
        }
        assert_eq!(runs.get(), 8 * CHECKS_PER_SPAWN);
        assert_eq!(plans.len(), 0);
    }

    #[test]
    fn a_changed_digest_is_validated_again() {
        let plans = ValidatedForkPlans::default();
        let runs = Cell::new(0);
        for digest in [1, 2, 1, 2] {
            plans
                .validate_once(key(digest), || {
                    runs.set(runs.get() + 1);
                    Ok(())
                })
                .unwrap();
        }
        assert_eq!(runs.get(), 2);
    }

    #[test]
    fn a_failed_validation_is_not_recorded() {
        let plans = ValidatedForkPlans::default();
        let refused = plans.validate_once(key(3), || {
            Err(acyclic_harness::Error::Invalid("changed plan".into()))
        });
        assert!(refused.is_err());
        let runs = Cell::new(0);
        plans
            .validate_once(key(3), || {
                runs.set(runs.get() + 1);
                Ok(())
            })
            .unwrap();
        assert_eq!(runs.get(), 1);
    }

    #[test]
    fn records_are_bounded_and_the_oldest_is_validated_again() {
        let plans = ValidatedForkPlans::default();
        let digest = |index: usize| {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
            Some(ValidatedPlanKey(bytes))
        };
        for index in 0..=VALIDATED_FORK_PLANS {
            plans.validate_once(digest(index), || Ok(())).unwrap();
        }
        assert_eq!(plans.len(), VALIDATED_FORK_PLANS);
        let runs = Cell::new(0);
        let count = || {
            runs.set(runs.get() + 1);
            Ok(())
        };
        plans
            .validate_once(digest(VALIDATED_FORK_PLANS), count)
            .unwrap();
        assert_eq!(runs.get(), 0);
        plans
            .validate_once(digest(0), || {
                runs.set(runs.get() + 1);
                Ok(())
            })
            .unwrap();
        assert_eq!(runs.get(), 1);
    }
}
