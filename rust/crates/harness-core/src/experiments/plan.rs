//! Counterbalanced assignment order for one source population. This plans
//! indices only; actual source cuts, configuration and execution stay external.

use super::Error;
use std::num::NonZeroU64;

/// One assigned arm, independent of whether its execution later succeeds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Assignment {
    /// Zero-based replicate within this source population.
    pub replicate: u64,
    /// Zero-based index into the caller's predeclared arm table.
    pub arm: u64,
}

/// A declaration, not a scheduler. Every replicate assigns each arm once.
/// Replicate r rotates arm order by r, counterbalancing every position over
/// each complete block of `arms` replicates. A partial block remains unbalanced.
/// Storage and work per assignment are O(1), with no allocation or waiting.
#[derive(Clone, Copy, Debug)]
pub struct AssignmentPlan {
    arms: NonZeroU64,
    trials: u64,
}

impl AssignmentPlan {
    /// Admit the counts once, before any paid execution. Zero replicates makes
    /// an empty plan; zero arms and an unrepresentable trial count are refused.
    pub fn new(arms: u64, replicates: u64) -> Result<Self, Error> {
        let arms = NonZeroU64::new(arms).ok_or(Error::Plan("at least one arm is required"))?;
        let trials = arms
            .get()
            .checked_mul(replicates)
            .ok_or(Error::Plan("arm-by-replicate count exceeds u64"))?;
        Ok(Self { arms, trials })
    }

    /// All assigned arm/replicate entries, including future failures.
    pub fn trials(&self) -> u64 {
        self.trials
    }

    /// Direct indexed access; an index outside the declaration is finished.
    pub fn assignment(&self, index: u64) -> Option<Assignment> {
        (index < self.trials).then(|| self.at(index))
    }

    /// Lazy execution order. The finite range advances once per assignment;
    /// it stores no rows and never changes its order in response to outcomes.
    pub fn iter(self) -> impl DoubleEndedIterator<Item = Assignment> {
        (0..self.trials).map(move |index| self.at(index))
    }

    fn at(&self, index: u64) -> Assignment {
        let arms = self.arms.get();
        let replicate = index / arms;
        // position + replicate < arms * replicates for every admitted index,
        // so the checked product also proves this addition cannot overflow.
        let arm = (index % arms + replicate) % arms;
        Assignment { replicate, arm }
    }
}

#[cfg(kani)]
mod proofs {
    #[kani::proof]
    fn assignment_indices_are_bijective_within_the_declared_population() {
        let arms: u64 = kani::any();
        let replicates: u64 = kani::any();
        kani::assume((1..=4).contains(&arms));
        kani::assume((1..=4).contains(&replicates));
        let plan = super::AssignmentPlan::new(arms, replicates).unwrap();
        let first: u64 = kani::any();
        let second: u64 = kani::any();
        kani::assume(first < plan.trials());
        kani::assume(second < plan.trials());
        let a = plan.assignment(first).unwrap();
        let b = plan.assignment(second).unwrap();
        assert!(a.arm < arms && a.replicate < replicates);
        assert!(b.arm < arms && b.replicate < replicates);
        assert_eq!(a == b, first == second);
        assert_eq!(plan.assignment(plan.trials()), None);

        let position: u64 = kani::any();
        kani::assume(position < arms);
        let block = super::AssignmentPlan::new(arms, arms).unwrap();
        let first_replicate = first % arms;
        let second_replicate = second % arms;
        let a = block.assignment(first_replicate * arms + position).unwrap();
        let b = block
            .assignment(second_replicate * arms + position)
            .unwrap();
        assert_eq!(a.arm == b.arm, first_replicate == second_replicate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_blocks_balance_positions_in_the_predeclared_order() -> Result<(), Error> {
        let plan = AssignmentPlan::new(4, 4)?;
        let mut positions = [0u8; 4];
        for (index, assignment) in plan.iter().enumerate() {
            let position = index % 4;
            // A repeated arm at a position is refused by this coverage control.
            let bit = 1 << assignment.arm;
            assert_eq!(positions[position] & bit, 0);
            positions[position] |= bit;
        }
        assert_eq!(positions, [15; 4]);

        // Negative control: repeating the same arm order never counterbalances.
        let mut repeated_order = [0u8; 4];
        for index in 0..16 {
            repeated_order[index % 4] |= 1 << (index % 4);
        }
        assert_ne!(repeated_order, positions);

        let mut order = AssignmentPlan::new(2, 2)?.iter();
        assert_eq!(
            order.next(),
            Some(Assignment {
                replicate: 0,
                arm: 0
            })
        );
        // Outcomes are not inputs: advancing keeps the declared next arm.
        assert_eq!(
            order.next(),
            Some(Assignment {
                replicate: 0,
                arm: 1
            })
        );
        assert_eq!(
            order.next(),
            Some(Assignment {
                replicate: 1,
                arm: 1
            })
        );
        assert_eq!(
            order.next(),
            Some(Assignment {
                replicate: 1,
                arm: 0
            })
        );
        assert_eq!(order.next(), None);
        Ok(())
    }

    #[test]
    fn indexed_work_is_bounded_even_at_the_largest_declaration() -> Result<(), Error> {
        assert!(AssignmentPlan::new(0, 1).is_err());
        assert!(AssignmentPlan::new(u64::MAX, 2).is_err());
        assert_eq!(AssignmentPlan::new(1, 0)?.iter().next(), None);
        let plan = AssignmentPlan::new(2, u64::MAX / 2)?;
        let mut last = None;
        let work = acyclic_test_support::measure(|| last = plan.assignment(plan.trials() - 1));
        assert_eq!(
            last,
            Some(Assignment {
                replicate: u64::MAX / 2 - 1,
                arm: 1
            })
        );
        assert_eq!((work.count_total, work.bytes_max), (0, 0));
        assert_eq!(plan.assignment(plan.trials()), None);
        assert_eq!(
            AssignmentPlan::new(u64::MAX, 1)?.assignment(u64::MAX - 1),
            Some(Assignment {
                replicate: 0,
                arm: u64::MAX - 1
            })
        );
        assert_eq!(
            AssignmentPlan::new(1, u64::MAX)?.assignment(u64::MAX - 1),
            Some(Assignment {
                replicate: u64::MAX - 1,
                arm: 0
            })
        );

        // Negative control: retaining assignments needs heap space.
        let mut retained = Vec::new();
        let work = acyclic_test_support::measure(|| {
            retained.extend(AssignmentPlan::new(2, 2).unwrap().iter());
        });
        assert!(work.count_total > 0 && work.bytes_max > 0);
        Ok(())
    }
}
