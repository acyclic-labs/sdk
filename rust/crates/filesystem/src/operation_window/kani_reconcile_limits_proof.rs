//! Kani harnesses for the private production reconciliation-limit validator.

use super::{OperationReconcileLimits, validate_reconcile_limits};
use crate::WorkspaceError;

#[kani::proof]
#[kani::unwind(2)]
fn validate_reconcile_limits_iff_all_three_nonzero() {
    let maximum_generations: u32 = kani::any();
    let maximum_changes: u32 = kani::any();
    let maximum_conflicts: u32 = kani::any();
    let limits = OperationReconcileLimits {
        maximum_generations,
        maximum_changes,
        maximum_conflicts,
    };
    let positive = maximum_generations != 0 && maximum_changes != 0 && maximum_conflicts != 0;
    let result = validate_reconcile_limits(limits);
    assert_eq!(result.is_ok(), positive);
    if !positive {
        assert!(matches!(result, Err(WorkspaceError::JoinLimit)));
    }
}

#[kani::proof]
#[kani::unwind(2)]
fn validate_reconcile_limits_negative_control_accepts_zero_generation() {
    let result = validate_reconcile_limits(OperationReconcileLimits {
        maximum_generations: 0,
        maximum_changes: 1,
        maximum_conflicts: 1,
    });
    assert!(result.is_ok());
}
