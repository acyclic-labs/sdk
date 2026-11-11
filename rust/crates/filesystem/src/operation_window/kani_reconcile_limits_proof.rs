use super::{OperationReconcileLimits, validate_reconcile_limits};
use crate::WorkspaceError;

#[kani::proof]
#[kani::unwind(2)]
fn validate_reconcile_limits_iff_all_three_nonzero() {
    let maximum_generations: Option<u32> = kani::any();
    let maximum_changes: Option<u32> = kani::any();
    let maximum_conflicts: Option<u32> = kani::any();
    let limits = OperationReconcileLimits {
        maximum_generations,
        maximum_changes,
        maximum_conflicts,
    };
    let positive = maximum_generations != Some(0)
        && maximum_changes != Some(0)
        && maximum_conflicts != Some(0);
    let result = validate_reconcile_limits(limits);
    assert_eq!(result.is_ok(), positive);
    if !positive {
        assert!(matches!(result, Err(WorkspaceError::JoinLimit)));
    }
}
