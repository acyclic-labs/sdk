use acyclic_fs_work::performance::{WorkBudget, work_budget};

pub(crate) const FINITE_BUDGET: WorkBudget = work_budget(100_000_000, 100_000_000);

/// Recovery may read, but may never publish or write.
pub(crate) const READ_ONLY_BUDGET: WorkBudget = WorkBudget {
    authority_records_appended: 0,
    authority_bytes_written: 0,
    backend_write_operations: 0,
    durability_operations: 0,
    page_writes: 0,
    object_bytes_written: 0,
    ..FINITE_BUDGET
};
