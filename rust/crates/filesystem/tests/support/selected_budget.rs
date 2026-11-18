use acyclic_fs_work::performance::{WorkBudget, work_budget};

pub(crate) fn selected_transaction_budget() -> WorkBudget {
    WorkBudget {
        peak_allocation_bytes: 64 * 1024 * 1024,
        ..work_budget(1_000_000, 1_000_000)
    }
}
