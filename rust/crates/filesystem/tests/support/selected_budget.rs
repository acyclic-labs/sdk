use acyclic_fs_work::performance::WorkBudget;

pub(crate) fn selected_transaction_budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 1_000_000,
        authority_records_appended: 1_000_000,
        authority_bytes_read: 1_000_000,
        authority_bytes_written: 1_000_000,
        object_probes: 1_000_000,
        backend_read_operations: 1_000_000,
        backend_write_operations: 1_000_000,
        durability_operations: 1_000_000,
        page_reads: 1_000_000,
        page_writes: 1_000_000,
        object_bytes_read: 1_000_000,
        object_bytes_written: 1_000_000,
        bytes_hashed: 1_000_000,
        bytes_copied: 1_000_000,
        bytes_encoded: 1_000_000,
        source_bytes_read: 1_000_000,
        source_path_components: 1_000_000,
        source_entries_visited: 1_000_000,
        output_bytes: 1_000_000,
        items_examined: 1_000_000,
        items_returned: 1_000_000,
        allocation_operations: 1_000_000,
        peak_allocation_bytes: 64 * 1024 * 1024,
        materializations: 1_000_000,
    }
}
