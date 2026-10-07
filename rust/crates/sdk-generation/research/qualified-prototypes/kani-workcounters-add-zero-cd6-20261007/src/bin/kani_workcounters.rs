#![deny(unsafe_code)]

use acyclic_fs::WorkCounters;

fn arbitrary_work_counters() -> WorkCounters {
    WorkCounters {
        authority_records_read: kani::any(),
        authority_records_appended: kani::any(),
        authority_bytes_read: kani::any(),
        authority_bytes_written: kani::any(),
        object_probes: kani::any(),
        backend_read_operations: kani::any(),
        backend_write_operations: kani::any(),
        durability_operations: kani::any(),
        page_reads: kani::any(),
        page_writes: kani::any(),
        object_bytes_read: kani::any(),
        object_bytes_written: kani::any(),
        bytes_hashed: kani::any(),
        bytes_copied: kani::any(),
        bytes_encoded: kani::any(),
        source_bytes_read: kani::any(),
        source_path_components: kani::any(),
        source_entries_visited: kani::any(),
        output_bytes: kani::any(),
        items_examined: kani::any(),
        items_returned: kani::any(),
        allocation_operations: kani::any(),
        peak_allocation_bytes: kani::any(),
        materializations: kani::any(),
    }
}

/// For every representable WorkCounters value, adding the production zero
/// receipt succeeds and preserves every counter exactly.
#[kani::proof]
#[kani::unwind(2)]
fn checked_add_zero_preserves_all_u64_counters() {
    let value = arbitrary_work_counters();
    assert_eq!(value.checked_add(WorkCounters::default()), Ok(value));
}

/// Deliberately false control: a zero receipt must not change a counter.
#[kani::proof]
#[kani::unwind(2)]
fn negative_control_zero_add_changes_object_probes() {
    let value = arbitrary_work_counters();
    let result = value.checked_add(WorkCounters::default());
    assert_eq!(result.map(|work| work.object_probes), Ok(value.object_probes + 1));
}

fn main() {}

