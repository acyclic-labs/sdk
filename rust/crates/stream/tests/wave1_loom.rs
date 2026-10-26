//! Production state-machine models; exhaustive at the explicit preemption bound.
#![cfg(all(acyclic_loom, feature = "local"))]
#[test]
fn subtree_fork_races_coordinated_descendant_writes() {
    acyclic_stream::verification::subtree_cut(false);
}
#[test]
#[should_panic(expected = "fork must inherit both descendants at one commit cut")]
fn negative_control_old_root_only_fork() {
    acyclic_stream::verification::subtree_cut(true);
}
#[test]
fn nested_fork_excludes_existing_siblings() {
    acyclic_stream::verification::subtree_exclusions(false);
}
#[test]
#[should_panic(expected = "excluded sibling must not leak into the fork")]
fn negative_control_old_fork_without_exclusions() {
    acyclic_stream::verification::subtree_exclusions(true);
}

#[test]
fn page_statuses_share_one_atomic_cut() {
    acyclic_stream::verification::page_tail_cut(false);
}
#[test]
#[should_panic(expected = "page statuses must share one namespace cut")]
fn negative_control_independent_status_reads() {
    acyclic_stream::verification::page_tail_cut(true);
}

#[test]
fn projected_status_records_obey_the_page_byte_bound() {
    acyclic_stream::verification::page_tail_bytes(false);
}

#[test]
#[should_panic(expected = "projected record headers and values must fit the page byte bound")]
fn negative_control_uncharged_status_records() {
    acyclic_stream::verification::page_tail_bytes(true);
}
