//! Production state-machine models; exhaustive at the explicit preemption bound.
#[test]
fn subtree_fork_races_coordinated_descendant_writes() {
    crate::verification::subtree_cut(false);
}
#[test]
#[should_panic(expected = "fork must inherit both descendants at one commit cut")]
fn negative_control_old_root_only_fork() {
    crate::verification::subtree_cut(true);
}
#[test]
fn nested_fork_excludes_existing_siblings() {
    crate::verification::subtree_exclusions(false);
}
#[test]
#[should_panic(expected = "excluded sibling must not leak into the fork")]
fn negative_control_old_fork_without_exclusions() {
    crate::verification::subtree_exclusions(true);
}

#[test]
fn page_statuses_share_one_atomic_cut() {
    crate::verification::page_tail_cut(false);
}
#[test]
#[should_panic(expected = "page statuses must share one namespace cut")]
fn negative_control_independent_status_reads() {
    crate::verification::page_tail_cut(true);
}

#[test]
fn projected_status_records_obey_the_page_byte_bound() {
    crate::verification::page_tail_bytes(false);
}

#[test]
#[should_panic(expected = "projected record headers and values must fit the page byte bound")]
fn negative_control_uncharged_status_records() {
    crate::verification::page_tail_bytes(true);
}

#[test]
fn agent_spawn_is_one_cut_with_exclusions() {
    crate::verification::agent_spawn(false);
}
#[test]
#[should_panic(expected = "invariant=SubtreeMembership")]
fn negative_control_spawn_without_exclusions() {
    crate::verification::agent_spawn(true);
}
#[test]
fn agent_closed_inbox_refuses_sends() {
    crate::verification::agent_closed_inbox(false);
}
#[test]
#[should_panic(expected = "invariant=ExactConflict")]
fn negative_control_closed_inbox_without_absent() {
    crate::verification::agent_closed_inbox(true);
}
#[test]
fn agent_inherited_paths_follow_privately() {
    crate::verification::agent_inherited_follow(false);
}
#[test]
#[should_panic(expected = "step=follow_inherited")]
fn negative_control_root_only_fork_follow() {
    crate::verification::agent_inherited_follow(true);
}
#[test]
fn agent_fork_of_fork_keeps_lineage() {
    crate::verification::agent_fork_lineage(false);
}
#[test]
#[should_panic(expected = "agent history rejected the provider")]
fn negative_control_root_only_fork_lineage() {
    crate::verification::agent_fork_lineage(true);
}

#[test]
fn one_stable_key_per_intent_survives_a_racing_conflict() {
    crate::verification::stable_intent_key(false);
}
#[test]
#[should_panic(expected = "a conflict must not spend the intent key")]
fn negative_control_conflict_spends_the_key() {
    crate::verification::stable_intent_key(true);
}
