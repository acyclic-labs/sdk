pub struct Invariant {
    pub id: &'static str,
    pub primary_test: &'static str,
}

pub const INVARIANTS: &[Invariant] = &[
    Invariant {
        id: "package.runtime-read-only",
        primary_test: "packaged_launcher_is_read_only",
    },
    Invariant {
        id: "hooks.pre-tool-fails-closed",
        primary_test: "pre_tool_failure_uses_the_host_blocking_exit_code",
    },
    Invariant {
        id: "hooks.session-end-deadline",
        primary_test: "codex_hook_manifest_respects_terminal_deadline",
    },
    Invariant {
        id: "roots.sequential-source-identity",
        primary_test: "sequential_same_root_sessions_reuse_the_durable_source_identity",
    },
    Invariant {
        id: "layout.single-plugin-root",
        primary_test: "repository_has_one_canonical_plugin_root",
    },
    Invariant {
        id: "service.platform-lock-contention",
        primary_test: "service_lock_contention_uses_platform_error_semantics",
    },
    Invariant {
        id: "service.concurrent-session-isolation",
        primary_test: "shared_service_isolates_multiple_sessions",
    },
    Invariant {
        id: "service.graceful-drain-releases-lock",
        primary_test: "service_drain_waits_for_shutdown_completion_and_lock_release",
    },
    Invariant {
        id: "service.binary-upgrade-handoff",
        primary_test: "service_handoff_durably_drains_a_mismatched_binary",
    },
    Invariant {
        id: "service.endpoint-shutdown-cancels-inflight",
        primary_test: "endpoint_shutdown_cancels_inflight_requests_before_reopen",
    },
    Invariant {
        id: "process.timeout-contains-descendants",
        primary_test: "termination_contains_descendants",
    },
    Invariant {
        id: "lifecycle.invalid-order-fails-closed",
        primary_test: "lifecycle_edges_recover_and_fail_closed",
    },
    Invariant {
        id: "roots.physical-identity-cannot-be-replaced",
        primary_test: "reopening_rejects_a_replaced_physical_root",
    },
    Invariant {
        id: "roots.direct-parent-recursive-authorization",
        primary_test: "recursive_lineage_and_direct_parent_authorization_survive_restart",
    },
    Invariant {
        id: "roots.recursive-discard-invalidates-routing",
        primary_test: "mount_routing_wins_and_discard_is_recursive",
    },
    Invariant {
        id: "publication.repeated-merge-is-idempotent",
        primary_test: "merge_plans_are_immutable_stale_safe_and_publication_is_idempotent",
    },
    Invariant {
        id: "publication.multi-root-is-atomic",
        primary_test: "multi_root_contexts_fork_route_publish_and_resume_atomically",
    },
    Invariant {
        id: "publication.crash-history-recovers",
        primary_test: "publication_history_recovers_after_a_crash_boundary",
    },
    Invariant {
        id: "git.unsupported-transport-is-rejected",
        primary_test: "argv_covers_local_history_and_rejects_transport",
    },
    Invariant {
        id: "git.conflict-can-continue-or-abort",
        primary_test: "conflicted_merge_can_continue_or_abort_without_a_second_sequencer",
    },
    Invariant {
        id: "git.ignore-transitions-preserve-tracked-files",
        primary_test: "capture_omits_newly_ignored_paths_but_keeps_tracked_descendants",
    },
    Invariant {
        id: "git.commands-share-one-materializer",
        primary_test: "root_git_uses_the_same_repository_and_materializer",
    },
    Invariant {
        id: "paths.patch-cannot-escape-child",
        primary_test: "patch_paths_cannot_escape_the_child",
    },
    Invariant {
        id: "paths.structured-input-cannot-escape-child",
        primary_test: "structured_paths_must_remain_inside_the_child",
    },
    Invariant {
        id: "paths.shell-expansion-cannot-escape-child",
        primary_test: "shell_expansion_cannot_escape_the_child",
    },
    Invariant {
        id: "state.previous-generation-recovery-is-bounded",
        primary_test: "adapter_state_recovers_only_from_a_valid_bounded_previous_snapshot",
    },
    Invariant {
        id: "journal.exclusive-owner-repairs-torn-tail",
        primary_test: "local_provider_excludes_a_second_process_owner_and_repairs_a_torn_tail",
    },
    Invariant {
        id: "journal.capacity-rejects-before-mutation",
        primary_test: "journal_capacity_rejects_before_mutating_visible_state",
    },
    Invariant {
        id: "journal.complete-corruption-fails-closed",
        primary_test: "complete_frame_corruption_fails_closed",
    },
    Invariant {
        id: "host.codex.actual-binary",
        primary_test: "actual_codex_binary_executes_the_scripted_scenario",
    },
    Invariant {
        id: "host.claude.actual-binary",
        primary_test: "actual_claude_binary_executes_the_scripted_scenario",
    },
];

pub const AUTHORITATIVE_TEST_SOURCES: &[&str] = &[
    include_str!("../e2e.rs"),
    include_str!("../../src/main.rs"),
    include_str!("../../../rust/crates/filesystem/src/git_compat.rs"),
    include_str!("../../../rust/crates/filesystem/src/workspace_context.rs"),
    include_str!("../../../rust/crates/filesystem/tests/merge_lineage.rs"),
    include_str!("../../../rust/crates/native-runtime/src/process_tree.rs"),
    include_str!("../../../rust/crates/stream/src/local.rs"),
];
