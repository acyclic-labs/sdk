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
        id: "host.codex.actual-binary",
        primary_test: "actual_codex_binary_executes_the_scripted_scenario",
    },
    Invariant {
        id: "host.claude.actual-binary",
        primary_test: "actual_claude_binary_executes_the_scripted_scenario",
    },
];
