//! Production-boundary qualification for the packaged Acyclic plugin.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "integration-test helpers outside #[test] functions also unwrap fixtures"
)]

mod support;

use serde_json::Value;
use std::fs;
#[cfg(windows)]
use std::os::windows::process::CommandExt as _;
use std::path::Path;
use std::time::Duration;
use support::{
    BoundedOutput, PackagedPlugin, RequestFingerprint, ScriptedProvider, ServiceGuard, command,
    installed_host_binary, isolated_state, output_after_provider_admission, output_with_timeout,
    package_production_plugin, test_tempdir, write_qualification_receipt,
};

const STALLED_PROVIDER_OBSERVATION: Duration = Duration::from_millis(250);

#[test]
fn native_compiler_runs_after_home_and_rustup_state_are_isolated() {
    let directory = tempfile::tempdir().expect("compiler fixture");
    let mut compiler = command("rustc");
    isolated_state(&mut compiler, directory.path());
    compiler.env("RUSTUP_HOME", directory.path().join("empty-rustup"));
    compiler.env_remove("RUSTUP_TOOLCHAIN");
    support::bind_native_compiler(&mut compiler);
    let output = compiler
        .arg("--version")
        .output()
        .expect("execute native compiler");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).starts_with("rustc "));
    assert!(!directory.path().join("empty-rustup").exists());
}

#[test]
fn owner_fixture_queries_task_files_and_drains_after_success_or_unwind() {
    for unwind in [false, true] {
        let temporary = test_tempdir("provider-owner-");
        let workspace = temporary.path().join("workspace");
        fs::create_dir(&workspace).expect("fixture workspace");
        let outcome = std::panic::catch_unwind(|| {
            let mut owner = ServiceGuard::new(temporary.path());
            owner.assert_hook_service_live();
            let mut attach = command(support::ACYCLIC);
            attach
                .args(["__hook", "pi", "session_start"])
                .current_dir(&workspace);
            isolated_state(&mut attach, temporary.path());
            let input = serde_json::to_vec(&serde_json::json!({
                "sessionId":"qualification-owner", "cwd":workspace,
            }))
            .expect("fixture attachment");
            let attached =
                support::output_with_stdin_timeout(&mut attach, &input, Duration::from_secs(3));
            assert!(!attached.expired);
            assert!(
                attached.output.status.success(),
                "{}",
                String::from_utf8_lossy(&attached.output.stderr)
            );
            owner.attach_process_tree(attached.process_tree);
            owner.assert_requests_settled(&workspace);
            if unwind {
                panic!("injected qualification unwind");
            }
            owner.drain();
        });
        assert_eq!(outcome.is_err(), unwind);
        support::assert_service_absent(temporary.path()).expect("owner lock released after drain");
    }
}

#[test]
#[ignore = "requires the exact Codex binary selected by the qualification workflow"]
fn actual_codex_binary_executes_the_scripted_scenario() {
    let Some(codex) = installed_host_binary("codex", "ACYCLIC_E2E_CODEX") else {
        panic!("Codex is unavailable; set ACYCLIC_E2E_CODEX to the exact binary");
    };
    let temporary = test_tempdir("codex-");
    let workspace = temporary.path().join("workspace");
    fs::create_dir(&workspace).expect("workspace directory");
    write_overlay_workflow_source(&workspace);
    let package = package_production_plugin(temporary.path());

    let mut service = install_host(&package, "codex", &codex, temporary.path());
    let provider = ScriptedProvider::start_codex(
        workspace.join("codex-e2e.txt").to_string_lossy().as_ref(),
        "codex-child-isolation.txt",
    );
    let mut host = codex_host_command(&codex, temporary.path(), &workspace, &provider);
    let mut path = vec![npm_command_directory(temporary.path(), &package.native)];
    if let Some(inherited) = std::env::var_os("PATH") {
        path.extend(std::env::split_paths(&inherited));
    }
    host.env(
        "PATH",
        std::env::join_paths(path).expect("packaged Acyclic PATH"),
    );
    support::bind_native_compiler(&mut host);
    let BoundedOutput {
        output,
        expired,
        process_tree,
    } = output_with_timeout(&mut host, Duration::from_secs(60));
    service.attach_process_tree(process_tree);
    let installed_config = fs::read_to_string(temporary.path().join("codex/config.toml"))
        .unwrap_or_else(|error| format!("<unreadable Codex config: {error}>"));
    let installed_debug = format!(
        "{installed_config}\nprovider fingerprints: {:#?}\nprovider tool results: {:#?}",
        provider.semantic_fingerprints(0, Duration::ZERO),
        provider.tool_results(),
    );
    assert_host_success("installed Codex", &output, expired, &installed_debug);
    assert_host_sentinel("installed Codex", &workspace, &output, &installed_debug);
    let child_ran = assert_semantic_provider_exchange(&provider, &output, &installed_debug);
    service.assert_hook_service_live();
    assert_native_hook_lifecycle(&service.assert_requests_settled(&workspace));
    assert_overlay_workflow_stayed_in_child(&workspace, "codex-child-isolation.txt");

    service.drain();
    assert_codex_timeout_cleanup(&codex, temporary.path(), &workspace);
    write_qualification_receipt(
        "codex",
        &codex,
        &[
            "host.codex.actual-binary",
            "host.codex.hook-service-observed",
            "routing.root-cwd",
            if child_ran {
                "routing.child-native-mount"
            } else {
                "routing.child-failed-closed"
            },
        ],
    );
}

// npm links the package's `acyclic-plugin` command to the native executable.
fn npm_command_directory(temporary: &Path, native: &Path) -> std::path::PathBuf {
    let commands = temporary.join("command-bin");
    fs::create_dir_all(&commands).expect("command directory");
    #[cfg(unix)]
    std::os::unix::fs::symlink(native, commands.join("acyclic-plugin"))
        .expect("acyclic-plugin command");
    #[cfg(windows)]
    fs::hard_link(native, commands.join("acyclic-plugin.exe")).expect("acyclic-plugin command");
    commands
}

#[test]
#[ignore = "requires the exact Claude Code binary selected by the qualification workflow"]
#[allow(
    clippy::too_many_lines,
    reason = "one host qualification validates the ordered Claude lifecycle transcript"
)]
fn actual_claude_binary_executes_the_scripted_scenario() {
    let Some(claude) = installed_host_binary("claude", "ACYCLIC_E2E_CLAUDE") else {
        panic!("Claude Code is unavailable; set ACYCLIC_E2E_CLAUDE to the exact binary");
    };
    let temporary = test_tempdir("claude-");
    let workspace = temporary.path().join("workspace");
    fs::create_dir(&workspace).expect("workspace directory");
    write_overlay_workflow_source(&workspace);
    let package = package_production_plugin(temporary.path());

    let mut service = install_host(&package, "claude-code", &claude, temporary.path());
    let provider = ScriptedProvider::start_claude_lifecycle(
        workspace.join("claude-e2e.txt").to_string_lossy().as_ref(),
        "claude-child-isolation.txt",
    );
    let debug_log = temporary.path().join("claude-debug.log");
    let mut host =
        claude_host_command(&claude, temporary.path(), &workspace, &provider, &debug_log);
    let BoundedOutput {
        output,
        expired,
        process_tree,
    } = output_with_timeout(&mut host, Duration::from_secs(30));
    service.attach_process_tree(process_tree);
    let debug = format!(
        "{}\nprovider tool results: {:#?}",
        fs::read_to_string(&debug_log).unwrap_or_default(),
        provider.tool_results(),
    );
    assert_host_success("installed Claude", &output, expired, &debug);
    let sentinel = fs::read_to_string(workspace.join("claude-e2e.txt"));
    assert!(
        sentinel
            .as_deref()
            .is_ok_and(|value| value.trim() == "qualified"),
        "sentinel: {sentinel:?}\nrequest count: {}\nstdout:\n{}\nstderr:\n{}\ndebug:\n{}",
        provider.wait_for_requests(0, Duration::ZERO).len(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        debug
    );
    service.assert_hook_service_live();
    assert_native_hook_lifecycle(&service.assert_requests_settled(&workspace));
    assert_overlay_workflow_stayed_in_child(&workspace, "claude-child-isolation.txt");
    let requests = provider.wait_for_requests(0, Duration::ZERO);
    let fingerprints = provider.semantic_fingerprints(0, Duration::ZERO);
    let request_text = requests.iter().map(Value::to_string).collect::<String>();
    let spawn_denied =
        request_text.contains("Acyclic denied the tool because workspace isolation failed");
    assert_fingerprint_before(
        &fingerprints,
        RequestFingerprint::ClaudeRootPrompt,
        RequestFingerprint::ClaudeRootToolResult,
    );
    let transcript = String::from_utf8_lossy(&output.stdout);
    let leaked_lease = debug.contains("filesystem tools are still active");
    assert!(!leaked_lease, "Claude leaked a filesystem operation lease");
    if spawn_denied {
        assert_claude_isolation_denial(&fingerprints, &transcript);
    } else {
        assert_claude_successful_lifecycle(&fingerprints, &request_text, &transcript);
    }

    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _service = service;
        panic!("injected host assertion failure");
    }));
    assert!(unwind.is_err(), "injected unwind must execute");
    assert_claude_timeout_cleanup(&claude, temporary.path(), &workspace);
    write_qualification_receipt(
        "claude-code",
        &claude,
        &[
            "host.claude.actual-binary",
            "host.claude.hook-service-observed",
            "routing.root-cwd",
        ],
    );
}

fn assert_native_hook_lifecycle(requests: &[acyclic_harness::external_executor::ExternalRequest]) {
    use acyclic_harness::external_executor::ExternalAction;
    let mut seen = [false; 5];
    for request in requests {
        seen[match request.action {
            ExternalAction::Attach { .. } => 0,
            ExternalAction::RunTool { .. } | ExternalAction::HostTool { .. } => 1,
            ExternalAction::ToolFinished { .. } => 2,
            ExternalAction::Fork { .. } => 3,
            ExternalAction::Join { .. } => 4,
            _ => continue,
        }] = true;
    }
    for (index, name) in ["attach", "tool call", "tool result", "fork", "join"]
        .into_iter()
        .enumerate()
    {
        assert!(
            seen[index],
            "native host did not publish its {name} request: {requests:?}"
        );
    }
}

fn assert_claude_isolation_denial(fingerprints: &[RequestFingerprint], transcript: &str) {
    assert!(
        fingerprints.iter().all(|fingerprint| matches!(
            fingerprint,
            RequestFingerprint::ClaudeRootPrompt
                | RequestFingerprint::ClaudeRootContinuation
                | RequestFingerprint::ClaudeRootToolResult
        )),
        "a denied spawn must not start scripted child conversations: {fingerprints:?}"
    );
    assert!(
        transcript.contains("permissionDecision") && transcript.contains("deny"),
        "Claude did not expose the structured fail-closed hook decision"
    );
    assert!(
        !transcript.contains("\"hook_event\":\"SubagentStart\"")
            && !transcript.contains("\"hook_event\": \"SubagentStart\""),
        "an isolation-denied spawn unexpectedly started a subagent"
    );
}

fn assert_claude_successful_lifecycle(
    fingerprints: &[RequestFingerprint],
    request_text: &str,
    transcript: &str,
) {
    for (earlier, later) in [
        (
            RequestFingerprint::ClaudeRootPrompt,
            RequestFingerprint::ClaudeSuccessChildPrompt,
        ),
        (
            RequestFingerprint::ClaudeSuccessChildPrompt,
            RequestFingerprint::ClaudeSuccessChildResult,
        ),
        (
            RequestFingerprint::ClaudeSuccessChildResult,
            RequestFingerprint::ClaudeFailureChildPrompt,
        ),
        (
            RequestFingerprint::ClaudeRootToolResult,
            RequestFingerprint::ClaudeFailureChildPrompt,
        ),
        (
            RequestFingerprint::ClaudeFailureChildPrompt,
            RequestFingerprint::ClaudeFailureChildResult,
        ),
        (
            RequestFingerprint::ClaudeFailureChildResult,
            RequestFingerprint::ClaudeRootContinuation,
        ),
        (
            RequestFingerprint::ClaudeRootFailedChildResult,
            RequestFingerprint::ClaudeRootContinuation,
        ),
    ] {
        assert_fingerprint_before(fingerprints, earlier, later);
    }
    let continuation_count = fingerprints
        .iter()
        .filter(|fingerprint| **fingerprint == RequestFingerprint::ClaudeRootContinuation)
        .count();
    assert!(
        (1..=2).contains(&continuation_count),
        "Claude emitted {continuation_count} hook-driven continuation turns"
    );
    let mut observed = fingerprints
        .iter()
        .copied()
        .filter(|fingerprint| *fingerprint != RequestFingerprint::ClaudeRootContinuation)
        .collect::<Vec<_>>();
    observed.sort_unstable();
    let mut expected = vec![
        RequestFingerprint::ClaudeRootPrompt,
        RequestFingerprint::ClaudeRootToolResult,
        RequestFingerprint::ClaudeRootFailedChildResult,
        RequestFingerprint::ClaudeSuccessChildPrompt,
        RequestFingerprint::ClaudeSuccessChildResult,
        RequestFingerprint::ClaudeFailureChildPrompt,
        RequestFingerprint::ClaudeFailureChildResult,
    ];
    expected.sort_unstable();
    assert_eq!(observed, expected, "Claude lifecycle schedule drifted");
    assert!(
        request_text.contains("ACYCLIC_SUCCESS_CHILD")
            && request_text.contains("ACYCLIC_FAILURE_CHILD")
            && request_text.contains("\"is_error\":true"),
        "Claude did not complete both child exchanges"
    );
    let subagent_starts = transcript
        .matches("\"hook_event\":\"SubagentStart\"")
        .count()
        + transcript
            .matches("\"hook_event\": \"SubagentStart\"")
            .count();
    assert!(
        subagent_starts >= 2
            && transcript.contains("PostToolUse")
            && transcript.contains("updatedInput")
            && transcript.contains("claude-child-isolation.txt")
            && transcript.contains("PostToolUseFailure"),
        "Claude lifecycle transcript lacked required structured evidence"
    );
}

fn write_overlay_workflow_source(workspace: &Path) {
    fs::write(
        workspace.join("acyclic-workflow.rs"),
        include_bytes!("fixtures/overlay_workflow.rs"),
    )
    .expect("overlay workflow source");
}
fn assert_overlay_workflow_stayed_in_child(workspace: &Path, output: &str) {
    assert!(
        !workspace.join(output).exists(),
        "child relative write escaped into the physical root"
    );
    assert!(
        !workspace.join("acyclic-workflow-bin").exists()
            && !workspace.join("acyclic-workflow-bin.exe").exists(),
        "child compiler output escaped into the physical root"
    );
    assert!(
        !(0..2).any(|server| workspace
            .join(format!(".acyclic-workflow-child-{server}"))
            .exists()),
        "workflow subprocess wrote through the physical root"
    );
}

fn assert_fingerprint_before(
    fingerprints: &[RequestFingerprint],
    earlier: RequestFingerprint,
    later: RequestFingerprint,
) {
    let position = |fingerprint| {
        fingerprints
            .iter()
            .position(|candidate| *candidate == fingerprint)
            .unwrap_or_else(|| panic!("missing semantic provider phase {fingerprint:?}"))
    };
    assert!(
        position(earlier) < position(later),
        "semantic provider phase {earlier:?} must precede {later:?}: {fingerprints:?}"
    );
}

fn assert_codex_timeout_cleanup(binary: &Path, home: &Path, workspace: &Path) {
    let provider = ScriptedProvider::start_stalled();
    let host = codex_host_command(binary, home, workspace, &provider);
    assert_host_timeout_cleanup("Codex", home, &provider, host);
}

fn assert_claude_timeout_cleanup(binary: &Path, home: &Path, workspace: &Path) {
    let provider = ScriptedProvider::start_stalled();
    let debug = home.join("claude-timeout-debug.log");
    let host = claude_host_command(binary, home, workspace, &provider, &debug);
    assert_host_timeout_cleanup("Claude", home, &provider, host);
}

fn assert_host_timeout_cleanup(
    host_name: &str,
    home: &Path,
    provider: &ScriptedProvider,
    mut host: std::process::Command,
) {
    // The lifecycle run installed the integration; a fresh host proves it can start a new service.
    let service = ServiceGuard::new(home);
    let (
        BoundedOutput {
            output,
            expired,
            process_tree,
        },
        admitted,
    ) = output_after_provider_admission(
        &mut host,
        provider,
        Duration::from_secs(30),
        STALLED_PROVIDER_OBSERVATION,
    );
    assert!(
        admitted,
        "{host_name} did not reach the stalled provider\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        expired,
        "stalled {host_name} must hit the process-tree deadline"
    );
    service.assert_timeout_cleanup(process_tree);
}

fn codex_host_command(
    binary: &Path,
    home: &Path,
    workspace: &Path,
    provider: &ScriptedProvider,
) -> std::process::Command {
    let mut host = command(binary);
    host.current_dir(workspace);
    host.args([
        "exec",
        "--json",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--dangerously-bypass-approvals-and-sandbox",
        "--dangerously-bypass-hook-trust",
        "--disable",
        "remote_plugin",
        "--disable",
        "recommended_plugins",
        "--disable",
        "plugin_sharing",
        "--enable",
        "hooks",
        "-c",
        "model_provider=\"acyclic_e2e\"",
        "-c",
        "model=\"gpt-5.6-sol\"",
        "-c",
        "model_providers.acyclic_e2e.name=\"Acyclic E2E\"",
        "-c",
        &format!(
            "model_providers.acyclic_e2e.base_url=\"{}/v1\"",
            provider.base_url()
        ),
        "-c",
        "model_providers.acyclic_e2e.wire_api=\"responses\"",
        "-c",
        "model_providers.acyclic_e2e.env_key=\"ACYCLIC_E2E_API_KEY\"",
    ]);
    host.arg("Run the deterministic qualification command.");
    host.env("ACYCLIC_E2E_API_KEY", "test");
    host.env("ACYCLIC_WORKFLOW_TOKEN", "qualified");
    isolated_codex_state(&mut host, home);
    host
}

fn claude_host_command(
    binary: &Path,
    home: &Path,
    workspace: &Path,
    provider: &ScriptedProvider,
    debug_log: &Path,
) -> std::process::Command {
    let mut host = command(binary);
    host.current_dir(workspace).args([
        "-p",
        "Run the deterministic qualification command.",
        "--output-format",
        "stream-json",
        "--verbose",
        "--include-hook-events",
        "--max-turns",
        "3",
        "--permission-mode",
        "bypassPermissions",
        "--permission-prompts",
        "none",
        "--debug",
        "hooks",
        "--debug-file",
    ]);
    host.arg(debug_log).args(["--setting-sources", "user"]);
    host.env("ANTHROPIC_API_KEY", "test");
    host.env("ANTHROPIC_BASE_URL", provider.base_url());
    host.env("ACYCLIC_WORKFLOW_TOKEN", "qualified");
    isolated_state(&mut host, home);
    support::bind_native_compiler(&mut host);
    host
}

fn assert_host_success(name: &str, output: &std::process::Output, expired: bool, debug: &str) {
    assert!(
        output.status.success() && !expired,
        "{name} expired={expired}\nstdout:\n{}\nstderr:\n{}\ndebug:\n{debug}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn assert_host_sentinel(name: &str, workspace: &Path, output: &std::process::Output, debug: &str) {
    let sentinel = fs::read_to_string(workspace.join("codex-e2e.txt"));
    assert!(
        sentinel
            .as_deref()
            .is_ok_and(|value| value.trim() == "qualified"),
        "{name} did not produce its sentinel: {sentinel:?}\nstdout:\n{}\nstderr:\n{}\ndebug:\n{debug}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn install_host(
    package: &PackagedPlugin,
    host: &str,
    host_binary: &Path,
    home: &Path,
) -> ServiceGuard {
    let mut install = package.command(&["install", host]);
    prepend_binary_directory(&mut install, host_binary);
    isolated_state(&mut install, home);
    if host == "codex" {
        preserve_windows_profile_identity(&mut install);
    }
    let output = acyclic_native_runtime::process_output(
        &mut install,
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )
    .expect("install host integration");
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    ServiceGuard::new(home)
}

fn isolated_codex_state(command: &mut std::process::Command, root: &Path) {
    isolated_state(command, root);
    command.env_remove("CODEX_PERMISSION_PROFILE");
    preserve_windows_profile_identity(command);
}

fn preserve_windows_profile_identity(command: &mut std::process::Command) {
    if cfg!(windows) {
        for name in ["HOME", "USERPROFILE"] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
    }
}

fn prepend_binary_directory(command: &mut std::process::Command, binary: &Path) {
    let mut paths = vec![binary.parent().expect("host binary parent").to_path_buf()];
    if let Some(existing) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&existing));
    }
    command.env("PATH", std::env::join_paths(paths).expect("host PATH"));
}

fn assert_semantic_provider_exchange(
    provider: &ScriptedProvider,
    output: &std::process::Output,
    debug: &str,
) -> bool {
    let fingerprints = provider.semantic_fingerprints(5, Duration::from_millis(250));
    let mut observed = fingerprints.clone();
    observed.sort_unstable();
    let mut success = vec![
        RequestFingerprint::CodexRootPrompt,
        RequestFingerprint::CodexRootToolResult,
        RequestFingerprint::CodexRootSpawnResult,
        RequestFingerprint::CodexRootWaitResult,
        RequestFingerprint::CodexChildPrompt,
        RequestFingerprint::CodexChildSearchResult,
        RequestFingerprint::CodexChildToolResult,
    ];
    success.sort_unstable();
    let mut denied = vec![
        RequestFingerprint::CodexRootPrompt,
        RequestFingerprint::CodexRootToolResult,
        RequestFingerprint::CodexRootSpawnResult,
    ];
    denied.sort_unstable();
    assert!(
        observed == success || observed == denied,
        "unexpected Codex request phases: {fingerprints:?}; tool results: {:#?}\nstdout:\n{}\nstderr:\n{}\ninstalled configuration:\n{debug}",
        provider.tool_results(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert_fingerprint_before(
        &fingerprints,
        RequestFingerprint::CodexRootPrompt,
        RequestFingerprint::CodexRootToolResult,
    );
    assert_fingerprint_before(
        &fingerprints,
        RequestFingerprint::CodexRootToolResult,
        RequestFingerprint::CodexRootSpawnResult,
    );
    if observed == success {
        assert_fingerprint_before(
            &fingerprints,
            RequestFingerprint::CodexChildToolResult,
            RequestFingerprint::CodexRootWaitResult,
        );
        assert_fingerprint_before(
            &fingerprints,
            RequestFingerprint::CodexChildPrompt,
            RequestFingerprint::CodexChildToolResult,
        );
        true
    } else {
        false
    }
}
