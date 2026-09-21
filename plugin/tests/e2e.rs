//! Production-boundary qualification for the packaged Acyclic plugin.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::permissions_set_readonly_false
)]

mod scenarios;
mod support;

use scenarios::INVARIANTS;
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::time::Duration;
use support::{
    ACYCLIC, ProviderProtocol, ScriptedProvider, ServiceGuard, command, installed_host_binary,
    isolated_state, make_read_only, make_writable, output_with_stdin, output_with_timeout,
    package_production_plugin, write_qualification_receipt,
};

#[test]
fn invariant_ledger_has_one_authoritative_test_per_requirement() {
    let mut identifiers = BTreeSet::new();
    let mut primary_tests = BTreeSet::new();
    for invariant in INVARIANTS {
        assert!(
            identifiers.insert(invariant.id),
            "duplicate invariant {}",
            invariant.id
        );
        assert!(
            primary_tests.insert(invariant.primary_test),
            "{} is not an authoritative one-to-one test",
            invariant.primary_test
        );
    }
}

#[test]
fn repository_has_one_canonical_plugin_root() {
    let plugin = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repository = plugin.parent().expect("repository root");
    assert_eq!(
        plugin.file_name().and_then(|name| name.to_str()),
        Some("plugin")
    );
    assert!(!repository.join("plugins/acyclic").exists());
}

#[test]
fn codex_hook_manifest_respects_terminal_deadline() {
    let manifest: Value =
        serde_json::from_slice(include_bytes!("../hooks/hooks.json")).expect("valid hook manifest");
    let terminal = manifest
        .pointer("/hooks/SessionEnd/0/hooks/0")
        .expect("SessionEnd hook");
    assert_eq!(terminal.get("timeout").and_then(Value::as_u64), Some(3));
    assert!(
        terminal["command"]
            .as_str()
            .is_some_and(|command| command.contains("${PLUGIN_ROOT}/bin/acyclic.js"))
    );
}

#[test]
fn pre_tool_failure_uses_the_host_blocking_exit_code() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let unusable_state = temporary.path().join("not-a-directory");
    fs::write(&unusable_state, b"file").expect("unusable state root");
    let mut hook = command(ACYCLIC);
    hook.args(["__hook", "codex", "PreToolUse"]);
    isolated_state(&mut hook, temporary.path());
    if cfg!(windows) {
        hook.env("LOCALAPPDATA", &unusable_state);
    } else {
        hook.env("XDG_STATE_HOME", &unusable_state);
    }
    let output = output_with_stdin(&mut hook, include_bytes!("fixtures/pre_tool_use.json"));
    assert_eq!(
        output.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut non_blocking = command(ACYCLIC);
    non_blocking.args(["__hook", "codex", "SessionStart"]);
    isolated_state(&mut non_blocking, temporary.path());
    if cfg!(windows) {
        non_blocking.env("LOCALAPPDATA", &unusable_state);
    } else {
        non_blocking.env("XDG_STATE_HOME", &unusable_state);
    }
    let output = output_with_stdin(
        &mut non_blocking,
        include_bytes!("fixtures/session_start.json"),
    );
    assert_eq!(output.status.code(), Some(1));
}

#[test]
fn packaged_launcher_is_read_only() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let package = package_production_plugin(temporary.path());
    let before = support::tree_snapshot(&package.root);
    make_read_only(&package.root);
    let mut launch = command("node");
    launch.arg(&package.launcher).arg("--version");
    isolated_state(&mut launch, temporary.path());
    let launched = launch.output().expect("launch immutable package");
    make_writable(&package.root);
    assert!(
        launched.status.success(),
        "{}",
        String::from_utf8_lossy(&launched.stderr)
    );
    assert_eq!(support::tree_snapshot(&package.root), before);
}

#[test]
#[ignore = "requires the exact Codex binary selected by the qualification workflow"]
fn actual_codex_binary_executes_the_scripted_scenario() {
    let Some(codex) = installed_host_binary("codex", "ACYCLIC_E2E_CODEX") else {
        panic!("Codex is unavailable; set ACYCLIC_E2E_CODEX to the exact binary");
    };
    let temporary = tempfile::tempdir().expect("temporary directory");
    let workspace = temporary.path().join("workspace");
    fs::create_dir(&workspace).expect("workspace directory");
    let package = package_production_plugin(temporary.path());
    let service = install_host(&package.launcher, "codex", &codex, temporary.path());
    let provider =
        ScriptedProvider::start(ProviderProtocol::Responses, shell_write("codex-e2e.txt"));
    let mut host = command(&codex);
    host.current_dir(&workspace).args([
        "exec",
        "--json",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--dangerously-bypass-approvals-and-sandbox",
        "--dangerously-bypass-hook-trust",
        "-c",
        "model_provider=\"acyclic_e2e\"",
        "-c",
        "model=\"stub-model\"",
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
        "Run the deterministic qualification command.",
    ]);
    host.env("ACYCLIC_E2E_API_KEY", "test");
    isolated_state(&mut host, temporary.path());
    let (output, expired) = output_with_timeout(&mut host, Duration::from_secs(30));
    assert!(
        output.status.success() && !expired,
        "expired: {expired}\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(workspace.join("codex-e2e.txt"))
            .expect("Codex qualification sentinel")
            .trim(),
        "qualified"
    );
    assert_semantic_provider_exchange(&provider);
    service.drain();
    write_qualification_receipt(
        "codex",
        &codex,
        &[
            "host.codex.actual-binary",
            "host.codex.hooks",
            "routing.root-cwd",
        ],
    );
}

#[test]
#[ignore = "requires the exact Claude Code binary selected by the qualification workflow"]
fn actual_claude_binary_executes_the_scripted_scenario() {
    let Some(claude) = installed_host_binary("claude", "ACYCLIC_E2E_CLAUDE") else {
        panic!("Claude Code is unavailable; set ACYCLIC_E2E_CLAUDE to the exact binary");
    };
    let temporary = tempfile::tempdir().expect("temporary directory");
    let workspace = temporary.path().join("workspace");
    fs::create_dir(&workspace).expect("workspace directory");
    let package = package_production_plugin(temporary.path());
    let service = install_host(&package.launcher, "claude-code", &claude, temporary.path());
    let provider = ScriptedProvider::start(
        ProviderProtocol::AnthropicMessages,
        workspace.join("claude-e2e.txt").to_string_lossy().as_ref(),
    );
    let debug_log = temporary.path().join("claude-debug.log");
    let mut host = command(&claude);
    host.current_dir(&workspace).args([
        "-p",
        "Run the deterministic qualification command.",
        "--output-format",
        "stream-json",
        "--verbose",
        "--include-hook-events",
        "--no-session-persistence",
        "--max-turns",
        "3",
        "--permission-mode",
        "bypassPermissions",
        "--permission-prompts",
        "none",
        "--debug-file",
    ]);
    host.arg(&debug_log).args(["--setting-sources", "user"]);
    host.env("ANTHROPIC_API_KEY", "test");
    host.env("ANTHROPIC_BASE_URL", provider.base_url());
    isolated_state(&mut host, temporary.path());
    let (output, expired) = output_with_timeout(&mut host, Duration::from_secs(30));
    let debug = fs::read_to_string(&debug_log).unwrap_or_default();
    assert!(
        output.status.success() && !expired,
        "expired: {expired}\nrequest count: {}\nstdout:\n{}\nstderr:\n{}\ndebug:\n{}",
        provider.wait_for_requests(0, Duration::ZERO).len(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        debug
    );
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
    assert_semantic_provider_exchange(&provider);
    service.drain();
    write_qualification_receipt(
        "claude-code",
        &claude,
        &[
            "host.claude.actual-binary",
            "host.claude.hooks",
            "routing.root-cwd",
        ],
    );
}

fn install_host(launcher: &Path, host: &str, host_binary: &Path, home: &Path) -> ServiceGuard {
    let mut install = command("node");
    install.arg(launcher).args(["install", host]);
    prepend_binary_directory(&mut install, host_binary);
    isolated_state(&mut install, home);
    let output = install.output().expect("install host integration");
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    ServiceGuard::new(launcher, home)
}

fn prepend_binary_directory(command: &mut std::process::Command, binary: &Path) {
    let mut paths = vec![binary.parent().expect("host binary parent").to_path_buf()];
    if let Some(existing) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&existing));
    }
    command.env("PATH", std::env::join_paths(paths).expect("host PATH"));
}

fn shell_write(path: &str) -> &'static str {
    match (std::env::consts::OS, path) {
        ("windows", "codex-e2e.txt") => "Set-Content -LiteralPath codex-e2e.txt -Value qualified",
        (_, "codex-e2e.txt") => "printf qualified > codex-e2e.txt",
        _ => panic!("unsupported qualification sentinel"),
    }
}

fn assert_semantic_provider_exchange(provider: &ScriptedProvider) {
    let requests = provider.wait_for_requests(2, Duration::from_secs(5));
    assert_eq!(requests.len(), 2, "expected tool and completion requests");
    assert!(requests.iter().any(|request| {
        request.to_string().contains("function_call_output")
            || request.to_string().contains("tool_result")
    }));
    assert!(requests.iter().any(|request| {
        !request.to_string().contains("function_call_output")
            && !request.to_string().contains("tool_result")
    }));
}
