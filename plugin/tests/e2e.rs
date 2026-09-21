//! Production-boundary qualification for the packaged Acyclic plugin.

#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::permissions_set_readonly_false
)]

mod scenarios;
mod support;

use scenarios::{AUTHORITATIVE_TEST_SOURCES, INVARIANTS};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use support::{
    ACYCLIC, BoundedOutput, ProviderProtocol, ScriptedProvider, ServiceGuard,
    assert_service_absent, command, installed_host_binary, isolated_state, make_read_only,
    make_writable, output_after_provider_admission, output_with_stdin, output_with_timeout,
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
        let declaration = format!("fn {}", invariant.primary_test);
        assert!(
            AUTHORITATIVE_TEST_SOURCES
                .iter()
                .any(|source| source.contains(&declaration)),
            "{} does not name a checked-in test function",
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
    let run_root = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .map(PathBuf::from)
        .expect("host home")
        .join(".cache/acyclic-agent-qualification");
    fs::create_dir_all(&run_root).expect("qualification run root");
    let temporary = tempfile::Builder::new()
        .prefix("run-")
        .tempdir_in(run_root)
        .expect("temporary qualification directory");
    let workspace = temporary.path().join("workspace");
    fs::create_dir(&workspace).expect("workspace directory");
    let package = package_production_plugin(temporary.path());
    let disabled_workspace = temporary.path().join("disabled-workspace");
    fs::create_dir(&disabled_workspace).expect("disabled workspace directory");
    let disabled_provider = ScriptedProvider::start(
        ProviderProtocol::Responses,
        disabled_workspace
            .join("codex-e2e.txt")
            .to_string_lossy()
            .as_ref(),
    );
    let mut disabled_host = codex_host_command(
        &codex,
        temporary.path(),
        &disabled_workspace,
        &disabled_provider,
        false,
    );
    let BoundedOutput {
        output,
        expired,
        process_tree,
    } = output_with_timeout(&mut disabled_host, Duration::from_secs(30));
    drop(process_tree);
    assert_host_success("disabled Codex", &output, expired, "");
    assert_host_sentinel("disabled Codex", &disabled_workspace, &output, "");
    assert_semantic_provider_exchange(&disabled_provider);
    assert_service_absent(&package.launcher, temporary.path())
        .expect("disabled Codex must not start Acyclic");

    let mut service = install_host(&package.launcher, "codex", &codex, temporary.path());
    let provider = ScriptedProvider::start(
        ProviderProtocol::Responses,
        workspace.join("codex-e2e.txt").to_string_lossy().as_ref(),
    );
    let mut host = codex_host_command(&codex, temporary.path(), &workspace, &provider, true);
    let BoundedOutput {
        output,
        expired,
        process_tree,
    } = output_with_timeout(&mut host, Duration::from_secs(30));
    service.attach_process_tree(process_tree);
    assert_host_success("installed Codex", &output, expired, "");
    let installed_config = fs::read_to_string(temporary.path().join("codex/config.toml"))
        .unwrap_or_else(|error| format!("<unreadable Codex config: {error}>"));
    let installed_debug = format!(
        "{installed_config}\nprovider requests: {:#?}",
        provider.wait_for_requests(0, Duration::ZERO)
    );
    assert_host_sentinel("installed Codex", &workspace, &output, &installed_debug);
    assert_semantic_provider_exchange(&provider);
    service.assert_hook_service_live();

    service.drain();
    assert_codex_timeout_cleanup(&package.launcher, &codex, temporary.path(), &workspace);
    write_qualification_receipt(
        "codex",
        &codex,
        &[
            "host.codex.actual-binary",
            "host.codex.hook-service-observed",
            "routing.root-cwd",
        ],
    );
}

#[test]
#[ignore = "local authenticated Codex eval; intentionally excluded from CI"]
fn local_codex_adversarial_workspace_eval() {
    assert_eq!(
        std::env::var("ACYCLIC_LOCAL_EVAL").as_deref(),
        Ok("1"),
        "set ACYCLIC_LOCAL_EVAL=1 to acknowledge a real authenticated model run"
    );
    let codex = installed_host_binary("codex", "ACYCLIC_E2E_CODEX")
        .expect("Codex is unavailable; set ACYCLIC_E2E_CODEX to its executable");
    let source_home = local_codex_source_home();
    let run_root = source_home
        .parent()
        .expect("Codex home parent")
        .join(".cache/acyclic-local-eval-runs");
    fs::create_dir_all(&run_root).expect("local eval run directory");
    let temporary = tempfile::Builder::new()
        .prefix("run-")
        .tempdir_in(run_root)
        .expect("local eval temporary directory");
    let workspace = temporary.path().join("workspace");
    fs::create_dir(&workspace).expect("workspace directory");
    fs::write(workspace.join("contract.txt"), "version=1\n").expect("initial contract");
    fs::write(
        workspace.join("README.eval.md"),
        "This directory is an Acyclic local evaluation fixture. Do not write outside it.\n",
    )
    .expect("eval fixture readme");
    let protected = temporary.path().join("outside-protected.txt");
    fs::write(&protected, "unchanged\n").expect("protected sentinel");

    let package = package_production_plugin(temporary.path());
    copy_codex_auth(&source_home, temporary.path());
    install_codex_plugin(&codex, temporary.path(), &package.root);

    let mut host = command(&codex);
    host.current_dir(&workspace).args([
        "exec",
        "--json",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--dangerously-bypass-hook-trust",
        "-c",
        "features.multi_agent=true",
    ]);
    host.arg(LOCAL_CODEX_ADVERSARIAL_PROMPT);
    isolated_codex_state(&mut host, temporary.path());
    let mut path = vec![package.root.join("bin")];
    if let Some(inherited) = std::env::var_os("PATH") {
        path.extend(std::env::split_paths(&inherited));
    }
    host.env(
        "PATH",
        std::env::join_paths(path).expect("packaged Acyclic PATH"),
    );
    let BoundedOutput {
        output,
        expired,
        process_tree,
    } = output_with_timeout(&mut host, Duration::from_secs(600));
    let artifact =
        persist_local_eval_artifacts("codex", &output.stdout, &output.stderr, temporary.path());
    let mut service = ServiceGuard::new(&package.launcher, temporary.path());
    service.attach_process_tree(process_tree);
    assert!(
        !expired && output.status.success(),
        "authenticated Codex eval failed; trace: {}",
        artifact.display()
    );
    service.assert_hook_service_live();
    assert_eq!(
        fs::read_to_string(&protected).expect("protected sentinel after eval"),
        "unchanged\n",
        "Codex escaped its workspace"
    );
    assert_eq!(
        fs::read_to_string(workspace.join("accepted.txt")).expect("accepted child output"),
        "accepted\n"
    );
    assert_eq!(
        fs::read_to_string(workspace.join("speculative.txt")).expect("speculative child output"),
        "contract=version=2\n"
    );
    assert_eq!(
        fs::read_to_string(workspace.join("nested.txt")).expect("recursive child output"),
        "nested\n"
    );
    for absent in ["debug-probe.txt", "rejected.txt"] {
        assert!(
            !workspace.join(absent).exists(),
            "discarded child leaked {absent}; trace: {}",
            artifact.display()
        );
    }
    let trace = String::from_utf8_lossy(&output.stdout);
    for evidence in ["acyclic git merge", "acyclic discard"] {
        assert!(
            trace.contains(evidence),
            "Codex trace lacks {evidence:?}; trace: {}",
            artifact.display()
        );
    }
    service.drain();
    assert_service_absent(&package.launcher, temporary.path())
        .expect("authenticated Codex eval cleanup");
}

const LOCAL_CODEX_ADVERSARIAL_PROMPT: &str = r#"
This is a local Acyclic behavioural evaluation. Work only inside the current workspace. Use native
subagents and the Acyclic commands described by the installed plugin; do not simulate children with
directories or Git worktrees. Run every `acyclic ...` command as a separate shell invocation without
combining it with other commands or shell operators. Use shell builtins and ordinary core utilities,
not Python.

Complete all phases, overlapping independent work where possible:

1. Debugging: spawn a child that creates `debug-probe.txt`, reads `contract.txt`, and reports its
   value. After it reports, discard that child without merging it.
2. Exploration: spawn two children concurrently. One writes exactly `accepted\n` to `accepted.txt`;
   the other writes exactly `rejected\n` to `rejected.txt`. Merge only the accepted child and discard
   the rejected child.
3. Speculation: while `contract.txt` says version 1, spawn a child that will eventually write
   `speculative.txt` from that contract. Before accepting its work, change the root `contract.txt` to
   exactly `version=2\n`, tell the child to reconcile with the new parent state, and require its final
   file to contain exactly `contract=version=2\n`. Merge that reconciled child.
4. Recursion: spawn a child that itself spawns a grandchild. The grandchild writes exactly `nested\n`
   to `nested.txt`; it publishes to its parent, then that parent publishes to you.

Use `acyclic agents` to discover refs, `acyclic git merge agents/<ref>` only for direct children, and
`acyclic discard agents/<ref>` for unwanted trees. Wait for children before acting on their results.
Do not finish until the root contains accepted.txt, speculative.txt, and nested.txt with the exact
contents above; debug-probe.txt and rejected.txt must be absent. Run final checks yourself.
"#;

fn local_codex_source_home() -> std::path::PathBuf {
    if let Some(home) = std::env::var_os("ACYCLIC_LOCAL_CODEX_HOME") {
        return home.into();
    }
    if let Some(home) = std::env::var_os("CODEX_HOME") {
        return home.into();
    }
    let home = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .expect("set ACYCLIC_LOCAL_CODEX_HOME to an authenticated Codex home");
    std::path::PathBuf::from(home).join(".codex")
}

fn copy_codex_auth(source_home: &Path, isolated_home: &Path) {
    let source = source_home.join("auth.json");
    assert!(
        source.is_file(),
        "authenticated Codex state is missing at {}",
        source.display()
    );
    let destination = isolated_home.join("codex").join("auth.json");
    fs::create_dir_all(destination.parent().expect("auth parent"))
        .expect("isolated auth directory");
    fs::copy(source, destination).expect("copy isolated Codex authentication");
}

fn install_codex_plugin(codex: &Path, home: &Path, plugin: &Path) {
    let mut marketplace = command(codex);
    marketplace
        .args(["plugin", "marketplace", "add"])
        .arg(plugin)
        .arg("--json");
    isolated_codex_state(&mut marketplace, home);
    let output = marketplace.output().expect("add local plugin marketplace");
    assert!(
        output.status.success(),
        "marketplace installation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut install = command(codex);
    install.args(["plugin", "add", "acyclic@acyclic", "--json"]);
    isolated_codex_state(&mut install, home);
    let output = install.output().expect("install local Acyclic plugin");
    assert!(
        output.status.success(),
        "plugin installation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn persist_local_eval_artifacts(
    host: &str,
    stdout: &[u8],
    stderr: &[u8],
    isolated_home: &Path,
) -> std::path::PathBuf {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repository root")
        .join("target")
        .join(format!("local-{host}-eval"));
    if directory.exists() {
        fs::remove_dir_all(&directory).expect("replace prior local eval artifacts");
    }
    fs::create_dir_all(&directory).expect("local eval artifact directory");
    fs::write(directory.join("trace.jsonl"), stdout).expect("persist local eval trace");
    fs::write(directory.join("stderr.log"), stderr).expect("persist local eval stderr");
    let state = if cfg!(windows) {
        isolated_home.join("local/Acyclic/state-v2")
    } else {
        isolated_home.join("state/acyclic/state-v2")
    };
    copy_local_eval_tree(&state, &directory.join("state"));
    for name in ["sessions", "log"] {
        copy_local_eval_tree(
            &isolated_home.join("codex").join(name),
            &directory.join("codex").join(name),
        );
    }
    directory
}

fn copy_local_eval_tree(source: &Path, destination: &Path) {
    let Ok(entries) = fs::read_dir(source) else {
        return;
    };
    fs::create_dir_all(destination).expect("local eval diagnostic directory");
    for entry in entries {
        let entry = entry.expect("local eval diagnostic entry");
        let path = entry.path();
        let target = destination.join(entry.file_name());
        let metadata = entry.metadata().expect("local eval diagnostic metadata");
        if metadata.is_dir() {
            copy_local_eval_tree(&path, &target);
        } else if metadata.is_file() && metadata.len() <= 16 * 1024 * 1024 {
            fs::copy(path, target).expect("persist local eval diagnostic");
        }
    }
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
    let disabled_workspace = temporary.path().join("disabled-workspace");
    fs::create_dir(&disabled_workspace).expect("disabled workspace directory");
    let disabled_provider = ScriptedProvider::start(
        ProviderProtocol::AnthropicMessages,
        disabled_workspace
            .join("claude-e2e.txt")
            .to_string_lossy()
            .as_ref(),
    );
    let disabled_debug = temporary.path().join("claude-disabled-debug.log");
    let mut disabled_host = claude_host_command(
        &claude,
        temporary.path(),
        &disabled_workspace,
        &disabled_provider,
        &disabled_debug,
    );
    let BoundedOutput {
        output,
        expired,
        process_tree,
    } = output_with_timeout(&mut disabled_host, Duration::from_secs(30));
    drop(process_tree);
    let disabled_debug_output = fs::read_to_string(&disabled_debug).unwrap_or_default();
    assert_host_success("disabled Claude", &output, expired, &disabled_debug_output);
    assert_eq!(
        fs::read_to_string(disabled_workspace.join("claude-e2e.txt"))
            .expect("disabled Claude sentinel")
            .trim(),
        "qualified"
    );
    assert_semantic_provider_exchange(&disabled_provider);
    assert_service_absent(&package.launcher, temporary.path())
        .expect("disabled Claude must not start Acyclic");

    let mut service = install_host(&package.launcher, "claude-code", &claude, temporary.path());
    let provider = ScriptedProvider::start(
        ProviderProtocol::AnthropicMessages,
        workspace.join("claude-e2e.txt").to_string_lossy().as_ref(),
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
    let debug = fs::read_to_string(&debug_log).unwrap_or_default();
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
    assert_semantic_provider_exchange(&provider);
    service.assert_hook_service_live();

    qualify_claude_child(&claude, temporary.path(), &workspace, &service);
    qualify_claude_failed_child(&claude, temporary.path(), &workspace);

    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _service = service;
        panic!("injected host assertion failure");
    }));
    assert!(unwind.is_err(), "injected unwind must execute");
    assert_service_absent(&package.launcher, temporary.path()).expect("Claude unwind cleanup");
    assert_claude_timeout_cleanup(&package.launcher, &claude, temporary.path(), &workspace);
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

fn qualify_claude_child(claude: &Path, home: &Path, workspace: &Path, _service: &ServiceGuard) {
    let provider = ScriptedProvider::start_claude_subagent("claude-child-isolation.txt");
    let debug_path = home.join("claude-child-debug.log");
    let mut host = claude_host_command(claude, home, workspace, &provider, &debug_path);
    let BoundedOutput {
        output,
        expired,
        process_tree,
    } = output_with_timeout(&mut host, Duration::from_secs(45));
    drop(process_tree);
    let debug = fs::read_to_string(&debug_path).unwrap_or_default();
    assert_host_success("Claude child isolation", &output, expired, &debug);
    assert!(
        !workspace.join("claude-child-isolation.txt").exists(),
        "child relative write escaped into the physical root"
    );
    let requests = provider.wait_for_requests(3, Duration::from_secs(5));
    assert!(
        requests.len() >= 3
            && requests
                .iter()
                .any(|request| request.to_string().contains("ACYCLIC_DETERMINISTIC_CHILD"))
            && requests
                .iter()
                .any(|request| contains_type(request, "tool_result")),
        "Claude did not complete the scripted child tool exchange: {requests:#?}"
    );
    let transcript = String::from_utf8_lossy(&output.stdout);
    assert!(
        transcript.contains("SubagentStart")
            && transcript.contains("PostToolUse")
            && transcript.contains("updatedInput")
            && transcript.contains("claude-child-isolation.txt"),
        "Claude child hooks did not expose the rewritten successful tool lifecycle:\n{transcript}\n{debug}"
    );
}

fn qualify_claude_failed_child(claude: &Path, home: &Path, workspace: &Path) {
    let provider = ScriptedProvider::start_claude_failed_subagent();
    let debug_path = home.join("claude-failed-child-debug.log");
    let mut host = claude_host_command(claude, home, workspace, &provider, &debug_path);
    let BoundedOutput {
        output,
        expired,
        process_tree,
    } = output_with_timeout(&mut host, Duration::from_secs(45));
    drop(process_tree);
    let debug = fs::read_to_string(&debug_path).unwrap_or_default();
    assert_host_success("Claude failed child", &output, expired, &debug);
    let requests = provider.wait_for_requests(3, Duration::from_secs(5));
    assert!(
        requests
            .iter()
            .any(|request| request.to_string().contains("\"is_error\":true")),
        "Claude did not report the expected failed child tool: {requests:#?}"
    );
    let transcript = String::from_utf8_lossy(&output.stdout);
    assert!(
        transcript.contains("PostToolUseFailure")
            && !debug.contains("filesystem tools are still active"),
        "failed child did not close its operation lease:\n{transcript}\n{debug}"
    );
}

fn assert_codex_timeout_cleanup(launcher: &Path, binary: &Path, home: &Path, workspace: &Path) {
    let service = install_host(launcher, "codex", binary, home);
    let provider = ScriptedProvider::start_stalled(ProviderProtocol::Responses);
    let mut host = codex_host_command(binary, home, workspace, &provider, true);
    let (
        BoundedOutput {
            output,
            expired,
            process_tree,
        },
        admitted,
    ) = output_after_provider_admission(
        &mut host,
        &provider,
        Duration::from_secs(30),
        Duration::from_secs(15),
    );
    assert!(
        admitted,
        "Codex did not reach the stalled provider\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(expired, "stalled Codex must hit the process-tree deadline");
    service.assert_timeout_cleanup(process_tree);
    assert_service_absent(launcher, home).expect("Codex timeout cleanup");
}

fn assert_claude_timeout_cleanup(launcher: &Path, binary: &Path, home: &Path, workspace: &Path) {
    let service = install_host(launcher, "claude-code", binary, home);
    let provider = ScriptedProvider::start_stalled(ProviderProtocol::AnthropicMessages);
    let debug = home.join("claude-timeout-debug.log");
    let mut host = claude_host_command(binary, home, workspace, &provider, &debug);
    let (
        BoundedOutput {
            output,
            expired,
            process_tree,
        },
        admitted,
    ) = output_after_provider_admission(
        &mut host,
        &provider,
        Duration::from_secs(30),
        Duration::from_secs(15),
    );
    assert!(
        admitted,
        "Claude did not reach the stalled provider\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(expired, "stalled Claude must hit the process-tree deadline");
    service.assert_timeout_cleanup(process_tree);
    assert_service_absent(launcher, home).expect("Claude timeout cleanup");
}

fn codex_host_command(
    binary: &Path,
    home: &Path,
    workspace: &Path,
    provider: &ScriptedProvider,
    integration_enabled: bool,
) -> std::process::Command {
    let mut host = command(binary);
    host.current_dir(workspace);
    host.args([
        "exec",
        "--json",
        "--ephemeral",
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
    if !integration_enabled {
        host.arg("--ignore-user-config");
    }
    host.arg("Run the deterministic qualification command.");
    host.env("ACYCLIC_E2E_API_KEY", "test");
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
        "--no-session-persistence",
        "--max-turns",
        "3",
        "--permission-mode",
        "bypassPermissions",
        "--permission-prompts",
        "none",
        "--debug-file",
    ]);
    host.arg(debug_log).args(["--setting-sources", "user"]);
    host.env("ANTHROPIC_API_KEY", "test");
    host.env("ANTHROPIC_BASE_URL", provider.base_url());
    isolated_state(&mut host, home);
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

fn install_host(launcher: &Path, host: &str, host_binary: &Path, home: &Path) -> ServiceGuard {
    let mut install = command("node");
    install.arg(launcher).args(["install", host]);
    prepend_binary_directory(&mut install, host_binary);
    isolated_state(&mut install, home);
    if host == "codex" {
        preserve_windows_profile_identity(&mut install);
    }
    let output = install.output().expect("install host integration");
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    ServiceGuard::new(launcher, home)
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

fn assert_semantic_provider_exchange(provider: &ScriptedProvider) {
    let requests = provider.wait_for_requests(2, Duration::from_secs(5));
    assert_eq!(requests.len(), 2, "expected tool and completion requests");
    let is_completion = |request: &Value| {
        [
            "function_call_output",
            "custom_tool_call_output",
            "tool_result",
        ]
        .iter()
        .any(|kind| contains_type(request, kind))
    };
    assert!(requests.iter().any(is_completion));
    assert!(requests.iter().any(|request| !is_completion(request)));
}

fn contains_type(value: &Value, expected: &str) -> bool {
    match value {
        Value::Object(values) => {
            values.get("type").and_then(Value::as_str) == Some(expected)
                || values.values().any(|value| contains_type(value, expected))
        }
        Value::Array(values) => values.iter().any(|value| contains_type(value, expected)),
        _ => false,
    }
}
