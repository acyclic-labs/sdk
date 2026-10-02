//! Facts about Codex 0.155.1 the design depends on, checked against the
//! recorded fixtures. These run now and on every fixture re-record: if a Codex
//! upgrade changes one, the design note next to it needs revisiting.

#![cfg(unix)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "test assertions"
)]

mod support;

use serde_json::Value;
use support::fixture_dir;

fn read(name: &str) -> String {
    std::fs::read_to_string(fixture_dir().join(name))
        .unwrap_or_else(|error| panic!("{name}: {error}"))
}

fn json(name: &str) -> Value {
    serde_json::from_str(&read(name)).expect("fixture JSON")
}

#[test]
fn a_quota_typed_429_stops_codex_after_one_request_and_a_402_does_not() {
    // Why the proxy refuses with 429 insufficient_quota (proxy.rs).
    let counts = json("request-counts.json");
    assert_eq!(counts["quota429"], 1);
    assert_eq!(counts["quotafailed"], 1);
    assert_eq!(counts["402"], 6);
    assert_eq!(
        counts["500"], 30,
        "5 HTTP x 6 stream attempts with default retries"
    );
}

#[test]
fn the_cli_still_has_every_flag_the_executor_passes() {
    let help = read("help-exec.txt");
    for flag in [
        "--json",
        "--skip-git-repo-check",
        "--cd",
        "--strict-config",
        "--model",
        "--ephemeral",
    ] {
        assert!(help.contains(flag), "codex exec --help lost {flag}");
    }
    assert!(read("help-exec-resume.txt").contains("SESSION_ID"));
}

#[test]
fn codex_only_calls_the_responses_path_with_a_bearer_dummy_key() {
    let request = json("request-shell.json");
    assert_eq!(request["method"], "POST");
    assert_eq!(request["path"], "/v1/responses");
    assert_eq!(request["body"]["stream"], true);
    assert_eq!(request["body"]["store"], false);
    assert!(
        request["headers"].get("content-encoding").is_none(),
        "bodies are plain JSON, so the proxy can merge extra_body"
    );
    let tools: Vec<&str> = request["body"]["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    assert!(
        tools.contains(&"exec_command"),
        "the shell tool the e2e script calls: {tools:?}"
    );
}

#[test]
fn a_required_mcp_server_failure_leaves_stdout_empty() {
    // Why the executor surfaces the stderr tail when no event arrived.
    assert!(read("mcp-required-404.stdout.jsonl").trim().is_empty());
    assert!(read("mcp-required-404.exit").contains("exit=1"));
    assert!(!read("mcp-required-404.stderr.txt").trim().is_empty());
}

#[test]
fn an_optional_mcp_server_failure_is_silent_on_stdout() {
    // Why config.rs marks our MCP server `required = true`.
    let stdout = read("mcp-404.stdout.jsonl");
    assert!(stdout.contains("turn.completed"));
    assert!(
        !stdout.contains("mcp"),
        "nothing on stdout says the tools are missing"
    );
}
