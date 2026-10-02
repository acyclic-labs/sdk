//! A8 acceptance: the real pinned Codex against a scripted upstream.
//!
//! Run with the binary the qualification workflow resolves:
//! `ACYCLIC_CODEX_BIN=$(node plugin/scripts/resolve-qualification-host.mjs codex …) \
//!   cargo test -p acyclic-harness-codex --test e2e -- --ignored`
//! or `rust/crates/harness-codex/verify.sh e2e`, which resolves it for you.

#![cfg(unix)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "test assertions"
)]

mod support;

use acyclic_harness::{
    OperationId,
    executor::{ExecutionEvent, Executor, TurnInput},
    model::ModelContent,
};
use acyclic_harness_codex::{CodexConfig, CodexExecutor, Upstream};
use serde_json::json;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use support::{
    FakeUpstream, Journal, RecordingMeter, function_call_turn, message_turn, namespaced_call_turn,
    sse,
};

fn codex() -> PathBuf {
    let path = std::env::var_os("ACYCLIC_CODEX_BIN")
        .expect("set ACYCLIC_CODEX_BIN to the pinned codex 0.155.1");
    PathBuf::from(path)
}

/// Script: call the MCP echo tool, then run a shell command that writes a
/// file, then answer. Tool names are looked up in the request, so the test
/// does not depend on how Codex namespaces MCP tools.
async fn scripted() -> FakeUpstream {
    FakeUpstream::start(|n, request| {
        // MCP tools arrive as {"type":"namespace","name":"mcp__acyclic","tools":[…]}.
        let echo = request.body["tools"]
            .as_array()
            .into_iter()
            .flatten()
            .find_map(|tool| {
                let namespace = tool["name"].as_str()?;
                tool["tools"].as_array()?.iter().find_map(|inner| {
                    let name = inner["name"].as_str()?;
                    name.contains("acyclic_echo")
                        .then(|| (namespace.to_owned(), name.to_owned()))
                })
            });
        let calls_so_far = request.body["input"].as_array().map_or(0, |input| {
            input
                .iter()
                .filter(|item| item["type"] == "function_call_output")
                .count()
        });
        match calls_so_far {
            0 => {
                let Some((namespace, name)) = echo else {
                    eprintln!("TOOLS: {}", request.body["tools"]);
                    return sse(message_turn(n, "no echo offered"));
                };
                sse(namespaced_call_turn(
                    n,
                    &namespace,
                    &name,
                    &json!({"text": "from codex"}),
                ))
            }
            1 => sse(function_call_turn(
                n,
                "exec_command",
                &json!({"cmd": "echo done > report.md"}),
            )),
            _ => sse(message_turn(n, "done")),
        }
    })
    .await
}

fn executor(
    upstream: &FakeUpstream,
    workspace: &std::path::Path,
    meter: Arc<RecordingMeter>,
    max_steps: u32,
) -> CodexExecutor {
    CodexExecutor::new(
        CodexConfig {
            binary: codex(),
            model: "gpt-5.5".into(),
            upstream: Upstream {
                base_url: format!("{}/v1", upstream.url),
                api_key: "real-upstream-key".into(),
                extra_body: json!({}),
            },
            workspace: workspace.to_owned(),
            instructions: "Use the acyclic_echo tool, then write report.md.".into(),
            subagents: false,
            max_steps,
            deadline: Some(Instant::now() + Duration::from_secs(120)),
            state_dir: workspace.with_extension("state"),
            resume_thread: None,
        },
        support::registry(),
    )
    .with_tool_authority(support::scope(), None)
    .expect("authority")
    .with_meter(meter)
}

fn input(max_steps: u32) -> TurnInput {
    TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text("Echo, then write report.md.".into()),
        selected_context: None,
        max_steps,
    }
}

#[tokio::test]
#[ignore = "needs ACYCLIC_CODEX_BIN (the pinned codex 0.155.1)"]
async fn real_codex_uses_our_tool_edits_the_workspace_and_is_metered() {
    let upstream = scripted().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let meter = Arc::new(RecordingMeter::default());
    let journal = Journal::default();
    let turn = input(8);
    let operation = turn.operation_id;
    let output = executor(&upstream, dir.path(), meter.clone(), 8)
        .execute(turn, &journal)
        .await
        .expect("turn succeeds");

    assert_eq!(output.text, "done");
    assert_eq!(output.steps, 3, "three proxied model calls");
    assert_eq!(
        std::fs::read_to_string(dir.path().join("report.md"))
            .expect("report")
            .trim(),
        "done"
    );
    assert_eq!(meter.recorded().len(), 3, "every model call metered");
    assert!(
        upstream
            .requests()
            .iter()
            .all(|r| r.headers["authorization"] == "Bearer real-upstream-key")
    );

    let events = journal.events(operation);
    let completed = events
        .iter()
        .filter(|e| matches!(e, ExecutionEvent::ToolCompleted { .. }))
        .count();
    assert!(completed >= 2, "the MCP call and the command: {events:?}");
}

#[tokio::test]
#[ignore = "needs ACYCLIC_CODEX_BIN (the pinned codex 0.155.1)"]
async fn a_spent_budget_ends_the_turn_after_one_more_request() {
    let upstream = scripted().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let started = Instant::now();
    let error = executor(
        &upstream,
        dir.path(),
        Arc::new(RecordingMeter::stopping_after(1)),
        8,
    )
    .execute(input(8), &Journal::default())
    .await
    .expect_err("budget stops the turn");
    assert!(error.to_string().contains("budget"), "{error}");
    assert_eq!(
        upstream.requests().len(),
        1,
        "the refused call never reached upstream"
    );
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "no retry storm"
    );
}

#[tokio::test]
#[ignore = "needs ACYCLIC_CODEX_BIN (the pinned codex 0.155.1)"]
async fn the_step_cap_ends_the_turn() {
    let upstream = scripted().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let error = executor(
        &upstream,
        dir.path(),
        Arc::new(RecordingMeter::default()),
        1,
    )
    .execute(input(1), &Journal::default())
    .await
    .expect_err("one step is not enough");
    assert!(error.to_string().contains("step"), "{error}");
    assert_eq!(upstream.requests().len(), 1);
}
