//! A6 + A7 acceptance: process control and the `Executor` contract, driven by a
//! fake `codex` that replays recorded 0.155.1 output (no network, no Codex).

#![cfg(unix)]
#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "test assertions"
)]

mod support;

use acyclic_harness::{
    Capabilities, OperationId,
    conversation::ModelContextSelection,
    executor::{ExecutionEvent, ExecutionJournal as _, Executor, TurnInput},
    model::{ModelContent, ModelMessage, ModelRole},
    projection::SelectedModelContext,
    runtime::RuntimeScope,
};
use acyclic_harness_codex::{CodexConfig, CodexExecutor, Upstream};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use support::{FakeCodex, FakeUpstream, Journal, RecordingMeter};

struct Turn {
    dir: tempfile::TempDir,
    workspace: std::path::PathBuf,
    upstream: FakeUpstream,
}

impl Turn {
    async fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let workspace = dir.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        Self {
            dir,
            workspace,
            upstream: FakeUpstream::message("unused by the fake").await,
        }
    }

    fn executor(&self, fake: &FakeCodex, deadline: Option<Instant>) -> CodexExecutor {
        self.executor_with_scope(fake, deadline, support::scope())
    }

    fn executor_with_scope(
        &self,
        fake: &FakeCodex,
        deadline: Option<Instant>,
        scope: RuntimeScope,
    ) -> CodexExecutor {
        let bin = self.dir.path().join("bin");
        std::fs::create_dir_all(&bin).expect("bin");
        let binary = fake.install(&bin);
        CodexExecutor::new(
            CodexConfig {
                binary,
                model: "gpt-5.5".into(),
                upstream: Upstream {
                    base_url: format!("{}/v1", self.upstream.url),
                    api_key: "real-upstream-key".into(),
                    extra_body: json!({}),
                },
                workspace: self.workspace.clone(),
                instructions: "You are the root agent. Write report.md.".into(),
                subagents: true,
                max_steps: 8,
                deadline,
                state_dir: self.dir.path().join("state"),
                resume_thread: None,
            },
            support::registry(),
        )
        .with_tool_authority(scope, None)
        .expect("authority")
        .with_meter(Arc::new(RecordingMeter::default()))
    }

    fn invocations(&self) -> Vec<std::path::PathBuf> {
        FakeCodex::invocations(&self.dir.path().join("bin"))
    }
}

fn input(operation_id: OperationId, text: &str) -> TurnInput {
    TurnInput {
        operation_id,
        input: ModelContent::Text(text.into()),
        selected_context: None,
        max_steps: 8,
    }
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

// ------------------------------------------------------------------ A6

#[tokio::test]
async fn codex_runs_in_a_private_home_with_closed_stdin_and_a_dummy_key() {
    let turn = Turn::new().await;
    let fake = FakeCodex {
        fixture: "ok",
        ..FakeCodex::default()
    };
    let journal = Journal::default();
    turn.executor(&fake, None)
        .execute(
            input(OperationId::new(), "compare payer fee schedules"),
            &journal,
        )
        .await
        .expect("turn succeeds");

    let calls = turn.invocations();
    assert_eq!(calls.len(), 1, "one codex process per turn");
    let argv: Vec<String> = read(&calls[0].join("argv"))
        .lines()
        .map(str::to_owned)
        .collect();
    for flag in ["exec", "--json", "--skip-git-repo-check", "--strict-config"] {
        assert!(argv.iter().any(|a| a == flag), "missing {flag} in {argv:?}");
    }
    let cd = argv.iter().position(|a| a == "-C").expect("-C");
    assert_eq!(Path::new(&argv[cd + 1]), turn.workspace);
    assert_eq!(
        argv.last().map(String::as_str),
        Some("compare payer fee schedules"),
        "the task is the prompt"
    );

    let env = read(&calls[0].join("env"));
    assert!(
        !env.contains("real-upstream-key"),
        "Codex never sees the real key:\n{env}"
    );
    assert!(
        env.contains("OPENAI_API_KEY=\n"),
        "no ambient OpenAI key leaks into Codex"
    );
    let home = calls[0].join("home");
    let config = read(&home.join("config.toml"));
    assert!(config.contains(r#"model_provider = "acyclic""#));
    assert!(
        config.contains(r#"[mcp_servers.acyclic]"#),
        "granted tools are offered over MCP"
    );
    assert_eq!(
        read(&home.join("AGENTS.md")),
        "You are the root agent. Write report.md."
    );
}

#[tokio::test]
async fn the_deadline_terminates_codex_and_returns_a_limit_error() {
    let turn = Turn::new().await;
    let fake = FakeCodex {
        fixture: "ok",
        sleep_secs: 60,
        ..FakeCodex::default()
    };
    let started = Instant::now();
    let error = turn
        .executor(&fake, Some(Instant::now() + Duration::from_secs(3)))
        .execute(input(OperationId::new(), "task"), &Journal::default())
        .await
        .expect_err("past the deadline");
    assert!(error.to_string().contains("deadline"), "{error}");
    assert!(
        started.elapsed() < Duration::from_secs(18),
        "SIGTERM, then SIGKILL after 10 s at most"
    );
    assert_eq!(
        read(&turn.invocations()[0].join("signal")).trim(),
        "terminated",
        "SIGTERM first"
    );
}

#[tokio::test]
async fn a_failed_turn_reports_codex_message() {
    let turn = Turn::new().await;
    let fake = FakeCodex {
        fixture: "quota429",
        exit: 1,
        ..FakeCodex::default()
    };
    let error = turn
        .executor(&fake, None)
        .execute(input(OperationId::new(), "task"), &Journal::default())
        .await
        .expect_err("turn.failed is an error");
    assert!(error.to_string().contains("Quota exceeded"), "{error}");
}

#[tokio::test]
async fn a_failure_before_any_event_reports_stderr() {
    // A required MCP server that fails to start: empty stdout, reason on stderr.
    let turn = Turn::new().await;
    let fake = FakeCodex {
        fixture: "mcp-required-404",
        exit: 1,
        stderr: Some("Error: required MCP servers failed to initialize: acyclic"),
        ..FakeCodex::default()
    };
    let error = turn
        .executor(&fake, None)
        .execute(input(OperationId::new(), "task"), &Journal::default())
        .await
        .expect_err("no events and exit 1");
    assert!(
        error.to_string().contains("required MCP servers failed"),
        "{error}"
    );
}

// ------------------------------------------------------------------ A7

#[tokio::test]
async fn a_turn_returns_the_last_message_with_codex_metadata() {
    let turn = Turn::new().await;
    let fake = FakeCodex {
        fixture: "ok",
        touch: Some("report.md"),
        ..FakeCodex::default()
    };
    let output = turn
        .executor(&fake, None)
        .execute(input(OperationId::new(), "task"), &Journal::default())
        .await
        .expect("turn succeeds");
    assert_eq!(output.text, "hi from fake");
    assert_eq!(output.metadata["harness"], "codex");
    assert_eq!(output.metadata["codex_version"], "0.155.1");
    assert_eq!(
        output.metadata["thread_id"],
        "01a0fdbc-f228-7790-9949-e2c76a4e7fd1"
    );
    // The replayed run makes no proxied call, so nothing was metered; Codex's
    // own (thread-cumulative) count is kept alongside for reference.
    assert_eq!(output.metadata["usage"]["output_tokens"], 0);
    assert_eq!(output.metadata["codex_thread_usage"]["output_tokens"], 7);
    assert!(
        turn.workspace.join("report.md").exists(),
        "Codex's edits stay in the workspace"
    );
}

#[tokio::test]
async fn the_journal_records_the_turn_like_the_stock_loop() {
    let turn = Turn::new().await;
    let fake = FakeCodex {
        fixture: "shell",
        ..FakeCodex::default()
    };
    let journal = Journal::default();
    let operation = OperationId::new();
    turn.executor(&fake, None)
        .execute(input(operation, "task"), &journal)
        .await
        .expect("turn");

    let events = journal.events(operation);
    assert!(
        matches!(events.first(), Some(ExecutionEvent::Started { .. })),
        "{events:?}"
    );

    let started = events.iter().find_map(|event| match event {
        ExecutionEvent::ToolStarted {
            call_id,
            invocation,
            ..
        } if call_id == "item_1" => Some(journal.json(invocation)),
        _ => None,
    });
    let started = started.expect("command_execution item_1 becomes ToolStarted");
    assert!(
        started.to_string().contains("echo probe-output"),
        "{started}"
    );
    assert!(
        events.iter().any(|event| matches!(event, ExecutionEvent::ToolCompleted { call_id, .. } if call_id == "item_1")),
        "and ToolCompleted"
    );

    let model_events: Vec<Value> = events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Model { event, .. } => Some(journal.json(event)),
            _ => None,
        })
        .collect();
    assert!(
        model_events
            .iter()
            .any(|event| event["kind"] == "content" && event["delta"] == "hi from fake")
    );
    let last = model_events.last().expect("a completion event");
    assert_eq!(last["kind"], "completed");
    assert_eq!(
        last["metadata"]["thread_id"],
        "01a0fdbd-4069-76c1-b310-5cf531cc8761"
    );
}

#[tokio::test]
async fn a_finished_turn_replays_without_running_codex_again() {
    let turn = Turn::new().await;
    let fake = FakeCodex {
        fixture: "ok",
        ..FakeCodex::default()
    };
    let journal = Journal::default();
    let operation = OperationId::new();
    let executor = turn.executor(&fake, None);
    let first = executor
        .execute(input(operation, "task"), &journal)
        .await
        .expect("first");
    let again = executor
        .execute(input(operation, "task"), &journal)
        .await
        .expect("replay");
    assert_eq!(first, again);
    assert_eq!(turn.invocations().len(), 1);
}

#[tokio::test]
async fn a_crashed_turn_resumes_its_codex_thread() {
    let turn = Turn::new().await;
    let journal = Journal::default();
    let operation = OperationId::new();
    let crashed = FakeCodex {
        fixture: "synthetic-crash",
        exit: 137,
        ..FakeCodex::default()
    };
    let error = turn
        .executor(&crashed, None)
        .execute(input(operation, "task"), &journal)
        .await
        .expect_err("killed mid-turn");
    assert!(!error.to_string().is_empty());

    let healthy = FakeCodex {
        fixture: "ok",
        ..FakeCodex::default()
    };
    turn.executor(&healthy, None)
        .execute(input(operation, "task"), &journal)
        .await
        .expect("resumed");
    let calls = turn.invocations();
    assert_eq!(calls.len(), 2);
    let argv = read(&calls[1].join("argv"));
    assert!(argv.lines().any(|a| a == "resume"), "{argv}");
    assert!(
        argv.contains("01a0fdbc-f228-7790-9949-e2c76a4e7fd1"),
        "the crashed thread, not a new one"
    );
}

#[tokio::test]
async fn a_different_input_on_the_same_operation_is_a_conflict() {
    let turn = Turn::new().await;
    let fake = FakeCodex {
        fixture: "ok",
        ..FakeCodex::default()
    };
    let journal = Journal::default();
    let operation = OperationId::new();
    let executor = turn.executor(&fake, None);
    executor
        .execute(input(operation, "task one"), &journal)
        .await
        .expect("first");
    let error = executor
        .execute(input(operation, "task two"), &journal)
        .await
        .expect_err("digest mismatch");
    assert!(
        matches!(error, acyclic_harness::Error::Conflict(_)),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_narrower_scope_on_the_same_operation_is_a_conflict() {
    let turn = Turn::new().await;
    let fake = FakeCodex {
        fixture: "ok",
        ..FakeCodex::default()
    };
    let journal = Journal::default();
    let operation = OperationId::new();
    turn.executor(&fake, None)
        .execute(input(operation, "task"), &journal)
        .await
        .expect("first");
    let narrower = RuntimeScope::new(
        Capabilities::new(Vec::<String>::new()),
        support::scope().limits(),
    )
    .expect("scope");
    let error = turn
        .executor_with_scope(&fake, None, narrower)
        .execute(input(operation, "task"), &journal)
        .await
        .expect_err("a finished turn is not replayed under other authority");
    assert!(
        matches!(error, acyclic_harness::Error::Conflict(_)),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_resumed_turn_only_gets_the_steps_that_are_left() {
    let turn = Turn::new().await;
    let journal = Journal::default();
    let operation = OperationId::new();
    let crashed = FakeCodex {
        fixture: "synthetic-crash",
        exit: 137,
        ..FakeCodex::default()
    };
    turn.executor(&crashed, None)
        .execute(input(operation, "task"), &journal)
        .await
        .expect_err("killed mid-turn");
    // The crashed run had already spent every step of the turn.
    journal
        .append(
            operation,
            "test:spent".into(),
            ExecutionEvent::ModelStarted {
                step: 8,
                request_digest: [0; 32],
            },
        )
        .await
        .expect("append");
    let error = turn
        .executor(&crashed, None)
        .execute(input(operation, "task"), &journal)
        .await
        .expect_err("no steps left");
    assert!(error.to_string().contains("step limit"), "{error}");
    assert_eq!(turn.invocations().len(), 1, "codex is not started again");
}

#[tokio::test]
async fn selected_context_reaches_a_new_codex_thread() {
    let turn = Turn::new().await;
    let fake = FakeCodex {
        fixture: "ok",
        ..FakeCodex::default()
    };
    let message = |role, text: &str| ModelMessage {
        role,
        content: ModelContent::Text(text.into()),
    };
    let selected = SelectedModelContext {
        selection: ModelContextSelection {
            conversation_revision: 3,
            message_ids: (0..3).map(|_| uuid::Uuid::new_v4()).collect(),
        },
        messages: vec![
            message(ModelRole::User, "the payer is Aetna"),
            message(ModelRole::Assistant, "noted: Aetna"),
            message(ModelRole::User, "now compare fee schedules"),
        ],
    };
    let turn_input =
        TurnInput::from_selected_context(OperationId::new(), selected, 8).expect("input");
    turn.executor(&fake, None)
        .execute(turn_input, &Journal::default())
        .await
        .expect("turn succeeds");
    let argv = read(&turn.invocations()[0].join("argv"));
    assert!(argv.contains("the payer is Aetna"), "{argv}");
    assert!(argv.contains("noted: Aetna"), "{argv}");
    assert!(argv.contains("now compare fee schedules"), "{argv}");
}

// ------------------------------------------------------- the fake itself

#[test]
fn the_fake_codex_replays_its_fixture_and_rejects_an_open_stdin() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fake = FakeCodex {
        fixture: "shell",
        exit: 3,
        ..FakeCodex::default()
    }
    .install(dir.path());
    let output = std::process::Command::new(&fake)
        .args(["exec", "--json", "-C", "/tmp", "task"])
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run fake");
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        read(&support::fixture_dir().join("shell.stdout.jsonl"))
    );
    let mut child = std::process::Command::new(&fake)
        .arg("exec")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("spawn fake");
    let _held = child.stdin.take();
    assert_eq!(
        child.wait().expect("wait").code(),
        Some(97),
        "an open stdin is caught"
    );
}
