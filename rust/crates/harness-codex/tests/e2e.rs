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
use acyclic_harness_codex::{
    CodexConfig, CodexEvent, CodexExecutor, CodexObserver, ItemKind, Upstream,
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
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
    let metered: u64 = meter
        .recorded()
        .iter()
        .map(|usage| usage.output_tokens)
        .sum();
    assert_eq!(
        output.metadata["usage"]["output_tokens"], metered,
        "the turn reports what the proxy metered for it"
    );
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

    // Each metered call is journaled with the provider's id for its response, which the meter
    // was told too: the key that joins a reply to its call.
    let ids: Vec<Value> = events
        .iter()
        .filter_map(|event| match event {
            ExecutionEvent::Model { event, .. } => Some(journal.json(event)),
            _ => None,
        })
        .filter(|event| event["kind"] == "completed" && event["metadata"].get("usage").is_some())
        .filter(|event| event["metadata"].get("harness").is_none())
        .map(|event| event["metadata"]["id"].clone())
        .collect();
    assert_eq!(ids.len(), 3, "{ids:?}");
    assert!(
        ids.iter()
            .all(|id| id.as_str().is_some_and(|id| id.starts_with("resp_"))),
        "{ids:?}"
    );
    let told = meter.response_ids.lock().expect("meter").clone();
    assert_eq!(
        told.iter().map(|id| json!(id)).collect::<Vec<_>>(),
        ids,
        "the meter was told the same ids"
    );
}

/// A reasoning summary the model returns reaches the observer as a reasoning item: what a swarm
/// agent records in its transcript, since the journal keeps no reasoning.
#[tokio::test]
#[ignore = "needs ACYCLIC_CODEX_BIN (the pinned codex 0.155.1)"]
async fn real_codex_reports_reasoning_summaries() {
    #[derive(Default)]
    struct Reasoning(Mutex<Vec<String>>);
    impl CodexObserver for Reasoning {
        fn event(&self, event: &CodexEvent) {
            if let CodexEvent::ItemCompleted(item) = event
                && let ItemKind::Reasoning { text } = &item.kind
            {
                self.0.lock().expect("observer").push(text.clone());
            }
        }
    }
    let upstream = FakeUpstream::start(|n, _| {
        let reasoning = json!({"type": "reasoning", "id": format!("rs_{n}"),
            "summary": [{"type": "summary_text", "text": "**Plan** Answer directly."}]});
        let mut events = message_turn(n, "done");
        events.insert(
            1,
            json!({"type": "response.output_item.added", "output_index": 0, "item": reasoning}),
        );
        events.insert(
            2,
            json!({"type": "response.output_item.done", "output_index": 0, "item": reasoning}),
        );
        for event in &mut events[3..] {
            if let Some(index) = event.get_mut("output_index") {
                *index = json!(1);
            }
        }
        sse(events)
    })
    .await;
    let dir = tempfile::tempdir().expect("tempdir");
    let observer = Arc::new(Reasoning::default());
    let output = executor(
        &upstream,
        dir.path(),
        Arc::new(RecordingMeter::default()),
        4,
    )
    .with_observer(observer.clone())
    .execute(input(4), &Journal::default())
    .await
    .expect("turn succeeds");
    assert_eq!(output.text, "done");
    let seen = observer.0.lock().expect("observer").clone();
    assert!(
        seen.iter().any(|text| text.contains("Answer directly.")),
        "{seen:?}"
    );
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

/// Live: the pinned codex against `OpenAI` itself, through the proxy and executor a swarm agent
/// uses. Each call is journaled with `OpenAI`'s response id, the meter is told the same ids,
/// and the reasoning summaries Codex asks for reach the observer. Run with `OPENAI_API_KEY` and
/// `ACYCLIC_CODEX_BIN`; `OPENAI_MODEL` picks the model (default gpt-6-luna).
#[tokio::test]
#[ignore = "needs ACYCLIC_CODEX_BIN, OPENAI_API_KEY and network access"]
async fn live_codex_against_openai_names_responses_and_reports_reasoning() {
    #[derive(Default)]
    struct Reasoning(Mutex<Vec<String>>);
    impl CodexObserver for Reasoning {
        fn event(&self, event: &CodexEvent) {
            if let CodexEvent::ItemCompleted(item) = event
                && let ItemKind::Reasoning { text } = &item.kind
            {
                self.0.lock().expect("observer").push(text.clone());
            }
        }
    }
    let api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY");
    let model = std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-6-luna".into());
    // `OpenAI` leaves the summary out of some calls entirely (it did in 1 of 2 here, and in about
    // 1 of 4 streamed calls measured directly), so the reasoning gets three tries; every try
    // must name its responses and answer.
    for attempt in 1..=3 {
        let dir = tempfile::tempdir().expect("tempdir");
        let meter = Arc::new(RecordingMeter::default());
        let observer = Arc::new(Reasoning::default());
        let journal = Journal::default();
        let turn = TurnInput {
            operation_id: OperationId::new(),
            input: ModelContent::Text(
                "Three payers underpaid us: P1 paid 82% of 4,310.50, P2 paid 91% of 12,004.20 minus \
                 a 150 adjustment, P3 paid 77% of 980. Which payer owes the most dollars, and how \
                 much in total is owed? Work it out yourself without running commands, then answer \
                 in one line."
                    .into(),
            ),
            selected_context: None,
            max_steps: 4,
        };
        let operation = turn.operation_id;
        let output = CodexExecutor::new(
            CodexConfig {
                binary: codex(),
                model: model.clone(),
                upstream: Upstream {
                    base_url: "https://api.openai.com/v1".into(),
                    api_key: api_key.clone(),
                    extra_body: json!({}),
                },
                workspace: dir.path().to_owned(),
                instructions: "Answer the question.".into(),
                subagents: false,
                max_steps: 4,
                deadline: Some(Instant::now() + Duration::from_secs(180)),
                state_dir: dir.path().with_extension("state"),
                resume_thread: None,
            },
            support::registry(),
        )
        .with_tool_authority(support::scope(), None)
        .expect("authority")
        .with_meter(meter.clone())
        .with_observer(observer.clone())
        .execute(turn, &journal)
        .await
        .expect("turn succeeds");
        let ids: Vec<Value> = journal
            .events(operation)
            .iter()
            .filter_map(|event| match event {
                ExecutionEvent::Model { event, .. } => Some(journal.json(event)),
                _ => None,
            })
            .filter(|event| {
                event["kind"] == "completed" && event["metadata"].get("usage").is_some()
            })
            .filter(|event| event["metadata"].get("harness").is_none())
            .map(|event| event["metadata"]["id"].clone())
            .collect();
        let reasoning = observer.0.lock().expect("observer").clone();
        eprintln!(
            "answer {:?}; ids {ids:?}; reasoning {reasoning:?}",
            output.text
        );
        assert!(output.text.contains("P2"), "{}", output.text);
        assert!(!ids.is_empty(), "no metered call");
        assert!(
            ids.iter()
                .all(|id| id.as_str().is_some_and(|id| id.starts_with("resp_"))),
            "{ids:?}"
        );
        let told = meter.response_ids.lock().expect("meter").clone();
        assert_eq!(told.iter().map(|id| json!(id)).collect::<Vec<_>>(), ids);
        if reasoning.iter().any(|text| !text.trim().is_empty()) {
            return;
        }
        assert!(attempt < 3, "no reasoning reported in three turns");
    }
}
