use super::goals::*;
use crate::{TestResult, goals};
use acyclic_harness::{
    OperationId, TaskId,
    goal::{GoalContinuation, GoalContinuationPolicy, GoalStatus, GoalView},
};
use proptest::prelude::*;
use serde_json::{Value, json};
use std::path::Path;

#[path = "../../../harness-fuzz/fuzz_targets/goal_parser_input.rs"]
mod goal_parser_input;
#[path = "../../../harness-fuzz/fuzz_targets/goal_scenario_input.rs"]
mod goal_scenario_input;

fn text() -> impl Strategy<Value = String> {
    proptest::collection::vec(any::<char>(), 0..64)
        .prop_map(|characters| characters.into_iter().collect())
}

proptest! {
    // The actual case codec preserves scripts, bounds, assertions and Unicode.
    #[test]
    fn goal_case_scripts_round_trip(body in text(), answer in text(), limit in any::<u32>(), streamed in any::<bool>()) {
        let turns = if streamed {
            json!([[{"type":"event", "kind":"tool_call", "call_id":"write", "name":"write",
                "arguments":{"path":".volumes/scratch/GOAL.md", "text":body, "media_type":"text/markdown", "display_name":"GOAL.md"}},
                {"type":"event", "kind":"completed", "metadata":{"answer":answer}}], answer])
        } else {
            json!([answer])
        };
        let case = GoalCase::parse(json!({"seed":{"GOAL.md":body},
            "prompt":body, "turns":turns, "max_steps":if streamed { 2 } else { 1 },
            "policy":{"max_continuations":limit},
            "expect":{"text":answer, "request_prompt":body, "result":{"requests":1},
                "statuses":[{"state":"pending"},{"state":"completed"}],
                "frames":[{"canonical_event_json":{"kind":"completed"}}],
                "view":{"markdown":body}, "files":{"GOAL.md":body}}})).unwrap();
        let canonical = serde_json::to_value(case).unwrap();
        let bytes = serde_json::to_vec(&canonical).unwrap();
        goal_scenario_input::check(&bytes);
        let parsed = GoalCase::parse(serde_json::from_slice(&bytes).unwrap()).unwrap();
        prop_assert_eq!(serde_json::to_value(parsed).unwrap(), canonical);
    }

    // Every u32 cap/count obeys the exact boundary. Blocking preserves the body,
    // stops the policy, and remains within the accepted file's metadata reserve.
    #[test]
    fn continuation_boundaries_preserve_goal_state(
        (limit, count) in prop_oneof![Just(0_u32), Just(1), Just(u32::MAX), any::<u32>()]
            .prop_flat_map(|limit| (Just(limit), prop_oneof![Just(limit),
                Just(limit.saturating_sub(1)), Just(limit.saturating_add(1)), any::<u32>()])),
        body in text(), status in 0..3_u8,
        task in any::<[u8;16]>(), turn in any::<[u8;16]>(),
    ) {
        let body = format!("# Goal\n{body}");
        let status = [GoalStatus::Active, GoalStatus::Blocked, GoalStatus::Done][status as usize];
        let view = GoalView::from_file(goal_parser_input::file(body.as_bytes()).unwrap(), body.as_bytes(), 8192).unwrap();
        let contents = view.status_contents(status, None).unwrap();
        let view = GoalView::from_file(goal_parser_input::file(contents.as_bytes()).unwrap(), contents.as_bytes(), 8192).unwrap();
        let policy = GoalContinuationPolicy { max_continuations: limit };
        let task = TaskId::from_bytes(task);
        let turn = OperationId::from_bytes(turn);
        let action = policy.on_settle(&view, task, turn, count);
        prop_assert_eq!(&action, &policy.on_settle(&view, task, turn, count));
        match action {
            None => prop_assert_ne!(status, GoalStatus::Active),
            Some(GoalContinuation::Continue(operation)) => {
                prop_assert_eq!(status, GoalStatus::Active);
                prop_assert!(count < limit);
                prop_assert_eq!(Some(operation), view.continuation_operation(task, turn));
            }
            Some(GoalContinuation::Block(contents)) => {
                prop_assert_eq!(status, GoalStatus::Active);
                prop_assert!(count >= limit);
                prop_assert!(contents.len() <= 8192);
                let blocked = GoalView::from_file(goal_parser_input::file(contents.as_bytes()).unwrap(), contents.as_bytes(), 8192).unwrap();
                prop_assert_eq!(&blocked.markdown, &body);
                prop_assert_eq!(blocked.status, GoalStatus::Blocked);
                let reason = format!("Maximum goal continuations reached (limit {limit}).");
                prop_assert_eq!(blocked.reason.as_deref(), Some(reason.as_str()));
                prop_assert_eq!(policy.on_settle(&blocked, task, turn, count), None);
            }
        }
    }
}

#[test]
fn goal_scenario_fuzz_corpus() -> TestResult {
    let directory =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../harness-fuzz/corpus/goal_scenario");
    let mut count = 0;
    for entry in std::fs::read_dir(directory)? {
        goal_scenario_input::check(&std::fs::read(entry?.path())?);
        count += 1;
    }
    assert!(count > 0);
    Ok(())
}

#[test]
fn goal_oracles_reject_changed_files_views_scripts_counts_and_policy() -> TestResult {
    crate::scenario_runtime()?.block_on(async {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
        let baseline =
            crate::scenario::load_value(&root, &root.join("goals/continuations/cap-two.json"))?;
        let settled =
            crate::scenario::load_value(&root, &root.join("goals/continuations/done.json"))?;
        futures::future::try_join_all([baseline.clone(), settled.clone()].into_iter().map(
            |case| async move {
                tokio::time::timeout(std::time::Duration::from_secs(5), crate::drivers::run(case))
                    .await??;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            },
        ))
        .await?;
        futures::future::try_join_all(
            [
                "file",
                "view",
                "count",
                "requests",
                "context",
                "tasks",
                "prompt",
                "script",
                "policy",
                "frames",
                "status",
                "automatic",
                "text",
                "max_steps",
            ]
            .into_iter()
            .map(|mutation| {
                // Only continuation-dependent assertions need the three-task case.
                let mut value = if matches!(mutation, "view" | "context" | "policy" | "automatic") {
                    baseline.clone()
                } else {
                    settled.clone()
                };
                async move {
                    match mutation {
                        "file" => value["expect"]["files"]["GOAL.md"] = json!("wrong"),
                        "view" => value["expect"]["view"]["status"] = json!("done"),
                        "count" => value["expect"]["result"]["continuations"] = json!(99),
                        "requests" => value["expect"]["result"]["requests"] = json!(99),
                        "context" => {
                            value["expect"]["requests"][1]["messages"][0]["content"] =
                                json!("wrong continuation prompt")
                        }
                        "tasks" => value["expect"]["result"]["tasks"] = json!(99),
                        "prompt" => value["prompt"] = json!("wrong prompt"),
                        "script" => value["turns"].as_array_mut().unwrap().push(json!("unused")),
                        "policy" => value["policy"]["max_continuations"] = json!(0),
                        "frames" => {
                            value["expect"]["frames"] =
                                json!([{ "canonical_event_json": {"kind":"missing_completion"}}])
                        }
                        "status" => value["expect"]["statuses"][1]["state"] = json!("pending"),
                        "automatic" => value["policy"] = Value::Null,
                        "text" => value["expect"]["text"] = json!("wrong"),
                        "max_steps" => value["max_steps"] = json!(0),
                        _ => unreachable!(),
                    }
                    let result = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        crate::drivers::run(value),
                    )
                    .await;
                    assert!(
                        matches!(result, Ok(Err(_))),
                        "changed {mutation} must reach its assertion, got {result:?}"
                    );
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                }
            }),
        )
        .await?;

        Ok(())
    })
}

#[test]
fn goal_schema_rejects_unknown_fields_and_vacuous_assertions() {
    let baseline = json!({"prompt":"Continue", "turns":["Settled"],
        "expect":{"text":"Settled", "request_prompt":"Continue", "result":{"requests":1},
        "view":{"status":"active"}, "statuses":[{"state":"pending"},{"state":"completed"}],
                "frames":[{"canonical_event_json":{"kind":"completed"}}],
        "files":{"GOAL.md":"Goal"}}});
    GoalCase::parse(baseline.clone()).unwrap();
    for (field, replacement) in [
        ("typo", json!(true)),
        ("settles", json!(1)),
        ("max_steps", json!(0)),
        ("policy", json!({"unlimited":true})),
        ("turns", json!([{"unknown":1}])),
    ] {
        let mut value = baseline.clone();
        value[field] = replacement;
        assert!(GoalCase::parse(value).is_err(), "accepted invalid {field}");
    }
    for (field, replacement) in [
        ("result", json!({})),
        ("view", json!({})),
        ("view_error", json!("error")),
        ("statuses", json!([])),
        ("statuses", json!([{}])),
        ("statuses", json!([{}, {}])),
        ("statuses", json!([{"state":true}, {"state":"completed"}])),
        ("frames", json!([])),
        ("frames", json!([{}])),
        ("frames", json!(true)),
        ("files", json!({})),
        ("runtime_error", json!("")),
        ("unchanged_goal", json!(true)),
    ] {
        let mut value = baseline.clone();
        value["expect"][field] = replacement;
        assert!(
            GoalCase::parse(value).is_err(),
            "accepted invalid expectation {field}"
        );
    }
}

proptest! {
    // The scenario codec preserves Unicode seed/prompt bytes, optional admission
    // policies and every frame matcher through a complete serde round trip.
    #[test]
    fn goal_case_codec_round_trips(
        body in text(), prompt in text(), cap in any::<u32>(), enabled in any::<bool>(),
        frames in proptest::collection::vec(text(), 1..5),
    ) {
        let value = json!({"seed":{"GOAL.md":body}, "prompt":prompt, "turns":["Settled"],
            "policy": if enabled { json!({"max_continuations":cap}) } else { Value::Null },
            "expect":{"text":"Settled", "request_prompt":prompt, "result":{"requests":1},
            "view":{"status":"active"}, "files":{"GOAL.md":body}, "statuses":[{"state":"pending"},{"state":"completed"}], "frames":frames.into_iter().map(|text| json!({"message":text})).collect::<Vec<_>>()}});
        let case = GoalCase::parse(value).unwrap();
        let encoded = serde_json::to_value(&case).unwrap();
        let decoded = GoalCase::parse(serde_json::from_slice(&serde_json::to_vec(&case).unwrap()).unwrap()).unwrap();
        prop_assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
    }
}
