use super::goals::*;
use crate::{TestResult, goals};
use acyclic_harness::{
    OperationId, TaskId,
    goal::{GoalContinuation, GoalContinuationPolicy, GoalStatus, GoalView},
};
use proptest::prelude::*;
use serde_json::{Value, json};
use std::path::Path;

#[path = "goal_workers_tests.rs"]
mod workers;

#[path = "../../../harness-fuzz/fuzz_targets/goal_parser_input.rs"]
mod goal_parser_input;
#[path = "../../../harness-fuzz/fuzz_targets/goal_scenario_input.rs"]
mod goal_scenario_input;

fn text() -> impl Strategy<Value = String> {
    proptest::collection::vec(any::<char>(), 0..64)
        .prop_map(|characters| characters.into_iter().collect())
}

proptest! {
    // Every admitted identity, including every continuation, must have exactly
    // one correctly addressed terminal file. Ordering is irrelevant. The old
    // root-only scope is the negative control for each continuation fault.
    #[test]
    fn terminal_status_oracle_covers_every_admitted_task(
        identities in proptest::collection::btree_set(any::<[u8; 16]>(), 2..8),
        offset in any::<usize>(),
    ) {
        use acyclic_harness::task_volume::{Channel, Component, FileKey, FileValue, TaskFile};
        let tasks: Vec<_> = identities.into_iter().map(TaskId::from_bytes).collect();
        let files: Vec<_> = tasks.iter().map(|task| FileValue::new(
            FileKey::Task {
                task: Component::new(task.to_string()).unwrap(),
                file: TaskFile::Status,
            },
            serde_json::to_vec(&json!({"task":task,"state":"completed"})).unwrap(),
        )).collect();
        let check = super::goal_execution::check_terminal_statuses;
        prop_assert!(check(&tasks, &files).is_ok());
        let mut reordered = files.clone();
        reordered.rotate_left(offset % files.len());
        prop_assert!(check(&tasks, &reordered).is_ok());
        for index in 0..files.len() {
            for fault in ["missing", "identity", "key", "state", "channel", "duplicate"] {
                let mut broken = files.clone();
                match fault {
                    "missing" => { broken.remove(index); }
                    "identity" => broken[index].bytes = serde_json::to_vec(
                        &json!({"task":"wrong","state":"completed"})
                    ).unwrap().into(),
                    "key" => broken[index].key = FileKey::Task {
                        task: Component::new("wrong").unwrap(), file: TaskFile::Status,
                    },
                    "state" => broken[index].bytes = serde_json::to_vec(
                        &json!({"task":tasks[index],"state":"pending"})
                    ).unwrap().into(),
                    "channel" => broken[index].channel = Some(Channel::Text),
                    "duplicate" => broken.push(files[index].clone()),
                    _ => unreachable!(),
                }
                prop_assert!(check(&tasks, &broken).is_err(), "accepted {fault} at {index}");
                if index > 0 {
                    // Restore the old scope using the same oracle: all faults
                    // outside the root task become invisible to it.
                    let root: Vec<_> = broken.iter().filter(|file| file.key == files[0].key).cloned().collect();
                    prop_assert!(check(&tasks[..1], &root).is_ok(), "control: {fault} at {index}");
                }
            }
        }
    }

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
            "project_seed":{"GOAL.md":answer},
            "prompt":body, "turns":turns, "max_steps":if streamed { 2 } else { 1 },
            "policy":{"max_continuations":limit},
            "expect":{"text":answer, "request_prompt":body, "result":{"requests":1},
                "statuses":[{"state":"pending"},{"state":"completed"}],
                "frames":[{"canonical_event_json":{"kind":"completed"}}],
                "view":{"markdown":body}, "files":{"GOAL.md":body},
                "project_files":{"GOAL.md":answer}}})).unwrap();
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
        let mut baseline =
            crate::scenario::load_value(&root, &root.join("goals/continuations/cap-two.json"))?;
        let mut settled =
            crate::scenario::load_value(&root, &root.join("goals/continuations/done.json"))?;
        for case in [&mut baseline, &mut settled] {
            case["project_seed"] = json!({"GOAL.md":"Project sentinel"});
            case["expect"]["project_files"] = json!({"GOAL.md":"Project sentinel"});
        }
        futures::future::try_join_all([baseline.clone(), settled.clone()].into_iter().map(
            |case| async move {
                crate::drivers::run(case).await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            },
        ))
        .await?;
        futures::future::try_join_all(
            [
                "file",
                "project-file",
                "missing-project-assertion",
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
                "runtime_error",
                "replay_runtime_error",
                "success_runtime_error",
                "success_replay_runtime_error",
            ]
            .into_iter()
            .map(|mutation| {
                // Continuation and blocked-result assertions use the three-task case.
                let mut value = if matches!(
                    mutation,
                    "view"
                        | "context"
                        | "policy"
                        | "automatic"
                        | "runtime_error"
                        | "replay_runtime_error"
                ) {
                    baseline.clone()
                } else {
                    settled.clone()
                };
                async move {
                    match mutation {
                        "file" => value["expect"]["files"]["GOAL.md"] = json!("wrong"),
                        "project-file" => {
                            value["expect"]["project_files"]["GOAL.md"] = json!("wrong")
                        }
                        "missing-project-assertion" => value["expect"]["project_files"] = json!({}),
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
                        "runtime_error" => value["expect"]["runtime_error"] = Value::Null,
                        "replay_runtime_error" => {
                            value["expect"]["replay_runtime_error"] = Value::Null
                        }
                        "success_runtime_error" => {
                            value["expect"]["runtime_error"] = json!("GOAL.md is blocked.")
                        }
                        "success_replay_runtime_error" => {
                            value["expect"]["replay_runtime_error"] = json!("GOAL.md is blocked.")
                        }
                        _ => unreachable!(),
                    }
                    let result = crate::drivers::run(value).await;
                    assert!(
                        result.is_err(),
                        "changed {mutation} must reach its assertion, got {result:?}"
                    );
                    if mutation.ends_with("runtime_error") {
                        let expected = if mutation.contains("replay") {
                            "unexpected reopened goal runtime result:"
                        } else {
                            "unexpected goal runtime result:"
                        };
                        assert!(result.unwrap_err().to_string().starts_with(expected));
                    }
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
        ("project_seed", json!({"GOAL.md":"unasserted"})),
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
        ("replay_runtime_error", json!("")),
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
        body in text(), project in text(), prompt in text(), cap in any::<u32>(), enabled in any::<bool>(),
        frames in proptest::collection::vec(text(), 1..5),
    ) {
        let value = json!({"seed":{"GOAL.md":body}, "project_seed":{"GOAL.md":project},
            "prompt":prompt, "turns":["Settled"],
            "policy": if enabled { json!({"max_continuations":cap}) } else { Value::Null },
            "expect":{"text":"Settled", "request_prompt":prompt, "result":{"requests":1},
            "view":{"status":"active"}, "files":{"GOAL.md":body}, "project_files":{"GOAL.md":project},
            "statuses":[{"state":"pending"},{"state":"completed"}], "frames":frames.into_iter().map(|text| json!({"message":text})).collect::<Vec<_>>()}});
        let case = GoalCase::parse(value).unwrap();
        let encoded = serde_json::to_value(&case).unwrap();
        let decoded = GoalCase::parse(serde_json::from_slice(&serde_json::to_vec(&case).unwrap()).unwrap()).unwrap();
        prop_assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
    }
}
