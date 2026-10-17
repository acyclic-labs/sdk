//! Data-driven harness scenarios. `E2E_FILTER` selects a path substring.
mod composer;
mod discovery;
mod drivers;
mod execution;
mod expectations;
mod limits_tests;
mod local_admission;
mod local_case;
mod local_cloud;
mod local_execution;
mod local_tests;
mod local_tool_files;
mod local_tool_files_check;
mod local_tool_files_tests;
mod local_workspace;
mod matchers;
mod notification_history;
mod notifications;
mod plan_query;
mod plans;
mod provider_case;
mod provider_http;
mod provider_tests;
mod providers;
#[path = "../support/quiescent.rs"]
mod quiescent;
mod scenario;
#[path = "../support/scenario_data.rs"]
mod scenario_data;
mod swarm;
mod swarm_case;
mod swarm_provider;
mod swarm_publish;
#[path = "../support/swarm_session.rs"]
mod swarm_runtime;
mod swarm_scaling;
mod swarm_tests;
mod swarm_views;
mod swarm_wire;
mod task_queries;
mod task_query_case;
mod task_query_tests;
use acyclic_harness::context::CompactionPolicy;
use drivers::run_execution as run;
use futures::{StreamExt as _, stream};
use scenario::Scenario;
use std::{
    path::Path,
    time::{Duration, Instant},
};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
use scenario_data::discover;

#[tokio::test]
async fn scenario_oracles_reject_wrong_text_context_events_files_and_unused_turns() -> TestResult {
    let baseline = include_bytes!("../scenarios/core/text.json");
    run(Scenario::parse(baseline)?).await?;
    for field in [
        "text",
        "requests",
        "not_requests",
        "empty_requests",
        "events",
        "empty_events",
        "not_events",
        "result",
        "files",
        "turns",
        "request_count",
        "reconciliation_count",
        "recoveries",
        "resume",
    ] {
        let mut case = Scenario::parse(baseline)?;
        match field {
            "text" => case.expect.text = Some("wrong".into()),
            "requests" => {
                case.expect.requests =
                    Some(vec![serde_json::json!({"messages":[{"content":"wrong"}]})]);
            }
            "not_requests" => {
                case.expect.not_requests = vec![serde_json::json!({"messages":[{"role":"user"}]})];
            }
            "empty_requests" => case.expect.requests = Some(vec![]),
            "events" => case.expect.events = Some(vec![serde_json::json!({"kind":"wrong"})]),
            "empty_events" => case.expect.events = Some(vec![]),
            "not_events" => {
                case.expect.not_events = vec![serde_json::json!({"kind":"model_started"})];
            }
            "result" => case.expect.result = Some(serde_json::json!({"steps":99})),
            "files" => {
                case.expect
                    .files
                    .insert("missing.txt".into(), Some("wrong".into()));
            }
            "turns" => case
                .turns
                .push(acyclic_harness::test_support::ScriptedTurn::Text(
                    "unused".into(),
                )),
            "request_count" => case.expect.request_count = Some(0),
            "reconciliation_count" => case.expect.reconciliation_count = Some(1),
            "recoveries" => {
                case.recoveries
                    .push(acyclic_harness::test_support::ScriptedReconciliation {
                        orphan: false,
                        observed: vec![],
                        continuation: None,
                    });
            }
            "resume" => case.resume.push("interrupted".into()),
            _ => return Err("unknown test mutation".into()),
        }
        assert!(run(case).await.is_err(), "accepted incorrect {field}");
    }
    Ok(())
}

#[tokio::test]
async fn summary_oracles_reject_ignored_mode_text_result_context_and_recovery() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let baseline = root.join("summary/exact-capacity.json");
    run(Scenario::load(&root, &baseline)?).await?;
    for field in ["mode", "text", "result", "context", "budget"] {
        let mut case = Scenario::load(&root, &baseline)?;
        match field {
            "mode" => {
                case.kind = scenario::Kind::Turn;
                case.prompt = case.instruction.take();
            }
            "text" => case.expect.text = Some("wrong".into()),
            "result" => case.expect.result = Some(serde_json::json!({"source_messages":99})),
            "context" => case.context.pop().map(|_| ()).ok_or("missing context")?,
            "budget" => case.max_output_tokens = None,
            _ => return Err("unknown summary mutation".into()),
        }
        assert!(run(case).await.is_err(), "accepted incorrect {field}");
    }
    let mut case = Scenario::load(&root, &root.join("summary/recover-prefix.json"))?;
    case.recoveries
        .first_mut()
        .ok_or("missing recovery")?
        .observed
        .clear();
    assert!(
        run(case).await.is_err(),
        "accepted incorrect summary recovery prefix"
    );
    Ok(())
}

#[tokio::test]
async fn compaction_oracles_reject_ignored_context_capacity_policy_and_accounting() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let baseline = root.join("compaction/threshold.json");
    run(Scenario::load(&root, &baseline)?).await?;
    for field in ["context", "capacity", "policy", "accounting", "output"] {
        let mut case = Scenario::load(&root, &baseline)?;
        match field {
            "context" => case.context.clear(),
            "capacity" => {
                case.capacity
                    .as_mut()
                    .ok_or("missing capacity")?
                    .context_tokens = 1_000;
            }
            "policy" => case.compaction = Some(CompactionPolicy::Disabled),
            "accounting" => case.message_tokens = Some(1),
            "output" => case.max_output_tokens = Some(11),
            _ => return Err("unknown compaction mutation".into()),
        }
        assert!(run(case).await.is_err(), "accepted incorrect {field}");
    }
    Ok(())
}

#[tokio::test]
async fn compaction_oracles_check_the_attached_recovered_summary() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let baseline = root.join("compaction/recover-summary.json");
    run(Scenario::load(&root, &baseline)?).await?;
    for files in [
        vec![],
        vec![vec![], vec![]],
        vec![vec![], vec!["partial-".into()]],
        vec![vec![], vec!["restored".into()]],
        vec![vec![], vec!["wrong".into()]],
        vec![vec!["partial-restored".into()], vec![]],
    ] {
        let mut case = Scenario::load(&root, &baseline)?;
        case.expect.request_files = Some(files);
        assert!(
            run(case).await.is_err(),
            "accepted incorrect summary attachment"
        );
    }
    Ok(())
}

#[tokio::test]
async fn recovery_oracles_reject_wrong_prefixes_replies_counts_and_attempts() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let baseline = root.join("recovery/midstream.json");
    run(Scenario::load(&root, &baseline)?).await?;
    for field in ["prefix", "continuation", "count", "resume", "unused"] {
        let mut case = Scenario::load(&root, &baseline)?;
        match field {
            "prefix" => case
                .recoveries
                .first_mut()
                .ok_or("missing recovery")?
                .observed
                .clear(),
            "continuation" => {
                case.recoveries
                    .first_mut()
                    .ok_or("missing recovery")?
                    .continuation = None;
            }
            "count" => case.expect.reconciliation_count = Some(0),
            "resume" => *case.resume.first_mut().ok_or("missing resume")? = "wrong".into(),
            "unused" => case
                .recoveries
                .push(case.recoveries.first().ok_or("missing recovery")?.clone()),
            _ => return Err("unknown recovery mutation".into()),
        }
        assert!(run(case).await.is_err(), "accepted incorrect {field}");
    }
    Ok(())
}

#[test]
fn scenarios() -> TestResult {
    let started = Instant::now();
    scenario_runtime()?.block_on(run_scenarios(started))
}

fn scenario_runtime() -> TestResult<tokio::runtime::Runtime> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err("scenario services must start before the async runtime".into());
    }
    acyclic_native_runtime::initialize_native_io_services()?;
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

#[tokio::test]
async fn scenario_bootstrap_rejects_an_already_running_runtime() {
    let error = scenario_runtime().expect_err("bootstrapped inside the runtime");
    assert!(
        error
            .to_string()
            .contains("services must start before the async runtime")
    );
}

async fn run_scenarios(started: Instant) -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let mut paths = Vec::new();
    discover(&root, &mut paths)?;
    paths.sort();
    if let Ok(filter) = std::env::var("E2E_FILTER") {
        paths.retain(|path| path.to_string_lossy().contains(&filter));
    }
    if paths.is_empty() {
        return Err("no scenarios discovered".into());
    }
    let count = paths.len();
    let mut results = stream::iter(paths.into_iter().map(|path| {
        let root = root.clone();
        async move {
            let case_path = path.clone();
            let result: TestResult = tokio::spawn(async move {
                let case = scenario::load_value(&root, &case_path)?;
                tokio::time::timeout(Duration::from_secs(5), drivers::run(case)).await??;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            })
            .await
            .unwrap_or_else(|error| Err(Box::new(error)));
            (path, result)
        }
    }))
    .buffer_unordered(count);
    let mut failures = Vec::new();
    while let Some((path, result)) = results.next().await {
        let name = path.strip_prefix(&root)?.display();
        match result {
            Ok(()) => println!("PASS {name}"),
            Err(error) => {
                eprintln!("FAIL {name}: {error}");
                failures.push(path);
            }
        }
    }
    println!(
        "{} scenarios, {} failed, {:.3}s",
        count,
        failures.len(),
        started.elapsed().as_secs_f64()
    );
    if !failures.is_empty() {
        return Err(format!("{} scenarios failed", failures.len()).into());
    }
    Ok(())
}

#[tokio::test]
async fn token_accounting_configuration_does_not_change_model_options() -> TestResult {
    run(Scenario::parse(
        br#"{
        "prompt":"x", "turns":["answer"], "message_tokens":7,
        "expect":{"text":"answer","requests":[{"model":{"options":null}}]}
    }"#,
    )?)
    .await
}
