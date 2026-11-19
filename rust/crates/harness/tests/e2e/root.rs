// Data-driven harness scenarios. `E2E_FILTER` selects a path substring.
mod case_workers;
mod discovery;
mod drivers;
mod execution;
mod expectations;
#[path = "../support/expected_outcome.rs"]
mod expected_outcome;
mod limits_tests;
mod local_case;
mod local_cloud;
mod local_tests;
mod local_workspace;
mod matchers;
#[path = "../support/ordered_matcher.rs"]
mod ordered_matcher;
#[path = "../support/progress_guard_tests.rs"]
mod progress_guard_tests;
#[path = "../support/quiescent.rs"]
mod quiescent;
mod resume;
mod runtime_tests;
mod scenario;
#[path = "../support/scenario_data.rs"]
mod scenario_data;
#[path = "../support/scenario_matchers.rs"]
mod scenario_matchers;
mod scenario_tests;
use acyclic_harness::context::CompactionPolicy;
use drivers::run;
use futures::{StreamExt as _, stream};
use scenario::Scenario;
use std::collections::BTreeSet;
type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
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
    let root = acyclic_test_support::source_path("rust/crates/harness").join("tests/scenarios");
    let baseline = root.join("summary/exact-capacity.json");
    run(Scenario::load(&root, &baseline)?).await?;
    for field in ["mode", "text", "result", "context"] {
        let mut case = Scenario::load(&root, &baseline)?;
        match field {
            "mode" => {
                case.kind = scenario::Kind::Turn;
                case.prompt = case.instruction.take();
            }
            "text" => case.expect.text = Some("wrong".into()),
            "result" => case.expect.result = Some(serde_json::json!({"source_messages":99})),
            "context" => case.context.pop().map(|_| ()).ok_or("missing context")?,
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
    let root = acyclic_test_support::source_path("rust/crates/harness").join("tests/scenarios");
    let baseline = root.join("compaction/threshold.json");
    run(Scenario::load(&root, &baseline)?).await?;
    for field in ["context", "capacity", "policy", "accounting"] {
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
            _ => return Err("unknown compaction mutation".into()),
        }
        assert!(run(case).await.is_err(), "accepted incorrect {field}");
    }
    for files in [
        vec![],
        vec![vec![], vec!["wrong".into()], vec!["retained facts".into()]],
        vec![
            vec![],
            vec!["retained earlier facts".into()],
            vec!["wrong".into()],
        ],
        vec![
            vec![],
            vec!["retained earlier facts".into()],
            vec!["retained facts".into(), "extra".into()],
        ],
    ] {
        let mut case = Scenario::load(&root, &baseline)?;
        case.expect.request_files = Some(files);
        assert!(
            run(case).await.is_err(),
            "accepted incorrect rolling summary attachment"
        );
    }
    Ok(())
}

#[tokio::test]
async fn recovery_oracles_reject_wrong_prefixes_replies_counts_and_attempts() -> TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness").join("tests/scenarios");
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

#[tokio::test]
async fn unrecoverable_scenario_oracles_reject_uncertain_outcomes_and_extra_dispatches()
-> TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness").join("tests/scenarios");
    for relative in [
        "recovery/unrecoverable.json",
        "summary/unrecoverable.json",
        "recovery/later-recovery.json",
    ] {
        let path = root.join(relative);
        run(Scenario::load(&root, &path)?)
            .await
            .map_err(|error| format!("{relative}: {error}"))?;
        for mutation in ["uncertain", "prefix", "dispatch", "reconciliation"] {
            let mut case = Scenario::load(&root, &path)?;
            match mutation {
                "uncertain" => {
                    if let Some(error) = &mut case.expect.error {
                        *error = "operation outcome is indeterminate".into();
                    } else {
                        *case
                            .resume
                            .get_mut(1)
                            .ok_or("missing interrupted recovery")? =
                            "operation outcome is indeterminate".into();
                    }
                }
                "prefix" => {
                    case.expect
                        .model_events
                        .as_mut()
                        .ok_or("missing retained prefix")?[0]["delta"] =
                        serde_json::json!("wrong-prefix");
                }
                "dispatch" => case.expect.request_count = Some(2),
                "reconciliation" => case.expect.reconciliation_count = Some(0),
                _ => unreachable!(),
            }
            assert!(
                run(case).await.is_err(),
                "accepted {mutation} for {relative}"
            );
        }
    }
    Ok(())
}
include!(concat!(env!("OUT_DIR"), "/scenario_cases.rs"));

#[test]
fn scenarios_cover_every_fixture() -> TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness/tests/scenarios");
    let mut paths = Vec::new();
    discover(&root, &mut paths)?;
    let discovered: BTreeSet<_> = paths.into_iter().collect();
    let registered: BTreeSet<_> = scenarios::VALUES
        .iter()
        .map(|path| root.join(path))
        .collect();
    if let Some(path) = discovered.difference(&registered).next() {
        return Err(format!("unregistered scenario: {}", path.display()).into());
    }
    if let Some(path) = registered.difference(&discovered).next() {
        return Err(format!("registered scenario is absent: {}", path.display()).into());
    }
    if let Some(filter) = scenario_filter()
        && !discovered
            .iter()
            .any(|path| path.to_string_lossy().contains(&filter))
    {
        return Err(format!("no scenarios match {filter:?}").into());
    }
    Ok(())
}

fn scenario_filter() -> Option<String> {
    acyclic_harness::test_support::environment("E2E_FILTER")
        .and_then(|value| value.into_string().ok())
}

fn run_scenario(relative: &str) -> TestResult {
    let root = acyclic_test_support::source_path("rust/crates/harness/tests/scenarios");
    let path = root.join(relative);
    if scenario_filter().is_some_and(|filter| !path.to_string_lossy().contains(&filter)) {
        return Ok(());
    }
    if acyclic_harness::test_support::environment("E2E_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(
                "warn,acyclic_harness::filesystem::task_runtime::dispatch=trace",
            ))
            .without_time()
            .try_init();
    }
    let runtime = scenario_runtime(1)?;
    let scenario = async { drivers::run(scenario::load_value(&root, &path)?).await };
    runtime.block_on(stall::guarded(&path, scenario))?
}

const BLOCKING_WORKERS: usize = 16;
const MAX_ASYNC_WORKERS: usize = 32;

fn scenario_worker_budget() -> usize {
    std::thread::available_parallelism()
        .map_or(1, usize::from)
        .clamp(2, MAX_ASYNC_WORKERS)
}

fn scenario_runtime(workers: usize) -> TestResult<tokio::runtime::Runtime> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err("scenario services must start before the async runtime".into());
    }
    acyclic_native_runtime::initialize_native_io_services()?;
    Ok(scenario_builder().worker_threads(workers).build()?)
}

fn scenario_builder() -> tokio::runtime::Builder {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .worker_threads(scenario_worker_budget())
        .max_blocking_threads(BLOCKING_WORKERS)
        .enable_all();
    builder
}

#[tokio::test]
async fn scenario_bootstrap_rejects_an_already_running_runtime() {
    let error =
        scenario_runtime(scenario_worker_budget()).expect_err("bootstrapped inside the runtime");
    assert!(
        error
            .to_string()
            .contains("services must start before the async runtime")
    );
}

fn scenario_admission<I>(
    cases: I,
    capacity: usize,
) -> impl futures::Stream<Item = <I::Item as std::future::Future>::Output>
where
    I: Iterator,
    I::Item: std::future::Future,
{
    assert!(capacity > 0, "scenario capacity is zero");
    let capacity = runtime_tests::admission_capacity(capacity);
    stream::iter(cases).buffer_unordered(capacity)
}

#[path = "remote_files.rs"]
mod remote_files;
