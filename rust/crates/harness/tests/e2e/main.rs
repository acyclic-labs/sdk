//! Data-driven harness scenarios. `E2E_FILTER` selects a path substring.
#[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
mod agents;
mod composer;
mod deadline_tests;
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
#[path = "../support/progress_guard.rs"]
mod progress_guard;
#[path = "../support/progress_guard_tests.rs"]
mod progress_guard_tests;
#[path = "../support/progress_registry.rs"]
mod progress_registry;
mod provider_case;
mod provider_http;
mod provider_tests;
mod providers;
#[path = "../support/quiescent.rs"]
mod quiescent;
mod remote_swarm;
mod resume;
mod runtime_tests;
mod scenario;
#[path = "../support/scenario_data.rs"]
mod scenario_data;
mod stall;
mod subagents;
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
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
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
    if std::env::var_os("E2E_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(
                "warn,acyclic_harness::filesystem::task_runtime::dispatch=trace",
            ))
            .without_time()
            .try_init();
    }
    let started = Instant::now();
    scenario_runtime()?.block_on(run_scenarios(started))
}

fn scenario_runtime() -> TestResult<tokio::runtime::Runtime> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err("scenario services must start before the async runtime".into());
    }
    acyclic_native_runtime::initialize_native_io_services()?;
    Ok(scenario_builder().build()?)
}

const BLOCKING_WORKERS: usize = 16;
const MAX_ASYNC_WORKERS: usize = 32;
fn scenario_builder() -> tokio::runtime::Builder {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    let available = std::thread::available_parallelism().map_or(1, usize::from);
    builder
        .worker_threads(available.clamp(2, MAX_ASYNC_WORKERS))
        .max_blocking_threads(BLOCKING_WORKERS)
        .enable_all();
    builder
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

/// Scenarios admitted per available CPU. Each admitted scenario holds a harness, its stores and its native work, so
/// admission follows the machine rather than the corpus: a larger corpus queues instead of exhausting fixtures.
const SCENARIOS_PER_CPU: usize = 4;

/// The admission bound; `E2E_SCENARIO_CAPACITY` overrides it for measurement.
fn scenario_capacity() -> TestResult<usize> {
    let capacity = match std::env::var("E2E_SCENARIO_CAPACITY") {
        Ok(capacity) => capacity.parse()?,
        Err(_) => std::thread::available_parallelism()?.get() * SCENARIOS_PER_CPU,
    };
    if capacity == 0 {
        return Err("scenario capacity is zero".into());
    }
    Ok(capacity)
}

fn scenario_admission<I>(
    cases: I,
    capacity: usize,
) -> impl futures::Stream<Item = <I::Item as std::future::Future>::Output>
where
    I: ExactSizeIterator,
    I::Item: std::future::Future,
{
    assert!(capacity > 0, "scenario capacity is zero");
    let capacity = runtime_tests::admission_capacity(capacity).min(cases.len());
    stream::iter(cases).buffer_unordered(capacity)
}

async fn run_scenarios(started: Instant) -> TestResult {
    let mut guard = progress_guard::Guard::new();
    let registry = guard.registry.clone();
    let io = guard.io.clone();
    guard
        .run(run_guarded_scenarios(started, registry, io))
        .await
        .map_err(std::io::Error::other)?
}

async fn run_guarded_scenarios(
    started: Instant,
    registry: std::sync::Arc<progress_registry::Registry>,
    io: acyclic_native_runtime::testing::Schedule,
) -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let mut paths = Vec::new();
    discover(&root, &mut paths)?;
    #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
    let local_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/local-scenarios");
    #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
    discover(&local_root, &mut paths)?;
    paths.sort();
    if let Ok(filter) = std::env::var("E2E_FILTER") {
        paths.retain(|path| path.to_string_lossy().contains(&filter));
    }
    if paths.is_empty() {
        return Err("no scenarios discovered".into());
    }
    let count = paths.len();
    let capacity = scenario_capacity()?;

    let admitted = std::sync::Arc::new((AtomicUsize::new(0), AtomicUsize::new(0)));
    let mut results = scenario_admission(
        paths.into_iter().map(|path| {
            let root = root.clone();
            let admitted = admitted.clone();
            let registry = registry.clone();
            let io = io.clone();
            #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
            let root = if path.starts_with(&local_root) {
                local_root.clone()
            } else {
                root
            };
            async move {
                let case_path = path.clone();
                let (running, peak) = &*admitted;
                peak.fetch_max(running.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                let result: TestResult = stall::run(io, registry, case_path.clone(), async move {
                    let case = scenario::load_value(&root, &case_path)?;
                    drivers::run(case).await?;
                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                })
                .await
                .unwrap_or_else(|error| Err(Box::new(error)));
                running.fetch_sub(1, Ordering::SeqCst);
                (path, result)
            }
        }),
        capacity,
    );
    let mut failures = Vec::new();
    while let Some((path, result)) = results.next().await {
        let name = path
            .strip_prefix(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"))?
            .display();
        match result {
            Ok(()) => println!("PASS {name}"),
            Err(error) => {
                eprintln!("FAIL {name}: {error}");
                failures.push(path);
            }
        }
    }
    let peak = admitted.1.load(Ordering::SeqCst);
    println!(
        "{} scenarios, {} failed, peak {} admitted of capacity {}, {:.3}s",
        count,
        failures.len(),
        peak,
        capacity,
        started.elapsed().as_secs_f64()
    );
    if peak > capacity {
        return Err(format!("{peak} scenarios ran at once, over capacity {capacity}").into());
    }
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
