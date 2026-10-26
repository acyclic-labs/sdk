//! Corpus parity, contract fault injection and executable report qualification.
mod policy_owner;
use acyclic_harness::{
    AgentId, Result as HarnessResult,
    package::{
        ActorPackage, ActorRuntime, Event, Placement, Snapshot, in_process::InProcessRuntime,
    },
};
use acyclic_harness_conformance::{
    check_local, check_local_composer,
    runner::{self, Status},
    scenario_data,
    substrate::{self, Actors, remote::Request},
};
use acyclic_stream::{BoxProviderFuture, LocalStream};
use serde_json::Value;
use std::{path::PathBuf, sync::Arc};

fn corpus() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../harness/tests/scenarios")
}

async fn original_corpus(root: PathBuf) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let mut paths = vec![];
    scenario_data::discover(&root, &mut paths)?;
    let count = paths.len();
    let root = Arc::new(root);
    let results = runner::run_cases(
        paths.into_iter().map(|path| {
            let root = Arc::clone(&root);
            async move {
                let mut value = scenario_data::load_value(&root, &path)?;
                let requirements = runner::requirements(&value)?;
                if requirements == ["executor"] || requirements == ["composer"] {
                    value
                        .as_object_mut()
                        .ok_or("missing recipe")?
                        .remove("driver");
                    let result = if requirements == ["composer"] {
                        check_local_composer(value).await
                    } else {
                        check_local(value).await
                    };
                    result.map_err(|e| format!("{}: {e}", path.display()))?;
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            }
        }),
        std::thread::available_parallelism()?,
    )
    .await?;
    for result in results {
        result?;
    }
    Ok(count)
}

#[tokio::test(flavor = "multi_thread")]
async fn corpus_matches_original_driver_and_reports_every_case()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    enum Traversal {
        Original(usize),
        Hosted(runner::Report),
        Local(runner::Report),
    }
    // These fixtures share no state or observations. Give each traversal its
    // own runnable task instead of a serial original-driver phase followed by
    // two CPU-heavy futures polled by one parent. JoinSet owns cancellation on
    // every error path; all three results must arrive before parity is judged.
    let mut traversals = tokio::task::JoinSet::new();
    traversals.spawn(async { original_corpus(corpus()).await.map(Traversal::Original) });
    traversals.spawn(async {
        runner::run(
            &corpus(),
            Arc::new(substrate::InProcess),
            None,
            std::thread::available_parallelism()?.get(),
        )
        .await
        .map(Traversal::Hosted)
    });
    traversals.spawn(async {
        runner::run(
            &corpus(),
            Arc::new(substrate::Local),
            None,
            std::thread::available_parallelism()?.get(),
        )
        .await
        .map(Traversal::Local)
    });
    let (mut count, mut report, mut local) = (None, None, None);
    while let Some(result) = traversals.join_next().await {
        match result?? {
            Traversal::Original(value) => count = Some(value),
            Traversal::Hosted(value) => report = Some(value),
            Traversal::Local(value) => local = Some(value),
        }
    }
    let count = count.ok_or("original traversal did not return its case count")?;
    let report = report.ok_or("hosted traversal did not return its report")?;
    let local = local.ok_or("local traversal did not return its report")?;
    for record in &report.records {
        assert_ne!(
            record.status,
            Status::Fail,
            "{}: {}",
            record.case,
            record.reason
        );
    }
    assert_eq!(report.records.len(), count);
    assert!(report.success());
    println!(
        "{} cases: {} passed, {} skipped",
        count, report.passed, report.skipped
    );
    assert_eq!(serde_json::to_value(local)?, serde_json::to_value(report)?);
    Ok(())
}

#[tokio::test]
async fn admitted_cases_have_independent_task_ownership()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for concurrency in 1usize..=4 {
        for count in 0..=8 {
            let parent = tokio::task::try_id();
            let active = Arc::new(AtomicUsize::new(0));
            let peak = AtomicUsize::new(0);
            let tasks = runner::run_cases(
                (0..count).map(|_| {
                    let active = Arc::clone(&active);
                    peak.fetch_max(active.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                    async move {
                        active.fetch_sub(1, Ordering::SeqCst);
                        tokio::task::try_id()
                    }
                }),
                concurrency.try_into()?,
            )
            .await?;
            assert_eq!(tasks.len(), count);
            assert_eq!(active.load(Ordering::SeqCst), 0);
            assert!(peak.load(Ordering::SeqCst) <= concurrency);
            assert!(tasks.iter().all(|task| task.is_some() && *task != parent));
            assert_eq!(
                tasks.iter().collect::<std::collections::HashSet<_>>().len(),
                count
            );
        }
    }
    Ok(())
}

struct DropCompletion<R>(R);
impl<R: ActorRuntime> ActorRuntime for DropCompletion<R> {
    fn install(&self, package: Arc<dyn ActorPackage>) -> BoxProviderFuture<'_, HarnessResult<()>> {
        self.0.install(package)
    }
    fn place(
        &self,
        placement: Placement,
        machine: acyclic_harness::workflow::MachineIdentity,
        input: Value,
    ) -> BoxProviderFuture<'_, HarnessResult<Snapshot>> {
        self.0.place(placement, machine, input)
    }
    fn deliver(
        &self,
        placement: Placement,
        event: Event,
    ) -> BoxProviderFuture<'_, HarnessResult<Snapshot>> {
        self.0.deliver(placement, event)
    }
    fn recover(&self, placement: Placement) -> BoxProviderFuture<'_, HarnessResult<Snapshot>> {
        Box::pin(async move {
            let mut snapshot = self.0.recover(placement).await?;
            snapshot.consumed -= 1;
            Ok(snapshot)
        })
    }
    fn cancel(
        &self,
        placement: Placement,
        event: Event,
    ) -> BoxProviderFuture<'_, HarnessResult<Snapshot>> {
        self.0.cancel(placement, event)
    }
}

#[tokio::test]
async fn lost_contract_observation_fails_and_filter_never_drops_cases()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let volume = Arc::new(
        acyclic_harness::filesystem::MemoryHarnessStorage::new(AgentId::new(), 1_048_576).await?,
    );
    let bindings = substrate::fixture_bindings();
    let stream = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
    let executor = Arc::new(substrate::Providers {
        stream: stream.clone(),
        filesystem: acyclic_fs::Fs::memory(),
    });
    let runtime = InProcessRuntime::new(
        stream,
        bindings.clone(),
        Arc::new(substrate::FixturePolicy),
        Arc::new(substrate::FixtureEffects::new(volume.clone(), executor)),
    );
    let broken = Actors {
        runtime: DropCompletion(substrate::Supervised::new(runtime)),
        volume,
        bindings,
    };
    let report = runner::run(&corpus(), Arc::new(broken), Some("core/text.json"), 1).await?;
    assert_eq!(report.failed, 1);
    assert_eq!(report.passed, 0);
    assert!(!report.success());
    assert!(
        report
            .records
            .iter()
            .any(|r| r.reason.contains("lost scenario completion"))
    );
    let mut paths = vec![];
    scenario_data::discover(&corpus(), &mut paths)?;
    assert_eq!(report.records.len(), paths.len());
    Ok(())
}

#[tokio::test]
async fn endpoint_bridge_uses_the_same_pinned_package_and_oracles()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let stream = Arc::new(LocalStream::open(directory.path(), Default::default()).await?);
    let filesystem = acyclic_fs::Fs::memory();
    let volume = Arc::new(
        acyclic_harness::filesystem::MemoryHarnessStorage::from_providers(
            AgentId::new(),
            1_048_576,
            filesystem.clone(),
            stream.clone(),
        )
        .await?,
    );
    let bindings = substrate::fixture_bindings();
    let executor = Arc::new(substrate::Providers {
        stream: stream.clone(),
        filesystem,
    });
    let runtime = Arc::new(substrate::Supervised::new(InProcessRuntime::new(
        stream,
        bindings.clone(),
        Arc::new(substrate::FixturePolicy),
        Arc::new(substrate::FixtureEffects::new(volume.clone(), executor)),
    )));
    let handler = move |axum::Json(request): axum::Json<Request>| {
        let runtime = runtime.clone();
        let volume = volume.clone();
        async move {
            axum::Json(
                request
                    .apply(runtime.as_ref(), volume.as_ref())
                    .await
                    .map_err(|e| e.to_string()),
            )
        }
    };
    let router = axum::Router::new().route("/contract", axum::routing::post(handler));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}/contract", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let http = substrate::remote::Http::new(endpoint)?;
    let host = Actors {
        runtime: http.clone(),
        volume: http,
        bindings,
    };
    let cases = tempfile::tempdir()?;
    for path in ["core/text.json", "composer/mentions/pinned.json"] {
        let value = scenario_data::load_value(&corpus(), &corpus().join(path))?;
        std::fs::write(
            cases.path().join(path.replace('/', "_")),
            serde_json::to_vec(&value)?,
        )?;
    }
    let result = runner::run(cases.path(), Arc::new(host), None, 2).await;
    server.abort();
    let report = result?;
    assert_eq!(
        report.failed,
        0,
        "{:?}",
        report
            .records
            .iter()
            .filter(|r| r.status == Status::Fail)
            .collect::<Vec<_>>()
    );
    assert_eq!(report.passed, 2);
    assert!(report.success());
    Ok(())
}

#[test]
fn cli_reports_are_deterministic_and_empty_selection_fails()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let report = directory.path().join("report.json");
    let command = |filter: &str| {
        std::process::Command::new(env!("CARGO_BIN_EXE_scenarios"))
            .env("CODEX_HOME", directory.path().join("codex"))
            .args(["--substrate", "in-process", "--filter", filter, "--report"])
            .arg(&report)
            .output()
    };
    let first = command("core/text.json")?;
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let bytes = std::fs::read(&report)?;
    let second = command("core/text.json")?;
    assert!(second.status.success());
    assert_eq!(bytes, std::fs::read(&report)?);
    let missing = command("no-such-case")?;
    assert!(!missing.status.success());
    let result: runner::Report = serde_json::from_slice(&std::fs::read(report)?)?;
    assert_eq!(result.passed, 0);
    assert_eq!(result.failed, 0);
    assert_eq!(result.skipped, result.records.len());
    Ok(())
}

#[tokio::test]
async fn bad_fixture_and_wrong_expected_answer_each_fail_with_a_record()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let mut value: Value = serde_json::from_slice(include_bytes!(
        "../../harness/tests/scenarios/core/text.json"
    ))?;
    *value
        .pointer_mut("/expect/text")
        .ok_or("missing text expectation")? = "WRONG".into();
    std::fs::write(
        directory.path().join("wrong.json"),
        serde_json::to_vec(&value)?,
    )?;
    std::fs::write(
        directory.path().join("unknown.json"),
        r#"{"driver":"typo"}"#,
    )?;
    std::fs::write(directory.path().join("malformed.json"), "{")?;
    let report = runner::run(directory.path(), Arc::new(substrate::InProcess), None, 1).await?;
    assert_eq!(report.records.len(), 3);
    assert_eq!(report.failed, 3);
    assert!(!report.success());
    Ok(())
}

#[tokio::test]
async fn malformed_known_drivers_fail_before_capability_and_filter_skips()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let drivers = [
        "local",
        "goal-local",
        "agents",
        "composer_entry",
        "notifications",
        "task_query",
        "subagents",
        "swarm",
        "plans",
        "executor",
        "composer",
    ];
    for driver in drivers {
        std::fs::write(
            directory.path().join(format!("{driver}.json")),
            serde_json::to_vec(&serde_json::json!({"driver":driver}))?,
        )?;
    }
    std::fs::write(
        directory.path().join("good.json"),
        include_bytes!("../../harness/tests/scenarios/core/text.json"),
    )?;
    for substrate in [
        Arc::new(substrate::Local) as Arc<dyn substrate::Substrate>,
        Arc::new(substrate::InProcess),
    ] {
        for filter in [None, Some("good")] {
            let report = runner::run(directory.path(), substrate.clone(), filter, 4).await?;
            assert_eq!(report.records.len(), drivers.len() + 1);
            assert_eq!(report.failed, drivers.len());
            assert_eq!(report.passed, 1);
            assert_eq!(report.skipped, 0);
            assert!(!report.success());
        }
    }
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_scenarios"))
        .args(["--substrate", "in-process", "--corpus"])
        .arg(directory.path())
        .arg("--report")
        .arg(directory.path().join("report.out"))
        .output()?;
    assert!(!output.status.success());
    Ok(())
}

#[tokio::test]
async fn client_oracles_reject_mutations_on_both_substrates()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let baseline: Value = serde_json::from_slice(include_bytes!(
        "../../harness/tests/scenarios/core/text.json"
    ))?;
    let mutations = [
        ("text", serde_json::json!("WRONG")),
        (
            "requests",
            serde_json::json!([{ "messages": [{ "content": "WRONG" }] }]),
        ),
        (
            "not_requests",
            serde_json::json!([{ "messages": [{ "role": "user" }] }]),
        ),
        ("events", serde_json::json!([])),
        (
            "not_events",
            serde_json::json!([{ "kind": "model_started" }]),
        ),
        ("model_events", serde_json::json!([])),
        ("result", serde_json::json!({ "steps": 99 })),
        ("files", serde_json::json!({ "missing.txt": "WRONG" })),
        ("request_count", serde_json::json!(0)),
        ("reconciliation_count", serde_json::json!(1)),
        ("request_files", serde_json::json!([["WRONG"]])),
    ];
    let count = mutations.len();
    for (field, mutation) in mutations {
        let mut value = baseline.clone();
        value
            .get_mut("expect")
            .and_then(Value::as_object_mut)
            .ok_or("missing expectations")?
            .insert(field.into(), mutation);
        std::fs::write(
            directory.path().join(format!("{field}.json")),
            serde_json::to_vec(&value)?,
        )?;
    }
    for substrate in [
        Arc::new(substrate::Local) as Arc<dyn substrate::Substrate>,
        Arc::new(substrate::InProcess),
    ] {
        let report = runner::run(directory.path(), substrate, None, 4).await?;
        assert_eq!(report.records.len(), count);
        assert_eq!(report.failed, count, "{report:?}");
        assert!(!report.success());
    }
    Ok(())
}

#[tokio::test]
async fn composer_oracle_mutations_fail_on_both_substrates()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let mutations = [
        (
            "composer/mentions/pinned.json",
            "result",
            serde_json::json!({"kind":"WRONG"}),
        ),
        (
            "composer/mentions/pinned.json",
            "mention_files",
            serde_json::json!(["WRONG"]),
        ),
        (
            "composer/mentions/pinned.json",
            "files",
            serde_json::json!({"missing.txt":"WRONG"}),
        ),
        (
            "composer/mentions/pinned.json",
            "file_values",
            serde_json::json!({"missing.json":{}}),
        ),
        (
            "composer/builtins/completion-all.json",
            "completions",
            serde_json::json!(["WRONG"]),
        ),
    ];
    let count = mutations.len();
    for (index, (path, field, mutation)) in mutations.into_iter().enumerate() {
        let mut value = scenario_data::load_value(&corpus(), &corpus().join(path))?;
        value
            .get_mut("expect")
            .and_then(Value::as_object_mut)
            .ok_or("missing expectations")?
            .insert(field.into(), mutation);
        std::fs::write(
            directory.path().join(format!("{index}.json")),
            serde_json::to_vec(&value)?,
        )?;
    }
    for substrate in [
        Arc::new(substrate::Local) as Arc<dyn substrate::Substrate>,
        Arc::new(substrate::InProcess),
    ] {
        let report = runner::run(directory.path(), substrate, None, 4).await?;
        assert_eq!(report.records.len(), count);
        assert_eq!(report.failed, count, "{report:?}");
        assert!(!report.success());
    }
    Ok(())
}
