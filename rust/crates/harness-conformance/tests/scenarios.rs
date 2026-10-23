//! Corpus parity, contract fault injection and executable report qualification.
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
use futures::{StreamExt, stream};
use serde_json::Value;
use std::{path::PathBuf, sync::Arc};

fn corpus() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../harness/tests/scenarios")
}

#[tokio::test(flavor = "multi_thread")]
async fn corpus_matches_original_driver_and_reports_every_case()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let root = corpus();
    let mut paths = vec![];
    scenario_data::discover(&root, &mut paths)?;
    let count = paths.len();
    let results = stream::iter(paths.into_iter().map(|path| {
        let root = &root;
        async move {
            let mut value = scenario_data::load_value(root, &path)?;
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
    }))
    .buffer_unordered(std::thread::available_parallelism()?.get())
    .collect::<Vec<_>>()
    .await;
    for result in results {
        result?;
    }
    let (report, local) = tokio::join!(
        runner::run(
            &root,
            &substrate::InProcess,
            None,
            std::thread::available_parallelism()?.get(),
        ),
        runner::run(
            &root,
            &substrate::Local,
            None,
            std::thread::available_parallelism()?.get()
        )
    );
    let report = report?;
    let local = local?;
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

struct DropCompletion<R>(R);
impl<R: ActorRuntime> ActorRuntime for DropCompletion<R> {
    fn install(&self, package: Arc<dyn ActorPackage>) -> BoxProviderFuture<'_, HarnessResult<()>> {
        self.0.install(package)
    }
    fn place(
        &self,
        placement: Placement,
        input: Value,
    ) -> BoxProviderFuture<'_, HarnessResult<Snapshot>> {
        self.0.place(placement, input)
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
    let report = runner::run(&corpus(), &broken, Some("core/text.json"), 1).await?;
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
    let result = runner::run(cases.path(), &host, None, 2).await;
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
    let report = runner::run(directory.path(), &substrate::InProcess, None, 1).await?;
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
        "plan-query",
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
        &substrate::Local as &dyn substrate::Substrate,
        &substrate::InProcess,
    ] {
        for filter in [None, Some("good")] {
            let report = runner::run(directory.path(), substrate, filter, 4).await?;
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
        &substrate::Local as &dyn substrate::Substrate,
        &substrate::InProcess,
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
        &substrate::Local as &dyn substrate::Substrate,
        &substrate::InProcess,
    ] {
        let report = runner::run(directory.path(), substrate, None, 4).await?;
        assert_eq!(report.records.len(), count);
        assert_eq!(report.failed, count, "{report:?}");
        assert!(!report.success());
    }
    Ok(())
}
