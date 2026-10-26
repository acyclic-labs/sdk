//! Corpus parity, oracle fault injection and executable report qualification.
mod policy_owner;
use acyclic_harness_conformance::{
    check_local, check_local_composer,
    runner::{self, Status},
    scenario_data, substrate,
};
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
    // The traversals share no state; each gets its own runnable task.
    let original = tokio::spawn(original_corpus(corpus()));
    let report = runner::run(
        &corpus(),
        Arc::new(substrate::Local),
        None,
        std::thread::available_parallelism()?.get(),
    )
    .await?;
    let count = original.await??;
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

#[test]
fn cli_reports_are_deterministic_and_empty_selection_fails()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directory = tempfile::tempdir()?;
    let report = directory.path().join("report.json");
    let command = |filter: &str| {
        std::process::Command::new(env!("CARGO_BIN_EXE_scenarios"))
            .env("CODEX_HOME", directory.path().join("codex"))
            .args(["--substrate", "local", "--filter", filter, "--report"])
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
    let report = runner::run(directory.path(), Arc::new(substrate::Local), None, 1).await?;
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
    for filter in [None, Some("good")] {
        let report = runner::run(directory.path(), Arc::new(substrate::Local), filter, 4).await?;
        assert_eq!(report.records.len(), drivers.len() + 1);
        assert_eq!(report.failed, drivers.len());
        assert_eq!(report.passed, 1);
        assert_eq!(report.skipped, 0);
        assert!(!report.success());
    }
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_scenarios"))
        .args(["--substrate", "local", "--corpus"])
        .arg(directory.path())
        .arg("--report")
        .arg(directory.path().join("report.out"))
        .output()?;
    assert!(!output.status.success());
    Ok(())
}

#[tokio::test]
async fn client_oracles_reject_mutations() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
    let report = runner::run(directory.path(), Arc::new(substrate::Local), None, 4).await?;
    assert_eq!(report.records.len(), count);
    assert_eq!(report.failed, count, "{report:?}");
    assert!(!report.success());
    Ok(())
}

#[tokio::test]
async fn composer_oracle_mutations_fail() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
    let report = runner::run(directory.path(), Arc::new(substrate::Local), None, 4).await?;
    assert_eq!(report.records.len(), count);
    assert_eq!(report.failed, count, "{report:?}");
    assert!(!report.success());
    Ok(())
}
