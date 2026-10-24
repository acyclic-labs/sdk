//! The shared scenarios through the shipped main thread and real HTTP transport.
mod assertions;
mod bindings;
mod faults;
mod history;
#[path = "../../../../../tests/support/json_matchers.rs"]
mod json_matchers;
mod provider;
#[path = "../../../../../tests/support/scenario_actions.rs"]
mod scenario_actions;
#[path = "../../../../../tests/support/schema/action.rs"]
mod action_record;
#[path = "../../../../../tests/support/scenario_data.rs"]
mod scenario_data;
mod wire;

use acyclic_harness::OperationId;
use futures::{StreamExt as _, stream::FuturesUnordered};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::{Output, Stdio},
    time::{Duration, Instant},
};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tests")
}

async fn execute(
    workspace: &Path,
    catalog: &Path,
    settings: &Path,
    state: &Path,
    operation: OperationId,
    model: &str,
    prompt: &str,
) -> TestResult<Output> {
    let executable = std::env::var_os("CARGO_BIN_EXE_graphcoder")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_graphcoder").into());
    let mut command = tokio::process::Command::new(executable);
    command
        .current_dir(workspace)
        .arg("--workspace")
        .arg(workspace)
        .arg("--catalog")
        .arg(catalog)
        .arg("--settings-root")
        .arg(settings)
        .args([
            "--model",
            model,
            "--operation",
            &operation.to_string(),
            "-p",
            prompt,
        ])
        .env("XDG_DATA_HOME", state)
        .env("LOCALAPPDATA", state)
        .env("HOME", settings)
        .env("USERPROFILE", settings)
        .env("CODEX_HOME", settings.join(".codex"))
        .env("GRAPHCODER_MODEL", model)
        .env_remove("RUST_MIN_STACK")
        .env_remove("RUST_BACKTRACE")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    use tokio::io::AsyncReadExt as _;
    let mut child = command.spawn()?;
    let mut stdout = child.stdout.take().ok_or("binary stdout absent")?;
    let mut stderr = child.stderr.take().ok_or("binary stderr absent")?;
    let read = tokio::spawn(async move {
        let (mut out, mut err) = (Vec::new(), Vec::new());
        tokio::try_join!(stdout.read_to_end(&mut out), stderr.read_to_end(&mut err))?;
        Ok::<_, std::io::Error>((out, err))
    });
    let status = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
    let timed_out = status.is_err();
    let status = match status {
        Ok(status) => status?,
        Err(_) => {
            child.kill().await?;
            child.wait().await?
        }
    };
    let (stdout, stderr) = read.await??;
    if timed_out {
        return Err(format!(
            "binary scenario exceeded 1 s\nstdout: {}\nstderr: {}",
            assertions::last_line(&stdout),
            String::from_utf8_lossy(&stderr)
        )
        .into());
    }
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

async fn run(case: Value, protocol: &str) -> TestResult<Duration> {
    let started = Instant::now();
    #[cfg(target_os = "linux")]
    let temporary = tempfile::tempdir_in("/dev/shm")?;
    #[cfg(not(target_os = "linux"))]
    let temporary = tempfile::tempdir()?;
    let workspace = temporary.path().join("workspace");
    let settings = temporary.path().join("settings");
    let state = temporary.path().join("state");
    std::fs::create_dir(&workspace)?;
    std::fs::create_dir(&settings)?;
    for (name, text) in case["seed"].as_object().into_iter().flatten() {
        let path = workspace.join(name);
        std::fs::create_dir_all(path.parent().ok_or("seed parent missing")?)?;
        std::fs::write(path, text.as_str().ok_or("seed must be text")?)?;
    }
    let settings_files =
        serde_json::from_value(case.get("settings").cloned().unwrap_or_else(|| json!({})))?;
    scenario_data::seed_settings(&settings, &settings_files)?;
    let provider = provider::start(&case, protocol, &workspace).await?;
    let catalog = temporary.path().join("catalog.json");
    let model = if case["driver"] == "provider" {
        "model"
    } else {
        "parent"
    };
    std::fs::write(
        &catalog,
        serde_json::to_vec(&json!({"binary":{
            "adapter":protocol,"authentication":"optional","base_url":provider.url,
            "models":{(model):{"capacity":{"context_tokens":131072,"output_tokens":4096},
                "features":{"tools":true,"images":false}}},"max_event_bytes":4096,
            "token_counter":{"path":"count","adapter":protocol,"response_field":"input_tokens","framing_tokens":0}
        }}))?,
    )?;
    let operation = OperationId::new();
    let model = format!("binary/{model}");
    let prompt = case["prompt"]
        .as_str()
        .ok_or("binary scenario needs a prompt")?;
    let output = async {
        let output = execute(
            &workspace, &catalog, &settings, &state, operation, &model, prompt,
        )
        .await?;
        assertions::output(&case, operation, &output, false)?;
        if case["replay"] == true {
            let replayed = execute(
                &workspace, &catalog, &settings, &state, operation, &model, prompt,
            )
            .await?;
            assertions::output(&case, operation, &replayed, true)?;
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    let recording = provider.finish().await?;
    if let Err(error) = output {
        return Err(format!(
            "{error}; HTTP attempts: {:?}; unused replies: {:?}",
            recording.attempts, recording.remaining
        )
        .into());
    }
    assertions::recording(&case, protocol, &recording)?;
    assertions::files(&case, &workspace)?;
    for (path, expected) in case["expect"]["settings_files"]
        .as_object()
        .into_iter()
        .flatten()
    {
        if std::fs::read_to_string(settings.join(path))?
            != expected
                .as_str()
                .ok_or("settings expectation must be text")?
        {
            return Err(format!("settings file differs: {path}").into());
        }
    }
    let elapsed = started.elapsed();
    if elapsed >= Duration::from_secs(1) {
        return Err("binary scenario exceeded 1 s".into());
    }
    Ok(elapsed)
}

#[tokio::test]
async fn binary_scenarios() -> TestResult {
    run_suite(&["scenarios", "local-scenarios"]).await
}

#[tokio::test]
async fn binary_http_fault_scenarios() -> TestResult {
    run_suite(&["binary-scenarios/http"]).await
}

async fn run_suite(families: &[&str]) -> TestResult {
    if let Some(directory) = std::env::var_os("BINARY_TEST_ARTIFACTS") {
        let directory = PathBuf::from(directory);
        std::fs::create_dir_all(&directory)?;
        let executable = std::env::var_os("CARGO_BIN_EXE_graphcoder")
            .unwrap_or_else(|| env!("CARGO_BIN_EXE_graphcoder").into());
        std::fs::copy(
            executable,
            directory.join(if cfg!(windows) {
                "graphcoder.exe"
            } else {
                "graphcoder"
            }),
        )?;
        std::fs::copy(
            std::env::current_exe()?,
            directory.join(if cfg!(windows) {
                "scenarios.exe"
            } else {
                "scenarios"
            }),
        )?;
    }
    let started = Instant::now();
    let mut cases = Vec::new();
    let filter = std::env::var("E2E_FILTER").unwrap_or_default();
    for family in families {
        let root = if family.starts_with("scenarios/providers") {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../../../harness-providers/tests")
                .join(family)
        } else {
            root().join(family)
        };
        let mut paths = Vec::new();
        scenario_data::discover(&root, &mut paths)?;
        paths.sort();
        for path in paths {
            let (case, enabled) = scenario_data::load_tiers(&root, &path)?;
            if enabled && path.to_string_lossy().contains(&filter) {
                if let Some(protocol) = case["protocol"].as_str() {
                    cases.push((path, case.clone(), protocol.to_owned()));
                } else {
                    for protocol in ["responses", "anthropic"] {
                        cases.push((path.clone(), case.clone(), protocol.into()));
                    }
                }
            }
        }
    }
    if cases.is_empty() {
        return Err("no binary scenarios selected".into());
    }
    let count = cases.len();
    let mut cases = cases.into_iter().map(|(path, case, protocol)| async move {
        let result = run(case, &protocol).await;
        (path, protocol, result)
    });
    let mut results = FuturesUnordered::new();
    results.extend(cases.by_ref().take(8));
    let mut failure = None;
    while let Some((path, protocol, result)) = results.next().await {
        let name = format!("{} [{protocol}]", path.display());
        match result {
            Ok(elapsed) => println!("PASS {name} {:.3}s", elapsed.as_secs_f64()),
            Err(error) => {
                failure.get_or_insert_with(|| format!("FAIL {name}: {error}"));
            }
        }
        if failure.is_none()
            && let Some(case) = cases.next()
        {
            results.push(case);
        }
    }
    if let Some(failure) = failure {
        return Err(failure.into());
    }
    println!(
        "{count} binary scenarios passed, {:.3}s",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}
