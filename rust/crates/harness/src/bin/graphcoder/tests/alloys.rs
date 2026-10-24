//! The same data cases qualify the shipping binary under the serialized Windows helper.
use super::*;
use axum::{body::Body, extract::State, http::StatusCode, response::Response};
use harness_mock_model::{Engine, Script, bind_result, events_with_usage};
use std::sync::Mutex;

type FixtureResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn crash_observer_drains_both_pipes_before_waiting_for_the_effect() -> FixtureResult {
    let directory = tempfile::tempdir()?;
    let marker = directory.path().join("effect");
    eprintln!("PIPE_PROOF_DIRECTORY={}", directory.path().display());
    // More than either platform's anonymous pipe capacity. The marker cannot
    // become visible until the parent has drained both streams.
    #[cfg(unix)]
    let mut command = {
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            "set -e; printf '%s' $$ > \"$PIPE_PRODUCER_PID\"; head -c 1048576 /dev/zero; head -c 1048576 /dev/zero >&2; printf done > \"$PIPE_EFFECT_MARKER.tmp\"; mv \"$PIPE_EFFECT_MARKER.tmp\" \"$PIPE_EFFECT_MARKER\"",
        ]);
        command
    };
    #[cfg(not(unix))]
    let mut command = {
        let data = directory.path().join("data");
        let script = directory.path().join("producer.cmd");
        std::fs::write(&data, vec![b'x'; 1048576])?;
        // Console.Write silently accepts a broken pipe on Windows. The native
        // type command fails instead, so the marker requires both full writes.
        std::fs::write(
            &script,
            "@echo off\r\ntype \"%PIPE_DATA_FILE%\" && type \"%PIPE_DATA_FILE%\" 1>&2 && (echo done)>\"%PIPE_EFFECT_MARKER%.tmp\" && move /y \"%PIPE_EFFECT_MARKER%.tmp\" \"%PIPE_EFFECT_MARKER%\" >nul\r\n",
        )?;
        let mut command = Command::new("cmd.exe");
        command
            .args(["/d", "/c"])
            .arg(script)
            .env("PIPE_DATA_FILE", data);
        command
    };
    command
        .env("PIPE_EFFECT_MARKER", &marker)
        .env("PIPE_PRODUCER_PID", directory.path().join("producer.pid"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let result = run(command, Some(marker.clone())).await;
    assert_eq!(std::fs::read_to_string(&marker)?.trim_end(), "done");
    match result {
        Ok(output) => {
            assert!(output.status.success());
            assert_eq!(output.stdout.len(), 1048576);
            assert_eq!(output.stderr.len(), 1048576);
        }
        Err(error) => assert!(error.starts_with("killed"), "{error}"),
    }
    Ok(())
}

struct Fixture {
    engine: Engine,
    records: Vec<Value>,
    killed: std::path::PathBuf,
}

fn required<T>(value: Option<T>, label: &str) -> std::io::Result<T> {
    value.ok_or_else(|| std::io::Error::other(format!("missing alloy fixture {label}")))
}

fn field<'a>(value: &'a Value, pointer: &str) -> &'a Value {
    value.pointer(pointer).unwrap_or(&Value::Null)
}

async fn reply(
    State(state): State<Arc<Mutex<Fixture>>>,
    uri: axum::http::Uri,
    Json(body): Json<Value>,
) -> Result<Response, (StatusCode, String)> {
    fixture_reply(&state, &uri, &body)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))
}

fn fixture_reply(
    state: &Mutex<Fixture>,
    uri: &axum::http::Uri,
    body: &Value,
) -> FixtureResult<Response> {
    let path = uri
        .path()
        .trim_start_matches('/')
        .strip_prefix("v1/")
        .unwrap_or_default();
    let mut state = state
        .lock()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let tokens = required(
        state.engine.input_tokens("alloy", body),
        "input token count",
    )?;
    let (step, status, bytes, hold) = match state.engine.take("alloy", path, body) {
        Some((index, step)) => {
            let step = bind_result(step, body)?;
            let bytes = match &step.json {
                Some(json) => json.to_string(),
                None => {
                    required(events_with_usage(path, &step, tokens), "response events")?.concat()
                }
            };
            (Some(index), step.status.unwrap_or(200), bytes, step.hold)
        }
        None if path == "count" => (None, 200, json!({"input_tokens":tokens}).to_string(), false),
        None => (None, 409, "unmatched alloy request".into(), false),
    };
    state
        .records
        .push(json!({"path":path,"step":step,"status":status,"body":body}));
    if status == 409 {
        eprintln!(
            "unmatched alloy tool result: {}",
            harness_mock_model::last_content(body, true)
        );
    }
    if hold {
        std::fs::write(&state.killed, b"accepted")?;
    }
    let body = if hold {
        Body::from_stream(futures::stream::pending::<
            Result<axum::body::Bytes, std::io::Error>,
        >())
    } else {
        Body::from(bytes)
    };
    Ok(Response::builder()
        .status(StatusCode::from_u16(status)?)
        .header(
            "content-type",
            if path == "count" {
                "application/json"
            } else {
                "text/event-stream"
            },
        )
        .body(body)?)
}

fn expand(value: &mut Value, url: &str) {
    match value {
        Value::String(text) => *text = text.replace("{{url}}", url),
        Value::Array(values) => values.iter_mut().for_each(|value| expand(value, url)),
        Value::Object(values) => values.values_mut().for_each(|value| expand(value, url)),
        _ => {}
    }
}

fn subset(actual: &Value, expected: &Value) -> bool {
    match expected {
        Value::Object(fields) => fields
            .iter()
            .all(|(name, value)| actual.get(name).is_some_and(|actual| subset(actual, value))),
        _ => actual == expected,
    }
}

async fn case(document: &str) -> FixtureResult {
    let mut case: Value = serde_json::from_str(document)?;
    let directory = tempfile::tempdir()?;
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    for path in [&workspace, &settings] {
        std::fs::create_dir(path)?;
    }
    if let Some(files) = field(&case, "/settings").as_object() {
        for (path, text) in files {
            let path = settings.join(path);
            std::fs::create_dir_all(required(path.parent(), "settings parent")?)?;
            std::fs::write(path, required(text.as_str(), "settings text")?)?;
        }
    }
    let killed = directory.path().join("kill-ready");
    let state = Arc::new(Mutex::new(Fixture {
        engine: Engine::new(Script {
            cases: [(
                "alloy".into(),
                serde_json::from_value(field(&case, "/model").clone())?,
            )]
            .into(),
        }),
        records: Vec::new(),
        killed: killed.clone(),
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    expand(
        required(case.get_mut("catalog"), "catalog")?,
        &format!("http://{}/v1", listener.local_addr()?),
    );
    let app = Router::new().fallback(reply).with_state(state.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, field(&case, "/catalog").to_string())?;
    for input in required(field(&case, "/inputs").as_array(), "inputs")? {
        let command = case_command(&case, input, &workspace, &settings, &catalog)?;
        let result = run(command, input.get("kill_on").map(|_| killed.clone())).await;
        if input.get("kill_on").is_some() {
            assert!(matches!(result, Err(ref error) if error.starts_with("killed")));
        } else {
            verify_output(&case, input, &result.map_err(std::io::Error::other)?)?;
        }
    }
    verify(
        &case,
        &state
            .lock()
            .map_err(|error| std::io::Error::other(error.to_string()))?
            .records,
        &workspace,
    )?;
    server.abort();
    Ok(())
}

fn verify_output(case: &Value, input: &Value, output: &std::process::Output) -> FixtureResult {
    assert_eq!(
        output.status.code(),
        Some(i32::try_from(field(input, "/exit").as_i64().unwrap_or(0))?),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    for expected in field(input, "/stderr_contains")
        .as_array()
        .into_iter()
        .flatten()
    {
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains(required(expected.as_str(), "stderr text")?)
        );
    }
    if output.status.success() {
        let outcomes = results(&output.stdout);
        let outcome = required(outcomes.last(), "final output")?;
        assert_eq!(field(outcome, "/ok/text"), field(input, "/answer"));
        if let Some(usage) = field(case, "/assert").get("member_usage") {
            assert!(
                subset(field(outcome, "/ok/metadata/members"), usage),
                "{outcome}"
            );
        }
    }
    Ok(())
}

fn case_command(
    case: &Value,
    input: &Value,
    workspace: &Path,
    settings: &Path,
    catalog: &Path,
) -> FixtureResult<Command> {
    let mut command = Command::new(build_metadata::executable());
    command
        .current_dir(workspace)
        .arg("--workspace")
        .arg(workspace)
        .arg("--settings-root")
        .arg(settings)
        .arg("--catalog")
        .arg(catalog)
        .args([
            "--operation",
            "00000000-0000-0000-0000-000000000001",
            "-p",
            required(field(input, "/text").as_str(), "input text")?,
        ])
        .env("HOME", settings)
        .env("USERPROFILE", settings)
        .env("CODEX_HOME", settings.join(".codex"))
        .env("XDG_DATA_HOME", settings)
        .env("LOCALAPPDATA", settings)
        .env_remove("GRAPHCODER_MODEL")
        .env_remove("OPENAI_API_KEY")
        .env_remove("ANTHROPIC_API_KEY")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(selection) = field(case, "/selection").as_str() {
        command.args(["--model", selection]);
    }
    Ok(command)
}

fn verify(case: &Value, records: &[Value], workspace: &Path) -> FixtureResult {
    let generations: Vec<_> = records
        .iter()
        .filter(|record| field(record, "/path") != "count")
        .collect();
    assert_eq!(
        generations.len() as u64,
        required(field(case, "/assert/requests").as_u64(), "request count")?
    );
    assert!(
        records.iter().all(|record| field(record, "/status") != 409),
        "{records:?}"
    );
    if let Some(paths) = field(case, "/assert").get("generation_paths") {
        assert_eq!(
            Value::Array(
                generations
                    .iter()
                    .map(|record| field(record, "/path").clone())
                    .collect()
            ),
            *paths
        );
    }
    if let Some(counts) = field(case, "/assert/generation_counts").as_object() {
        for (path, count) in counts {
            assert_eq!(
                generations
                    .iter()
                    .filter(|record| field(record, "/path") == path)
                    .count() as u64,
                required(count.as_u64(), "path count")?
            );
        }
    }
    if let Some(period) = field(case, "/assert/generation_period").as_u64() {
        let period = usize::try_from(period)?;
        assert!(period > 0);
        for (index, record) in generations.iter().enumerate() {
            let expected = required(generations.get(index % period), "period member")?;
            assert_eq!(field(record, "/path"), field(expected, "/path"));
        }
    }
    let mut consumed: Vec<_> = records
        .iter()
        .filter_map(|record| field(record, "/step").as_u64())
        .collect();
    consumed.sort_unstable();
    assert_eq!(
        consumed,
        (0..required(field(case, "/model/steps").as_array(), "model steps")?.len() as u64)
            .collect::<Vec<_>>()
    );
    for expected in field(case, "/assert/http").as_array().into_iter().flatten() {
        assert!(
            records.iter().any(|record| subset(record, expected)),
            "{expected}"
        );
    }
    if let Some(files) = field(case, "/assert/files").as_object() {
        for (path, text) in files {
            assert_eq!(
                std::fs::read_to_string(workspace.join(path))?,
                required(text.as_str(), "file text")?
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn alloy_round_robin() -> FixtureResult {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-round-robin.json"
    ))
    .await
}
#[tokio::test]
async fn alloy_weighted() -> FixtureResult {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-weighted.json"
    ))
    .await
}
#[tokio::test]
async fn alloy_per_role() -> FixtureResult {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-per-role.json"
    ))
    .await
}
#[tokio::test]
async fn alloy_settings() -> FixtureResult {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-settings.json"
    ))
    .await
}
#[tokio::test]
async fn alloy_capacity() -> FixtureResult {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-capacity.json"
    ))
    .await
}
#[tokio::test]
async fn alloy_retry() -> FixtureResult {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-retry.json"
    ))
    .await
}
#[tokio::test]
async fn alloy_resume() -> FixtureResult {
    case(include_str!(
        "../../../../../../../scripts/graphcoder-e2e/cases/alloy-resume.json"
    ))
    .await
}
