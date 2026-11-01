//! The shipping binary over a loopback model: argv, provider catalog, the local
//! harness and its root agent, end to end.
use acyclic_native_runtime::{ProcessCommand as Command, ProcessStdio as Stdio};
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Output,
    sync::{Arc, Mutex},
};

const ANSWER: &str = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"fixture answer\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":64,\"output_tokens\":2}}}\n\n";

mod build_metadata {
    #[expect(
        clippy::disallowed_macros,
        reason = "Cargo-provided executable metadata for this integration crate"
    )]
    pub(super) const EXECUTABLE: &str = env!("CARGO_BIN_EXE_graphcoder");
}

/// A loopback Responses endpoint; it records every generation request body.
struct Model {
    catalog: PathBuf,
    requests: Arc<Mutex<Vec<Value>>>,
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
}

async fn model(directory: &Path) -> Model {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&requests);
    let app = Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens": 64})) }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                recorded.lock().unwrap().push(body);
                async { ([("content-type", "text/event-stream")], ANSWER) }
            }),
        );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let catalog = directory.join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"graphcoder-qual":{
        "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
        "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},"max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
    }})).unwrap()).unwrap();
    Model {
        catalog,
        requests,
        stop,
        server,
    }
}

impl Model {
    fn generated(&self) -> usize {
        self.requests.lock().unwrap().len()
    }

    async fn close(self) {
        self.stop.send(()).unwrap();
        self.server.await.unwrap();
    }
}

struct Fixture {
    custody: acyclic_native_runtime::signing_key::FixtureCustody,
    _directory: tempfile::TempDir,
    workspace: PathBuf,
    settings: PathBuf,
    home: PathBuf,
}

fn fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    let home = directory.path().join("read-only-home");
    std::fs::create_dir(&home).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o500)).unwrap();
    }
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(settings.join(".codex")).unwrap();
    Fixture {
        custody: acyclic_native_runtime::signing_key::FixtureCustody::new(
            &directory.path().join("custody"),
        )
        .unwrap(),
        _directory: directory,
        workspace,
        settings,
        home,
    }
}

/// Runs the binary through the shared native capture, exit and physical-close
/// owner, retaining the original outer deadline without readiness polling.
async fn graphcoder(fixture: &Fixture, catalog: &Path, args: Vec<&str>, stdin: &str) -> Output {
    let mut command = Command::new(build_metadata::EXECUTABLE);
    fixture.custody.configure(|key, value| {
        command.env(key, value);
    });
    command
        .env("CODEX_HOME", fixture.settings.join(".codex"))
        .env("HOME", &fixture.home)
        .env("USERPROFILE", &fixture.settings)
        .env("LOCALAPPDATA", &fixture.settings)
        .env("XDG_DATA_HOME", &fixture.settings)
        .env("TERM", "dumb")
        .env("GRAPHCODER_LOG", "warn")
        .env_remove("GRAPHCODER_MODEL")
        .env_remove("OPENAI_API_KEY")
        .current_dir(&fixture.workspace)
        .arg("--workspace")
        .arg(&fixture.workspace)
        .arg("--settings-root")
        .arg(&fixture.settings)
        .arg("--catalog")
        .arg(catalog)
        .args(["--model", "graphcoder-qual/tiny"])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let stdin = stdin.to_owned();
    tokio::task::spawn_blocking(move || {
        use acyclic_native_runtime::{CaptureInput, ProcessInput, ProcessTree};
        let mut child = ProcessTree::spawn(&mut command).expect("spawn GraphCoder process tree");
        child
            .wait_with_output(
                std::time::Duration::from_secs(60),
                usize::MAX,
                Some(CaptureInput::Literal(stdin.as_bytes())),
                || Ok(()),
                |_| Ok(ProcessInput::default()),
                None,
                None,
                None,
            )
            .unwrap_or_else(|failure| {
                panic!(
                    "GraphCoder capture failed: {}; physical_tree_retired={}; stdout={}; stderr={}",
                    failure.error,
                    failure.cleanup_completed,
                    String::from_utf8_lossy(&failure.stdout),
                    String::from_utf8_lossy(&failure.stderr)
                )
            })
    })
    .await
    .unwrap()
}

fn succeeded(output: &Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(
        output.status.success(),
        "exit {:?}\n{stdout}\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );
    stdout
}

#[tokio::test]
async fn a_prompt_runs_one_agent_turn_and_its_operation_replays_exactly() {
    let fixture = fixture();
    // The actual CLI processes share only the parent's in-memory fixture store;
    // they cannot use HOME for a credential or transaction-lock fallback.
    #[cfg(unix)]
    assert!(std::fs::write(fixture.home.join("must-not-write"), b"refused").is_err());
    let model = model(fixture._directory.path()).await;
    let run = |operation: &'static str| {
        graphcoder(
            &fixture,
            &model.catalog,
            vec!["--operation", operation, "-p", "hello"],
            "",
        )
    };
    assert_eq!(succeeded(&run("first").await), "fixture answer\n");
    assert_eq!(model.generated(), 1);
    // The same operation names the same message: a second process prints the
    // settled answer without generating again.
    assert_eq!(succeeded(&run("first").await), "fixture answer\n");
    assert_eq!(model.generated(), 1);
    // Another operation is a new message to the same root agent.
    assert_eq!(succeeded(&run("second").await), "fixture answer\n");
    assert_eq!(model.generated(), 2);
    // Invalid user settings cannot become an implicit model or tool source in
    // default-config mode. Ordinary MCP loading still rejects these same bytes.
    std::fs::write(fixture.settings.join(".codex/config.toml"), "[invalid\n").unwrap();
    // A fresh explicit owner must use its selected file custody on the first
    // process and reopen that same custody on replay, ignoring the configured
    // fixture backend as well as the malformed ambient settings.
    let explicit = self::fixture();
    std::fs::write(explicit.settings.join(".codex/config.toml"), "[invalid\n").unwrap();
    let usage = explicit._directory.path().join("usage.jsonl");
    for _ in 0..2 {
        assert_eq!(
            succeeded(
                &graphcoder(
                    &explicit,
                    &model.catalog,
                    vec![
                        "--default-config",
                        "--usage-jsonl",
                        usage.to_str().unwrap(),
                        "--operation",
                        "explicit-defaults",
                        "-p",
                        "hello"
                    ],
                    "",
                )
                .await
            ),
            "fixture answer\n"
        );
        assert_eq!(model.generated(), 3);
        let rows: Vec<Value> = std::fs::read_to_string(&usage)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            rows.len(),
            1,
            "replay must replace the export without duplicating calls"
        );
        assert_eq!(rows[0]["version"], 2);
        assert!(rows[0]["estimated_cost"]["token_nano_units"].is_null());
        assert!(rows[0]["estimated_cost"]["price_revision"].is_null());
        let row = &rows[0]["usage"];
        assert_eq!(row["provider"], "graphcoder-qual");
        assert_eq!(row["model"], "tiny");
        assert_eq!(row["completed"], true);
        assert_eq!(row["tokens"]["input"], 64);
        assert_eq!(row["tokens"]["output"], 2);
        assert!(row["tokens"]["cached"].is_null());
        assert!(row["agent_path"].as_str().is_some());
        assert!(row["latency_ns"].as_u64().is_some());
    }
    let streamed = graphcoder(
        &explicit,
        &model.catalog,
        vec![
            "--default-config",
            "--usage-jsonl",
            "-",
            "--operation",
            "explicit-defaults",
            "-p",
            "hello",
        ],
        "",
    )
    .await;
    let row: Value = serde_json::from_str(succeeded(&streamed).trim()).unwrap();
    assert_eq!(row["usage"]["completed"], true);
    assert!(String::from_utf8_lossy(&streamed.stderr).contains("fixture answer"));
    assert_eq!(model.generated(), 3);
    model.close().await;
}

#[tokio::test]
async fn jsonl_prompts_share_one_conversation_and_exit_once_settled() {
    let fixture = fixture();
    let model = model(fixture._directory.path()).await;
    for args in [
        vec!["exec"],
        vec!["--output-format", "jsonl"],
        vec!["--output-format", "jsonl", "sessions"],
        vec!["--output-schema", "missing.json", "exec", "hello"],
        vec!["--output-format", "jsonl", "--output", "-", "exec", "hello"],
    ] {
        let refused = graphcoder(&fixture, &model.catalog, args, "").await;
        assert!(!refused.status.success());
        assert!(
            String::from_utf8_lossy(&refused.stderr).contains("recovery: graphcoder"),
            "{}",
            String::from_utf8_lossy(&refused.stderr)
        );
    }
    for input in [
        "{\"prompt\":\"do not dispatch\",\"cancel\":true}\n",
        "{\"approve\":\"approval\",\"approved\":true}\n",
        "{\"cancel\":false}\n",
    ] {
        let refused = graphcoder(
            &fixture,
            &model.catalog,
            vec![
                "--output-format",
                "jsonl",
                "--input-format",
                "jsonl",
                "exec",
            ],
            input,
        )
        .await;
        assert!(!refused.status.success());
        let records = String::from_utf8_lossy(&refused.stdout)
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            records.last().map(|record| &record["kind"]),
            Some(&Value::String("error".into())),
            "{}",
            String::from_utf8_lossy(&refused.stderr)
        );
        assert_eq!(model.generated(), 0, "invalid input dispatched model work");
    }
    let schema = fixture._directory.path().join("output.schema.json");
    std::fs::write(&schema, b"{\"type\":42}").unwrap();
    let refused = graphcoder(
        &fixture,
        &model.catalog,
        vec![
            "--output-format",
            "jsonl",
            "--output-schema",
            schema.to_str().unwrap(),
            "exec",
            "hello",
        ],
        "",
    )
    .await;
    assert!(!refused.status.success());
    let failure: Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(failure["kind"], "error");
    assert_eq!(failure["step"], "prepare_output");
    assert!(
        failure["recovery"]["command"]
            .as_str()
            .unwrap()
            .contains(failure["operation"].as_str().unwrap())
    );
    assert_eq!(model.generated(), 0, "invalid input dispatched model work");
    let input = "{\"prompt\":\"hello\"}\n{\"prompt\":\"again\",\"id\":\"second\"}\n";
    let output = graphcoder(
        &fixture,
        &model.catalog,
        vec!["--operation", "jsonl", "--output-format", "jsonl", "exec"],
        input,
    )
    .await;
    let lines = succeeded(&output)
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let mut ids = lines
        .iter()
        .filter(|line| line["kind"] == "result")
        .map(|line| line["id"].clone())
        .collect::<Vec<_>>();
    ids.sort_by_key(Value::to_string);
    assert_eq!(ids, [json!("jsonl-1"), json!("second")]);
    assert!(
        lines
            .iter()
            .filter(|line| line["kind"] == "result")
            .all(|line| line["text"] == "fixture answer")
    );
    assert_eq!(lines.last().unwrap()["kind"], "completed");
    assert!(
        lines.iter().any(|line| line["kind"] == "execution"
            && line["record"]["event"]["kind"] == "model_started")
    );
    let requests = model.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    // The later turn's model context carries the earlier settled turn.
    let later = requests
        .iter()
        .find(|body| body.to_string().contains("again"))
        .unwrap();
    assert!(later.to_string().contains("hello"), "{later}");
    let replayed = graphcoder(
        &fixture,
        &model.catalog,
        vec!["--operation", "jsonl", "exec"],
        input,
    )
    .await;
    assert_eq!(succeeded(&replayed).lines().count(), 2);
    assert_eq!(model.generated(), 2, "replay dispatched new model work");
    let cursor = fixture._directory.path().join("events.cursor.json");
    for resumed in [false, true] {
        let exported = graphcoder(
            &fixture,
            &model.catalog,
            vec![
                "--operation",
                "jsonl",
                "--output-format",
                "jsonl",
                "--event-cursor",
                cursor.to_str().unwrap(),
                "exec",
            ],
            input,
        )
        .await;
        let lines = succeeded(&exported)
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert!(
            lines
                .iter()
                .all(|line| line["version"] == 1 && line["operation"] == "jsonl")
        );
        let completion = lines.last().unwrap();
        assert_eq!(completion["kind"], "completed");
        assert_eq!(completion["success"], true);
        let turns = lines
            .iter()
            .filter(|line| {
                line["kind"] == "record"
                    && line["resource"]
                        .as_str()
                        .is_some_and(|resource| resource.ends_with("/conversation"))
            })
            .count();
        assert_eq!(turns, if resumed { 0 } else { 2 });
        let execution = lines
            .iter()
            .filter(|line| line["kind"] == "execution")
            .collect::<Vec<_>>();
        if resumed {
            assert!(execution.is_empty(), "resume replayed completed journals");
            assert!(lines.iter().all(|line| line["kind"] != "execution_payload"));
        } else {
            let mut sequences = std::collections::BTreeMap::new();
            for line in &execution {
                let record = &line["record"];
                let previous = sequences
                    .entry(record["operation_id"].to_string())
                    .or_insert(0);
                let sequence = record["sequence"].as_u64().unwrap();
                assert_eq!(
                    sequence,
                    *previous + 1,
                    "journal export skipped or repeated an observation"
                );
                *previous = sequence;
            }
            assert_eq!(
                execution
                    .iter()
                    .filter(|line| line["record"]["event"]["kind"] == "model_started")
                    .count(),
                2
            );
            assert_eq!(
                execution
                    .iter()
                    .filter(|line| !line["record"]["event"]["usage"].is_null())
                    .count(),
                2
            );
            // Reconstruct only captured binary output, never a hand-authored
            // provider fixture. Every referenced field must have exact bytes.
            let mut payloads = std::collections::BTreeMap::new();
            for line in &execution {
                let record: acyclic_harness::executor::ExecutionRecord =
                    serde_json::from_value(line["record"].clone()).unwrap();
                for (field, reference) in
                    acyclic_harness::executor::ExecutionPayloadField::fields(&record.event)
                {
                    let key = (
                        record.operation_id.to_string(),
                        record.sequence,
                        serde_json::to_string(&field).unwrap(),
                    );
                    assert!(
                        payloads
                            .insert(key, (reference.clone(), Vec::new()))
                            .is_none()
                    );
                }
            }
            for line in lines
                .iter()
                .filter(|line| line["kind"] == "execution_payload")
            {
                let turn: acyclic_harness::OperationId =
                    serde_json::from_value(line["turn"].clone()).unwrap();
                let key = (
                    turn.to_string(),
                    line["sequence"].as_u64().unwrap(),
                    serde_json::to_string(&line["field"]).unwrap(),
                );
                let (reference, body) = payloads
                    .get_mut(&key)
                    .expect("chunk has no original observation");
                let original: acyclic_harness::conversation::FileRef =
                    serde_json::from_value(line["file"].clone()).unwrap();
                assert_eq!(&original, reference);
                assert_eq!(line["offset"].as_u64().unwrap(), body.len() as u64);
                let (bytes, _) = acyclic_harness::client::AgentReply::Content {
                    data: line["data"].as_str().unwrap().into(),
                    length: reference.descriptor().byte_length(),
                }
                .content()
                .unwrap();
                assert!(bytes.len() as u64 <= acyclic_harness::client::MAX_READ_BYTES);
                assert!(!bytes.is_empty() || reference.descriptor().byte_length() == 0);
                body.extend_from_slice(&bytes);
            }
            assert!(!payloads.is_empty());
            for (_, (reference, bytes)) in payloads {
                reference.descriptor().verify(&bytes, &mut None).unwrap();
            }
        }
        assert!(cursor.exists());
        let checkpoint: Value = serde_json::from_slice(&std::fs::read(&cursor).unwrap()).unwrap();
        assert_eq!(checkpoint["execution"]["position"]["after"], 0);
        assert_eq!(
            model.generated(),
            2,
            "event export dispatched new model work"
        );
    }
    std::fs::write(&schema, b"{\"type\":\"object\"}").unwrap();
    let artifact = fixture._directory.path().join("final-output.json");
    std::fs::write(&artifact, b"previous complete output").unwrap();
    let refused = graphcoder(
        &fixture,
        &model.catalog,
        vec![
            "--operation",
            "jsonl",
            "--output-format",
            "jsonl",
            "--output-schema",
            schema.to_str().unwrap(),
            "--output",
            artifact.to_str().unwrap(),
            "exec",
        ],
        input,
    )
    .await;
    assert!(!refused.status.success());
    let failure: Value = serde_json::from_str(
        String::from_utf8_lossy(&refused.stdout)
            .lines()
            .last()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(failure["kind"], "error");
    assert_eq!(failure["step"], "execute");
    assert!(
        failure["recovery"]["command"]
            .as_str()
            .unwrap()
            .contains("jsonl")
    );
    assert_eq!(
        std::fs::read(&artifact).unwrap(),
        b"previous complete output"
    );
    assert_eq!(
        model.generated(),
        2,
        "schema projection dispatched model work"
    );
    model.close().await;
}

#[tokio::test]
async fn catalog_errors_fail_through_the_binary() {
    let fixture = fixture();
    let catalog = fixture.settings.join("catalog.json");
    for (document, expected) in [
        (None, "cannot read provider catalog"),
        (
            Some("{\n  \"broken\": {\n    \"adapter\": 42\n  }\n}\n"),
            "broken.adapter",
        ),
    ] {
        if let Some(document) = document {
            std::fs::write(&catalog, document).unwrap();
        }
        let output = graphcoder(&fixture, &catalog, vec!["-p", "hello"], "").await;
        assert!(!output.status.success());
        let error = String::from_utf8(output.stderr).unwrap();
        assert!(error.contains(expected), "{error}");
    }
}
