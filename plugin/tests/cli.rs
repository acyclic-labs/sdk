//! The shipping binary over a loopback model: argv, provider catalog, the local
//! harness and its root agent, end to end.
use acyclic_native_runtime::{ProcessCommand as Command, ProcessStdio as Stdio};
use axum::response::IntoResponse as _;
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Output,
    sync::{Arc, Mutex},
};

const ANSWER: &str = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"fixture answer\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":64,\"output_tokens\":2}}}\n\n";

#[tokio::test]
async fn installers_observe_the_shipping_product_name_and_version() {
    for (product, binary) in [
        ("acyclic", acyclic_test_support::cargo_binary!("acyclic")),
        (
            "graphcoder",
            acyclic_test_support::cargo_binary!("graphcoder"),
        ),
    ] {
        for flag in ["--version", "-V"] {
            let mut command = Command::new(&binary);
            command.arg(flag);
            let output = capture(command, "").await;
            assert!(output.status.success(), "{product} {flag}: {output:?}");
            assert_eq!(
                String::from_utf8_lossy(&output.stdout).trim(),
                format!("{product} {}", env!("CARGO_PKG_VERSION"))
            );
            assert!(output.stderr.is_empty(), "{product} {flag}: {output:?}");
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn explicit_schema_links_are_refused_before_model_dispatch() {
    let fixture = fixture();
    let model = model(fixture._directory.path()).await;
    let schema = fixture._directory.path().join("schema.json");
    let link = fixture._directory.path().join("schema-link.json");
    std::fs::write(&schema, br#"{"type":"object"}"#).unwrap();
    std::os::unix::fs::symlink(&schema, &link).unwrap();
    let refused = graphcoder(
        &fixture,
        &model.catalog,
        vec![
            "--operation",
            "schema-admission",
            "--output-format",
            "jsonl",
            "--output-schema",
            link.to_str().unwrap(),
            "exec",
            "unadmitted input",
        ],
        "",
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
    assert_eq!(failure["step"], "prepare_output");
    assert_eq!(model.generated(), 0);
    model.close().await;
}

#[path = "../examples/embedded.rs"]
mod embedded;

/// A loopback Responses endpoint; it records every generation request body.
struct Model {
    catalog: PathBuf,
    requests: Arc<Mutex<Vec<Value>>>,
    stop: tokio::sync::oneshot::Sender<()>,
    server: tokio::task::JoinHandle<()>,
}

async fn model(directory: &Path) -> Model {
    model_response(directory, axum::http::StatusCode::OK, ANSWER).await
}

async fn model_response(
    directory: &Path,
    status: axum::http::StatusCode,
    response: &'static str,
) -> Model {
    model_adapter(directory, status, response, "responses").await
}

async fn model_adapter(
    directory: &Path,
    status: axum::http::StatusCode,
    response: &'static str,
    adapter: &str,
) -> Model {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&requests);
    let app = Router::new()
        .route(
            "/count",
            post(move || async move {
                if status.is_success() {
                    Json(json!({"input_tokens": 64})).into_response()
                } else {
                    (status, [("content-type", "application/json")], response).into_response()
                }
            }),
        )
        .route(
            if adapter == "chat" {
                "/chat/completions"
            } else {
                "/responses"
            },
            post(move |Json(body): Json<Value>| {
                recorded.lock().unwrap().push(body);
                async move { ([("content-type", "text/event-stream")], response) }
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
        "adapter":adapter,"authentication":"optional","base_url":format!("http://{address}"),
        "models":{
            "tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}},
            "drifted":{"capacity":{"context_tokens":4096,"output_tokens":512},"features":{"tools":true,"images":false}}
        },"max_event_bytes":4096,
        "token_counter": (adapter != "chat").then(|| json!({"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}))
    }})).unwrap()).unwrap();
    Model {
        catalog,
        requests,
        stop,
        server,
    }
}

#[tokio::test]
async fn chat_without_a_counter_starts_with_explicit_no_compaction() {
    const CHAT: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"fixture answer\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
    for (unchecked, session_client) in [(false, false), (true, false), (false, true), (true, true)]
    {
        let fixture = fixture();
        let model = model_adapter(
            fixture._directory.path(),
            axum::http::StatusCode::OK,
            CHAT,
            "chat",
        )
        .await;
        let mut args = vec!["-p", "hello"];
        if unchecked {
            args.push("--no-compaction");
        }
        if session_client {
            args.push("--new-session");
        }
        let output = graphcoder(&fixture, &model.catalog, args, "").await;
        if unchecked {
            assert_eq!(succeeded(&output), "fixture answer\n");
            assert_eq!(model.generated(), 1);
        } else {
            assert!(!output.status.success());
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains("configure a verified input token counter"),
                "{output:?}"
            );
            assert_eq!(
                model.generated(),
                0,
                "counted negative control never generates"
            );
        }
        model.close().await;
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

fn graphcoder_command(fixture: &Fixture) -> Command {
    let mut command = Command::new(acyclic_test_support::cargo_binary!("acyclic"));
    fixture.custody.configure(|key, value| {
        command.env(key, value);
    });
    command
        .env("CODEX_HOME", fixture.settings.join(".codex"))
        .env("HOME", &fixture.home)
        .env("USERPROFILE", &fixture.settings)
        .env("LOCALAPPDATA", &fixture.settings)
        .env("XDG_DATA_HOME", &fixture.settings)
        .env("XDG_STATE_HOME", &fixture.settings)
        .env("TERM", "dumb")
        .env("ACYCLIC_LOG", "off")
        .env_remove("ACYCLIC_MODEL")
        .env_remove("OPENAI_API_KEY")
        .current_dir(&fixture.workspace)
        .arg("--workspace")
        .arg(&fixture.workspace);
    command
}

async fn graphcoder(fixture: &Fixture, catalog: &Path, args: Vec<&str>, stdin: &str) -> Output {
    let mut command = graphcoder_command(fixture);
    command
        .arg("--settings-root")
        .arg(&fixture.settings)
        .arg("--catalog")
        .arg(catalog)
        .args(["--model", "graphcoder-qual/tiny"])
        .args(args);
    capture(command, stdin).await
}

/// Runs the binary through the shared native capture, exit and physical-close
/// owner, retaining the original outer deadline without readiness polling.
async fn capture(mut command: Command, stdin: &str) -> Output {
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    capture_configured(command, Some(stdin)).await
}

async fn capture_configured(mut command: Command, stdin: Option<&str>) -> Output {
    let stdin = stdin.map(str::to_owned);
    tokio::task::spawn_blocking(move || {
        use acyclic_native_runtime::{CaptureInput, ProcessInput, ProcessTree};
        let mut child = ProcessTree::spawn(&mut command).expect("spawn GraphCoder process tree");
        drop(command);
        child
            .wait_with_output(
                std::time::Duration::from_secs(60),
                usize::MAX,
                stdin
                    .as_deref()
                    .map(|text| CaptureInput::Literal(text.as_bytes())),
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

fn verify_execution_payloads(lines: &[Value]) {
    // Reconstruct only captured binary output, never a hand-authored
    // provider fixture. Every referenced field must have exact bytes.
    let mut payloads = std::collections::BTreeMap::new();
    for line in lines.iter().filter(|line| line["kind"] == "execution") {
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

#[tokio::test]
async fn forced_continuation_launches_independent_runs_from_a_trace_cut() {
    let fixture = fixture();
    let model = model(fixture._directory.path()).await;
    let trace = succeeded(
        &graphcoder(
            &fixture,
            &model.catalog,
            vec![
                "--output-format",
                "jsonl",
                "--operation",
                "parent",
                "exec",
                "prefix",
            ],
            "",
        )
        .await,
    );
    let cut = trace
        .lines()
        .find_map(|line| {
            let event: Value = serde_json::from_str(line).unwrap();
            let resource = event["resource"].as_str()?;
            let path = acyclic_stream::wire_codec::session::stream_path(resource).ok()?;
            let agent = path.as_str().strip_suffix("/conversation")?;
            Some(format!("{agent}@0"))
        })
        .expect("conversation event identifies the source run");
    let override_file = fixture._directory.path().join("override.json");
    std::fs::write(
        &override_file,
        r#"{"version":1,"override_item":{"kind":"assistant","text":"forced answer","calls":[]}}"#,
    )
    .unwrap();
    for operation in ["fork-a", "fork-b", "fork-a"] {
        let output = graphcoder(
            &fixture,
            &model.catalog,
            vec![
                "--fork-from",
                &cut,
                "--override",
                override_file.to_str().unwrap(),
                "--operation",
                operation,
                "exec",
                "continue",
            ],
            "",
        )
        .await;
        assert_eq!(succeeded(&output), "forced answer\n");
    }
    assert_eq!(
        model.generated(),
        1,
        "forced actions and operation replay never sample"
    );
    let control = graphcoder(
        &fixture,
        &model.catalog,
        vec![
            "--fork-from",
            &cut,
            "--operation",
            "control",
            "exec",
            "continue",
        ],
        "",
    )
    .await;
    assert_eq!(succeeded(&control), "fixture answer\n");
    assert_eq!(model.generated(), 2);
    model.close().await;
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
    let transcripts = explicit._directory.path().join("transcripts");
    let mut previous_transcript = None;
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
                        "--transcripts",
                        transcripts.to_str().unwrap(),
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
        let files = std::fs::read_dir(&transcripts)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let files: Vec<_> = files
            .into_iter()
            .filter(|file| file.file_name() != "agents.jsonl")
            .collect();
        assert_eq!(files.len(), 1);
        let transcript = std::fs::read_to_string(files[0].path()).unwrap();
        let transcript: Vec<Value> = transcript
            .lines()
            .map(|line| {
                let mut row: Value = serde_json::from_str(line).unwrap();
                // A live final page can precede its actor's settlement fence.
                row.as_object_mut().unwrap().remove("after");
                row
            })
            .collect();
        assert!(!transcript.is_empty());
        if let Some(previous) = previous_transcript.replace(transcript.clone()) {
            assert_eq!(transcript, previous, "replay replaces the same transcript");
        }
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
        let index: Vec<Value> = std::fs::read_to_string(transcripts.join("agents.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(index.len(), 1, "replay replaces the agent index");
        assert_eq!(index[0]["agent_path"], row["agent_path"]);
        assert_eq!(index[0]["parent"], Value::Null);
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
async fn failed_prompt_and_replay_report_the_original_cause() {
    let fixture = fixture();
    let model = model_response(
        fixture._directory.path(),
        axum::http::StatusCode::OK,
        concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"fixture partial\"}\n\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":403}}}\n\n",
        ),
    )
    .await;
    // Native dispatch retains the admitted diagnostic, without the host's
    // typed Display prefix. Prompt output must preserve those original bytes.
    let cause = "model HTTP status 403";
    for _ in 0..2 {
        let output = graphcoder(
            &fixture,
            &model.catalog,
            vec!["--operation", "failed-prompt", "-p", "hello"],
            "",
        )
        .await;
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert_eq!(output.stdout, b"fixture partial\n");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(stderr.trim_end(), format!("acyclic: {cause}"));
        assert_eq!(model.generated(), 1, "replay dispatched model work");
    }
    model.close().await;
}

/// The shipping readiness path checks prerequisites without changing any
/// configured state, checkout metadata or signing custody, even on refusal.
#[tokio::test]
#[cfg(unix)]
async fn doctor_selects_report_mode_from_each_original_terminal_stream() {
    use std::{fs::File, io::Read as _};
    let fixture = fixture();
    let model = model(fixture._directory.path()).await;
    for (input_tty, output_tty, prompt) in [
        (true, false, false),
        (false, true, false),
        (true, true, true),
        (true, true, false),
    ] {
        let pty = rustix_openpty::openpty(None, None).unwrap();
        let mut controller = Some(File::from(pty.controller));
        let mut command = graphcoder_command(&fixture);
        command
            .arg("--settings-root")
            .arg(&fixture.settings)
            .arg("--catalog")
            .arg(&model.catalog)
            .args(["--model", "graphcoder-qual/tiny"]);
        if prompt {
            command.args(["-p", "hello"]);
        }
        command.arg("doctor");
        command
            .stdin(if input_tty {
                Stdio::from(pty.user.try_clone().unwrap())
            } else {
                Stdio::piped()
            })
            .stdout(if output_tty {
                Stdio::from(pty.user.try_clone().unwrap())
            } else {
                Stdio::piped()
            })
            .stderr(Stdio::piped());
        drop(pty.user);
        // Drain the terminal while the child owns its slave. Darwin can discard
        // unread terminal output when the final slave closes at process exit.
        let terminal_output = output_tty.then(|| {
            let mut controller = controller.take().unwrap();
            tokio::task::spawn_blocking(move || {
                let mut bytes = Vec::new();
                if let Err(error) = controller.read_to_end(&mut bytes) {
                    // Linux reports a closed PTY producer as EIO instead of EOF.
                    assert_eq!(error.raw_os_error(), Some(5), "{error}");
                }
                String::from_utf8(bytes).unwrap()
            })
        });
        let result = capture_configured(command, (!input_tty).then_some("")).await;
        // An input-only terminal keeps its live controller until capture ends.
        drop(controller);
        let stdout = match terminal_output {
            Some(reader) => reader.await.unwrap(),
            None => String::from_utf8(result.stdout).unwrap(),
        };
        assert!(
            !stdout.is_empty(),
            "doctor output was lost: input_tty={input_tty}, output_tty={output_tty}, prompt={prompt}, stderr={}",
            String::from_utf8_lossy(&result.stderr)
        );
        if !input_tty || !output_tty {
            assert!(result.status.success(), "{stdout}");
            let report: Value = serde_json::from_str(&stdout).unwrap();
            assert_eq!(report["mode"], "headless");
            assert_eq!(report["ready"], true);
        } else {
            assert!(serde_json::from_str::<Value>(&stdout).is_err());
            assert!(
                stdout.contains("check: provider network: reachable"),
                "{stdout}"
            );
            if prompt {
                assert!(result.status.success(), "{stdout}");
                assert!(
                    stdout.contains("ok: local prerequisites checked"),
                    "{stdout}"
                );
            } else {
                // Both original streams are TTYs but TERM=dumb: retain the
                // interactive refusal rather than silently selecting JSON.
                assert!(!result.status.success(), "{stdout}");
                assert!(
                    stdout.contains("refused: terminal: interactive terminal unavailable"),
                    "{stdout}"
                );
            }
        }
    }
    assert_eq!(model.generated(), 0);
    model.close().await;
}

#[tokio::test]
async fn doctor_lists_the_startup_refusals_of_the_same_flags() {
    let fixture = fixture();
    let model = model(fixture._directory.path()).await;
    fn snapshot(root: &Path) -> std::collections::BTreeMap<PathBuf, Option<Vec<u8>>> {
        fn visit(
            root: &Path,
            path: &Path,
            result: &mut std::collections::BTreeMap<PathBuf, Option<Vec<u8>>>,
        ) {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                let directory = path.is_dir();
                result.insert(
                    path.strip_prefix(root).unwrap().to_owned(),
                    if directory {
                        None
                    } else {
                        Some(std::fs::read(&path).unwrap())
                    },
                );
                if directory {
                    visit(root, &path, result);
                }
            }
        }
        let mut result = std::collections::BTreeMap::new();
        visit(root, root, &mut result);
        result
    }
    let original = snapshot(fixture._directory.path());
    // Installation pipes both streams and has no prompt or terminal.
    let ready = graphcoder(&fixture, &model.catalog, vec!["doctor"], "").await;
    let report: serde_json::Value = serde_json::from_str(&succeeded(&ready)).unwrap();
    assert_eq!(report["version"], 1);
    assert_eq!(report["kind"], "doctor");
    assert_eq!(report["mode"], "headless");
    assert_eq!(report["ready"], true);
    assert_eq!(report["refusals"], serde_json::json!([]));
    // A JSONL run's exact flags are accepted; output flags are inert here.
    let flagged = graphcoder(
        &fixture,
        &model.catalog,
        vec![
            "-p",
            "hello",
            "--output-format",
            "jsonl",
            "--operation",
            "doctor-flags",
            "doctor",
        ],
        "",
    )
    .await;
    assert!(succeeded(&flagged).contains("provider network: reachable"));
    assert_eq!(
        snapshot(fixture._directory.path()),
        original,
        "doctor changed fresh state or signing authority"
    );
    // The same check must preserve existing real state byte-for-byte.
    let ran = graphcoder(&fixture, &model.catalog, vec!["-p", "hello"], "").await;
    succeeded(&ran);
    let existing = snapshot(fixture._directory.path());
    let reopened = graphcoder(&fixture, &model.catalog, vec!["-p", "hello", "doctor"], "").await;
    succeeded(&reopened);
    assert_eq!(
        snapshot(fixture._directory.path()),
        existing,
        "doctor changed existing state or signing authority"
    );
    // Readiness cannot enter a serving or remote conversation mode.
    let mut host = graphcoder_command(&fixture);
    host.args(["--host-workspace", "--host-state-directory"])
        .arg(&fixture.settings)
        .arg("doctor");
    let mut remote = graphcoder_command(&fixture);
    remote.args(["--endpoint", "http://127.0.0.1:1", "doctor"]);
    for command in [host, remote] {
        let refused = capture(command, "").await;
        let stderr = String::from_utf8_lossy(&refused.stderr);
        assert_eq!(refused.status.code(), Some(1), "{stderr}");
        assert!(stderr.contains("doctor checks local startup"), "{stderr}");
        assert!(stderr.contains("recovery:"), "{stderr}");
    }
    // State nested in the checkout is the refusal opening meets, with its recovery.
    let nested = Fixture {
        workspace: fixture.workspace.clone(),
        settings: fixture.workspace.join("state"),
        ..self::fixture()
    };
    std::fs::create_dir_all(nested.settings.join(".codex")).unwrap();
    let nested_original = snapshot(nested._directory.path());
    let checkout_original = snapshot(&fixture.workspace);
    let refused = graphcoder(&nested, &model.catalog, vec!["-p", "hello", "doctor"], "").await;
    let stdout = String::from_utf8_lossy(&refused.stdout);
    assert_eq!(refused.status.code(), Some(1), "{stdout}");
    let report: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(report["ready"], false);
    assert_eq!(report["refusals"].as_array().unwrap().len(), 1);
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("1 startup refusal(s)"), "{stderr}");
    assert!(
        stdout.contains("separate from the checkout") && stdout.contains("recovery:"),
        "{stdout}"
    );
    assert_eq!(
        snapshot(nested._directory.path()),
        nested_original,
        "refused doctor changed state"
    );
    assert_eq!(
        snapshot(&fixture.workspace),
        checkout_original,
        "refused doctor changed checkout"
    );
    assert_eq!(model.generated(), 1);
    model.close().await;
}

#[tokio::test]
async fn jsonl_exit_status_retains_prompt_refusals_in_both_output_formats() {
    let fixture = fixture();
    let model = model(fixture._directory.path()).await;
    for versioned in [false, true] {
        let mut args = vec![
            "--operation",
            if versioned {
                "events-refusal"
            } else {
                "jsonl-refusal"
            },
        ];
        if versioned {
            args.extend(["--output-format", "jsonl"]);
        }
        args.push("exec");
        for (input, refused) in [
            ("", false),
            ("{\"prompt\":\"/gone\",\"id\":\"refused\"}\n", true),
        ] {
            let output = graphcoder(&fixture, &model.catalog, args.clone(), input).await;
            let records = String::from_utf8_lossy(&output.stdout)
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(output.status.success(), !refused, "{output:?}");
            // A nonzero exit carries its reason on stderr in both formats.
            if refused {
                let stderr = String::from_utf8_lossy(&output.stderr);
                assert!(stderr.contains(": prompt refused refused: "), "{stderr}");
            }
            let results = records
                .iter()
                .filter(|record| record["id"] == "refused")
                .collect::<Vec<_>>();
            assert_eq!(results.len(), usize::from(refused));
            if let Some(result) = results.first() {
                assert!(result["error"].is_string());
                assert!(result.get("text").is_none());
            }
            if versioned {
                let completed = records.last().unwrap();
                assert_eq!(completed["kind"], "completed");
                assert_eq!(completed["success"], !refused);
            } else {
                assert_eq!(records.len(), usize::from(refused));
            }
            assert_eq!(model.generated(), 0, "refusal dispatched model work");
        }
    }
    model.close().await;
}

#[tokio::test]
async fn jsonl_prompts_share_one_conversation_and_exit_once_settled() {
    let fixture = fixture();
    let model = model(fixture._directory.path()).await;
    for (args, diagnostic) in [
        (vec!["exec"], "recovery: acyclic"),
        (vec!["--output-format", "jsonl"], "no task or JSONL input"),
        (
            vec!["--event-cursor", "cursor.json", "exec", "hello"],
            "--operation",
        ),
        (
            vec![
                "--operation",
                "invalid-options",
                "--output-format",
                "jsonl",
                "sessions",
            ],
            "requires an execution",
        ),
        (
            vec!["--output-schema", "missing.json", "exec", "hello"],
            "requires JSONL output",
        ),
        (
            vec![
                "--operation",
                "invalid-options",
                "--output-format",
                "jsonl",
                "--output",
                "-",
                "exec",
                "hello",
            ],
            "stdout is reserved for events",
        ),
    ] {
        let refused = graphcoder(&fixture, &model.catalog, args, "").await;
        assert!(!refused.status.success());
        assert!(
            String::from_utf8_lossy(&refused.stderr).contains(diagnostic),
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
                "--operation",
                "invalid-input",
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
            verify_execution_payloads(&lines);
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
    // Mutate only the real export's saved continuation. A refused resume must
    // leave its input identity unused, even if model dispatch would race exit.
    let checkpoint = std::fs::read(&cursor).unwrap();
    let saved: Value = serde_json::from_slice(&checkpoint).unwrap();
    let invalid_execution =
        [("input", u64::MAX), ("after", 1)]
            .into_iter()
            .map(|(field, value)| {
                let mut invalid = saved.clone();
                invalid["execution"]["position"][field] = json!(value);
                invalid
            });
    let invalid_sources = saved["resources"]
        .as_object()
        .unwrap()
        .keys()
        .map(|resource| {
            let mut invalid = saved.clone();
            // Mutate the actual saved cursor into invalid input for its follow.
            invalid["resources"][resource]["opaque"] = json!("AA==");
            invalid
        });
    for invalid in invalid_execution.chain(invalid_sources) {
        let bytes = serde_json::to_vec(&invalid).unwrap();
        std::fs::write(&cursor, &bytes).unwrap();
        let refused = graphcoder(
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
                "unadmitted prompt",
            ],
            "",
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
        assert_eq!(std::fs::read(&cursor).unwrap(), bytes);
        assert_eq!(model.generated(), 2, "refused resume dispatched model work");
    }
    std::fs::write(&cursor, &checkpoint).unwrap();
    let admitted = graphcoder(
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
            "admitted after cursor correction",
        ],
        "",
    )
    .await;
    let lines = succeeded(&admitted)
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lines.last().unwrap()["kind"], "completed");
    assert_eq!(lines.last().unwrap()["success"], true);
    let inbox = lines.iter().find(|line| line["kind"] == "result").unwrap()["inbox"]
        .as_u64()
        .unwrap();
    let execution = lines.iter().filter(|line| line["kind"] == "execution");
    assert!(execution.clone().all(|line| line["input"] == inbox));
    assert_eq!(
        execution
            .filter(|line| line["record"]["event"]["kind"] == "model_started")
            .count(),
        1
    );
    verify_execution_payloads(&lines);
    let saved: Value = serde_json::from_slice(&std::fs::read(&cursor).unwrap()).unwrap();
    assert_eq!(saved["execution"]["position"]["input"], inbox + 1);
    assert_eq!(saved["execution"]["position"]["after"], 0);
    assert_eq!(
        model.generated(),
        3,
        "corrected cursor did not admit its input"
    );
    model.close().await;
}

#[tokio::test]
async fn selected_agent_queries_use_the_explicit_owner_state() {
    use acyclic_harness::local::LocalHarness;
    use acyclic_harness_providers::{Credentials, Registry};

    let fixture = fixture();
    let model = model(fixture._directory.path()).await;
    let state = fixture._directory.path().join("owner-state");
    let mut registry = Registry::new(Credentials::explicit(Default::default())).unwrap();
    registry.load_catalog(&model.catalog).unwrap();
    let config = registry
        .graphcoder(&fixture.workspace, Some("graphcoder-qual/tiny".into()))
        .unwrap()
        .without_user_settings()
        .state_directory(&state)
        .working_directory(&fixture.workspace)
        .unwrap();
    let local = Arc::new(LocalHarness::open(config).await.unwrap());
    let connection = local.connect().await.unwrap();
    let agent = connection.root().to_string();
    drop(connection);
    Arc::try_unwrap(local)
        .unwrap_or_else(|_| panic!("owner retains a connection"))
        .disconnect()
        .await
        .unwrap();

    for command in ["sessions", "history"] {
        let output = graphcoder(
            &fixture,
            &model.catalog,
            vec![
                "--default-config",
                "--host-state-directory",
                state.to_str().unwrap(),
                "--agent",
                &agent,
                command,
            ],
            "",
        )
        .await;
        let reply: Value = serde_json::from_str(&succeeded(&output)).unwrap();
        if command == "history" {
            assert_eq!(reply["kind"], "history");
        }
        assert_eq!(reply["items"], json!([]));
        assert!(reply["next"].is_null());
    }
    assert_eq!(model.generated(), 0, "observation dispatched model work");
    model.close().await;
}

#[tokio::test]
async fn cli_and_embedding_share_the_selected_model_and_tool_contract() {
    use acyclic_harness_providers::{Credentials, Registry};
    let fixture = fixture();
    std::fs::write(
        fixture.workspace.join("AGENTS.md"),
        "Preserve Unicode Ï€ and retain the complete answer in this checkout.\n",
    )
    .unwrap();
    let model = model(fixture._directory.path()).await;
    assert_eq!(
        succeeded(
            &graphcoder(
                &fixture,
                &model.catalog,
                vec!["--default-config", "--operation", "parity", "-p", "hello"],
                "",
            )
            .await
        ),
        "fixture answer\n"
    );
    let configured = |selected: &str, state: &str| {
        let mut registry = Registry::new(Credentials::explicit(Default::default())).unwrap();
        registry.load_catalog(&model.catalog).unwrap();
        registry
            .graphcoder(
                &fixture.workspace,
                Some(format!("graphcoder-qual/{selected}")),
            )
            .unwrap()
            .without_user_settings()
            .state_directory(fixture._directory.path().join(state))
            .working_directory(&fixture.workspace)
            .unwrap()
    };
    let embedded_config = || configured("tiny", "embedded-state");
    assert_eq!(
        embedded::answer(embedded_config(), "parity", "hello")
            .await
            .unwrap(),
        "fixture answer"
    );
    let requests = model.requests.lock().unwrap().clone();
    let [cli, embedded] = requests.as_slice() else {
        panic!("both hosts must submit exactly one original request");
    };
    let same_contract = |left: &Value, right: &Value| {
        [
            "model",
            "tools",
            "instructions",
            "input",
            "max_output_tokens",
            "reasoning",
        ]
        .iter()
        .all(|field| left.get(field) == right.get(field))
    };
    assert_eq!(cli["model"], "tiny");
    assert!(!cli["tools"].as_array().unwrap().is_empty());
    assert!(cli.to_string().contains("Preserve Unicode Ï€"));
    assert!(
        same_contract(cli, embedded),
        "CLI={cli}; embedding={embedded}"
    );
    // The alternate embedding still returns the same answer from this endpoint.
    // Its actual different model/capacity or missing tool must fail request parity.
    for changed_model in [true, false] {
        let selected = if changed_model { "drifted" } else { "tiny" };
        let mut config = configured(selected, selected);
        if !changed_model {
            config = config.without_default_tools();
        }
        assert_eq!(
            embedded::answer(config, "parity", "hello").await.unwrap(),
            "fixture answer"
        );
        let actual = model.requests.lock().unwrap().last().unwrap().clone();
        if changed_model {
            assert_eq!(actual["model"], "drifted");
            assert_eq!(actual["tools"], cli["tools"]);
        } else {
            assert_eq!(actual["model"], cli["model"]);
            assert_ne!(actual["tools"], cli["tools"]);
            assert!(
                actual["tools"].as_array().unwrap().len() < cli["tools"].as_array().unwrap().len()
            );
        }
        assert!(!same_contract(cli, &actual), "{actual}");
    }
    assert_eq!(model.generated(), 4);
    // A new identity appends another prompt to the same persistent conversation.
    assert_eq!(
        embedded::answer(embedded_config(), "next", "another prompt")
            .await
            .unwrap(),
        "fixture answer"
    );
    assert_eq!(model.generated(), 5);
    // Reopening with the same identity and prompt recovers without dispatch.
    assert_eq!(
        embedded::answer(embedded_config(), "next", "another prompt")
            .await
            .unwrap(),
        "fixture answer"
    );
    assert_eq!(model.generated(), 5);
    assert!(
        embedded::answer(embedded_config(), "next", "changed prompt")
            .await
            .is_err()
    );
    assert_eq!(model.generated(), 5);
    model.close().await;
}

#[tokio::test]
async fn catalog_errors_exit_with_their_cause_on_stderr() {
    let fixture = fixture();
    let catalog = fixture.settings.join("catalog.json");
    for document in [
        None,
        Some("{\n  \"broken\": {\n    \"adapter\": 42\n  }\n}\n"),
    ] {
        if let Some(document) = document {
            std::fs::write(&catalog, document).unwrap();
        }
        let output = graphcoder(&fixture, &catalog, vec!["-p", "hello"], "").await;
        assert!(!output.status.success());
        // The host API returns the typed error; the text CLI renders it once,
        // never the empty stderr that left callers without a cause.
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.starts_with("acyclic: ") && stderr.trim_end().len() > "acyclic:".len(),
            "{stderr:?}"
        );
        assert_eq!(stderr.matches("acyclic: ").count(), 1, "{stderr:?}");
    }
}

#[tokio::test]
async fn execution_recorded_provider_refusal_settles_through_the_shipping_binary() {
    // Unmodified llama.cpp HTTP 401 captured on 2026-10-26; its request,
    // response headers, digest and provenance remain beside the original body.
    let refusal =
        include_str!("../../rust/crates/conformance/fixtures/inference/llamacpp/auth.body");
    for versioned in [false, true] {
        let fixture = fixture();
        let model = model_response(
            fixture._directory.path(),
            axum::http::StatusCode::UNAUTHORIZED,
            refusal,
        )
        .await;
        let mut args = vec![
            "run",
            "--operation",
            "recorded-refusal",
            "--input-format",
            "jsonl",
        ];
        if versioned {
            args.extend(["--output-format", "jsonl"]);
        }
        let output = graphcoder(
            &fixture,
            &model.catalog,
            args,
            "{\"id\":\"refused\",\"prompt\":\"hello\"}\n",
        )
        .await;
        assert!(!output.status.success(), "{output:?}");
        let records = String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        let result = records
            .iter()
            .find(|record| record["id"] == "refused")
            .unwrap();
        assert!(
            result["error"].as_str().unwrap().contains("401"),
            "{result}"
        );
        assert!(result.get("text").is_none(), "{result}");
        if versioned {
            let completed = records.last().unwrap();
            assert_eq!(completed["kind"], "completed");
            assert_eq!(completed["success"], false);
        }
        assert_eq!(
            model.generated(),
            0,
            "count refusal must prevent generation"
        );
        model.close().await;
    }
}
