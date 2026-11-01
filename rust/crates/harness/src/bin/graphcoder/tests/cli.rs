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

#[path = "../examples/embedded.rs"]
mod embedded;

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
        "models":{
            "tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}},
            "drifted":{"capacity":{"context_tokens":4096,"output_tokens":512},"features":{"tools":true,"images":false}}
        },"max_event_bytes":4096,
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
        .env("GRAPHCODER_LOG", "off")
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
    let input = "{\"prompt\":\"hello\"}\n{\"prompt\":\"again\",\"id\":\"second\"}\n";
    let output = graphcoder(
        &fixture,
        &model.catalog,
        vec!["--operation", "jsonl", "--input-format", "jsonl"],
        input,
    )
    .await;
    let lines = succeeded(&output)
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    let mut ids = lines
        .iter()
        .map(|line| line["id"].clone())
        .collect::<Vec<_>>();
    ids.sort_by_key(Value::to_string);
    assert_eq!(ids, [json!("jsonl-1"), json!("second")]);
    assert!(lines.iter().all(|line| line["text"] == "fixture answer"));
    let requests = model.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    // The later turn's model context carries the earlier settled turn.
    let later = requests
        .iter()
        .find(|body| body.to_string().contains("again"))
        .unwrap();
    assert!(later.to_string().contains("hello"), "{later}");
    model.close().await;
}

#[tokio::test]
async fn cli_and_embedding_share_the_selected_model_and_tool_contract() {
    use acyclic_harness_providers::{Credentials, Registry};
    let fixture = fixture();
    std::fs::write(
        fixture.workspace.join("AGENTS.md"),
        "Preserve Unicode π and retain the complete answer in this checkout.\n",
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
    let mut registry = Registry::new(Credentials::explicit(Default::default())).unwrap();
    registry.load_catalog(&model.catalog).unwrap();
    let config = registry
        .graphcoder(&fixture.workspace, Some("graphcoder-qual/tiny".into()))
        .without_user_settings()
        .state_directory(fixture._directory.path().join("embedded-state"))
        .working_directory(&fixture.workspace)
        .unwrap();
    assert_eq!(
        embedded::answer(config, "parity", "hello").await.unwrap(),
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
    assert!(cli.to_string().contains("Preserve Unicode π"));
    assert!(
        same_contract(cli, embedded),
        "CLI={cli}; embedding={embedded}"
    );
    // The alternate embedding still returns the same answer from this endpoint.
    // Its actual different model/capacity or missing tool must fail request parity.
    for changed_model in [true, false] {
        let mut registry = Registry::new(Credentials::explicit(Default::default())).unwrap();
        registry.load_catalog(&model.catalog).unwrap();
        let selected = if changed_model { "drifted" } else { "tiny" };
        let mut config = registry
            .graphcoder(
                &fixture.workspace,
                Some(format!("graphcoder-qual/{selected}")),
            )
            .without_user_settings()
            .state_directory(fixture._directory.path().join(selected))
            .working_directory(&fixture.workspace)
            .unwrap();
        if !changed_model {
            let mut preset = acyclic_harness::preset::Preset::graphcoder();
            preset.tools.remove(0);
            config = config.preset(&preset);
        }
        assert_eq!(
            embedded::answer(config, "parity", "hello").await.unwrap(),
            "fixture answer"
        );
        let actual = model.requests.lock().unwrap().last().unwrap().clone();
        assert!(!same_contract(cli, &actual), "{actual}");
    }
    assert_eq!(model.generated(), 4);
    model.close().await;
}

#[tokio::test]
async fn catalog_errors_exit_without_terminal_rendering() {
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
        // The host API returns the typed error. The CLI only maps its status:
        // it cannot start a write after the report/shutdown owner has ended.
        assert!(output.stdout.is_empty());
        assert!(output.stderr.is_empty());
    }
}
