//! The shipping binary over a loopback model: argv, provider catalog, the local
//! harness and its root agent, end to end.
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::{
    io::Write as _,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
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
        custody: acyclic_native_runtime::signing_key::FixtureCustody::new(&directory.path().join("custody")).unwrap(),
        _directory: directory,
        workspace,
        settings,
        home,
    }
}

/// Runs the binary to its exit, or kills it after a minute.
async fn graphcoder(fixture: &Fixture, catalog: &Path, args: Vec<&str>, stdin: &str) -> Output {
    let mut command = Command::new(build_metadata::EXECUTABLE);
    fixture.custody.configure(&mut command);
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
        let mut child = command.spawn().unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(stdin.as_bytes())
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while child.try_wait().unwrap().is_none() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let _ = child.kill();
        child.wait_with_output().unwrap()
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
