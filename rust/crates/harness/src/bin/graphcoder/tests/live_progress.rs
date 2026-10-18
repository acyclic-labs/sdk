//! Progress must escape the process while the provider's terminal SSE is withheld.
use acyclic_harness::{
    client::jsonl,
    task_volume::{Channel, FileKey, TaskFile},
};
use acyclic_native_runtime::{ProcessInput, ProcessOutput, ProcessTree};
use axum::{Json, Router, body::Body, response::Response, routing::post};
use futures::StreamExt as _;
use serde_json::{Value, json};
use std::{
    convert::Infallible,
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Notify;

const DELTAS: &str = concat!(
    "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"thinking\"}\n\n",
    "data: {\"type\":\"response.output_text.delta\",\"delta\":\"working\"}\n\n",
);
const COMPLETED: &str = "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":64,\"output_tokens\":2}}}\n\n";

#[derive(Default)]
struct ProgressObserver {
    seen: (bool, bool),
}

impl ProgressObserver {
    fn observe(&mut self, file: &acyclic_harness::task_volume::FileValue) {
        if matches!(
            file.key,
            FileKey::Task {
                file: TaskFile::Chunk { .. },
                ..
            }
        ) {
            match file.channel {
                Some(Channel::Text) => {
                    assert_eq!(file.bytes.as_ref(), b"working");
                    self.seen.0 = true;
                }
                Some(Channel::Reasoning) => {
                    assert_eq!(file.bytes.as_ref(), b"thinking");
                    self.seen.1 = true;
                }
                _ => panic!("unexpected chunk channel"),
            }
        }
    }

    fn acknowledged(&self) -> bool {
        self.seen == (true, true)
    }
}

async fn scenario(final_output_only: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let release = Arc::new(Notify::new());
    let provider_release = release.clone();
    let (records, mut recorded) = tokio::sync::mpsc::unbounded_channel();
    let (acknowledged, mut acknowledgements) = tokio::sync::mpsc::unbounded_channel();
    let ready = Arc::new(Notify::new());
    let provider_ready = ready.clone();
    let app = Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens":64})) }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let release = provider_release.clone();
                let ready = provider_ready.clone();
                records.send(body).unwrap();
                async move {
                    let initial = futures::stream::once(async { Ok::<_, Infallible>(DELTAS) });
                    let terminal = futures::stream::once(async move {
                        ready.notify_one();
                        release.notified().await;
                        Ok::<_, Infallible>(COMPLETED)
                    });
                    Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(Body::from_stream(initial.chain(terminal)))
                        .unwrap()
                }
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
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&settings).unwrap();
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"loopback":{
        "adapter":"responses", "authentication":"optional", "base_url":format!("http://{address}"),
        "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},
        "max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
    }})).unwrap()).unwrap();
    let process = tokio::task::spawn_blocking(move || {
        acyclic_native_runtime::initialize_native_io_services().unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_graphcoder"));
        command
            .env("CODEX_HOME", &settings)
            .env("LOCALAPPDATA", &settings)
            .env("XDG_DATA_HOME", &settings)
            .env_remove("OPENAI_API_KEY")
            .env_remove("LOOPBACK_API_KEY")
            .arg("--workspace")
            .arg(&workspace)
            .arg("--settings-root")
            .arg(&settings)
            .arg("--catalog")
            .arg(&catalog)
            .args(["--model", "loopback/tiny", "-p", "hello"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut pending = Vec::new();
        let mut observer = ProgressObserver::default();
        let mut final_seen = false;
        let output = ProcessTree::spawn(&mut command)
            .unwrap()
            .wait_with_output(
                Duration::from_secs(6),
                65_536,
                None,
                || Ok(()),
                |chunk| {
                    if let ProcessOutput::Stdout(bytes) = chunk {
                        pending.extend_from_slice(bytes);
                        while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
                            let line: Vec<_> = pending.drain(..=end).collect();
                            let line = std::str::from_utf8(&line).unwrap();
                            if let Ok(change) = jsonl::parse_change(line) {
                                for file in change.file.iter().chain(&change.files) {
                                    let final_file = matches!(
                                        file.key,
                                        FileKey::Session {
                                            file: TaskFile::ExecutionResult(_),
                                            ..
                                        }
                                    );
                                    final_seen |= final_file;
                                    // The broken observer follows only final results.
                                    // It still uses the identical acknowledgement gate.
                                    if !final_output_only || final_file {
                                        observer.observe(file);
                                    }
                                }
                            } else {
                                jsonl::parse_snapshot(line)
                                    .expect("stdout must use the shared JSONL codec");
                            }
                        }
                        if observer.acknowledged() {
                            let _ = acknowledged.send(());
                        }
                    }
                    Ok(ProcessInput::default())
                },
                None,
                None,
            )
            .unwrap_or_else(|failure| {
                panic!(
                    "{}: {}",
                    failure.error,
                    String::from_utf8_lossy(&failure.stderr)
                )
            });
        assert!(final_seen);
        assert_eq!(observer.acknowledged(), !final_output_only);
        output
    });
    // Completion is controlled by observation, with the process timeout only a
    // watchdog. The final-only observer is explicitly released by this driver.
    ready.notified().await;
    if !final_output_only {
        tokio::time::timeout(Duration::from_secs(6), acknowledgements.recv())
            .await
            .unwrap()
            .unwrap();
    } else {
        assert!(acknowledgements.try_recv().is_err());
    }
    release.notify_one();
    let output = process.await.unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    if final_output_only {
        assert!(
            acknowledgements.try_recv().is_err(),
            "a final-output-only observer acknowledged live chunks"
        );
    }
    let request = recorded.recv().await.unwrap();
    assert_eq!(request["model"], "tiny");
    assert_eq!(request["store"], false);
    assert!(
        recorded.try_recv().is_err(),
        "progress cannot resend generation"
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn text_and_reasoning_are_visible_before_the_provider_completes() {
    tokio::time::timeout(Duration::from_secs(8), scenario(false))
        .await
        .unwrap();
}

#[tokio::test]
async fn negative_control_final_output_cannot_acknowledge_live_chunks() {
    tokio::time::timeout(Duration::from_secs(8), scenario(true))
        .await
        .unwrap();
}
