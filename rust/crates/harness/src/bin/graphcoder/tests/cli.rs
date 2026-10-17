//! Real argv/provider/LocalHarness/client qualification over a loopback endpoint.
use acyclic_harness::{
    OperationId,
    client::prepare_local_prompt,
    task_volume::{Change, FileKey, TaskFile},
};
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::{
    io::Write as _,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

const ANSWER: &str = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"fixture answer\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":64,\"output_tokens\":2}}}\n\n";

#[derive(Clone, Copy)]
enum Mode {
    Prompt,
    Instructions,
    Jsonl,
    OpenJsonl,
    Dumb,
}

async fn execute(
    workspace: &Path,
    catalog: &Path,
    settings: &Path,
    operation: OperationId,
    mode: Mode,
) -> Vec<u8> {
    let output = spawn(workspace, catalog, settings, operation, mode, "hello").await;
    assert!(
        output.status.success(),
        "exit {:?}
{}
{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

async fn spawn(
    workspace: &Path,
    catalog: &Path,
    settings: &Path,
    operation: OperationId,
    mode: Mode,
    prompt: &str,
) -> std::process::Output {
    let jsonl = !matches!(mode, Mode::Prompt | Mode::Instructions);
    let keep_open = matches!(mode, Mode::OpenJsonl);
    let mut input = tempfile::NamedTempFile::new().unwrap();
    let request = if jsonl {
        format!(
            "{}
",
            serde_json::to_string(&prepare_local_prompt(operation, prompt).unwrap()).unwrap()
        )
        .into_bytes()
    } else {
        Vec::new()
    };
    input.write_all(&request).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_graphcoder"));
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
            "graphcoder-qual/tiny",
            "--operation",
            &operation.to_string(),
        ])
        .stdin(if keep_open {
            Stdio::piped()
        } else {
            Stdio::from(input.reopen().unwrap())
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if matches!(mode, Mode::Dumb) {
        command.env("TERM", "dumb");
    } else if jsonl {
        command.args(["--input-format", "jsonl"]);
    } else {
        command.args(["-p", prompt]);
    }
    tokio::task::spawn_blocking(move || {
        acyclic_native_runtime::initialize_native_io_services().unwrap();
        acyclic_native_runtime::ProcessTree::spawn(&mut command)
            .unwrap()
            .wait_with_output(
                Duration::from_secs(6),
                65_536,
                keep_open.then_some(acyclic_native_runtime::CaptureInput::Exchange {
                    initial: &request,
                    maximum: request.len(),
                }),
                || Ok(()),
                |_| Ok(acyclic_native_runtime::ProcessInput::default()),
                None,
                None,
            )
            .unwrap_or_else(|failure| {
                panic!(
                    "GraphCoder process failed: {}
stdout: {}
stderr: {}",
                    failure.error,
                    String::from_utf8_lossy(&failure.stdout),
                    String::from_utf8_lossy(&failure.stderr)
                )
            })
    })
    .await
    .unwrap()
}

async fn scenario(mode: Mode) {
    let instructions = matches!(mode, Mode::Instructions);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (requests, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let counter = requests.clone();
    let app = Router::new()
        .route(
            "/count",
            post(move || {
                let requests = counter.clone();
                async move {
                    requests.send("count").unwrap();
                    Json(json!({"input_tokens":64}))
                }
            }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let requests = requests.clone();
                async move {
                    assert!(body.to_string().contains("hello"));
                    if instructions {
                        assert_eq!(body["input"], json!([
                            {"type":"message","role":"system","content":[{"type":"input_text","text":"Keep the current goal in GOAL.md in the agent private volume; use active, blocked or done frontmatter status."}]},
                            {"type":"message","role":"system","content":[{"type":"input_text","text":"Keep each Markdown plan in plans/<id>.md beside GOAL.md in the agent private volume; use ordinary file tools and - [ ], - [~], - [x] task lists."}]},
                            {"type":"message","role":"system","content":[{"type":"input_text","text":"Volumes: . (rw), .volumes/scratch (rw)"}]},
                            {"type":"message","role":"system","content":[{"type":"input_text","text":"Reply using the workspace instructions. λ"}]},
                            {"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}
                        ]));
                        assert_eq!(body["store"], false);
                        assert_eq!(body["stream"], true);
                        assert!(body.get("instructions").is_none());
                    }
                    requests.send("model").unwrap();
                    ([("content-type", "text/event-stream")], ANSWER)
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
    if instructions {
        std::fs::write(
            workspace.join("AGENTS.md"),
            "Reply using the workspace instructions. λ",
        )
        .unwrap();
    }
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"graphcoder-qual":{
        "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
        "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},"max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
    }})).unwrap()).unwrap();
    let operation = OperationId::new();
    let output = execute(&workspace, &catalog, &settings, operation, mode).await;
    let mut lines = std::str::from_utf8(&output).unwrap().lines();
    let header: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    assert_eq!(header[0]["TaskSession"]["session"], operation.to_string());
    assert_eq!(header[0]["TaskSession"]["task"], operation.to_string());
    let mut answers = Vec::new();
    for line in lines {
        let change: Change = serde_json::from_str(line).unwrap();
        for file in change.file.into_iter().chain(change.files) {
            if matches!(
                file.key,
                FileKey::Session {
                    file: TaskFile::ExecutionResult(_),
                    ..
                }
            ) {
                answers.push(serde_json::from_slice::<Value>(&file.bytes).unwrap());
            }
        }
    }
    assert_eq!(answers.len(), 1);
    assert_eq!(answers[0]["ok"]["text"], "fixture answer");
    assert_eq!(answers[0]["completion"], "completed");
    // A second real process restores the same request/result instead of generating again.
    execute(&workspace, &catalog, &settings, operation, mode).await;
    let mut generated = 0;
    let mut counted = 0;
    while let Ok(kind) = observed.try_recv() {
        match kind {
            "model" => generated += 1,
            "count" => counted += 1,
            _ => unreachable!(),
        }
    }
    assert_eq!(generated, 1);
    assert!(counted > 0);
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn prompt_argv_uses_the_real_registry_and_file_client() {
    scenario(Mode::Prompt).await;
}

#[tokio::test]
async fn fix_live_instructions_reach_the_loopback_responses_request_and_survive_reopen() {
    scenario(Mode::Instructions).await;
}

#[tokio::test]
async fn jsonl_stdin_eof_preserves_the_real_client_operation() {
    scenario(Mode::Jsonl).await;
}

#[tokio::test]
async fn settled_jsonl_exits_while_the_input_pipe_stays_open() {
    scenario(Mode::OpenJsonl).await;
}

#[tokio::test]
async fn dumb_terminal_uses_the_shared_headless_runner_without_tty() {
    scenario(Mode::Dumb).await;
}

/// Headless `-p` resolves slash input through the TUI's catalog: project
/// prompts expand, direct tools run, and anything else fails without chat.
#[tokio::test]
async fn headless_slash_input_is_a_command_or_an_error_never_chat() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (requests, mut observed) = tokio::sync::mpsc::unbounded_channel::<String>();
    let app = Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens":64})) }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let requests = requests.clone();
                async move {
                    requests.send(body["input"].to_string()).unwrap();
                    ([("content-type", "text/event-stream")], ANSWER)
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
    std::fs::create_dir_all(workspace.join(".claude/commands")).unwrap();
    std::fs::create_dir(&settings).unwrap();
    std::fs::write(
        workspace.join(".claude/commands/marker.md"),
        "Marker body $ARGUMENTS",
    )
    .unwrap();
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"graphcoder-qual":{
        "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
        "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},"max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
    }})).unwrap()).unwrap();
    let run = |prompt: &'static str| {
        let (workspace, catalog, settings) = (workspace.clone(), catalog.clone(), settings.clone());
        async move {
            spawn(
                &workspace,
                &catalog,
                &settings,
                OperationId::new(),
                Mode::Prompt,
                prompt,
            )
            .await
        }
    };
    let results = |output: &std::process::Output| -> Vec<Value> {
        std::str::from_utf8(&output.stdout)
            .unwrap()
            .lines()
            .skip(1)
            .flat_map(|line| {
                let change: Change = serde_json::from_str(line).unwrap();
                change.file.into_iter().chain(change.files)
            })
            .filter(|file| {
                matches!(
                    file.key,
                    FileKey::Session {
                        file: TaskFile::ExecutionResult(_),
                        ..
                    }
                )
            })
            .map(|file| serde_json::from_slice(&file.bytes).unwrap())
            .collect()
    };

    // A project prompt expands; the model never sees the slash command.
    let output = run("/marker world").await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body = observed.try_recv().unwrap();
    assert!(body.contains("Marker body world"), "{body}");
    assert!(!body.contains("/marker"), "{body}");

    // A direct tool runs without a model turn and reports its output.
    let output = run("/shell echo LPSHELL").await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let shell = results(&output);
    assert_eq!(shell.len(), 1);
    assert!(
        shell[0]["ok"]["stdout"]
            .as_str()
            .unwrap()
            .contains("LPSHELL")
    );

    // Unknown commands and renderer-only selections fail with no model call.
    for (prompt, message) in [
        (
            "/definitely-unknown",
            "Unknown command: /definitely-unknown",
        ),
        ("/help", "no headless form"),
    ] {
        let output = run(prompt).await;
        assert!(!output.status.success(), "{prompt}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(message), "{prompt}: {stderr}");
    }
    assert!(
        observed.try_recv().is_err(),
        "only the prompt reached the model"
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}
