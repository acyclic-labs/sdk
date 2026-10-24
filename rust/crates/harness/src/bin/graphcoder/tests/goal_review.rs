use super::*;
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, Mutex},
};

fn guidance(body: &Value) -> (bool, bool) {
    let texts: Vec<_> = body["input"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|message| message["content"][0]["text"].as_str())
        .collect();
    (
        texts.iter().any(|s| s.starts_with("Keep the current goal")),
        texts
            .iter()
            .any(|s| s.starts_with("Keep each Markdown plan")),
    )
}

fn tool(name: &str, arguments: Value) -> String {
    format!(
        "data: {}\n\ndata: {}\n\n",
        json!({"type":"response.output_item.done","output_index":0,"item":{
            "type":"function_call","call_id":"change_file","name":name,"arguments":arguments.to_string()}}),
        json!({"type":"response.completed","response":{"status":"completed"}})
    )
}

fn write(path: &str, text: &str) -> String {
    tool(
        "write",
        json!({"path":format!(".volumes/scratch/{path}"),"text":text,
        "media_type":"text/markdown","display_name":"document"}),
    )
}

// The subprocess is the shipping binary, with its shipping thread/runtime and
// real HTTP provider. Every invocation reopens the same canonical private files.
#[tokio::test]
async fn file_guidance_and_blocked_exit_survive_live_edits_and_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    let [goal, plan] = goal_and_plan();
    run([
        ("hello", (false, false), None, false),
        goal,
        plan,
        (
            "hello",
            (true, true),
            Some(write(
                "GOAL.md",
                "---\nstatus: blocked\nreason: waiting\n---\nShip",
            )),
            true,
        ),
        (
            "hello",
            (true, true),
            Some(write("GOAL.md", "---\nstatus: done\n---\nShip")),
            false,
        ),
    ])
    .await
}

#[tokio::test]
async fn guidance_tracks_deleted_goal_and_plan_files() -> Result<(), Box<dyn std::error::Error>> {
    let [goal, plan] = goal_and_plan();
    run([
        goal,
        plan,
        (
            "hello",
            (true, true),
            Some(tool(
                "shell",
                json!({"command":"rm .volumes/scratch/GOAL.md"}),
            )),
            false,
        ),
        (
            "hello",
            (false, true),
            Some(tool(
                "shell",
                json!({"command":"rm .volumes/scratch/plans/release.md && rmdir .volumes/scratch/plans"}),
            )),
            false,
        ),
        ("hello", (false, false), None, false),
    ])
    .await
}

// Prompt-scoped plan mode has its own fresh lifecycle. It must not inherit
// durable goal/plan files from the independent deletion lifecycle above.
#[tokio::test]
async fn temporary_plan_mode_does_not_persist_to_the_next_submission()
-> Result<(), Box<dyn std::error::Error>> {
    run([
        ("Use plan mode", (false, true), None, false),
        ("hello", (false, false), None, false),
    ])
    .await
}

type ResponseScript = Arc<Mutex<VecDeque<((bool, bool), String)>>>;

struct Fixture {
    _directory: tempfile::TempDir,
    workspace: PathBuf,
    settings: PathBuf,
    catalog: PathBuf,
    script: ResponseScript,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn fixture() -> Result<Fixture, Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let script = Arc::new(Mutex::new(VecDeque::<((bool, bool), String)>::new()));
    let received = script.clone();
    let app =
        Router::new()
            .route(
                "/count",
                post(|| async { Json(json!({"input_tokens":64})) }),
            )
            .route(
                "/responses",
                post(move |Json(body): Json<Value>| {
                    let request = received.lock().map_err(|error| error.to_string()).and_then(
                        |mut script| {
                            script
                                .pop_front()
                                .ok_or_else(|| "unexpected request".to_owned())
                        },
                    );
                    async move {
                        let (expected, answer) =
                            request.map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
                        assert_eq!(guidance(&body), expected, "{body}");
                        Ok::<_, axum::http::StatusCode>((
                            [("content-type", "text/event-stream")],
                            answer,
                        ))
                    }
                }),
            );
    let directory = tempfile::tempdir()?;
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    std::fs::create_dir(&workspace)?;
    std::fs::create_dir(&settings)?;
    let catalog = directory.path().join("catalog.json");
    std::fs::write(
        &catalog,
        serde_json::to_vec(&json!({"graphcoder-qual":{
            "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
            "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},
            "max_event_bytes":4096,
            "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
        }}))?,
    )?;

    Ok(Fixture {
        _directory: directory,
        workspace,
        settings,
        catalog,
        script,
        server: tokio::spawn(async move { axum::serve(listener, app).await }),
    })
}

async fn run<const N: usize>(scenarios: [Scenario; N]) -> Result<(), Box<dyn std::error::Error>> {
    let fixture = fixture().await?;
    for (index, (prompt, expected, edit, blocked)) in scenarios.into_iter().enumerate() {
        let started = std::time::Instant::now();
        eprintln!(
            "guidance scenario {index}/{N} start fixture={}",
            fixture.workspace.display()
        );
        if let Some(edit) = edit {
            fixture
                .script
                .lock()
                .map_err(|error| error.to_string())?
                .push_back((expected, edit));
        }
        fixture
            .script
            .lock()
            .map_err(|error| error.to_string())?
            .push_back((expected, ANSWER.into()));
        let operation = OperationId::new();
        let output = execute_with(
            &fixture.workspace,
            &fixture.catalog,
            &fixture.settings,
            operation,
            Mode::Prompt,
            (prompt, if blocked { 3 } else { 0 }),
        )
        .await;
        let changes = std::str::from_utf8(&output)?
            .lines()
            .skip(1)
            .map(jsonl::parse_change)
            .collect::<Result<Vec<_>, _>>()?;
        let completions = changes
            .into_iter()
            .flat_map(|change| change.file.into_iter().chain(change.files))
            .filter(|file| {
                matches!(
                    file.key,
                    FileKey::Session {
                        file: TaskFile::ExecutionResult(_),
                        ..
                    }
                )
            })
            .map(|file| {
                serde_json::from_slice::<Value>(&file.bytes)
                    .map(|value| value["completion"].clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let expected_completion = if blocked { "blocked" } else { "completed" };
        assert_eq!(completions, [json!(expected_completion)]);
        assert!(
            fixture
                .script
                .lock()
                .map_err(|error| error.to_string())?
                .is_empty()
        );
        // Replay, including blocked output, must issue no further HTTP work.
        execute_with(
            &fixture.workspace,
            &fixture.catalog,
            &fixture.settings,
            operation,
            Mode::Prompt,
            (prompt, if blocked { 3 } else { 0 }),
        )
        .await;
        if blocked {
            for mode in [Mode::Jsonl, Mode::Dumb] {
                execute_with(
                    &fixture.workspace,
                    &fixture.catalog,
                    &fixture.settings,
                    operation,
                    mode,
                    (prompt, 3),
                )
                .await;
            }
            assert!(
                fixture
                    .script
                    .lock()
                    .map_err(|error| error.to_string())?
                    .is_empty()
            );
        }
        eprintln!(
            "guidance scenario {index}/{N} settled and replayed in {:?}",
            started.elapsed()
        );
    }
    Ok(())
}

type Scenario = (&'static str, (bool, bool), Option<String>, bool);

fn goal_and_plan() -> [Scenario; 2] {
    [
        (
            "Use GOAL MODE",
            (true, false),
            Some(write("GOAL.md", "---\nstatus: done\n---\nShip")),
            false,
        ),
        (
            "hello",
            (true, false),
            Some(write("plans/release.md", "- [ ] Ship")),
            false,
        ),
    ]
}
