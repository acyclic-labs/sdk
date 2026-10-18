use super::*;
use std::{
    collections::VecDeque,
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
async fn file_guidance_and_blocked_exit_survive_live_edits_and_reopen() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let script = Arc::new(Mutex::new(VecDeque::<((bool, bool), String)>::new()));
    let received = script.clone();
    let app = Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens":64})) }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let (expected, answer) = received
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("unexpected request");
                assert_eq!(guidance(&body), expected, "{body}");
                async move { ([("content-type", "text/event-stream")], answer) }
            }),
        );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&settings).unwrap();
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"graphcoder-qual":{
        "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
        "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},
        "max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
    }})).unwrap()).unwrap();

    for (prompt, expected, edit, blocked) in [
        ("hello", (false, false), None, false),
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
        (
            "hello",
            (true, true),
            Some(tool(
                "bash",
                json!({"command":"rm .volumes/scratch/GOAL.md"}),
            )),
            false,
        ),
        (
            "hello",
            (false, true),
            Some(tool(
                "bash",
                json!({"command":"rm .volumes/scratch/plans/release.md && rmdir .volumes/scratch/plans"}),
            )),
            false,
        ),
        ("hello", (false, false), None, false),
        ("Use plan mode", (false, true), None, false),
        ("hello", (false, false), None, false),
    ] {
        if let Some(edit) = edit {
            script.lock().unwrap().push_back((expected, edit));
        }
        script.lock().unwrap().push_back((expected, ANSWER.into()));
        let operation = OperationId::new();
        let output = execute_with(
            &workspace,
            &catalog,
            &settings,
            operation,
            Mode::Prompt,
            (prompt, if blocked { 3 } else { 0 }),
        )
        .await;
        let completions: Vec<_> = std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .skip(1)
            .flat_map(|line| {
                let change = jsonl::parse_change(line).unwrap();
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
            .map(|file| serde_json::from_slice::<Value>(&file.bytes).unwrap()["completion"].clone())
            .collect();
        let expected_completion = if blocked { "blocked" } else { "completed" };
        assert_eq!(completions, [json!(expected_completion)]);
        // The former success mapping must disagree with the wire detector.
        if blocked {
            assert_ne!(completions, [json!("completed")]);
        }
        assert!(script.lock().unwrap().is_empty());
        // Replay, including blocked output, must issue no further HTTP work.
        execute_with(
            &workspace,
            &catalog,
            &settings,
            operation,
            Mode::Prompt,
            (prompt, if blocked { 3 } else { 0 }),
        )
        .await;
    }
    server.abort();
}
