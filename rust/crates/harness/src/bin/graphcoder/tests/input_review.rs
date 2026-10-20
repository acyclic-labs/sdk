use super::*;
use std::sync::{Arc, Mutex};

async fn composer(mode: Mode) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let observed = Arc::clone(&seen);
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route(
                    "/count",
                    post(|| async { Json(json!({"input_tokens":64})) }),
                )
                .route(
                    "/responses",
                    post(move |Json(body): Json<Value>| {
                        let observed = Arc::clone(&observed);
                        async move {
                            observed.lock().unwrap().push(body);
                            ([("content-type", "text/event-stream")], ANSWER)
                        }
                    }),
                ),
        )
        .await
        .unwrap();
    });
    let directory = tempfile::tempdir().unwrap();
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&settings).unwrap();
    std::fs::create_dir_all(workspace.join(".claude/commands")).unwrap();
    std::fs::write(
        workspace.join(".claude/commands/marker.md"),
        "INPUT-MARKER $ARGUMENTS @calc.py @calc.py",
    )
    .unwrap();
    std::fs::write(workspace.join("calc.py"), "unique_file_value = 42\n").unwrap();
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({"graphcoder-qual":{
        "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
        "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},"features":{"tools":true,"images":false}}},"max_event_bytes":4096,
        "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
    }})).unwrap()).unwrap();
    let operation = OperationId::new();
    execute_with(
        &workspace,
        &catalog,
        &settings,
        operation,
        mode,
        ("/marker foo", 0),
    )
    .await;
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let body = seen[0].to_string();
        assert!(body.contains("INPUT-MARKER foo"), "{body}");
        assert_eq!(
            body.matches("unique_file_value = 42").count(),
            1,
            "mention attachment must be present once: {body}"
        );
        assert!(
            !body.contains("/marker foo"),
            "raw command reached model: {body}"
        );
    }
    // Reopen with different source files. The same operation retains its exact
    // normalized request, completed result and original file projection.
    std::fs::write(workspace.join("calc.py"), "changed_file_value = 99\n").unwrap();
    std::fs::write(
        workspace.join(".claude/commands/marker.md"),
        "changed template",
    )
    .unwrap();
    execute_with(
        &workspace,
        &catalog,
        &settings,
        operation,
        mode,
        ("/marker foo", 0),
    )
    .await;
    assert_eq!(
        seen.lock().unwrap().len(),
        1,
        "retry generated another model turn"
    );
    for input in [
        "/missing-command",
        "/help",
        "/compact",
        "inspect @../outside",
    ] {
        execute_with(
            &workspace,
            &catalog,
            &settings,
            OperationId::new(),
            mode,
            (input, 1),
        )
        .await;
    }
    assert_eq!(
        seen.lock().unwrap().len(),
        1,
        "invalid input reached the model"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prompt_commands_expand_and_mentions_reach_the_real_provider_once() {
    composer(Mode::Prompt).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonl_commands_expand_and_mentions_reach_the_real_provider_once() {
    composer(Mode::Jsonl).await;
}
