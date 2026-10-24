use super::*;
use acyclic_harness::task_volume::Channel;

fn event(value: &Value) -> String {
    format!("data: {value}\n\n")
}

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn responses(
    requests: &tokio::sync::mpsc::UnboundedSender<usize>,
    answer: &'static str,
    stream_answer: bool,
) -> Router {
    let requests = requests.clone();
    Router::new()
        .route(
            "/count",
            post(|| async { Json(json!({"input_tokens":64})) }),
        )
        .route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let requests = requests.clone();
                async move {
                    let tool_results = body
                        .get("input")
                        .ok_or(axum::http::StatusCode::BAD_REQUEST)?
                        .as_array()
                        .ok_or(axum::http::StatusCode::BAD_REQUEST)?
                        .iter()
                        .filter(|item| {
                            item.get("type").and_then(Value::as_str) == Some("function_call_output")
                        })
                        .count();
                    requests
                        .send(tool_results)
                        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
                    let mut response = String::new();
                    if tool_results < 2 {
                        response += &event(&json!({"type":"response.output_text.delta",
                        "delta":format!("intermediate {tool_results} λ\n")}));
                        let tool = body
                            .get("tools")
                            .ok_or(axum::http::StatusCode::BAD_REQUEST)?
                            .as_array()
                            .ok_or(axum::http::StatusCode::BAD_REQUEST)?
                            .iter()
                            .find(|tool| tool.get("name").and_then(Value::as_str) == Some("fs"))
                            .ok_or(axum::http::StatusCode::BAD_REQUEST)?;
                        response +=
                            &event(&json!({"type":"response.output_item.done","output_index":1,
                        "item":{"type":"function_call","call_id":format!("status_{tool_results}"),
                            "name":tool["name"],"arguments":"{\"command\":\"status\"}"}}));
                    } else {
                        if stream_answer {
                            response += &event(
                                &json!({"type":"response.output_text.delta", "output_index":0,
                            "delta":"older answer in the final response"}),
                            );
                            // Separate deltas must still form exactly one final message.
                            for delta in ["", answer] {
                                response += &event(
                                    &json!({"type":"response.output_text.delta","delta":delta}),
                                );
                            }
                        }
                        // An older item's late completion cannot replace the last
                        // output position, including an explicitly empty answer.
                        for (position, text) in
                            [(1, answer), (0, "older answer in the final response")]
                        {
                            response += &event(
                                &json!({"type":"response.output_item.done", "output_index":position,
                                "item":{"type":"message", "role":"assistant", "content":[
                                    {"type":"output_text", "text":text}]}}),
                            );
                        }
                    }
                    response += &event(&json!({"type":"response.completed","response":{
                    "status":"completed","usage":{"input_tokens":64,"output_tokens":8}}}));
                    Ok::<_, axum::http::StatusCode>((
                        [("content-type", "text/event-stream")],
                        response,
                    ))
                }
            }),
        )
}

async fn scenario(mode: &Mode, answer: &'static str, stream_answer: bool) -> TestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (requests, mut observed) = tokio::sync::mpsc::unbounded_channel();
    let app = responses(&requests, answer, stream_answer);
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let directory = tempfile::tempdir()?;
    let workspace = directory.path().join("workspace");
    let settings = directory.path().join("settings");
    std::fs::create_dir(&workspace)?;
    std::fs::create_dir(&settings)?;
    std::fs::write(workspace.join("input.txt"), "fixture input")?;
    let catalog = directory.path().join("catalog.json");
    std::fs::write(
        &catalog,
        serde_json::to_vec(&json!({"graphcoder-qual":{
            "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
            "models":{"tiny":{"capacity":{"context_tokens":8192,"output_tokens":1024},
                "features":{"tools":true,"images":false}}},"max_event_bytes":4096,
            "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
        }}))?,
    )?;
    let operation = OperationId::new();
    let output = execute(&workspace, &catalog, &settings, operation, *mode).await;
    let result = check_output(&output, answer, stream_answer)?;
    // Reopening exercises the persisted result through the harness client too.
    let reopened = execute(&workspace, &catalog, &settings, operation, *mode).await;
    check_reopened(&reopened, &result)?;
    assert_eq!(
        std::iter::from_fn(|| observed.try_recv().ok()).collect::<Vec<_>>(),
        [0, 1, 2]
    );
    stop.send(())
        .map_err(|_| "server stopped before test completed")?;
    server.await??;
    Ok(())
}

fn check_output(output: &[u8], answer: &str, stream_answer: bool) -> TestResult<Value> {
    let mut results = Vec::new();
    let mut chunks = Vec::new();
    for line in std::str::from_utf8(output)?.lines().skip(1) {
        let change = jsonl::parse_change(line)?;
        for file in change.file.into_iter().chain(change.files) {
            match file.key {
                FileKey::Session {
                    file: TaskFile::ExecutionResult(_),
                    ..
                } => {
                    results.push(serde_json::from_slice::<Value>(&file.bytes)?);
                }
                FileKey::Task {
                    file: TaskFile::Chunk { .. },
                    ..
                } if file.channel == Some(Channel::Text) => {
                    chunks.push(String::from_utf8(file.bytes.to_vec())?);
                }
                _ => {}
            }
        }
    }
    assert_eq!(results.len(), 1);
    let result = results.first().ok_or("missing final result")?;
    assert_eq!(result["completion"], "completed");
    assert_eq!(result.pointer("/ok/steps"), Some(&json!(3)));
    assert_eq!(
        result.pointer("/ok/text").and_then(Value::as_str),
        Some(answer)
    );
    assert_eq!(
        chunks
            .iter()
            .filter(|text| !text.is_empty())
            .cloned()
            .collect::<Vec<_>>(),
        [
            "intermediate 0 λ\n",
            "intermediate 1 λ\n",
            if stream_answer {
                "older answer in the final response"
            } else {
                ""
            },
            if stream_answer { answer } else { "" }
        ]
        .into_iter()
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>()
    );
    Ok(result.clone())
}

fn check_reopened(reopened: &[u8], result: &Value) -> TestResult {
    let (_, snapshot) = jsonl::parse_snapshot(
        std::str::from_utf8(reopened)?
            .lines()
            .next()
            .ok_or("missing retained snapshot")?,
    )?;
    let restored = snapshot
        .files
        .iter()
        .find(|file| {
            matches!(
                file.key,
                FileKey::Session {
                    file: TaskFile::ExecutionResult(_),
                    ..
                }
            )
        })
        .ok_or("missing retained result")?;
    let bytes: Vec<_> = restored
        .segments
        .iter()
        .flat_map(|part| part.iter().copied())
        .collect();
    assert_eq!(&serde_json::from_slice::<Value>(&bytes)?, result);
    Ok(())
}

#[tokio::test]
async fn prompt_final_message_excludes_intermediate_jsonl_frames() -> TestResult {
    scenario(&Mode::Prompt, "  final λ\nanswer\n", true).await
}

#[tokio::test]
async fn jsonl_final_message_excludes_intermediate_jsonl_frames() -> TestResult {
    scenario(&Mode::Jsonl, "final answer", true).await
}

#[tokio::test]
async fn empty_final_message_does_not_reuse_intermediate_text() -> TestResult {
    scenario(&Mode::Prompt, "", true).await
}

#[tokio::test]
async fn completed_output_without_deltas_is_rendered_and_retained() -> TestResult {
    scenario(&Mode::Prompt, "completed λ\nanswer", false).await
}

#[tokio::test]
async fn jsonl_completed_output_without_deltas_is_rendered_and_retained() -> TestResult {
    scenario(&Mode::Jsonl, "", false).await
}
