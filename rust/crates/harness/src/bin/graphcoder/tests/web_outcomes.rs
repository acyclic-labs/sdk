//! Shipping CLI, real Responses HTTP adapter, and real credential-free GETs.
use super::*;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Redirect},
    routing::get,
};

const BODY: &str = "exact 東京 \u{10437} 🦀\r\n& source bytes";

#[derive(Clone, Copy)]
enum Case {
    AnswerOnly,
    Malformed,
    Http503,
    UnsafeRedirect,
    Exact,
    Redirect,
}

async fn scenario(case: Case) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unsafe_url = format!("http://{}/private", target.local_addr().unwrap());
    let source_url = match case {
        Case::AnswerOnly => String::new(),
        Case::Malformed => "not-a-url".to_owned(),
        Case::Http503 => format!("http://{address}/unavailable"),
        Case::UnsafeRedirect => format!("http://{address}/unsafe"),
        Case::Exact => format!("http://{address}/source"),
        Case::Redirect => format!("http://{address}/redirect"),
    };
    let expected_source = source_url.clone();
    let final_url = format!("http://{address}/source");
    let model_requests = Arc::new(AtomicUsize::new(0));
    let model_count = model_requests.clone();
    let fetch_requests = Arc::new(AtomicUsize::new(0));
    let fetched = fetch_requests.clone();
    let http_paths = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let observed_paths = http_paths.clone();
    let app = Router::new()
        .route("/count", post(|| async { Json(json!({"input_tokens":64})) }))
        .route("/responses", post(move |Json(body): Json<Value>| {
            let count = model_count.clone();
            let source_url = source_url.clone();
            let final_url = final_url.clone();
            async move {
                let tools = body["tools"].as_array().unwrap();
                assert_eq!(tools.iter().filter(|tool| tool["name"] == "url_fetch").count(), 1);
                assert!(!tools.iter().any(|tool| tool["name"] == "web_search"), "unconfigured search advertised");
                let results: Vec<_> = body["input"].as_array().unwrap().iter()
                    .filter(|item| item["type"] == "function_call_output").collect();
                let first = count.fetch_add(1, Ordering::SeqCst) == 0;
                let answer = if matches!(case, Case::AnswerOnly) {
                    assert!(first);
                    assert!(results.is_empty());
                    ANSWER.to_owned()
                } else if first {
                    assert!(results.is_empty());
                    let event = json!({"type":"response.output_item.done","output_index":0,
                        "item":{"type":"function_call","call_id":"actual_fetch","name":"url_fetch",
                            "arguments":json!({"url":source_url}).to_string()}});
                    format!("data: {event}\n\ndata: {{\"type\":\"response.completed\",\"response\":{{\"status\":\"completed\"}}}}\n\n")
                } else {
                    assert_eq!(results.len(), 1);
                    assert_eq!(results[0]["call_id"], "actual_fetch");
                    let value: Value = serde_json::from_str(results[0]["output"].as_str().unwrap()).unwrap();
                    match case {
                        Case::AnswerOnly => unreachable!("answer-only model requested a tool"),
                        Case::Malformed | Case::Http503 | Case::UnsafeRedirect => {
                            let reason = match case {
                                Case::Malformed => "fetch URL must use HTTP or HTTPS",
                                Case::Http503 => "fetch HTTP request rejected",
                                _ => "fetch redirect targets a non-public address",
                            };
                            assert_eq!(value, json!({"status":"rejected","code":"invalid","reason":reason}));
                        }
                        Case::Exact | Case::Redirect => {
                            assert_eq!(value["content"].as_str().unwrap().as_bytes(), BODY.as_bytes());
                            assert_eq!(value["source_url"], source_url);
                            assert_eq!(value["final_url"], final_url);
                            assert_eq!(value["status"], 200);
                        }
                    }
                    ANSWER.to_owned()
                };
                ([("content-type", "text/event-stream")], answer)
            }
        }))
        .route("/source", get(move || { let count = fetched.clone(); async move {
            count.fetch_add(1, Ordering::SeqCst);
            ([("content-type", "text/plain; charset=utf-8")], BODY)
        }}))
        .route("/redirect", get(|| async { Redirect::temporary("/source") }))
        .route("/unavailable", get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "private provider body").into_response() }))
        .route("/unsafe", get(move || { let url = unsafe_url.clone(); async move { Redirect::temporary(&url) } }))
        .layer(axum::middleware::from_fn(move |request: axum::extract::Request, next: axum::middleware::Next| {
            let paths = observed_paths.clone();
            async move {
                let path = request.uri().path();
                if path != "/responses" && path != "/count" { paths.lock().unwrap().push(path.to_owned()); }
                next.run(request).await
            }
        }));
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
    std::fs::create_dir_all(settings.join(".codex")).unwrap();
    std::fs::create_dir(settings.join("codex-home")).unwrap();
    // Windows known-folder discovery verifies these within the fixture profile.
    std::fs::create_dir_all(settings.join("AppData/Local")).unwrap();
    std::fs::create_dir_all(settings.join("AppData/Roaming")).unwrap();
    std::fs::write(settings.join(".codex/config.toml"), "").unwrap();
    let catalog = catalog(directory.path(), address);
    let operation = OperationId::new();
    let output = execute(&workspace, &catalog, &settings, operation, Mode::Prompt).await;
    assert!(
        String::from_utf8_lossy(&output).contains("fixture answer"),
        "{expected_source}"
    );
    let expected_requests = if matches!(case, Case::AnswerOnly) {
        1
    } else {
        2
    };
    assert_eq!(model_requests.load(Ordering::SeqCst), expected_requests);
    assert_eq!(
        fetch_requests.load(Ordering::SeqCst),
        usize::from(matches!(case, Case::Exact | Case::Redirect))
    );
    let expected_paths: &[&str] = match case {
        Case::AnswerOnly => &[],
        Case::Malformed => &[],
        Case::Http503 => &["/unavailable"],
        Case::UnsafeRedirect => &["/unsafe"],
        Case::Exact => &["/source"],
        Case::Redirect => &["/redirect", "/source"],
    };
    assert_eq!(*http_paths.lock().unwrap(), expected_paths);
    // Reopening the same operation replays the completed turn, including its result.
    execute(&workspace, &catalog, &settings, operation, Mode::Prompt).await;
    assert_eq!(model_requests.load(Ordering::SeqCst), expected_requests);
    assert_eq!(
        fetch_requests.load(Ordering::SeqCst),
        usize::from(matches!(case, Case::Exact | Case::Redirect))
    );
    assert_eq!(
        *http_paths.lock().unwrap(),
        expected_paths,
        "reopen repeated an HTTP request"
    );
    assert_eq!(
        target.into_std().unwrap().accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn plain_answer_finishes_without_requesting_a_tool() {
    scenario(Case::AnswerOnly).await;
}

#[tokio::test]
async fn malformed_url_returns_a_tool_result_and_the_cli_finishes() {
    scenario(Case::Malformed).await;
}

#[tokio::test]
async fn http_503_returns_a_tool_result_and_the_cli_finishes() {
    scenario(Case::Http503).await;
}

#[tokio::test]
async fn unsafe_redirect_returns_a_tool_result_without_contacting_the_target() {
    scenario(Case::UnsafeRedirect).await;
}

#[tokio::test]
async fn exact_utf8_nonbmp_body_reaches_the_model_without_phantom_search() {
    scenario(Case::Exact).await;
}

#[tokio::test]
async fn safe_redirect_preserves_exact_source_bytes_and_final_url() {
    scenario(Case::Redirect).await;
}
