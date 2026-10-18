//! The shipping LocalHarness binding, real HTTP count/generate, and a forced summary.
use acyclic_harness::{
    LocalHarness, OperationId, Result,
    model::ModelProvider,
    runtime::ToolContext,
    tool::{JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_harness_providers::{Credentials, Registry};
use acyclic_stream::BoxProviderFuture;
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::sync::Arc;

const MODEL: &str = "vendor/compact";
const CONTEXT: u64 = 32_768;
const PAYLOAD: usize = 12_000;

struct Large;
impl ToolExecutor for Large {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Ok(ToolResult {
                value: json!({"text":"x".repeat(PAYLOAD)}),
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

fn tokens(body: &Value) -> u64 {
    body.to_string().len() as u64 + 512
}

fn completion(text: &str, input: u64) -> String {
    format!(
        "data: {}\n\ndata: {}\n\n",
        json!({"type":"response.output_text.delta","delta":text}),
        json!({"type":"response.completed","response":{"status":"completed",
            "usage":{"input_tokens":input,"output_tokens":2}}})
    )
}

#[tokio::test]
async fn catalog_unknown_output_compacts_complete_exchanges_over_http_and_reopens() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel::<(bool, Value)>();
    let counts = sent.clone();
    let app = Router::new()
        .route("/count", post(move |Json(body): Json<Value>| {
            let counts = counts.clone();
            async move {
                assert_eq!(body["model"], MODEL, "count preserves selected model");
                let count = tokens(&body);
                counts.send((false, body)).unwrap();
                Json(json!({"input_tokens":count}))
            }
        }))
        .route("/responses", post(move |Json(body): Json<Value>| {
            let sent = sent.clone();
            async move {
                assert_eq!(body["model"], MODEL, "generation preserves selected model");
                let input = tokens(&body);
                assert!(input + body["max_output_tokens"].as_u64().unwrap() <= CONTEXT,
                    "every summary and ordinary request fits the selected context");
                let encoded = body.to_string();
                let summary = encoded.contains("Summarize the preceding context");
                let compacted = encoded.contains("compressed history");
                let calls = body["input"].as_array().unwrap().iter()
                    .filter(|item| item["type"] == "function_call_output").count();
                let response = if summary {
                    assert!(encoded.contains(&"x".repeat(PAYLOAD)), "summary receives complete tool source");
                    completion("compressed history", input)
                } else if compacted {
                    assert_eq!(calls, 1, "the recent exchange remains complete");
                    completion("done", input)
                } else {
                    assert!(calls < 2, "pressure must compact before the third ordinary generation");
                    format!("data: {}\n\ndata: {}\n\n",
                        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"function_call",
                            "id":format!("item-{calls}"),"call_id":format!("call-{calls}"),
                            "name":body["tools"].as_array().unwrap().iter().find(|tool| tool["type"] == "function").expect("declared tool")["name"],"arguments":"{}"}}),
                        json!({"type":"response.completed","response":{"status":"completed",
                            "usage":{"input_tokens":input,"output_tokens":2}}}))
                };
                sent.send((true, body)).unwrap();
                ([("content-type", "text/event-stream")], response)
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
    std::fs::create_dir(directory.path().join("project")).unwrap();
    let catalog = directory.path().join("catalog.json");
    std::fs::write(&catalog, serde_json::to_vec(&json!({
        "default_model":format!("fixture/{MODEL}"),"fixture":{
            "adapter":"responses","authentication":"optional","base_url":format!("http://{address}"),
            "models":{MODEL:{"capacity":{"context_tokens":CONTEXT,"output_tokens":null},
                "features":{"tools":true,"images":false}}},"max_event_bytes":4096,
            "token_counter":{"path":"count","adapter":"responses","response_field":"input_tokens","framing_tokens":0}
        }
    })).unwrap()).unwrap();
    let mut registry = Registry::new(Credentials::explicit(Default::default())).unwrap();
    registry.load_catalog(&catalog).unwrap();
    let provider = Arc::new(registry.resolve(registry.default_model()).unwrap());
    assert_eq!(
        provider
            .context_capacity(provider.model())
            .unwrap()
            .output_tokens,
        None
    );
    let config = || {
        acyclic_harness::test_support::local_harness_config(
            directory.path().join("project"),
            (provider.model().clone(), provider.clone()),
        )
        .unwrap()
        .state_directory(directory.path().join("state"))
        .without_default_tools()
        .tool(Tool {
            hook_names: &[],
            definition: ToolDefinition {
                name: "test.large".into(),
                revision: "1".into(),
                description: "Return a complete large exchange".into(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
                projection_schema: acyclic_harness::tool::json_projection_schema(
                    json!({"type":"object"}),
                ),
            },
            executor: Arc::new(Large),
            projection: Arc::new(JsonToolProjection),
        })
    };
    let operation = OperationId::new();
    let harness = LocalHarness::open(config()).await.unwrap();
    let initialization = received.try_recv().unwrap();
    assert!(!initialization.0, "opening only counts fixed tool overhead");
    assert_eq!(
        initialization.1["input"],
        json!([
            {"type":"message","role":"user","content":[{"type":"input_text","text":"token accounting"}]}
        ])
    );
    assert!(received.try_recv().is_err());
    let task = match harness
        .submit(operation, "perform two operations")
        .await
        .unwrap()
    {
        acyclic_harness::Admission::Accepted(acyclic_harness::runtime::RuntimeTask::Durable {
            task_id,
            ..
        }) => task_id,
        _ => panic!("admission rejected"),
    };
    assert_eq!(harness.resume(task).await.unwrap().text, "done");
    harness.close().await.unwrap();
    let mut generations = Vec::new();
    let mut pressure = false;
    while let Ok((generation, body)) = received.try_recv() {
        if generation {
            generations.push(body);
        } else {
            pressure |= tokens(&body) > CONTEXT / 2;
        }
    }
    assert!(
        pressure,
        "the count endpoint observed real context pressure"
    );
    assert_eq!(
        generations.len(),
        4,
        "two tools, one summary, one continued response"
    );
    assert!(
        generations[2]
            .to_string()
            .contains("Summarize the preceding context")
    );
    assert!(generations[3].to_string().contains("compressed history"));
    let reopened = LocalHarness::open(config()).await.unwrap();
    assert_eq!(
        received.try_recv().unwrap(),
        initialization,
        "reopen repeats exactly the same fixed-overhead count"
    );
    assert!(received.try_recv().is_err());
    assert_eq!(reopened.resume(task).await.unwrap().text, "done");
    reopened.close().await.unwrap();
    assert!(
        received.try_recv().is_err(),
        "settled compaction replays without any HTTP work"
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}
