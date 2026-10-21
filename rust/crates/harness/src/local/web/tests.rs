use crate::{
    Error, Result,
    client::{CommandSession, consume_local_command},
    context::ModelContextCapacity,
    conversation::ContentResidencyVerifier,
    local::{
        LocalHarness,
        tests::{volume, volume_config},
    },
    model::{ModelContentPart, ModelEvent, ToolCall, ToolResultContent},
    test_support::{
        ScriptedEvent, ScriptedModelProvider, ScriptedSearch, ScriptedSearchProvider, ScriptedTurn,
    },
};
use acyclic_stream::StreamProvider;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

struct NoReads;
impl ContentResidencyVerifier for NoReads {}

fn model(turns: impl IntoIterator<Item = ScriptedTurn>) -> Arc<ScriptedModelProvider> {
    Arc::new(
        ScriptedModelProvider::new(turns).with_context_capacity(ModelContextCapacity {
            context_tokens: 32_768,
            output_tokens: Some(4096),
        }),
    )
}

async fn send(
    local: &LocalHarness,
    client: &mut CommandSession<dyn StreamProvider>,
    text: &str,
) -> Result<Value> {
    client.submit(text, &NoReads).await?;
    let request = client.next_request().await?;
    consume_local_command(client, local, &request)
        .await?
        .ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "fixture command was not consumed",
        })
}

fn assert_published_web_contract(name: &str, value: &Value) -> Result<()> {
    let definition = crate::search::web::definitions(true)?
        .into_iter()
        .find(|tool| tool.name == name)
        .unwrap();
    let output = jsonschema::validator_for(&definition.output_schema)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let projection = jsonschema::validator_for(&definition.projection_schema)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(
        output.is_valid(value),
        "actual web result violates its advertised output contract"
    );
    assert!(
        projection.is_valid(&json!({"kind":"json","value":value})),
        "actual web projection violates its advertised contract"
    );
    for bad in [
        json!({"status":"rejected"}),
        json!({"status":"rejected","code":"unauthorized","reason":"denied"}),
        json!({"status":"rejected","code":"cancelled","reason":"cancelled"}),
        json!({"status":"rejected","code":"invalid","reason":""}),
        json!({"status":"rejected","code":"invalid","reason":"denied","response":{}}),
        json!({"status":"indeterminate"}),
    ] {
        assert!(
            !output.is_valid(&bad),
            "advertised schema accepts a fatal/incomplete outcome"
        );
        assert!(!projection.is_valid(&json!({"kind":"json","value":bad})));
    }
    Ok(())
}

#[tokio::test]
async fn stock_direct_commands_search_and_fetch_distinct_sources_without_a_checkout_or_model()
-> Result<()> {
    let directory =
        tempfile::tempdir().map_err(|_| Error::Storage("fixture directory failed".into()))?;
    let provider = model([]);
    let marker = format!("remote-source-{}", crate::OperationId::new());
    let search = Arc::new(ScriptedSearchProvider::new([ScriptedSearch {
        request: crate::search::SearchRequest {
            query: "independent query".into(),
            max_results: 5,
        },
        response: Some(crate::search::SearchResponse {
            results: vec![crate::search::SearchResult {
                url: "https://example.test/citation".into(),
                title: "Independent source".into(),
                snippet: marker.clone(),
            }],
        }),
        error: None,
    }])?);
    let config = volume_config(
        directory.path(),
        volume(directory.path()).await?,
        provider.clone(),
    )?
    .without_default_tools()
    .search_provider(search.clone());
    let local = LocalHarness::open(config).await?;
    let mut client = CommandSession::open_local(&local, uuid::Uuid::new_v4(), 65_536, 256).await?;
    let result = send(&local, &mut client, "/search independent query").await?;
    assert_eq!(result["results"][0]["url"], "https://example.test/citation");
    assert_eq!(result["results"][0]["snippet"], marker);
    assert_published_web_contract("web_search", &result)?;
    assert_eq!(search.remaining(), 0);
    let fetch_content = format!("exact fetched body {marker} 東京 \u{10437}");
    let (url, server) = serve(fetch_content.clone()).await?;
    let result = send(&local, &mut client, &format!("/fetch {url}")).await?;
    assert_eq!(result["source_url"], url);
    assert_eq!(result["final_url"], url);
    assert_eq!(result["content"], fetch_content);
    assert!(result.get("results").is_none());
    assert_published_web_contract("url_fetch", &result)?;
    server
        .await
        .map_err(|_| Error::Storage("fixture server task failed".into()))??;
    assert!(
        provider.requests().is_empty(),
        "direct web commands dispatched the model"
    );
    Ok(())
}

#[tokio::test]
async fn missing_search_is_typed_unavailable_but_stock_fetch_still_retrieves_real_bytes()
-> Result<()> {
    let directory =
        tempfile::tempdir().map_err(|_| Error::Storage("fixture directory failed".into()))?;
    let provider = model([]);
    let config = volume_config(
        directory.path(),
        volume(directory.path()).await?,
        provider.clone(),
    )?
    .web_tools();
    let local = LocalHarness::open(config).await?;
    let mut client = CommandSession::open_local(&local, uuid::Uuid::new_v4(), 65_536, 256).await?;
    let result = send(&local, &mut client, "/search no configured provider").await;
    assert!(
        matches!(
            result,
            Err(Error::ToolRejected {
                code: crate::wire::ErrorCode::Unavailable,
                ..
            })
        ),
        "missing search was not typed unavailable"
    );
    let marker = format!("fetch-only-{}", crate::OperationId::new());
    let (url, server) = serve(marker.clone()).await?;
    let result = send(&local, &mut client, &format!("/fetch {url}")).await?;
    assert_eq!(result["content"], marker);
    server
        .await
        .map_err(|_| Error::Storage("fixture server task failed".into()))??;
    assert!(provider.requests().is_empty());
    Ok(())
}

#[tokio::test]
async fn model_consumer_receives_real_search_result_not_a_tool_name_or_echo() -> Result<()> {
    let directory =
        tempfile::tempdir().map_err(|_| Error::Storage("fixture directory failed".into()))?;
    let marker = format!("model-source-{}", crate::OperationId::new());
    let search = Arc::new(ScriptedSearchProvider::new([ScriptedSearch {
        request: crate::search::SearchRequest {
            query: "model query".into(),
            max_results: 1,
        },
        response: Some(crate::search::SearchResponse {
            results: vec![crate::search::SearchResult {
                url: "https://example.test/model-source".into(),
                title: "Model source".into(),
                snippet: marker.clone(),
            }],
        }),
        error: None,
    }])?);
    let provider = model([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                "source-call".into(),
                "web_search".into(),
                json!({"query":"model query","max_results":1}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ]),
        ScriptedTurn::Text("complete".into()),
    ]);
    let config = volume_config(
        directory.path(),
        volume(directory.path()).await?,
        provider.clone(),
    )?
    .without_default_tools()
    .search_provider(search.clone());
    let local = LocalHarness::open(config).await?;
    assert_eq!(
        local.run("consult the configured source").await?.text,
        "complete"
    );
    assert_eq!(search.remaining(), 0);
    let requests = provider.requests();
    let result = requests
        .last()
        .unwrap()
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            ModelContentPart::ToolResult {
                call_id,
                content: ToolResultContent::Json { value },
                ..
            } if call_id == "source-call" => Some(value),
            _ => None,
        })
        .expect("model did not receive the actual search tool result");
    assert_eq!(
        result["results"][0]["url"],
        "https://example.test/model-source"
    );
    assert_eq!(result["results"][0]["snippet"], marker);
    assert_published_web_contract("web_search", result)?;
    Ok(())
}

async fn expected_fetch_failure(case: u8) -> Result<()> {
    let (url, reason, server) = match case {
        0 => ("not-a-url".into(), "fetch URL must use HTTP or HTTPS", None),
        1 => (
            "http://user:private-input@example.test/".into(),
            "invalid fetch URL scheme, host, or user information",
            None,
        ),
        2 => {
            let (url, server) = serve_response(503, "", "private-provider-body".into()).await?;
            (url, "fetch HTTP request rejected", Some(server))
        }
        3 => {
            let (url, server) =
                serve_response(302, "Location: file:///private-redirect\r\n", String::new())
                    .await?;
            (
                url,
                "invalid fetch URL scheme, host, or user information",
                Some(server),
            )
        }
        _ => {
            let (url, server) = serve_response(
                302,
                "Location: http://127.0.0.1:9/private-redirect\r\n",
                String::new(),
            )
            .await?;
            (
                url,
                "fetch redirect targets a non-public address",
                Some(server),
            )
        }
    };
    let directory =
        tempfile::tempdir().map_err(|_| Error::Storage("fixture directory failed".into()))?;
    let provider = model([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                "refused-fetch".into(),
                "url_fetch".into(),
                json!({"url":url}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ]),
        ScriptedTurn::Text("adapted after the refusal".into()),
    ]);
    let config = volume_config(
        directory.path(),
        volume(directory.path()).await?,
        provider.clone(),
    )?
    .web_tools();
    let local = LocalHarness::open(config).await?;
    assert_eq!(
        local
            .run("consult the source, or adapt to its refusal")
            .await?
            .text,
        "adapted after the refusal"
    );
    if let Some(server) = server {
        server
            .await
            .map_err(|_| Error::Storage("fixture server task failed".into()))??;
    }
    let requests = provider.requests();
    assert_eq!(
        requests.len(),
        2,
        "expected failure terminated the session or retried the model"
    );
    for request in &requests {
        assert!(request.tools.iter().any(|tool| tool.name == "url_fetch"));
        assert!(
            !request.tools.iter().any(|tool| tool.name == "web_search"),
            "unconfigured search was advertised"
        );
    }
    let results: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            ModelContentPart::ToolResult {
                call_id,
                name,
                content: ToolResultContent::Json { value },
            } if call_id == "refused-fetch" && name == "url_fetch" => Some(value),
            _ => None,
        })
        .collect();
    assert_eq!(
        results,
        [&json!({"status":"rejected","code":"invalid","reason":reason})]
    );
    assert_published_web_contract("url_fetch", results[0])?;
    let encoded = serde_json::to_string(results[0]).unwrap();
    for private in ["private-input", "private-provider-body", "private-redirect"] {
        assert!(
            !encoded.contains(private),
            "tool refusal exposed raw provider/input data"
        );
    }
    Ok(())
}

#[tokio::test]
async fn expected_fetch_malformed_url_continues_the_model_turn() -> Result<()> {
    expected_fetch_failure(0).await?;
    expected_fetch_failure(1).await
}

#[tokio::test]
async fn expected_fetch_http_503_continues_the_model_turn() -> Result<()> {
    expected_fetch_failure(2).await
}

#[tokio::test]
async fn expected_fetch_unsafe_redirect_continues_the_model_turn() -> Result<()> {
    expected_fetch_failure(3).await?;
    expected_fetch_failure(4).await
}

#[tokio::test]
async fn model_fetch_preserves_exact_nonbmp_body_redirect_and_absent_search() -> Result<()> {
    let body = "exact 東京 \u{10437} 🦀\r\n& raw source";
    for redirect in [false, true] {
        let (url, final_url, server) = if redirect {
            same_origin_redirect(302, body.into()).await?
        } else {
            let (url, server) = serve(body.into()).await?;
            (url.clone(), url, server)
        };
        let directory = tempfile::tempdir().map_err(crate::local::storage)?;
        let provider = model([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                    "exact-fetch".into(),
                    "url_fetch".into(),
                    json!({"url":url}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]),
            ScriptedTurn::Text("source read".into()),
        ]);
        let config = volume_config(
            directory.path(),
            volume(directory.path()).await?,
            provider.clone(),
        )?
        .web_tools();
        let local = LocalHarness::open(config).await?;
        assert_eq!(
            local.run("read the exact source").await?.text,
            "source read"
        );
        server.await.map_err(crate::local::storage)??;
        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        for request in &requests {
            assert!(request.tools.iter().any(|tool| tool.name == "url_fetch"));
            assert!(!request.tools.iter().any(|tool| tool.name == "web_search"));
        }
        let value = requests[1]
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .find_map(|part| match part {
                ModelContentPart::ToolResult {
                    call_id,
                    name,
                    content: ToolResultContent::Json { value },
                } if call_id == "exact-fetch" && name == "url_fetch" => Some(value),
                _ => None,
            })
            .expect("exact fetch result did not reach the next model");
        assert_eq!(
            value["content"].as_str().unwrap().as_bytes(),
            body.as_bytes()
        );
        assert_eq!(value["source_url"], url);
        assert_eq!(value["final_url"], final_url);
        assert_eq!(value["status"], 200);
        assert_published_web_contract("url_fetch", value)?;
    }
    Ok(())
}

#[tokio::test]
async fn search_provider_http_failure_is_a_schema_valid_refusal_for_the_next_model() -> Result<()> {
    let (endpoint, server) =
        serve_web_response("POST", 503, "", "private-search-body".into()).await?;
    let search = Arc::new(crate::search::Exa::with_endpoint(
        "test-only-placeholder",
        &endpoint,
    )?);
    let directory =
        tempfile::tempdir().map_err(|_| Error::Storage("fixture directory failed".into()))?;
    let provider = model([
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                "refused-search".into(),
                "web_search".into(),
                json!({"query":"independent query"}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ]),
        ScriptedTurn::Text("adapted after search refusal".into()),
    ]);
    let config = volume_config(
        directory.path(),
        volume(directory.path()).await?,
        provider.clone(),
    )?
    .without_default_tools()
    .search_provider(search);
    let local = LocalHarness::open(config).await?;
    assert_eq!(
        local
            .run("consult the configured search provider")
            .await?
            .text,
        "adapted after search refusal"
    );
    server
        .await
        .map_err(|_| Error::Storage("fixture server task failed".into()))??;
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let result = requests[1]
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            ModelContentPart::ToolResult {
                call_id,
                name,
                content: ToolResultContent::Json { value },
            } if call_id == "refused-search" && name == "web_search" => Some(value),
            _ => None,
        })
        .expect("search refusal did not reach the next model request");
    assert_eq!(
        result,
        &json!({"status":"rejected","code":"invalid","reason":"search HTTP request rejected"})
    );
    assert_published_web_contract("web_search", result)?;
    let encoded = serde_json::to_string(result).unwrap();
    assert!(!encoded.contains("private-search-body"));
    assert!(!encoded.contains("test-only-placeholder"));
    Ok(())
}

async fn serve(content: String) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    serve_response(200, "Content-Type: text/plain; charset=utf-8\r\n", content).await
}

async fn serve_response(
    status: u16,
    headers: &str,
    content: String,
) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    serve_web_response("GET", status, headers, content).await
}

async fn serve_web_response(
    method: &'static str,
    status: u16,
    headers: &str,
    content: String,
) -> Result<(String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|_| Error::Storage("fixture bind failed".into()))?;
    let address = listener
        .local_addr()
        .map_err(|_| Error::Storage("fixture address failed".into()))?;
    let response = format!(
        "HTTP/1.1 {status} Test\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{content}",
        content.len()
    );
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .map_err(|_| Error::Storage("fixture accept failed".into()))?;
        let mut request = Vec::new();
        loop {
            let mut buffer = [0; 1024];
            let count = socket
                .read(&mut buffer)
                .await
                .map_err(|_| Error::Storage("fixture read failed".into()))?;
            assert!(count > 0);
            request.extend_from_slice(&buffer[..count]);
            assert!(request.len() <= 8192);
            if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                break;
            }
        }
        assert!(request.starts_with(format!("{method} /source HTTP/1.1\r\n").as_bytes()));
        socket
            .write_all(response.as_bytes())
            .await
            .map_err(|_| Error::Storage("fixture write failed".into()))?;
        Ok(())
    });
    Ok((format!("http://{address}/source"), server))
}

async fn same_origin_redirect(
    status: u16,
    content: String,
) -> Result<(String, String, tokio::task::JoinHandle<Result<()>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
    let address = listener
        .local_addr()
        .map_err(|e| Error::Storage(e.to_string()))?;
    let source = format!("http://{address}/");
    let final_url = format!("{source}final");
    let server = tokio::spawn(async move {
        for hop in 0..2 {
            let (mut socket, _) = listener
                .accept()
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            let request = read_redirect_request(&mut socket).await?;
            assert!(request.starts_with(if hop == 0 {
                "GET / HTTP/1.1\r\n"
            } else {
                "GET /final HTTP/1.1\r\n"
            }));
            let response = if hop == 0 {
                format!(
                    "HTTP/1.1 {status} Redirect\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
            } else {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{content}",
                    content.len()
                )
            };
            socket
                .write_all(response.as_bytes())
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        Ok(())
    });
    Ok((source, final_url, server))
}

async fn read_redirect_request(socket: &mut tokio::net::TcpStream) -> Result<String> {
    let mut request = Vec::new();
    while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
        let mut buffer = [0; 1024];
        let count = socket
            .read(&mut buffer)
            .await
            .map_err(crate::local::storage)?;
        assert!(count > 0);
        request.extend_from_slice(&buffer[..count]);
        assert!(request.len() <= 8192);
    }
    String::from_utf8(request).map_err(crate::local::storage)
}

mod executor_entrypoints;
