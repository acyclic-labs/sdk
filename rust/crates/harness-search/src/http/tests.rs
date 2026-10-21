use super::*;
use crate::SearchResult;
use std::sync::Arc;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[tokio::test]
async fn adapters_use_the_documented_protocol_and_normalize_ranked_results() -> Result<()> {
    for exa in [true, false] {
        let body = if exa {
            r#"{"results":[{"url":"https://example.com/a","title":"A","highlights":["first","second"]},{"url":"https://example.com/b","title":null}]}"#
        } else {
            r#"{"results":[{"url":"https://example.com/a","title":"A","snippet":"first\nsecond","position":1},{"url":"https://example.com/b","title":"B","snippet":"b","position":2}],"total_results":2}"#
        };
        let (endpoint, server) = serve(200, body.into()).await?;
        let provider: Arc<dyn SearchProvider> = if exa {
            Arc::new(Exa::with_endpoint("test-only-placeholder", &endpoint)?)
        } else {
            Arc::new(TinyFish::with_endpoint("test-only-placeholder", &endpoint)?)
        };
        let request = SearchRequest {
            query: "rust & 東京 #?".into(),
            max_results: 1,
        };
        let response = provider.search(&request).await?;
        assert_eq!(
            response.results,
            vec![SearchResult {
                url: "https://example.com/a".into(),
                title: "A".into(),
                snippet: "first\nsecond".into(),
            }]
        );
        let input = server.await.map_err(|e| Error::Storage(e.to_string()))??;
        assert!(
            input
                .to_ascii_lowercase()
                .contains("x-api-key: test-only-placeholder")
        );
        if exa {
            assert!(input.starts_with("POST / HTTP/1.1"));
            let body = input
                .split_once("\r\n\r\n")
                .ok_or_else(|| Error::Invalid("missing HTTP body".into()))?
                .1;
            let body: serde_json::Value =
                serde_json::from_str(body).map_err(|e| Error::Invalid(e.to_string()))?;
            assert_eq!(body["query"], request.query);
            assert_eq!(body["numResults"], 1);
            assert_eq!(body["contents"]["highlights"], true);
        } else {
            let target = input
                .split_whitespace()
                .nth(1)
                .ok_or_else(|| Error::Invalid("missing target".into()))?;
            let target = Url::parse(&format!("http://localhost{target}"))
                .map_err(|e| Error::Invalid(e.to_string()))?;
            assert_eq!(
                target.query_pairs().collect::<Vec<_>>(),
                vec![("query".into(), request.query.into())]
            );
            assert!(input.starts_with("GET /?query="));
        }
    }
    Ok(())
}

#[tokio::test]
async fn response_matrix_rejects_status_errors_malformed_payloads_and_oversized_bodies()
-> Result<()> {
    for exa in [true, false] {
        for (status, body, succeeds) in [
            (200, r#"{"results":[]}"#.to_owned(), true),
            (200, "{}".into(), false),
            (200, r#"{"results":[{"url":7}]}"#.into(), false),
            (200, "test-only-placeholder".into(), false),
            (200, "x".repeat(MAXIMUM_RESPONSE_BYTES + 1), false),
            (200, "x".repeat(MAXIMUM_RESPONSE_BYTES + 2), false),
            (401, "test-only-placeholder".into(), false),
            (429, "test-only-placeholder".into(), false),
            (500, "test-only-placeholder".into(), false),
            (307, "test-only-placeholder".into(), false),
        ] {
            let (endpoint, server) = serve(status, body).await?;
            let provider: Arc<dyn SearchProvider> = if exa {
                Arc::new(Exa::with_endpoint("test-only-placeholder", &endpoint)?)
            } else {
                Arc::new(TinyFish::with_endpoint("test-only-placeholder", &endpoint)?)
            };
            let request = SearchRequest {
                query: "test".into(),
                max_results: 5,
            };
            let result = provider.search(&request).await;
            assert_eq!(result.is_ok(), succeeds);
            if let Err(error) = result {
                assert!(!error.to_string().contains("test-only-placeholder"));
                if status != 200 {
                    assert_eq!(
                        error.to_string(),
                        format!("invalid request: search HTTP status {status}")
                    );
                }
            }
            server.await.map_err(|e| Error::Storage(e.to_string()))??;
        }
    }
    Ok(())
}

#[test]
fn invalid_credentials_and_endpoints_are_rejected_without_echoing_them() {
    for endpoint in [
        "file:///tmp/a",
        "https://user:password@example.com",
        "https://example.com?key=secret",
        "https://example.com/#secret",
        "invalid-secret",
    ] {
        assert!(Exa::with_endpoint("test-only-placeholder", endpoint).is_err());
        assert!(TinyFish::with_endpoint("test-only-placeholder", endpoint).is_err());
    }
    for key in ["", "   ", "secret\r\nheader: value"] {
        assert!(Exa::new(key).is_err());
        assert!(TinyFish::new(key).is_err());
    }
}

#[tokio::test]
async fn dropping_a_search_cancels_the_in_flight_exchange() -> Result<()> {
    for exa in [true, false] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let address = listener
            .local_addr()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let (started, waiting) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener
                .accept()
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            started
                .send(())
                .map_err(|_| Error::Invalid("cancel barrier dropped".into()))?;
            let mut buffer = [0; 4096];
            // Read the request, then observe EOF after the caller drops its future.
            loop {
                if socket
                    .read(&mut buffer)
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?
                    == 0
                {
                    return Ok::<_, Error>(());
                }
            }
        });
        let endpoint = format!("http://{address}/");
        let provider: Arc<dyn SearchProvider> = if exa {
            Arc::new(Exa::with_endpoint("test-only-placeholder", &endpoint)?)
        } else {
            Arc::new(TinyFish::with_endpoint("test-only-placeholder", &endpoint)?)
        };
        let search = tokio::spawn(async move {
            provider
                .search(&SearchRequest {
                    query: "test".into(),
                    max_results: 5,
                })
                .await
        });
        waiting.await.map_err(|e| Error::Storage(e.to_string()))?;
        search.abort();
        assert!(search.await.is_err_and(|error| error.is_cancelled()));
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .map_err(|_| Error::Invalid("HTTP exchange survived cancellation".into()))?
            .map_err(|e| Error::Storage(e.to_string()))??;
    }
    Ok(())
}

pub(super) async fn serve(
    status: u16,
    body: String,
) -> Result<(String, tokio::task::JoinHandle<Result<String>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
    let address = listener
        .local_addr()
        .map_err(|e| Error::Storage(e.to_string()))?;
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let mut input = Vec::new();
        let mut buffer = [0; 1024];
        loop {
            let n = socket
                .read(&mut buffer)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            if n == 0 {
                return Err(Error::Invalid("request disconnected".into()));
            }
            input.extend_from_slice(&buffer[..n]);
            if let Some(end) = input.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let header = String::from_utf8_lossy(&input[..end]).to_ascii_lowercase();
                let length = header
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length:")?
                            .trim()
                            .parse::<usize>()
                            .ok()
                    })
                    .unwrap_or(0);
                if input.len() >= end + 4 + length {
                    break;
                }
            }
            if input.len() > 16384 {
                return Err(Error::Invalid("request exceeded fixture bound".into()));
            }
        }
        let length = if body.len() == MAXIMUM_RESPONSE_BYTES + 1 {
            String::new()
        } else {
            format!("Content-Length: {}\r\n", body.len())
        };
        let output = format!(
            "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nLocation: http://{address}/redirect\r\n{length}Connection: close\r\n\r\n{body}"
        );
        // Early body rejection may close the socket before all bytes are written.
        let _ = socket.write_all(output.as_bytes()).await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
        String::from_utf8(input).map_err(|e| Error::Invalid(e.to_string()))
    });
    Ok((format!("http://{address}/"), task))
}
