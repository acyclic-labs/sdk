use super::*;
use crate::SearchResult;
use std::sync::Arc;
use tokio::io::AsyncReadExt as _;

pub(super) use crate::url_fetch::http::tests::{Framing, joined, serve};

#[tokio::test]
async fn adapters_use_the_documented_protocol_and_normalize_ranked_results() -> Result<()> {
    for exa in [true, false] {
        let body = if exa {
            r#"{"results":[{"url":"https://example.com/a","title":"A","highlights":["first","second"]},{"url":"https://example.com/b","title":null}]}"#
        } else {
            r#"{"results":[{"url":"https://example.com/a","title":"A","snippet":"first\nsecond","position":1},{"url":"https://example.com/b","title":"B","snippet":"b","position":2}],"total_results":2}"#
        };
        let (endpoint, server) = serve(
            200,
            "Content-Type: application/json\r\n",
            body.into(),
            Framing::Length,
        )
        .await?;
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
        let input = joined(server).await?;
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
        for (status, body, framing, succeeds) in [
            (200, r#"{"results":[]}"#.to_owned(), Framing::Length, true),
            (200, "{}".into(), Framing::Length, false),
            (
                200,
                r#"{"results":[{"url":7}]}"#.into(),
                Framing::Length,
                false,
            ),
            (200, "test-only-placeholder".into(), Framing::Length, false),
            (
                200,
                "x".repeat(MAXIMUM_RESPONSE_BYTES + 1),
                Framing::Close,
                false,
            ),
            (
                200,
                "x".repeat(MAXIMUM_RESPONSE_BYTES + 2),
                Framing::Length,
                false,
            ),
            (401, "test-only-placeholder".into(), Framing::Length, false),
            (500, "test-only-placeholder".into(), Framing::Length, false),
            (307, "test-only-placeholder".into(), Framing::Length, false),
        ] {
            let (endpoint, server) = serve(
                status,
                "Content-Type: application/json\r\nLocation: /redirect\r\n",
                body.into_bytes(),
                framing,
            )
            .await?;
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
                    assert!(matches!(
                        error,
                        Error::ToolRejected {
                            code: crate::wire::ErrorCode::Invalid,
                            ..
                        }
                    ));
                }
            }
            joined(server).await?;
        }
    }
    Ok(())
}

#[test]
fn invalid_credentials_and_unparseable_endpoints_are_rejected() {
    // The four unsafe endpoint classes are covered by
    // config::tests::configured_gateway_preserves_adapter_endpoint_and_rejects_unsafe_urls.
    for endpoint in ["invalid-secret"] {
        assert!(Exa::with_endpoint("test-only-placeholder", endpoint).is_err());
        assert!(TinyFish::with_endpoint("test-only-placeholder", endpoint).is_err());
    }
    for key in ["", "   ", "secret\r\nheader: value"] {
        assert!(Exa::new(key).is_err());
        assert!(TinyFish::new(key).is_err());
    }
}

#[tokio::test(start_paused = true)]
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
        let request = SearchRequest {
            query: "test".into(),
            max_results: 5,
        };
        let mut search = provider.search(&request);
        tokio::select! {
            biased;
            result = &mut search => panic!("held search completed: {result:?}"),
            result = waiting => { result.map_err(|e| Error::Storage(e.to_string()))?; },
        }
        tokio::time::advance(std::time::Duration::from_secs(31)).await;
        assert!(futures::poll!(search.as_mut()).is_pending());
        drop(search);
        server.await.map_err(|e| Error::Storage(e.to_string()))??;
    }
    Ok(())
}
