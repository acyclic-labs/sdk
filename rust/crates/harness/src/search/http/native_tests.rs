use super::*;
use crate::search::native::response_bytes;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use futures::FutureExt as _;
use std::sync::Arc;
use tokio::io::AsyncReadExt as _;

fn work(cancellation: &CancellationToken) -> Result<NativeMaterializationWork> {
    NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

#[tokio::test]
async fn search_native_http_adapters_return_funded_results() -> Result<()> {
    for exa in [true, false] {
        let cancellation = CancellationToken::new();
        let work = work(&cancellation)?;
        let baseline = work.live_allocation_bytes();
        let body = if exa {
            r#"{"results":[{"url":"https://example.com/native","title":"Native","highlights":["first","second"]}]}"#
        } else {
            r#"{"results":[{"url":"https://example.com/native","title":"Native","snippet":"first\nsecond"}]}"#
        };
        let (endpoint, server) = super::tests::serve(200, body.into()).await?;
        let provider: Arc<dyn SearchProvider> = if exa {
            Arc::new(Exa::with_endpoint("test-only-placeholder", &endpoint)?)
        } else {
            Arc::new(TinyFish::with_endpoint("test-only-placeholder", &endpoint)?)
        };
        let query = SearchRequest {
            query: "東京 & Rust".into(),
            max_results: 1,
        };
        assert!(provider.supports_native_search(&work)?);
        let result = provider.search_with_native_work(&query, &work)?.await?;
        assert_eq!(
            result.0.results.first().map(|r| r.snippet.as_str()),
            Some("first\nsecond")
        );
        assert_eq!(result.1.bytes(), response_bytes(&result.0)?);
        assert!(result.1.work().shares_ledger(&work));
        assert!(work.live_allocation_bytes() >= baseline + result.1.bytes());
        drop(result);
        assert_eq!(work.live_allocation_bytes(), baseline);
        let request = server.await.map_err(|e| Error::Storage(e.to_string()))??;
        assert!(
            request
                .to_ascii_lowercase()
                .contains("x-api-key: test-only-placeholder")
        );
        assert_eq!(work.receipt().backend_read_operations, 1);
        assert!(work.receipt().source_bytes_read > 0);
    }
    Ok(())
}

#[tokio::test]
async fn search_native_http_rejections_are_not_uncertain_transport_failures() -> Result<()> {
    for exa in [true, false] {
        for (status, body) in [
            (200, "{}"),
            (200, "invalid json"),
            (200, r#"{"results":"test-only-placeholder"}"#),
            (401, "test-only-placeholder"),
        ] {
            let work = work(&CancellationToken::new())?;
            let baseline = work.live_allocation_bytes();
            assert_eq!(
                baseline,
                NativeMaterializationWork::accounting_state_bytes()
            );
            let (endpoint, server) = super::tests::serve(status, body.into()).await?;
            let provider: Arc<dyn SearchProvider> = if exa {
                Arc::new(Exa::with_endpoint("test-only-placeholder", &endpoint)?)
            } else {
                Arc::new(TinyFish::with_endpoint("test-only-placeholder", &endpoint)?)
            };
            let query = SearchRequest {
                query: "test".into(),
                max_results: 1,
            };
            let error = provider
                .search_with_native_work(&query, &work)?
                .await
                .unwrap_err();
            assert_eq!(error.code(), crate::wire::ErrorCode::Invalid, "{error:?}");
            assert!(!error.to_string().contains("test-only-placeholder"));
            drop(error);
            assert_eq!(
                work.live_allocation_bytes(),
                baseline,
                "exa={exa}, status={status}, body={body}"
            );
            server.await.map_err(|e| Error::Storage(e.to_string()))??;
        }
    }
    Ok(())
}

#[tokio::test]
async fn search_native_decode_errors_release_their_original_custody() -> Result<()> {
    for exa in [true, false] {
        for body in [
            "{}",
            "invalid json",
            r#"{"results":"test-only-placeholder"}"#,
        ] {
            let work = work(&CancellationToken::new())?;
            let baseline = work.live_allocation_bytes();
            assert_eq!(
                baseline,
                NativeMaterializationWork::accounting_state_bytes()
            );
            let error = if exa {
                codec::exa(body.as_bytes(), 1, Some(&work)).unwrap_err()
            } else {
                codec::tinyfish(body.as_bytes(), 1, Some(&work)).unwrap_err()
            };
            assert_eq!(error.code(), crate::wire::ErrorCode::Invalid);
            drop(error);
            assert_eq!(
                work.live_allocation_bytes(),
                baseline,
                "exa={exa}, body={body}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn search_native_http_exhaustion_rejects_before_network() -> Result<()> {
    for exa in [true, false] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let address = listener
            .local_addr()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let work = NativeMaterializationWork::new(
            acyclic_fs::WorkBudget {
                backend_read_operations: 0,
                ..acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024)
            },
            &CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let endpoint = format!("http://{address}/");
        let provider: Arc<dyn SearchProvider> = if exa {
            Arc::new(Exa::with_endpoint("test-only-placeholder", &endpoint)?)
        } else {
            Arc::new(TinyFish::with_endpoint("test-only-placeholder", &endpoint)?)
        };
        let query = SearchRequest {
            query: "test".into(),
            max_results: 1,
        };
        assert!(
            provider
                .search_with_native_work(&query, &work)?
                .await
                .is_err()
        );
        assert!(listener.accept().now_or_never().is_none());
    }
    Ok(())
}

#[tokio::test]
async fn search_native_http_cancellation_stops_an_in_flight_exchange() -> Result<()> {
    let cancellation = CancellationToken::new();
    let work = work(&cancellation)?;
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
        let mut bytes = [0; 4096];
        while socket
            .read(&mut bytes)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            != 0
        {}
        Ok::<_, Error>(())
    });
    let provider = Exa::with_endpoint("test-only-placeholder", &format!("http://{address}/"))?;
    let query = SearchRequest {
        query: "test".into(),
        max_results: 1,
    };
    let future = provider.search_with_native_work(&query, &work)?;
    let cancel = async {
        waiting.await.map_err(|e| Error::Storage(e.to_string()))?;
        cancellation.cancel();
        Ok::<_, Error>(())
    };
    let (result, cancelled) = tokio::join!(future, cancel);
    cancelled?;
    assert!(result.is_err());
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .map_err(|_| Error::Invalid("native search survived cancellation".into()))?
        .map_err(|e| Error::Storage(e.to_string()))??;
    Ok(())
}
