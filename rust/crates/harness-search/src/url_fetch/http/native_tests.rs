use super::tests::{Framing, joined, nonce, serve};
use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};

fn budget() -> WorkBudget {
    acyclic_fs::performance::work_budget(2_000_000, 32 * 1_024 * 1_024)
}

fn work(budget: WorkBudget, cancellation: &CancellationToken) -> Result<NativeMaterializationWork> {
    NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
}

#[tokio::test]
async fn native_fetch_retains_exact_source_content_in_the_original_ledger() -> Result<()> {
    let work = work(budget(), &CancellationToken::new())?;
    let baseline = work.live_allocation_bytes();
    let content = format!("<p>native-source:{} 東京</p>\r\n", nonce());
    let (url, server) = serve(
        200,
        "Content-Type: text/html; charset=utf-8\r\n",
        content.as_bytes().to_vec(),
        Framing::Chunked,
    )
    .await?;
    let provider = HttpFetch::new()?;
    let request = FetchRequest {
        url: format!("{url}source?nonce={}", nonce()),
    };
    assert!(provider.supports_native_fetch(&work)?);
    let result = provider.fetch_with_native_work(&request, &work)?.await?;
    assert_eq!(result.0.content.as_bytes(), content.as_bytes());
    assert_eq!(result.0.source_url, request.url);
    assert_eq!(result.0.final_url, request.url);
    assert_eq!(
        result.1.bytes(),
        crate::url_fetch::response_bytes(&result.0)?
    );
    assert!(result.1.work().shares_ledger(&work));
    assert_eq!(work.live_allocation_bytes(), baseline + result.1.bytes());
    assert_eq!(work.receipt().backend_read_operations, 1);
    assert!(work.receipt().source_bytes_read > content.len() as u64);
    assert!(work.receipt().bytes_copied >= content.len() as u64);
    assert!(work.receipt().items_examined >= content.len() as u64);
    let sent = joined(server).await?;
    assert!(sent.starts_with("GET /source?nonce="));
    assert!(!sent.to_ascii_lowercase().contains("authorization:"));
    assert!(!sent.to_ascii_lowercase().contains("x-api-key:"));
    drop(result);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn native_redirects_charge_both_real_exchanges_and_retain_final_provenance() -> Result<()> {
    let work = work(budget(), &CancellationToken::new())?;
    let baseline = work.live_allocation_bytes();
    let content = format!("native-final-{}", nonce());
    let (source, final_url, server) =
        super::tests::same_origin_redirect(307, content.clone()).await?;
    let request = FetchRequest {
        url: source.clone(),
    };
    let provider = HttpFetch::new()?;
    let result = provider.fetch_with_native_work(&request, &work)?.await?;
    assert_eq!(result.0.source_url, source);
    assert_eq!(result.0.final_url, final_url);
    assert_eq!(result.0.content, content);
    assert_eq!(work.receipt().backend_read_operations, 2);
    assert_eq!(
        result.1.bytes(),
        crate::url_fetch::response_bytes(&result.0)?
    );
    assert!(result.1.work().shares_ledger(&work));
    server.await.map_err(|e| Error::Storage(e.to_string()))??;
    drop(result);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn native_fetch_budget_denial_precedes_connect() -> Result<()> {
    for denied in ["backend", "source", "items", "allocations"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let address = listener
            .local_addr()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let mut allowance = budget();
        match denied {
            "backend" => allowance.backend_read_operations = 0,
            "source" => allowance.source_bytes_read = 0,
            "items" => allowance.items_examined = 0,
            // Opening the original ledger itself consumes one allocation.
            "allocations" => allowance.allocation_operations = 1,
            _ => unreachable!(),
        }
        let work = work(allowance, &CancellationToken::new())?;
        let baseline = work.live_allocation_bytes();
        let request = FetchRequest {
            url: format!("http://{address}/"),
        };
        let result = match HttpFetch::new()?.fetch_with_native_work(&request, &work) {
            Ok(future) => future.await,
            Err(error) => Err(error),
        };
        let error = result.unwrap_err();
        assert!(error.is_native_failure(), "{denied}: {error:?}");
        assert_eq!(work.receipt().source_bytes_read, 0);
        assert_eq!(work.receipt().backend_read_operations, 0);
        drop(error);
        assert_eq!(work.live_allocation_bytes(), baseline, "{denied}");
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_fetch_one_byte_budget_cannot_read_a_complete_http_frame() -> Result<()> {
    let work = work(
        WorkBudget {
            source_bytes_read: 1,
            ..budget()
        },
        &CancellationToken::new(),
    )?;
    let baseline = work.live_allocation_bytes();
    let (url, server) = serve(
        200,
        "",
        format!("nonce-source-{}", nonce()).into_bytes(),
        Framing::Length,
    )
    .await?;
    let request = FetchRequest { url };
    let error = HttpFetch::new()?
        .fetch_with_native_work(&request, &work)?
        .await
        .unwrap_err();
    assert!(error.is_native_failure());
    assert!(matches!(error, Error::NativeWork { .. }), "{error:?}");
    assert_eq!(work.receipt().source_bytes_read, 1);
    assert_eq!(work.receipt().backend_read_operations, 1);
    drop(error);
    assert_eq!(work.live_allocation_bytes(), baseline);
    joined(server).await?;
    Ok(())
}

#[tokio::test]
async fn native_fetch_protocol_body_and_size_rejections_release_all_custody() -> Result<()> {
    for (status, headers, body, framing) in [
        (404, "", b"private-nonce".to_vec(), Framing::Length),
        (200, "", vec![0xff], Framing::Length),
        (
            200,
            "Content-Encoding: gzip\r\n",
            b"private-nonce".to_vec(),
            Framing::Length,
        ),
        (
            302,
            "Location: file:///private-nonce\r\n",
            Vec::new(),
            Framing::Length,
        ),
        (
            302,
            "Location: http://user:private-nonce@example.com/\r\n",
            Vec::new(),
            Framing::Length,
        ),
        (
            200,
            "",
            vec![b'x'; MAXIMUM_RESPONSE_BYTES + 1],
            Framing::Length,
        ),
        (
            200,
            "",
            vec![b'x'; MAXIMUM_RESPONSE_BYTES + 1],
            Framing::Chunked,
        ),
        (
            200,
            "",
            vec![b'x'; MAXIMUM_RESPONSE_BYTES + 1],
            Framing::Close,
        ),
    ] {
        let work = work(budget(), &CancellationToken::new())?;
        let baseline = work.live_allocation_bytes();
        let (url, server) = serve(status, headers, body, framing).await?;
        let request = FetchRequest { url };
        let error = HttpFetch::new()?
            .fetch_with_native_work(&request, &work)?
            .await
            .unwrap_err();
        assert!(matches!(error, Error::ToolRejected { .. }), "{error:?}");
        assert!(!error.is_native_failure());
        assert_eq!(error.code(), crate::wire::ErrorCode::Invalid);
        assert!(!error.to_string().contains("private-nonce"));
        drop(error);
        assert_eq!(work.live_allocation_bytes(), baseline);
        joined(server).await?;
    }
    Ok(())
}

#[tokio::test]
async fn native_cancellation_closes_the_socket_and_releases_original_custody() -> Result<()> {
    let cancellation = CancellationToken::new();
    let work = work(budget(), &cancellation)?;
    let baseline = work.live_allocation_bytes();
    let (url, waiting, server) = super::tests::stalled_server().await?;
    let provider = HttpFetch::new()?;
    let request = FetchRequest { url };
    let fetch = provider.fetch_with_native_work(&request, &work)?;
    let cancel = async {
        waiting.await.map_err(|e| Error::Storage(e.to_string()))?;
        cancellation.cancel();
        Ok::<_, Error>(())
    };
    let (result, cancelled) = tokio::join!(fetch, cancel);
    cancelled?;
    let error = result.unwrap_err();
    assert!(error.is_native_failure(), "{error:?}");
    drop(error);
    assert_eq!(work.live_allocation_bytes(), baseline);
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .map_err(|_| Error::Invalid("native fetch survived cancellation".into()))?
        .map_err(|e| Error::Storage(e.to_string()))??;
    Ok(())
}

#[test]
fn fetch_records_decode_with_preparation_io_and_keep_the_original_ledger() -> Result<()> {
    use crate::filesystem::UploadRecord as _;
    let work = work(budget(), &CancellationToken::new())?;
    let baseline = work.live_allocation_bytes();
    let request_json = br#"{"url":"https://example.com/native?nonce=owned"}"#;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let request: FetchRequest = io.decode_json(request_json)?;
    request.validate_with_work(Some(&work))?;
    let retained = request.logical_owned_bytes()? + std::mem::size_of::<FetchRequest>() as u64;
    let custody = io.into_native_custody(retained)?;
    assert!(custody.work().shares_ledger(&work));
    assert_eq!(custody.bytes(), retained);
    drop((request, custody));
    assert_eq!(work.live_allocation_bytes(), baseline);

    let response_json = br#"{"source_url":"https://example.com/native","final_url":"https://example.com/final","status":200,"content_type":"text/plain","content":"native source nonce"}"#;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let response: FetchResponse = io.decode_json(response_json)?;
    let custody = io.into_native_custody(crate::url_fetch::response_bytes(&response)?)?;
    assert!(custody.work().shares_ledger(&work));
    assert_eq!(
        custody.bytes(),
        crate::url_fetch::response_bytes(&response)?
    );
    drop((response, custody));
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn native_fetch_connection_failure_is_not_a_safe_provider_rejection() -> Result<()> {
    let work = work(budget(), &CancellationToken::new())?;
    let baseline = work.live_allocation_bytes();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let address = listener
        .local_addr()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let request = super::tests::read_request(&mut socket).await?;
        // Closing without an HTTP response is a real transport failure.
        drop(socket);
        Ok::<_, Error>(request)
    });
    let provider = HttpFetch::new()?;
    let request = FetchRequest {
        url: format!("http://{address}/source"),
    };
    let error = provider
        .fetch_with_native_work(&request, &work)?
        .await
        .unwrap_err();
    assert_eq!(error.code(), crate::wire::ErrorCode::Storage);
    assert!(error.is_native_failure());
    assert!(!matches!(error, Error::ToolRejected { .. }));
    assert_eq!(work.receipt().backend_read_operations, 1);
    drop(error);
    assert_eq!(work.live_allocation_bytes(), baseline);
    assert!(joined(server).await?.starts_with("GET /source HTTP/1.1"));
    Ok(())
}
