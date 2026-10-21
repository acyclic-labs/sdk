use super::*;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

type Server = tokio::task::JoinHandle<Result<String>>;

pub(super) enum Framing {
    Length,
    Close,
    Chunked,
}

pub(super) async fn serve(
    status: u16,
    headers: &str,
    body: Vec<u8>,
    framing: Framing,
) -> Result<(String, Server)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let address = listener
        .local_addr()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let framing_header = match framing {
        Framing::Length => format!("Content-Length: {}\r\n", body.len()),
        Framing::Close => String::new(),
        Framing::Chunked => "Transfer-Encoding: chunked\r\n".into(),
    };
    let head =
        format!("HTTP/1.1 {status} Test\r\n{headers}{framing_header}Connection: close\r\n\r\n");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let request = read_request(&mut socket).await?;
        // Rejection is allowed to close the connection before all body bytes are sent.
        let _ = socket.write_all(head.as_bytes()).await;
        if matches!(framing, Framing::Chunked) {
            for chunk in body.chunks(4096) {
                if socket
                    .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                    .await
                    .is_err()
                    || socket.write_all(chunk).await.is_err()
                    || socket.write_all(b"\r\n").await.is_err()
                {
                    return Ok(request);
                }
            }
            let _ = socket.write_all(b"0\r\n\r\n").await;
        } else {
            let _ = socket.write_all(&body).await;
        }
        Ok(request)
    });
    Ok((format!("http://{address}/"), server))
}

pub(super) async fn read_request(socket: &mut tokio::net::TcpStream) -> Result<String> {
    let mut input = Vec::new();
    let mut buffer = [0; 1024];
    loop {
        let count = socket
            .read(&mut buffer)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        if count == 0 {
            return Err(Error::Invalid("fixture request disconnected".into()));
        }
        input.extend_from_slice(&buffer[..count]);
        if input.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if input.len() > 32_768 {
            return Err(Error::Invalid("fixture request exceeds bound".into()));
        }
    }
    String::from_utf8(input).map_err(|_| Error::Invalid("fixture request is not UTF-8".into()))
}

pub(super) async fn joined(server: Server) -> Result<String> {
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .map_err(|_| Error::Invalid("fixture server did not finish".into()))?
        .map_err(|error| Error::Storage(error.to_string()))?
}

pub(super) fn nonce() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    format!(
        "fetch-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

#[tokio::test]
async fn fetch_get_returns_exact_nonce_source_bytes_and_provenance_without_credentials()
-> Result<()> {
    let nonce = nonce();
    let content = format!("<html><title>{nonce}</title>\n東京 & raw source\r\n</html>");
    let (endpoint, server) = serve(
        200,
        "Content-Type: text/html; charset=utf-8\r\n",
        content.as_bytes().to_vec(),
        Framing::Length,
    )
    .await?;
    let request = FetchRequest {
        url: format!("{endpoint}document/{nonce}?literal=%26"),
    };
    let provider: Box<dyn FetchProvider> = Box::new(HttpFetch::new()?);
    let response = provider.fetch(&request).await?;
    assert_eq!(response.content.as_bytes(), content.as_bytes());
    assert_eq!(response.source_url, request.url);
    assert_eq!(response.final_url, request.url);
    assert_eq!(response.status, 200);
    assert_eq!(
        response.content_type.as_deref(),
        Some("text/html; charset=utf-8")
    );
    let sent = joined(server).await?;
    assert!(sent.starts_with(&format!("GET /document/{nonce}?literal=%26 HTTP/1.1\r\n")));
    let sent = sent.to_ascii_lowercase();
    assert!(sent.contains("accept-encoding: identity\r\n"));
    for forbidden in [
        "authorization:",
        "proxy-authorization:",
        "x-api-key:",
        "cookie:",
    ] {
        assert!(!sent.contains(forbidden), "{forbidden}");
    }
    Ok(())
}

#[tokio::test]
async fn fetch_redirects_retrieve_the_final_source_instead_of_searching() -> Result<()> {
    for status in [301, 302, 303, 307, 308] {
        let content = format!("redirected-source-{}", nonce());
        let (source, final_url, server) = same_origin_redirect(status, content.clone()).await?;
        let response = HttpFetch::new()?
            .fetch(&FetchRequest {
                url: source.clone(),
            })
            .await?;
        assert_eq!(response.source_url, source);
        assert_eq!(response.final_url, final_url);
        assert_eq!(response.content, content);
        server
            .await
            .map_err(|error| Error::Storage(error.to_string()))??;
    }
    Ok(())
}

pub(crate) async fn same_origin_redirect(
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
            let request = read_request(&mut socket).await?;
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

#[tokio::test]
async fn fetch_relative_redirects_are_bounded() -> Result<()> {
    for succeeds in [true, false] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let address = listener
            .local_addr()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let nonce = nonce();
        let expected = nonce.clone();
        let server = tokio::spawn(async move {
            for hop in 0..=MAXIMUM_REDIRECTS {
                let (mut socket, _) = listener
                    .accept()
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
                let sent = read_request(&mut socket).await?;
                let path = if hop == 0 {
                    "/start".to_owned()
                } else {
                    format!("/hop/{hop}")
                };
                assert!(sent.starts_with(&format!("GET {path} HTTP/1.1")));
                let output = if succeeds && hop == MAXIMUM_REDIRECTS {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{nonce}",
                        nonce.len()
                    )
                } else {
                    format!(
                        "HTTP/1.1 302 Found\r\nLocation: /hop/{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        hop + 1
                    )
                };
                socket
                    .write_all(output.as_bytes())
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(30), listener.accept())
                    .await
                    .is_err()
            );
            Ok::<_, Error>(())
        });
        let result = HttpFetch::new()?
            .fetch(&FetchRequest {
                url: format!("http://{address}/start"),
            })
            .await;
        if succeeds {
            let response = result?;
            assert_eq!(response.content, expected);
            assert_eq!(
                response.final_url,
                format!("http://{address}/hop/{MAXIMUM_REDIRECTS}")
            );
        } else {
            assert!(result.unwrap_err().to_string().contains("redirect limit"));
        }
        server.await.map_err(|e| Error::Storage(e.to_string()))??;
    }
    Ok(())
}

#[tokio::test]
async fn fetch_rejects_unsafe_urls_and_redirect_locations_without_echoing_them() -> Result<()> {
    let provider = HttpFetch::new()?;
    for url in [
        "file:///private/nonce",
        "ftp://example.com/a",
        "data:text/plain,nonce",
        "https://user:private-nonce@example.com/",
        "http://example.com/#private-nonce",
        "http://example.com:99999/",
        "http:///",
        "https://example.com/\r\nprivate-nonce",
    ] {
        let error = provider
            .fetch(&FetchRequest { url: url.into() })
            .await
            .unwrap_err();
        assert!(!error.to_string().contains("private-nonce"));
        assert_eq!(error.code(), crate::wire::ErrorCode::Invalid);
    }
    for location in [
        "file:///private-nonce",
        "ftp://example.com/a",
        "http://user:private-nonce@example.com/",
        "http://example.com/#private-nonce",
        "",
    ] {
        let (url, server) = serve(
            302,
            &format!("Location: {location}\r\n"),
            Vec::new(),
            Framing::Length,
        )
        .await?;
        let error = provider.fetch(&FetchRequest { url }).await.unwrap_err();
        assert_eq!(error.code(), crate::wire::ErrorCode::Invalid);
        assert!(!error.to_string().contains("private-nonce"));
        joined(server).await?;
    }
    Ok(())
}

#[tokio::test]
async fn fetch_enforces_the_one_mebibyte_cap_for_length_chunked_and_close_delimited_sources()
-> Result<()> {
    let provider = HttpFetch::new()?;
    for framing in [Framing::Length, Framing::Chunked, Framing::Close] {
        let (url, server) = serve(200, "", vec![b'x'; MAXIMUM_RESPONSE_BYTES + 1], framing).await?;
        let error = provider.fetch(&FetchRequest { url }).await.unwrap_err();
        assert!(error.to_string().contains("byte limit"));
        joined(server).await?;
    }
    for length in [0, MAXIMUM_RESPONSE_BYTES] {
        let (url, server) = serve(200, "", vec![b'x'; length], Framing::Length).await?;
        let response = provider.fetch(&FetchRequest { url }).await?;
        assert_eq!(response.content.len(), length);
        assert!(response.content.as_bytes().iter().all(|byte| *byte == b'x'));
        joined(server).await?;
    }
    Ok(())
}

#[tokio::test]
async fn fetch_rejects_http_errors_non_utf8_and_encoded_bodies() -> Result<()> {
    for (status, headers, body) in [
        (401, "", b"private-nonce".to_vec()),
        (404, "", Vec::new()),
        (500, "", Vec::new()),
        (304, "Location: /not-a-redirect\r\n", Vec::new()),
        (200, "", vec![0xff, 0xfe]),
        (200, "Content-Encoding: gzip\r\n", b"private-nonce".to_vec()),
    ] {
        let (url, server) = serve(status, headers, body, Framing::Length).await?;
        let error = HttpFetch::new()?
            .fetch(&FetchRequest { url })
            .await
            .unwrap_err();
        assert_eq!(error.code(), crate::wire::ErrorCode::Invalid);
        assert!(!error.to_string().contains("private-nonce"));
        joined(server).await?;
    }
    Ok(())
}

#[test]
fn redirect_address_policy_excludes_local_reserved_and_mapped_addresses() {
    for text in [
        "0.0.0.0",
        "127.0.0.1",
        "10.0.0.1",
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",
        "100.64.0.1",
        "192.0.0.1",
        "198.18.0.1",
        "192.0.2.1",
        "224.0.0.1",
        "255.255.255.255",
        "::",
        "::1",
        "fc00::1",
        "fe80::1",
        "::ffff:127.0.0.1",
        "2001:db8::1",
        "2002:7f00:1::1",
        "3fff::1",
    ] {
        assert!(!public_address(text.parse().unwrap()), "{text}");
    }
    for text in ["1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"] {
        assert!(public_address(text.parse().unwrap()), "{text}");
    }
}

#[tokio::test]
#[cfg(feature = "filesystem")]
async fn unsafe_redirect_is_rejected_before_connect_on_ordinary_and_native_paths() -> Result<()> {
    for native in [false, true] {
        let target = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let address = target
            .local_addr()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let (url, server) = serve(
            302,
            &format!("Location: http://{address}/private\r\n"),
            Vec::new(),
            Framing::Length,
        )
        .await?;
        let request = FetchRequest { url };
        let provider = HttpFetch::new()?;
        let error = if native {
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            provider
                .fetch_with_native_work(&request, &work)?
                .await
                .unwrap_err()
        } else {
            provider.fetch(&request).await.unwrap_err()
        };
        assert!(matches!(
            error,
            Error::ToolRejected {
                code: crate::wire::ErrorCode::Invalid,
                reason: "fetch redirect targets a non-public address"
            }
        ));
        joined(server).await?;
        let target = target
            .into_std()
            .map_err(|e| Error::Storage(e.to_string()))?;
        assert_eq!(
            target.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "unsafe redirect contacted the target"
        );
    }
    Ok(())
}

pub(super) async fn stalled_server() -> Result<(
    String,
    tokio::sync::oneshot::Receiver<()>,
    tokio::task::JoinHandle<Result<()>>,
)> {
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
        read_request(&mut socket).await?;
        started
            .send(())
            .map_err(|_| Error::Invalid("fixture barrier dropped".into()))?;
        let mut bytes = [0; 1024];
        assert_eq!(
            socket
                .read(&mut bytes)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?,
            0
        );
        Ok(())
    });
    Ok((format!("http://{address}/"), waiting, server))
}

#[tokio::test]
async fn dropping_fetch_closes_the_real_in_flight_connection() -> Result<()> {
    let (url, waiting, server) = stalled_server().await?;
    let provider = HttpFetch::new()?;
    let request = FetchRequest { url };
    let mut future = provider.fetch(&request);
    tokio::select! {
        biased;
        result = &mut future => panic!("stalled fetch completed: {result:?}"),
        result = waiting => { result.map_err(|e| Error::Storage(e.to_string()))?; },
    }
    drop(future);
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .map_err(|_| Error::Invalid("fetch socket survived future drop".into()))?
        .map_err(|e| Error::Storage(e.to_string()))??;
    Ok(())
}
