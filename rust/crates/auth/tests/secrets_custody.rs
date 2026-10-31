//! Original source invalidation cancels real HTTP headers and body waits.
#![cfg(feature = "client")]
use acyclic_auth::{
    contract::SecretListRequest,
    credential::{CredentialCache, CredentialSnapshot, CredentialValidity},
    secrets::{Client, Error, HttpTransport},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type TestResult = Result<(), Box<dyn std::error::Error>>;

async fn source_change_ends_response_wait(body: bool) -> TestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let cache = CredentialCache::new(
        |_| async { CredentialSnapshot::new("harmless-fixture".into(), [1; 32].into(), 2) },
        || CredentialValidity {
            now_unix_seconds: 1,
            revision: Some([1; 32].into()),
        },
    );
    let client = Client::new(HttpTransport::new(&origin)?, cache.clone().into());
    let request = SecretListRequest {
        scope: "fixture".into(),
        cursor: None,
        page_size: 0,
    };
    let response = client.list(&request);
    tokio::pin!(response);
    let peer = async {
        let (mut socket, _) = listener.accept().await?;
        let mut request = Vec::new();
        let mut bytes = [0; 512];
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = socket.read(&mut bytes).await?;
            if count == 0 {
                return Err(std::io::Error::other("fixture request closed"));
            }
            request.extend_from_slice(&bytes[..count]);
        }
        if body {
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4096\r\n\r\n")
                .await?;
            socket.write_all(b"{").await?;
        }
        Ok::<_, std::io::Error>(socket)
    };
    let socket = tokio::select! {
        peer = peer => peer?,
        _ = &mut response => return Err("request completed before source change".into()),
    };
    cache.invalidate()?;
    assert_eq!(response.await, Err(Error::Credential));
    drop(socket);
    Ok(())
}

#[tokio::test]
async fn source_change_cancels_original_http_response_waits() -> TestResult {
    source_change_ends_response_wait(false).await?;
    source_change_ends_response_wait(true).await
}
