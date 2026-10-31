//! Real native HTTP timeout coverage without wall-clock sleeps or detached tasks.
#![cfg(feature = "client")]
use acyclic_auth::{
    credential::HTTP_REQUEST_TIMEOUT,
    secrets::{Error, HttpTransport, Transport},
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type TestResult = Result<(), Box<dyn std::error::Error>>;
const PLANTED: &str = "PLANTED_TIMEOUT_SECRET";

enum Stall {
    Headers,
    Body,
}

async fn refuses_silence(stall: Stall) -> TestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let transport = HttpTransport::new(&origin)?;
    let response = transport.post("/v1/identity/secrets/list", PLANTED, b"{}");
    tokio::pin!(response);
    let peer = async {
        let (mut socket, _) = listener.accept().await?;
        let mut request = Vec::new();
        let mut bytes = [0; 512];
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = socket.read(&mut bytes).await?;
            if count == 0 || request.len() + count > 8192 {
                return Err(std::io::Error::other("fixture request headers missing"));
            }
            request.extend_from_slice(
                bytes
                    .get(..count)
                    .ok_or_else(|| std::io::Error::other("fixture read exceeds buffer"))?,
            );
        }
        match stall {
            Stall::Headers => {}
            Stall::Body => {
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4096\r\n\r\n")
                    .await?;
                socket.write_all(PLANTED.as_bytes()).await?;
            }
        }
        Ok::<_, std::io::Error>(socket)
    };
    // Keep the real peer open after the chosen stall. The request is already
    // running and owns its timer before virtual time is paused and advanced.
    let socket = tokio::select! {
        peer = peer => peer?,
        _ = &mut response => return Err("request completed before the stall".into()),
    };
    tokio::time::pause();
    tokio::time::advance(HTTP_REQUEST_TIMEOUT + Duration::from_millis(1)).await;
    let result = tokio::time::timeout(Duration::from_secs(1), &mut response).await?;
    let error = result.err().ok_or("silent peer unexpectedly succeeded")?;
    assert_eq!(error, Error::Transport);
    assert_eq!(error.to_string(), "secrets: transport failed");
    assert!(!format!("{error:?}").contains(PLANTED));
    drop(socket);
    Ok(())
}

#[tokio::test]
async fn native_secrets_refuse_silent_response_headers() -> TestResult {
    refuses_silence(Stall::Headers).await
}

#[tokio::test]
async fn native_secrets_refuse_a_stalled_successful_response_body() -> TestResult {
    refuses_silence(Stall::Body).await
}
