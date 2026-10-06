//! Verify authenticated automatic transport selection against actual HTTP endpoints.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
use std::{error::Error, io};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn test_error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

async fn endpoint(
    family: BindingFamily,
    digest: String,
    requests: usize,
) -> TestResult<(String, tokio::task::JoinHandle<TestResult<Vec<String>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let mut observed = Vec::new();
        for index in 0..requests {
            let (mut stream, _) = listener.accept().await?;
            let mut bytes = Vec::new();
            while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                let mut chunk = [0; 1024];
                let count = stream.read(&mut chunk).await?;
                if count == 0 {
                    return Err(test_error("HTTP client closed before sending headers").into());
                }
                let total = bytes
                    .len()
                    .checked_add(count)
                    .filter(|length| *length <= 64 * 1024)
                    .ok_or_else(|| test_error("HTTP request headers exceeded 64 KiB"))?;
                let chunk = chunk
                    .get(..count)
                    .ok_or_else(|| test_error("HTTP read returned an invalid byte count"))?;
                bytes.extend_from_slice(chunk);
                if bytes.len() != total {
                    return Err(test_error("HTTP request byte accounting failed").into());
                }
            }
            let request = String::from_utf8(bytes)?;
            if !request
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-token\r\n")
            {
                return Err(test_error("HTTP request omitted the fixture authorization").into());
            }
            let request_line = request
                .lines()
                .next()
                .ok_or_else(|| test_error("HTTP request omitted its request line"))?;
            observed.push(request_line.to_owned());
            let body = if index == 0 {
                serde_json::json!({
                    "protocol": { "version": control::control_protocol_version(family), "descriptorDigest": digest },
                    "supported": { "capabilities": [{"name": family.name(), "version": control::control_protocol_version(family)}] }
                }).to_string()
            } else {
                "\"7\"".to_owned()
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await?;
            stream.shutdown().await?;
        }
        Ok(observed)
    });
    Ok((format!("http://{address}"), task))
}

#[tokio::test]
async fn stream_chooses_verified_http_before_sending_the_application_request() -> TestResult {
    let family = BindingFamily::Stream;
    let (endpoint, server) = endpoint(family, control::archived_descriptor_digest(family), 2).await?;
    let client = acyclic_stream::connect(&endpoint, "fixture-token").await?;
    assert_eq!(client.transport(), "http");
    let stream = client
        .stream("fixture")
        .map_err(|error| test_error(format!("stream setup failed: {error}")))?;
    assert_eq!(stream.tail().await?, 7);
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server).await??;
    let observed = observed?;
    assert_eq!(
        observed,
        [
            "GET /v1/sdk/stream/handshake HTTP/1.1",
            "POST /v1/stream/tail HTTP/1.1"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn incompatible_identity_is_terminal_before_any_application_request() -> TestResult {
    let (endpoint, server) =
        endpoint(BindingFamily::Stream, "substituted-descriptor".into(), 1).await?;
    if acyclic_stream::connect(&endpoint, "fixture-token").await.is_ok() {
        return Err(test_error("incompatible identity unexpectedly succeeded").into());
    }
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server).await??;
    let observed = observed?;
    assert_eq!(observed, ["GET /v1/sdk/stream/handshake HTTP/1.1"]);
    Ok(())
}
