//! Verify authenticated automatic transport selection against actual HTTP endpoints.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn endpoint(
    family: BindingFamily,
    digest: String,
    requests: usize,
) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut observed = Vec::new();
        for index in 0..requests {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                let mut chunk = [0; 1024];
                let count = stream.read(&mut chunk).await.unwrap();
                assert!(count > 0 && bytes.len() + count <= 64 * 1024);
                bytes.extend_from_slice(&chunk[..count]);
            }
            let request = String::from_utf8(bytes).unwrap();
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("authorization: bearer fixture-token\r\n")
            );
            observed.push(request.lines().next().unwrap().to_owned());
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
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        }
        observed
    });
    (format!("http://{address}"), task)
}

#[tokio::test]
async fn stream_chooses_verified_http_before_sending_the_application_request() {
    let family = BindingFamily::Stream;
    let (endpoint, server) = endpoint(family, control::archived_descriptor_digest(family), 2).await;
    let client = acyclic_stream::connect(&endpoint, "fixture-token")
        .await
        .unwrap();
    assert_eq!(client.transport(), "http");
    assert_eq!(client.stream("fixture").unwrap().tail().await.unwrap(), 7);
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        observed,
        [
            "GET /v1/sdk/stream/handshake HTTP/1.1",
            "POST /v1/stream/tail HTTP/1.1"
        ]
    );
}

#[tokio::test]
async fn incompatible_identity_is_terminal_before_any_application_request() {
    let (endpoint, server) =
        endpoint(BindingFamily::Stream, "substituted-descriptor".into(), 1).await;
    assert!(
        acyclic_stream::connect(&endpoint, "fixture-token")
            .await
            .is_err()
    );
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed, ["GET /v1/sdk/stream/handshake HTTP/1.1"]);
}
