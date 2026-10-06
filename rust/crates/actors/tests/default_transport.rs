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
                "{}".to_owned()
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
async fn actors_choose_verified_http_before_sending_the_application_request() {
    let family = BindingFamily::Actors;
    let (endpoint, server) = endpoint(family, control::archived_descriptor_digest(family), 2).await;
    let client = acyclic_actors::connect(&endpoint, "fixture-token")
        .await
        .unwrap();
    assert_eq!(client.transport(), "http");
    let response = client
        .inspect_actor(&acyclic_actors::wire::InspectActorRequest {
            actor_id: "fixture-actor".into(),
        })
        .await
        .unwrap();
    assert!(response.actor.is_none());
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        observed,
        [
            "GET /v1/sdk/actors/handshake HTTP/1.1",
            "POST /v1/actors/inspect HTTP/1.1"
        ]
    );
}

#[tokio::test]
async fn workers_choose_verified_http_without_consumer_feature_flags() {
    let family = BindingFamily::Workers;
    let (endpoint, server) = endpoint(family, control::archived_descriptor_digest(family), 1).await;
    let client = acyclic_workers::connect(&endpoint, "fixture-token")
        .await
        .unwrap();
    assert_eq!(client.transport(), "http");
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed, ["GET /v1/sdk/workers/handshake HTTP/1.1"]);
}

#[tokio::test]
async fn incompatible_identity_is_terminal_before_any_application_request() {
    let (endpoint, server) =
        endpoint(BindingFamily::Actors, "substituted-descriptor".into(), 1).await;
    assert!(
        acyclic_actors::connect(&endpoint, "fixture-token")
            .await
            .is_err()
    );
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed, ["GET /v1/sdk/actors/handshake HTTP/1.1"]);
}
