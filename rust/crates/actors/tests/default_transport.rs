//! Verify authenticated automatic transport selection against actual HTTP endpoints.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
use std::error::Error;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

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
                    return Err(io::Error::other("fixture closed before the HTTP headers").into());
                }
                let new_len = bytes
                    .len()
                    .checked_add(count)
                    .filter(|length| *length <= 64 * 1024)
                    .ok_or_else(|| io::Error::other("fixture request exceeded 64 KiB"))?;
                let received = chunk
                    .get(..count)
                    .ok_or_else(|| io::Error::other("fixture returned an invalid read length"))?;
                bytes.extend_from_slice(received);
                debug_assert_eq!(bytes.len(), new_len);
            }
            let request = String::from_utf8(bytes)?;
            if !request
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-token\r\n")
            {
                return Err(io::Error::other("fixture request omitted bearer authentication").into());
            }
            let request_line = request
                .lines()
                .next()
                .ok_or_else(|| io::Error::other("fixture request had no request line"))?;
            observed.push(request_line.to_owned());
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
            stream.write_all(response.as_bytes()).await?;
            stream.shutdown().await?;
        }
        Ok::<Vec<String>, Box<dyn Error + Send + Sync>>(observed)
    });
    Ok((format!("http://{address}"), task))
}

#[tokio::test]
async fn actors_choose_verified_http_before_sending_the_application_request() -> TestResult<()> {
    let family = BindingFamily::Actors;
    let (endpoint, server) = endpoint(family, control::archived_descriptor_digest(family), 2).await?;
    let client = acyclic_actors::connect(&endpoint, "fixture-token").await?;
    assert_eq!(client.transport(), "http");
    let response = client
        .inspect_actor(&acyclic_actors::wire::InspectActorRequest {
            actor_id: "fixture-actor".into(),
        })
        .await?;
    assert!(response.actor.is_none());
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server).await???;
    assert_eq!(
        observed,
        [
            "GET /v1/sdk/actors/handshake HTTP/1.1",
            "POST /v1/actors/inspect HTTP/1.1"
        ]
    );
    Ok(())
}

#[tokio::test]
async fn workers_choose_verified_http_without_consumer_feature_flags() -> TestResult<()> {
    let family = BindingFamily::Workers;
    let (endpoint, server) = endpoint(family, control::archived_descriptor_digest(family), 1).await?;
    let client = acyclic_workers::connect(&endpoint, "fixture-token").await?;
    assert_eq!(client.transport(), "http");
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server).await???;
    assert_eq!(observed, ["GET /v1/sdk/workers/handshake HTTP/1.1"]);
    Ok(())
}

#[tokio::test]
async fn incompatible_identity_is_terminal_before_any_application_request() -> TestResult<()> {
    let (endpoint, server) =
        endpoint(BindingFamily::Actors, "substituted-descriptor".into(), 1).await?;
    assert!(
        acyclic_actors::connect(&endpoint, "fixture-token")
            .await
            .is_err(),
        "an incompatible descriptor must stop before an application request"
    );
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server).await???;
    assert_eq!(observed, ["GET /v1/sdk/actors/handshake HTTP/1.1"]);
    Ok(())
}
