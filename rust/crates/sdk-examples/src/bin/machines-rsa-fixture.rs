//! Bounded RSA/mTLS Machines fixture for installed language consumers.

use acyclic_sdk_examples::tls_fixture::{RsaTlsMaterial, serve_machines_rsa};
use serde_json::json;
use std::env;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let seconds = env::var("ACYCLIC_FIXTURE_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(120)
        .clamp(1, 300);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
    let material = RsaTlsMaterial::generate()?;
    let (shutdown, receiver) = oneshot::channel();
    let server_material = material.clone();
    let server =
        tokio::spawn(async move { serve_machines_rsa(listener, &server_material, receiver).await });
    println!(
        "{}",
        serde_json::to_string(&json!({
            "schema": "acyclic.sdk.machines-rsa-fixture.v1",
            "endpoint": endpoint,
            "caCertificate": material.ca_certificate,
            "certificate": material.client_certificate,
            "privateKey": material.client_private_key,
            "machineId": hex::encode(acyclic_sdk_examples::tls_fixture::FIXTURE_MACHINE),
            "operationId": hex::encode(acyclic_sdk_examples::tls_fixture::FIXTURE_OPERATION),
            "seconds": seconds,
            "tls": { "keyAlgorithm": "RSA-2048", "mutual": true, "hostname": "localhost" },
            "serviceAvailability": "local_fixture_only",
        }))?
    );
    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
    let _ = shutdown.send(());
    server.await??;
    Ok(())
}
