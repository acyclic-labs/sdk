//! Bounded RSA/mTLS Inference Runs fixture for installed language consumers.

use acyclic_sdk_examples::tls_fixture::{
    INFERENCE_RUNS_RPC_METHODS, RsaTlsMaterial, new_method_transcript_log,
    serve_inference_runs_rsa_with_transcript,
};
use serde_json::json;
use std::{collections::BTreeSet, env, fs, path::PathBuf};
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
    let transcript_file = env::var_os("ACYCLIC_FIXTURE_TRANSCRIPT_FILE").map(PathBuf::from);
    let transcript = new_method_transcript_log();
    let (shutdown, receiver) = oneshot::channel();
    let server_material = material.clone();
    let server_transcript = transcript.clone();
    let server = tokio::spawn(async move {
        serve_inference_runs_rsa_with_transcript(
            listener,
            &server_material,
            receiver,
            server_transcript,
        )
        .await
    });
    println!(
        "{}",
        serde_json::to_string(&json!({
            "schema": "acyclic.sdk.inference-runs-rsa-fixture.v1",
            "endpoint": endpoint,
            "caCertificate": material.ca_certificate,
            "certificate": material.client_certificate,
            "privateKey": material.client_private_key,
            "runId": hex::encode([2u8; 16]),
            "expectedRpcs": INFERENCE_RUNS_RPC_METHODS,
            "sourceSha256": option_env!("SDK_EXAMPLES_SOURCE_SHA256"),
            "buildTarget": option_env!("SDK_EXAMPLES_BUILD_TARGET"),
            "seconds": seconds,
            "transcriptFile": transcript_file.as_ref().map(|path| path.to_string_lossy()),
            "tls": { "keyAlgorithm": "RSA-2048", "mutual": true, "hostname": "localhost" },
            "serviceAvailability": "local_fixture_only",
        }))?
    );
    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
    let _ = shutdown.send(());
    server.await??;
    if let Some(path) = transcript_file {
        let mut entries = transcript
            .lock()
            .expect("Inference fixture transcript mutex poisoned")
            .clone();
        entries.sort_by_key(|entry| entry.rpc);
        let methods = entries
            .iter()
            .map(|entry| {
                json!({
                    "rpc": entry.rpc,
                    "requestBytes": entry.request_bytes,
                    "requestBase64": entry.request_base64,
                    "requestSha256": entry.request_sha256,
                    "responseBytes": entry.response_bytes,
                    "responseBase64": entry.response_base64,
                    "responseSha256": entry.response_sha256,
                    "responseFrames": entry.response_frames.iter().map(|frame| json!({
                        "bytesBase64": frame.response_base64,
                        "sha256": frame.response_sha256,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect::<Vec<_>>();
        let observed_rpcs = methods
            .iter()
            .filter_map(|entry| entry.get("rpc").and_then(|rpc| rpc.as_str()))
            .collect::<BTreeSet<_>>();
        let expected_rpcs = INFERENCE_RUNS_RPC_METHODS
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let artifact = json!({
            "schema": "acyclic.sdk.inference-runs-rsa-fixture-transcript.v1",
            "source": {
                "path": "rust/crates/sdk-examples",
                "sha256": option_env!("SDK_EXAMPLES_SOURCE_SHA256"),
                "buildTarget": option_env!("SDK_EXAMPLES_BUILD_TARGET"),
            },
            "expectedRpcs": INFERENCE_RUNS_RPC_METHODS,
            "observedRpcs": observed_rpcs,
            "expectedMethodCount": INFERENCE_RUNS_RPC_METHODS.len(),
            "observedMethodCount": methods.len(),
            "complete": observed_rpcs == expected_rpcs
                && methods.len() == INFERENCE_RUNS_RPC_METHODS.len(),
            "methods": methods,
        });
        fs::write(path, serde_json::to_vec_pretty(&artifact)?)?;
    }
    Ok(())
}
