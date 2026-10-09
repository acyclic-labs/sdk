#![allow(missing_docs, reason = "contract example binary, not public API")]

use sha2::{Digest, Sha256};

fn main() -> Result<(), acyclic_workers::ContractError> {
    let module = b"export default async function run() { return new Uint8Array([7]); }";
    let digest = Sha256::digest(module).to_vec();
    let publication = acyclic_workers::wire::PublishVersionRequest {
        javascript_module: module.to_vec(),
        expected_sha256: digest.clone(),
        idempotency_key: "publish-example".into(),
    };
    acyclic_workers::validate_publish(&publication)?;

    let submission = acyclic_workers::wire::SubmitJobRequest {
        target: Some(acyclic_workers::wire::JobTarget {
            target: Some(acyclic_workers::wire::job_target::Target::DeploymentAlias(
                "current".into(),
            )),
        }),
        input: Some(acyclic_workers::wire::Payload {
            source: Some(acyclic_workers::wire::payload::Source::InlineBytes(vec![
                1, 2, 3,
            ])),
        }),
        limits: Some(acyclic_workers::wire::JobLimits {
            timeout_millis: 1_000,
            memory_bytes: 4 * 1024 * 1024,
            output_bytes: 4 * 1024,
        }),
        retry: Some(acyclic_workers::wire::RetryPolicy {
            max_attempts: 2,
            backoff_millis: 25,
        }),
        idempotency_key: "job-example".into(),
    };
    acyclic_workers::validate_submit(&submission)?;

    println!(
        "{}",
        serde_json::json!({
            "validated": true,
            "module": module.to_vec(),
            "digest": digest,
            "alias": "current",
            "publish_idempotency": "publish-example",
            "job_idempotency": "job-example",
            "input": [1, 2, 3],
            "max_attempts": 2,
            "timeout_millis": 1000,
            "memory_bytes": 4 * 1024 * 1024,
            "output_bytes": 4 * 1024,
            "backoff_millis": 25,
        })
    );
    Ok(())
}
