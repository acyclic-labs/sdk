use super::*;
use proptest::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn credential_failure_covers_publication_and_every_lifecycle_operation() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32))
        .run(&prop::collection::vec(0..4u8, 1..16), |operations| {
            runtime.block_on(async {
                let count = Arc::new(AtomicUsize::new(0));
                let calls = count.clone();
                let client = Client::with_credential_provider("https://actors.invalid", "https://objects.invalid",
                    CredentialProvider::new(move || {
                        calls.fetch_add(1, Ordering::Relaxed);
                        std::future::ready(Ok(String::new()))
                    }), 1024).unwrap();
                for operation in &operations {
                    let denied = match operation {
                        0 => matches!(client.publish(bytes::Bytes::from_static(b"module")).await,
                            Err(Error::Objects(error)) if error.code == acyclic_objects::wire::ErrorCode::AccessDenied),
                        1 => matches!(client.invoke(&wire::InvokeWorkerRequest {
                            worker_name: "worker-a".into(), method: "POST".into(), url: "/".into(), ..Default::default()
                        }).await, Err(Error::Actors(http::Error::Credential(_)))),
                        2 => matches!(client.delete(&wire::DeleteWorkerRequest {
            worker_name: "worker-a".into(), idempotency_key: "delete".into(), ..Default::default() }).await,
                            Err(Error::Actors(http::Error::Credential(_)))),
                        _ => matches!(client.deploy(&wire::DeploymentRequest { name: "worker-a".into(), code_sha256: vec![1;32], worker_limits: Some(wire::WorkerLimits { timeout_millis: 1, memory_bytes: 1, cpu_millis: 1 }), idempotency_key: "deploy".into(), ..Default::default() }).await,
                            Err(Error::Actors(http::Error::Credential(_)))),
                    };
                    prop_assert!(denied);
                }
                prop_assert_eq!(count.load(Ordering::Relaxed), operations.len());
                Ok(())
            })
        }).unwrap();
}

#[tokio::test]
async fn published_code_survives_collection_until_unpublished() {
    let actors = http::Client::with_credential_provider(
        "https://actors.invalid",
        CredentialProvider::new(|| std::future::ready(Ok(String::new()))),
        1024,
    )
    .unwrap();
    let client = Client::new(actors, acyclic_objects::MemoryObjects::memory());
    let code = bytes::Bytes::from_static(b"module");
    let digest = client.publish(code.clone()).await.unwrap();
    let blob = acyclic_objects::Digest::of(&code);
    for _ in 0..2 {
        client.objects.collect().await.unwrap();
    }
    assert_eq!(client.objects.head(blob).await.unwrap().length, 6);
    assert!(client.unpublish(&digest).await.unwrap());
    for _ in 0..2 {
        client.objects.collect().await.unwrap();
    }
    assert!(client.objects.head(blob).await.is_err());
}
