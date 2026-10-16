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
                let client = Client::with_credential_provider("https://actors.invalid", "https://objects.invalid", "workers".into(),
                    CredentialProvider::new(move || {
                        calls.fetch_add(1, Ordering::Relaxed);
                        std::future::ready(Ok(String::new()))
                    }), 1024).unwrap();
                for operation in &operations {
                    let denied = match operation {
                        0 => matches!(client.publish(bytes::Bytes::from_static(b"module"), "publish".into()).await,
                            Err(Error::Objects(error)) if error.code == objects::ErrorCode::AccessDenied),
                        1 => matches!(client.invoke(&wire::InvokeActorRequest {
                            actor_id: "worker-a".into(), method: "POST".into(), url: "/".into(), ..Default::default()
                        }).await, Err(Error::Actors(http::Error::Credential(_)))),
                        2 => matches!(client.delete(&wire::DeleteActorRequest { actor_id: "worker-a".into(), ..Default::default() }).await,
                            Err(Error::Actors(http::Error::Credential(_)))),
                        _ => matches!(client.create(&wire::CreateActorRequest::default()).await,
                            Err(Error::Actors(http::Error::Credential(_)))),
                    };
                    prop_assert!(denied);
                }
                prop_assert_eq!(count.load(Ordering::Relaxed), operations.len());
                Ok(())
            })
        }).unwrap();
}
