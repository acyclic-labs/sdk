use super::*;
use proptest::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn credential_callbacks_are_current_sensitive_and_never_fall_back() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32))
        .run(
            &prop::collection::vec("[a-zA-Z0-9_-]{1,64}", 2..8),
            |values| {
                runtime.block_on(async {
                    let expected = values.clone();
                    let index = Arc::new(AtomicUsize::new(0));
                    let calls = index.clone();
                    let client = GrpcObjects {
                        channel: Endpoint::from_static("https://fixture.invalid").connect_lazy(),
                        authorization: None,
                        credential: Some(CredentialProvider::new(move || {
                            std::future::ready(
                                values
                                    .get(calls.fetch_add(1, Ordering::Relaxed))
                                    .cloned()
                                    .ok_or(acyclic_auth::credential::CredentialError::Invalid),
                            )
                        })),
                        transfer_timeout: REQUEST_TIMEOUT,
                    };
                    for value in &expected {
                        let request = client.authenticated((), REQUEST_TIMEOUT).await.unwrap();
                        let header = request.metadata().get("authorization").unwrap();
                        prop_assert_eq!(header.to_str().unwrap(), format!("Bearer {value}"));
                        prop_assert!(header.is_sensitive());
                    }
                    prop_assert_eq!(index.load(Ordering::Relaxed), expected.len());
                    let error = client
                        .create_bucket(wire::CreateBucketRequest {
                            name: "fresh".into(),
                            mutation: Some(wire::MutationIdentity {
                                idempotency_key: "fresh-credential".into(),
                            }),
                        })
                        .await
                        .unwrap_err();
                    prop_assert_eq!(error.code, wire::ErrorCode::AccessDenied);
                    Ok(())
                })
            },
        )
        .unwrap();
}
