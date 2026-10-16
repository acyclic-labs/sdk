use super::*;
use proptest::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn credential_callbacks_are_fresh_sensitive_and_fail_before_rpc() {
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
                    let provider = GrpcProvider::grpc_with_auth(
                        TonicEndpoint::from_static("https://fixture.invalid").connect_lazy(),
                        BearerAuth::new("stale").unwrap(),
                    )
                    .with_credential_provider(CredentialProvider::new(move || {
                        std::future::ready(
                            values
                                .get(calls.fetch_add(1, Ordering::Relaxed))
                                .cloned()
                                .ok_or(acyclic_auth::credential::CredentialError::Invalid),
                        )
                    }));
                    prop_assert!(provider.authorization.0.is_none());
                    for value in &expected {
                        let mut authorization = provider.authorization().await.unwrap();
                        let request = authorization.call(Request::new(())).unwrap();
                        let header = request.metadata().get("authorization").unwrap();
                        prop_assert_eq!(header.to_str().unwrap(), format!("Bearer {value}"));
                        prop_assert!(header.is_sensitive());
                    }
                    prop_assert_eq!(index.load(Ordering::Relaxed), expected.len());
                    prop_assert!(matches!(
                        provider.client().await,
                        Err(ProviderError::Rejected(_))
                    ));
                    let key =
                        IdempotencyKey::parse("00000000-0000-0000-0000-000000000041").unwrap();
                    prop_assert!(matches!(
                        recovery::recovered_admission(&provider, key).await,
                        Err(ProviderError::Indeterminate(found)) if found == key
                    ));
                    Ok(())
                })
            },
        )
        .unwrap();
}
