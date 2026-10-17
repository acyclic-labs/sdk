use super::*;
use proptest::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn credential_callbacks_preserve_current_headers_and_fail_before_transport() {
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
                    let client = Client::with_credential_provider(
                        "https://fixture.invalid",
                        CredentialProvider::new(move || {
                            std::future::ready(
                                values
                                    .get(calls.fetch_add(1, Ordering::Relaxed))
                                    .cloned()
                                    .ok_or(acyclic_auth::credential::CredentialError::Invalid),
                            )
                        }),
                        1024,
                    )
                    .unwrap();
                    for value in &expected {
                        let header = client.authorization().await.unwrap();
                        prop_assert_eq!(header.to_str().unwrap(), format!("Bearer {value}"));
                        prop_assert!(header.is_sensitive());
                    }
                    prop_assert_eq!(index.load(Ordering::Relaxed), expected.len());
                    let result = client
                        .inspect_actor(&wire::InspectActorRequest {
                            actor_id: "actor-a".into(),
                        })
                        .await;
                    prop_assert!(matches!(result, Err(Error::Credential(_))));
                    Ok(())
                })
            },
        )
        .unwrap();
}
