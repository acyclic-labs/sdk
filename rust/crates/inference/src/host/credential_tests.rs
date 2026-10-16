use super::*;
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn credential_callbacks_are_fresh_sensitive_and_never_use_a_fixed_fallback() {
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
                    let client = Inference(Arc::new(Connection {
                        channel: Endpoint::from_static("https://fixture.invalid").connect_lazy(),
                        authorization: Some(authorization("stale").unwrap()),
                        credential: Some(CredentialProvider::new(move || {
                            std::future::ready(
                                values
                                    .get(calls.fetch_add(1, Ordering::Relaxed))
                                    .cloned()
                                    .ok_or(acyclic_auth::credential::CredentialError::Invalid),
                            )
                        })),
                        client_instance: [1; 16],
                    }));
                    for value in &expected {
                        let request = client.clone().request(()).await.unwrap();
                        let header = request.metadata().get("authorization").unwrap();
                        prop_assert_eq!(header.to_str().unwrap(), format!("Bearer {value}"));
                        prop_assert!(header.is_sensitive());
                    }
                    prop_assert_eq!(index.load(Ordering::Relaxed), expected.len());
                    prop_assert!(matches!(client.models().await, Err(Error::Credential(_))));
                    Ok(())
                })
            },
        )
        .unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn credential_callbacks_preserve_the_inference_byte_limit(length in 8193..=12288usize) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let client = Inference(Arc::new(Connection {
                channel: Endpoint::from_static("https://fixture.invalid").connect_lazy(),
                authorization: Some(authorization("stale").unwrap()),
                credential: Some(CredentialProvider::new(move || std::future::ready(Ok("a".repeat(length))))),
                client_instance: [1; 16],
            }));
            prop_assert!(matches!(client.request(()).await, Err(Error::Invalid(_))));
            Ok(())
        })?;
    }
}
