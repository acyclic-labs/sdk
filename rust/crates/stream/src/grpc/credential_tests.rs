use super::*;
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn request_credentials_never_mutate_the_original_client() {
    let original = Client::from_channel(
        Endpoint::from_static("https://fixture.invalid").connect_lazy(),
        authorization("fixture").unwrap(),
    );
    let strategy = prop::collection::vec("[a-zA-Z0-9_-]{1,128}", 1..16);
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(128))
        .run(&strategy, |tokens| {
            for token in tokens {
                let renewed = original.with_credential(&token).unwrap();
                let request = futures::executor::block_on(renewed.request(())).unwrap();
                let metadata = request.metadata().get("authorization").unwrap();
                prop_assert_eq!(metadata.to_str().unwrap(), format!("Bearer {token}"));
                prop_assert!(metadata.is_sensitive());
                let fixed = futures::executor::block_on(original.request(())).unwrap();
                prop_assert_eq!(
                    fixed
                        .metadata()
                        .get("authorization")
                        .unwrap()
                        .to_str()
                        .unwrap(),
                    "Bearer fixture"
                );
                for delimiter in ['\r', '\n', '\0'] {
                    let invalid = format!("{token}{delimiter}");
                    prop_assert!(original.with_credential(&invalid).is_err());
                }
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn credential_provider_renews_each_physical_retry_without_changing_the_body() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32))
        .run(
            &prop::collection::vec("[a-zA-Z0-9_-]{1,32}", 2..8),
            |tokens| {
                runtime.block_on(async {
                    let index = Arc::new(AtomicUsize::new(0));
                    let calls = index.clone();
                    let values = tokens.clone();
                    let client = Client::from_channel(
                        Endpoint::from_static("https://fixture.invalid").connect_lazy(),
                        authorization("stale").unwrap(),
                    )
                    .with_credential_provider(CredentialProvider::new(move || {
                        let value = values.get(calls.fetch_add(1, Ordering::Relaxed)).cloned();
                        futures::future::ready(
                            value.ok_or(acyclic_auth::credential::CredentialError::Invalid),
                        )
                    }));
                    let mut sent = 0;
                    let (response, _) = client
                        .unary(
                            ATTEMPT_TIMEOUT,
                            wire::TailRequest {
                                path: "same/path".into(),
                            },
                            |_, request| {
                                assert_eq!(request.get_ref().path, "same/path");
                                let header = request.metadata().get("authorization").unwrap();
                                assert!(header.is_sensitive());
                                assert_eq!(
                                    header.to_str().unwrap(),
                                    format!("Bearer {}", tokens[sent])
                                );
                                sent += 1;
                                Box::pin(futures::future::ready(if sent < tokens.len() {
                                    Err(Status::unavailable("retry"))
                                } else {
                                    Ok(Response::new(wire::TailResponse { tail: 7 }))
                                }))
                            },
                        )
                        .await
                        .unwrap();
                    prop_assert_eq!(response.tail, 7);
                    prop_assert_eq!(sent, tokens.len());
                    prop_assert_eq!(index.load(Ordering::Relaxed), tokens.len());
                    // Exhaustion fails before transport; the static credential cannot be used.
                    let result: Result<((), Completion), StreamError> = client
                        .unary(ATTEMPT_TIMEOUT, (), |_, _| {
                            unreachable!("provider failure reached transport")
                        })
                        .await;
                    prop_assert!(matches!(result, Err(StreamError::AccessDenied)));
                    Ok(())
                })
            },
        )
        .unwrap();
}
