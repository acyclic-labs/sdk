use super::*;
use proptest::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn credential_callbacks_are_fresh_sensitive_and_fail_before_requests() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32))
        .run(&prop::collection::vec("[a-zA-Z0-9_-]{1,64}", 2..8), |values| {
            runtime.block_on(async {
                let expected = values.clone();
                let index = Arc::new(AtomicUsize::new(0));
                let calls = index.clone();
                let filesystem = HostedFs {
                    client: Client::new(Endpoint::from_static("https://fixture.invalid").connect_lazy()),
                    authorization: None,
                    credential: Some(CredentialProvider::new(move || std::future::ready(
                        values.get(calls.fetch_add(1, Ordering::Relaxed)).cloned()
                            .ok_or(acyclic_auth::credential::CredentialError::Invalid)))),
                    capabilities: wire::Capabilities::default(),
                    owner: Arc::new(()),
                };
                for value in &expected {
                    let request = filesystem.clone().request(()).await.unwrap();
                    let header = request.metadata().get("authorization").unwrap();
                    prop_assert_eq!(header.to_str().unwrap(), format!("Bearer {value}"));
                    prop_assert!(header.is_sensitive());
                }
                prop_assert_eq!(index.load(Ordering::Relaxed), expected.len());
                prop_assert!(matches!(filesystem.open_workspace("fresh").await,
                    Err(HostedFsError::Status(status)) if status.code() == tonic::Code::Unauthenticated));
                Ok(())
            })
        }).unwrap();
}

#[test]
fn invalid_connection_bounds_never_invoke_the_credential_callback() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32))
        .run(
            &(any::<bool>(), 0..MINIMUM_HANDSHAKE_RESPONSE_BYTES),
            |(request, response)| {
                runtime.block_on(async {
                    let calls = Arc::new(AtomicUsize::new(0));
                    let resolved = calls.clone();
                    let mut options = HostedFsOptions::new("https://fixture.invalid", "");
                    if request {
                        options.maximum_request_bytes = 0;
                    } else {
                        options.maximum_response_bytes = response;
                    }
                    let result = HostedFs::connect_with_credential(
                        options,
                        CredentialProvider::new(move || {
                            resolved.fetch_add(1, Ordering::Relaxed);
                            std::future::ready(Ok("fresh".into()))
                        }),
                    )
                    .await;
                    prop_assert!(matches!(result, Err(HostedFsError::InvalidOptions(_))));
                    prop_assert_eq!(calls.load(Ordering::Relaxed), 0);
                    Ok(())
                })
            },
        )
        .unwrap();
}
