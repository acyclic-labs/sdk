use super::{BearerAuth, Client, CredentialProvider};
use proptest::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tonic::service::Interceptor;

#[test]
fn credential_callbacks_are_fresh_sensitive_and_fail_closed() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32))
        .run(&prop::collection::vec("[a-zA-Z0-9_-]{1,64}", 2..8), |tokens| {
            runtime.block_on(async {
                let expected = tokens.clone();
                let index = Arc::new(AtomicUsize::new(0));
                let calls = index.clone();
                let channel = tonic::transport::Endpoint::from_static("https://fixture.invalid").connect_lazy();
                let client = Client {
                    inner: crate::wire::actors_service_client::ActorsServiceClient::with_interceptor(
                        crate::grpc::TracedChannel::new(channel, crate::grpc::transport_span), BearerAuth(None)),
                    credential: None,
                }.with_credential_provider(CredentialProvider::new(move || {
                    std::future::ready(tokens.get(calls.fetch_add(1, Ordering::Relaxed)).cloned()
                        .ok_or(acyclic_auth::credential::CredentialError::Invalid))
                }));
                let mut interceptor = BearerAuth(None);
                prop_assert!(interceptor.call(tonic::Request::new(())).is_err());
                for token in &expected {
                    let request = interceptor.call(client.request((), None).await.unwrap()).unwrap();
                    let header = request.metadata().get("authorization").unwrap();
                    prop_assert_eq!(header.to_str().unwrap(), format!("Bearer {token}"));
                    prop_assert!(header.is_sensitive());
                }
                prop_assert_eq!(index.load(Ordering::Relaxed), expected.len());
                prop_assert!(matches!(client.request((), None).await, Err(super::Error::Credential(_))));
                Ok(())
            })
        }).unwrap();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn credential_overrides_are_sensitive_and_request_local(
        fixed in "[a-zA-Z0-9_-]{1,128}",
        renewed in prop::collection::vec("[a-zA-Z0-9_-]{1,128}", 1..16),
    ) {
        let mut interceptor = BearerAuth::new(&fixed).unwrap();
        for token in renewed {
            let mut request = tonic::Request::new(());
            request.extensions_mut().insert(BearerAuth::new(&token).unwrap());
            let request = interceptor.call(request).unwrap();
            let metadata = request.metadata().get("authorization").unwrap();
            prop_assert_eq!(metadata.to_str().unwrap(), format!("Bearer {token}"));
            prop_assert!(metadata.is_sensitive());
            prop_assert!(request.extensions().get::<BearerAuth>().is_none());

            let next = interceptor.call(tonic::Request::new(())).unwrap();
            prop_assert_eq!(next.metadata().get("authorization").unwrap().to_str().unwrap(), format!("Bearer {fixed}"));
        }
    }

    #[test]
    fn credential_admission_never_accepts_header_delimiters(
        prefix in "[a-zA-Z0-9_-]{0,64}",
        suffix in "[a-zA-Z0-9_-]{0,64}",
        delimiter in prop::sample::select(vec!['\r', '\n', '\0']),
    ) {
        let token = format!("{prefix}{delimiter}{suffix}");
        prop_assert!(BearerAuth::new(&token).is_err());
    }
}
