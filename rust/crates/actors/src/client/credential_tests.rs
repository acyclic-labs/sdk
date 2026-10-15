use super::BearerAuth;
use proptest::prelude::*;
use tonic::service::Interceptor;

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
