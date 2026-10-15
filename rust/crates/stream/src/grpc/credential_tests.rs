use super::*;
use proptest::prelude::*;

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
                let request = renewed.request(());
                let metadata = request.metadata().get("authorization").unwrap();
                prop_assert_eq!(metadata.to_str().unwrap(), format!("Bearer {token}"));
                prop_assert!(metadata.is_sensitive());
                let fixed = original.request(());
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
