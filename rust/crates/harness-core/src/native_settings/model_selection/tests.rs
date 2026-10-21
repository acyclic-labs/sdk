use super::*;
use proptest::prelude::*;

thread_local! {
    pub(super) static DROP_CONFIGURED_PROVIDER: std::cell::Cell<bool> = const {std::cell::Cell::new(false)};
}

fn opaque_name(provider: &str, segments: &[String]) {
    let name = segments.join("/");
    let text = toml::to_string(&std::collections::BTreeMap::from([
        ("model_provider", provider),
        ("model", name.as_str()),
    ]))
    .unwrap();
    assert_eq!(
        parse(text.as_bytes()).unwrap(),
        Some(format!("{provider}/{name}")),
        "provider and opaque model stay paired"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn configured_provider_preserves_every_opaque_model_segment(
        provider in "[a-z]{1,12}", segments in proptest::collection::vec("[a-z]{1,12}",2..6),
    ) {
        opaque_name(&provider, &segments);
    }
}

#[test]
fn negative_control_detects_provider_loss_for_slash_names() {
    DROP_CONFIGURED_PROVIDER.with(|flag| flag.set(true));
    let detected =
        std::panic::catch_unwind(|| opaque_name("fixture", &["vendor".into(), "tiny".into()]));
    DROP_CONFIGURED_PROVIDER.with(|flag| flag.set(false));
    let panic = detected.expect_err("discarding configured provider must fail the pair oracle");
    let text = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .unwrap_or("");
    assert!(
        text.contains("provider and opaque model stay paired"),
        "{text}"
    );
}
