use super::*;
use proptest::prelude::*;

thread_local! {
    pub(super) static DROP_CONFIGURED_PROVIDER: std::cell::Cell<bool> = const {std::cell::Cell::new(false)};
}

fn parse(bytes: &[u8]) -> Result<Option<String>, String> {
    Ok(parse_configuration(bytes, |_| false)?.selection)
}

#[test]
fn named_policies_and_roles_share_the_settings_snapshot() {
    let document = br#"
model = "mixed"
[roles]
root = "planner"
child = "worker"
[alloys.mixed]
models = ["a/tiny", "b/tiny"]
select = { per_role = { roles = { planner = 1 }, fallback = 0 } }
"#;
    let configured = parse_configuration(document, |_| false).unwrap();
    assert_eq!(configured.selection.as_deref(), Some("mixed"));
    assert_eq!(configured.roles.root, "planner");
    assert_eq!(configured.roles.child, "worker");
    assert_eq!(
        configured.alloys["mixed"]["models"],
        serde_json::json!(["a/tiny", "b/tiny"])
    );
    let catalog =
        parse_configuration(b"model = 'catalog-policy'", |name| name == "catalog-policy").unwrap();
    assert_eq!(catalog.selection.as_deref(), Some("catalog-policy"));
    assert_eq!(catalog.roles.root, "root");
    assert_eq!(catalog.roles.child, "child");
    for document in [
        b"model = 'mixed'\n[roles]\nroot = ' '".as_slice(),
        b"[roles]\nchild = ''",
        b"[roles]\nunknown = 'worker'",
    ] {
        assert!(parse_configuration(document, |_| true).is_err());
    }
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
