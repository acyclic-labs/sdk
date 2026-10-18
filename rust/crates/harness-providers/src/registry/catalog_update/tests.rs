use super::*;
use proptest::prelude::*;

thread_local! {
    pub(super) static SKIP_RETAINED_DEFAULT: std::cell::Cell<bool> = const {std::cell::Cell::new(false)};
}

fn specification(mask: u8) -> ProviderSpec {
    let base = crate::catalog::default_catalog()["openai"].clone();
    let model = base.models.values().next().unwrap().clone();
    ProviderSpec {
        authentication: Authentication::Optional,
        token_counter: Some(crate::TokenCounterSpec {
            path: "count".into(),
            adapter: base.adapter,
            response_field: crate::TokenCountField::InputTokens,
            framing_tokens: 0,
        }),
        models: (0..3)
            .filter(|index| mask & (1 << index) != 0)
            .map(|index| (format!("model{index}"), model.clone()))
            .collect(),
        ..base
    }
}

fn load(registry: &mut Registry, mask: u8, default: Option<u8>) -> Result<()> {
    let mut document = serde_json::json!({"fixture":specification(mask)});
    if let Some(default) = default {
        document["default_model"] = serde_json::json!(format!("fixture/model{default}"));
    }
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), serde_json::to_vec(&document).unwrap()).unwrap();
    registry.load_catalog(file.path())
}

fn sequence(operations: &[(bool, u8, Option<u8>)]) {
    let mut registry = Registry::new(Credentials::explicit(BTreeMap::new())).unwrap();
    load(&mut registry, 7, Some(0)).unwrap();
    let (mut retained_mask, mut retained_default) = (7u8, 0u8);
    for &(reload, mask, proposed) in operations {
        let proposed = if reload { proposed } else { None };
        let candidate = proposed.unwrap_or(retained_default);
        let expected = mask & (1 << candidate) != 0;
        let before = serde_json::to_vec(registry.catalog()["fixture"].as_ref()).unwrap();
        let result = if reload {
            load(&mut registry, mask, proposed)
        } else {
            registry.register("fixture".into(), specification(mask))
        };
        assert_eq!(
            result.is_ok(),
            expected,
            "catalog transaction preserves its declared default"
        );
        if expected {
            retained_mask = mask;
            retained_default = candidate;
        } else {
            assert_eq!(
                serde_json::to_vec(registry.catalog()["fixture"].as_ref()).unwrap(),
                before,
                "rejected transaction is atomic"
            );
        }
        assert_eq!(
            registry.default_model(),
            format!("fixture/model{retained_default}")
        );
        assert!(registry.resolve(registry.default_model()).is_ok());
        let actual: Vec<_> = registry.catalog()["fixture"]
            .models
            .keys()
            .cloned()
            .collect();
        let expected: Vec<_> = (0..3)
            .filter(|index| retained_mask & (1 << index) != 0)
            .map(|index| format!("model{index}"))
            .collect();
        assert_eq!(actual, expected);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn arbitrary_load_and_register_sequences_keep_defaults_resolvable(
        operations in proptest::collection::vec((any::<bool>(),0u8..8,proptest::option::of(0u8..3)),0..24),
    ) {
        sequence(&operations);
    }
}

#[test]
fn negative_control_detects_retained_default_loss_in_both_entry_points() {
    for reload in [false, true] {
        SKIP_RETAINED_DEFAULT.with(|flag| flag.set(true));
        let detected = std::panic::catch_unwind(|| sequence(&[(reload, 2, None)]));
        SKIP_RETAINED_DEFAULT.with(|flag| flag.set(false));
        let panic =
            detected.expect_err("unvalidated replacement must fail the same default oracle");
        let text = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .unwrap_or("");
        assert!(
            text.contains("catalog transaction preserves its declared default"),
            "{text}"
        );
    }
}
