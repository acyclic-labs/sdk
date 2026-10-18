use super::*;
use proptest::prelude::*;

thread_local! {
    pub(super) static SKIP_RETAINED_DEFAULT: std::cell::Cell<bool> = const {std::cell::Cell::new(false)};
    pub(super) static SKIP_DEFAULT_COUNTER: std::cell::Cell<bool> = const {std::cell::Cell::new(false)};
    pub(in crate::registry) static SKIP_MODEL_IDENTITY: std::cell::Cell<bool> = const {std::cell::Cell::new(false)};
}

fn specification(mask: u8) -> ProviderSpec {
    counted_specification(mask, true)
}

fn counted_specification(mask: u8, counted: bool) -> ProviderSpec {
    let base = crate::catalog::default_catalog()["openai"].clone();
    let model = base.models.values().next().unwrap().clone();
    ProviderSpec {
        authentication: Authentication::Optional,
        token_counter: counted.then(|| crate::TokenCounterSpec {
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

fn load(registry: &mut Registry, spec: ProviderSpec, default: Option<u8>) -> Result<()> {
    let mut document = serde_json::json!({"fixture":spec});
    if let Some(default) = default {
        document["default_model"] = serde_json::json!(format!("fixture/model{default}"));
    }
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), serde_json::to_vec(&document).unwrap()).unwrap();
    registry.load_catalog(file.path())
}

fn sequence(operations: &[(bool, u8, Option<u8>, bool)]) {
    let mut registry = Registry::new(Credentials::explicit(BTreeMap::new())).unwrap();
    load(&mut registry, specification(7), Some(0)).unwrap();
    let (mut retained_mask, mut retained_default) = (7u8, 0u8);
    for &(reload, mask, proposed, counted) in operations {
        let proposed = if reload { proposed } else { None };
        let candidate = proposed.unwrap_or(retained_default);
        // Independent oracle: the default's model survives and its endpoint can count input.
        let expected = mask & (1 << candidate) != 0 && counted;
        let before = serde_json::to_vec(registry.catalog()["fixture"].as_ref()).unwrap();
        let spec = counted_specification(mask, counted);
        let result = if reload {
            load(&mut registry, spec, proposed)
        } else {
            registry.register("fixture".into(), spec)
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
        operations in proptest::collection::vec(
            (any::<bool>(), 0u8..8, proptest::option::of(0u8..3), any::<bool>()), 0..24),
    ) {
        sequence(&operations);
    }

    #[test]
    fn malformed_model_keys_never_commit_in_either_entry_point(
        operations in proptest::collection::vec(
            (any::<bool>(), 0u8..8, proptest::option::of(model_name()),
             proptest::collection::vec(model_name(), 0..5)), 0..20),
    ) {
        model_sequence(&operations);
    }
}

fn model_name() -> impl Strategy<Value = String> {
    prop_oneof![
        Just(String::new()),
        "[ \\t\\r\\n\\u{2003}]{1,8}",
        "model[0-2]",
        "[a-z][a-z /λ]{0,16}",
    ]
}

fn model_sequence(operations: &[(bool, u8, Option<String>, Vec<String>)]) {
    let mut registry = Registry::new(Credentials::explicit(BTreeMap::new())).unwrap();
    load(&mut registry, specification(7), Some(0)).unwrap();
    for (reload, mask, proposed, names) in operations {
        let mut spec = specification(*mask);
        let model = specification(1).models.remove("model0").unwrap();
        spec.models
            .extend(names.iter().cloned().map(|name| (name, model.clone())));
        let proposed = proposed.as_ref().filter(|_| *reload);
        let candidate = proposed
            .map(String::as_str)
            .unwrap_or_else(|| registry.default_model().strip_prefix("fixture/").unwrap());
        // Independent oracle: all identities must contain a non-whitespace character,
        // including entries that are never selected as the default.
        let expected = spec
            .models
            .keys()
            .all(|name| name.chars().any(|c| !c.is_whitespace()))
            && spec.models.contains_key(candidate);
        let before = catalog_snapshot(&registry);
        let default_before = registry.default_model().to_owned();
        let expected_keys: Vec<_> = spec.models.keys().cloned().collect();
        let result = if *reload {
            let mut document = serde_json::json!({"fixture":spec});
            if let Some(default) = proposed {
                document["default_model"] = serde_json::json!(format!("fixture/{default}"));
            }
            let file = tempfile::NamedTempFile::new().unwrap();
            std::fs::write(file.path(), serde_json::to_vec(&document).unwrap()).unwrap();
            registry.load_catalog(file.path())
        } else {
            registry.register("fixture".into(), spec)
        };
        assert_eq!(
            result.is_ok(),
            expected,
            "catalog transaction validates every model identity"
        );
        if expected {
            assert_eq!(
                registry.catalog()["fixture"]
                    .models
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>(),
                expected_keys
            );
            if let Some(default) = proposed {
                assert_eq!(registry.default_model(), format!("fixture/{default}"));
            }
        } else {
            assert_eq!(
                catalog_snapshot(&registry),
                before,
                "malformed identity leaves the entire catalog unchanged"
            );
            assert_eq!(registry.default_model(), default_before);
        }
        assert!(registry.resolve(registry.default_model()).is_ok());
    }
}

fn catalog_snapshot(registry: &Registry) -> Vec<u8> {
    let specs: BTreeMap<_, _> = registry
        .catalog()
        .iter()
        .map(|(id, spec)| (id, spec.as_ref()))
        .collect();
    serde_json::to_vec(&specs).unwrap()
}

#[test]
fn negative_control_detects_empty_model_keys_even_when_not_the_default() {
    for reload in [false, true] {
        for selected in [false, true] {
            if selected && !reload {
                continue;
            }
            let operations = [(reload, 1, selected.then(String::new), vec![String::new()])];
            model_sequence(&operations);
            SKIP_MODEL_IDENTITY.with(|flag| flag.set(true));
            let detected = std::panic::catch_unwind(|| model_sequence(&operations));
            SKIP_MODEL_IDENTITY.with(|flag| flag.set(false));
            let panic =
                detected.expect_err("unvalidated model keys must fail the same transaction oracle");
            assert!(
                panic
                    .downcast_ref::<String>()
                    .unwrap()
                    .contains("catalog transaction validates every model identity")
            );
        }
    }
}

#[test]
fn negative_control_detects_retained_default_loss_in_both_entry_points() {
    for reload in [false, true] {
        SKIP_RETAINED_DEFAULT.with(|flag| flag.set(true));
        let detected = std::panic::catch_unwind(|| sequence(&[(reload, 2, None, true)]));
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

#[test]
fn negative_control_detects_uncountable_defaults_in_both_entry_points() {
    for (reload, proposed) in [(false, None), (true, None), (true, Some(1))] {
        let operations = [(reload, 7, proposed, false)];
        sequence(&operations);
        SKIP_DEFAULT_COUNTER.with(|flag| flag.set(true));
        let detected = std::panic::catch_unwind(|| sequence(&operations));
        SKIP_DEFAULT_COUNTER.with(|flag| flag.set(false));
        let panic = detected.expect_err("an uncountable default must fail the same oracle");
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

#[test]
fn codex_login_route_remains_a_countable_default_without_a_counter() {
    let mut openai = crate::catalog::default_catalog()["openai"].clone();
    openai.token_counter = None;
    for (authentication, accepted) in [
        (Authentication::ApiKeyOrCodexLogin, true),
        (Authentication::ApiKey, false),
    ] {
        openai.authentication = authentication;
        let mut registry = Registry::new(Credentials::explicit(BTreeMap::new())).unwrap();
        let document = serde_json::json!({"default_model":"openai/gpt-5","openai":openai});
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), serde_json::to_vec(&document).unwrap()).unwrap();
        assert_eq!(registry.load_catalog(file.path()).is_ok(), accepted);
    }
}
