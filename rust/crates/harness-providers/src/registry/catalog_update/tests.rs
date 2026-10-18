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
        let mut registry = Registry::new(Credentials::explicit(BTreeMap::new()))
            .unwrap()
            .with_codex_login(crate::CodexLogin::from_path("unused-auth.json".into()));
        let document = serde_json::json!({"default_model":"openai/gpt-5","openai":openai});
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), serde_json::to_vec(&document).unwrap()).unwrap();
        assert_eq!(registry.load_catalog(file.path()).is_ok(), accepted);
    }
}

// Five independent bits: counter, built-in URL, Responses dialect, API key,
// Codex login. The remaining dimension selects all three authentication policies.
fn credential_sequence(case: u8, operations: &[(bool, Option<u8>)]) {
    let counter = case & 1 != 0;
    let builtin = case & 2 != 0;
    let responses = case & 4 != 0;
    let key = case & 8 != 0;
    let login = case & 16 != 0;
    let authentication = match case / 32 {
        0 => Authentication::Optional,
        1 => Authentication::ApiKey,
        _ => Authentication::ApiKeyOrCodexLogin,
    };
    let credentials = Credentials::explicit(if key {
        BTreeMap::from([("fixture".into(), "test-key".into())])
    } else {
        BTreeMap::new()
    });
    let mut registry = Registry::new(credentials).unwrap();
    if login {
        registry =
            registry.with_codex_login(crate::CodexLogin::from_path("unused-auth.json".into()));
    }
    load(&mut registry, specification(7), Some(0)).unwrap();
    let mut spec = counted_specification(7, counter);
    spec.authentication = authentication;
    if !builtin {
        spec.base_url = "https://fixture.invalid/v1".into();
    }
    if !responses {
        spec.adapter = crate::Adapter::Chat;
        if let Some(counter) = &mut spec.token_counter {
            counter.adapter = spec.adapter;
        }
    }
    // Independent counting oracle: an available login is selected only without
    // an API key; its local counter exists only on the native Responses route.
    let selected_login = login && !key && case / 32 == 2;
    let valid_route = !selected_login || (builtin && responses);
    let accepted = valid_route && (counter || selected_login);
    for &(reload, proposed) in operations {
        let before = catalog_snapshot(&registry);
        let default_before = registry.default_model().to_owned();
        let result = if reload {
            load(&mut registry, spec.clone(), proposed)
        } else {
            registry.register("fixture".into(), spec.clone())
        };
        assert_eq!(
            result.is_ok(),
            accepted,
            "actual credential selection must supply the default counter: case {case}"
        );
        if accepted {
            let authenticated = key || selected_login || case / 32 == 0;
            assert_eq!(
                registry.resolve(registry.default_model()).is_ok(),
                authenticated && valid_route,
                "resolution agrees with credential and route selection: case {case}"
            );
        } else {
            assert_eq!(catalog_snapshot(&registry), before);
            assert_eq!(registry.default_model(), default_before);
            assert!(registry.resolve(registry.default_model()).is_ok());
        }
    }
}

#[test]
fn every_counting_credential_configuration_is_atomic_in_both_entry_points() {
    for case in 0..96 {
        credential_sequence(case, &[(false, None), (true, None), (true, Some(1))]);
    }
}

#[test]
fn counted_defaults_reject_unusable_login_routes_before_committing() {
    // Selected login, counter present, custom URL and/or non-Responses adapter.
    // Cover registration, retained-default reload, and incoming-default reload.
    for case in [81, 83, 85] {
        for operation in [(false, None), (true, None), (true, Some(1))] {
            credential_sequence(case, &[operation]);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn credential_selection_sequences_preserve_default_counting(
        case in 0u8..96,
        operations in proptest::collection::vec(
            (any::<bool>(), proptest::option::of(0u8..3)), 0..12),
    ) {
        credential_sequence(case, &operations);
    }
}

#[test]
fn negative_control_detects_assuming_the_codex_counting_exception() {
    // Both have a built-in Responses route and allow Codex authentication:
    // one has no login, the other has a login displaced by an API key.
    for case in [70, 94] {
        for operations in [[(false, None)], [(true, None)], [(true, Some(1))]] {
            credential_sequence(case, &operations);
            SKIP_DEFAULT_COUNTER.with(|flag| flag.set(true));
            let detected = std::panic::catch_unwind(|| credential_sequence(case, &operations));
            SKIP_DEFAULT_COUNTER.with(|flag| flag.set(false));
            let panic = detected.expect_err("assuming a login must fail the credential oracle");
            assert!(
                panic
                    .downcast_ref::<String>()
                    .unwrap()
                    .contains("actual credential selection must supply the default counter")
            );
        }
    }
}
