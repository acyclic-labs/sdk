use super::*;
use crate::credentials::{KeychainDecode, Source};
use proptest::prelude::*;
use std::sync::Mutex;

#[derive(Default)]
struct Sources {
    key: Option<Source>,
    decode_error: bool,
    reads: BTreeMap<(String, Source), usize>,
}

fn fixture() -> (Registry, Arc<Mutex<Sources>>) {
    let sources = Arc::new(Mutex::new(Sources::default()));
    let mut credentials = Credentials::default();
    let reader = sources.clone();
    credentials.source = Some(Arc::new(move |provider, source| {
        let mut state = reader.lock().unwrap();
        *state.reads.entry((provider.into(), source)).or_default() += 1;
        if provider != "openai" {
            return Ok(None);
        }
        if state.decode_error && source == Source::Keychain {
            return Err(KeychainDecode {
                provider: provider.into(),
            });
        }
        Ok((state.key == Some(source)).then(|| "snapshot-key".into()))
    }));
    let registry = Registry::new(credentials)
        .unwrap()
        .with_codex_login(crate::CodexLogin::from_path("unused-auth.json".into()));
    (registry, sources)
}

fn specification(counted: bool) -> ProviderSpec {
    let mut spec = crate::catalog::default_catalog()["openai"].clone();
    if !counted {
        spec.token_counter = None;
    }
    spec
}

fn load(registry: &mut Registry, counted: bool) -> Result<()> {
    let file = tempfile::NamedTempFile::new().unwrap();
    let document = json!({"default_model":"openai/gpt-5", "openai":specification(counted)});
    std::fs::write(file.path(), serde_json::to_vec(&document).unwrap()).unwrap();
    registry.load_catalog(file.path())
}

fn reads(sources: &Mutex<Sources>) -> BTreeMap<(String, Source), usize> {
    sources.lock().unwrap().reads.clone()
}

fn changed_key_sequence(broken: bool, operations: &[(Option<Source>, u8)]) {
    let (mut registry, sources) = fixture();
    registry.selection.live = broken;
    load(&mut registry, false).unwrap();
    for &(key, action) in operations {
        sources.lock().unwrap().key = key;
        let before = reads(&sources);
        match action {
            0 => {}
            1 | 2 => {
                let result = if action == 1 {
                    load(&mut registry, false)
                } else {
                    registry.register("openai".into(), specification(false))
                };
                assert_eq!(
                    result.is_ok(),
                    key.is_none(),
                    "refresh validates the new selection"
                );
                if let Err(error) = result {
                    assert!(error.to_string().contains("verified input token counter"));
                }
            }
            _ => unreachable!(),
        }
        // Rejected refreshes roll back the catalog AND its admitted selection.
        // Changed source state alone never displaces that selection.
        let resolved = registry.resolve(registry.default_model());
        assert!(
            resolved.is_ok(),
            "resolution must retain its admitted snapshot"
        );
        let resolved = resolved.unwrap();
        assert!(resolved.key.is_none());
        assert!(resolved.codex_login.is_some());
        if action == 0 {
            assert_eq!(
                reads(&sources),
                before,
                "resolve performs no credential I/O"
            );
        }
    }
}

#[test]
fn adding_a_key_changes_only_an_explicit_reload_or_registration() {
    for source in [Source::Keychain, Source::Environment, Source::Configuration] {
        changed_key_sequence(
            false,
            &[
                (Some(source), 0),
                (Some(source), 1),
                (Some(source), 2),
                (None, 1),
                (None, 2),
            ],
        );
    }
}

#[test]
#[should_panic(expected = "resolution must retain its admitted snapshot")]
fn negative_control_detects_live_reads_after_catalog_admission() {
    changed_key_sequence(true, &[(Some(Source::Keychain), 0)]);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn arbitrary_source_changes_and_updates_preserve_the_admitted_selection(
        operations in proptest::collection::vec(
            (proptest::option::of(prop_oneof![Just(Source::Keychain),
                Just(Source::Environment), Just(Source::Configuration)]), 0u8..3), 0..24),
    ) {
        changed_key_sequence(false, &operations);
    }
}

#[test]
fn every_entry_point_observes_each_credential_source_once_per_snapshot() {
    for first in 0..3 {
        for key in [
            None,
            Some(Source::Keychain),
            Some(Source::Environment),
            Some(Source::Configuration),
        ] {
            let (mut registry, sources) = fixture();
            sources.lock().unwrap().key = key;
            match first {
                0 => {
                    load(&mut registry, true).unwrap();
                }
                1 => {
                    registry
                        .register("openai".into(), specification(true))
                        .unwrap();
                }
                _ => {
                    registry.resolve("openai/gpt-5").unwrap();
                }
            }
            let observed = reads(&sources);
            assert!(observed.values().all(|&count| count == 1));
            for (source, expected) in [
                (Source::Keychain, 1),
                (
                    Source::Environment,
                    usize::from(key != Some(Source::Keychain)),
                ),
                (
                    Source::Configuration,
                    usize::from(!matches!(key, Some(Source::Keychain | Source::Environment))),
                ),
            ] {
                assert_eq!(
                    observed
                        .get(&("openai".into(), source))
                        .copied()
                        .unwrap_or(0),
                    expected
                );
            }
            for _ in 0..3 {
                let resolved = registry.resolve("openai/gpt-5").unwrap();
                assert_eq!(resolved.key.as_deref(), key.map(|_| "snapshot-key"));
                assert_eq!(resolved.codex_login.is_some(), key.is_none());
            }
            assert_eq!(reads(&sources), observed);
            load(&mut registry, true).unwrap();
            assert_eq!(
                reads(&sources),
                observed
                    .iter()
                    .map(|(id, count)| (id.clone(), count * 2))
                    .collect()
            );
            registry
                .register("openai".into(), specification(true))
                .unwrap();
            assert_eq!(
                reads(&sources),
                observed
                    .iter()
                    .map(|(id, count)| (id.clone(), count * 3))
                    .collect()
            );
        }
    }
}

#[test]
fn decode_failure_names_its_provider_and_is_observed_only_at_snapshot_time() {
    for first in 0..3 {
        let (mut registry, sources) = fixture();
        sources.lock().unwrap().decode_error = true;
        let error = match first {
            0 => load(&mut registry, true).err().unwrap(),
            1 => registry
                .register("openai".into(), specification(true))
                .err()
                .unwrap(),
            _ => registry.resolve("openai/gpt-5").err().unwrap(),
        };
        assert!(matches!(error, acyclic_harness::Error::Invalid(_)));
        assert_eq!(
            error.to_string(),
            "invalid request: provider openai: cannot decode keychain credential"
        );
        let observed = reads(&sources);
        sources.lock().unwrap().decode_error = false;
        for _ in 0..3 {
            assert_eq!(
                registry.resolve("openai/gpt-5").err().unwrap().to_string(),
                error.to_string()
            );
        }
        assert_eq!(reads(&sources), observed);
        load(&mut registry, true).unwrap();
        assert!(registry.resolve("openai/gpt-5").is_ok());
    }
}

#[test]
fn a_rejected_first_update_keeps_source_observations_without_its_policy() {
    let (mut registry, sources) = fixture();
    let mut spec = specification(false);
    spec.authentication = Authentication::ApiKey;
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        file.path(),
        serde_json::to_vec(&json!({
            "default_model":"openai/gpt-5", "openai":spec,
        }))
        .unwrap(),
    )
    .unwrap();
    assert!(registry.load_catalog(file.path()).is_err());
    let before = reads(&sources);
    assert!(
        registry
            .resolve("openai/gpt-5")
            .unwrap()
            .codex_login
            .is_some()
    );
    assert_eq!(reads(&sources), before);
}
