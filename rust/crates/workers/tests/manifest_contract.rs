//! Shrinking manifest admission properties and generated artifact drift.
use acyclic_workers::manifest::*;
use proptest::prelude::*;

fn fixture() -> Result<DeploymentManifest, ManifestError> {
    parse(include_bytes!("../fuzz/corpus/manifest_json/valid.json"))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn admitted_manifests_round_trip_and_codegen_only_selected_handles(
        suffix in "[a-z][a-z0-9_]{0,24}", secret in "[A-Z][A-Z0-9_]{0,24}",
        port in 1u16..=u16::MAX, minute in 0u8..60, hour in 0u8..24,
    ) {
        let mut manifest = fixture()?;
        let environment = &mut manifest.environments[0];
        let entry = &mut environment.entries[0];
        entry.bindings = vec![suffix.clone()];
        entry.secrets = vec![secret.clone()];
        environment.bindings[0].name = suffix.clone();
        environment.secrets[0].name = secret.clone();
        environment.egress[0].port = port;
        let cron = &mut environment.crons[0];
        cron.schedule = format!("{minute} {hour} * * *");
        cron.inbox_binding = suffix.clone();
        let unselected = format!("other_{suffix}");
        environment.bindings.push(BindingSpec { name: unselected.clone(), kind: BindingKind::Compute,
            resource: "sandbox".into(), capability: "policies/compute".into() });
        let bytes = serde_json::to_vec(&manifest)?;
        prop_assert_eq!(parse(&bytes)?, manifest.clone());
        let generated = environment_typescript(&manifest, "production", "counter")?;
        prop_assert!(generated.contains(&format!("\"{suffix}\": Streams")), "selected Stream handle is present");
        prop_assert!(generated.contains(&format!("\"{secret}\": Secret")), "selected secret handle is present");
        prop_assert!(!generated.contains(&unselected));
    }

    #[test]
    fn duplicate_env_properties_are_always_rejected(name in "[a-z][a-z0-9_]{0,24}") {
        let mut manifest = fixture()?;
        let environment = &mut manifest.environments[0];
        let mut binding = environment.bindings[0].clone();
        binding.name = name.clone();
        environment.bindings.push(binding.clone());
        environment.bindings.push(binding);
        prop_assert!(validate(&manifest).is_err());
    }

    #[test]
    fn invalid_cron_ranges_are_rejected(value in 60u32..u32::MAX) {
        let mut manifest = fixture()?;
        manifest.environments[0].crons[0].schedule = format!("{value} * * * *");
        prop_assert!(validate(&manifest).is_err());
    }

    #[test]
    fn accepted_untrusted_json_round_trips(bytes in prop::collection::vec(any::<u8>(), 0..4096)) {
        if let Ok(manifest) = parse(&bytes) {
            prop_assert_eq!(parse(&serde_json::to_vec(&manifest)?)?, manifest);
        }
    }
}

#[test]
fn references_and_policy_fields_are_validated() -> Result<(), Box<dyn std::error::Error>> {
    let valid = fixture()?;
    let mutations: &[fn(&mut DeploymentManifest)] = &[
        |m: &mut DeploymentManifest| m.schema_version = 2,
        |m: &mut DeploymentManifest| m.environments[0].entries[0].bindings.push("absent".into()),
        |m: &mut DeploymentManifest| m.environments[0].entries[0].secrets.push("absent".into()),
        |m: &mut DeploymentManifest| m.environments[0].entries[0].artifact_sha256 = "AB".repeat(32),
        |m: &mut DeploymentManifest| m.environments[0].crons[0].entry = "absent".into(),
        |m: &mut DeploymentManifest| m.environments[0].crons[0].inbox_binding = "absent".into(),
        |m: &mut DeploymentManifest| {
            m.environments[0].crons[0].inbox_stream = "agents/../inbox".into()
        },
        |m: &mut DeploymentManifest| m.environments[0].entries[0].kind = EntryKind::Worker,
        |m: &mut DeploymentManifest| m.environments[0].bindings[0].kind = BindingKind::Compute,
        |m: &mut DeploymentManifest| m.environments[0].routes[0].entry = "absent".into(),
        |m: &mut DeploymentManifest| m.environments[0].secrets[0].name = "inbox".into(),
        |m: &mut DeploymentManifest| m.environments[0].egress[0].port = 0,
    ];
    for mutate in mutations {
        let mut manifest = valid.clone();
        mutate(&mut manifest);
        assert!(validate(&manifest).is_err());
    }
    for host in [
        "127.0.0.1",
        "::1",
        "localhost",
        "a.localhost",
        "a.local",
        "UPPER.example.com",
        "*.example.com",
        "user@example.com",
        "-a.example.com",
        "a.example.com.",
    ] {
        let mut manifest = valid.clone();
        manifest.environments[0].egress[0].host = host.into();
        assert!(validate(&manifest).is_err(), "{host}");
    }
    for schedule in [
        "* * * *",
        "* * * * * *",
        "*/0 * * * *",
        "* 24 * * *",
        "* * 0 * *",
        "* * * 13 *",
        "* * * * 7",
        "5-2 * * * *",
        "* * * jan *",
    ] {
        let mut manifest = valid.clone();
        manifest.environments[0].crons[0].schedule = schedule.into();
        assert!(validate(&manifest).is_err(), "{schedule}");
    }
    let mut unknown: serde_json::Value =
        serde_json::from_slice(include_bytes!("../fuzz/corpus/manifest_json/valid.json"))?;
    unknown["environments"][0]["secretValues"] = serde_json::json!({"MODEL_KEY":"plaintext"});
    assert!(parse(&serde_json::to_vec(&unknown)?).is_err());
    Ok(())
}

#[test]
fn generated_artifacts_have_no_drift() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = fixture()?;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let outputs = [
        (
            "typescript/packages/workers/src/generated/manifest.ts",
            typescript(),
        ),
        (
            "typescript/packages/workers/src/generated/manifest.schema.json",
            json_schema()?,
        ),
        (
            "typescript/packages/workers/test/generated/env.ts",
            environment_typescript(&manifest, "production", "counter")?,
        ),
    ];
    for (path, generated) in outputs {
        let path = root.join(path);
        if std::env::var_os("ACYCLIC_WRITE_MANIFEST_TYPES").is_some() {
            std::fs::create_dir_all(path.parent().ok_or("missing parent")?)?;
            std::fs::write(&path, &generated)?;
        }
        assert_eq!(std::fs::read_to_string(path)?, generated);
    }
    Ok(())
}

#[test]
fn environments_assets_and_worker_entries_are_isolated() -> Result<(), Box<dyn std::error::Error>> {
    let mut manifest = fixture()?;
    let production = &mut manifest.environments[0];
    production.bindings.push(BindingSpec {
        name: "static_assets".into(),
        kind: BindingKind::Objects,
        resource: "buckets/static".into(),
        capability: "policies/static-read".into(),
    });
    production.entries[0].bindings.push("static_assets".into());
    production.assets.push(AssetSpec {
        entry: "counter".into(),
        binding: "static_assets".into(),
        prefix: "static/".into(),
    });
    validate(&manifest)?;
    let mut staging = manifest.environments[0].clone();
    staging.name = "staging".into();
    staging.crons.clear();
    staging.entries[0].kind = EntryKind::Worker;
    staging.bindings[0].kind = BindingKind::Volume;
    manifest.environments.push(staging);
    let production = environment_typescript(&manifest, "production", "counter")?;
    let staging = environment_typescript(&manifest, "staging", "counter")?;
    assert!(production.contains("\"inbox\": Streams"));
    assert!(staging.contains("\"inbox\": Volume"));
    assert!(environment_typescript(&manifest, "absent", "counter").is_err());
    assert!(environment_typescript(&manifest, "production", "absent").is_err());
    manifest.environments[0].assets[0].binding = "inbox".into();
    assert!(validate(&manifest).is_err());
    Ok(())
}

#[test]
fn checked_fuzz_corpus_has_accepted_and_rejected_inputs() -> Result<(), Box<dyn std::error::Error>>
{
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus/manifest_json");
    let mut accepted = 0;
    let mut rejected = 0;
    for entry in std::fs::read_dir(root)? {
        match parse(&std::fs::read(entry?.path())?) {
            Ok(manifest) => {
                assert_eq!(parse(&serde_json::to_vec(&manifest)?)?, manifest);
                accepted += 1;
            }
            Err(_) => rejected += 1,
        }
    }
    assert!(accepted > 0 && rejected > 0);
    Ok(())
}
