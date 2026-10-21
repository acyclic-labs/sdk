//! Cargo must not select native checkout scenarios for a portable-only build.

const REQUIRED: [&str; 3] = ["filesystem-local", "native-execution", "test-support"];

fn supports_scenarios(target: &toml::Value) -> bool {
    let Some(features) = target
        .get("required-features")
        .and_then(toml::Value::as_array)
    else {
        return false;
    };
    REQUIRED.into_iter().all(|required| {
        features
            .iter()
            .any(|feature| feature.as_str() == Some(required))
    })
}

#[test]
fn cargo_selects_dst_only_with_its_production_entry_points() {
    let manifest: toml::Value = toml::from_str(include_str!("../../Cargo.toml")).unwrap();
    let target = manifest["test"]
        .as_array()
        .unwrap()
        .iter()
        .find(|target| target["name"].as_str() == Some("dst"))
        .expect("registered DST target");
    assert!(
        supports_scenarios(target),
        "DST selected without its native entry points"
    );
    for required in REQUIRED {
        let mut broken = target.clone();
        broken["required-features"]
            .as_array_mut()
            .unwrap()
            .retain(|feature| feature.as_str() != Some(required));
        assert!(
            !supports_scenarios(&broken),
            "missing {required} escaped the gate"
        );
    }
}
