use super::{
    TestResult,
    diff_case::{DiffCase, Input},
    diff_oracle, scenario_data,
};
use proptest::prelude::*;
use serde_json::{Value, json};
use std::path::Path;

fn fixture(name: &str) -> TestResult<Value> {
    let root = Path::new(acyclic_harness::build_metadata::MANIFEST_DIR).join("tests/fixtures");
    Ok(scenario_data::load_value(
        &root,
        &root.join(format!("diffs/{name}.json")),
    )?)
}

fn input() -> impl Strategy<Value = Option<Input>> {
    prop::option::of(prop_oneof![
        prop::collection::vec(any::<char>(), 0..32)
            .prop_map(|chars| Input::Text(chars.into_iter().collect())),
        prop::collection::vec(any::<u8>(), 0..64).prop_map(Input::Bytes),
    ])
}

proptest! {
    // Presence, UTF-8 text, arbitrary bytes, bounds and exact expectations survive
    // the same codec used by discovery and fuzzing, without normalizing inputs.
    #[test]
    fn diff_case_codec_preserves_bytes_presence_bounds_and_expectations(
        before in input(), after in input(), render in any::<usize>(), work in any::<usize>(),
        expect in prop_oneof![
            Just(Value::Null), any::<bool>().prop_map(Value::from),
            any::<i64>().prop_map(Value::from), ".{0,32}".prop_map(Value::from),
            prop::collection::vec(any::<i64>(), 0..8).prop_map(|values| json!({"values":values})),
        ],
    ) {
        let expected = DiffCase { before, after, render, work, expect };
        let actual: DiffCase = serde_json::from_slice(&serde_json::to_vec(&expected)?)?;
        prop_assert_eq!(expected, actual);
    }
}

#[test]
fn diff_oracles_reject_wrong_patch_counts_hunks_kind_and_outcome() -> TestResult {
    let baseline = fixture("text/replace")?;
    run(baseline.clone())?;
    for field in [
        "kind",
        "binary",
        "patch",
        "patch-insertion",
        "added",
        "removed",
        "hunks",
        "hunk-extra",
        "extra",
    ] {
        let mut changed = baseline.clone();
        if field == "hunk-extra" {
            changed["expect"]["hunks"][0]["extra"] = json!(true);
            assert!(run(changed).is_err(), "accepted extra hunk field");
            continue;
        }
        let key = if field == "patch-insertion" {
            "patch"
        } else {
            field
        };
        changed["expect"][key] = match field {
            "kind" => json!("added"),
            "binary" => json!(true),
            "patch" => json!("corrupted patch"),
            "patch-insertion" => json!(format!(
                "{}\n",
                changed["expect"]["patch"].as_str().ok_or("patch")?
            )),
            "added" | "removed" => json!(99),
            "hunks" => {
                let mut hunks = changed["expect"]["hunks"]
                    .as_array()
                    .ok_or("hunks")?
                    .clone();
                hunks.push(hunks[0].clone());
                json!(hunks)
            }
            _ => json!("injected"),
        };
        assert!(run(changed).is_err(), "accepted wrong {field}");
    }
    for name in [
        "binary/invalid-bytes",
        "unchanged/missing",
        "errors/header-bound",
    ] {
        let baseline = fixture(name)?;
        run(baseline.clone())?;
        let mut changed = baseline;
        changed["expect"] = json!("wrong outcome");
        assert!(run(changed).is_err(), "accepted wrong {name}");
    }
    Ok(())
}

#[test]
fn diff_case_codec_rejects_unknown_fields_missing_expectations_and_invalid_bytes() -> TestResult {
    let baseline = fixture("text/replace")?;
    let _: DiffCase = serde_json::from_value(baseline.clone())?;
    for field in [
        "unknown",
        "expect",
        "before-object",
        "negative-byte",
        "large-byte",
        "render",
        "work",
    ] {
        let mut changed = baseline.clone();
        match field {
            "unknown" => changed["typo"] = json!(true),
            "expect" => {
                changed.as_object_mut().ok_or("case")?.remove("expect");
            }
            "before-object" => changed["before"] = json!({"text":"wrong representation"}),
            "negative-byte" => changed["before"] = json!([-1]),
            "large-byte" => changed["after"] = json!([256]),
            "render" => changed["render"] = json!(-1),
            _ => changed["work"] = json!("wrong type"),
        }
        assert!(
            serde_json::from_value::<DiffCase>(changed).is_err(),
            "accepted {field}"
        );
    }
    Ok(())
}

#[test]
fn diff_case_codec_replays_fuzz_seeds() -> TestResult {
    let root = Path::new(acyclic_harness::build_metadata::MANIFEST_DIR)
        .join("../harness-fuzz/corpus/client_diff");
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().starts_with("case-") {
            let case: DiffCase = serde_json::from_slice(&std::fs::read(entry.path())?)?;
            let decoded: DiffCase = serde_json::from_slice(&serde_json::to_vec(&case)?)?;
            assert_eq!(case, decoded);
        }
    }
    Ok(())
}

fn run(value: Value) -> TestResult {
    diff_oracle::run(serde_json::from_value(value)?)
}

#[test]
fn bounded_diff_unit_cases() -> TestResult {
    let root =
        Path::new(acyclic_harness::build_metadata::MANIFEST_DIR).join("tests/fixtures/diffs");
    let mut cases = Vec::new();
    scenario_data::discover(&root, &mut cases)?;
    assert_eq!(cases.len(), 29);
    for path in cases {
        run(scenario_data::load_value(&root, &path)?)?;
    }
    Ok(())
}
