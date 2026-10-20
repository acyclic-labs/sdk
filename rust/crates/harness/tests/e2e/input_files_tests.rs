use super::{
    TestResult, drivers,
    file_bytes::FileBytes, input_files::InputFile,
    scenario,
};
use acyclic_harness::model::{
    FileProjectionPolicy, ImageDetail, NativeMediaIntent, NativeMediaPolicy,
};
use proptest::prelude::*;
use serde_json::{Value, json};
use std::path::Path;

#[path = "../../../harness-fuzz/fuzz_targets/input_files_input.rs"]
mod input_files_input;

fn contents() -> impl Strategy<Value = FileBytes> {
    prop_oneof![
        proptest::collection::vec(any::<char>(), 0..64)
            .prop_map(|text| FileBytes::Text(text.into_iter().collect())),
        proptest::collection::vec(any::<u8>(), 0..256).prop_map(FileBytes::Binary),
    ]
}

proptest! {
    // JSON round trips preserve every binary byte, Unicode character, path and
    // declared native bound; string and byte-array representations stay distinct.
    #[test]
    fn file_recipes_round_trip(
        bytes in contents(), path in proptest::collection::vec(any::<char>(), 0..64)
            .prop_map(|characters| characters.into_iter().collect::<String>()), native in any::<bool>(),
        maximum_bytes in any::<u64>(), maximum_work in any::<u64>(), detail in 0..3_usize,
    ) {
        let encoded = serde_json::to_vec(&bytes).unwrap();
        input_files_input::check(&encoded);
        prop_assert_eq!(serde_json::from_slice::<FileBytes>(&encoded).unwrap(), bytes.clone());
        let input = InputFile { path, contents:bytes, media_type:"image/png".into(),
            policy:if native { FileProjectionPolicy::Native(Box::new(NativeMediaPolicy {
                intent: NativeMediaIntent::Image { detail:[ImageDetail::Auto, ImageDetail::Low, ImageDetail::High][detail] },
                maximum_bytes, maximum_work, configuration:None,
            })) } else { FileProjectionPolicy::Reference } };
        let encoded = serde_json::to_vec(std::slice::from_ref(&input)).unwrap();
        input_files_input::check(&encoded);
        prop_assert_eq!(serde_json::from_slice::<Vec<InputFile>>(&encoded).unwrap(), vec![input]);
    }
}

#[test]
fn input_file_codec_rejects_bad_bytes_policies_and_fields() {
    for value in [
        json!(null),
        json!(true),
        json!([256]),
        json!([-1]),
        json!([1.5]),
        json!({"bytes":[]}),
    ] {
        assert!(serde_json::from_value::<FileBytes>(value).is_err());
    }
    for value in [
        json!({"path":"x", "contents":[], "typo":true}),
        json!({"path":"x", "contents":[256]}),
        json!({"path":"x", "contents":[], "policy":"guess"}),
    ] {
        assert!(serde_json::from_value::<InputFile>(value).is_err());
    }
    for (field, value) in [("input_files", json!([])), ("after_capture", json!({}))] {
        let mut input = json!({"kind":"summary", "instruction":"Summarize", "turns":["done"], "expect":{"text":"done"}});
        input[field] = value;
        assert!(scenario::Scenario::from_value(input).is_err());
    }
}

#[test]
fn input_file_fuzz_corpus() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../harness-fuzz/corpus/input_files");
    let mut count = 0;
    for entry in std::fs::read_dir(root)? {
        input_files_input::check(&std::fs::read(entry?.path())?);
        count += 1;
    }
    assert!(count > 0);
    Ok(())
}

#[tokio::test]
async fn image_oracles_reject_changed_policy_bytes_order_and_pins() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    for (file, mutation) in [
        ("native-auto", "policy"),
        ("native-auto", "request_bytes"),
        ("native-auto", "file_bytes"),
        ("native-auto", "metadata"),
        ("native-auto", "extra"),
        ("reordered-attachments", "order"),
        ("replaced-after-capture", "pin"),
        ("image-only", "missing"),
        ("image-only", "inserted_text"),
    ] {
        let path = root.join(format!("images/{file}.json"));
        let mut value = scenario::load_value(&root, &path)?;
        drivers::run(value.clone()).await?;
        match mutation {
            "policy" => {
                value["input_files"][0]["policy"]["native"]["intent"]["detail"] = json!("high")
            }
            "request_bytes" => value["expect"]["request_files"][0][0]
                .as_array_mut()
                .unwrap()
                .push(json!(0)),
            "file_bytes" => value["expect"]["files"]["image.png"]
                .as_array_mut()
                .unwrap()
                .push(json!(0)),
            "metadata" => {
                value["expect"]["requests"][0]["messages"][0]["content"][1]["file"]["descriptor"]
                    ["media_type"] = json!("text/plain")
            }
            "extra" => {
                let mut extra = value["input_files"][0].clone();
                extra["path"] = json!("extra.png");
                value["input_files"].as_array_mut().unwrap().push(extra);
            }
            "order" => value["input_files"].as_array_mut().unwrap().swap(0, 1),
            "pin" => value["expect"]["request_files"] = json!([["replaced"]]),
            "missing" => value["input_files"] = Value::Array(Vec::new()),
            "inserted_text" => value["prompt"] = json!("UNEXPECTED TEXT IN IMAGE-ONLY INPUT"),
            _ => unreachable!(),
        }
        assert!(
            drivers::run(value).await.is_err(),
            "accepted changed {mutation}"
        );
    }
    Ok(())
}
