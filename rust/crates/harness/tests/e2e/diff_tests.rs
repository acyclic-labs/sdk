use super::{TestResult, diff_case::DiffCase, diffs, scenario};
use std::path::Path;

proptest::proptest! {
    #[test]
    fn local_diff_case_codec_preserves_script_files_and_wire_expectations(text in ".{0,64}") {
        let bytes = include_bytes!("../../../harness-fuzz/corpus/client_diff/local-harness");
        let mut value: serde_json::Value = serde_json::from_slice(bytes)?;
        value["turns"][0][0]["arguments"]["text"] = text.clone().into();
        value["expect"]["files"]["plans/a.md"] = text.into();
        let case: DiffCase = serde_json::from_value(value)?;
        let encoded = serde_json::to_vec(&case)?;
        let decoded: DiffCase = serde_json::from_slice(&encoded)?;
        proptest::prop_assert_eq!(serde_json::to_value(case)?, serde_json::to_value(decoded)?);
    }
}

#[tokio::test]
async fn dropping_a_real_publication_breaks_unchanged_renderer_expectations() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let value = scenario::load_value(&root, &root.join("diffs/replace.json"))?;
    let mut value = value;
    value.as_object_mut().ok_or("case")?.remove("driver");
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        diffs::run(serde_json::from_value(value.clone())?),
    )
    .await??;
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        diffs::run_with_consumer(serde_json::from_value::<DiffCase>(value)?, true),
    )
    .await?
    .unwrap_err();
    assert!(
        error.to_string().contains("rendered diffs differ"),
        "{error}"
    );
    Ok(())
}
