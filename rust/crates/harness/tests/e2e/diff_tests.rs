use super::diff_case::DiffCase;

proptest::proptest! {
    #[test]
    fn local_diff_case_codec_preserves_script_files_and_wire_expectations(
        text in ".{0,64}", deleted in proptest::bool::ANY,
    ) {
        for bytes in [
            include_bytes!("../../../harness-fuzz/corpus/client_diff/local-harness").as_slice(),
            include_bytes!("../../../harness-fuzz/corpus/client_diff/local-remove").as_slice(),
        ] {
            let mut value: serde_json::Value = serde_json::from_slice(bytes)?;
            if value["turns"][0][0]["name"] == "write" {
                value["turns"][0][0]["arguments"]["text"] = text.clone().into();
            }
            value["expect"]["files"]["plans/a.md"] = if deleted { serde_json::Value::Null } else { text.clone().into() };
            let case: DiffCase = serde_json::from_value(value)?;
            let encoded = serde_json::to_vec(&case)?;
            let decoded: DiffCase = serde_json::from_slice(&encoded)?;
            proptest::prop_assert_eq!(serde_json::to_value(case)?, serde_json::to_value(decoded)?);
        }
    }
}
