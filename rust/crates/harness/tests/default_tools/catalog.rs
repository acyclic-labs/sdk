use super::*;

#[test]
fn catalog_preserves_pinned_file_contracts_and_digests() -> Result<()> {
    let contract: Value = serde_json::from_str(include_str!(
        "../../../../../fixtures/harness/v2/file-tools-contract.json"
    ))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    for name in ["read", "write", "edit"] {
        let spec = tools::catalog().find(|spec| spec.name == name).unwrap();
        assert_eq!(spec.binding, tools::ToolBinding::Volume);
        let pinned = contract["tool_definitions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| {
                entry["definition"]["name"] == spec.name
                    && entry["variant"].as_str().unwrap().ends_with("_full")
            })
            .unwrap();
        let definition = spec.definition()?;
        assert_eq!(
            serde_json::to_value(&definition).unwrap(),
            pinned["definition"]
        );
        assert_eq!(
            serde_json::to_value(definition.digest()?).unwrap(),
            pinned["definition_digest"]
        );
    }
    Ok(())
}
