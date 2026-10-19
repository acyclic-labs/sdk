use super::*;
use proptest::prelude::*;

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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Registration, help/model contracts and call grants are the same catalog
    // projection for every binding/opt-out combination; custom tools survive.
    #[test]
    fn catalog_drives_registration_contracts_and_grants(
        volume in any::<bool>(), workspace in any::<bool>(), disabled in any::<bool>(),
        custom in "custom[.][a-z]{1,24}", description in ".{0,64}",
    ) {
        let mut extra = example_tool();
        extra.definition.name = custom.clone();
        extra.definition.description = description;
        let expected_extra = extra.definition.clone();
        let mut builder = HarnessBuilder::new()
            .agent_loop(Arc::new(DelegatingLoop))
            .tool(extra);
        if volume { builder = builder.content(super::registry::content()); }
        if workspace { builder = builder.workspace(Arc::new(Echo)); }
        if disabled { builder = builder.without_default_tools(); }
        let bundle = builder.build().unwrap();
        prop_assert_eq!(bundle.runtime().tool(&custom).unwrap(), expected_extra);
        let mut names = std::collections::BTreeSet::new();
        for spec in tools::catalog() {
            prop_assert!(names.insert(spec.name));
            let available = !disabled && match spec.binding {
                tools::ToolBinding::Volume => volume,
                tools::ToolBinding::Workspace => workspace,
            };
            let actual = bundle.runtime().tool(spec.name);
            prop_assert_eq!(actual.is_ok(), available);
            prop_assert_eq!(bundle.capabilities().contains(&spec.capability()), available);
            if available {
                let actual = actual.unwrap();
                prop_assert_eq!(actual.description.as_str(), spec.description);
                prop_assert_eq!(actual, spec.definition().unwrap());
            }
            let bound = spec.bind().unwrap();
            prop_assert_eq!(bound.hook_names, spec.hook_names);
        }
        prop_assert_eq!(names.len(), 9);
    }
}
