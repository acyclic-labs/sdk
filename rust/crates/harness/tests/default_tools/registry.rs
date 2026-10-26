use super::*;
use proptest::prelude::*;
use std::collections::BTreeSet;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Disjoint catalogs compose as set union; any overlap refuses atomically,
    // preserving every original selected definition.
    #[test]
    fn registry_extension_is_disjoint_union_or_unchanged(
        left in proptest::collection::btree_set("[a-c]{1,4}", 0..8),
        right in proptest::collection::btree_set("[a-c]{1,4}", 0..8),
    ) {
        let registry = |names: &BTreeSet<String>| {
            let mut registry = ToolRegistry::default();
            for name in names {
                let mut tool = example_tool();
                tool.definition.name = name.clone();
                registry.register((tool, None), &mut None, None).unwrap();
            }
            registry
        };
        let mut original = registry(&left);
        let before = original.definitions(None).unwrap();
        let result = original.extend(registry(&right));
        let accepted = result.is_ok();
        prop_assert_eq!(accepted, left.is_disjoint(&right));
        if result.is_ok() {
            let actual: BTreeSet<_> = original.definitions(None).unwrap().into_iter().map(|definition| definition.name).collect();
            prop_assert_eq!(actual, left.union(&right).cloned().collect::<BTreeSet<_>>());
        } else {
            prop_assert_eq!(original.definitions(None).unwrap(), before);
        }
    }
}

#[test]
fn registry_merge_rejects_duplicates_before_mutating_destination() -> Result<()> {
    let mut destination = acyclic_harness::tools::defaults()?;
    let mut incoming = ToolRegistry::default();
    incoming.register((example_tool(), None), &mut None, None)?;
    incoming.register(
        (acyclic_harness::tool::files::read()?, None),
        &mut None,
        None,
    )?;
    assert!(matches!(
        destination.extend(incoming),
        Err(Error::Conflict(_))
    ));
    assert!(destination.get("example.echo", None, None)?.is_none());
    assert!(destination.get("read", None, None)?.is_some());
    Ok(())
}

#[test]
fn registry_merge_retains_pinned_revisions_and_hidden_model_selections() -> Result<()> {
    let mut custom = ToolRegistry::default();
    custom.register((example_tool(), None), &mut None, None)?;
    let mut second = example_tool();
    second.definition.revision = "2".into();
    custom.register((second, None), &mut None, None)?;
    custom.select_model_version("example.echo", "2")?;
    custom.remove_from_model("example.echo")?;
    let mut combined = acyclic_harness::tools::defaults()?;
    combined.extend(custom)?;
    assert!(combined.get("example.echo", None, None)?.is_none());
    assert!(combined.get("example.echo", Some("1"), None)?.is_some());
    assert!(combined.get("example.echo", Some("2"), None)?.is_some());
    assert_eq!(combined.definitions(None)?.len(), 9);
    combined.select_model_version("example.echo", "2")?;
    assert_eq!(
        combined
            .get("example.echo", None, None)?
            .map(|tool| tool.definition.revision.as_str()),
        Some("2")
    );
    Ok(())
}
