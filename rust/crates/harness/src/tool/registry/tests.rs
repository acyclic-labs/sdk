use super::*;
use proptest::prelude::*;

// Property: cloning a registry allocates nothing; later selection/registration
// changes preserve the old snapshot and reuse every retained tool payload.
proptest! {
    // Property: discarding model visibility never discards retained versions;
    // inserting either an older or newer revision must remain ambiguous.
    #[test]
    fn retained_versions_are_found_on_both_sides_of_an_insertion(
        existing in 0u8..16, offset in 1u8..16, retained in any::<bool>(),
    ) {
        let mut tools = ToolRegistry::default();
        let mut template = crate::tool::files::read_file()?;
        for name in ["before", "target", "z-after"] {
            if name == "target" && !retained { continue; }
            template.definition.name = name.into();
            template.definition.revision = existing.to_string();
            tools.register((template.clone(), None), &mut None, None)?;
        }
        tools.pin_model_definitions(&[])?;
        template.definition.name = "target".into();
        template.definition.revision = ((existing + offset) % 16).to_string();
        tools.register((template, None), &mut None, None)?;
        let selected = tools.definitions_for_grants(
            &crate::Capabilities::new(["tool:call:target"]), None,
        );
        if retained {
            prop_assert!(matches!(selected, Err(Error::Conflict(_))));
        } else {
            prop_assert_eq!(selected?.len(), 1);
        }
    }

    #[test]
    fn immutable_snapshots_share_payloads_and_isolate_selection(
        description in ".{0,80}", hide in any::<bool>(),
    ) {
        let mut original = ToolRegistry::default();
        let mut tool = crate::tool::files::read_file()?;
        tool.definition.description = description;
        let name = tool.definition.name.clone();
        let revision = tool.definition.revision.clone();
        original.register((tool, None), &mut None, None)?;
        let before = original.definitions(None)?;
        let mut cloned = None;
        let allocation = allocation_counter::measure(|| cloned = Some(original.clone()));
        prop_assert_eq!(allocation.count_total, 0);
        let mut cloned = cloned.ok_or_else(|| TestCaseError::fail("missing clone"))?;
        if hide { cloned.remove_from_model(&name)?; }
        let mut additional = crate::tool::files::read_file()?;
        additional.definition.name = "additional".into();
        cloned.register((additional, None), &mut None, None)?;
        prop_assert_eq!(original.definitions(None)?, before);
        prop_assert!(std::ptr::eq(
            original.get(&name, Some(&revision), None)?.unwrap(),
            cloned.get(&name, Some(&revision), None)?.unwrap(),
        ));
        let visible = cloned.definitions_for_grants(&crate::Capabilities::new([
            format!("tool:call:{name}"), "tool:call:additional".into(),
        ]), None)?;
        prop_assert_eq!(visible.len(), if hide { 1 } else { 2 });
        prop_assert!(visible.iter().any(|definition| definition.name == "additional"));
        prop_assert_eq!(visible.iter().any(|definition| definition.name == name), !hide);
        prop_assert!(visible.windows(2).all(|pair| pair[0].name < pair[1].name));
    }
}

#[test]
fn denied_ambiguous_tools_are_filtered_before_resolution_or_copying() -> Result<()> {
    let mut tools = ToolRegistry::default();
    for revision in ["1", "2"] {
        let mut tool = crate::tool::files::read_file()?;
        tool.definition.revision = revision.into();
        tools.register((tool, None), &mut None, None)?;
    }
    assert!(tools.definitions(None).is_err());
    let mut selected = None;
    let allocation = allocation_counter::measure(|| {
        selected = Some(tools.definitions_for_grants(&crate::Capabilities::default(), None));
    });
    assert_eq!(allocation.count_total, 0);
    assert!(selected.expect("selection ran")?.is_empty());
    Ok(())
}

#[cfg(feature = "filesystem")]
#[test]
fn native_registry_storage_and_copy_remain_funded() -> Result<()> {
    use crate::filesystem::UploadRecord as _;
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .expect("native registry work");
    let make_tool = |name: &str| {
        let mut tool = crate::tool::files::read_file()?;
        tool.definition.name = name.into();
        let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
        tool.definition = crate::model::clone_tool_definition(&tool.definition, Some(&io))?;
        let retained =
            std::mem::size_of::<ToolDefinition>() as u64 + tool.definition.logical_owned_bytes()?;
        Ok::<_, Error>((tool, Some(io.into_native_custody(retained)?)))
    };
    let (mut registry, custody) = ToolRegistry::new(&work)?;
    let mut custody = Some(custody);
    for name in ["first", "second"] {
        registry.register(make_tool(name)?, &mut custody, Some(&work))?;
        assert_eq!(
            custody.as_ref().unwrap().bytes(),
            registry.native_owned_bytes()?
        );
    }
    let snapshot = registry.clone();
    assert!(matches!(
        registry.register(make_tool("shared")?, &mut custody, Some(&work)),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            ..
        })
    ));
    let (mut copied, paid) = snapshot.clone_with_native_work(&work)?;
    let mut paid = Some(paid);
    copied.register(make_tool("third")?, &mut paid, Some(&work))?;
    assert_eq!(paid.as_ref().unwrap().bytes(), copied.native_owned_bytes()?);
    assert_eq!(registry.definitions(None)?.len(), 2);
    assert_eq!(snapshot.definitions(None)?.len(), 2);
    assert_eq!(copied.definitions(None)?.len(), 3);
    Ok(())
}
