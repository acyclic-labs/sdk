use super::*;
use proptest::prelude::*;

mod binding;
mod lookup;

#[cfg(feature = "filesystem")]
mod scaling;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn child_family_requires_spawn_as_well_as_each_tools_own_grant(selected in any::<[bool;9]>()) {
        let names = ["bash", "fs", "read", "write", "edit", "spawn", "send", "wait", "archive"];
        let registry = crate::tools::defaults().unwrap();
        let grants = crate::Capabilities::new(names.iter().enumerate().filter(|(i, _)| selected[*i]).map(|(_, name)| format!("tool:call:{name}")));
        let definitions = registry.definitions_for_grants(&grants, None).unwrap();
        for (index, name) in names.iter().enumerate() {
            prop_assert_eq!(definitions.iter().any(|definition| definition.name == *name),
                selected[index] && (index < 5 || selected[5]));
        }
    }
}

// Property: cloning a registry allocates nothing; later selection/registration
// changes preserve the old snapshot and reuse every retained tool payload.
proptest! {
    // Property: discarding model visibility never discards retained versions;
    // inserting either an older or newer revision must preserve its hidden state.
    #[test]
    fn manifest_hidden_names_remain_hidden_after_registering_new_revisions(
        existing in 0u8..16, offset in 1u8..16, retained in any::<bool>(),
    ) {
        let mut tools = ToolRegistry::default();
        let mut template = crate::tool::files::read()?;
        for name in ["before", "target", "z-after"] {
            if name == "target" && !retained { continue; }
            template.definition.name = name.into();
            template.definition.revision = existing.to_string();
            tools.register((template.clone(), None), &mut None, None)?;
        }
        tools.pin_model_definitions(&[], None)?;
        template.definition.name = "target".into();
        template.definition.revision = ((existing + offset) % 16).to_string();
        tools.register((template, None), &mut None, None)?;
        let selected = tools.definitions_for_grants(
            &crate::Capabilities::new(["tool:call:target"]), None,
        );
        if retained {
            prop_assert!(selected?.is_empty());
            prop_assert!(tools.get("target", Some(&existing.to_string()), None)?.is_some());
        } else {
            prop_assert_eq!(selected?.len(), 1);
        }
    }

    #[test]
    fn immutable_snapshots_share_payloads_and_isolate_selection(
        description in ".{0,80}", hide in any::<bool>(),
    ) {
        let mut original = ToolRegistry::default();
        let mut tool = crate::tool::files::read()?;
        tool.definition.description = description;
        let name = tool.definition.name.clone();
        let revision = tool.definition.revision.clone();
        original.register((tool, None), &mut None, None)?;
        let before = original.definitions(None)?;
        let mut cloned = None;
        let allocation = acyclic_test_support::measure(|| cloned = Some(original.clone()));
        prop_assert_eq!(allocation.count_total, 0);
        let mut cloned = cloned.ok_or_else(|| TestCaseError::fail("missing clone"))?;
        if hide { cloned.remove_from_model(&name)?; }
        let mut additional = crate::tool::files::read()?;
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
        let mut tool = crate::tool::files::read()?;
        tool.definition.revision = revision.into();
        tools.register((tool, None), &mut None, None)?;
    }
    assert!(tools.definitions(None).is_err());
    let mut selected = None;
    let allocation = acyclic_test_support::measure(|| {
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
        let mut tool = crate::tool::files::read()?;
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

fn checked<T, E: std::fmt::Debug>(
    result: std::result::Result<T, E>,
) -> std::result::Result<T, proptest::test_runner::TestCaseError> {
    result.map_err(|error| proptest::test_runner::TestCaseError::fail(format!("{error:?}")))
}

fn prototype() -> Result<Tool> {
    Ok(Tool {
        hook_names: &[],
        definition: ToolDefinition {
            name: "example.tool".into(),
            revision: "1".into(),
            description: String::new(),
            input_schema: Value::Bool(true),
            output_schema: Value::Bool(true),
            projection_schema: json_projection_schema(Value::Bool(true)),
        },
        executor: files::read()?.executor,
        projection: Arc::new(JsonToolProjection),
    })
}

fn indexed_names(registry: &ToolRegistry) -> bool {
    registry.data.selected.keys().eq(registry
        .data
        .versions
        .keys()
        .map(|(name, _)| name)
        .collect::<std::collections::BTreeSet<_>>())
}

// Property: the logical-name index is exactly the set of registered names
// after every successful or rejected public mutation, extension and clone.
proptest::proptest! {
    #[test]
    fn logical_name_index_is_complete(
        names in proptest::collection::vec("[a-z]{1,32}", 3),
        revisions in proptest::collection::vec("[a-z]{1,32}", 3),
        actions in proptest::collection::vec((0u8..7, 0usize..3, 0usize..3), 0..32),
    ) {
        let prototype = checked(prototype())?;
        let mut registry = ToolRegistry::default();
        for (action, name_index, revision_index) in actions {
            let name = &names[name_index];
            let revision = &revisions[revision_index];
            let existed = registry.data.versions.keys().any(|(logical, _)| logical == name);
            let registered = registry.data.versions.contains_key(&(name.clone(), revision.clone()));
            match action {
                0 | 5 => {
                    let mut tool = prototype.clone();
                    tool.definition.name = if action == 5 { format!("{name}@") } else { name.clone() };
                    tool.definition.revision.clone_from(revision);
                    let result = registry.register((tool, None), &mut None, None);
                    proptest::prop_assert_eq!(result.is_ok(), action == 0 && !registered);
                }
                1 => proptest::prop_assert_eq!(registry.select_model_version(name, revision).is_ok(), registered),
                2 => {
                    let result = registry.remove_from_model(name);
                    proptest::prop_assert_eq!(result.is_ok(), existed);
                    if !existed {
                        proptest::prop_assert!(matches!(result, Err(Error::NotFound(message)) if message == format!("tool {name}")), "missing-tool error changed");
                    }
                }
                3 => {
                    let copied = registry.clone();
                    proptest::prop_assert!(registry.data.versions.keys().eq(copied.data.versions.keys()));
                    proptest::prop_assert!(registry.data.selected.iter().eq(copied.data.selected.iter()));
                    registry = copied;
                }
                6 => {
                    let selected = registry.data.versions.get(&(name.clone(), revision.clone()))
                        .map(|tool| vec![tool.definition.clone()]).unwrap_or_default();
                    checked(registry.pin_model_definitions(&selected, None))?;
                    proptest::prop_assert_eq!(checked(registry.definitions(None))?, selected);
                }
                _ => {
                    let mut additional = ToolRegistry::default();
                    let mut tool = prototype.clone();
                    tool.definition.name.clone_from(name);
                    tool.definition.revision.clone_from(revision);
                    checked(additional.register((tool, None), &mut None, None))?;
                    let result = registry.extend(additional);
                    proptest::prop_assert_eq!(result.is_ok(), !existed);
                }
            }
            proptest::prop_assert!(indexed_names(&registry));
        }
        #[cfg(feature = "filesystem")]
        {
            use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
            let token = CancellationToken::new();
            let work = checked(NativeMaterializationWork::new(WorkBudget {
                bytes_copied: 16_777_216, bytes_encoded: 16_777_216,
                bytes_hashed: 16_777_216, source_bytes_read: 16_777_216,
                items_examined: 1_000_000, allocation_operations: 100_000,
                peak_allocation_bytes: 16_777_216, ..WorkBudget::default()
            }, &token))?;
            let (mut copied, custody) = checked(registry.clone_with_native_work(&work))?;
            proptest::prop_assert!(registry.data.versions.keys().eq(copied.data.versions.keys()));
            proptest::prop_assert!(registry.data.selected.iter().eq(copied.data.selected.iter()));
            proptest::prop_assert!(indexed_names(&copied));
            let remaining = checked(work.remaining())?;
            for name in registry.data.selected.keys() {
                checked(copied.remove_from_model(name))?;
            }
            proptest::prop_assert_eq!(checked(work.remaining())?, remaining);
            proptest::prop_assert!(indexed_names(&copied));
            drop(copied);
            drop(custody);
        }
    }
}

#[test]
fn hiding_requires_no_allocation_or_payload_copy() -> Result<()> {
    let prototype = prototype()?;
    let mut totals = Vec::new();
    for observing in [false, true] {
        let dispatch = if observing {
            tracing::Dispatch::new(tracing_subscriber::registry())
        } else {
            tracing::Dispatch::none()
        };
        let _guard = tracing::dispatcher::set_default(&dispatch);
        for count in [1, 64, 4096] {
            // Populate the two maps directly so fixture setup does not repeatedly
            // compile schemas. The property above verifies their shared key set.
            let mut registry = ToolRegistry::default();
            let data = Arc::make_mut(&mut registry.data);
            for index in 0..count {
                let name = format!("tool.{index:06}");
                let mut tool = prototype.clone();
                tool.definition.name.clone_from(&name);
                data.versions
                    .insert((name.clone(), "1".into()), Arc::new(tool));
                data.selected
                    .insert(name, ModelSelection::Revision("1".into()));
            }
            for position in [0, count / 2, count - 1] {
                let name = format!("tool.{position:06}");
                let mut times = Vec::with_capacity(64);
                let mut expected = None;
                for _ in 0..64 {
                    let mut result = None;
                    let started = std::time::Instant::now();
                    let allocation = acyclic_test_support::measure(|| {
                        result = Some(registry.remove_from_model(&name));
                    });
                    times.push(started.elapsed());
                    result.ok_or_else(|| Error::Invalid("removal did not run".into()))??;
                    let actual = (
                        allocation.count_total,
                        allocation.bytes_total,
                        allocation.bytes_max,
                    );
                    assert_eq!(*expected.get_or_insert(actual), actual);
                }
                times.sort_unstable();
                eprintln!(
                    "hide tracing={observing} tools={count} position={position}: allocation={expected:?}; median={:?}",
                    times.get(times.len() / 2)
                );
                totals.push(expected);
            }
        }
    }
    assert!(
        totals.iter().all(|value| *value == Some((0, 0, 0))),
        "hiding allocates: {totals:?}"
    );
    Ok(())
}
