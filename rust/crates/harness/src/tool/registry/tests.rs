use super::*;

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
        executor: files::read_file()?.executor,
        projection: Arc::new(JsonToolProjection),
    })
}

fn indexed_names(registry: &ToolRegistry) -> bool {
    registry.selected.keys().eq(registry
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
        actions in proptest::collection::vec((0u8..6, 0usize..3, 0usize..3), 0..32),
    ) {
        let prototype = checked(prototype())?;
        let mut registry = ToolRegistry::default();
        for (action, name_index, revision_index) in actions {
            let name = &names[name_index];
            let revision = &revisions[revision_index];
            let existed = registry.versions.keys().any(|(logical, _)| logical == name);
            let registered = registry.versions.contains_key(&(name.clone(), revision.clone()));
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
                3 => registry = registry.clone(),
                _ => {
                    let mut additional = ToolRegistry::default();
                    let mut tool = prototype.clone();
                    tool.definition.name.clone_from(name);
                    tool.definition.revision.clone_from(revision);
                    checked(additional.register((tool, None), &mut None, None))?;
                    let combined = registry.clone().extend(additional);
                    proptest::prop_assert_eq!(combined.is_ok(), !existed);
                    if let Ok(combined) = combined { registry = combined; }
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
            proptest::prop_assert!(indexed_names(&copied));
            let remaining = checked(work.remaining())?;
            for name in registry.selected.keys() {
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
            for index in 0..count {
                let name = format!("tool.{index:06}");
                let mut tool = prototype.clone();
                tool.definition.name.clone_from(&name);
                registry.versions.insert((name.clone(), "1".into()), tool);
                registry
                    .selected
                    .insert(name, ModelSelection::Revision("1".into()));
            }
            for position in [0, count / 2, count - 1] {
                let name = format!("tool.{position:06}");
                let mut times = Vec::with_capacity(64);
                let mut expected = None;
                for _ in 0..64 {
                    let mut result = None;
                    let started = std::time::Instant::now();
                    let allocation = allocation_counter::measure(|| {
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
