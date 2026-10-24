use super::*;

#[test]
fn rejected_projection_releases_its_private_pool_on_finite_and_native_ledgers() -> Result<()> {
    let source =
        serde_json::json!({"kind":"parts", "parts":[{"kind":"text", "text":"over limit"}]});
    let definition = ToolDefinition {
        name: "tool".into(),
        revision: "1".into(),
        description: "fixture".into(),
        input_schema: Value::Bool(true),
        output_schema: Value::Bool(true),
        projection_schema: Value::Bool(true),
    };
    let limits = crate::conversation::Limits {
        render_bytes: 1,
        ..Default::default()
    };
    for native in [false, true] {
        for unscoped_control in [false, true] {
            if native && unscoped_control {
                continue;
            }
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = work.live_allocation_bytes();
            let io = if native {
                crate::executor::PreparationIo::from_native_work(&work)?
            } else {
                crate::executor::PreparationIo::from_budget(budget, &cancellation)?
            };
            if unscoped_control {
                // The former finite path decoded directly into the caller's
                // pool. Rejecting and dropping its value left its charge live.
                let content = decode_projection(&source, Some(&io))?;
                let rejected = crate::executor::with_history_io(Some(&io), |work| {
                    content.validate_limits(limits, work)
                });
                assert!(rejected.is_err());
                drop(rejected);
                drop(content);
                assert!(io.owned_bytes()? > 0);
            } else {
                let rejected = definition.validate_projection(&source, limits, Some(&io));
                assert!(rejected.is_err());
                drop(rejected);
                assert_eq!(
                    io.owned_bytes()?,
                    0,
                    "rejected projection leaked its allocation charge"
                );
            }
            drop(io);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    Ok(())
}

#[test]
fn native_tool_definition_keeps_sparse_schema_slots_until_destruction() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let schema = r#"{"type":"object","properties":{"field":{"type":"string"}}}"#;
    let (definition, custody) =
        ToolDefinition::new("tool", "1", "fixture", schema, schema, schema, 4096, &work)?;
    let mut retained = std::mem::size_of::<ToolDefinition>() as u64
        + definition.name.capacity() as u64
        + definition.revision.capacity() as u64
        + definition.description.capacity() as u64;
    let mut former = retained;
    for schema in [
        &definition.input_schema,
        &definition.output_schema,
        &definition.projection_schema,
    ] {
        retained += crate::executor::json::json_value_retained_owned_bytes(schema)?;
        former += crate::executor::json::json_value_logical_owned_bytes(schema)?;
    }
    assert!(
        former < retained,
        "old inventory must omit sparse schema slots"
    );
    assert_eq!(work.live_allocation_bytes(), baseline + retained);
    drop(definition);
    drop(custody);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}
