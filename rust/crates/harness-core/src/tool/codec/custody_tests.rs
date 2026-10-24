use super::*;

#[test]
fn tool_json_handoffs_keep_sparse_map_custody_until_value_destruction() -> Result<()> {
    let source = serde_json::json!({"outer": [{"inner": "value"}]});
    for encode in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let (copied, custody) = if encode {
            let (copied, custody) = value(&source, 4096, Some(&work))?;
            (
                copied,
                custody.ok_or_else(|| Error::Invalid("missing JSON custody".into()))?,
            )
        } else {
            clone_value(&source, Some(4096), &work)?
        };
        assert_eq!(copied, source);
        let retained = retained_value_bytes(&copied)?;
        assert_eq!(work.live_allocation_bytes(), baseline + retained);
        // The old handoff resized custody to logical entries, losing the
        // sparse BTreeMap slots while the returned Value was still alive.
        assert!(crate::executor::json::json_value_logical_owned_bytes(&copied)? < retained);
        drop(copied);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[test]
fn projected_sparse_json_retains_original_custody_on_finite_and_native_paths() -> Result<()> {
    let source = serde_json::json!({"kind":"json", "value":{"outer":[{"inner":"value"}]}});
    let definition = crate::tool::ToolDefinition {
        name: "tool".into(),
        revision: "1".into(),
        description: "fixture".into(),
        input_schema: Value::Bool(true),
        output_schema: Value::Bool(true),
        projection_schema: Value::Bool(true),
    };
    for native in [false, true] {
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
        let content = definition.validate_projection(
            &source,
            crate::conversation::Limits::default(),
            Some(&io),
        )?;
        let retained = content.logical_owned_bytes()?;
        assert_eq!(
            io.owned_bytes()?,
            retained,
            "projection retained unrelated scratch or lost its result custody"
        );
        let crate::model::ToolResultContent::Json { value } = &content else {
            return Err(Error::Invalid("unexpected projection fixture".into()));
        };
        let old_handoff = std::mem::size_of::<crate::model::ToolResultContent>() as u64
            + crate::executor::json::json_value_logical_owned_bytes(value)?;
        assert!(
            old_handoff < retained,
            "old projection handoff must omit sparse slots"
        );
        let custody = io.guard_owned(retained)?;
        drop(content);
        drop(custody);
        assert_eq!(io.owned_bytes()?, 0);
        cancellation.cancel();
        assert!(
            definition
                .validate_projection(&source, crate::conversation::Limits::default(), Some(&io))
                .is_err()
        );
        assert_eq!(io.owned_bytes()?, 0);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
