use super::*;
pub(super) fn tool_propagate(error: &Error, io: Option<&PreparationIo>) -> bool {
    if native_work(io).is_some() {
        return true;
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        error.is_native_failure()
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = error;
        false
    }
}
pub(super) fn tool_json_bound<T: serde::Serialize>(
    value: &T,
    maximum: u64,
    io: Option<&PreparationIo>,
) -> Result<()> {
    with_history_io(io, |work| {
        crate::executor::json::json_byte_length(value, maximum, work).map(|_| ())
    })
}
pub(super) fn tool_validate_projection(
    definition: &ToolDefinition,
    value: &Value,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<Result<()>> {
    let content = match definition.check_projection(value, limits, io)? {
        Ok(content) => content,
        Err(violation) => return Ok(Err(violation)),
    };
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let bytes = content.logical_owned_bytes()?;
        let custody = io.guard_owned(bytes)?;
        drop(content);
        drop(custody);
        return Ok(Ok(()));
    }
    drop(content);
    Ok(Ok(()))
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn validation_only_projection_releases_finite_and_native_result_trees() -> Result<()> {
    let definition = ToolDefinition {
        name: "tool".into(),
        revision: "1".into(),
        description: "fixture".into(),
        input_schema: Value::Bool(true),
        output_schema: Value::Bool(true),
        projection_schema: Value::Bool(true),
    };
    let source = serde_json::json!({"kind":"json", "value":{"outer":[{"inner":"value"}]}});
    for native in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        tool_validate_projection(&definition, &source, Limits::default(), Some(&io))??;
        assert_eq!(io.owned_bytes()?, 0);
        // A discarded decoded tree without a custody guard is the previous
        // finite path: dropping its Rust value alone leaves its charge live.
        let control = definition.validate_projection(&source, Limits::default(), Some(&io))?;
        drop(control);
        assert!(io.owned_bytes()? > 0);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
pub(super) fn tool_text(
    value: std::fmt::Arguments<'_>,
    io: Option<&PreparationIo>,
) -> Result<(String, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = native_work(io) {
        let (value, hold) = work.display(&value)?;
        return Ok((value, Some(hold)));
    }
    let _ = io;
    Ok((value.to_string(), None))
}
pub(super) fn tool_success(
    result: (ToolResult, NativeCustody),
    io: Option<&PreparationIo>,
) -> Result<(Outcome<Value>, NativeCustody)> {
    let _ = io;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = native_work(io) {
        let mut result = result;
        let guard = result
            .1
            .as_mut()
            .ok_or_else(|| boundary(io, Storage, "tool result has no custody"))?;
        if !guard.work().shares_ledger(work) {
            return Err(boundary(
                io,
                Unauthorized,
                "tool result belongs to another work",
            ));
        }
        let metadata =
            (std::mem::size_of::<Outcome<Value>>() - std::mem::size_of::<ToolResult>()) as u64;
        guard.resize(
            guard
                .bytes()
                .checked_add(metadata)
                .ok_or_else(|| boundary(io, Invalid, "tool ownership overflow"))?,
        )?;
        return Ok((Outcome::Succeeded(result.0.value), result.1));
    }
    Ok((Outcome::Succeeded(result.0.value), None))
}
pub(super) fn tool_outcome(
    operation_id: OperationId,
    failure: Option<(ToolFailureKind, &crate::executor::ToolFailureCause)>,
    io: Option<&PreparationIo>,
) -> Result<(Outcome<Value>, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let mut custody = native_work(io)
        .map(|work| {
            work.claim(
                std::mem::size_of::<Outcome<Value>>() as u64,
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )
        })
        .transpose()?;
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let custody = None;
    let outcome = if let Some((reason, cause)) = failure {
        let message = tool_text(
            format_args!("{}", crate::executor::ToolFailureText(reason, cause)),
            io,
        )?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut message = message;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let (Some(custody), Some(hold)) = (&mut custody, &mut message.1) {
            custody.absorb(hold)?;
        }
        Outcome::Failed { message: message.0 }
    } else {
        Outcome::Indeterminate {
            operation_id,
            cause: crate::IndeterminateCause::Unobserved,
        }
    };
    Ok((outcome, custody))
}
