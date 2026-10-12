use super::*;

pub(super) fn tool_propagate(error: &Error, io: Option<&crate::executor::PreparationIo>) -> bool {
    if tool_native(io) {
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

// Tuple values precede their allocation guards, including on early error.
pub(super) fn tool_metadata<T>(
    io: Option<&crate::executor::PreparationIo>,
) -> Result<crate::NativeCustody> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        return Ok(Some(work.claim(
            std::mem::size_of::<T>() as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?));
    }
    Ok(None)
}

pub(super) fn tool_native(io: Option<&crate::executor::PreparationIo>) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        io.and_then(crate::executor::PreparationIo::original_native_work)
            .is_some()
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    {
        let _ = io;
        false
    }
}

pub(in crate::durable_tool) fn tool_error(
    io: Option<&crate::executor::PreparationIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if tool_native(io) {
        return Error::NativeBoundary { code, reason };
    }
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        _ => Error::Conflict(reason.into()),
    }
}

pub(super) fn tool_json_bound<T: serde::Serialize>(
    value: &T,
    maximum: u64,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let encoded_hold;
        let encoded = io.encode(value, maximum, true)?;
        encoded_hold = io.guard_bytes(&encoded)?;
        drop(encoded);
        drop(encoded_hold);
        return Ok(());
    }
    crate::executor::json::json_byte_length(value, maximum, &mut None).map(|_| ())
}

pub(super) fn tool_validate_projection(
    definition: &ToolDefinition,
    value: &Value,
    limits: Limits,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let content = definition.validate_projection(value, limits, io)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io
        && io.original_native_work().is_some()
    {
        let bytes = content.logical_owned_bytes()?;
        drop(content);
        return io.release(bytes).map_err(Into::into);
    }
    drop(content);
    Ok(())
}

pub(super) fn tool_project(
    tool: &crate::tool::Tool,
    invocation: &ToolInvocation,
    result: &ToolResult,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Value, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let (value, custody) = tool.projection.project(invocation, result, Some(work))?;
        let custody = custody.ok_or_else(|| {
            tool_error(
                io,
                crate::wire::ErrorCode::Unsupported,
                "tool projection has no native producer",
            )
        })?;
        return Ok((value, Some(custody)));
    }
    tool.projection.project(invocation, result, None)
}

pub(super) fn tool_string(
    value: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(String, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let (value, custody) = work.display(value)?;
        return Ok((value, Some(custody)));
    }
    Ok((value.into(), None))
}

pub(super) fn tool_label(
    prefix: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(String, crate::NativeCustody)> {
    let id = OperationId::new();
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let (value, custody) = work.display(&format_args!("{prefix}:{id}"))?;
        return Ok((value, Some(custody)));
    }
    Ok((format!("{prefix}:{id}"), None))
}

pub(super) fn tool_success(
    mut result: (ToolResult, crate::NativeCustody),
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Outcome<Value>, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let guard = result.1.as_mut().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "native tool result has no custody",
        })?;
        if !guard.work().shares_ledger(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native tool result belongs to another work",
            });
        }
        let metadata =
            (std::mem::size_of::<Outcome<Value>>() - std::mem::size_of::<ToolResult>()) as u64;
        guard.resize(
            guard
                .bytes()
                .checked_add(metadata)
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "native tool outcome ownership overflow",
                })?,
        )?;
        return Ok((Outcome::Succeeded(result.0.value), result.1));
    }
    Ok((Outcome::Succeeded(result.0.value), None))
}

pub(super) fn tool_outcome(
    mut outcome: Outcome<Value>,
    failure: Option<ToolFailureKind>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Outcome<Value>, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let mut custody = work.claim(
            std::mem::size_of::<Outcome<Value>>() as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        if let Some(reason) = failure {
            let (message, mut hold) = work.display(reason.message())?;
            custody.absorb(&mut hold)?;
            outcome = Outcome::Failed { message };
        }
        return Ok((outcome, Some(custody)));
    }
    if let Some(reason) = failure {
        outcome = Outcome::Failed {
            message: reason.message().into(),
        };
    }
    Ok((outcome, None))
}
