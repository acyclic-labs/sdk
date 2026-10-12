use super::*;

pub(crate) fn stock_boundary(
    io: Option<&PreparationIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.and_then(PreparationIo::original_native_work).is_some() {
        return crate::Error::NativeBoundary { code, reason };
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = io;
    match code {
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        crate::wire::ErrorCode::Storage => Error::Storage(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

pub(crate) fn stock_string_copy(value: &str, io: Option<&PreparationIo>) -> Result<String> {
    let mut copied = String::new();
    stock_string_push(&mut copied, value, io)?;
    Ok(copied)
}

pub(in crate::executor) fn stock_selection_copy(
    value: &ModelContextSelection,
    io: Option<&PreparationIo>,
) -> Result<ModelContextSelection> {
    let mut ids = Vec::new();
    for id in &value.message_ids {
        stock_vec_push(&mut ids, *id, io)?;
    }
    let checkpoint = value
        .checkpoint
        .as_ref()
        .map(|reference| crate::model::clone_file(reference, io))
        .transpose()?;
    Ok(ModelContextSelection {
        conversation_revision: value.conversation_revision,
        message_ids: ids,
        checkpoint,
    })
}

pub(in crate::executor) fn stock_compaction_copy(
    value: &crate::context::CompactionPolicy,
    io: Option<&PreparationIo>,
) -> Result<crate::context::CompactionPolicy> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io
        && let Some(work) = io.original_native_work()
    {
        return Ok(match value {
            crate::context::CompactionPolicy::Disabled => {
                crate::context::CompactionPolicy::Disabled
            }
            crate::context::CompactionPolicy::Threshold(policy) => {
                let (retention, custody) =
                    crate::context::clone_retention_native(&policy.retention, work)?;
                io.absorb_native_custody(custody)?;
                crate::context::CompactionPolicy::Threshold(crate::context::ThresholdCompaction {
                    response_reserve_tokens: policy.response_reserve_tokens,
                    recent_tokens: policy.recent_tokens,
                    retention,
                })
            }
        });
    }
    Ok(value.clone())
}

pub(crate) fn stock_vec_push<T>(
    values: &mut Vec<T>,
    value: T,
    io: Option<&PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let size = std::mem::size_of::<T>();
        if values.len() == values.capacity() && size != 0 {
            let before = values.capacity();
            io.preflight(acyclic_fs::WorkCounters {
                allocation_operations: 1,
                ..Default::default()
            })?;
            io.retain_owned(size as u64)?;
            io.allocation_attempt(size as u64)?;
            values.try_reserve_exact(1).map_err(Error::Allocation)?;
            let extra = (values.capacity() - before - 1)
                .checked_mul(size)
                .ok_or_else(|| {
                    stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Storage,
                        "stock vector capacity overflow",
                    )
                })?;
            io.retain_owned(extra as u64)?;
        }
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = io;
    values.push(value);
    Ok(())
}

pub(crate) fn stock_string_push(
    text: &mut String,
    suffix: &str,
    io: Option<&PreparationIo>,
) -> Result<()> {
    with_history_io(io, |work| {
        if let Some(work) = work.as_deref_mut() {
            let target = text.len().checked_add(suffix.len()).ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "stock text size overflow",
                )
            })?;
            work.admit_copy(suffix.len() as u64)?;
            work.reserve_text(text, target)?;
            text.push_str(suffix);
            work.copy_bytes(suffix.len() as u64)
        } else {
            text.push_str(suffix);
            Ok(())
        }
    })
}
