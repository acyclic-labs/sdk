use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn context_contract_native<T: Serialize>(
    value: &T,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(Value, acyclic_fs::NativeAllocation)> {
    use crate::store::history::HistoryIo as _;
    let remaining = work.remaining()?;
    let bounds = crate::contract::JsonBounds {
        maximum_bytes: remaining.bytes_encoded,
        maximum_items: remaining.items_examined,
        maximum_depth: 256,
    };
    let labels: u64 = 597;
    let private_labels = labels
        .checked_add(std::any::type_name::<T>().len() as u64 + 4)
        .ok_or_else(|| crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "context serializer diagnostic witness overflows",
        })?;
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(0, private_labels)?;
    let diagnostic = work.claim(diagnostic_bytes, 0, acyclic_fs::WorkCounters::default())?;
    let mut io = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?);
    let encoded = (|| {
        crate::contract::validate_json_byte_bound_native_trusted(
            value,
            bounds,
            crate::wire::ErrorCode::Invalid,
            &mut |charge| io.codec_charge(charge),
        )?;
        crate::contract::json_value_bounded(
            value,
            bounds,
            Some(crate::wire::ErrorCode::Invalid),
            &mut |charge| io.codec_charge(charge),
        )
    })();
    match encoded {
        Ok(value) => Ok((value, io.into_custody())),
        Err(crate::Error::NativeEncode {
            code,
            source,
            custody: None,
        }) => Err(crate::Error::NativeEncode {
            code,
            source,
            custody: Some(diagnostic),
        }),
        Err(error) => Err(error),
    }
}
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn reserve_context_vec_native<T>(
    values: &mut Vec<T>,
    capacity: usize,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<acyclic_fs::NativeAllocation> {
    let mut guard = work.claim(0, 0, acyclic_fs::WorkCounters::default())?;
    if capacity <= values.capacity() {
        return Ok(guard);
    }
    let old = values.capacity();
    let bytes = capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "context vector capacity overflows",
        })?;
    guard.resize_retained(bytes as u64)?;
    let copied = values
        .len()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "context vector copy overflows",
        })?;
    work.charge(acyclic_fs::WorkCounters {
        allocation_operations: 1,
        bytes_copied: copied as u64,
        ..acyclic_fs::WorkCounters::default()
    })?;
    values
        .try_reserve_exact(capacity - values.len())
        .map_err(|cause| crate::Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause),
            custody: None,
        })?;
    let actual = values
        .capacity()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "context vector capacity overflows",
        })?;
    if let Err(cause) = guard.reconcile_capacity(actual as u64) {
        return Err(crate::Error::NativeWork {
            cause,
            custody: Some(guard),
        });
    }
    let retired =
        old.checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "context vector capacity overflows",
            })?;
    if let Err(cause) = guard.release(retired as u64) {
        return Err(crate::Error::NativeWork {
            cause,
            custody: Some(guard),
        });
    }
    if let Err(cause) = work.remaining() {
        return Err(crate::Error::NativeWork {
            cause,
            custody: Some(guard),
        });
    }
    Ok(guard)
}
pub(super) fn append_context_messages(
    first: &mut Vec<ModelMessage>,
    second: &mut Vec<ModelMessage>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let capacity = first.len().checked_add(second.len()).ok_or_else(|| {
        crate::executor::stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "context vector capacity overflows",
        )
    })?;
    crate::executor::with_history_io(io, |work| {
        compaction::reserve_vector(first, capacity, work)?;
        if let Some(work) = work.as_deref_mut() {
            let copied = second
                .len()
                .checked_mul(std::mem::size_of::<ModelMessage>())
                .ok_or_else(|| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "context vector copy overflows",
                    )
                })?;
            work.admit_copy(copied as u64)?;
            work.copy_bytes(copied as u64)?;
        }
        first.append(second);
        if let Some(work) = work.as_deref_mut() {
            let bytes = second
                .capacity()
                .checked_mul(std::mem::size_of::<ModelMessage>())
                .ok_or_else(|| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "context vector capacity overflows",
                    )
                })?;
            drop(std::mem::take(second));
            work.release_owned(bytes as u64)?;
        }
        Ok(())
    })
}
