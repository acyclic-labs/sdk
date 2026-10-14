use super::*;

#[cfg(test)]
pub(super) fn line(value: &Value) -> Result<Vec<u8>> {
    Ok(encode_line(value, usize::MAX, None)?.0)
}

pub(super) fn reserve_write(
    write: &mut (Vec<u8>, NativeCustody),
    capacity: usize,
    work: NativeWork<'_>,
) -> Result<()> {
    if write.0.capacity() >= capacity {
        return charge(work, 0, 0, 0);
    }
    let old = write.0.capacity();
    let _ = old;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(guard) = write.1.as_mut() {
        // Reallocation temporarily retains BOTH old and replacement buffers.
        guard.grow(capacity as u64).map_err(Error::from)?;
    }
    write
        .0
        .try_reserve_exact(capacity - write.0.len())
        .map_err(|_| {
            error(
                work,
                ErrorCode::Invalid,
                "MCP stdio write allocation failed",
            )
        })?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(guard) = write.1.as_mut() {
        let retained = guard
            .bytes()
            .checked_sub(capacity as u64)
            .and_then(|bytes| bytes.checked_sub(old as u64))
            .and_then(|bytes| bytes.checked_add(write.0.capacity() as u64))
            .ok_or_else(|| error(work, ErrorCode::Invalid, "MCP write capacity overflows"))?;
        guard.reconcile_capacity(retained).map_err(Error::from)?;
    }
    Ok(())
}

fn bounds(maximum: usize) -> crate::JsonBounds {
    crate::JsonBounds {
        maximum_bytes: maximum as u64,
        maximum_items: maximum as u64,
        maximum_depth: 256,
    }
}

pub(super) fn encode_line<T: Serialize + ?Sized>(
    value: &T,
    maximum: usize,
    work: NativeWork<'_>,
) -> Result<(Vec<u8>, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let owned_io = work
        .map(crate::filesystem::PreparationIo::from_native_work)
        .transpose()?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let diagnostics = owned_io
        .as_ref()
        .map(|io| {
            io.reserve_decode(
                crate::executor::json::sdk_json_diagnostic_bytes()
                    .max(crate::executor::json::lexical_json_diagnostic_bytes()),
            )
        })
        .transpose()?;
    let encoded = crate::json_bytes_bounded(
        value,
        bounds(maximum.saturating_sub(1)),
        work.is_some().then_some(ErrorCode::Invalid),
        &mut |event| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(io) = &owned_io {
                return io.codec_charge(event, true);
            }
            let _ = event;
            Ok(())
        },
    );
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Err(Error::NativeJson {
        code,
        source,
        custody: None,
    }) = encoded
    {
        let (source, custody) = crate::filesystem::native_decoded((
            source,
            diagnostics.ok_or_else(|| {
                error(
                    work,
                    ErrorCode::Storage,
                    "native encoder lacks diagnostics ownership",
                )
            })?,
        ))?;
        return Err(Error::NativeJson {
            code,
            source,
            custody: Some(custody),
        });
    }
    let bytes = encoded?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    drop(diagnostics);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let custody = owned_io
        .as_ref()
        .map(|io| io.take_native_custody(bytes.capacity() as u64))
        .transpose()?;
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let custody = None;
    let mut write = (bytes, custody);
    let capacity = write
        .0
        .len()
        .checked_add(1)
        .ok_or_else(|| error(work, ErrorCode::Invalid, "MCP line length overflows"))?;
    reserve_write(&mut write, capacity, work)?;
    charge(work, 0, 0, 1)?;
    write.0.push(b'\n');
    if write.0.len() > maximum {
        return Err(error(
            work,
            ErrorCode::Invalid,
            "MCP stdio writes exceed admitted bound",
        ));
    }
    Ok(write)
}

pub(super) fn decode_value(body: &[u8], work: NativeWork<'_>) -> Result<(Value, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let value: Value = io.decode_json(body).map_err(|error| match error {
            Error::NativeJson {
                source, custody, ..
            } => Error::NativeJson {
                code: ErrorCode::Invalid,
                source,
                custody,
            },
            error => error,
        })?;
        let retained = crate::executor::json::json_value_logical_owned_bytes(&value)?;
        let custody = io.into_native_custody(retained)?;
        return Ok((value, Some(custody)));
    }
    charge(work, 0, 0, 0)?;
    crate::contract::json_from_slice(body)
        .map(|value| (value, None))
        .map_err(|source| Error::Invalid(format!("invalid MCP stdio JSON: {source}")))
}
