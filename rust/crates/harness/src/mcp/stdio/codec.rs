use super::*;

#[derive(Serialize)]
pub(super) struct Envelope<'a, P> {
    pub(super) id: OperationId,
    pub(super) jsonrpc: &'static str,
    pub(super) method: &'a str,
    pub(super) params: P,
}
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
        guard.release(old as u64).map_err(Error::from)?;
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn bounds(maximum: usize) -> crate::JsonBounds {
    crate::JsonBounds {
        maximum_bytes: maximum as u64,
        maximum_items: maximum as u64,
        maximum_depth: 256,
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn codec_charge(
    work: &acyclic_fs::NativeMaterializationWork,
    custody: &mut acyclic_fs::NativeAllocation,
    quota: &mut acyclic_fs::NativeAllocation,
    event: crate::JsonCodecCharge,
) -> Result<()> {
    use crate::JsonCodecCharge as Charge;
    match event {
        Charge::Release { bytes } => custody.release(bytes).map_err(Error::from),
        Charge::Retain { bytes } => custody
            .resize(custody.bytes().checked_add(bytes).ok_or_else(|| {
                error(
                    Some(work),
                    ErrorCode::Invalid,
                    "MCP codec custody overflows",
                )
            })?)
            .map_err(Error::from),
        Charge::Admission {
            maximum_owned_bytes,
            ..
        } => quota.resize(maximum_owned_bytes).map_err(Error::from),
        Charge::CopyAdmission { bytes } => work
            .admit(acyclic_fs::WorkCounters {
                bytes_copied: bytes,
                ..acyclic_fs::WorkCounters::default()
            })
            .map_err(Error::from),
        Charge::AllocationOperation { .. } => work
            .charge(acyclic_fs::WorkCounters {
                allocation_operations: 1,
                ..acyclic_fs::WorkCounters::default()
            })
            .map_err(Error::from),
        Charge::Copy { bytes } => work
            .charge(acyclic_fs::WorkCounters {
                bytes_copied: bytes,
                ..acyclic_fs::WorkCounters::default()
            })
            .map_err(Error::from),
        Charge::Read { bytes } => work
            .charge(acyclic_fs::WorkCounters {
                source_bytes_read: bytes,
                ..acyclic_fs::WorkCounters::default()
            })
            .map_err(Error::from),
        Charge::Inspect { items } => work
            .charge(acyclic_fs::WorkCounters {
                items_examined: items,
                ..Default::default()
            })
            .map_err(Error::from),
        Charge::Work { bytes, items } => work
            .charge(acyclic_fs::WorkCounters {
                bytes_encoded: bytes,
                items_examined: items,
                ..acyclic_fs::WorkCounters::default()
            })
            .map_err(Error::from),
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn diagnostic(error: Error, mut custody: acyclic_fs::NativeAllocation) -> Error {
    let Error::Invalid(message) = error else {
        return error;
    };
    let release = custody
        .bytes()
        .checked_sub(message.capacity() as u64)
        .ok_or(acyclic_fs::NativeTreeMaterializationError::Work(
            acyclic_fs::WorkError::Overflow,
        ))
        .and_then(|bytes| custody.release(bytes));
    if let Err(cause) = release {
        return Error::from(cause);
    }
    Error::NativeDiagnostic {
        code: crate::wire::ErrorCode::Invalid,
        message,
        custody,
    }
}

pub(super) fn encode_line<T: Serialize + ?Sized>(
    value: &T,
    maximum: usize,
    work: NativeWork<'_>,
) -> Result<(Vec<u8>, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let diagnostic_custody = work
            .claim(
                crate::executor::json::sdk_json_diagnostic_bytes()
                    .max(crate::executor::json::lexical_json_diagnostic_bytes()),
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )
            .map_err(Error::from)?;
        let mut custody = work
            .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(Error::from)?;
        let mut quota = work
            .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(Error::from)?;
        let encoded = crate::json_bytes_bounded(
            value,
            bounds(maximum.saturating_sub(1)),
            None,
            &mut |event| codec_charge(work, &mut custody, &mut quota, event),
        );
        let bytes = encoded.map_err(|error| diagnostic(error, diagnostic_custody))?;
        drop(quota);
        let mut write = (bytes, Some(custody));
        let capacity =
            write.0.len().checked_add(1).ok_or_else(|| {
                error(Some(work), ErrorCode::Invalid, "MCP line length overflows")
            })?;
        reserve_write(&mut write, capacity, Some(work))?;
        charge(Some(work), 0, 0, 1)?;
        write.0.push(b'\n');
        return Ok(write);
    }
    let mut bytes = serde_json::to_vec(value).map_err(|error| Error::Invalid(error.to_string()))?;
    bytes.push(b'\n');
    if bytes.len() > maximum {
        return Err(error(
            work,
            ErrorCode::Invalid,
            "MCP stdio writes exceed admitted bound",
        ));
    }
    Ok((bytes, None))
}

pub(super) fn frame_admission(
    body: &[u8],
    maximum: usize,
    work: NativeWork<'_>,
) -> Result<NativeCustody> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let per_token =
            (8 * std::mem::size_of::<Value>() + std::mem::size_of::<(String, Value)>() + 4) as u64;
        let quota = (body.len() as u64)
            .max(8)
            .checked_mul(per_token)
            .and_then(|n| n.checked_add(crate::executor::json::sdk_json_diagnostic_bytes()))
            .ok_or_else(|| error(Some(work), ErrorCode::Invalid, "MCP frame quota overflows"))?;
        let custody = work.claim(quota, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        // Per-byte original cancellation and item admission before any serde.
        let diagnostic_custody = work
            .claim(
                crate::executor::json::lexical_json_diagnostic_bytes(),
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )
            .map_err(Error::from)?;
        let scanned =
            crate::executor::json::admit_json(body, bounds(maximum), &mut |event| match event {
                crate::JsonCodecCharge::Read { bytes } => charge(Some(work), 0, bytes, 0),
                crate::JsonCodecCharge::Work { items, .. }
                | crate::JsonCodecCharge::Inspect { items } => charge(Some(work), items, 0, 0),
                _ => charge(Some(work), 0, 0, 0),
            });
        scanned.map_err(|error| diagnostic(error, diagnostic_custody))?;
        return Ok(Some(custody));
    }
    let _ = (body, maximum, work);
    Ok(None)
}

pub(super) fn decode_value(
    body: &[u8],
    work: NativeWork<'_>,
    custody: &mut NativeCustody,
) -> Result<Value> {
    charge(work, 0, 0, 0)?;
    crate::contract::json_from_slice(body).map_err(|source| {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.is_some() {
            return Error::NativeJson {
                code: ErrorCode::Invalid,
                source,
                custody: custody.take(),
            };
        }
        let _ = custody;
        Error::Invalid(format!("invalid MCP stdio JSON: {source}"))
    })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn value_storage(value: &Value, work: NativeWork<'_>) -> Result<u64> {
    charge(work, 0, 0, 0)?;
    let add = |a: u64, b: u64| {
        a.checked_add(b)
            .ok_or_else(|| error(work, ErrorCode::Invalid, "MCP result storage overflows"))
    };
    match value {
        Value::String(text) => Ok(text.capacity() as u64),
        Value::Array(values) => {
            let mut bytes = (values.capacity() * std::mem::size_of::<Value>()) as u64;
            for value in values {
                charge(work, 1, 1, 0)?;
                bytes = add(bytes, value_storage(value, work)?)?;
            }
            Ok(bytes)
        }
        Value::Object(values) => {
            let mut bytes = (values.len() * std::mem::size_of::<(String, Value)>()) as u64;
            for (key, value) in values {
                charge(work, 1, 1, 0)?;
                bytes = add(
                    add(bytes, key.capacity() as u64)?,
                    value_storage(value, work)?,
                )?;
            }
            Ok(bytes)
        }
        _ => Ok(0),
    }
}
