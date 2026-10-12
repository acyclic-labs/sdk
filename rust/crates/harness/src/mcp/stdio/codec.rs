use super::*;

#[derive(Serialize)]
pub(super) struct Envelope<'a, P> {
    pub(super) id: OperationId,
    pub(super) jsonrpc: &'static str,
    pub(super) method: &'a str,
    pub(super) params: P,
}
#[derive(Serialize)]
pub(super) struct Empty {}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct InitializeParams {
    pub(super) capabilities: Empty,
    pub(super) client_info: ClientInfo,
    pub(super) protocol_version: &'static str,
}
#[derive(Serialize)]
pub(super) struct ClientInfo {
    pub(super) name: &'static str,
    pub(super) version: &'static str,
}
#[derive(Serialize)]
pub(super) struct Initialized {
    pub(super) jsonrpc: &'static str,
    pub(super) method: &'static str,
}

#[cfg(test)]
pub(super) fn line(value: &Value) -> Result<Vec<u8>> {
    Ok(encode_line(value, usize::MAX, None)?.0)
}

pub(super) fn rejection(work: NativeWork<'_>, reason: &'static str) -> Error {
    let _ = work;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason,
        };
    }
    Error::Invalid(reason.into())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork {
        cause,
        custody: None,
    }
}

pub(super) fn native_custody(work: NativeWork<'_>) -> Result<NativeCustody> {
    let _ = work;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        return work
            .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .map(Some)
            .map_err(native_error);
    }
    Ok(None)
}

pub(super) fn checkpoint(work: NativeWork<'_>) -> Result<()> {
    let _ = work;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters::default())
            .map_err(native_error)?;
    }
    Ok(())
}

pub(super) fn copy_work(work: NativeWork<'_>, bytes: usize) -> Result<()> {
    let _ = (work, bytes);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            bytes_copied: bytes as u64,
            ..acyclic_fs::WorkCounters::default()
        })
        .map_err(native_error)?;
    }
    Ok(())
}

pub(super) fn scan_work(work: NativeWork<'_>) -> Result<()> {
    let _ = work;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            source_bytes_read: 1,
            items_examined: 1,
            ..acyclic_fs::WorkCounters::default()
        })
        .map_err(native_error)?;
    }
    Ok(())
}

pub(super) fn reserve_write(
    write: &mut (Vec<u8>, NativeCustody),
    capacity: usize,
    work: NativeWork<'_>,
) -> Result<()> {
    if write.0.capacity() >= capacity {
        return checkpoint(work);
    }
    let old = write.0.capacity();
    let _ = old;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(guard) = write.1.as_mut() {
        // Reallocation temporarily retains BOTH old and replacement buffers.
        guard.grow(capacity as u64).map_err(native_error)?;
    }
    write
        .0
        .try_reserve_exact(capacity - write.0.len())
        .map_err(|_| rejection(work, "MCP stdio write allocation failed"))?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(guard) = write.1.as_mut() {
        guard.release(old as u64).map_err(native_error)?;
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
        Charge::Release { bytes } => custody.release(bytes).map_err(native_error),
        Charge::Retain { bytes } => custody
            .resize_retained(
                custody
                    .bytes()
                    .checked_add(bytes)
                    .ok_or_else(|| rejection(Some(work), "MCP codec custody overflows"))?,
            )
            .map_err(native_error),
        Charge::Admission {
            maximum_owned_bytes,
            ..
        } => quota
            .resize_retained(maximum_owned_bytes)
            .map_err(native_error),
        Charge::CopyAdmission { bytes } => work
            .admit(acyclic_fs::WorkCounters {
                bytes_copied: bytes,
                ..acyclic_fs::WorkCounters::default()
            })
            .map_err(native_error),
        Charge::AllocationOperation { .. } => work
            .charge(acyclic_fs::WorkCounters {
                allocation_operations: 1,
                ..acyclic_fs::WorkCounters::default()
            })
            .map_err(native_error),
        Charge::Copy { bytes } => work
            .charge(acyclic_fs::WorkCounters {
                bytes_copied: bytes,
                ..acyclic_fs::WorkCounters::default()
            })
            .map_err(native_error),
        Charge::Read { bytes } => work
            .charge(acyclic_fs::WorkCounters {
                source_bytes_read: bytes,
                ..acyclic_fs::WorkCounters::default()
            })
            .map_err(native_error),
        Charge::Inspect { items } => work
            .charge(acyclic_fs::WorkCounters {
                items_examined: items,
                ..Default::default()
            })
            .map_err(native_error),
        Charge::Work { bytes, items } => work
            .charge(acyclic_fs::WorkCounters {
                bytes_encoded: bytes,
                items_examined: items,
                ..acyclic_fs::WorkCounters::default()
            })
            .map_err(native_error),
    }
}

pub(super) fn encode_line<T: Serialize>(
    value: &T,
    maximum: usize,
    work: NativeWork<'_>,
) -> Result<(Vec<u8>, NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let mut diagnostic = work
            .claim(
                crate::contract::sdk_json_diagnostic_bytes()
                    .max(crate::contract::lexical_json_diagnostic_bytes()),
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )
            .map_err(native_error)?;
        let mut custody = work
            .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(native_error)?;
        let mut quota = work
            .claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(native_error)?;
        let encoded = crate::json_bytes_bounded(
            value,
            bounds(maximum.saturating_sub(1)),
            None,
            &mut |event| codec_charge(work, &mut custody, &mut quota, event),
        );
        let bytes = match encoded {
            Ok(bytes) => bytes,
            Err(Error::Invalid(message)) => {
                diagnostic
                    .substitute(message.capacity() as u64)
                    .map_err(native_error)?;
                return Err(Error::NativeDiagnostic {
                    code: crate::wire::ErrorCode::Invalid,
                    message,
                    custody: diagnostic,
                });
            }
            Err(error) => return Err(error),
        };
        drop(quota);
        drop(diagnostic);
        let mut write = (bytes, Some(custody));
        let capacity = write
            .0
            .len()
            .checked_add(1)
            .ok_or_else(|| rejection(Some(work), "MCP line length overflows"))?;
        reserve_write(&mut write, capacity, Some(work))?;
        copy_work(Some(work), 1)?;
        write.0.push(b'\n');
        return Ok(write);
    }
    let mut bytes = serde_json::to_vec(value).map_err(|error| Error::Invalid(error.to_string()))?;
    bytes.push(b'\n');
    if bytes.len() > maximum {
        return Err(rejection(work, "MCP stdio writes exceed admitted bound"));
    }
    Ok((bytes, None))
}

pub(super) fn frame_admission(
    body: &[u8],
    maximum: usize,
    work: NativeWork<'_>,
) -> Result<NativeCustody> {
    let _ = (body, maximum);
    #[allow(unused_mut)] // Native-only quota admission below.
    let mut custody = native_custody(work)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let per_token =
            (8 * std::mem::size_of::<Value>() + std::mem::size_of::<(String, Value)>() + 4) as u64;
        let quota = (body.len() as u64)
            .max(8)
            .checked_mul(per_token)
            .and_then(|n| n.checked_add(crate::contract::sdk_json_diagnostic_bytes()))
            .ok_or_else(|| rejection(Some(work), "MCP frame quota overflows"))?;
        if let Some(guard) = custody.as_mut() {
            guard.resize_retained(quota).map_err(native_error)?;
        }
        // Per-byte original cancellation and item admission before any serde.
        let mut diagnostic = work
            .claim(
                crate::contract::lexical_json_diagnostic_bytes(),
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )
            .map_err(native_error)?;
        let scanned =
            crate::contract::admit_json(body, bounds(maximum), &mut |event| match event {
                crate::JsonCodecCharge::Read { bytes } => work
                    .charge(acyclic_fs::WorkCounters {
                        source_bytes_read: bytes,
                        ..acyclic_fs::WorkCounters::default()
                    })
                    .map_err(native_error),
                crate::JsonCodecCharge::Work { items, .. }
                | crate::JsonCodecCharge::Inspect { items } => work
                    .charge(acyclic_fs::WorkCounters {
                        items_examined: items,
                        ..acyclic_fs::WorkCounters::default()
                    })
                    .map_err(native_error),
                _ => checkpoint(Some(work)),
            });
        match scanned {
            Err(Error::Invalid(message)) => {
                diagnostic
                    .substitute(message.capacity() as u64)
                    .map_err(native_error)?;
                return Err(Error::NativeDiagnostic {
                    code: crate::wire::ErrorCode::Invalid,
                    message,
                    custody: diagnostic,
                });
            }
            Err(error) => return Err(error),
            Ok(()) => {}
        }
    }
    Ok(custody)
}

pub(super) fn serde_error(
    source: serde_json::Error,
    work: NativeWork<'_>,
    custody: &mut NativeCustody,
) -> Error {
    let _ = (&work, &custody);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeDecode {
            code: crate::wire::ErrorCode::Invalid,
            source,
            custody: custody.take(),
        };
    }
    Error::Invalid(format!("invalid MCP stdio JSON: {source}"))
}

pub(super) fn decode_value(
    body: &[u8],
    work: NativeWork<'_>,
    custody: &mut NativeCustody,
) -> Result<Value> {
    checkpoint(work)?;
    let mut decoder = serde_json::Deserializer::from_slice(body);
    decoder.disable_recursion_limit();
    let value =
        Value::deserialize(&mut decoder).map_err(|source| serde_error(source, work, custody))?;
    decoder
        .end()
        .map_err(|source| serde_error(source, work, custody))?;
    drop(decoder);
    Ok(value)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn value_storage(value: &Value, work: NativeWork<'_>) -> Result<u64> {
    checkpoint(work)?;
    let add = |a: u64, b: u64| {
        a.checked_add(b)
            .ok_or_else(|| rejection(work, "MCP result storage overflows"))
    };
    match value {
        Value::String(text) => Ok(text.capacity() as u64),
        Value::Array(values) => {
            let mut bytes = (values.capacity() * std::mem::size_of::<Value>()) as u64;
            for value in values {
                scan_work(work)?;
                bytes = add(bytes, value_storage(value, work)?)?;
            }
            Ok(bytes)
        }
        Value::Object(values) => {
            let mut bytes = (values.len() * std::mem::size_of::<(String, Value)>()) as u64;
            for (key, value) in values {
                scan_work(work)?;
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
