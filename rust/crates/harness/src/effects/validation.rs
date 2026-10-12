use super::*;

fn decode_schema(
    reference: &FileRef,
    bytes: &[u8],
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Value> {
    crate::executor::with_history_io(io, |work| reference.validate(work))?;
    if reference.descriptor().media_type() != "application/schema+json" {
        return Err(crate::executor::stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "effect result schema must be JSON Schema content",
        ));
    }
    crate::executor::with_history_io(io, |work| reference.descriptor().verify(bytes, work))?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        let schema = io.decode(
            std::marker::PhantomData::<Value>,
            bytes,
            crate::wire::ErrorCode::Storage,
        )?;
        drop(crate::executor::with_history_io(Some(io), |work| crate::contract::compile_json_schema(&schema.0, "effect result", work))?);
        return Ok(crate::filesystem::retain_decoded(schema));
    }
    let schema: Value = crate::contract::json_from_slice(bytes)
        .map_err(|error| Error::Invalid(format!("effect result schema is not JSON: {error}")))?;
    crate::contract::compile_json_schema(&schema, "effect result", &mut None)?;
    Ok(schema)
}

pub(crate) fn validate_schema_bytes(
    reference: &FileRef,
    bytes: &[u8],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if work.is_none() {
        return decode_schema(reference, bytes, None).map(drop);
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        let original = work
            .as_deref()
            .and_then(|io| io.original_native_work())
            .ok_or_else(|| {
                Error::Unsupported("effect schema observer has no original native ledger".into())
            })?;
        let decode = crate::filesystem::PreparationIo::from_native_work(original)?;
        drop(decode_schema(reference, bytes, Some(&decode))?);
        return Ok(());
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    Err(Error::Unsupported(
        "native effect schema validation is unavailable".into(),
    ))
}

pub(crate) fn validate_result_bytes(
    schema_reference: &FileRef,
    schema_bytes: &[u8],
    result: &FileRef,
    bytes: &[u8],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if work.is_none() {
        let schema = decode_schema(schema_reference, schema_bytes, None)?;
        result.validate(work)?;
        if result.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("effect result must be JSON content".into()));
        }
        result.descriptor().verify(bytes, work)?;
        let value: Value = crate::contract::json_from_slice(bytes)
            .map_err(|error| Error::Invalid(format!("effect result is not JSON: {error}")))?;
        return crate::contract::validate_json_schema_value(&schema, &value, "effect result", &mut None);
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        result.validate(work)?;
        let original = work
            .as_deref()
            .and_then(|io| io.original_native_work())
            .ok_or_else(|| {
                Error::Unsupported("effect result observer has no original native ledger".into())
            })?;
        let decode = crate::filesystem::PreparationIo::from_native_work(original)?;
        let schema = decode_schema(schema_reference, schema_bytes, Some(&decode))?;
        if result.descriptor().media_type() != "application/json" {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "effect result must be JSON content",
            });
        }
        result.descriptor().verify(bytes, work)?;
        let value = decode.decode(
            std::marker::PhantomData::<Value>,
            bytes,
            crate::wire::ErrorCode::Storage,
        )?;
        return crate::executor::with_history_io(Some(&decode), |work| crate::contract::validate_json_schema_value(&schema, &value.0, "effect result", work));
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    Err(Error::Unsupported(
        "native effect result validation is unavailable".into(),
    ))
}
