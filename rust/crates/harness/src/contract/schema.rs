use super::*;
use crate::store::history::HistoryIo;
use crate::wire_codec::CodecQuota;
use std::sync::Arc;

/// Compiles a schema under the optional original ledger.
pub(crate) fn compile_json_schema(
    schema: &serde_json::Value,
    label: &str,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Arc<jsonschema::Validator>> {
    let mut scope = CodecQuota::new(work, 0)?;
    admit_schema_value(schema, &mut scope)?;
    compile_validator(schema, label, &mut scope)
}

/// Validates a value while retaining both schema and input allowances.
pub(crate) fn validate_json_schema_value(
    schema: &serde_json::Value,
    value: &serde_json::Value,
    label: &str,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let mut scope = CodecQuota::new(work, 0)?;
    admit_schema_value(schema, &mut scope)?;
    let validator = compile_validator(schema, label, &mut scope)?;
    admit_schema_value(value, &mut scope)?;
    schema_step(scope.work())?;
    match validator.validate(value) {
        Ok(()) => Ok(()),
        Err(source) => Err(schema_error(
            source.to_owned(),
            format_args!("{label} failed validation"),
            &mut scope,
        )),
    }
}

fn compile_validator(
    schema: &serde_json::Value,
    label: &str,
    scope: &mut CodecQuota<'_, '_>,
) -> Result<Arc<jsonschema::Validator>> {
    schema_step(scope.work())?;
    jsonschema::validator_for(schema)
        .map(Arc::new)
        .map_err(|source| schema_error(source, format_args!("invalid {label} schema"), scope))
}

fn schema_step(work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
    if let Some(work) = work.as_deref_mut() {
        work.codec_charge(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    }
    Ok(())
}

fn admit_schema_value(value: &serde_json::Value, scope: &mut CodecQuota<'_, '_>) -> Result<()> {
    if scope.work().is_none() {
        return Ok(());
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if scope
        .work()
        .as_deref()
        .and_then(HistoryIo::original_native_work)
        .is_some()
    {
        let shape = json_owned_value_shape_bounded(
            value,
            JsonBounds {
                maximum_bytes: u64::MAX,
                maximum_items: u64::MAX,
                maximum_depth: 256,
            },
            JsonValueSchema::None,
            &mut |charge| {
                scope
                    .work()
                    .as_deref_mut()
                    .expect("schema ledger")
                    .codec_charge(charge)
            },
        )?;
        let owned = json_value_decode_owned_bytes(shape)?;
        let observer = scope.work().as_deref_mut().expect("schema ledger");
        observer.admit_owned(owned)?;
        observer.retain_owned(owned)?;
        return scope.adopt(owned);
    }
    let bytes = crate::store::history::canonical_bytes_with_work(
        value,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        scope.work(),
    )?;
    scope.adopt(bytes.capacity() as u64)?;
    let items = (bytes.len() as u64)
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("schema data witness overflow".into()))?;
    let slots = std::mem::size_of::<serde_json::Value>()
        .max(std::mem::size_of::<(String, serde_json::Value)>()) as u64;
    let owned = items
        .checked_mul(slots)
        .and_then(|size| size.checked_mul(8))
        .and_then(|size| {
            (bytes.len() as u64)
                .checked_mul(4)
                .and_then(|strings| size.checked_add(strings))
        })
        .ok_or_else(|| Error::Invalid("schema data scope overflow".into()))?;
    let observer = scope.work().as_deref_mut().expect("schema ledger");
    observer.admit_owned(owned)?;
    observer.retain_owned(owned)?;
    scope.adopt(owned)?;
    let capacity = bytes.capacity() as u64;
    drop(bytes);
    scope.relinquish(capacity)
}

fn schema_error(
    source: jsonschema::ValidationError<'static>,
    context: std::fmt::Arguments<'_>,
    scope: &mut CodecQuota<'_, '_>,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if scope
        .work()
        .as_deref()
        .and_then(HistoryIo::original_native_work)
        .is_some()
    {
        let retained = scope.retained();
        let custody = scope
            .work()
            .as_deref_mut()
            .expect("schema ledger")
            .take_native_custody(retained)
            .expect("admitted schema custody");
        scope.transfer();
        return Error::NativeSchema {
            code: ErrorCode::Invalid,
            source,
            custody: Some(custody),
        };
    }
    preparation_format(format_args!("{context}: {source}"), scope.work())
        .map_or_else(|error| error, Error::Invalid)
}
