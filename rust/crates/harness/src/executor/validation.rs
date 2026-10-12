//! Schema admission and diagnostic formatting on the invocation ledger.
use crate::executor::json::{
    JsonBounds, JsonCodecCharge, JsonValueSchema, json_owned_value_shape_bounded,
};
use crate::store::history::HistoryIo;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::wire::ErrorCode;
use crate::wire_codec::CodecQuota;
use crate::{Error, Result};
use std::sync::Arc;

/// Compiles a schema under the optional original ledger.
pub(crate) fn compile_json_schema(
    schema: &serde_json::Value,
    label: &str,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Arc<jsonschema::Validator>> {
    if work.is_none() {
        use std::sync::{Arc, Mutex, PoisonError};
        type Cache = std::collections::BTreeMap<[u8; 32], Arc<jsonschema::Validator>>;
        static CACHE: Mutex<Cache> = Mutex::new(Cache::new());
        let key = *blake3::hash(
            &serde_json::to_vec(schema).map_err(|error| Error::Invalid(error.to_string()))?,
        )
        .as_bytes();
        let cached = CACHE
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
            .cloned();
        if let Some(validator) = cached {
            return Ok(validator);
        }
        let validator = Arc::new(
            jsonschema::validator_for(schema)
                .map_err(|error| Error::Invalid(format!("invalid {label} schema: {error}")))?,
        );
        let mut cache = CACHE.lock().unwrap_or_else(PoisonError::into_inner);
        if cache.len() >= 256 {
            cache.clear();
        }
        cache.insert(key, Arc::clone(&validator));
        return Ok(validator);
    }
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
    if work.is_none() {
        return compile_json_schema(schema, label, work)?
            .validate(value)
            .map_err(|error| Error::Invalid(format!("{label} failed validation: {error}")));
    }
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

pub(crate) fn preparation_format(
    message: std::fmt::Arguments<'_>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<String> {
    let Some(work) = work.as_deref_mut() else {
        return Ok(message.to_string());
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.original_native_work() {
        let (text, custody) = native.display(&message)?;
        work.absorb_native_custody(custody)?;
        return Ok(text);
    }
    struct Diagnostic<'a> {
        text: String,
        work: &'a mut dyn crate::store::history::HistoryIo,
        error: Option<Error>,
    }
    impl std::fmt::Write for Diagnostic<'_> {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let result = (|| -> Result<()> {
                let length = self
                    .text
                    .len()
                    .checked_add(text.len())
                    .ok_or_else(|| Error::Invalid("schema diagnostic size overflow".into()))?;
                self.work
                    .codec_charge(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                self.work.admit_copy(text.len() as u64)?;
                self.work.reserve_text(&mut self.text, length)?;
                self.text.push_str(text);
                self.work.copy_bytes(text.len() as u64)
            })();
            if let Err(error) = result {
                self.error = Some(error);
                return Err(std::fmt::Error);
            }
            Ok(())
        }
    }
    let mut diagnostic = Diagnostic {
        text: String::new(),
        work,
        error: None,
    };
    if std::fmt::write(&mut diagnostic, message).is_err() {
        let Diagnostic { text, work, error } = diagnostic;
        let capacity = text.capacity() as u64;
        drop(text);
        let _ = work.release_owned(capacity);
        return Err(
            error.unwrap_or_else(|| Error::Invalid("schema diagnostic formatting failed".into()))
        );
    }
    Ok(diagnostic.text)
}
