use super::*;

/// The ordinary and measured paths use the same validator and shared cache.
pub(crate) fn validate_json_schema_value(
    schema: &serde_json::Value,
    value: &serde_json::Value,
    label: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let compiled = compile_json_schema_engine(schema, label, io)?;
    #[cfg(feature = "filesystem")]
    let _value_scope = json_schema_scope(value, io)?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        preparation_step(io)?;
    }
    match compiled.validator.validate(value) {
        Ok(()) => Ok(()),
        Err(source) => {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(JsonSchemaDataScope::Native(custody)) = _value_scope {
                return Err(Error::NativeSchema {
                    code: ErrorCode::Invalid,
                    source: source.to_owned(),
                    custody: Some(custody),
                });
            }
            Err(json_schema_error(format_args!("{label} failed validation: {source}"), io))
        }
    }
}

#[cfg(feature = "filesystem")]
fn preparation_step(io: &crate::filesystem::PreparationIo) -> Result<()> {
    let work = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
    io.preflight(work)?;
    io.charge(work)
}

#[cfg(feature = "filesystem")]
enum JsonSchemaDataScope<'a> {
    Observed { _reservation: crate::filesystem::DecodeReservation<'a> },
    #[cfg(not(target_arch = "wasm32"))]
    Native(acyclic_fs::NativeAllocation),
}

#[cfg(feature = "filesystem")]
fn json_schema_scope<'a>(
    value: &serde_json::Value,
    io: Option<&'a crate::filesystem::PreparationIo>,
) -> Result<Option<JsonSchemaDataScope<'a>>> {
    let Some(io) = io else { return Ok(None); };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = io.original_native_work() {
        let shape = json_owned_value_shape_bounded(
            value,
            JsonBounds { maximum_bytes: u64::MAX, maximum_items: u64::MAX, maximum_depth: 256 },
            JsonValueSchema::None,
            &mut |charge| io.codec_charge(charge, false),
        )?;
        let owned = json_value_decode_owned_bytes(shape)?;
        return work.claim(owned, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .map(|custody| Some(JsonSchemaDataScope::Native(custody)))
            .map_err(|cause| Error::NativeWork { cause, custody: None });
    }
    let before = io.receipt().items_examined;
    let _bytes;
    let bytes = io.encode(value, io.remaining()?.bytes_encoded, false)?;
    _bytes = io.guard_owned(bytes.len() as u64)?;
    let items = io.receipt().items_examined.checked_sub(before)
        .ok_or_else(|| Error::Invalid("schema data witness counter underflow".into()))?;
    Ok(Some(json_schema_data_scope(io, bytes.len() as u64, items)?))
}

#[cfg(feature = "filesystem")]
fn json_schema_data_scope(
    io: &crate::filesystem::PreparationIo, wire_bytes: u64, items: u64,
) -> Result<JsonSchemaDataScope<'_>> {
    let owned = json_schema_data_owned(wire_bytes, items)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = io.original_native_work() {
        return work.claim(owned, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .map(JsonSchemaDataScope::Native)
            .map_err(|cause| Error::NativeWork { cause, custody: None });
    }
    io.reserve_decode(owned).map(|_reservation| JsonSchemaDataScope::Observed { _reservation })
}

fn json_schema_data_owned(wire_bytes: u64, items: u64) -> Result<u64> {
    let slots = std::mem::size_of::<serde_json::Value>()
        .max(std::mem::size_of::<(String, serde_json::Value)>()) as u64;
    let owned = items.checked_mul(slots).and_then(|size| size.checked_mul(8))
        .and_then(|size| wire_bytes.checked_mul(4)
            .and_then(|strings| size.checked_add(strings)))
        .ok_or_else(|| Error::Invalid("schema data scope overflow".into()))?;
    Ok(owned)
}

fn json_schema_error(
    message: std::fmt::Arguments<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Error {
    match preparation_format(message, io) {
        Ok(message) => {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
                let capacity = message.capacity() as u64;
                return match io.take_native_custody(capacity) {
                    Ok(custody) => Error::NativeDiagnostic {
                        code: crate::wire::ErrorCode::Invalid, message, custody,
                    },
                    Err(error) => {
                        drop(message);
                        let _ = io.release(capacity);
                        error
                    },
                };
            }
            Error::Invalid(message)
        },
        Err(error) => error,
    }
}

#[cfg(not(feature = "filesystem"))]
pub(crate) fn preparation_format(
    message: std::fmt::Arguments<'_>,
    _io: Option<&crate::executor::PreparationIo>,
) -> Result<String> {
    Ok(message.to_string())
}

#[cfg(feature = "filesystem")]
pub(crate) fn preparation_format(
    message: std::fmt::Arguments<'_>,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<String> {
    let Some(io) = io else { return Ok(message.to_string()); };
    struct Diagnostic<'a> {
        io: &'a crate::filesystem::PreparationIo,
        text: String,
        held: u64,
        error: Option<Error>,
    }
    impl std::fmt::Write for Diagnostic<'_> {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let result = (|| -> Result<()> {
                let length = self.text.len().checked_add(text.len())
                    .ok_or_else(|| Error::Invalid("schema diagnostic size overflow".into()))?;
                let work = acyclic_fs::WorkCounters {
                    bytes_copied: text.len() as u64, items_examined: 1,
                    ..Default::default()
                };
                self.io.preflight(work)?;
                if length > self.text.capacity() {
                    let growth = length.checked_sub(self.text.capacity())
                        .ok_or_else(|| Error::Invalid("schema diagnostic capacity underflow".into()))?;
                    self.io.retain_owned(growth as u64)?;
                    self.held += growth as u64;
                    self.io.allocation_attempt(growth as u64)?;
                    self.text.try_reserve_exact(length - self.text.len())
                        .map_err(|cause| {
                            #[cfg(not(target_arch = "wasm32"))]
                            if self.io.original_native_work().is_some() {
                                return Error::NativeWork {
                                    cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause),
                                    custody: None,
                                };
                            }
                            Error::Allocation(cause)
                        })?;
                    let extra = self.text.capacity().saturating_sub(length) as u64;
                    self.io.retain_owned(extra)?;
                    self.held += extra;
                }
                self.text.push_str(text);
                self.io.charge(work)
            })();
            if let Err(error) = result { self.error = Some(error); return Err(std::fmt::Error); }
            Ok(())
        }
    }
    let mut diagnostic = Diagnostic { io, text: String::new(), held: 0, error: None };
    if std::fmt::write(&mut diagnostic, message).is_err() {
        let Diagnostic { text, held, error, .. } = diagnostic;
        drop(text);
        let _ = io.release(held);
        return Err(error.unwrap_or_else(|| Error::Invalid("schema diagnostic formatting failed".into())));
    }
    Ok(diagnostic.text)
}

struct CompiledJsonSchema<'a> {
    validator: std::sync::Arc<jsonschema::Validator>,
    #[cfg(feature = "filesystem")]
    _scope: Option<JsonSchemaDataScope<'a>>,
    #[cfg(not(feature = "filesystem"))]
    _scope: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn compile_json_schema(
    schema: &serde_json::Value,
    label: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<std::sync::Arc<jsonschema::Validator>> {
    Ok(compile_json_schema_engine(schema, label, io)?.validator)
}

fn compile_json_schema_engine<'a>(
    schema: &serde_json::Value,
    label: &str,
    io: Option<&'a crate::executor::PreparationIo>,
) -> Result<CompiledJsonSchema<'a>> {
    #[cfg(feature = "filesystem")]
    let before = io.map(|io| io.receipt().items_examined).unwrap_or(0);
    #[cfg(feature = "filesystem")]
    let _encoded;
    let encoded: acyclic_stream::OwnedBytes = match io {
        #[cfg(feature = "filesystem")]
        Some(io) => io.encode(schema, io.remaining()?.bytes_encoded, false)?,
        _ => serde_json::to_vec(schema).map_err(|error| Error::Invalid(error.to_string()))?.into(),
    };
    #[cfg(feature = "filesystem")]
    {
        _encoded = io.map(|io| io.guard_owned(encoded.len() as u64)).transpose()?;
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let work = acyclic_fs::WorkCounters { bytes_hashed: encoded.len() as u64, ..Default::default() };
        io.preflight(work)?;
        io.charge(work)?;
    }
    #[cfg(feature = "filesystem")]
    let scope = io.map(|io| {
        let items = io.receipt().items_examined.checked_sub(before)
            .ok_or_else(|| Error::Invalid("schema data witness counter underflow".into()))?;
        json_schema_data_scope(io, encoded.len() as u64, items)
    }).transpose()?;
    #[cfg(not(feature = "filesystem"))]
    let scope = std::marker::PhantomData;
    let key = *blake3::hash(&encoded).as_bytes();
    let validator = compile_json_schema_cached(schema, label, key,
        &mut JsonSchemaObserver::Preparation(io))?;
    Ok(CompiledJsonSchema { validator, _scope: scope })
}

fn compile_json_schema_cached(
    schema: &serde_json::Value,
    label: &str,
    key: [u8; 32],
    observer: &mut JsonSchemaObserver<'_>,
) -> Result<std::sync::Arc<jsonschema::Validator>> {
    use std::sync::{Arc, Mutex, PoisonError};
    type Cache = std::collections::BTreeMap<[u8; 32], Arc<jsonschema::Validator>>;
    static CACHE: Mutex<Cache> = Mutex::new(Cache::new());

    let cached = CACHE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&key)
        .cloned();
    if let Some(validator) = cached {
        return Ok(validator);
    }
    observer.step()?;
    let validator = Arc::new(
        jsonschema::validator_for(schema)
            .map_err(|source| observer.validation_error(source, label))?,
    );
    let mut cache = CACHE.lock().unwrap_or_else(PoisonError::into_inner);
    if cache.len() >= 256 {
        cache.clear();
    }
    cache.insert(key, Arc::clone(&validator));
    Ok(validator)
}

enum JsonSchemaObserver<'a> {
    Preparation(Option<&'a crate::executor::PreparationIo>),
    History(&'a mut dyn crate::store::history::HistoryIo),
}

impl JsonSchemaObserver<'_> {
    fn step(&mut self) -> Result<()> {
        match self {
            Self::Preparation(io) => {
                #[cfg(feature = "filesystem")]
                if let Some(io) = io { preparation_step(io)?; }
                Ok(())
            }
            Self::History(work) => work.codec_charge(JsonCodecCharge::Work { bytes: 0, items: 1 }),
        }
    }
    fn validation_error(
        &mut self,
        source: jsonschema::ValidationError<'static>,
        label: &str,
    ) -> Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Self::Preparation(Some(io)) = self {
            if io.original_native_work().is_some() {
                return Error::NativeSchema {
                    code: ErrorCode::Invalid, source, custody: None,
                };
            }
        }
        self.error(format_args!("invalid {label} schema: {source}"))
    }

    fn error(&mut self, message: std::fmt::Arguments<'_>) -> Error {
        match self {
            Self::Preparation(io) => json_schema_error(message, *io),
            Self::History(work) => match history_schema_format(message, *work) {
                Ok(message) => Error::Invalid(message),
                Err(error) => error,
            },
        }
    }
}

fn history_schema_format(
    message: std::fmt::Arguments<'_>,
    work: &mut dyn crate::store::history::HistoryIo,
) -> Result<String> {
    struct Diagnostic<'a> {
        text: String,
        work: &'a mut dyn crate::store::history::HistoryIo,
        error: Option<Error>,
    }
    impl std::fmt::Write for Diagnostic<'_> {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let result = (|| -> Result<()> {
                let length = self.text.len().checked_add(text.len())
                    .ok_or_else(|| Error::Invalid("schema diagnostic size overflow".into()))?;
                self.work.admit_copy(text.len() as u64)?;
                self.work.reserve_text(&mut self.text, length)?;
                self.text.push_str(text);
                self.work.copy_bytes(text.len() as u64)
            })();
            if let Err(error) = result { self.error = Some(error); return Err(std::fmt::Error); }
            Ok(())
        }
    }
    let mut diagnostic = Diagnostic { text: String::new(), work, error: None };
    if std::fmt::write(&mut diagnostic, message).is_err() {
        let Diagnostic { text, work, error } = diagnostic;
        let capacity = text.capacity() as u64;
        drop(text);
        let _ = work.release_owned(capacity);
        return Err(error.unwrap_or_else(|| Error::Invalid("schema diagnostic formatting failed".into())));
    }
    Ok(diagnostic.text)
}

/// History uses its original native ledger when present, not reconstructed remaining counters.
pub(crate) fn compile_json_schema_with_work(
    schema: &serde_json::Value,
    label: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<std::sync::Arc<jsonschema::Validator>> {
    if work.is_none() { return compile_json_schema(schema, label, None); }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.as_deref().and_then(|work| work.original_native_work()) {
        let io = crate::filesystem::PreparationIo::from_native_work(native)?;
        return compile_json_schema(schema, label, Some(&io));
    }
    let mut scope = crate::wire_codec::CodecQuota::new(work, 0)?;
    let key = history_schema_scope(schema, &mut scope, true)?;
    compile_json_schema_cached(schema, label, key,
        &mut JsonSchemaObserver::History(scope.work().as_deref_mut().expect("observed schema")))
}

/// Keep both schema and input logical allowances live through validation and diagnostic construction, including when the shared validator is cached.
pub(crate) fn validate_json_schema_value_with_work(
    schema: &serde_json::Value,
    value: &serde_json::Value,
    label: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if work.is_none() { return validate_json_schema_value(schema, value, label, None); }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.as_deref().and_then(|work| work.original_native_work()) {
        let io = crate::filesystem::PreparationIo::from_native_work(native)?;
        return validate_json_schema_value(schema, value, label, Some(&io));
    }
    let mut scope = crate::wire_codec::CodecQuota::new(work, 0)?;
    let key = history_schema_scope(schema, &mut scope, true)?;
    let validator = compile_json_schema_cached(schema, label, key,
        &mut JsonSchemaObserver::History(scope.work().as_deref_mut().expect("observed schema")))?;
    history_schema_scope(value, &mut scope, false)?;
    let mut observer = JsonSchemaObserver::History(scope.work().as_deref_mut().expect("observed schema"));
    observer.step()?;
    validator.validate(value).map_err(|error| {
        observer.error(format_args!("{label} failed validation: {error}"))
    })
}

fn history_schema_scope(
    value: &serde_json::Value,
    scope: &mut crate::wire_codec::CodecQuota<'_, '_>,
    hash: bool,
) -> Result<[u8; 32]> {
    let bytes = crate::store::history::canonical_bytes_with_work(
        value, acyclic_stream::MAX_RECORD_BYTES as u64, scope.work())?;
    scope.adopt(bytes.capacity() as u64)?;
    let items = (bytes.len() as u64).checked_add(1)
        .ok_or_else(|| Error::Invalid("schema data witness overflow".into()))?;
    let owned = json_schema_data_owned(bytes.len() as u64, items)?;
    if let Some(work) = scope.work().as_deref_mut() {
        work.admit_owned(owned)?;
        work.retain_owned(owned)?;
    }
    scope.adopt(owned)?;
    let key = if hash {
        if let Some(work) = scope.work().as_deref_mut() { work.hashed_bytes(bytes.len() as u64)?; }
        *blake3::hash(&bytes).as_bytes()
    } else { [0; 32] };
    let capacity = bytes.capacity() as u64;
    drop(bytes);
    scope.relinquish(capacity)?;
    Ok(key)
}

