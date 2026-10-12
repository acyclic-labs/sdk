use super::*;

/// Sorted-key JSON encoding for durable identities and Rust/WASM output.
pub(crate) fn canonical_json_bytes<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let mut value =
        serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
    value.sort_all_objects();
    serde_json::to_vec(&value).map_err(|error| Error::Invalid(error.to_string()))
}

/// Checks encoded JSON size without admitting owned-tree or serde heap storage.
pub(crate) fn validate_json_byte_bound<T: Serialize + ?Sized>(value: &T, maximum: u64) -> Result<()> {
    json_byte_length(value, maximum).map(|_| ())
}

pub(crate) fn validate_trusted_json_byte_bound<T: Serialize + ?Sized>(
    value: &T,
    maximum: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    count_trusted_json_bytes_with_work(value, maximum, work).map(|_| ())
}

/// Counts the original compact serialization of an audited, finite SDK DTO.
pub(crate) fn count_trusted_json_bytes_with_work<T: Serialize + ?Sized>(
    value: &T,
    maximum: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<u64> {
    let Some(observer) = work.as_deref_mut() else {
        return json_byte_length(value, maximum);
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let native_code = observer.original_native_work().map(|_| crate::wire::ErrorCode::Invalid);
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let native_code = None;
    observer.check_cancellation()?;
    const CONTEXT_LABEL_BYTES: u64 = {
        let labels = ["EffectState", "Context", "BorrowedContext", "ContextMessages",
            "JoinedContext", "metadata", "messages", "current_input_index",
            "ContextAttribute", "ContextSelection", "ContextSourceValue",
            "ContextExtent", "ContextRepresentation",
            "provider", "guarantee", "effect_kind", "request", "request_digest",
            "result_schema", "result_schema_digest", "status", "attempts",
            "EffectStatus", "FileRef", "VolumeRef", "FileDescriptor",
            "ProviderRef", "key must be a string", "number out of range"];
        let mut bytes = 0;
        let mut index = 0;
        while index < labels.len() {
            bytes += labels[index].len() as u64 + 4;
            index += 1;
        }
        bytes
    };
    let diagnostic = audited_serde_diagnostic_bytes(
        0, preparation_serde_diagnostic_label_bytes() + CONTEXT_LABEL_BYTES,
    )?;
    observer.admit_owned(diagnostic)?;
    observer.retain_owned(diagnostic)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let mut custody = if native_code.is_some() {
        Some(observer.take_native_custody(diagnostic)?)
    } else {
        None
    };
    let result = json::count_json_bytes_trusted(
        value,
        JsonBounds {
            maximum_bytes: maximum,
            maximum_items: u64::MAX,
            maximum_depth: if native_code.is_some() { 256 } else { u32::MAX },
        },
        native_code,
        &mut |charge| observer.codec_charge(charge),
    );
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Err(Error::NativeEncode { code, source, custody: None }) = result {
        return Err(Error::NativeEncode { code, source, custody: custody.take() });
    }
    if result.is_ok() && native_code.is_none() {
        observer.release_owned(diagnostic)?;
    }
    result
}

/// Known owned payload/slot capacity of an already bounded SDK Value.
pub(crate) fn json_value_logical_owned_bytes(value: &serde_json::Value) -> Result<u64> {
    let overflow = || {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        { Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "JSON value capacity inventory overflows" } }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        { Error::Invalid("JSON value capacity inventory overflows".into()) }
    };
    match value {
        serde_json::Value::String(text) => Ok(text.capacity() as u64),
        serde_json::Value::Array(values) => {
            let slots = values.capacity().checked_mul(std::mem::size_of::<serde_json::Value>())
                .ok_or_else(overflow)? as u64;
            values.iter().try_fold(slots, |bytes, value| {
                bytes.checked_add(json_value_logical_owned_bytes(value)?).ok_or_else(overflow)
            })
        }
        serde_json::Value::Object(values) => {
            let slots = values.len().checked_mul(std::mem::size_of::<(String, serde_json::Value)>())
                .ok_or_else(overflow)? as u64;
            values.iter().try_fold(slots, |bytes, (key, value)| {
                let value_bytes = json_value_logical_owned_bytes(value)?;
                bytes.checked_add(key.capacity() as u64)
                    .and_then(|bytes| bytes.checked_add(value_bytes))
                    .ok_or_else(overflow)
            })
        }
        _ => Ok(0),
    }
}

/// Counts compact JSON input using a bounded writer, without allocating its bytes.
pub(crate) fn json_byte_length<T: Serialize + ?Sized>(value: &T, maximum: u64) -> Result<u64> {
    struct Budget(u64);

    impl std::io::Write for Budget {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(bytes.len() as u64)
                .ok_or_else(|| std::io::Error::other("JSON exceeds declared byte bound"))?;
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut budget = Budget(maximum);
    serde_json::to_writer(&mut budget, value).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(maximum - budget.0)
}

/// Complete typed decoding for trusted existing record implementations.
pub(crate) fn json_from_slice<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> serde_json::Result<T> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let value = T::deserialize(&mut decoder)?;
    decoder.end()?;
    Ok(value)
}

/// Digest of the exact canonical JSON bytes used at durable identity boundaries.
pub(crate) fn canonical_json_digest<T: Serialize>(value: &T) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&canonical_json_bytes(value)?).as_bytes())
}

/// The same canonical digest, charging its actual original encoder/hash work.
pub(crate) fn canonical_json_digest_with_work<T: Serialize + ?Sized>(
    value: &T,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let bytes = crate::store::history::canonical_bytes_with_work(
        value, maximum_bytes, custody.work())?;
    custody.adopt(bytes.capacity() as u64)?;
    if let Some(work) = custody.work().as_deref_mut() {
        work.hashed_bytes(bytes.len() as u64)?;
    }
    Ok(*blake3::hash(&bytes).as_bytes())
}

