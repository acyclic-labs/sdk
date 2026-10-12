use super::*;
use crate::contract::canonical_json_bytes;
/// Counts compact JSON under its byte limit and optional original ledger.
pub(crate) fn json_byte_length<T: Serialize + ?Sized>(
    value: &T,
    maximum: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<u64> {
    let Some(observer) = work.as_deref_mut() else {
        return {
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
            serde_json::to_writer(&mut budget, value)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            Ok(maximum - budget.0)
        };
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let native_code = observer
        .original_native_work()
        .map(|_| crate::wire::ErrorCode::Invalid);
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let native_code = None;
    observer.check_cancellation()?;
    const CONTEXT_LABEL_BYTES: u64 = {
        let labels = [
            "EffectState",
            "Context",
            "BorrowedContext",
            "ContextMessages",
            "JoinedContext",
            "metadata",
            "messages",
            "current_input_index",
            "ContextAttribute",
            "ContextSelection",
            "ContextSourceValue",
            "ContextExtent",
            "ContextRepresentation",
            "provider",
            "guarantee",
            "effect_kind",
            "request",
            "request_digest",
            "result_schema",
            "result_schema_digest",
            "status",
            "attempts",
            "EffectStatus",
            "FileRef",
            "VolumeRef",
            "FileDescriptor",
            "ProviderRef",
            "key must be a string",
            "number out of range",
        ];
        let mut bytes = 0;
        let mut index = 0;
        while index < labels.len() {
            bytes += labels[index].len() as u64 + 4;
            index += 1;
        }
        bytes
    };
    let diagnostic = audited_serde_diagnostic_bytes(
        0,
        preparation_serde_diagnostic_label_bytes() + CONTEXT_LABEL_BYTES,
    )?;
    observer.admit_owned(diagnostic)?;
    observer.retain_owned(diagnostic)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let mut custody = if native_code.is_some() {
        Some(observer.take_native_custody(diagnostic)?)
    } else {
        None
    };
    let result = count_json_bytes_trusted(
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
    if let Err(Error::NativeJson {
        code,
        source,
        custody: None,
    }) = result
    {
        return Err(Error::NativeJson {
            code,
            source,
            custody: custody.take(),
        });
    }
    if result.is_ok() && native_code.is_none() {
        observer.release_owned(diagnostic)?;
    }
    result
}

/// Canonical digest with optional original encoding and hashing charges.
pub(crate) fn canonical_json_digest<T: Serialize + ?Sized>(
    value: &T,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    if work.is_none() {
        return Ok(*blake3::hash(&canonical_json_bytes(value)?).as_bytes());
    }
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let bytes =
        crate::store::history::canonical_bytes_with_work(value, maximum_bytes, custody.work())?;
    custody.adopt(bytes.capacity() as u64)?;
    if let Some(work) = custody.work().as_deref_mut() {
        work.hashed_bytes(bytes.len() as u64)?;
    }
    Ok(*blake3::hash(&bytes).as_bytes())
}
