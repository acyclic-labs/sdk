use super::*;

struct DigestWriter {
    buffer: arrayvec::ArrayVec<u8, 4096>,
    hasher: Option<blake3::Hasher>,
    maximum: Option<u64>,
    written: u64,
}

impl std::io::Write for DigestWriter {
    #[inline]
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.write_all(bytes)?;
        Ok(bytes.len())
    }

    #[inline]
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        let written = self
            .written
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("JSON byte count overflow"))?;
        if self.maximum.is_some_and(|maximum| written > maximum) {
            return Err(std::io::Error::other("JSON exceeds declared byte bound"));
        }
        self.written = written;
        if self.buffer.try_extend_from_slice(bytes).is_err() {
            self.spill(bytes)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl DigestWriter {
    fn spill(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        let hasher = self.hasher.get_or_insert_with(blake3::Hasher::new);
        hasher.update(&self.buffer);
        self.buffer.clear();
        if bytes.len() >= self.buffer.capacity() {
            hasher.update(bytes);
        } else {
            self.buffer
                .try_extend_from_slice(bytes)
                .map_err(|_| std::io::ErrorKind::WriteZero)?;
        }
        Ok(())
    }
}

// Keep the encoding buffer out of accounting-enabled stack frames.
#[inline(never)]
#[doc(hidden)]
pub fn serialized_json_digest<T: Serialize + ?Sized>(value: &T) -> Result<[u8; 32]> {
    serialized_json_digest_bounded(value, None)
}

#[inline(never)]
fn serialized_json_digest_bounded<T: Serialize + ?Sized>(
    value: &T,
    maximum: Option<u64>,
) -> Result<[u8; 32]> {
    let mut writer = DigestWriter {
        buffer: arrayvec::ArrayVec::new(),
        hasher: None,
        maximum,
        written: 0,
    };
    #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
    super::record_serialization::<T>();
    serde_json::to_writer(&mut writer, value).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(*match writer.hasher {
        Some(mut hasher) => hasher.update(&writer.buffer).finalize(),
        None => blake3::hash(&writer.buffer),
    }
    .as_bytes())
}

/// Counts compact JSON under its byte limit and optional original ledger.
#[doc(hidden)]
pub fn json_byte_length<T: Serialize + ?Sized>(
    value: &T,
    maximum: Option<u64>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<u64> {
    json_byte_length_at_depth(value, maximum, u32::MAX, work)
}

#[doc(hidden)]
pub fn json_byte_length_at_depth<T: Serialize + ?Sized>(
    value: &T,
    maximum: Option<u64>,
    maximum_depth: u32,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<u64> {
    let Some(observer) = work.as_deref_mut() else {
        if maximum_depth != u32::MAX {
            return count_json_bytes_trusted(
                value,
                JsonBounds {
                    maximum_bytes: maximum,
                    maximum_items: None,
                    maximum_depth,
                },
                None,
                &mut |_| Ok(()),
            );
        }
        return {
            struct Budget {
                maximum: Option<u64>,
                written: u64,
            }

            impl std::io::Write for Budget {
                fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                    let written = self
                        .written
                        .checked_add(bytes.len() as u64)
                        .ok_or_else(|| std::io::Error::other("JSON byte count overflow"))?;
                    if self.maximum.is_some_and(|maximum| written > maximum) {
                        return Err(std::io::Error::other("JSON exceeds declared byte bound"));
                    }
                    self.written = written;
                    Ok(bytes.len())
                }

                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }

            let mut budget = Budget {
                maximum,
                written: 0,
            };
            serde_json::to_writer(&mut budget, value)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            Ok(budget.written)
        };
    };
    #[cfg(not(target_arch = "wasm32"))]
    let native_code = observer
        .original_native_work()
        .map(|_| crate::wire::ErrorCode::Invalid);
    #[cfg(target_arch = "wasm32")]
    let native_code = None;
    observer.check_cancellation()?;
    const CONTEXT_LABEL_BYTES: u64 = 484;
    let diagnostic = audited_serde_diagnostic_bytes(
        0,
        preparation_serde_diagnostic_label_bytes() + CONTEXT_LABEL_BYTES,
    )?;
    observer.admit_owned(diagnostic)?;
    observer.retain_owned(diagnostic)?;
    #[cfg(not(target_arch = "wasm32"))]
    let mut custody = if native_code.is_some() {
        Some(observer.take_native_custody(diagnostic)?)
    } else {
        None
    };
    let result = count_json_bytes_trusted(
        value,
        JsonBounds {
            maximum_bytes: maximum,
            maximum_items: None,
            maximum_depth: maximum_depth.min(if native_code.is_some() {
                RECORD_MAXIMUM_DEPTH
            } else {
                u32::MAX
            }),
        },
        native_code,
        &mut |charge| observer.codec_charge(charge),
    );
    #[cfg(not(target_arch = "wasm32"))]
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
#[doc(hidden)]
pub fn canonical_json_digest<T: Serialize + ?Sized>(
    value: &T,
    maximum_bytes: Option<u64>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    if work.is_none() {
        let mut value =
            serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
        value.sort_all_objects();
        return serialized_json_digest_bounded(&value, maximum_bytes);
    }
    if maximum_bytes == Some(0) {
        return Err(crate::store::guarded_boundary(
            work,
            crate::wire::ErrorCode::Invalid,
            "canonical encoder byte allowance must be positive",
        ));
    }
    let observer = work.as_deref_mut().expect("observed canonical digest");
    let code = crate::store::operations::original_native_decode_mode(observer)
        .then_some(crate::wire::ErrorCode::Invalid);
    serialization::canonical_digest_with_work(
        value,
        JsonBounds {
            maximum_bytes,
            maximum_items: maximum_bytes.map(|maximum| maximum.saturating_add(1)),
            maximum_depth: RECORD_MAXIMUM_DEPTH,
        },
        code,
        observer,
    )
}

/// Hash a serializer that already emits every object in canonical key order.
pub(crate) fn borrowed_json_digest<T: Serialize + ?Sized>(
    value: &T,
    maximum_bytes: Option<u64>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    if work.is_none() {
        return serialized_json_digest_bounded(value, maximum_bytes);
    }
    if maximum_bytes == Some(0) {
        return Err(crate::store::guarded_boundary(
            work,
            crate::wire::ErrorCode::Invalid,
            "canonical encoder byte allowance must be positive",
        ));
    }
    let observer = work.as_deref_mut().expect("observed borrowed digest");
    let code = crate::store::operations::original_native_decode_mode(observer)
        .then_some(crate::wire::ErrorCode::Invalid);
    serialization::borrowed_digest_with_work(
        value,
        JsonBounds {
            maximum_bytes,
            maximum_items: maximum_bytes.map(|maximum| maximum.saturating_add(1)),
            maximum_depth: RECORD_MAXIMUM_DEPTH,
        },
        code,
        observer,
    )
}
