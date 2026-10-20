//! Bounded JSON buffers and borrowed Value conversions.
use super::*;
use crate::executor::json::{JsonBounds, JsonCodecCharge as Charge, JsonValueSchema as Schema};
use std::mem::size_of;

pub(in crate::filesystem) fn decode_record<T: PreparationRecord>(
    bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<T> {
    match io {
        Some(io) => retain_decoded(io.decode(
            std::marker::PhantomData::<T>,
            bytes,
            crate::wire::ErrorCode::Storage,
        )?),
        None => crate::contract::json_from_slice(bytes)
            .map_err(|error| Error::Storage(error.to_string())),
    }
}

impl PreparationIo {
    pub(crate) fn canonical_digest<T: Serialize>(&self, value: &T) -> Result<[u8; 32]> {
        let remaining = self.remaining()?;
        if remaining.bytes_encoded != 0 {
            self.preflight(WorkCounters {
                bytes_encoded: 1,
                items_examined: 1,
                ..Default::default()
            })?;
        }
        let bounds = JsonBounds {
            maximum_bytes: remaining.bytes_encoded,
            maximum_items: remaining.items_examined,
            maximum_depth: 256,
        };
        let code = self
            .original_native_work()
            .is_some()
            .then_some(crate::wire::ErrorCode::Invalid);
        #[cfg(not(target_arch = "wasm32"))]
        let diagnostics = code
            .map(|_| self.reserve_decode(crate::executor::json::sdk_json_diagnostic_bytes()))
            .transpose()?;
        let result = crate::executor::with_history_io(Some(self), |work| {
            crate::executor::json::canonical_digest_with_work(
                value,
                bounds,
                code,
                work.as_deref_mut().expect("preparation history observer"),
            )
        });
        #[cfg(not(target_arch = "wasm32"))]
        let result = match diagnostics {
            Some(guard) => finish_encoding(guard, result),
            None => result,
        };
        result
    }

    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    pub(crate) fn write_json<T: Serialize>(
        &self,
        value: &T,
        output: &mut Vec<u8>,
        maximum: u64,
    ) -> Result<()> {
        let bounds = self.bounds(maximum, 256)?;
        let diagnostics = self.reserve_decode(crate::executor::json::sdk_json_diagnostic_bytes())?;
        let result =
            crate::executor::json::json_write_bounded(value, output, bounds, &mut |charge| {
                self.codec_charge(charge, true)
            });
        finish_encoding(diagnostics, result)
    }

    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    pub(crate) fn digest<T: Serialize>(
        &self,
        value: &T,
        maximum: u64,
        expected: Option<&[u8]>,
    ) -> Result<Option<(u64, [u8; 32])>> {
        let work = self
            .original_native_work()
            .ok_or_else(|| self.invalid("native digest lacks original work"))?;
        let bounds = self.bounds(maximum.min(work.remaining()?.bytes_encoded), 256)?;
        let diagnostics = self.reserve_decode(crate::executor::json::sdk_json_diagnostic_bytes())?;
        let result =
            crate::executor::json::json_digest_bounded(value, expected, bounds, &mut |charge| {
                if let Charge::Work { bytes, items } = charge {
                    work.charge(WorkCounters {
                        bytes_encoded: bytes,
                        bytes_hashed: bytes,
                        items_examined: items,
                        ..Default::default()
                    })
                    .map_err(Into::into)
                } else {
                    self.codec_charge(charge, true)
                }
            });
        finish_encoding(diagnostics, result)
    }

    fn bounds(&self, bytes: u64, depth: u32) -> Result<JsonBounds> {
        Ok(JsonBounds {
            maximum_bytes: bytes,
            maximum_items: self.remaining()?.items_examined,
            maximum_depth: depth,
        })
    }
    pub(crate) fn encode<T: Serialize>(
        &self,
        value: &T,
        maximum: u64,
        canonical: bool,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let remaining = self.remaining()?;
        if maximum != 0 {
            self.preflight(WorkCounters {
                bytes_encoded: 1,
                items_examined: 1,
                ..Default::default()
            })?;
        }
        let bounds = JsonBounds {
            maximum_bytes: maximum.min(remaining.bytes_encoded),
            maximum_items: remaining.items_examined,
            maximum_depth: 256,
        };
        let code = self
            .original_native_work()
            .is_some()
            .then_some(crate::wire::ErrorCode::Invalid);
        #[cfg(not(target_arch = "wasm32"))]
        let diagnostics = code
            .map(|_| self.reserve_decode(crate::executor::json::sdk_json_diagnostic_bytes()))
            .transpose()?;
        let result = if canonical {
            crate::executor::json::canonical_json_bytes_bounded(
                value,
                bounds,
                code,
                &mut |charge| self.codec_charge(charge, true),
            )
        } else {
            crate::executor::json::json_bytes_bounded(value, bounds, code, &mut |charge| {
                self.codec_charge(charge, true)
            })
        };
        #[cfg(not(target_arch = "wasm32"))]
        let result = match diagnostics {
            Some(guard) => finish_encoding(guard, result),
            None => result,
        };
        crate::executor::with_history_io(Some(self), |work| {
            crate::store::operations::owned_bytes_from_retained_vec(result?, work)
                .map(|(bytes, _)| bytes)
        })
    }

    fn decode_guarded<T>(
        &self,
        labels: Option<u64>,
        code: Option<crate::wire::ErrorCode>,
        mut allowance: impl FnMut(crate::executor::json::JsonValueShape, u64) -> Result<u64>,
        decode: impl FnOnce(&mut dyn FnMut(Charge) -> Result<()>) -> Result<T>,
    ) -> Result<(T, DecodeReservation<'_>)> {
        let mut admission = None;
        let result = decode(&mut |charge| {
            if let Charge::Admission {
                maximum_owned_bytes,
                value_shape,
            } = charge
            {
                let owned = allowance(value_shape, maximum_owned_bytes)?;
                let diagnostics = labels
                    .map(|labels| {
                        crate::executor::json::json_typed_diagnostic_owned_bytes(
                            value_shape,
                            labels,
                            code,
                        )
                    })
                    .transpose()?
                    .unwrap_or(0);
                let bytes = owned.checked_add(diagnostics).ok_or_else(|| {
                    decode_allowance_error("typed diagnostic reservation overflow", code)
                })?;
                admission = Some((self.reserve_decode(bytes)?, diagnostics));
                Ok(())
            } else {
                self.codec_charge(charge, false)
            }
        });
        finish_decode(self, result, admission)
    }

    pub(crate) fn decode_json<T: UploadRecord>(&self, bytes: &[u8]) -> Result<T> {
        let code = self
            .original_native_work()
            .is_some()
            .then_some(crate::wire::ErrorCode::Storage);
        let base = (bytes.len() as u64)
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(size_of::<T>() as u64))
            .ok_or_else(|| decode_allowance_error("upload decode ownership overflow", code))?;
        let bounds = self.bounds(bytes.len() as u64, 256)?;
        let schema = T::json_value_schema();
        let mut decoded: (T, _) = self.decode_guarded(
            Some(T::diagnostic_label_bytes()),
            code,
            |shape, maximum| {
                if schema == Schema::None {
                    return maximum
                        .checked_add(crate::executor::json::sdk_json_diagnostic_bytes())
                        .ok_or_else(|| decode_allowance_error("upload codec base overflow", code));
                }
                let scratch = maximum.checked_sub(base).ok_or_else(|| {
                    decode_allowance_error("upload decode scratch bound underflow", code)
                })?;
                base.checked_add(
                    scratch.max(
                        crate::executor::json::json_value_decode_owned_bytes(shape)
                            .map_err(|error| decode_allowance_with_code(error, code))?,
                    ),
                )
                .ok_or_else(|| decode_allowance_error("upload decode ownership overflow", code))
            },
            |observe| {
                crate::executor::json::json_from_slice_bounded(
                    std::marker::PhantomData::<T>,
                    bytes,
                    bounds,
                    base,
                    code,
                    Some(schema),
                    &mut |charge| observe(charge),
                )
            },
        )?;
        decoded.1.substitute(decoded.0.logical_owned_bytes()?)?;
        retain_decoded(decoded)
    }

    pub(crate) fn decode<'a, 'de, T: PreparationRecord>(
        &'a self,
        seed: impl serde::de::DeserializeSeed<'de, Value = T>,
        bytes: &'de [u8],
        code: crate::wire::ErrorCode,
    ) -> Result<(T, DecodeReservation<'a>)> {
        let bounds = self.bounds(
            bytes.len() as u64,
            crate::executor::json::RECORD_MAXIMUM_DEPTH,
        )?;
        let schema = T::json_value_schema();
        let code = self.original_native_work().is_some().then_some(code);
        let mut decoded = self.decode_guarded(
            (schema != Schema::Root).then(T::diagnostic_label_bytes),
            code,
            |shape, scratch| T::decode_owned_allowance(shape, scratch, bytes.len() as u64, code),
            |observe| {
                crate::executor::json::json_from_slice_bounded(
                    seed,
                    bytes,
                    bounds,
                    0,
                    code,
                    Some(schema),
                    &mut |charge| observe(charge),
                )
            },
        )?;
        if let Some(bytes) = decoded.0.decoded_owned_bytes()? {
            decoded.1.substitute(bytes)?;
        }
        Ok(decoded)
    }

    pub(crate) fn value_json<T: Serialize>(
        &self,
        value: &T,
        maximum: u64,
    ) -> Result<serde_json::Value> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.original_native_work().is_none() {
            return Err(self.invalid("native JSON graph conversion requires original work"));
        }
        let bounds = self.bounds(maximum, 256)?;
        let root = size_of::<serde_json::Value>() as u64;
        self.retain_owned(root)?;
        let held = self.guard_owned(root)?;
        let diagnostics = self.reserve_decode(crate::executor::json::sdk_json_diagnostic_bytes())?;
        let result = crate::executor::json::json_value_bounded(
            value,
            bounds,
            Some(crate::wire::ErrorCode::Invalid),
            &mut |charge| self.codec_charge(charge, false),
        );
        let value = finish_encoding(diagnostics, result)?;
        held.into_retained()?;
        Ok(value)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn check_json_byte_bound(
        &self,
        value: &serde_json::Value,
        maximum: u64,
    ) -> Result<()> {
        if self.original_native_work().is_none() {
            return Err(self.invalid("native JSON sizing requires original work"));
        }
        let bounds = self.bounds(maximum, 256)?;
        crate::executor::json::json_owned_value_shape_bounded(
            value,
            bounds,
            Schema::None,
            &mut |charge| self.codec_charge(charge, false),
        )?;
        crate::executor::json::count_json_bytes_trusted(
            value,
            bounds,
            Some(crate::wire::ErrorCode::Invalid),
            &mut |charge| self.codec_charge(charge, false),
        )
        .map(|_| ())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn value_decode<T: UploadRecord>(
        &self,
        value: &serde_json::Value,
        code: crate::wire::ErrorCode,
    ) -> Result<T> {
        let mut decoded = self.value_decode_with_schema::<T>(
            value,
            code,
            T::json_value_schema(),
            T::diagnostic_label_bytes(),
            |shape| {
                crate::executor::json::json_typed_value_decode_owned_bytes(
                    shape,
                    preparation_slot_bytes::<T>(),
                )
                .map_err(|error| decode_allowance_with_code(error, Some(code)))?
                .checked_add(size_of::<T>() as u64)
                .ok_or_else(|| {
                    decode_allowance_error("typed Value reservation overflow", Some(code))
                })
            },
        )?;
        let actual = (size_of::<T>() as u64)
            .checked_add(decoded.0.logical_owned_bytes()?)
            .ok_or_else(|| decode_allowance_error("typed Value ownership overflow", Some(code)))?;
        decoded.1.substitute(actual)?;
        retain_decoded(decoded)
    }

    pub(crate) fn value_decode_preparation<T: PreparationRecord>(
        &self,
        value: &serde_json::Value,
        code: crate::wire::ErrorCode,
    ) -> Result<(T, DecodeReservation<'_>)> {
        #[cfg(target_arch = "wasm32")]
        {
            let bytes = self.encode(value, acyclic_stream::MAX_RECORD_BYTES as u64, false)?;
            let _bytes = self.guard_bytes(&bytes)?;
            self.decode(std::marker::PhantomData::<T>, &bytes, code)
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut decoded = self.value_decode_with_schema::<T>(
                value,
                code,
                T::json_value_schema(),
                T::diagnostic_label_bytes(),
                |shape| T::decode_owned_allowance(shape, 0, shape.input_bytes(), Some(code)),
            )?;
            if let Some(bytes) = decoded.0.decoded_owned_bytes()? {
                decoded.1.substitute(bytes)?;
            }
            Ok(decoded)
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn value_decode_with_schema<T: serde::de::DeserializeOwned>(
        &self,
        value: &serde_json::Value,
        code: crate::wire::ErrorCode,
        schema: &'static Schema,
        labels: u64,
        mut owned: impl FnMut(crate::executor::json::JsonValueShape) -> Result<u64>,
    ) -> Result<(T, DecodeReservation<'_>)> {
        if self.original_native_work().is_none() {
            return Err(self.invalid("native Value decoding requires original work"));
        }
        let bounds = self.bounds(u64::MAX, 128)?;
        self.decode_guarded(
            (schema != Schema::Root).then_some(labels),
            Some(code),
            |shape, _| owned(shape),
            |observe| {
                crate::executor::json::json_from_value_bounded(
                    std::borrow::Cow::Borrowed(value),
                    bounds,
                    0,
                    Some(code),
                    schema,
                    &mut |charge| observe(charge),
                )
            },
        )
    }

    pub(crate) fn codec_charge(&self, charge: Charge, encode: bool) -> Result<()> {
        let next = match charge {
            Charge::Retain { bytes } => return self.retain_owned(bytes).map_err(Into::into),
            Charge::Release { bytes } => return self.release(bytes).map_err(Into::into),
            Charge::AllocationOperation { bytes } => return self.allocation_attempt(bytes),
            Charge::AllocationOperations { operations } => WorkCounters {
                allocation_operations: operations,
                ..Default::default()
            },
            Charge::CopyAdmission { bytes } => {
                return self
                    .preflight(WorkCounters {
                        bytes_copied: bytes,
                        ..Default::default()
                    })
                    .map_err(Into::into);
            }
            Charge::Admission { .. } => {
                return Err(self.invalid("trusted decode admission requires a scoped reservation"));
            }
            Charge::Copy { bytes } => WorkCounters {
                bytes_copied: bytes,
                ..Default::default()
            },
            Charge::Read { bytes } => WorkCounters {
                source_bytes_read: bytes,
                ..Default::default()
            },
            Charge::Inspect { items } => WorkCounters {
                items_examined: items,
                ..Default::default()
            },
            Charge::Work { bytes, items } => WorkCounters {
                bytes_encoded: if encode { bytes } else { 0 },
                source_bytes_read: if encode { 0 } else { bytes },
                items_examined: items,
                ..Default::default()
            },
        };
        // Charging admits under the same ledger guard before changing counters.
        // Keep native denials typed, including at an exhausted allocation limit.
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = self.original_native_work() {
            return native.charge(next).map_err(Into::into);
        }
        self.charge(next).map_err(Into::into)
    }
}

pub(in crate::filesystem) fn clone_bytes(
    bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<acyclic_stream::OwnedBytes> {
    let output = crate::executor::with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let output = crate::core::copy_copy_vector(bytes, &mut quota)?;
        quota.transfer();
        Ok::<_, Error>(output)
    })?;
    crate::executor::with_history_io(io, |work| {
        crate::store::operations::owned_bytes_from_retained_vec(output, work)
            .map(|(bytes, _)| bytes)
    })
}

pub(in crate::filesystem) fn hex_key(bytes: &[u8], io: Option<&PreparationIo>) -> Result<String> {
    let Some(io) = io else {
        return Ok(hex::encode(bytes));
    };
    let capacity = bytes
        .len()
        .checked_mul(2)
        .ok_or_else(|| io.invalid("hex key size overflow"))?;
    let copied = WorkCounters {
        bytes_copied: capacity as u64,
        ..Default::default()
    };
    io.retain_owned(capacity as u64)?;
    io.preflight(copied)?;
    io.allocation_attempt(capacity as u64)?;
    let mut text = String::new();
    text.try_reserve_exact(capacity)
        .map_err(Error::Allocation)?;
    for byte in bytes {
        for nibble in [byte >> 4, byte & 15] {
            let digit = b"0123456789abcdef"
                .get(usize::from(nibble))
                .ok_or_else(|| io.invalid("hexadecimal nibble is out of range"))?;
            text.push(char::from(*digit));
        }
    }
    io.retain_owned((text.capacity() - capacity) as u64)?;
    io.charge(copied)?;
    Ok(text)
}
