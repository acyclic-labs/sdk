//! Bounded JSON buffers and borrowed Value conversions.
use super::*;
use crate::executor::json::{JsonBounds, JsonCodecCharge as Charge, JsonValueSchema as Schema};
use std::mem::size_of;

impl PreparationIo {
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
            crate::executor::json::canonical_json_bytes_bounded(value, bounds, code, &mut |charge| {
                self.codec_charge(charge, true)
            })
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
        let mut admission = None;
        let result: Result<T> = crate::executor::json::json_from_slice_bounded(
            std::marker::PhantomData::<T>,
            bytes,
            bounds,
            base,
            code,
            Some(T::json_value_schema()),
            &mut |charge| {
                if let Charge::Admission {
                    maximum_owned_bytes,
                    value_shape,
                } = charge
                {
                    let owned = if T::json_value_schema() == Schema::None {
                        maximum_owned_bytes
                            .checked_add(crate::executor::json::sdk_json_diagnostic_bytes())
                            .ok_or_else(|| {
                                decode_allowance_error("upload codec base overflow", code)
                            })?
                    } else {
                        let scratch = maximum_owned_bytes.checked_sub(base).ok_or_else(|| {
                            decode_allowance_error("upload decode scratch bound underflow", code)
                        })?;
                        base.checked_add(
                            scratch.max(
                                crate::executor::json::json_value_decode_owned_bytes(value_shape)
                                    .map_err(|error| decode_allowance_with_code(error, code))?,
                            ),
                        )
                        .ok_or_else(|| {
                            decode_allowance_error("upload decode ownership overflow", code)
                        })?
                    };
                    let diagnostics = crate::executor::json::json_typed_diagnostic_owned_bytes(
                        value_shape,
                        T::diagnostic_label_bytes(),
                        code,
                    )?;
                    admission = Some((self.reserve_typed(owned, diagnostics, code)?, diagnostics));
                    Ok(())
                } else {
                    self.codec_charge(charge, false)
                }
            },
        );
        let mut decoded = finish_decode(self, result, admission)?;
        decoded.1.substitute(decoded.0.logical_owned_bytes()?)?;
        Ok(retain_decoded(decoded))
    }

    pub(crate) fn decode<'a, 'de, T: PreparationRecord>(
        &'a self,
        seed: impl serde::de::DeserializeSeed<'de, Value = T>,
        bytes: &'de [u8],
        code: crate::wire::ErrorCode,
    ) -> Result<(T, DecodeReservation<'a>)> {
        let bounds = self.bounds(bytes.len() as u64, 128)?;
        let schema = T::json_value_schema();
        let code = self.original_native_work().is_some().then_some(code);
        let mut admission = None;
        let result = crate::executor::json::json_from_slice_bounded(
            seed,
            bytes,
            bounds,
            0,
            code,
            Some(schema),
            &mut |charge| {
                if let Charge::Admission {
                    maximum_owned_bytes,
                    value_shape,
                } = charge
                {
                    let owned = T::decode_owned_allowance(
                        value_shape,
                        maximum_owned_bytes,
                        bytes.len() as u64,
                        code,
                    )?;
                    let diagnostics = if schema == Schema::Root {
                        0
                    } else {
                        crate::executor::json::json_typed_diagnostic_owned_bytes(
                            value_shape,
                            T::diagnostic_label_bytes(),
                            code,
                        )?
                    };
                    admission = Some((self.reserve_typed(owned, diagnostics, code)?, diagnostics));
                    Ok(())
                } else {
                    self.codec_charge(charge, false)
                }
            },
        );
        finish_decode(self, result, admission)
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
        held.into_retained();
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
        crate::executor::json::validate_json_byte_bound_native_trusted(
            value,
            bounds,
            crate::wire::ErrorCode::Invalid,
            &mut |charge| self.codec_charge(charge, false),
        )
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
        Ok(retain_decoded(decoded))
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
            self.value_decode_with_schema::<T>(
                value,
                code,
                T::json_value_schema(),
                T::diagnostic_label_bytes(),
                |shape| T::decode_owned_allowance(shape, 0, shape.input_bytes(), Some(code)),
            )
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn value_decode_with_schema<T: serde::de::DeserializeOwned>(
        &self,
        value: &serde_json::Value,
        code: crate::wire::ErrorCode,
        schema: Schema,
        labels: u64,
        mut owned: impl FnMut(crate::executor::json::JsonValueShape) -> Result<u64>,
    ) -> Result<(T, DecodeReservation<'_>)> {
        if self.original_native_work().is_none() {
            return Err(self.invalid("native Value decoding requires original work"));
        }
        let bounds = self.bounds(u64::MAX, 128)?;
        let mut admission = None;
        let result = crate::executor::json::json_from_value_bounded(
            std::borrow::Cow::Borrowed(value),
            bounds,
            0,
            Some(code),
            schema,
            &mut |charge| {
                if let Charge::Admission { value_shape, .. } = charge {
                    let allowance = owned(value_shape)?;
                    let diagnostics = if schema == Schema::Root {
                        0
                    } else {
                        crate::executor::json::json_typed_diagnostic_owned_bytes(
                            value_shape,
                            labels,
                            Some(code),
                        )?
                    };
                    admission = Some((
                        self.reserve_typed(allowance, diagnostics, Some(code))?,
                        diagnostics,
                    ));
                    Ok(())
                } else {
                    self.codec_charge(charge, false)
                }
            },
        );
        finish_decode(self, result, admission)
    }

    pub(crate) fn codec_charge(&self, charge: Charge, encode: bool) -> Result<()> {
        let next = match charge {
            Charge::Retain { bytes } => return self.retain_owned(bytes).map_err(Into::into),
            Charge::Release { bytes } => return self.release(bytes).map_err(Into::into),
            Charge::AllocationOperation { bytes } => return self.allocation_attempt(bytes),
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
        self.charged(next)
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
        text.push(b"0123456789abcdef"[(byte >> 4) as usize] as char);
        text.push(b"0123456789abcdef"[(byte & 15) as usize] as char);
    }
    io.retain_owned((text.capacity() - capacity) as u64)?;
    io.charge(copied)?;
    Ok(text)
}
