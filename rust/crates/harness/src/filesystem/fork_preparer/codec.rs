//! Bounded records through the existing contract codec.
use super::*;
use crate::contract::{JsonBounds, JsonCodecCharge as Charge, JsonValueSchema as Schema};
use std::mem::size_of;

impl PreparationIo {
    fn bounds(&self, bytes: u64, depth: u32) -> Result<JsonBounds> {
        Ok(JsonBounds {
            maximum_bytes: bytes,
            maximum_items: self.remaining()?.items_examined,
            maximum_depth: depth,
        })
    }
    fn code(&self, code: crate::wire::ErrorCode) -> Option<crate::wire::ErrorCode> {
        self.original_native_work().is_some().then_some(code)
    }
    fn finish_codec<'a, T>(
        &'a self,
        result: Result<T>,
        admission: Option<(DecodeReservation<'a>, u64)>,
    ) -> Result<(T, DecodeReservation<'a>)> {
        let (mut held, diagnostics) =
            admission.ok_or_else(|| self.invalid("codec omitted admission"))?;
        match result {
            Ok(value) => {
                held.substitute(held.bytes() - diagnostics)?;
                Ok((value, held))
            }
            #[cfg(not(target_arch = "wasm32"))]
            Err(Error::NativeJson {
                code,
                source,
                custody: None,
            }) if self.original_native_work().is_some() => {
                let (source, custody) = native_decoded((source, held))?;
                Err(Error::NativeJson {
                    code,
                    source,
                    custody: Some(custody),
                })
            }
            Err(error) => Err(error),
        }
    }
    fn admission(
        &self,
        owned: u64,
        shape: crate::contract::JsonValueShape,
        labels: u64,
        code: Option<crate::wire::ErrorCode>,
    ) -> Result<(DecodeReservation<'_>, u64)> {
        let diagnostics = crate::contract::json_typed_diagnostic_owned_bytes(shape, labels, code)?;
        Ok((
            self.reserve_decode(
                owned
                    .checked_add(diagnostics)
                    .ok_or_else(|| self.invalid("codec ownership overflow"))?,
            )?,
            diagnostics,
        ))
    }
    pub(crate) fn encode<T: Serialize + ?Sized>(
        &self,
        value: &T,
        maximum: u64,
        canonical: bool,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let bounds = self.bounds(maximum.min(self.remaining()?.bytes_encoded), 256)?;
        let code = self.code(crate::wire::ErrorCode::Invalid);
        let diagnostics = code
            .map(|_| self.reserve_decode(crate::contract::sdk_json_diagnostic_bytes()))
            .transpose()?;
        let mut charge = |charge| self.codec_charge(charge, true);
        let result = if canonical {
            crate::contract::canonical_json_bytes_bounded(value, bounds, code, &mut charge)
        } else {
            crate::contract::json_bytes_bounded(value, bounds, code, &mut charge)
        };
        let result = match diagnostics {
            Some(held) => self
                .finish_codec(result, Some((held, 0)))
                .map(|(bytes, _)| bytes),
            None => result,
        };
        crate::executor::with_history_io(Some(self), |work| {
            crate::store::operations::owned_bytes_from_retained_vec(result?, work)
                .map(|(bytes, _)| bytes)
        })
    }
    pub(crate) fn decode<'a, 'de, T: PreparationRecord>(
        &'a self,
        seed: impl serde::de::DeserializeSeed<'de, Value = T>,
        bytes: &'de [u8],
        code: crate::wire::ErrorCode,
    ) -> Result<(T, DecodeReservation<'a>)> {
        let code = self.code(code);
        let schema = T::json_value_schema();
        let mut admission = None;
        let result = crate::contract::json_from_slice_bounded(
            seed,
            bytes,
            self.bounds(bytes.len() as u64, 128)?,
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
                    admission = Some(if schema == Schema::Root {
                        (self.reserve_decode(owned)?, 0)
                    } else {
                        self.admission(owned, value_shape, T::diagnostic_label_bytes(), code)?
                    });
                    Ok(())
                } else {
                    self.codec_charge(charge, false)
                }
            },
        );
        self.finish_codec(result, admission)
    }
    pub(crate) fn decode_json<T: UploadRecord>(&self, bytes: &[u8]) -> Result<T> {
        let code = self.code(crate::wire::ErrorCode::Storage);
        let base = (bytes.len() as u64)
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(size_of::<T>() as u64))
            .ok_or_else(|| self.invalid("record ownership overflow"))?;
        let schema = T::json_value_schema();
        let mut admission = None;
        let result: Result<T> = crate::contract::json_from_slice_bounded(
            std::marker::PhantomData,
            bytes,
            self.bounds(bytes.len() as u64, 256)?,
            base,
            code,
            Some(schema),
            &mut |charge| {
                if let Charge::Admission {
                    maximum_owned_bytes,
                    value_shape,
                } = charge
                {
                    let owned = if schema == Schema::None {
                        maximum_owned_bytes
                            .checked_add(crate::contract::sdk_json_diagnostic_bytes())
                    } else {
                        let value = crate::contract::json_value_decode_owned_bytes(value_shape)?;
                        maximum_owned_bytes
                            .checked_sub(base)
                            .and_then(|scratch| base.checked_add(scratch.max(value)))
                    }
                    .ok_or_else(|| self.invalid("record ownership overflow"))?;
                    admission = Some(self.admission(
                        owned,
                        value_shape,
                        T::diagnostic_label_bytes(),
                        code,
                    )?);
                    Ok(())
                } else {
                    self.codec_charge(charge, false)
                }
            },
        );
        let mut decoded = self.finish_codec(result, admission)?;
        decoded.1.substitute(decoded.0.logical_owned_bytes()?)?;
        Ok(retain_decoded(decoded))
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn value_record<T: serde::de::DeserializeOwned>(
        &self,
        value: &serde_json::Value,
        code: crate::wire::ErrorCode,
        schema: Schema,
        labels: u64,
        owned: impl Fn(crate::contract::JsonValueShape) -> Result<u64>,
    ) -> Result<(T, DecodeReservation<'_>)> {
        if self.original_native_work().is_none() {
            return Err(self.invalid("borrowed Value requires original work"));
        }
        let mut admission = None;
        let result = crate::contract::json_from_value_bounded(
            std::borrow::Cow::Borrowed(value),
            self.bounds(u64::MAX, 128)?,
            0,
            code,
            schema,
            &mut |charge| {
                if let Charge::Admission { value_shape, .. } = charge {
                    admission = Some(if schema == Schema::Root {
                        (self.reserve_decode(owned(value_shape)?)?, 0)
                    } else {
                        self.admission(owned(value_shape)?, value_shape, labels, Some(code))?
                    });
                    Ok(())
                } else {
                    self.codec_charge(charge, false)
                }
            },
        );
        self.finish_codec(result, admission)
    }
    pub(crate) fn value_decode_preparation<T: PreparationRecord>(
        &self,
        value: &serde_json::Value,
        code: crate::wire::ErrorCode,
    ) -> Result<(T, DecodeReservation<'_>)> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.value_record(
                value,
                code,
                T::json_value_schema(),
                T::diagnostic_label_bytes(),
                |shape| T::decode_owned_allowance(shape, 0, shape.input_bytes(), Some(code)),
            )
        }
        #[cfg(target_arch = "wasm32")]
        {
            let bytes = self.encode(value, acyclic_stream::MAX_RECORD_BYTES as u64, false)?;
            let _held = self.guard_bytes(&bytes)?;
            self.decode(std::marker::PhantomData, &bytes, code)
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn value_decode<T: UploadRecord>(
        &self,
        value: &serde_json::Value,
        code: crate::wire::ErrorCode,
    ) -> Result<T> {
        let mut decoded: (T, _) = self.value_record(
            value,
            code,
            T::json_value_schema(),
            T::diagnostic_label_bytes(),
            |shape| {
                crate::contract::json_typed_value_decode_owned_bytes(
                    shape,
                    preparation_slot_bytes::<T>(),
                )?
                .checked_add(size_of::<T>() as u64)
                .ok_or_else(|| self.invalid("record ownership overflow"))
            },
        )?;
        decoded.1.substitute(
            (size_of::<T>() as u64)
                .checked_add(decoded.0.logical_owned_bytes()?)
                .ok_or_else(|| self.invalid("record ownership overflow"))?,
        )?;
        Ok(retain_decoded(decoded))
    }
    pub(crate) fn value_json<T: Serialize + ?Sized>(
        &self,
        value: &T,
        maximum: u64,
    ) -> Result<serde_json::Value> {
        if self.original_native_work().is_none() {
            return Err(self.invalid("Value conversion requires original work"));
        }
        let diagnostics = self.reserve_decode(crate::contract::sdk_json_diagnostic_bytes())?;
        self.retain_owned(size_of::<serde_json::Value>() as u64)?;
        let held = self.guard_owned(size_of::<serde_json::Value>() as u64)?;
        let result = crate::contract::json_value_bounded(
            value,
            self.bounds(maximum, 256)?,
            self.code(crate::wire::ErrorCode::Invalid),
            &mut |charge| self.codec_charge(charge, false),
        );
        let (value, _) = self.finish_codec(result, Some((diagnostics, 0)))?;
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
            return Err(self.invalid("Value sizing requires original work"));
        }
        let bounds = self.bounds(maximum, 256)?;
        crate::contract::json_owned_value_shape_bounded(
            value,
            bounds,
            Schema::None,
            &mut |charge| self.codec_charge(charge, false),
        )?;
        crate::contract::validate_json_byte_bound_native_trusted(
            value,
            bounds,
            crate::wire::ErrorCode::Invalid,
            &mut |charge| self.codec_charge(charge, false),
        )
    }
    pub(crate) fn codec_charge(&self, charge: Charge, encode: bool) -> Result<()> {
        let work = match charge {
            Charge::Retain { bytes } => return self.retain_owned(bytes),
            Charge::Release { bytes } => return self.release(bytes),
            Charge::AllocationOperation { bytes } => return self.allocation_attempt(bytes),
            Charge::CopyAdmission { bytes } => {
                return self.preflight(acyclic_fs::WorkCounters {
                    bytes_copied: bytes,
                    ..Default::default()
                });
            }
            Charge::Copy { bytes } => acyclic_fs::WorkCounters {
                bytes_copied: bytes,
                ..Default::default()
            },
            Charge::Read { bytes } => acyclic_fs::WorkCounters {
                source_bytes_read: bytes,
                ..Default::default()
            },
            Charge::Inspect { items } => acyclic_fs::WorkCounters {
                items_examined: items,
                ..Default::default()
            },
            Charge::Work { bytes, items } => acyclic_fs::WorkCounters {
                bytes_encoded: if encode { bytes } else { 0 },
                source_bytes_read: if encode { 0 } else { bytes },
                items_examined: items,
                ..Default::default()
            },
            Charge::Admission { .. } => return Err(self.invalid("codec requires scoped admission")),
        };
        self.charge(work)
    }
}
