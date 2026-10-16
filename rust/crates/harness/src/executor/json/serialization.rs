use super::*;
use serde::{
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
    ser::Serialize,
};
use serde_json::ser::{CompactFormatter, Formatter};
use std::{cell::RefCell, io::Write};

pub(super) struct Scope<'a> {
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &'a mut dyn FnMut(JsonCodecCharge) -> Result<()>,
    failed: Option<Error>,
    retained: u64,
    lexical: admission::Lexical,
}
impl Scope<'_> {
    pub(super) fn event(&mut self, event: JsonCodecCharge) -> Result<()> {
        if self.failed.is_some() && !matches!(event, JsonCodecCharge::Release { .. }) {
            return Err(boundary("JSON admission interrupted", self.code));
        }
        (self.charge)(event)
    }
    pub(super) fn map_nodes(&mut self, entries: usize) -> Result<()> {
        // An insertion can split each ancestor and add a root. Branching is
        // at least two, so this bounds allocations even with full ancestors.
        let nodes = if entries == 0 {
            1
        } else {
            1 + usize::BITS - entries.leading_zeros()
        };
        for _ in 0..nodes {
            self.event(JsonCodecCharge::AllocationOperation {
                bytes: std::mem::size_of::<(String, Value)>() as u64,
            })?;
        }
        Ok(())
    }
    fn retain(&mut self, bytes: u64) -> Result<()> {
        let retained = checked(self.retained.checked_add(bytes), "JSON retention overflow")?;
        self.event(JsonCodecCharge::Retain { bytes })?;
        self.retained = retained;
        Ok(())
    }
    fn release(&mut self, bytes: u64) -> Result<()> {
        let retained = checked(
            self.retained.checked_sub(bytes),
            "JSON retention accounting differs",
        )?;
        self.event(JsonCodecCharge::Release { bytes })?;
        self.retained = retained;
        Ok(())
    }
    fn error(&mut self, error: serde_json::Error) -> Error {
        self.failed
            .take()
            .unwrap_or_else(|| records::decode_error(error, self.code))
    }
    fn stop(&mut self, error: Error) -> std::io::Error {
        if self.failed.is_none() {
            self.failed = Some(error);
        }
        std::io::ErrorKind::Other.into()
    }
}
impl Drop for Scope<'_> {
    fn drop(&mut self) {
        if self.retained != 0 {
            let _ = (self.charge)(JsonCodecCharge::Release {
                bytes: self.retained,
            });
        }
    }
}
type State<'a, 'b> = RefCell<&'a mut Scope<'b>>;
struct Writer<'a, 'b, 'c> {
    state: &'a State<'b, 'c>,
    admitted_items: u64,
    written: u64,
    maximum: u64,
    output: Option<&'a mut Vec<u8>>,
    digest: Option<&'a mut blake3::Hasher>,
    compare: Option<(&'a [u8], &'a mut bool)>,
}
impl Write for Writer<'_, '_, '_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut scope = self.state.borrow_mut();
        let result = (|| {
            let written = checked(
                self.written.checked_add(bytes.len() as u64),
                "JSON byte count overflow",
            )?;
            if written > self.maximum {
                return Err(boundary("JSON exceeds admitted output", scope.code));
            }
            scope.event(JsonCodecCharge::Work {
                bytes: bytes.len() as u64,
                items: 0,
            })?;
            {
                let Scope {
                    lexical,
                    bounds,
                    code,
                    charge,
                    ..
                } = &mut **scope;
                let mut forward = |event| {
                    let event = match event {
                        JsonCodecCharge::Work { bytes, items } => {
                            let admitted = items.min(self.admitted_items);
                            self.admitted_items -= admitted;
                            JsonCodecCharge::Work {
                                bytes,
                                items: items - admitted,
                            }
                        }
                        event => event,
                    };
                    charge(event)
                };
                let charge =
                    RefCell::new(&mut forward as &mut dyn FnMut(JsonCodecCharge) -> Result<()>);
                for byte in bytes {
                    lexical.byte(*byte, *bounds, *code, &charge)?;
                }
            }
            if let Some(output) = &mut self.output {
                scope.event(JsonCodecCharge::Copy {
                    bytes: bytes.len() as u64,
                })?;
                output.extend_from_slice(bytes);
            }
            if let Some(digest) = &mut self.digest {
                digest.update(bytes);
            }
            if let Some((expected, matched)) = &mut self.compare {
                **matched &= expected.get(self.written as usize..written as usize) == Some(bytes);
            }
            self.written = written;
            Ok(())
        })();
        result.map_err(|error| scope.stop(error))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
struct Format<'a, 'b, 'c> {
    state: &'a State<'b, 'c>,
    canonical: bool,
    key: bool,
}
impl Formatter for Format<'_, '_, '_> {
    fn write_i128<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        value: i128,
    ) -> std::io::Result<()> {
        if self.canonical && !self.key {
            serde::Serializer::serialize_i128(serde_json::value::Serializer, value).map_err(
                |error| {
                    let mut scope = self.state.borrow_mut();
                    let code = scope.code;
                    scope.stop(records::decode_error(error, code))
                },
            )?;
        }
        CompactFormatter.write_i128(writer, value)
    }
    fn write_u128<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        value: u128,
    ) -> std::io::Result<()> {
        if self.canonical && !self.key {
            serde::Serializer::serialize_u128(serde_json::value::Serializer, value).map_err(
                |error| {
                    let mut scope = self.state.borrow_mut();
                    let code = scope.code;
                    scope.stop(records::decode_error(error, code))
                },
            )?;
        }
        CompactFormatter.write_u128(writer, value)
    }
    fn begin_object_key<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> std::io::Result<()> {
        self.key = true;
        CompactFormatter.begin_object_key(writer, first)
    }
    fn end_object_key<W: Write + ?Sized>(&mut self, writer: &mut W) -> std::io::Result<()> {
        self.key = false;
        CompactFormatter.end_object_key(writer)
    }
    fn write_f32<W: Write + ?Sized>(&mut self, writer: &mut W, value: f32) -> std::io::Result<()> {
        if self.canonical && !self.key {
            CompactFormatter.write_f64(writer, f64::from(value))
        } else {
            CompactFormatter.write_f32(writer, value)
        }
    }
    fn write_raw_fragment<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        fragment: &str,
    ) -> std::io::Result<()> {
        if !self.canonical {
            return CompactFormatter.write_raw_fragment(writer, fragment);
        }
        let (mut value, owned) = {
            let mut scope = self.state.borrow_mut();
            let result = (|| {
                let bounds = JsonBounds {
                    maximum_depth: scope
                        .bounds
                        .maximum_depth
                        .checked_sub(scope.lexical.depth)
                        .ok_or_else(|| boundary("JSON nesting limit exceeded", scope.code))?,
                    maximum_items: scope
                        .bounds
                        .maximum_items
                        .saturating_sub(scope.lexical.items),
                    ..scope.bounds
                };
                let code = scope.code;
                let shape = admission::source(
                    fragment.as_bytes(),
                    bounds,
                    Some(JsonValueSchema::Root),
                    code,
                    &mut |event| scope.event(event),
                )?;
                let owned = json_value_decode_owned_bytes(shape)?;
                scope.retain(owned)?;
                let value: Value = serde_json::from_str(fragment)
                    .map_err(|error| records::decode_error(error, code))?;
                Ok((value, owned))
            })();
            result.map_err(|error| scope.stop(error))?
        };
        value.sort_all_objects();
        let result = serde_json::to_writer(writer, &value);
        drop(value);
        let mut scope = self.state.borrow_mut();
        scope.release(owned).map_err(|error| scope.stop(error))?;
        result.map_err(|error| {
            let code = scope.code;
            scope.stop(records::decode_error(error, code))
        })
    }
}
fn write<T: Serialize + ?Sized>(
    value: &T,
    scope: &mut Scope<'_>,
    output: Option<&mut Vec<u8>>,
    digest: Option<&mut blake3::Hasher>,
    compare: Option<(&[u8], &mut bool)>,
    maximum: u64,
    canonical: bool,
) -> Result<u64> {
    let admitted_items = scope.lexical.items;
    scope.lexical = admission::Lexical::default();
    let state = RefCell::new(&mut *scope);
    let mut writer = Writer {
        state: &state,
        admitted_items,
        written: 0,
        maximum,
        output,
        digest,
        compare,
    };
    let result = value.serialize(&mut serde_json::Serializer::with_formatter(
        &mut writer,
        Format {
            state: &state,
            canonical,
            key: false,
        },
    ));
    let written = writer.written;

    result.map_err(|error| scope.error(error))?;
    if let Some(error) = scope.failed.take() {
        return Err(error);
    }
    if scope.lexical.string || scope.lexical.depth != 0 {
        return Err(boundary("incomplete JSON document", scope.code));
    }
    Ok(written)
}
fn output<T: Serialize + ?Sized>(
    value: &T,
    scope: &mut Scope<'_>,
    canonical: bool,
) -> Result<Vec<u8>> {
    let size = write(
        value,
        scope,
        None,
        None,
        None,
        scope.bounds.maximum_bytes,
        canonical,
    )?;
    let length = usize::try_from(size)
        .map_err(|_| boundary("JSON output exceeds address space", scope.code))?;
    scope.event(JsonCodecCharge::CopyAdmission { bytes: size })?;
    scope.retain(size)?;
    if size != 0 {
        scope.event(JsonCodecCharge::AllocationOperation { bytes: size })?;
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(Error::Allocation)?;
    scope.retain((bytes.capacity() - length) as u64)?;
    if write(value, scope, Some(&mut bytes), None, None, size, canonical)? != size {
        return Err(boundary(
            "JSON serialization changed between admission and encoding",
            scope.code,
        ));
    }
    Ok(bytes)
}
// Preserve literal maps containing serde's private RawValue key, as to_value does.
struct Plain<'a, 'b, 'c>(&'a State<'b, 'c>);
impl<'de> DeserializeSeed<'de> for Plain<'_, '_, '_> {
    type Value = Value;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<Value, D::Error> {
        decoder.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Plain<'_, '_, '_> {
    type Value = Value;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("JSON value")
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_bool<E: de::Error>(self, value: bool) -> std::result::Result<Value, E> {
        Ok(Value::Bool(value))
    }
    fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Value, E> {
        Ok(Value::from(value))
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Value, E> {
        Ok(Value::from(value))
    }
    fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Value, E> {
        Ok(Value::from(value))
    }
    fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Value, E> {
        let mut scope = self.0.borrow_mut();
        let result = (|| {
            let bytes = value.len() as u64;
            scope.event(JsonCodecCharge::CopyAdmission { bytes })?;
            if bytes != 0 {
                scope.event(JsonCodecCharge::AllocationOperation { bytes })?;
            }
            let mut text = String::new();
            text.try_reserve_exact(value.len())
                .map_err(Error::Allocation)?;
            text.push_str(value);
            scope.event(JsonCodecCharge::Copy { bytes })?;
            Ok(Value::String(text))
        })();
        result.map_err(|error| {
            scope.stop(error);
            E::custom("JSON admission interrupted")
        })
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(Plain(self.0))? {
            if values.len() == values.capacity() {
                let mut scope = self.0.borrow_mut();
                let result = (|| {
                    let bytes = checked(
                        (values.len() as u64 + 1).checked_mul(std::mem::size_of::<Value>() as u64),
                        "JSON capacity overflow",
                    )?;
                    scope.event(JsonCodecCharge::AllocationOperation { bytes })?;
                    values.try_reserve_exact(1).map_err(Error::Allocation)
                })();
                result.map_err(|error| {
                    scope.stop(error);
                    de::Error::custom("JSON admission interrupted")
                })?;
            }
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Value, A::Error> {
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key_seed(Plain(self.0))? {
            let Value::String(key) = key else {
                return Err(de::Error::custom("key must be a string"));
            };
            let value = map.next_value_seed(Plain(self.0))?;
            if !values.contains_key(&key) {
                let mut scope = self.0.borrow_mut();
                scope.map_nodes(values.len()).map_err(|error| {
                    scope.stop(error);
                    de::Error::custom("JSON admission interrupted")
                })?;
            }
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}
fn build<T: Serialize + ?Sized>(value: &T, scope: &mut Scope<'_>) -> Result<Value> {
    let bytes = output(value, scope, true)?;
    let shape = JsonValueShape {
        input_bytes: bytes.len() as u64,
        lexical_items: scope.lexical.items,
        containers: scope.lexical.containers,
        virtual_items: 0,
        raw_bytes: 0,
        raw_frames: 0,
        unresolved_raw: false,
        origin: Origin::Slice,
    };
    let owned = json_value_decode_owned_bytes(shape)?;
    scope.retain(owned)?;
    let state = RefCell::new(&mut *scope);
    let mut decoder = serde_json::Deserializer::from_slice(&bytes);
    decoder.disable_recursion_limit();
    let result = Plain(&state).deserialize(&mut decoder).and_then(|value| {
        decoder.end()?;
        Ok(value)
    });

    let mut value = result.map_err(|error| scope.error(error))?;
    let actual = records::json_value_retained_owned_bytes(&value)?;
    let released = checked(
        owned
            .checked_sub(actual)
            .and_then(|release| release.checked_add(bytes.capacity() as u64)),
        "JSON retention accounting differs",
    )?;
    drop(bytes);
    scope.release(released)?;
    value.sort_all_objects();
    Ok(value)
}
fn scope<'a>(
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &'a mut dyn FnMut(JsonCodecCharge) -> Result<()>,
) -> Scope<'a> {
    Scope {
        bounds: JsonBounds {
            maximum_depth: bounds.maximum_depth.min(256),
            ..bounds
        },
        code,
        charge,
        failed: None,
        retained: 0,
        lexical: admission::Lexical::default(),
    }
}
/// Compact encoding admitted before its output buffer is allocated.
pub fn json_bytes_bounded<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<Vec<u8>> {
    let mut scope = scope(bounds, code, charge);
    let bytes = output(value, &mut scope, false)?;
    scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    scope.retained = 0;
    Ok(bytes)
}
/// Sorted-key encoding with funded buffers and Value storage.
pub(crate) fn canonical_digest_with_work<T: Serialize + ?Sized>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    observer: &mut dyn crate::store::HistoryIo,
) -> Result<[u8; 32]> {
    let observer = RefCell::new(observer);
    let mut charge = |event| observer.borrow_mut().codec_charge(event);
    let mut scope = scope(bounds, code, &mut charge);
    let value = build(value, &mut scope)?;
    let size = write(
        &value,
        &mut scope,
        None,
        None,
        None,
        bounds.maximum_bytes,
        false,
    )?;
    observer.borrow_mut().hashed_bytes(size)?;
    let mut digest = blake3::Hasher::new();
    if write(
        &value,
        &mut scope,
        None,
        Some(&mut digest),
        None,
        size,
        false,
    )? != size
    {
        return Err(boundary("canonical digest length changed", code));
    }
    drop(value);
    scope.release(scope.retained)?;
    scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    Ok(*digest.finalize().as_bytes())
}

/// Sorted-key encoding with funded buffers and Value storage.
pub fn canonical_json_bytes_bounded<
    T: Serialize + ?Sized,
    F: FnMut(JsonCodecCharge) -> Result<()>,
>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<Vec<u8>> {
    let mut scope = scope(bounds, code, charge);
    let value = build(value, &mut scope)?;
    let bytes = output(&value, &mut scope, false)?;
    let released = checked(
        scope.retained.checked_sub(bytes.capacity() as u64),
        "JSON retention accounting differs",
    )?;
    drop(value);
    scope.release(released)?;
    scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    scope.retained = 0;
    Ok(bytes)
}
/// Converts through an admitted document without interpreting literal `RawValue` keys.
pub fn json_value_bounded<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<Value> {
    let mut scope = scope(bounds, code, charge);
    let value = build(value, &mut scope)?;
    scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    scope.retained = 0;
    Ok(value)
}
/// Copies a resident Value without a temporary document or decoder scratch.
pub fn json_value_copy_bounded<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &Value,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<Value> {
    let mut scope = scope(bounds, code, charge);
    write(
        value,
        &mut scope,
        None,
        None,
        None,
        bounds.maximum_bytes,
        true,
    )?;
    let retained = records::json_value_retained_owned_bytes(value)?;
    scope.retain(retained)?;
    let copied = super::copy::value(value, &mut scope)?;
    let actual = records::json_value_retained_owned_bytes(&copied)?;
    scope.release(checked(
        retained.checked_sub(actual),
        "JSON copy capacity differs",
    )?)?;
    scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    scope.retained = 0;
    Ok(copied)
}
pub(crate) fn count_json_bytes_trusted<
    T: Serialize + ?Sized,
    F: FnMut(JsonCodecCharge) -> Result<()>,
>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<u64> {
    let mut scope = scope(bounds, code, charge);
    scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    let size = write(
        value,
        &mut scope,
        None,
        None,
        None,
        bounds.maximum_bytes,
        false,
    )?;
    scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    Ok(size)
}

#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) fn json_write_bounded<T: Serialize + ?Sized>(
    value: &T,
    output: &mut Vec<u8>,
    bounds: JsonBounds,
    charge: &mut impl FnMut(JsonCodecCharge) -> Result<()>,
) -> Result<()> {
    let mut scope = scope(bounds, Some(crate::wire::ErrorCode::Invalid), charge);
    if !output.is_empty() || (output.capacity() as u64) < bounds.maximum_bytes {
        return Err(boundary("JSON output lacks admitted capacity", scope.code));
    }
    write(
        value,
        &mut scope,
        Some(output),
        None,
        None,
        bounds.maximum_bytes,
        false,
    )?;
    scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 })
}

#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) fn json_digest_bounded<T: Serialize + ?Sized>(
    value: &T,
    expected: Option<&[u8]>,
    bounds: JsonBounds,
    charge: &mut impl FnMut(JsonCodecCharge) -> Result<()>,
) -> Result<Option<(u64, [u8; 32])>> {
    let mut scope = scope(bounds, Some(crate::wire::ErrorCode::Invalid), charge);
    let mut matched = true;
    let mut digest = blake3::Hasher::new();
    let size = write(
        value,
        &mut scope,
        None,
        Some(&mut digest),
        expected.map(|bytes| (bytes, &mut matched)),
        bounds.maximum_bytes,
        false,
    )?;
    scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    Ok(
        (matched && expected.is_none_or(|bytes| size == bytes.len() as u64))
            .then(|| (size, *digest.finalize().as_bytes())),
    )
}
