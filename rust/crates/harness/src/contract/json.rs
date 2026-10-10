//! The canonical codec bounds logical data, not allocator metadata or RSS.
//! Custom Serialize/Deserialize code is trusted application code: serde cannot
//! prevent it allocating or looping before it calls a serializer/visitor.

use super::{Error, Result};
use serde::ser::{self, Serialize};
use serde_json::Value;

/// Finite admission for one JSON document. Items include map keys and duplicate
/// values, even when canonical last-key-wins output discards an earlier value.
#[derive(Clone, Copy, Debug)]
pub struct JsonBounds {
    /// UTF-8 encoded/input byte limit; duplicate emitted entries count too.
    pub maximum_bytes: u64,
    /// Values and map keys visited, including replaced duplicate values.
    pub maximum_items: u64,
    /// Zero-based nesting depth. Bounded entry points support at most 256.
    pub maximum_depth: u32,
}

impl JsonBounds {
    pub(crate) const IDENTITY: Self = Self {
        maximum_bytes: usize::MAX as u64,
        maximum_items: usize::MAX as u64,
        maximum_depth: u32::MAX,
    };
}

/// Called before admitted logical storage grows and after it is freed. Keep a
/// custody guard through the returned output's lifetime. Errors release every
/// codec Retain after its buffers drop; separate caller-owned guards still
/// belong to the caller. Allocator/node overhead and borrowed input are excluded.
#[derive(Clone, Copy, Debug)]
pub enum JsonCodecCharge {
    /// Logical encoded bytes produced (including sizing passes), and items
    /// visited. Zero amounts are cancellation/fence checkpoints.
    Work {
        /// Encoded bytes.
        bytes: u64,
        /// Structural items.
        items: u64,
    },
    /// Admit requested logical payload/slot storage before allocation.
    Retain {
        /// Logical storage bytes, excluding opaque allocator/map node overhead.
        bytes: u64,
    },
    /// Release storage after its codec-owned buffer/tree has been freed.
    /// This is unconditional cleanup: callbacks must accept it after cancellation
    /// or fencing and must not use it as another work/authority checkpoint.
    Release {
        /// Previously admitted logical storage bytes.
        bytes: u64,
    },
    /// Bytes copied into owned buffers.
    Copy {
        /// Copied bytes.
        bytes: u64,
    },
    /// Bytes consumed by lexical input scanning, not backend physical reads.
    Read {
        /// Borrowed input bytes consumed.
        bytes: u64,
    },
    /// Conservative schema/scratch preflight, NOT actual allocated bytes.
    Admission {
        /// Upper bound justified by the concrete audited schema.
        maximum_owned_bytes: u64,
    },
}

fn owned_logical_bytes(value: &Value) -> u64 {
    match value {
        Value::String(value) => value.len() as u64,
        Value::Array(values) => (values.capacity() * std::mem::size_of::<Value>()) as u64
            + values.iter().map(owned_logical_bytes).sum::<u64>(),
        Value::Object(values) => (values.len() * std::mem::size_of::<(String, Value)>()) as u64
            + values.iter().map(|(key, value)| key.len() as u64 + owned_logical_bytes(value)).sum::<u64>(),
        _ => 0,
    }
}

struct Meter<'a, F: FnMut(JsonCodecCharge) -> Result<()>> {
    bounds: JsonBounds,
    items: u64,
    bytes: u64,
    charge: &'a mut F,
    failure: Option<Error>,
    retained: u64,
}

impl<F: FnMut(JsonCodecCharge) -> Result<()>> Drop for Meter<'_, F> {
    fn drop(&mut self) {
        // The meter is declared before its owned tree/output, so those buffers
        // are already freed on every error/unwind path. Release is cleanup,
        // never a cancellation/fence checkpoint, and must be accepted by the
        // callback even after an earlier failure. Preserve that first failure.
        let retained = std::mem::take(&mut self.retained);
        if retained != 0 {
            let _ = (self.charge)(JsonCodecCharge::Release { bytes: retained });
        }
    }
}

impl<F: FnMut(JsonCodecCharge) -> Result<()>> Meter<'_, F> {
    fn error(message: impl std::fmt::Display) -> serde_json::Error {
        ser::Error::custom(message)
    }
    fn charge(&mut self, charge: JsonCodecCharge) -> serde_json::Result<()> {
        if self.failure.is_some() && !matches!(charge, JsonCodecCharge::Release { .. }) {
            return Err(Self::error("JSON admission interrupted"));
        }
        match (self.charge)(charge) {
            Ok(()) => Ok(()),
            Err(error) => {
                if self.failure.is_none() { self.failure = Some(error); }
                Err(Self::error("JSON admission interrupted"))
            }
        }
    }


    fn item(&mut self, depth: u32) -> serde_json::Result<()> {
        if depth > self.bounds.maximum_depth || self.items >= self.bounds.maximum_items {
            return Err(Self::error("JSON exceeds declared structural bound"));
        }
        self.charge(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        self.items += 1;
        Ok(())
    }

    fn data(&mut self, bytes: u64) -> serde_json::Result<()> {
        let next = self.bytes.checked_add(bytes).ok_or_else(|| Self::error("JSON byte count overflow"))?;
        if next > self.bounds.maximum_bytes {
            return Err(Self::error("JSON exceeds declared byte bound"));
        }
        self.charge(JsonCodecCharge::Work { bytes, items: 0 })?;
        self.bytes = next;
        Ok(())
    }

    fn retain(&mut self, bytes: usize) -> serde_json::Result<()> {
        let retained = self.retained.checked_add(bytes as u64)
            .ok_or_else(|| Self::error("JSON retention overflow"))?;
        self.charge(JsonCodecCharge::Retain { bytes: bytes as u64 })?;
        self.retained = retained;
        Ok(())
    }

    fn release(&mut self, bytes: u64) -> serde_json::Result<()> {
        let retained = self.retained.checked_sub(bytes)
            .ok_or_else(|| Self::error("JSON retention accounting differs"))?;
        self.charge(JsonCodecCharge::Release { bytes })?;
        self.retained = retained;
        Ok(())
    }

    fn string(&mut self, value: &str) -> serde_json::Result<String> {
        // Charge before scanning/copying a potentially huge borrowed string.
        if value.len() as u64 > self.bounds.maximum_bytes.saturating_sub(self.bytes) {
            return Err(Self::error("JSON exceeds declared byte bound"));
        }
        self.charge(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
        struct StringCounter<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>>(&'a mut Meter<'b, F>);
        impl<F: FnMut(JsonCodecCharge) -> Result<()>> std::io::Write for StringCounter<'_, '_, F> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.data(bytes.len() as u64).map_err(std::io::Error::other)?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }
        serde_json::to_writer(StringCounter(self), value)?;
        self.retain(value.len())?;
        self.charge(JsonCodecCharge::Copy { bytes: value.len() as u64 })?;
        let mut output = String::new();
        output.try_reserve_exact(value.len()).map_err(Self::error)?;
        output.push_str(value);
        Ok(output)
    }
}

struct Counter(u64);
impl std::io::Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.checked_add(bytes.len() as u64).ok_or_else(|| std::io::Error::other("JSON byte count overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}
fn encoded_length<T: Serialize + ?Sized>(value: &T) -> serde_json::Result<u64> {
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

struct Encoder<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> { meter: &'a mut Meter<'b, F>, depth: u32 }
struct Sequence<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> { encoder: Encoder<'a, 'b, F>, values: Vec<Value>, tag: Option<String> }
struct Object<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> { encoder: Encoder<'a, 'b, F>, values: serde_json::Map<String, Value>, key: Option<String>, tag: Option<String>, raw: bool, raw_value: Option<Value> }

type JsonResult<T> = serde_json::Result<T>;

macro_rules! scalar {
    ($name:ident, $ty:ty) => {
        fn $name(self, value: $ty) -> JsonResult<Value> {
            self.meter.item(self.depth)?;
            let value = ser::Serializer::$name(serde_json::value::Serializer, value)?;
            self.meter.data(encoded_length(&value)?)?;
            Ok(value)
        }
    };
}

impl<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> ser::Serializer for Encoder<'a, 'b, F> {
    type Ok = Value;
    type Error = serde_json::Error;
    type SerializeSeq = Sequence<'a, 'b, F>;
    type SerializeTuple = Sequence<'a, 'b, F>;
    type SerializeTupleStruct = Sequence<'a, 'b, F>;
    type SerializeTupleVariant = Sequence<'a, 'b, F>;
    type SerializeMap = Object<'a, 'b, F>;
    type SerializeStruct = Object<'a, 'b, F>;
    type SerializeStructVariant = Object<'a, 'b, F>;
    scalar!(serialize_bool, bool);
    scalar!(serialize_i8, i8);
    scalar!(serialize_i16, i16);
    scalar!(serialize_i32, i32);
    scalar!(serialize_i64, i64);
    scalar!(serialize_i128, i128);
    scalar!(serialize_u8, u8);
    scalar!(serialize_u16, u16);
    scalar!(serialize_u32, u32);
    scalar!(serialize_u64, u64);
    scalar!(serialize_u128, u128);
    scalar!(serialize_f32, f32);
    scalar!(serialize_f64, f64);
    fn serialize_char(self, value: char) -> JsonResult<Value> { self.serialize_str(value.encode_utf8(&mut [0; 4])) }
    fn serialize_str(self, value: &str) -> JsonResult<Value> {
        self.meter.item(self.depth)?;
        Ok(Value::String(self.meter.string(value)?))
    }
    fn serialize_bytes(self, value: &[u8]) -> JsonResult<Value> {
        use ser::SerializeSeq;
        let mut seq = self.serialize_seq(Some(value.len()))?;
        for value in value { seq.serialize_element(value)?; }
        seq.end()
    }
    fn serialize_none(self) -> JsonResult<Value> { self.serialize_unit() }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> JsonResult<Value> { value.serialize(self) }
    fn serialize_unit(self) -> JsonResult<Value> {
        self.meter.item(self.depth)?;
        self.meter.data(4)?;
        Ok(Value::Null)
    }
    fn serialize_unit_struct(self, _: &'static str) -> JsonResult<Value> { self.serialize_unit() }
    fn serialize_unit_variant(self, _: &'static str, _: u32, variant: &'static str) -> JsonResult<Value> { self.serialize_str(variant) }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, value: &T) -> JsonResult<Value> { value.serialize(self) }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(self, _: &'static str, _: u32, variant: &'static str, value: &T) -> JsonResult<Value> {
        use ser::SerializeMap;
        let mut map = self.serialize_map(Some(1))?;
        map.serialize_entry(variant, value)?;
        map.end()
    }
    fn serialize_seq(self, _: Option<usize>) -> JsonResult<Self::SerializeSeq> {
        self.meter.item(self.depth)?;
        self.meter.data(2)?;
        Ok(Sequence { encoder: self, values: Vec::new(), tag: None })
    }
    fn serialize_tuple(self, len: usize) -> JsonResult<Self::SerializeTuple> { self.serialize_seq(Some(len)) }
    fn serialize_tuple_struct(self, _: &'static str, len: usize) -> JsonResult<Self::SerializeTupleStruct> { self.serialize_seq(Some(len)) }
    fn serialize_tuple_variant(self, _: &'static str, _: u32, variant: &'static str, len: usize) -> JsonResult<Self::SerializeTupleVariant> {
        self.meter.item(self.depth)?;
        self.meter.data(3)?;
        let tag = self.meter.string(variant)?;
        self.meter.retain(std::mem::size_of::<(String, Value)>())?;
        let mut seq = Encoder { meter: self.meter, depth: self.depth + 1 }.serialize_seq(Some(len))?;
        seq.tag = Some(tag);
        Ok(seq)
    }
    fn serialize_map(self, _: Option<usize>) -> JsonResult<Self::SerializeMap> {
        self.meter.item(self.depth)?;
        self.meter.data(2)?;
        Ok(Object { encoder: self, values: serde_json::Map::new(), key: None, tag: None, raw: false, raw_value: None })
    }
    fn serialize_struct(self, name: &'static str, len: usize) -> JsonResult<Self::SerializeStruct> {
        if name == "$serde_json::private::RawValue" {
            Ok(Object { encoder: self, values: serde_json::Map::new(), key: None, tag: None, raw: true, raw_value: None })
        } else {
            self.serialize_map(Some(len))
        }
    }
    fn serialize_struct_variant(self, _: &'static str, _: u32, variant: &'static str, len: usize) -> JsonResult<Self::SerializeStructVariant> {
        self.meter.item(self.depth)?;
        self.meter.data(3)?;
        let tag = self.meter.string(variant)?;
        self.meter.retain(std::mem::size_of::<(String, Value)>())?;
        let mut map = Encoder { meter: self.meter, depth: self.depth + 1 }.serialize_map(Some(len))?;
        map.tag = Some(tag);
        Ok(map)
    }
    fn collect_str<T: std::fmt::Display + ?Sized>(self, value: &T) -> JsonResult<Value> {
        // Display has no borrowed string interface. Stream it through the same
        // bounded string builder rather than serde's unbounded format! default.
        self.meter.item(self.depth)?;
        let mut text = BoundedText { meter: self.meter, text: String::new(), error: None };
        if std::fmt::write(&mut text, format_args!("{value}")).is_err() {
            return Err(text.error.unwrap_or_else(|| Meter::<F>::error("Display failed")));
        }
        let value = Value::String(text.meter.string(&text.text)?);
        let released = text.text.len() as u64;
        drop(text.text);
        text.meter.release(released)?;
        Ok(value)
    }
}

struct BoundedText<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> { meter: &'a mut Meter<'b, F>, text: String, error: Option<serde_json::Error> }
impl<F: FnMut(JsonCodecCharge) -> Result<()>> std::fmt::Write for BoundedText<'_, '_, F> {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        let result = (|| {
            // This transient formatting buffer is separate retained/work data;
            // final escaped bytes are charged by serialize_str.
            if (self.text.len() as u64).checked_add(value.len() as u64).is_none_or(|size| size > self.meter.bounds.maximum_bytes) {
                return Err(Meter::<F>::error("JSON exceeds declared byte bound"));
            }
            self.meter.charge(JsonCodecCharge::Work { bytes: value.len() as u64, items: 0 })?;
            self.meter.retain(value.len())?;
            self.text.try_reserve_exact(value.len()).map_err(Meter::<F>::error)?;
            self.text.push_str(value);
            Ok(())
        })();
        result.map_err(|error| { self.error = Some(error); std::fmt::Error })
    }
}

impl<F: FnMut(JsonCodecCharge) -> Result<()>> Sequence<'_, '_, F> {
    fn element<T: Serialize + ?Sized>(&mut self, value: &T) -> JsonResult<()> {
        if !self.values.is_empty() { self.encoder.meter.data(1)?; }
        if self.encoder.meter.items >= self.encoder.meter.bounds.maximum_items {
            return Err(Meter::<F>::error("JSON exceeds declared structural bound"));
        }
        // Geometric, explicitly admitted growth; ignore untrusted serde hints.
        if self.values.len() == self.values.capacity() {
            let remaining = self.encoder.meter.bounds.maximum_items - self.encoder.meter.items;
            let growth = self.values.capacity().max(1).min(usize::try_from(remaining).unwrap_or(usize::MAX));
            let bytes = growth.checked_mul(std::mem::size_of::<Value>()).ok_or_else(|| Meter::<F>::error("JSON capacity overflow"))?;
            self.encoder.meter.retain(bytes)?;
            self.values.try_reserve_exact(growth).map_err(Meter::<F>::error)?;
        }
        self.values.push(value.serialize(Encoder { meter: self.encoder.meter, depth: self.encoder.depth + 1 })?);
        Ok(())
    }
    fn finish(self) -> Value {
        let value = Value::Array(self.values);
        match self.tag { Some(tag) => Value::Object([(tag, value)].into_iter().collect()), None => value }
    }
}
macro_rules! sequence {
    ($trait:ident, $method:ident) => {
        impl<F: FnMut(JsonCodecCharge) -> Result<()>> ser::$trait for Sequence<'_, '_, F> {
            type Ok = Value; type Error = serde_json::Error;
            fn $method<T: Serialize + ?Sized>(&mut self, value: &T) -> JsonResult<()> { self.element(value) }
            fn end(self) -> JsonResult<Value> { Ok(self.finish()) }
        }
    };
}
sequence!(SerializeSeq, serialize_element);
sequence!(SerializeTuple, serialize_element);
sequence!(SerializeTupleStruct, serialize_field);
sequence!(SerializeTupleVariant, serialize_field);

impl<F: FnMut(JsonCodecCharge) -> Result<()>> Object<'_, '_, F> {
    fn entry<T: Serialize + ?Sized>(&mut self, key: &str, value: &T) -> JsonResult<()> {
        self.encoder.meter.item(self.encoder.depth + 1)?;
        let key = self.encoder.meter.string(key)?;
        self.key = Some(key);
        ser::SerializeMap::serialize_value(self, value)
    }
    fn finish(self) -> JsonResult<Value> {
        if self.raw { return self.raw_value.ok_or_else(|| Meter::<F>::error("expected value")); }
        if self.key.is_some() { return Err(Meter::<F>::error("map value is missing")); }
        let value = Value::Object(self.values);
        Ok(match self.tag { Some(tag) => Value::Object([(tag, value)].into_iter().collect()), None => value })
    }
}
impl<F: FnMut(JsonCodecCharge) -> Result<()>> ser::SerializeMap for Object<'_, '_, F> {
    type Ok = Value; type Error = serde_json::Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> JsonResult<()> {
        self.encoder.meter.item(self.encoder.depth + 1)?;
        self.key = Some(key.serialize(Key { meter: self.encoder.meter })?);
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> JsonResult<()> {
        let key = self.key.take().ok_or_else(|| Meter::<F>::error("map key is missing"))?;
        if self.encoder.meter.items >= self.encoder.meter.bounds.maximum_items {
            return Err(Meter::<F>::error("JSON exceeds declared structural bound"));
        }
        self.encoder.meter.data(if self.values.is_empty() { 1 } else { 2 })?;
        let duplicate = self.values.contains_key(&key);
        if !duplicate { self.encoder.meter.retain(std::mem::size_of::<(String, Value)>())?; }
        let value = value.serialize(Encoder { meter: self.encoder.meter, depth: self.encoder.depth + 1 })?;
        if duplicate {
            let (old_key, old_value) = self.values.remove_entry(&key).expect("known duplicate key");
            let released = old_key.len() as u64 + owned_logical_bytes(&old_value);
            drop(old_key);
            drop(old_value);
            self.encoder.meter.release(released)?;
        }
        self.values.insert(key, value);
        Ok(())
    }
    fn end(self) -> JsonResult<Value> { self.finish() }
}
macro_rules! object {
    ($trait:ident) => {
        impl<F: FnMut(JsonCodecCharge) -> Result<()>> ser::$trait for Object<'_, '_, F> {
            type Ok = Value; type Error = serde_json::Error;
            fn serialize_field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> JsonResult<()> { self.entry(key, value) }
            fn end(self) -> JsonResult<Value> { self.finish() }
        }
    };
}
impl<F: FnMut(JsonCodecCharge) -> Result<()>> ser::SerializeStruct for Object<'_, '_, F> {
    type Ok = Value; type Error = serde_json::Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> JsonResult<()> {
        if self.raw {
            if key != "$serde_json::private::RawValue" { return Err(Meter::<F>::error("expected value")); }
            self.raw_value = Some(value.serialize(Raw { encoder: Encoder { meter: self.encoder.meter, depth: self.encoder.depth } })?);
            Ok(())
        } else { self.entry(key, value) }
    }
    fn end(self) -> JsonResult<Value> { self.finish() }
}
object!(SerializeStructVariant);

struct Key<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> { meter: &'a mut Meter<'b, F> }
struct Stack { bytes: [u8; 64], len: usize }
impl std::io::Write for Stack {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.bytes.len() - self.len { return Err(std::io::Error::other("scalar key exceeds stack buffer")); }
        self.bytes[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}
macro_rules! key_scalar {
    ($method:ident, $ty:ty $(, $finite:expr)?) => {
        fn $method(self, value: $ty) -> JsonResult<String> {
            $(if !($finite)(value) { return Err(Meter::<F>::error("float key must be finite (got NaN or +/-inf)")); })?
            // Same primitive formatter as serde_json's private map-key codec,
            // but fixed stack storage avoids a pre-admission temporary map.
            struct Scalar($ty);
            impl Serialize for Scalar {
                fn serialize<S: ser::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> { serializer.$method(self.0) }
            }
            let mut stack = Stack { bytes: [0; 64], len: 0 };
            serde_json::to_writer(&mut stack, &Scalar(value))?;
            self.meter.string(std::str::from_utf8(&stack.bytes[..stack.len]).expect("JSON scalar is UTF-8"))
        }
    };
}
macro_rules! bad_key {
    ($method:ident $(, $ty:ty)?) => {
        fn $method(self $(, _: $ty)?) -> JsonResult<String> { Err(Meter::<F>::error("key must be a string")) }
    };
}
impl<F: FnMut(JsonCodecCharge) -> Result<()>> ser::Serializer for Key<'_, '_, F> {
    type Ok = String; type Error = serde_json::Error;
    type SerializeSeq = ser::Impossible<String, Self::Error>;
    type SerializeTuple = ser::Impossible<String, Self::Error>;
    type SerializeTupleStruct = ser::Impossible<String, Self::Error>;
    type SerializeTupleVariant = ser::Impossible<String, Self::Error>;
    type SerializeMap = ser::Impossible<String, Self::Error>;
    type SerializeStruct = ser::Impossible<String, Self::Error>;
    type SerializeStructVariant = ser::Impossible<String, Self::Error>;
    key_scalar!(serialize_bool, bool);
    key_scalar!(serialize_i8, i8); key_scalar!(serialize_i16, i16); key_scalar!(serialize_i32, i32); key_scalar!(serialize_i64, i64); key_scalar!(serialize_i128, i128);
    key_scalar!(serialize_u8, u8); key_scalar!(serialize_u16, u16); key_scalar!(serialize_u32, u32); key_scalar!(serialize_u64, u64); key_scalar!(serialize_u128, u128);
    key_scalar!(serialize_f32, f32, |value: f32| value.is_finite());
    key_scalar!(serialize_f64, f64, |value: f64| value.is_finite());
    fn serialize_char(self, value: char) -> JsonResult<String> { self.serialize_str(value.encode_utf8(&mut [0; 4])) }
    fn serialize_str(self, value: &str) -> JsonResult<String> { self.meter.string(value) }
    bad_key!(serialize_bytes, &[u8]); bad_key!(serialize_none); bad_key!(serialize_unit);
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> JsonResult<String> { Err(Meter::<F>::error("key must be a string")) }
    fn serialize_unit_struct(self, _: &'static str) -> JsonResult<String> { self.serialize_unit() }
    fn serialize_unit_variant(self, _: &'static str, _: u32, variant: &'static str) -> JsonResult<String> { self.serialize_str(variant) }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, value: &T) -> JsonResult<String> { value.serialize(self) }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(self, _: &'static str, _: u32, _: &'static str, _: &T) -> JsonResult<String> { self.serialize_unit() }
    fn serialize_seq(self, _: Option<usize>) -> JsonResult<Self::SerializeSeq> { Err(Meter::<F>::error("key must be a string")) }
    fn serialize_tuple(self, _: usize) -> JsonResult<Self::SerializeTuple> { Err(Meter::<F>::error("key must be a string")) }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> JsonResult<Self::SerializeTupleStruct> { Err(Meter::<F>::error("key must be a string")) }
    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> JsonResult<Self::SerializeTupleVariant> { Err(Meter::<F>::error("key must be a string")) }
    fn serialize_map(self, _: Option<usize>) -> JsonResult<Self::SerializeMap> { Err(Meter::<F>::error("key must be a string")) }
    fn serialize_struct(self, _: &'static str, _: usize) -> JsonResult<Self::SerializeStruct> { Err(Meter::<F>::error("key must be a string")) }
    fn serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> JsonResult<Self::SerializeStructVariant> { Err(Meter::<F>::error("key must be a string")) }
    fn collect_str<T: std::fmt::Display + ?Sized>(self, value: &T) -> JsonResult<String> {
        let mut text = BoundedText { meter: self.meter, text: String::new(), error: None };
        if std::fmt::write(&mut text, format_args!("{value}")).is_err() {
            return Err(text.error.unwrap_or_else(|| Meter::<F>::error("Display failed")));
        }
        text.meter.data(encoded_length(&text.text)?)?;
        Ok(text.text)
    }
}

/// Allocates only after each logical payload/item has passed admission. A
/// canonical tree is necessary for sorted maps and last-duplicate-key wins;
/// streaming serde_json directly would change f32 and duplicate-key identities.
/// Byte admission counts all emitted entries (including discarded duplicates).
pub fn canonical_json_bytes_bounded<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, charge: &mut F,
) -> Result<Vec<u8>> {
    encode(value, JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds }, charge)
}

pub(super) fn canonical_json_bytes_trusted<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    encode(value, JsonBounds::IDENTITY, &mut |_| Ok(()))
}

fn encode<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, charge: &mut F,
) -> Result<Vec<u8>> {
    let mut meter = Meter { bounds, items: 0, bytes: 0, charge, failure: None, retained: 0 };
    let mut value = match value.serialize(Encoder { meter: &mut meter, depth: 0 }) {
        Ok(value) => value,
        Err(error) => return Err(meter.failure.take().unwrap_or_else(|| Error::Invalid(error.to_string()))),
    };
    if let Some(error) = meter.failure.take() { return Err(error); }
    // serde_json::Map may use insertion-order storage. Explicit sorting pins
    // the same raw UTF-8 key ordering for all feature configurations.
    (meter.charge)(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    value.sort_all_objects();
    let size = encoded_length(&value).map_err(|error| Error::Invalid(error.to_string()))?;
    if size > bounds.maximum_bytes { return Err(Error::Invalid("JSON exceeds declared byte bound".into())); }
    let size = usize::try_from(size).map_err(|_| Error::Invalid("JSON output exceeds address space".into()))?;
    if let Err(error) = meter.retain(size) {
        return Err(meter.failure.take().unwrap_or_else(|| Error::Invalid(error.to_string())));
    }
    (meter.charge)(JsonCodecCharge::Work { bytes: size as u64, items: 0 })?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(size).map_err(|error| Error::Invalid(error.to_string()))?;
    struct Output<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> { bytes: &'a mut Vec<u8>, maximum: usize, meter: &'a mut Meter<'b, F> }
    impl<F: FnMut(JsonCodecCharge) -> Result<()>> std::io::Write for Output<'_, '_, F> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.maximum - self.bytes.len() { return Err(std::io::Error::other("JSON exceeds admitted output")); }
            self.meter.charge(JsonCodecCharge::Work { bytes: bytes.len() as u64, items: 0 }).map_err(std::io::Error::other)?;
            self.meter.charge(JsonCodecCharge::Copy { bytes: bytes.len() as u64 }).map_err(std::io::Error::other)?;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    if let Err(error) = serde_json::to_writer(Output { bytes: &mut bytes, maximum: size, meter: &mut meter }, &value) {
        return Err(meter.failure.take().unwrap_or_else(|| Error::Invalid(error.to_string())));
    }
    let released = meter.retained - size as u64;
    drop(value);
    if let Err(error) = meter.release(released) {
        return Err(meter.failure.take().unwrap_or_else(|| Error::Invalid(error.to_string())));
    }
    // Transfer only the returned output reservation to the caller. Every
    // temporary reservation has already been released after its data was freed.
    meter.retained = 0;
    Ok(bytes)
}

/// Allocation-free lexical admission before serde scratch/collections grow.
#[derive(Default)]
struct Scan {
    bytes: u64,
    depth: u32,
    items: u64,
    string: bool,
    escape: bool,
    atom: bool,
}
impl Scan {
    fn push<F: FnMut(JsonCodecCharge) -> Result<()>>(&mut self, bytes: &[u8], bounds: JsonBounds, charge: &mut F) -> Result<()> {
        for &byte in bytes {
            if self.bytes >= bounds.maximum_bytes { return Err(Error::Invalid("JSON input exceeds declared byte bound".into())); }
            charge(JsonCodecCharge::Read { bytes: 1 })?;
            self.bytes += 1;
            if self.string {
                if self.escape { self.escape = false; }
                else if byte == b'\\' { self.escape = true; }
                else if byte == b'"' { self.string = false; }
                continue;
            }
            let item_depth = self.depth;
            let item = match byte {
                b'"' => { self.string = true; self.atom = false; true }
                b'[' | b'{' => { self.depth += 1; self.atom = false; true }
                b']' | b'}' => { self.depth = self.depth.checked_sub(1).ok_or_else(|| Error::Invalid("invalid JSON nesting".into()))?; self.atom = false; false }
                b':' | b',' | b' ' | b'\r' | b'\n' | b'\t' => { self.atom = false; false }
                _ => { let first = !self.atom; self.atom = true; first }
            };
            if item {
                if item_depth > bounds.maximum_depth || self.items >= bounds.maximum_items {
                    return Err(Error::Invalid("JSON exceeds declared structural bound".into()));
                }
                charge(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                self.items += 1;
            }
        }
        Ok(())
    }
    fn end(&self) -> Result<()> {
        if self.string || self.depth != 0 { return Err(Error::Invalid("incomplete JSON document".into())); }
        Ok(())
    }
}

/// Bounds lexical tokens, including duplicate keys. Complete grammar, UTF-8 and
/// numeric validation follows in the typed serde decoder.
pub(crate) fn admit_json<F: FnMut(JsonCodecCharge) -> Result<()>>(bytes: &[u8], bounds: JsonBounds, charge: &mut F) -> Result<()> {
    admit_json_trusted(bytes, JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds }, charge)
}

pub(super) fn admit_json_trusted<F: FnMut(JsonCodecCharge) -> Result<()>>(bytes: &[u8], bounds: JsonBounds, charge: &mut F) -> Result<()> {
    if bytes.len() as u64 > bounds.maximum_bytes { return Err(Error::Invalid("JSON input exceeds declared byte bound".into())); }
    let mut scan = Scan::default();
    scan.push(bytes, bounds, charge)?;
    scan.end()
}

/// Existing noncanonical record bytes (not identities). Serialize must be
/// trusted and deterministic across these two passes. Neither pass constructs
/// a Value tree; emitted bytes/items/depth are checked before owned output
/// growth. Inconsistent second-pass serialization errors rather than reallocs.
pub fn json_bytes_bounded_trusted<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, charge: &mut F,
) -> Result<Vec<u8>> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    let mut meter = Meter { bounds, items: 0, bytes: 0, charge, failure: None, retained: 0 };
    struct Writer<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> {
        scan: Scan,
        bounds: JsonBounds,
        meter: &'a mut Meter<'b, F>,
        output: Option<&'a mut Vec<u8>>,
        maximum: usize,
    }
    impl<F: FnMut(JsonCodecCharge) -> Result<()>> std::io::Write for Writer<'_, '_, F> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let meter = &mut self.meter;
            if let Err(error) = self.scan.push(bytes, self.bounds, &mut |event| {
                meter.charge(match event {
                    JsonCodecCharge::Read { bytes } => JsonCodecCharge::Work { bytes, items: 0 },
                    other => other,
                }).map_err(|error| Error::Invalid(error.to_string()))
            }) {
                if meter.failure.is_none() { meter.failure = Some(error); }
                return Err(std::io::Error::other("JSON admission interrupted"));
            }
            if let Some(output) = self.output.as_mut() {
                if bytes.len() > self.maximum - output.len() {
                    return Err(std::io::Error::other("JSON exceeds admitted output"));
                }
                meter.charge(JsonCodecCharge::Copy { bytes: bytes.len() as u64 }).map_err(std::io::Error::other)?;
                output.extend_from_slice(bytes);
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let size = {
        let mut first = Writer { scan: Scan::default(), bounds, meter: &mut meter, output: None, maximum: 0 };
        if let Err(error) = serde_json::to_writer(&mut first, value) {
            return Err(first.meter.failure.take().unwrap_or_else(|| Error::Invalid(error.to_string())));
        }
        if let Some(error) = first.meter.failure.take() { return Err(error); }
        first.scan.end()?;
        usize::try_from(first.scan.bytes).map_err(|_| Error::Invalid("JSON output exceeds address space".into()))?
    };
    if let Err(error) = meter.retain(size) {
        return Err(meter.failure.take().unwrap_or_else(|| Error::Invalid(error.to_string())));
    }
    let mut output = Vec::new();
    output.try_reserve_exact(size).map_err(|error| Error::Invalid(error.to_string()))?;
    {
        let mut second = Writer { scan: Scan::default(), bounds, meter: &mut meter, output: Some(&mut output), maximum: size };
        if let Err(error) = serde_json::to_writer(&mut second, value) {
            return Err(second.meter.failure.take().unwrap_or_else(|| Error::Invalid(error.to_string())));
        }
        if let Some(error) = second.meter.failure.take() { return Err(error); }
        second.scan.end()?;
        if second.scan.bytes != size as u64 {
            return Err(Error::Invalid("JSON serialization changed between admission and encoding".into()));
        }
    }
    meter.retained = 0;
    Ok(output)
}

/// Typed decoding is NOT a generic heap admission proof. Only audited
/// Deserialize implementations may use this boundary. The caller supplies and
/// reserves the concrete type's maximum owned logical bytes BEFORE decoding;
/// its schema must establish that bound from admitted bytes/items. Custom
/// Deserialize (including ignored/flattened/custom fields) is trusted and must
/// not allocate beyond that witness or perform unmetered external work. No
/// allocator-node/RSS bound or arbitrary DeserializeOwned guarantee is made.
pub fn json_from_slice_bounded_trusted<T: serde::de::DeserializeOwned, F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8], bounds: JsonBounds, maximum_owned_bytes: u64, charge: &mut F,
) -> Result<T> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    admit_json(bytes, bounds, charge)?;
    // Vec scratch growth is geometric; reserve its conservative logical
    // capacity, not just string length. Borrowed input itself is not newly owned.
    let scratch = (bytes.len() as u64).max(8).checked_mul(2)
        .ok_or_else(|| Error::Invalid("JSON scratch bound overflow".into()))?;
    let maximum_owned_bytes = maximum_owned_bytes.checked_add(scratch)
        .ok_or_else(|| Error::Invalid("JSON owned bound overflow".into()))?;
    charge(JsonCodecCharge::Admission { maximum_owned_bytes })?;
    charge(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let value = T::deserialize(&mut decoder).map_err(|error| Error::Invalid(error.to_string()))?;
    decoder.end().map_err(|error| Error::Invalid(error.to_string()))?;
    drop(decoder);
    Ok(value)
}

struct Raw<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> { encoder: Encoder<'a, 'b, F> }
macro_rules! bad_raw {
    ($method:ident $(, $ty:ty)?) => {
        fn $method(self $(, _: $ty)?) -> JsonResult<Value> { Err(Meter::<F>::error("expected value")) }
    };
}
impl<F: FnMut(JsonCodecCharge) -> Result<()>> ser::Serializer for Raw<'_, '_, F> {
    type Ok = Value; type Error = serde_json::Error;
    type SerializeSeq = ser::Impossible<Value, Self::Error>;
    type SerializeTuple = ser::Impossible<Value, Self::Error>;
    type SerializeTupleStruct = ser::Impossible<Value, Self::Error>;
    type SerializeTupleVariant = ser::Impossible<Value, Self::Error>;
    type SerializeMap = ser::Impossible<Value, Self::Error>;
    type SerializeStruct = ser::Impossible<Value, Self::Error>;
    type SerializeStructVariant = ser::Impossible<Value, Self::Error>;
    fn serialize_str(self, value: &str) -> JsonResult<Value> {
        let meter = self.encoder.meter;
        let maximum_depth = meter.bounds.maximum_depth.checked_sub(self.encoder.depth)
            .ok_or_else(|| Meter::<F>::error("JSON exceeds declared structural bound"))?;
        let bounds = JsonBounds { maximum_depth, maximum_items: meter.bounds.maximum_items - meter.items, ..meter.bounds };
        let mut scan = Scan::default();
        scan.push(value.as_bytes(), bounds, &mut |event| {
            let items = match event { JsonCodecCharge::Work { items, .. } => items, _ => 0 };
            meter.charge(event).map_err(|error| Error::Invalid(error.to_string()))?;
            meter.items += items;
            Ok(())
        }).map_err(Meter::<F>::error)?;
        scan.end().map_err(Meter::<F>::error)?;
        // RawValue's parser is the audited serde_json Value implementation.
        // String payload totals fit input bytes. Vector storage includes both
        // doubling and up-to-eight initial slots per nonempty array; array
        // count and total element count each fit lexical items. Map entries
        // and geometric scratch are separate from these slots.
        let maximum_owned_bytes = scan.items
            .checked_mul((10 * std::mem::size_of::<Value>() + std::mem::size_of::<(String, Value)>()) as u64)
            .and_then(|nodes| (value.len() as u64).max(8).checked_mul(4).and_then(|strings| nodes.checked_add(strings)))
            .ok_or_else(|| Meter::<F>::error("JSON owned bound overflow"))?;
        meter.charge(JsonCodecCharge::Admission { maximum_owned_bytes })?;
        // Keep serde_json::to_value(RawValue)'s existing number/grammar/recursion
        // semantics, rather than treating its private token as an ordinary map.
        let value: Value = serde_json::from_str(value)?;
        let owned = usize::try_from(owned_logical_bytes(&value)).map_err(Meter::<F>::error)?;
        meter.retain(owned)?;
        meter.data(encoded_length(&value)?)?;
        Ok(value)
    }
    bad_raw!(serialize_bool, bool);
    bad_raw!(serialize_i8, i8); bad_raw!(serialize_i16, i16); bad_raw!(serialize_i32, i32); bad_raw!(serialize_i64, i64); bad_raw!(serialize_i128, i128);
    bad_raw!(serialize_u8, u8); bad_raw!(serialize_u16, u16); bad_raw!(serialize_u32, u32); bad_raw!(serialize_u64, u64); bad_raw!(serialize_u128, u128);
    bad_raw!(serialize_f32, f32); bad_raw!(serialize_f64, f64); bad_raw!(serialize_char, char);
    bad_raw!(serialize_bytes, &[u8]); bad_raw!(serialize_none); bad_raw!(serialize_unit);
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> JsonResult<Value> { self.serialize_unit() }
    fn serialize_unit_struct(self, _: &'static str) -> JsonResult<Value> { self.serialize_unit() }
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> JsonResult<Value> { self.serialize_unit() }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, _: &T) -> JsonResult<Value> { self.serialize_unit() }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(self, _: &'static str, _: u32, _: &'static str, _: &T) -> JsonResult<Value> { self.serialize_unit() }
    fn serialize_seq(self, _: Option<usize>) -> JsonResult<Self::SerializeSeq> { Err(Meter::<F>::error("expected value")) }
    fn serialize_tuple(self, _: usize) -> JsonResult<Self::SerializeTuple> { Err(Meter::<F>::error("expected value")) }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> JsonResult<Self::SerializeTupleStruct> { Err(Meter::<F>::error("expected value")) }
    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> JsonResult<Self::SerializeTupleVariant> { Err(Meter::<F>::error("expected value")) }
    fn serialize_map(self, _: Option<usize>) -> JsonResult<Self::SerializeMap> { Err(Meter::<F>::error("expected value")) }
    fn serialize_struct(self, _: &'static str, _: usize) -> JsonResult<Self::SerializeStruct> { Err(Meter::<F>::error("expected value")) }
    fn serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> JsonResult<Self::SerializeStructVariant> { Err(Meter::<F>::error("expected value")) }
    fn collect_str<T: std::fmt::Display + ?Sized>(self, _: &T) -> JsonResult<Value> { self.serialize_unit() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use serde::ser::SerializeMap as _;

    const BOUNDS: JsonBounds = JsonBounds { maximum_bytes: 65_536, maximum_items: 4_096, maximum_depth: 32 };
    struct Pairs(Vec<(String, u64)>);
    impl Serialize for Pairs {
        fn serialize<S: ser::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
            let mut map = serializer.serialize_map(Some(usize::MAX))?;
            for (key, value) in &self.0 { map.serialize_entry(key, value)?; }
            map.end()
        }
    }

    #[test]
    fn exact_byte_item_depth_and_escape_admission() -> Result<()> {
        let value = serde_json::json!({"z": ["\u{0}\"\\\n", u64::MAX], "a": i64::MIN});
        let expected = super::super::canonical_json_bytes(&value)?;
        let bounds = JsonBounds { maximum_bytes: expected.len() as u64, maximum_items: 7, maximum_depth: 2 };
        assert_eq!(canonical_json_bytes_bounded(&value, bounds, &mut |_| Ok(()))?, expected);
        assert!(canonical_json_bytes_bounded(&value, JsonBounds { maximum_bytes: bounds.maximum_bytes - 1, ..bounds }, &mut |_| Ok(())).is_err());
        assert!(canonical_json_bytes_bounded(&value, JsonBounds { maximum_items: 6, ..bounds }, &mut |_| Ok(())).is_err());
        assert!(canonical_json_bytes_bounded(&value, JsonBounds { maximum_depth: 1, ..bounds }, &mut |_| Ok(())).is_err());
        Ok(())
    }

    #[test]
    fn canonical_duplicate_custody_releases_dead_values() -> Result<()> {
        let pairs = Pairs(vec![("x".into(), 1), ("x".into(), 2)]);
        let mut live = 0u64;
        let mut peak = 0u64;
        let bytes = canonical_json_bytes_bounded(&pairs, BOUNDS, &mut |event| {
            match event {
                JsonCodecCharge::Retain { bytes } => { live += bytes; peak = peak.max(live); }
                JsonCodecCharge::Release { bytes } => { live = live.checked_sub(bytes).expect("no double release"); }
                _ => {}
            }
            Ok(())
        })?;
        assert_eq!(bytes, br#"{"x":2}"#);
        assert_eq!(live, bytes.len() as u64);
        assert!(peak > live);
        Ok(())
    }

    #[test]
    fn every_codec_cancellation_schedule_preserves_original_error() -> Result<()> {
        let value = serde_json::json!({"b": ["escape\n", 0.1], "a": {"later": 2, "earlier": 1}});
        let mut calls = 0usize;
        canonical_json_bytes_bounded(&value, BOUNDS, &mut |event| {
            if !matches!(event, JsonCodecCharge::Release { .. }) { calls += 1; }
            Ok(())
        })?;
        for stop in 0..calls {
            let mut seen = 0;
            let mut live = 0u64;
            let error = Error::Conflict(format!("cancel-{stop}"));
            let result = canonical_json_bytes_bounded(&value, BOUNDS, &mut |event| {
                if let JsonCodecCharge::Release { bytes } = event {
                    live = live.checked_sub(bytes).expect("release only admitted quota");
                    return Ok(());
                }
                if seen == stop { return Err(error.clone()); }
                seen += 1;
                if let JsonCodecCharge::Retain { bytes } = event { live += bytes; }
                Ok(())
            });
            assert_eq!(result, Err(error));
            assert_eq!(live, 0, "cancel schedule {stop} leaks live codec quota");
        }
        Ok(())
    }

    #[test]
    fn ordinary_codec_cancellation_releases_admitted_output() -> Result<()> {
        let value = serde_json::json!({"a": "owned output", "b": [1, 2, 3]});
        let mut calls = 0usize;
        json_bytes_bounded_trusted(&value, BOUNDS, &mut |event| {
            if !matches!(event, JsonCodecCharge::Release { .. }) { calls += 1; }
            Ok(())
        })?;
        for stop in 0..calls {
            let mut seen = 0;
            let mut live = 0u64;
            let error = Error::Conflict(format!("ordinary-cancel-{stop}"));
            let result = json_bytes_bounded_trusted(&value, BOUNDS, &mut |event| {
                if let JsonCodecCharge::Release { bytes } = event {
                    live = live.checked_sub(bytes).expect("release only admitted quota");
                    return Ok(());
                }
                if seen == stop { return Err(error.clone()); }
                seen += 1;
                if let JsonCodecCharge::Retain { bytes } = event { live += bytes; }
                Ok(())
            });
            assert_eq!(result, Err(error));
            assert_eq!(live, 0);
        }
        Ok(())
    }

    #[test]
    fn serializer_failure_and_ignored_errors_cannot_leak_or_replace_first_failure() {
        use serde::ser::SerializeSeq as _;
        struct PartialFailure;
        impl Serialize for PartialFailure {
            fn serialize<S: ser::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
                let mut sequence = serializer.serialize_seq(Some(2))?;
                sequence.serialize_element("owned partial tree")?;
                Err(ser::Error::custom("original serializer failure"))
            }
        }
        struct IgnoredErrors;
        impl Serialize for IgnoredErrors {
            fn serialize<S: ser::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
                let mut sequence = serializer.serialize_seq(Some(2))?;
                let _ = sequence.serialize_element("ignored first");
                let _ = sequence.serialize_element("ignored later");
                sequence.end()
            }
        }
        let mut live = 0u64;
        let failure = canonical_json_bytes_bounded(&PartialFailure, BOUNDS, &mut |event| {
            match event {
                JsonCodecCharge::Retain { bytes } => live += bytes,
                JsonCodecCharge::Release { bytes } => live = live.checked_sub(bytes).expect("balanced release"),
                _ => {}
            }
            Ok(())
        });
        assert_eq!(failure, Err(Error::Invalid("original serializer failure".into())));
        assert_eq!(live, 0);
        let first = Error::Conflict("first callback failure".into());
        let mut copy_failures = 0;
        let failure = canonical_json_bytes_bounded(&IgnoredErrors, BOUNDS, &mut |event| {
            match event {
                JsonCodecCharge::Retain { bytes } => live += bytes,
                JsonCodecCharge::Release { bytes } => live = live.checked_sub(bytes).expect("balanced release"),
                JsonCodecCharge::Copy { .. } => {
                    copy_failures += 1;
                    return Err(if copy_failures == 1 { first.clone() } else { Error::Conflict("later failure".into()) });
                }
                _ => {}
            }
            Ok(())
        });
        assert_eq!(failure, Err(first));
        assert_eq!(copy_failures, 1);
        assert_eq!(live, 0);
    }

    #[test]
    fn ordinary_second_pass_error_and_size_change_release_output_quota() {
        use std::cell::Cell;
        struct Changing<'a> { calls: &'a Cell<usize>, fail: bool }
        impl Serialize for Changing<'_> {
            fn serialize<S: ser::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
                let call = self.calls.get();
                self.calls.set(call + 1);
                if call == 0 { serializer.serialize_str("first output") }
                else if self.fail { Err(ser::Error::custom("second pass failure")) }
                else { serializer.serialize_str("short") }
            }
        }
        for fail in [true, false] {
            let calls = Cell::new(0);
            let mut live = 0u64;
            let mut peak = 0;
            let result = json_bytes_bounded_trusted(&Changing { calls: &calls, fail }, BOUNDS, &mut |event| {
                match event {
                    JsonCodecCharge::Retain { bytes } => { live += bytes; peak = peak.max(live); }
                    JsonCodecCharge::Release { bytes } => live = live.checked_sub(bytes).expect("balanced release"),
                    _ => {}
                }
                Ok(())
            });
            assert!(result.is_err());
            assert!(peak > 0);
            assert_eq!(live, 0);
        }
    }

    #[test]
    fn trusted_decoder_rejects_structure_before_schema_allocation() -> Result<()> {
        #[derive(Debug, Deserialize)]
        struct Record { text: String }
        let valid = br#"{"text":"escape\n\u0041"}"#;
        let mut admission = None;
        let record: Record = json_from_slice_bounded_trusted(valid, BOUNDS, 128, &mut |event| {
            if let JsonCodecCharge::Admission { maximum_owned_bytes } = event { admission = Some(maximum_owned_bytes); }
            Ok(())
        })?;
        assert_eq!(record.text, "escape\nA");
        assert_eq!(admission, Some(128 + 2 * valid.len() as u64));
        for bytes in [b"{} {}".as_slice(), b"{\"text\":".as_slice(), b"{\"text\":\"\\q\"}".as_slice(), b"[}".as_slice()] {
            assert!(json_from_slice_bounded_trusted::<Record, _>(bytes, BOUNDS, 128, &mut |_| Ok(())).is_err());
        }
        let deep = format!("{}0{}", "[".repeat(10_000), "]".repeat(10_000));
        let mut admitted = false;
        assert!(json_from_slice_bounded_trusted::<Value, _>(deep.as_bytes(), BOUNDS, 1_024, &mut |event| {
            admitted |= matches!(event, JsonCodecCharge::Admission { .. });
            Ok(())
        }).is_err());
        assert!(!admitted);
        Ok(())
    }

    #[test]
    fn ordinary_record_bytes_and_raw_value_identity_remain_exact() -> Result<()> {
        #[derive(Serialize)]
        struct Record { z: f32, a: u64 }
        let record = Record { z: 0.1, a: u64::MAX };
        let expected = serde_json::to_vec(&record).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(json_bytes_bounded_trusted(&record, BOUNDS, &mut |_| Ok(()))?, expected);
        let raw = serde_json::value::RawValue::from_string(r#"{"z":[0.1,-0.0],"a":18446744073709551615,"z":[2]}"#.into())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let mut old = serde_json::to_value(&raw).map_err(|error| Error::Invalid(error.to_string()))?;
        old.sort_all_objects();
        let expected = serde_json::to_vec(&old).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(canonical_json_bytes_bounded(&raw, BOUNDS, &mut |_| Ok(()))?, expected);
        Ok(())
    }

    proptest::proptest! {
        #[test]
        fn arbitrary_identity_bytes_match_the_existing_value_codec(
            pairs in proptest::collection::vec((proptest::prelude::any::<String>(), proptest::prelude::any::<u64>()), 0..16),
            integers in proptest::collection::vec(proptest::prelude::any::<i64>(), 0..16),
            narrow in proptest::prelude::any::<f32>(),
            wide in proptest::prelude::any::<f64>(),
        ) {
            let value = (Pairs(pairs), integers, narrow, wide);
            let mut old = serde_json::to_value(&value).expect("valid property record");
            old.sort_all_objects();
            let expected = serde_json::to_vec(&old).expect("oracle serialization");
            let actual = canonical_json_bytes_bounded(&value, BOUNDS, &mut |_| Ok(())).expect("admitted property record");
            proptest::prop_assert_eq!(actual, expected);
        }
    }
}
