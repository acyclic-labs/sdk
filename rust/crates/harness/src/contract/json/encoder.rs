use super::*;

pub(super) struct Encoder<'a, 'b> { pub(super) meter: &'a mut Meter<'b>, pub(super) depth: u32 }
pub(super) struct Sequence<'a, 'b> { encoder: Encoder<'a, 'b>, values: Vec<Value>, tag: Option<String> }
pub(super) struct Object<'a, 'b> { encoder: Encoder<'a, 'b>, values: serde_json::Map<String, Value>, key: Option<String>, tag: Option<String>, raw: bool, raw_value: Option<Value> }


macro_rules! scalar {
    ($name:ident, $ty:ty) => {
        fn $name(self, value: $ty) -> JsonResult<Value> {
            self.meter.item(self.depth)?;
            let value = ser::Serializer::$name(serde_json::value::Serializer, value)?;
            let size = self.meter.size(&value)?;
            self.meter.data(size)?;
            Ok(value)
        }
    };
}

impl<'a, 'b> ser::Serializer for Encoder<'a, 'b> {
    type Ok = Value;
    type Error = CodecError;
    type SerializeSeq = Sequence<'a, 'b>;
    type SerializeTuple = Sequence<'a, 'b>;
    type SerializeTupleStruct = Sequence<'a, 'b>;
    type SerializeTupleVariant = Sequence<'a, 'b>;
    type SerializeMap = Object<'a, 'b>;
    type SerializeStruct = Object<'a, 'b>;
    type SerializeStructVariant = Object<'a, 'b>;
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
        self.meter.item(self.depth)?;
        let mut text = BoundedText { meter: self.meter, text: String::new(), error: None };
        if std::fmt::write(&mut text, format_args!("{value}")).is_err() {
            return Err(text.error.unwrap_or_else(|| CodecError::Bound("Display failed")));
        }
        let value = Value::String(text.meter.string(&text.text)?);
        let released = text.text.len() as u64;
        drop(text.text);
        text.meter.release(released)?;
        Ok(value)
    }
}

struct BoundedText<'a, 'b> { meter: &'a mut Meter<'b>, text: String, error: Option<CodecError> }
impl std::fmt::Write for BoundedText<'_, '_> {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        let result = (|| {
            if (self.text.len() as u64).checked_add(value.len() as u64).is_none_or(|size| size > self.meter.bounds.maximum_bytes) {
                return Err(CodecError::Bound("JSON exceeds declared byte bound"));
            }
            self.meter.charge(JsonCodecCharge::Work { bytes: value.len() as u64, items: 0 })?;
            self.meter.retain(value.len())?;
            self.meter.charge(JsonCodecCharge::CopyAdmission { bytes: value.len() as u64 })?;
            let requested = self.text.len().checked_add(value.len())
                .ok_or_else(|| CodecError::Bound("JSON capacity overflow"))?;
            if requested > self.text.capacity() {
                self.meter.allocation(requested)?;
                self.text.try_reserve_exact(value.len()).map_err(CodecError::from)?;
            }
            self.meter.charge(JsonCodecCharge::Copy { bytes: value.len() as u64 })?;
            self.text.push_str(value);
            Ok(())
        })();
        result.map_err(|error| { self.error = Some(error); std::fmt::Error })
    }
}

impl Sequence<'_, '_> {
    fn element<T: Serialize + ?Sized>(&mut self, value: &T) -> JsonResult<()> {
        if !self.values.is_empty() { self.encoder.meter.data(1)?; }
        if self.encoder.meter.items >= self.encoder.meter.bounds.maximum_items {
            return Err(CodecError::Bound("JSON exceeds declared structural bound"));
        }
        if self.values.len() == self.values.capacity() {
            let remaining = self.encoder.meter.bounds.maximum_items - self.encoder.meter.items;
            let growth = self.values.capacity().max(1).min(usize::try_from(remaining).unwrap_or(usize::MAX));
            let bytes = growth.checked_mul(std::mem::size_of::<Value>()).ok_or_else(|| CodecError::Bound("JSON capacity overflow"))?;
            self.encoder.meter.retain(bytes)?;
            let requested = self.values.len().checked_mul(std::mem::size_of::<Value>())
                .and_then(|current| current.checked_add(bytes))
                .ok_or_else(|| CodecError::Bound("JSON capacity overflow"))?;
            self.encoder.meter.allocation(requested)?;
            self.values.try_reserve_exact(growth).map_err(CodecError::from)?;
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
        impl ser::$trait for Sequence<'_, '_> {
            type Ok = Value; type Error = CodecError;
            fn $method<T: Serialize + ?Sized>(&mut self, value: &T) -> JsonResult<()> { self.element(value) }
            fn end(self) -> JsonResult<Value> { Ok(self.finish()) }
        }
    };
}
sequence!(SerializeSeq, serialize_element);
sequence!(SerializeTuple, serialize_element);
sequence!(SerializeTupleStruct, serialize_field);
sequence!(SerializeTupleVariant, serialize_field);

impl Object<'_, '_> {
    fn entry<T: Serialize + ?Sized>(&mut self, key: &str, value: &T) -> JsonResult<()> {
        self.encoder.meter.item(self.encoder.depth + 1)?;
        let key = self.encoder.meter.string(key)?;
        self.key = Some(key);
        ser::SerializeMap::serialize_value(self, value)
    }
    fn finish(self) -> JsonResult<Value> {
        if self.raw { return self.raw_value.ok_or_else(|| CodecError::Bound("expected value")); }
        if self.key.is_some() { return Err(CodecError::Bound("map value is missing")); }
        let value = Value::Object(self.values);
        Ok(match self.tag { Some(tag) => Value::Object([(tag, value)].into_iter().collect()), None => value })
    }
}
impl ser::SerializeMap for Object<'_, '_> {
    type Ok = Value; type Error = CodecError;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> JsonResult<()> {
        self.encoder.meter.item(self.encoder.depth + 1)?;
        self.key = Some(key.serialize(Key { meter: self.encoder.meter })?);
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> JsonResult<()> {
        let key = self.key.take().ok_or_else(|| CodecError::Bound("map key is missing"))?;
        if self.encoder.meter.items >= self.encoder.meter.bounds.maximum_items {
            return Err(CodecError::Bound("JSON exceeds declared structural bound"));
        }
        self.encoder.meter.data(if self.values.is_empty() { 1 } else { 2 })?;
        let duplicate = self.values.contains_key(&key);
        if !duplicate { self.encoder.meter.retain(std::mem::size_of::<(String, Value)>())?; }
        let value = value.serialize(Encoder { meter: self.encoder.meter, depth: self.encoder.depth + 1 })?;
        if duplicate {
            let (old_key, old_value) = self.values.remove_entry(&key).expect("known duplicate key");
            let released = old_key.capacity() as u64 + owned_logical_bytes(&old_value);
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
        impl ser::$trait for Object<'_, '_> {
            type Ok = Value; type Error = CodecError;
            fn serialize_field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> JsonResult<()> { self.entry(key, value) }
            fn end(self) -> JsonResult<Value> { self.finish() }
        }
    };
}
impl ser::SerializeStruct for Object<'_, '_> {
    type Ok = Value; type Error = CodecError;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> JsonResult<()> {
        if self.raw {
            if key != "$serde_json::private::RawValue" { return Err(CodecError::Bound("expected value")); }
            self.raw_value = Some(value.serialize(Raw { encoder: Encoder { meter: self.encoder.meter, depth: self.encoder.depth } })?);
            Ok(())
        } else { self.entry(key, value) }
    }
    fn end(self) -> JsonResult<Value> { self.finish() }
}
object!(SerializeStructVariant);

struct Key<'a, 'b> { meter: &'a mut Meter<'b> }
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
            $(if !($finite)(value) { return Err(CodecError::Bound("float key must be finite (got NaN or +/-inf)")); })?
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
        fn $method(self $(, _: $ty)?) -> JsonResult<String> { Err(CodecError::Bound("key must be a string")) }
    };
}
impl ser::Serializer for Key<'_, '_> {
    type Ok = String; type Error = CodecError;
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
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> JsonResult<String> { Err(CodecError::Bound("key must be a string")) }
    fn serialize_unit_struct(self, _: &'static str) -> JsonResult<String> { self.serialize_unit() }
    fn serialize_unit_variant(self, _: &'static str, _: u32, variant: &'static str) -> JsonResult<String> { self.serialize_str(variant) }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, value: &T) -> JsonResult<String> { value.serialize(self) }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(self, _: &'static str, _: u32, _: &'static str, _: &T) -> JsonResult<String> { self.serialize_unit() }
    fn serialize_seq(self, _: Option<usize>) -> JsonResult<Self::SerializeSeq> { Err(CodecError::Bound("key must be a string")) }
    fn serialize_tuple(self, _: usize) -> JsonResult<Self::SerializeTuple> { Err(CodecError::Bound("key must be a string")) }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> JsonResult<Self::SerializeTupleStruct> { Err(CodecError::Bound("key must be a string")) }
    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> JsonResult<Self::SerializeTupleVariant> { Err(CodecError::Bound("key must be a string")) }
    fn serialize_map(self, _: Option<usize>) -> JsonResult<Self::SerializeMap> { Err(CodecError::Bound("key must be a string")) }
    fn serialize_struct(self, _: &'static str, _: usize) -> JsonResult<Self::SerializeStruct> { Err(CodecError::Bound("key must be a string")) }
    fn serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> JsonResult<Self::SerializeStructVariant> { Err(CodecError::Bound("key must be a string")) }
    fn collect_str<T: std::fmt::Display + ?Sized>(self, value: &T) -> JsonResult<String> {
        let mut text = BoundedText { meter: self.meter, text: String::new(), error: None };
        if std::fmt::write(&mut text, format_args!("{value}")).is_err() {
            return Err(text.error.unwrap_or_else(|| CodecError::Bound("Display failed")));
        }
        let size = text.meter.size(&text.text)?;
        text.meter.data(size)?;
        Ok(text.text)
    }
}

struct Raw<'a, 'b> { encoder: Encoder<'a, 'b> }
macro_rules! bad_raw {
    ($method:ident $(, $ty:ty)?) => {
        fn $method(self $(, _: $ty)?) -> JsonResult<Value> { Err(CodecError::Bound("expected value")) }
    };
}
impl ser::Serializer for Raw<'_, '_> {
    type Ok = Value; type Error = CodecError;
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
            .ok_or_else(|| CodecError::Bound("JSON exceeds declared structural bound"))?;
        let bounds = JsonBounds { maximum_depth, maximum_items: meter.bounds.maximum_items - meter.items, ..meter.bounds };
        let schema = if meter.bounds.maximum_depth == u32::MAX { None } else { Some(JsonValueSchema::Root) };
        let mut scan = Scan::with_value_schema(schema, bounds);
        scan.push_inline(value.as_bytes(), bounds, &mut |event| {
            let items = match event { JsonCodecCharge::Work { items, .. } => items, _ => 0 };
            meter.charge(event).map_err(|error| meter.root_error(error))?;
            meter.items += items;
            Ok(())
        }).map_err(|error| meter.forward(error))?;
        let value_shape = scan.value_shape().map_err(|error| meter.forward(error))?;
        meter.items = meter.items.checked_add(value_shape.virtual_items)
            .ok_or_else(|| CodecError::Bound("JSON item count overflow"))?;
        let maximum_owned_bytes = value_storage_bytes(value_shape)
            .and_then(|bytes| value_shape.input_bytes.max(8).checked_mul(4).and_then(|strings| bytes.checked_add(strings)))
            .ok_or(CodecError::Bound("JSON owned bound overflow"))?;
        meter.charge(JsonCodecCharge::Admission { maximum_owned_bytes, value_shape })?;
        let value: Value = serde_json::from_str(value)?;
        let owned = usize::try_from(owned_logical_bytes(&value))
            .map_err(|_| CodecError::Bound("JSON owned bound overflow"))?;
        meter.retain(owned)?;
        let size = meter.size(&value)?;
        meter.data(size)?;
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
    fn serialize_seq(self, _: Option<usize>) -> JsonResult<Self::SerializeSeq> { Err(CodecError::Bound("expected value")) }
    fn serialize_tuple(self, _: usize) -> JsonResult<Self::SerializeTuple> { Err(CodecError::Bound("expected value")) }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> JsonResult<Self::SerializeTupleStruct> { Err(CodecError::Bound("expected value")) }
    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> JsonResult<Self::SerializeTupleVariant> { Err(CodecError::Bound("expected value")) }
    fn serialize_map(self, _: Option<usize>) -> JsonResult<Self::SerializeMap> { Err(CodecError::Bound("expected value")) }
    fn serialize_struct(self, _: &'static str, _: usize) -> JsonResult<Self::SerializeStruct> { Err(CodecError::Bound("expected value")) }
    fn serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> JsonResult<Self::SerializeStructVariant> { Err(CodecError::Bound("expected value")) }
    fn collect_str<T: std::fmt::Display + ?Sized>(self, _: &T) -> JsonResult<Value> { self.serialize_unit() }
}
