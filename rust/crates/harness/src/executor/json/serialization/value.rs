use super::*;
use serde::ser::{self, SerializeMap, SerializeSeq, SerializeStruct};

#[derive(Debug)]
pub(super) enum Failure {
    Json(serde_json::Error),
    Stopped,
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(error) => error.fmt(f),
            Self::Stopped => f.write_str("JSON admission interrupted"),
        }
    }
}
impl std::error::Error for Failure {}
impl ser::Error for Failure {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self::Json(ser::Error::custom(message))
    }
}
impl From<serde_json::Error> for Failure {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}
type Encoded<T> = std::result::Result<T, Failure>;

pub(super) struct Scope<'a> {
    pub bounds: JsonBounds,
    pub code: Option<crate::wire::ErrorCode>,
    pub charge: &'a mut dyn FnMut(JsonCodecCharge) -> Result<()>,
    pub failed: Option<Error>,
    pub retained: u64,
    pub(super) items: u64,
    bytes: u64,
}
impl Scope<'_> {
    pub fn new(
        bounds: JsonBounds,
        code: Option<crate::wire::ErrorCode>,
        charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
    ) -> Scope<'_> {
        Scope {
            bounds: JsonBounds {
                maximum_depth: bounds.maximum_depth.min(256),
                ..bounds
            },
            code,
            charge,
            failed: None,
            retained: 0,
            items: 0,
            bytes: 0,
        }
    }
    pub fn error(&mut self, error: Failure) -> Error {
        self.failed.take().unwrap_or_else(|| match error {
            Failure::Json(error) => records::decode_error(error, self.code),
            Failure::Stopped => boundary("JSON admission interrupted", self.code),
        })
    }
    fn stop<T>(&mut self, error: Error) -> Encoded<T> {
        if self.failed.is_none() {
            self.failed = Some(error);
        }
        Err(Failure::Stopped)
    }
    pub(super) fn bound<T>(&mut self, reason: &'static str) -> Encoded<T> {
        self.stop(boundary(reason, self.code))
    }
    pub fn event(&mut self, event: JsonCodecCharge) -> Encoded<()> {
        if self.failed.is_some() && !matches!(event, JsonCodecCharge::Release { .. }) {
            return Err(Failure::Stopped);
        }
        match (self.charge)(event) {
            Ok(()) => Ok(()),
            Err(error) => self.stop(error),
        }
    }
    fn item(&mut self, depth: u32) -> Encoded<()> {
        if depth > self.bounds.maximum_depth || self.items >= self.bounds.maximum_items {
            return self.bound("JSON exceeds declared structural bound");
        }
        self.event(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        self.items += 1;
        Ok(())
    }
    fn data(&mut self, bytes: u64) -> Encoded<()> {
        let Some(total) = self.bytes.checked_add(bytes) else {
            return self.bound("JSON byte count overflow");
        };
        if total > self.bounds.maximum_bytes {
            return self.bound("JSON exceeds declared byte bound");
        }
        self.bytes = total;
        Ok(())
    }
    pub fn retain(&mut self, bytes: u64) -> Encoded<()> {
        let Some(retained) = self.retained.checked_add(bytes) else {
            return self.bound("JSON retention overflow");
        };
        self.event(JsonCodecCharge::Retain { bytes })?;
        self.retained = retained;
        Ok(())
    }
    pub fn release(&mut self, bytes: u64) -> Encoded<()> {
        let Some(retained) = self.retained.checked_sub(bytes) else {
            return self.bound("JSON retention accounting differs");
        };
        self.event(JsonCodecCharge::Release { bytes })?;
        self.retained = retained;
        Ok(())
    }
    pub fn allocate(&mut self, bytes: u64) -> Encoded<()> {
        if bytes != 0 {
            self.event(JsonCodecCharge::AllocationOperation { bytes })?;
        }
        Ok(())
    }
    fn size<T: Serialize + ?Sized>(&mut self, value: &T) -> Encoded<u64> {
        struct Counter<'a, 'b> {
            scope: &'a mut Scope<'b>,
            bytes: u64,
        }
        impl std::io::Write for Counter<'_, '_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.scope
                    .event(JsonCodecCharge::Work {
                        bytes: bytes.len() as u64,
                        items: 0,
                    })
                    .map_err(|_| std::io::ErrorKind::Interrupted)?;
                let Some(total) = self.bytes.checked_add(bytes.len() as u64) else {
                    let _ = self.scope.bound::<()>("JSON byte count overflow");
                    return Err(std::io::ErrorKind::Interrupted.into());
                };
                self.bytes = total;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut counter = Counter {
            scope: self,
            bytes: 0,
        };
        serde_json::to_writer(&mut counter, value)?;
        Ok(counter.bytes)
    }
    fn string(&mut self, value: &str) -> Encoded<String> {
        if value.len() as u64 > self.bounds.maximum_bytes.saturating_sub(self.bytes) {
            return self.bound("JSON exceeds declared byte bound");
        }
        let size = self.size(value)?;
        self.data(size)?;
        self.retain(value.len() as u64)?;
        self.event(JsonCodecCharge::CopyAdmission {
            bytes: value.len() as u64,
        })?;
        self.allocate(value.len() as u64)?;
        let mut copied = String::new();
        if let Err(error) = copied.try_reserve_exact(value.len()) {
            return self.stop(Error::Allocation(error));
        }
        if copied.capacity() > value.len() {
            self.retain((copied.capacity() - value.len()) as u64)?;
        }
        self.event(JsonCodecCharge::Copy {
            bytes: value.len() as u64,
        })?;
        copied.push_str(value);
        Ok(copied)
    }
    fn display<T: std::fmt::Display + ?Sized>(&mut self, value: &T) -> Encoded<String> {
        struct Text<'a, 'b> {
            scope: &'a mut Scope<'b>,
            value: String,
            failed: Option<Failure>,
        }
        impl std::fmt::Write for Text<'_, '_> {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                let result = (|| {
                    let encoded = self.scope.size(text)?;
                    self.scope.data(encoded - 2)?;
                    let Some(length) = self.value.len().checked_add(text.len()) else {
                        return self.scope.bound("JSON string size overflow");
                    };
                    self.scope.event(JsonCodecCharge::CopyAdmission {
                        bytes: text.len() as u64,
                    })?;
                    if length > self.value.capacity() {
                        self.scope.retain((length - self.value.capacity()) as u64)?;
                        self.scope.allocate(length as u64)?;
                        if let Err(error) = self.value.try_reserve_exact(length - self.value.len())
                        {
                            return self.scope.stop(Error::Allocation(error));
                        }
                        if self.value.capacity() > length {
                            self.scope.retain((self.value.capacity() - length) as u64)?;
                        }
                    }
                    self.scope.event(JsonCodecCharge::Copy {
                        bytes: text.len() as u64,
                    })?;
                    self.value.push_str(text);
                    Ok(())
                })();
                if let Err(error) = result {
                    self.failed = Some(error);
                    return Err(std::fmt::Error);
                }
                Ok(())
            }
        }
        self.data(2)?;
        let mut text = Text {
            scope: self,
            value: String::new(),
            failed: None,
        };
        if std::fmt::write(&mut text, format_args!("{value}")).is_err() {
            return match text.failed.take() {
                Some(error) => Err(error),
                None => text.scope.bound("JSON string formatting failed"),
            };
        }
        Ok(text.value)
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

#[derive(Clone, Copy)]
enum Position {
    Value,
    Key,
    Raw,
}
struct Builder<'a, 'b> {
    scope: &'a mut Scope<'b>,
    depth: u32,
    position: Position,
}
struct Compound<'a, 'b> {
    builder: Builder<'a, 'b>,
    values: Values,
}
enum Values {
    Array(Vec<Value>, Option<String>),
    Object(
        serde_json::Map<String, Value>,
        Option<String>,
        Option<String>,
    ),
    Raw(Option<Value>),
}
struct Stack {
    bytes: [u8; 64],
    length: usize,
}
impl std::io::Write for Stack {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let end = self
            .length
            .checked_add(bytes.len())
            .filter(|end| *end <= 64)
            .ok_or(std::io::ErrorKind::InvalidData)?;
        self.bytes[self.length..end].copy_from_slice(bytes);
        self.length = end;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
macro_rules! scalar {
    ($($method:ident($ty:ty)),* $(,)?) => {$(
        fn $method(self, value: $ty) -> Encoded<Value> {
            if matches!(self.position, Position::Raw) { return self.scope.bound("expected value"); }
            self.scope.item(self.depth)?;
            if matches!(self.position, Position::Key) {
                struct Scalar($ty);
                impl Serialize for Scalar { fn serialize<S: ser::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> { serializer.$method(self.0) } }
                let mut stack = Stack { bytes: [0; 64], length: 0 };
                serde_json::to_writer(&mut stack, &Scalar(value))?;
                let key = std::str::from_utf8(&stack.bytes[..stack.length]).expect("JSON scalar is UTF-8");
                if key == "null" { return self.scope.bound("float key must be finite (got NaN or +/-inf)"); }
                return self.scope.string(key).map(Value::String);
            }
            let value = ser::Serializer::$method(serde_json::value::Serializer, value)?;
            let bytes = self.scope.size(&value)?; self.scope.data(bytes)?; Ok(value)
        }
    )*};
}
impl Builder<'_, '_> {
    fn ordinary(&mut self) -> Encoded<()> {
        match self.position {
            Position::Value => Ok(()),
            Position::Key => self.scope.bound("key must be a string"),
            Position::Raw => self.scope.bound("expected value"),
        }
    }
    fn tag(&mut self, tag: &str) -> Encoded<String> {
        self.ordinary()?;
        self.scope.item(self.depth)?;
        self.scope.data(3)?;
        self.scope.item(self.depth + 1)?;
        let tag = self.scope.string(tag)?;
        self.scope
            .retain(std::mem::size_of::<(String, Value)>() as u64)?;
        Ok(tag)
    }
}
impl<'a, 'b> ser::Serializer for Builder<'a, 'b> {
    type Ok = Value;
    type Error = Failure;
    type SerializeSeq = Compound<'a, 'b>;
    type SerializeTuple = Compound<'a, 'b>;
    type SerializeTupleStruct = Compound<'a, 'b>;
    type SerializeTupleVariant = Compound<'a, 'b>;
    type SerializeMap = Compound<'a, 'b>;
    type SerializeStruct = Compound<'a, 'b>;
    type SerializeStructVariant = Compound<'a, 'b>;
    scalar!(
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_i128(i128),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_u128(u128),
        serialize_f32(f32),
        serialize_f64(f64)
    );
    fn serialize_char(self, value: char) -> Encoded<Value> {
        if matches!(self.position, Position::Raw) {
            return self.scope.bound("expected value");
        }
        self.serialize_str(value.encode_utf8(&mut [0; 4]))
    }
    fn serialize_str(self, value: &str) -> Encoded<Value> {
        if matches!(self.position, Position::Raw) {
            let Some(depth) = self.scope.bounds.maximum_depth.checked_sub(self.depth) else {
                return self.scope.bound("JSON exceeds declared structural bound");
            };
            let bounds = JsonBounds {
                maximum_depth: depth,
                maximum_items: self.scope.bounds.maximum_items - self.scope.items,
                ..self.scope.bounds
            };
            let shape = admission::source(
                value.as_bytes(),
                bounds,
                Some(JsonValueSchema::Root),
                self.scope.code,
                &mut |event| {
                    if let JsonCodecCharge::Work { items, .. } = event {
                        self.scope.items += items;
                    }
                    self.scope
                        .event(event)
                        .map_err(|error| self.scope.error(error))
                },
            )
            .map_err(|error| {
                if self.scope.failed.is_none() {
                    self.scope.failed = Some(error);
                }
                Failure::Stopped
            })?;
            self.scope.items += shape.virtual_items;
            let allowance = json_value_decode_owned_bytes(shape).map_err(|error| {
                self.scope.failed = Some(error);
                Failure::Stopped
            })?;
            self.scope.event(JsonCodecCharge::Admission {
                maximum_owned_bytes: allowance,
                value_shape: shape,
            })?;
            let value: Value = serde_json::from_str(value)?;
            let bytes = records::json_value_logical_owned_bytes(&value).map_err(|error| {
                self.scope.failed = Some(error);
                Failure::Stopped
            })?;
            self.scope.retain(bytes)?;
            let size = self.scope.size(&value)?;
            self.scope.data(size)?;
            return Ok(value);
        }
        self.scope.item(self.depth)?;
        self.scope.string(value).map(Value::String)
    }
    fn serialize_bytes(mut self, value: &[u8]) -> Encoded<Value> {
        self.ordinary()?;
        let mut sequence = self.serialize_seq(None)?;
        for value in value {
            sequence.serialize_element(value)?;
        }
        SerializeSeq::end(sequence)
    }
    fn serialize_none(self) -> Encoded<Value> {
        self.serialize_unit()
    }
    fn serialize_some<T: Serialize + ?Sized>(mut self, value: &T) -> Encoded<Value> {
        self.ordinary()?;
        value.serialize(self)
    }
    fn serialize_unit(mut self) -> Encoded<Value> {
        self.ordinary()?;
        self.scope.item(self.depth)?;
        self.scope.data(4)?;
        Ok(Value::Null)
    }
    fn serialize_unit_struct(mut self, _: &'static str) -> Encoded<Value> {
        self.ordinary()?;
        self.serialize_unit()
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Encoded<Value> {
        if matches!(self.position, Position::Raw) {
            return self.scope.bound("expected value");
        }
        self.serialize_str(variant)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        mut self,
        _: &'static str,
        value: &T,
    ) -> Encoded<Value> {
        if matches!(self.position, Position::Raw) {
            self.ordinary()?;
        }
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        mut self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> Encoded<Value> {
        let tag = self.tag(variant)?;
        let value = value.serialize(Builder {
            scope: self.scope,
            depth: self.depth + 1,
            position: Position::Value,
        })?;
        let mut map = serde_json::Map::new();
        map.insert(tag, value);
        Ok(Value::Object(map))
    }
    fn serialize_seq(mut self, _: Option<usize>) -> Encoded<Self::SerializeSeq> {
        self.ordinary()?;
        self.scope.item(self.depth)?;
        self.scope.data(2)?;
        Ok(Compound {
            builder: self,
            values: Values::Array(Vec::new(), None),
        })
    }
    fn serialize_tuple(self, _: usize) -> Encoded<Self::SerializeTuple> {
        self.serialize_seq(None)
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Encoded<Self::SerializeTupleStruct> {
        self.serialize_seq(None)
    }
    fn serialize_tuple_variant(
        mut self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Encoded<Self::SerializeTupleVariant> {
        let tag = self.tag(variant)?;
        self.depth += 1;
        let mut sequence = self.serialize_seq(None)?;
        if let Values::Array(_, name) = &mut sequence.values {
            *name = Some(tag);
        }
        Ok(sequence)
    }
    fn serialize_map(mut self, _: Option<usize>) -> Encoded<Self::SerializeMap> {
        self.ordinary()?;
        self.scope.item(self.depth)?;
        self.scope.data(2)?;
        Ok(Compound {
            builder: self,
            values: Values::Object(serde_json::Map::new(), None, None),
        })
    }
    fn serialize_struct(mut self, name: &'static str, _: usize) -> Encoded<Self::SerializeStruct> {
        self.ordinary()?;
        if name == "$serde_json::private::RawValue" {
            Ok(Compound {
                builder: self,
                values: Values::Raw(None),
            })
        } else {
            self.serialize_map(None)
        }
    }
    fn serialize_struct_variant(
        mut self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> Encoded<Self::SerializeStructVariant> {
        let tag = self.tag(variant)?;
        self.depth += 1;
        let mut map = self.serialize_map(None)?;
        if let Values::Object(_, _, name) = &mut map.values {
            *name = Some(tag);
        }
        Ok(map)
    }
    fn collect_str<T: std::fmt::Display + ?Sized>(self, value: &T) -> Encoded<Value> {
        if matches!(self.position, Position::Raw) {
            return self.scope.bound("expected value");
        }
        self.scope.item(self.depth)?;
        self.scope.display(value).map(Value::String)
    }
}
impl Compound<'_, '_> {
    fn element<T: Serialize + ?Sized>(&mut self, value: &T) -> Encoded<()> {
        let Values::Array(values, _) = &mut self.values else {
            return self.builder.scope.bound("expected sequence");
        };
        if !values.is_empty() {
            self.builder.scope.data(1)?;
        }
        if values.len() == values.capacity() {
            let Some(bytes) = values
                .len()
                .checked_add(1)
                .and_then(|length| length.checked_mul(std::mem::size_of::<Value>()))
            else {
                return self.builder.scope.bound("JSON array capacity overflow");
            };
            let bytes = bytes as u64;
            let requested = values.len() + 1;
            self.builder
                .scope
                .retain(std::mem::size_of::<Value>() as u64)?;
            self.builder.scope.allocate(bytes)?;
            if let Err(error) = values.try_reserve_exact(1) {
                return self.builder.scope.stop(Error::Allocation(error));
            }
            if values.capacity() > requested {
                self.builder.scope.retain(
                    ((values.capacity() - requested) * std::mem::size_of::<Value>()) as u64,
                )?;
            }
        }
        let value = value.serialize(Builder {
            scope: self.builder.scope,
            depth: self.builder.depth + 1,
            position: Position::Value,
        })?;
        values.push(value);
        Ok(())
    }
    fn key<T: Serialize + ?Sized>(&mut self, key: &T) -> Encoded<()> {
        let Values::Object(values, target, _) = &mut self.values else {
            return self.builder.scope.bound("expected object");
        };
        if !values.is_empty() {
            self.builder.scope.data(1)?;
        }
        let key = key.serialize(Builder {
            scope: self.builder.scope,
            depth: self.builder.depth + 1,
            position: Position::Key,
        })?;
        let Value::String(key) = key else {
            return self.builder.scope.bound("key must be a string");
        };
        *target = Some(key);
        Ok(())
    }
    fn field<T: Serialize + ?Sized>(&mut self, value: &T) -> Encoded<()> {
        let Values::Object(values, key, _) = &mut self.values else {
            return self.builder.scope.bound("expected object");
        };
        let Some(key) = key.take() else {
            return self.builder.scope.bound("map value missing key");
        };
        self.builder.scope.data(1)?;
        let replaced = values.contains_key(&key);
        let key_bytes = key.capacity() as u64;
        if !replaced {
            self.builder
                .scope
                .retain(std::mem::size_of::<(String, Value)>() as u64)?;
        }
        let value = value.serialize(Builder {
            scope: self.builder.scope,
            depth: self.builder.depth + 1,
            position: Position::Value,
        })?;
        if let Some(previous) = values.insert(key, value) {
            let bytes = records::json_value_logical_owned_bytes(&previous).map_err(|error| {
                self.builder.scope.failed = Some(error);
                Failure::Stopped
            })?;
            drop(previous);
            self.builder.scope.release(bytes + key_bytes)?;
        }
        Ok(())
    }
    fn finish(mut self) -> Encoded<Value> {
        let (value, tag) = match self.values {
            Values::Array(values, tag) => (Value::Array(values), tag),
            Values::Object(values, key, tag) => {
                if key.is_some() {
                    return self.builder.scope.bound("map value missing");
                }
                (Value::Object(values), tag)
            }
            Values::Raw(value) => {
                return value.ok_or_else(|| {
                    self.builder.scope.failed =
                        Some(boundary("expected value", self.builder.scope.code));
                    Failure::Stopped
                });
            }
        };
        if let Some(tag) = tag {
            let mut map = serde_json::Map::new();
            map.insert(tag, value);
            Ok(Value::Object(map))
        } else {
            Ok(value)
        }
    }
}
macro_rules! sequence {
    ($($trait:ident => $method:ident),* $(,)?) => {$(
        impl ser::$trait for Compound<'_, '_> {
            type Ok = Value; type Error = Failure;
            fn $method<T: Serialize + ?Sized>(&mut self, value: &T) -> Encoded<()> { self.element(value) }
            fn end(self) -> Encoded<Value> { self.finish() }
        }
    )*};
}
sequence!(SerializeSeq => serialize_element, SerializeTuple => serialize_element, SerializeTupleStruct => serialize_field, SerializeTupleVariant => serialize_field);
impl SerializeMap for Compound<'_, '_> {
    type Ok = Value;
    type Error = Failure;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Encoded<()> {
        self.key(key)
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Encoded<()> {
        self.field(value)
    }
    fn end(self) -> Encoded<Value> {
        self.finish()
    }
}
impl SerializeStruct for Compound<'_, '_> {
    type Ok = Value;
    type Error = Failure;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Encoded<()> {
        if let Values::Raw(output) = &mut self.values {
            if key != "$serde_json::private::RawValue" {
                return self.builder.scope.bound("expected value");
            }
            *output = Some(value.serialize(Builder {
                scope: self.builder.scope,
                depth: self.builder.depth,
                position: Position::Raw,
            })?);
            return Ok(());
        }
        self.key(key)?;
        self.field(value)
    }
    fn end(self) -> Encoded<Value> {
        self.finish()
    }
}
impl ser::SerializeStructVariant for Compound<'_, '_> {
    type Ok = Value;
    type Error = Failure;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Encoded<()> {
        SerializeStruct::serialize_field(self, key, value)
    }
    fn end(self) -> Encoded<Value> {
        self.finish()
    }
}
pub(super) fn build<T: Serialize + ?Sized>(value: &T, scope: &mut Scope<'_>) -> Result<Value> {
    let result = value.serialize(Builder {
        scope,
        depth: 0,
        position: Position::Value,
    });
    match result {
        Ok(mut value) => {
            if let Some(error) = scope.failed.take() {
                return Err(error);
            }
            value.sort_all_objects();
            Ok(value)
        }
        Err(error) => Err(scope.error(error)),
    }
}
