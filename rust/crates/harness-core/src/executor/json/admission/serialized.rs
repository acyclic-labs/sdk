//! Layout charge of a resident value, walked through its own serialization.
//!
//! The value is never encoded: each serializer event places the JSON node
//! `serde_json` would write at the same position of the type's layout, so a
//! resident record is charged exactly as its typed decode would be.

use super::*;
use serde::ser::{self, Impossible, Serialize, Serializer};

type Failure = serde_json::Error;

fn failure(reason: &'static str) -> Failure {
    <Failure as ser::Error>::custom(reason)
}

/// Heap bytes the typed decode of `value`'s JSON form allocates under `layout`.
pub(crate) fn serialized_layout_bytes<T: Serialize + ?Sized>(
    value: &T,
    layout: &'static JsonLayout,
) -> Result<u64> {
    let cost = RefCell::new(LayoutCost::default());
    value
        .serialize(Site(Some(Placement::root(layout, &cost))))
        .map_err(|error| Error::Invalid(format!("record layout walk failed: {error}")))?;
    let cost = cost.into_inner();
    if cost.refused {
        return Err(Error::Invalid(
            "record shape is outside its typed layout".into(),
        ));
    }
    Ok(cost.bytes())
}

/// One position: the JSON node serialized here.
#[derive(Clone, Copy)]
struct Site<'a>(Option<Placement<'a>>);

impl<'a> Site<'a> {
    fn string(self, text: &str) {
        if let Some(placement) = self.0 {
            placement.string(text.as_bytes(), escapes(text));
        }
    }
    /// A one-member object `{"variant": …}` holding the variant's value.
    fn variant(self, variant: &'static str) -> (Entries<'a>, Site<'a>) {
        let mut entries = Entries::new(self.0, true);
        entries.count = 1;
        entries.key(variant.as_bytes(), escapes(variant));
        let inner = Site(entries.child(Some(variant.as_bytes()), 0));
        (entries, inner)
    }
}

/// A sequence's or object's entries.
struct Container<'a> {
    entries: Entries<'a>,
    index: usize,
    /// The member a map key selected, until its value is serialized.
    pending: Option<Site<'a>>,
}

impl<'a> Container<'a> {
    fn new(site: Site<'a>, map: bool) -> Self {
        Self {
            entries: Entries::new(site.0, map),
            index: 0,
            pending: None,
        }
    }
    fn element<T: Serialize + ?Sized>(&mut self, value: &T) -> std::result::Result<(), Failure> {
        let site = Site(self.entries.child(None, self.index));
        self.entries.count += 1;
        self.index += 1;
        value.serialize(site)
    }
    fn member(&mut self, key: &[u8]) -> Site<'a> {
        self.entries.key(key, escapes_bytes(key));
        let site = Site(self.entries.child(Some(key), self.index));
        self.entries.count += 1;
        self.index += 1;
        site
    }
    fn field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Failure> {
        value.serialize(self.member(key.as_bytes()))
    }
}

/// A container inside a one-member variant object. Fields drop in order, so
/// the inner container is charged before the object that encloses it.
struct Variant<'a> {
    inner: Container<'a>,
    _outer: Entries<'a>,
}

/// `serde_json::value::RawValue` serializes as this one-field struct whose
/// field holds the raw JSON text.
struct Raw<'a>(Site<'a>);

enum Struct<'a> {
    Fields(Container<'a>),
    Raw(Raw<'a>),
}

impl<'a> Serializer for Site<'a> {
    type Ok = ();
    type Error = Failure;
    type SerializeSeq = Container<'a>;
    type SerializeTuple = Container<'a>;
    type SerializeTupleStruct = Container<'a>;
    type SerializeTupleVariant = Variant<'a>;
    type SerializeMap = Container<'a>;
    type SerializeStruct = Struct<'a>;
    type SerializeStructVariant = Variant<'a>;

    fn serialize_bool(self, _: bool) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_i8(self, _: i8) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_i16(self, _: i16) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_i32(self, _: i32) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_i64(self, _: i64) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_i128(self, _: i128) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_u8(self, _: u8) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_u16(self, _: u16) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_u32(self, _: u32) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_u64(self, _: u64) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_u128(self, _: u128) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_f32(self, _: f32) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_f64(self, _: f64) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_char(self, value: char) -> std::result::Result<(), Failure> {
        self.string(value.encode_utf8(&mut [0; 4]));
        Ok(())
    }
    fn serialize_str(self, value: &str) -> std::result::Result<(), Failure> {
        self.string(value);
        Ok(())
    }
    /// `serde_json` writes bytes as an array of numbers.
    fn serialize_bytes(self, value: &[u8]) -> std::result::Result<(), Failure> {
        let mut bytes = Container::new(self, false);
        for byte in value {
            bytes.element(byte)?;
        }
        Ok(())
    }
    fn serialize_none(self) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> std::result::Result<(), Failure> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_unit_struct(self, _: &'static str) -> std::result::Result<(), Failure> {
        Ok(())
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> std::result::Result<(), Failure> {
        self.string(variant);
        Ok(())
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> std::result::Result<(), Failure> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> std::result::Result<(), Failure> {
        let (_outer, inner) = self.variant(variant);
        value.serialize(inner)
    }
    fn serialize_seq(self, _: Option<usize>) -> std::result::Result<Container<'a>, Failure> {
        Ok(Container::new(self, false))
    }
    fn serialize_tuple(self, _: usize) -> std::result::Result<Container<'a>, Failure> {
        Ok(Container::new(self, false))
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> std::result::Result<Container<'a>, Failure> {
        Ok(Container::new(self, false))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> std::result::Result<Variant<'a>, Failure> {
        let (outer, inner) = self.variant(variant);
        Ok(Variant {
            inner: Container::new(inner, false),
            _outer: outer,
        })
    }
    fn serialize_map(self, _: Option<usize>) -> std::result::Result<Container<'a>, Failure> {
        Ok(Container::new(self, true))
    }
    fn serialize_struct(
        self,
        name: &'static str,
        _: usize,
    ) -> std::result::Result<Struct<'a>, Failure> {
        if name.as_bytes() == RAW {
            return Ok(Struct::Raw(Raw(self)));
        }
        Ok(Struct::Fields(Container::new(self, true)))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        _: usize,
    ) -> std::result::Result<Variant<'a>, Failure> {
        let (outer, inner) = self.variant(variant);
        Ok(Variant {
            inner: Container::new(inner, true),
            _outer: outer,
        })
    }
}

impl ser::SerializeSeq for Container<'_> {
    type Ok = ();
    type Error = Failure;
    fn serialize_element<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Failure> {
        self.element(value)
    }
    fn end(self) -> std::result::Result<(), Failure> {
        Ok(())
    }
}

impl ser::SerializeTuple for Container<'_> {
    type Ok = ();
    type Error = Failure;
    fn serialize_element<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Failure> {
        self.element(value)
    }
    fn end(self) -> std::result::Result<(), Failure> {
        Ok(())
    }
}

impl ser::SerializeTupleStruct for Container<'_> {
    type Ok = ();
    type Error = Failure;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Failure> {
        self.element(value)
    }
    fn end(self) -> std::result::Result<(), Failure> {
        Ok(())
    }
}

impl ser::SerializeTupleVariant for Variant<'_> {
    type Ok = ();
    type Error = Failure;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Failure> {
        self.inner.element(value)
    }
    fn end(self) -> std::result::Result<(), Failure> {
        Ok(())
    }
}

impl ser::SerializeStructVariant for Variant<'_> {
    type Ok = ();
    type Error = Failure;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Failure> {
        self.inner.field(key, value)
    }
    fn end(self) -> std::result::Result<(), Failure> {
        Ok(())
    }
}

impl ser::SerializeStruct for Struct<'_> {
    type Ok = ();
    type Error = Failure;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Failure> {
        match self {
            Self::Fields(fields) => fields.field(key, value),
            Self::Raw(raw) => value.serialize(RawText(raw.0)),
        }
    }
    fn end(self) -> std::result::Result<(), Failure> {
        Ok(())
    }
}

impl ser::SerializeMap for Container<'_> {
    type Ok = ();
    type Error = Failure;
    fn serialize_key<T: Serialize + ?Sized>(
        &mut self,
        key: &T,
    ) -> std::result::Result<(), Failure> {
        let site = key.serialize(Key(self))?;
        self.pending = Some(site);
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Failure> {
        let site = self
            .pending
            .take()
            .ok_or_else(|| failure("map value without a key"))?;
        value.serialize(site)
    }
    fn end(self) -> std::result::Result<(), Failure> {
        Ok(())
    }
}

/// The raw JSON text of a `RawValue`, walked in place as the value it holds.
struct RawText<'a>(Site<'a>);

/// A map key, written by `serde_json` as a JSON string. Keys that are not
/// strings are written in their decimal or literal form.
struct Key<'c, 'a>(&'c mut Container<'a>);

macro_rules! unsupported {
    ($($method:ident($($ty:ty),*) -> $ok:ty;)*) => {$(
        fn $method(self, $(_: $ty),*) -> std::result::Result<$ok, Failure> {
            Err(failure("value is not a JSON form serde_json writes here"))
        }
    )*};
}

macro_rules! decimal_keys {
    ($($method:ident($ty:ty),)*) => {$(
        fn $method(self, value: $ty) -> std::result::Result<Site<'a>, Failure> {
            let mut text = Decimal::new();
            std::fmt::Write::write_fmt(&mut text, format_args!("{value}"))
                .map_err(|_| failure("map key exceeds its decimal form"))?;
            Ok(self.0.member(text.as_bytes()))
        }
    )*};
}

/// A number or literal key's text, written without allocation.
struct Decimal {
    bytes: [u8; 48],
    length: usize,
}

impl Decimal {
    const fn new() -> Self {
        Self {
            bytes: [0; 48],
            length: 0,
        }
    }
    fn as_bytes(&self) -> &[u8] {
        self.bytes.get(..self.length).unwrap_or_default()
    }
}

impl std::fmt::Write for Decimal {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        let end = self.length.saturating_add(text.len());
        let slot = self
            .bytes
            .get_mut(self.length..end)
            .ok_or(std::fmt::Error)?;
        slot.copy_from_slice(text.as_bytes());
        self.length = end;
        Ok(())
    }
}

impl<'a> Serializer for Key<'_, 'a> {
    type Ok = Site<'a>;
    type Error = Failure;
    type SerializeSeq = Impossible<Site<'a>, Failure>;
    type SerializeTuple = Impossible<Site<'a>, Failure>;
    type SerializeTupleStruct = Impossible<Site<'a>, Failure>;
    type SerializeTupleVariant = Impossible<Site<'a>, Failure>;
    type SerializeMap = Impossible<Site<'a>, Failure>;
    type SerializeStruct = Impossible<Site<'a>, Failure>;
    type SerializeStructVariant = Impossible<Site<'a>, Failure>;

    fn serialize_str(self, value: &str) -> std::result::Result<Site<'a>, Failure> {
        Ok(self.0.member(value.as_bytes()))
    }
    fn serialize_char(self, value: char) -> std::result::Result<Site<'a>, Failure> {
        let mut buffer = [0; 4];
        Ok(self.0.member(value.encode_utf8(&mut buffer).as_bytes()))
    }
    fn serialize_bool(self, value: bool) -> std::result::Result<Site<'a>, Failure> {
        Ok(self.0.member(if value { b"true" } else { b"false" }))
    }
    decimal_keys!(
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
        serialize_f64(f64),
    );
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> std::result::Result<Site<'a>, Failure> {
        Ok(self.0.member(variant.as_bytes()))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> std::result::Result<Site<'a>, Failure> {
        value.serialize(self)
    }
    unsupported! {
        serialize_bytes(&[u8]) -> Site<'a>;
        serialize_none() -> Site<'a>;
        serialize_unit() -> Site<'a>;
        serialize_unit_struct(&'static str) -> Site<'a>;
        serialize_seq(Option<usize>) -> Self::SerializeSeq;
        serialize_tuple(usize) -> Self::SerializeTuple;
        serialize_tuple_struct(&'static str, usize) -> Self::SerializeTupleStruct;
        serialize_tuple_variant(&'static str, u32, &'static str, usize) -> Self::SerializeTupleVariant;
        serialize_map(Option<usize>) -> Self::SerializeMap;
        serialize_struct(&'static str, usize) -> Self::SerializeStruct;
        serialize_struct_variant(&'static str, u32, &'static str, usize) -> Self::SerializeStructVariant;
    }
    fn serialize_some<T: Serialize + ?Sized>(
        self,
        _: &T,
    ) -> std::result::Result<Site<'a>, Failure> {
        Err(failure("map key is not a JSON string"))
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> std::result::Result<Site<'a>, Failure> {
        Err(failure("map key is not a JSON string"))
    }
}

impl Serializer for RawText<'_> {
    type Ok = ();
    type Error = Failure;
    type SerializeSeq = Impossible<(), Failure>;
    type SerializeTuple = Impossible<(), Failure>;
    type SerializeTupleStruct = Impossible<(), Failure>;
    type SerializeTupleVariant = Impossible<(), Failure>;
    type SerializeMap = Impossible<(), Failure>;
    type SerializeStruct = Impossible<(), Failure>;
    type SerializeStructVariant = Impossible<(), Failure>;

    fn serialize_str(self, text: &str) -> std::result::Result<(), Failure> {
        let mut ignore = |_: JsonCodecCharge| Ok(());
        let charge: Charge<'_> = RefCell::new(&mut ignore);
        document(
            text.as_bytes(),
            &UNKNOWN,
            self.0.0,
            0,
            false,
            &charge,
            &RefCell::new(None),
        )
        .map(|_| ())
        .map_err(|_| failure("RawValue text is not one JSON document"))
    }
    unsupported! {
        serialize_bool(bool) -> ();
        serialize_i8(i8) -> ();
        serialize_i16(i16) -> ();
        serialize_i32(i32) -> ();
        serialize_i64(i64) -> ();
        serialize_u8(u8) -> ();
        serialize_u16(u16) -> ();
        serialize_u32(u32) -> ();
        serialize_u64(u64) -> ();
        serialize_f32(f32) -> ();
        serialize_f64(f64) -> ();
        serialize_char(char) -> ();
        serialize_bytes(&[u8]) -> ();
        serialize_none() -> ();
        serialize_unit() -> ();
        serialize_unit_struct(&'static str) -> ();
        serialize_unit_variant(&'static str, u32, &'static str) -> ();
        serialize_seq(Option<usize>) -> Self::SerializeSeq;
        serialize_tuple(usize) -> Self::SerializeTuple;
        serialize_tuple_struct(&'static str, usize) -> Self::SerializeTupleStruct;
        serialize_tuple_variant(&'static str, u32, &'static str, usize) -> Self::SerializeTupleVariant;
        serialize_map(Option<usize>) -> Self::SerializeMap;
        serialize_struct(&'static str, usize) -> Self::SerializeStruct;
        serialize_struct_variant(&'static str, u32, &'static str, usize) -> Self::SerializeStructVariant;
    }
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> std::result::Result<(), Failure> {
        Err(failure("RawValue holds text"))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: &T,
    ) -> std::result::Result<(), Failure> {
        Err(failure("RawValue holds text"))
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> std::result::Result<(), Failure> {
        Err(failure("RawValue holds text"))
    }
}

/// Whether `serde_json` writes `text` with an escape, which its decode then
/// owns in every buffer holding it.
fn escapes(text: &str) -> bool {
    escapes_bytes(text.as_bytes())
}
fn escapes_bytes(text: &[u8]) -> bool {
    text.iter()
        .any(|byte| matches!(byte, b'"' | b'\\' | 0..=0x1f))
}
