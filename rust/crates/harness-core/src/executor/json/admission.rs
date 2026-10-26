use super::layout::{Entries, JsonLayout, LayoutCost, Placement};
use super::schema::{JsonValueSchema, NONE, ROOT, UNKNOWN};
use super::*;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use std::cell::RefCell;

type Charge<'a> = RefCell<&'a mut dyn FnMut(JsonCodecCharge) -> Result<()>>;
const RAW: &[u8] = b"$serde_json::private::RawValue";
#[derive(Clone, Copy, Default)]
struct Counts {
    nodes: u64,
    bytes: u64,
    frames: u64,
    depth: u32,
    unresolved: bool,
}
impl Counts {
    fn add(&mut self, other: Self) -> Result<()> {
        self.nodes = checked(
            self.nodes.checked_add(other.nodes),
            "JSON item count overflow",
        )?;
        self.bytes = checked(
            self.bytes.checked_add(other.bytes),
            "JSON byte count overflow",
        )?;
        self.frames = checked(
            self.frames.checked_add(other.frames),
            "JSON RawValue count overflow",
        )?;
        self.depth = self.depth.max(other.depth);
        self.unresolved |= other.unresolved;
        Ok(())
    }
}
#[derive(Default)]
struct Branches {
    always: Counts,
    gated: [Counts; 3],
}
impl Branches {
    fn add(&mut self, mut child: Self, gate: u8) -> Result<()> {
        if gate != 0 {
            for counts in child.gated {
                child.always.add(counts)?;
            }
            for (index, counts) in self.gated.iter_mut().enumerate() {
                if gate & (1 << index) != 0 {
                    counts.add(child.always)?;
                }
            }
        } else {
            self.always.add(child.always)?;
            for (counts, child) in self.gated.iter_mut().zip(child.gated) {
                counts.add(child)?;
            }
        }
        Ok(())
    }
    fn resolve(&mut self, mask: u8) -> Result<()> {
        for (index, counts) in self.gated.iter().enumerate() {
            if mask & (1 << index) != 0 {
                self.always.add(*counts)?;
            }
        }
        self.gated = [Counts::default(); 3];
        Ok(())
    }
}

struct Walk<'a, 'b> {
    schema: &'static JsonValueSchema,
    depth: u32,
    virtual_value: bool,
    raw: Option<&'static JsonValueSchema>,
    layout: Option<Placement<'a>>,
    charge: &'a Charge<'b>,
    failed: &'a RefCell<Option<Error>>,
}
impl<'a, 'b> Walk<'a, 'b> {
    fn base(&self) -> Branches {
        Branches {
            always: Counts {
                nodes: u64::from(self.virtual_value),
                depth: if self.virtual_value { self.depth } else { 0 },
                ..Default::default()
            },
            ..Default::default()
        }
    }
    fn error<E: de::Error>(&self, error: Error) -> E {
        if self.failed.borrow().is_none() {
            *self.failed.borrow_mut() = Some(error);
        }
        E::custom("JSON admission interrupted")
    }
    fn child(
        &self,
        schema: &'static JsonValueSchema,
        raw: Option<&'static JsonValueSchema>,
        layout: Option<Placement<'a>>,
    ) -> Walk<'a, 'b> {
        Walk {
            schema,
            raw,
            layout,
            depth: self.depth + 1,
            virtual_value: self.virtual_value,
            charge: self.charge,
            failed: self.failed,
        }
    }
}
impl<'de> DeserializeSeed<'de> for Walk<'_, '_> {
    type Value = Branches;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<Branches, D::Error> {
        if self.depth > 256 + usize::BITS {
            <IgnoredAny as serde::Deserialize>::deserialize(decoder)?;
            return Ok(Branches {
                always: Counts {
                    depth: self.depth,
                    unresolved: true,
                    ..Default::default()
                },
                ..Default::default()
            });
        }
        decoder.deserialize_any(self)
    }
}
macro_rules! walk_scalar {
    ($($method:ident($ty:ty)),*) => {$(fn $method<E: de::Error>(self, _: $ty) -> std::result::Result<Branches, E> { Ok(self.base()) })*};
}
impl Walk<'_, '_> {
    fn string<E: de::Error>(self, text: &str, escaped: bool) -> std::result::Result<Branches, E> {
        if let Some(layout) = self.layout {
            layout.string(text.as_bytes(), escaped);
        }
        // A `Value` expands a raw member into the document its text holds.
        let expands =
            self.raw.is_some() && self.layout.is_some_and(|layout| layout.decodes_value());
        let raw = if expands { Some(&ROOT) } else { self.raw };
        let mut counts = self.base();
        match raw {
            Some(schema) if std::ptr::eq(schema, &ROOT) => {
                self.charge.borrow_mut()(JsonCodecCharge::Inspect {
                    items: text.len() as u64,
                })
                .map_err(|error| self.error::<E>(error))?;
                let mut nested = document(
                    text.as_bytes(),
                    &ROOT,
                    self.layout.map(Placement::expanded),
                    self.depth,
                    true,
                    self.charge,
                    self.failed,
                )
                .map_err(|error| self.error::<E>(error))?;
                nested.always.bytes = checked(
                    nested.always.bytes.checked_add(text.len() as u64),
                    "JSON byte count overflow",
                )
                .map_err(|error| self.error::<E>(error))?;
                nested.always.frames = checked(
                    nested.always.frames.checked_add(1),
                    "JSON RawValue count overflow",
                )
                .map_err(|error| self.error::<E>(error))?;
                counts
                    .add(nested, 0)
                    .map_err(|error| self.error::<E>(error))?;
            }
            Some(schema) if std::ptr::eq(schema, &UNKNOWN) => counts.always.unresolved = true,
            _ => {}
        }
        Ok(counts)
    }
}
impl<'de> Visitor<'de> for Walk<'_, '_> {
    type Value = Branches;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JSON value")
    }
    walk_scalar!(
        visit_bool(bool),
        visit_i64(i64),
        visit_u64(u64),
        visit_f64(f64)
    );
    fn visit_unit<E: de::Error>(self) -> std::result::Result<Branches, E> {
        Ok(self.base())
    }
    // `from_slice` lends an unescaped string from the document and decodes
    // an escaped one into a scratch buffer: only that one is owned by the
    // `Content` buffers holding it.
    fn visit_borrowed_str<E: de::Error>(self, text: &'de str) -> std::result::Result<Branches, E> {
        self.string(text, false)
    }
    fn visit_str<E: de::Error>(self, text: &str) -> std::result::Result<Branches, E> {
        self.string(text, true)
    }
    fn visit_seq<A: SeqAccess<'de>>(
        self,
        mut values: A,
    ) -> std::result::Result<Branches, A::Error> {
        let mut counts = self.base();
        let mut entries = Entries::new(self.layout, false);
        let mut index = 0;
        loop {
            let (schema, gate) = self.schema.child(b"", Some(index));
            let layout = entries.child(None, index);
            let Some(child) = values.next_element_seed(self.child(schema, None, layout))? else {
                break;
            };
            entries.count += 1;
            counts
                .add(child, gate)
                .map_err(|error| self.error::<A::Error>(error))?;
            index += 1;
        }
        Ok(counts)
    }
    fn visit_map<A: MapAccess<'de>>(
        self,
        mut fields: A,
    ) -> std::result::Result<Branches, A::Error> {
        let mut counts = self.base();
        let mut entries = Entries::new(self.layout, true);
        let (mut index, mut tag) = (0, None);
        while let Some((key, escaped)) = fields.next_key_seed(Key)? {
            entries.count += 1;
            entries.key(key.as_bytes(), escaped);
            let key = key.as_bytes();
            let layout = entries.child(Some(key), index);
            if self.virtual_value {
                counts
                    .always
                    .add(Counts {
                        nodes: 1,
                        depth: self.depth + 1,
                        ..Default::default()
                    })
                    .map_err(|error| self.error::<A::Error>(error))?;
            }
            let (schema, gate) = self.schema.child(key, None);
            let mut child = if !self.schema.tag.is_empty() && key == self.schema.tag.as_bytes() {
                let label: &serde_json::value::RawValue = fields.next_value()?;
                let escaped = label.get().contains('\\');
                let label = serde_json::from_str::<String>(label.get()).unwrap_or_default();
                if let Some(layout) = layout {
                    layout.string(label.as_bytes(), escaped);
                }
                tag = Some(if tag.is_some() {
                    0
                } else {
                    self.schema.mask(label.as_bytes())
                });
                self.child(schema, None, layout).base()
            } else {
                fields.next_value_seed(self.child(
                    schema,
                    (index == 0 && key == RAW).then_some(schema),
                    layout,
                ))?
            };
            if std::ptr::eq(schema, &NONE) {
                child = Branches::default();
            }
            counts
                .add(child, gate)
                .map_err(|error| self.error::<A::Error>(error))?;
            index += 1;
        }
        if !self.schema.tag.is_empty() {
            counts
                .resolve(tag.unwrap_or(0))
                .map_err(|error| self.error::<A::Error>(error))?;
        }
        Ok(counts)
    }
}
fn document(
    bytes: &[u8],
    schema: &'static JsonValueSchema,
    layout: Option<Placement<'_>>,
    depth: u32,
    virtual_value: bool,
    charge: &Charge<'_>,
    failed: &RefCell<Option<Error>>,
) -> Result<Branches> {
    let scratch = checked(
        (bytes.len() as u64).max(8).checked_mul(4),
        "JSON scratch bound overflow",
    )?;
    charge.borrow_mut()(JsonCodecCharge::Retain { bytes: scratch })?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let result = Walk {
        schema,
        depth,
        virtual_value,
        raw: None,
        layout,
        charge,
        failed,
    }
    .deserialize(&mut decoder)
    .and_then(|counts| {
        decoder.end()?;
        Ok(counts)
    });
    drop(decoder);
    let release = charge.borrow_mut()(JsonCodecCharge::Release { bytes: scratch });
    if let Some(error) = failed.borrow_mut().take() {
        return Err(error);
    }
    release?;
    Ok(result.unwrap_or_else(|_| Branches {
        always: Counts {
            unresolved: true,
            ..Default::default()
        },
        ..Default::default()
    }))
}
#[derive(Default)]
pub(super) struct Lexical {
    pub(super) items: u64,
    pub(super) containers: u64,
    pub(super) arrays: usize,
    pub(super) depth: u32,
    pub(super) string: bool,
    escape: bool,
    atom: bool,
}
impl Lexical {
    pub(super) fn byte(
        &mut self,
        byte: u8,
        bounds: JsonBounds,
        code: Option<crate::wire::ErrorCode>,
        charge: &Charge<'_>,
    ) -> Result<bool> {
        if self.string {
            if self.escape {
                self.escape = false;
            } else if byte == b'\\' {
                self.escape = true;
            } else if byte == b'"' {
                self.string = false;
            }
            return Ok(false);
        }
        let depth = self.depth;
        let item = match byte {
            b'"' => {
                self.string = true;
                self.atom = false;
                true
            }
            b'[' | b'{' => {
                self.containers += 1;
                self.arrays += usize::from(byte == b'[');
                self.depth += 1;
                self.atom = false;
                true
            }
            b']' | b'}' => {
                self.depth = self
                    .depth
                    .checked_sub(1)
                    .ok_or_else(|| input_error(JsonInputError::Nesting, code))?;
                self.atom = false;
                false
            }
            b':' | b',' | b' ' | b'\n' | b'\r' | b'\t' => {
                self.atom = false;
                false
            }
            _ => {
                let first = !self.atom;
                self.atom = true;
                first
            }
        };
        if item {
            if self.items >= bounds.maximum_items {
                return Err(boundary("JSON exceeds declared structural bound", code));
            }
            if depth > bounds.maximum_depth {
                return Err(input_error(JsonInputError::Depth, code));
            }
            charge.borrow_mut()(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            self.items += 1;
        }
        Ok(item)
    }
}

pub(super) fn source(
    bytes: &[u8],
    bounds: JsonBounds,
    schema: Option<&'static JsonValueSchema>,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
) -> Result<JsonValueShape> {
    laid_out(bytes, bounds, schema, None, code, charge)
}
fn laid_out(
    bytes: &[u8],
    mut bounds: JsonBounds,
    schema: Option<&'static JsonValueSchema>,
    layout: Option<Placement<'_>>,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
) -> Result<JsonValueShape> {
    bounds.maximum_depth = bounds.maximum_depth.min(256);
    if bytes.len() as u64 > bounds.maximum_bytes {
        return Err(boundary("JSON input exceeds declared byte bound", code));
    }
    let charge = RefCell::new(charge);
    let mut lexical = Lexical::default();
    charge.borrow_mut()(JsonCodecCharge::Read {
        bytes: bytes.len() as u64,
    })?;
    for byte in bytes {
        lexical.byte(*byte, bounds, code, &charge)?;
    }
    if lexical.string || lexical.depth != 0 {
        return Err(input_error(JsonInputError::Incomplete, code));
    }
    let counts = document(
        bytes,
        schema.unwrap_or(&UNKNOWN),
        layout,
        0,
        false,
        &charge,
        &RefCell::new(None),
    )?
    .always;
    finish(
        bytes.len() as u64,
        lexical.items,
        lexical.containers,
        counts,
        bounds,
        Origin::Slice,
        code,
    )
}
fn finish(
    bytes: u64,
    items: u64,
    containers: u64,
    counts: Counts,
    bounds: JsonBounds,
    origin: Origin,
    code: Option<crate::wire::ErrorCode>,
) -> Result<JsonValueShape> {
    if items
        .checked_add(counts.nodes)
        .is_none_or(|items| items > bounds.maximum_items)
    {
        return Err(boundary("JSON item limit exceeded", code));
    }
    if counts.depth > bounds.maximum_depth.min(256) {
        return Err(input_error(JsonInputError::Depth, code));
    }
    Ok(JsonValueShape {
        longest_string: 0,
        input_bytes: bytes,
        lexical_items: items,
        containers,
        virtual_items: counts.nodes,
        raw_bytes: counts.bytes,
        raw_frames: counts.frames,
        unresolved_raw: counts.unresolved,
        origin,
    })
}
#[cfg(feature = "filesystem")]
pub(crate) const fn lexical_json_diagnostic_bytes() -> u64 {
    "JSON exceeds declared structural bound".len() as u64
}
#[cfg(feature = "filesystem")]
pub(crate) fn admit_json<F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8],
    bounds: JsonBounds,
    charge: &mut F,
) -> Result<()> {
    source(bytes, bounds, Some(JsonValueSchema::None), None, charge).map(|_| ())
}

/// Admits a typed document's value shape and the heap its decode allocates
/// at each position of `layout`.
#[cfg(any(test, fuzzing, feature = "test-support"))]
pub(crate) fn admit_json_layout(
    bytes: &[u8],
    bounds: JsonBounds,
    schema: Option<&'static JsonValueSchema>,
    layout: &'static JsonLayout,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
) -> Result<(JsonValueShape, LayoutCost)> {
    admit_typed(bytes, bounds, schema, layout, code, charge)?
        .map_err(|(_, syntax)| super::records::decode_error(syntax, code))
}

/// A typed document's layout admission, or the shape and syntax error of a
/// document the typed decode would fail to parse.
pub(super) type Typed =
    std::result::Result<(JsonValueShape, LayoutCost), (JsonValueShape, serde_json::Error)>;

pub(super) fn admit_typed(
    bytes: &[u8],
    bounds: JsonBounds,
    schema: Option<&'static JsonValueSchema>,
    layout: &'static JsonLayout,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
) -> Result<Typed> {
    // Admission refusals are the caller's boundary; a decode's own failure
    // keeps the caller's code.
    let boundary_code = code.map(|_| crate::wire::ErrorCode::Invalid);
    let cost = RefCell::new(LayoutCost::default());
    let shape = laid_out(
        bytes,
        bounds,
        schema,
        Some(Placement::root(layout, &cost)),
        boundary_code,
        charge,
    )?;
    if shape.unresolved_raw() {
        // A document the walk cannot parse fails the typed decode with the
        // same syntax error, so report that cause. The parse keeps one byte
        // per open container in a vector that doubles, at most twice the
        // admitted depth while it grows.
        let scratch = u64::from(bounds.maximum_depth.min(256)) * 2;
        charge(JsonCodecCharge::Retain { bytes: scratch })?;
        let parsed = serde_json::from_slice::<serde::de::IgnoredAny>(bytes);
        charge(JsonCodecCharge::Release { bytes: scratch })?;
        if let Err(syntax) = parsed {
            return Ok(Err((shape, syntax)));
        }
    }
    // A shape the layout refuses is the typed decode's own rejection, so it
    // keeps the caller's code like the decode error it stands in for.
    typed_cost(shape, cost.into_inner(), code).map(Ok)
}
/// What a typed decode of `bytes` into `T` reserves at its entry point, or
/// `None` when admission refuses the document.
#[cfg(any(test, fuzzing, feature = "test-support"))]
pub(crate) fn layout_reservation<T: super::layout::HeapLayout>(bytes: &[u8]) -> Option<u64> {
    let bounds = JsonBounds {
        maximum_bytes: u64::MAX,
        maximum_items: u64::MAX,
        maximum_depth: 256,
    };
    admit_json_layout(bytes, bounds, None, T::LAYOUT, None, &mut |_| Ok(()))
        .ok()
        .and_then(|(_, cost)| super::records::decoded_bytes::<T>(cost).ok())
}
/// Encodes `value`, reserves its typed decode from the layout, and measures
/// the decode's real peak: the reservation must cover it. Returns both.
#[cfg(test)]
pub(crate) fn assert_layout_covers_decode<T>(value: &T) -> (u64, u64)
where
    T: serde::Serialize + DeserializeOwned + super::layout::HeapLayout,
{
    let bytes = serde_json::to_vec(value).expect("sample encodes");
    let reserved = layout_reservation::<T>(&bytes).expect("layout admits its own encoding");
    let mut decoded = None;
    let measured = acyclic_test_support::measure(|| {
        decoded = Some(serde_json::from_slice::<T>(&bytes));
    })
    .bytes_max as u64;
    assert!(
        decoded.is_some_and(|decoded| decoded.is_ok()),
        "sample decodes"
    );
    assert!(
        measured <= reserved,
        "{} decode peak {measured} exceeds its reservation {reserved}",
        std::any::type_name::<T>()
    );
    (reserved, measured)
}
/// A walk that stopped early, or met a shape its layout does not bound,
/// admits nothing: the typed decode would reject that document.
fn typed_cost(
    mut shape: JsonValueShape,
    cost: LayoutCost,
    code: Option<crate::wire::ErrorCode>,
) -> Result<(JsonValueShape, LayoutCost)> {
    if shape.unresolved_raw() || cost.refused {
        // The typed decode would reject this document: an input failure,
        // classified like every other malformed source.
        return Err(input_error(JsonInputError::Shape, code));
    }
    shape.longest_string = cost.longest();
    Ok((shape, cost))
}

pub(crate) fn json_owned_value_shape_bounded<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &Value,
    bounds: JsonBounds,
    schema: &'static JsonValueSchema,
    charge: &mut F,
) -> Result<JsonValueShape> {
    owned::inspect(value, bounds, schema, None, charge)
}
/// Admits a resident value's shape and the heap a typed decode of it
/// allocates at each position of `layout`.
pub(crate) fn admit_value_layout(
    value: &Value,
    bounds: JsonBounds,
    schema: &'static JsonValueSchema,
    layout: &'static JsonLayout,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
) -> Result<(JsonValueShape, LayoutCost)> {
    let cost = RefCell::new(LayoutCost::default());
    let shape = owned::inspect(
        value,
        bounds,
        schema,
        Some(Placement::root(layout, &cost)),
        charge,
    )?;
    typed_cost(shape, cost.into_inner(), code)
}
mod owned;

/// An object key, and whether the document escapes it.
struct Key;
impl<'de> de::DeserializeSeed<'de> for Key {
    type Value = (String, bool);
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        decoder.deserialize_str(self)
    }
}
impl<'de> Visitor<'de> for Key {
    type Value = (String, bool);
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("object key")
    }
    fn visit_borrowed_str<E: de::Error>(
        self,
        key: &'de str,
    ) -> std::result::Result<Self::Value, E> {
        Ok((key.to_owned(), false))
    }
    fn visit_str<E: de::Error>(self, key: &str) -> std::result::Result<Self::Value, E> {
        Ok((key.to_owned(), true))
    }
}
