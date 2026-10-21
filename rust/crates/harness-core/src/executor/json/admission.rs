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
    value_string_bytes: u64,
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
        self.value_string_bytes = checked(
            self.value_string_bytes
                .checked_add(other.value_string_bytes),
            "JSON Value string count overflow",
        )?;
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
    ) -> Walk<'a, 'b> {
        Walk {
            schema,
            raw,
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
    fn visit_str<E: de::Error>(self, text: &str) -> std::result::Result<Branches, E> {
        let mut counts = self.base();
        // Value accepts every JSON string. These decoded source bytes cannot
        // appear in an invalid-type diagnostic at a typed metadata boundary.
        // Virtual RawValue expansion has no disjoint original source span.
        if !self.virtual_value && self.raw.is_none() && std::ptr::eq(self.schema, &ROOT) {
            counts.always.value_string_bytes = text.len() as u64;
        }
        match self.raw {
            Some(schema) if std::ptr::eq(schema, &ROOT) => {
                self.charge.borrow_mut()(JsonCodecCharge::Inspect {
                    items: text.len() as u64,
                })
                .map_err(|error| self.error::<E>(error))?;
                let mut nested = document(
                    text.as_bytes(),
                    &ROOT,
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
    fn visit_seq<A: SeqAccess<'de>>(
        self,
        mut values: A,
    ) -> std::result::Result<Branches, A::Error> {
        let mut counts = self.base();
        let mut index = 0;
        loop {
            let (schema, gate) = self.schema.child(b"", Some(index));
            let Some(child) = values.next_element_seed(self.child(schema, None))? else {
                break;
            };
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
        let (mut index, mut tag) = (0, None);
        while let Some(key) = fields.next_key::<String>()? {
            let key = key.as_bytes();
            if !self.virtual_value && std::ptr::eq(self.schema, &ROOT) {
                counts.always.value_string_bytes = checked(
                    counts
                        .always
                        .value_string_bytes
                        .checked_add(key.len() as u64),
                    "JSON Value key count overflow",
                )
                .map_err(|error| self.error::<A::Error>(error))?;
            }
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
                let label = serde_json::from_str::<String>(label.get()).unwrap_or_default();
                tag = Some(if tag.is_some() {
                    0
                } else {
                    self.schema.mask(label.as_bytes())
                });
                self.child(schema, None).base()
            } else {
                fields.next_value_seed(
                    self.child(schema, (index == 0 && key == RAW).then_some(schema)),
                )?
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
    mut bounds: JsonBounds,
    schema: Option<&'static JsonValueSchema>,
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
        input_bytes: bytes,
        value_string_bytes: counts.value_string_bytes,
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
pub(crate) fn admit_json_value_shape<F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8],
    bounds: JsonBounds,
    schema: &'static JsonValueSchema,
    charge: &mut F,
) -> Result<JsonValueShape> {
    source(bytes, bounds, Some(schema), None, charge)
}

pub(crate) fn json_owned_value_shape_bounded<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &Value,
    bounds: JsonBounds,
    schema: &'static JsonValueSchema,
    charge: &mut F,
) -> Result<JsonValueShape> {
    owned::inspect(value, bounds, schema, charge)
}
mod owned;
