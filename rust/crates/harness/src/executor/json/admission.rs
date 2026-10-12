use super::schema::{NONE, ROOT, Schema, UNKNOWN};
use super::*;
use std::{cell::RefCell, iter::Peekable};

type Input<'a> = dyn Iterator<Item = Result<u8>> + 'a;
type Charge<'a> = RefCell<&'a mut dyn FnMut(JsonCodecCharge) -> Result<()>>;
const RAW: &[u8] = b"$serde_json::private::RawValue";

fn next(input: &mut Peekable<&mut Input<'_>>) -> Result<Option<u8>> {
    input.next().transpose()
}
fn peek(input: &mut Peekable<&mut Input<'_>>) -> Result<Option<u8>> {
    match input.peek() {
        Some(Ok(byte)) => Ok(Some(*byte)),
        Some(Err(_)) => next(input),
        None => Ok(None),
    }
}
fn space(input: &mut Peekable<&mut Input<'_>>) -> Result<Option<u8>> {
    while peek(input)?.is_some_and(|byte| byte.is_ascii_whitespace()) {
        next(input)?;
    }
    peek(input)
}

struct Quoted<'a, 'b> {
    input: &'a mut Input<'b>,
    pending: [u8; 4],
    position: usize,
    length: usize,
    done: bool,
    invalid: bool,
}
impl<'a, 'b> Quoted<'a, 'b> {
    fn new(input: &'a mut Input<'b>) -> Self {
        Self {
            input,
            pending: [0; 4],
            position: 0,
            length: 0,
            done: false,
            invalid: false,
        }
    }
    fn hex(&mut self) -> Result<Option<u32>> {
        let mut value = 0;
        for _ in 0..4 {
            let Some(digit) = self
                .input
                .next()
                .transpose()?
                .and_then(|byte| (byte as char).to_digit(16))
            else {
                return Ok(None);
            };
            value = value * 16 + digit;
        }
        Ok(Some(value))
    }
    fn byte(&mut self) -> Result<Option<u8>> {
        if self.position < self.length {
            let byte = self.pending[self.position];
            self.position += 1;
            return Ok(Some(byte));
        }
        if self.done {
            return Ok(None);
        }
        let Some(byte) = self.input.next().transpose()? else {
            self.invalid = true;
            self.done = true;
            return Ok(None);
        };
        if byte == b'"' {
            self.done = true;
            return Ok(None);
        }
        if byte != b'\\' {
            if byte < 0x20 {
                self.invalid = true;
                self.done = true;
                return Ok(None);
            }
            return Ok(Some(byte));
        }
        let byte = match self.input.next().transpose()? {
            Some(b'"') => b'"',
            Some(b'\\') => b'\\',
            Some(b'/') => b'/',
            Some(b'b') => 8,
            Some(b'f') => 12,
            Some(b'n') => b'\n',
            Some(b'r') => b'\r',
            Some(b't') => b'\t',
            Some(b'u') => {
                let mut scalar = self.hex()?;
                if scalar.is_some_and(|value| (0xd800..=0xdbff).contains(&value)) {
                    if self.input.next().transpose()? != Some(b'\\')
                        || self.input.next().transpose()? != Some(b'u')
                    {
                        scalar = None;
                    } else {
                        scalar = scalar.zip(self.hex()?).and_then(|(high, low)| {
                            (0xdc00..=0xdfff)
                                .contains(&low)
                                .then_some(0x10000 + ((high - 0xd800) << 10) + low - 0xdc00)
                        });
                    }
                }
                let Some(character) = scalar.and_then(char::from_u32) else {
                    self.invalid = true;
                    self.done = true;
                    return Ok(None);
                };
                self.length = character.encode_utf8(&mut self.pending).len();
                self.position = 1;
                return Ok(Some(self.pending[0]));
            }
            _ => {
                self.invalid = true;
                self.done = true;
                return Ok(None);
            }
        };
        Ok(Some(byte))
    }
}
impl Iterator for Quoted<'_, '_> {
    type Item = Result<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        self.byte().transpose()
    }
}
fn token(input: &mut Peekable<&mut Input<'_>>) -> Result<([u8; 64], usize, bool)> {
    let mut quoted = Quoted::new(input);
    let (mut bytes, mut length) = ([0; 64], 0);
    for byte in quoted.by_ref() {
        let byte = byte?;
        if length < bytes.len() {
            bytes[length] = byte;
        }
        length = length.saturating_add(1);
    }
    Ok((
        bytes,
        if length <= 64 { length } else { 0 },
        !quoted.invalid && quoted.done,
    ))
}

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
            for index in 0..3 {
                if gate & (1 << index) != 0 {
                    self.gated[index].add(child.always)?;
                }
            }
        } else {
            self.always.add(child.always)?;
            for index in 0..3 {
                self.gated[index].add(child.gated[index])?;
            }
        }
        Ok(())
    }
    fn resolve(&mut self, mask: u8) -> Result<()> {
        for index in 0..3 {
            if mask & (1 << index) != 0 {
                self.always.add(self.gated[index])?;
            }
        }
        self.gated = [Counts::default(); 3];
        Ok(())
    }
}

fn value(
    input: &mut Peekable<&mut Input<'_>>,
    schema: &'static Schema,
    depth: u32,
    virtual_value: bool,
    charge: &Charge<'_>,
) -> Result<(Branches, bool)> {
    let Some(byte) = space(input)? else {
        return Ok((Branches::default(), false));
    };
    next(input)?;
    let mut counts = Branches::default();
    if virtual_value {
        counts.always.nodes = 1;
        counts.always.depth = depth;
    }
    match byte {
        b'"' => {
            let (_, _, valid) = token(input)?;
            Ok((counts, valid))
        }
        b'{' | b'[' => {
            if depth > 256 + usize::BITS {
                counts.always.unresolved = true;
                return Ok((counts, false));
            }
            let object = byte == b'{';
            let end = if object { b'}' } else { b']' };
            let (mut index, mut tag) = (0usize, None);
            loop {
                let Some(byte) = space(input)? else {
                    return Ok((counts, false));
                };
                if byte == end {
                    next(input)?;
                    if !schema.tag.is_empty() {
                        counts.resolve(tag.unwrap_or(0))?;
                    }
                    return Ok((counts, true));
                }
                if index != 0 {
                    if byte != b',' {
                        return Ok((counts, false));
                    }
                    next(input)?;
                }
                let (key, length) = if object {
                    if space(input)? != Some(b'"') {
                        return Ok((counts, false));
                    }
                    next(input)?;
                    let (key, length, valid) = token(input)?;
                    if !valid || space(input)? != Some(b':') {
                        return Ok((counts, false));
                    }
                    next(input)?;
                    if virtual_value {
                        counts.always.nodes = checked(
                            counts.always.nodes.checked_add(1),
                            "JSON item count overflow",
                        )?;
                        counts.always.depth = counts.always.depth.max(depth + 1);
                    }
                    (key, length)
                } else {
                    ([0; 64], 0)
                };
                let key = &key[..length];
                let (child, gate) = schema.child(key, (!object).then_some(index));
                let quoted = space(input)? == Some(b'"');
                let mut selected = if object
                    && index == 0
                    && key == RAW
                    && quoted
                    && std::ptr::eq(child, &ROOT)
                {
                    next(input)?;
                    let mut quoted = Quoted::new(input);
                    let mut bytes = 0u64;
                    let mut decoded = quoted.by_ref().map(|byte| {
                        let byte = byte?;
                        charge.borrow_mut()(JsonCodecCharge::Inspect { items: 1 })?;
                        bytes = checked(bytes.checked_add(1), "JSON byte count overflow")?;
                        Ok(byte)
                    });
                    let mut reader: &mut Input<'_> = &mut decoded;
                    let mut reader = reader.by_ref().peekable();
                    let (mut selected, valid) = value(&mut reader, &ROOT, depth + 1, true, charge)?;
                    let ended = space(&mut reader)?.is_none();
                    while next(&mut reader)?.is_some() {}
                    drop(reader);
                    drop(decoded);
                    selected.always.unresolved |=
                        !valid || !ended || quoted.invalid || !quoted.done;
                    selected.always.bytes = checked(
                        selected.always.bytes.checked_add(bytes),
                        "JSON byte count overflow",
                    )?;
                    selected.always.frames = checked(
                        selected.always.frames.checked_add(1),
                        "JSON RawValue count overflow",
                    )?;
                    if virtual_value {
                        selected.always.nodes = checked(
                            selected.always.nodes.checked_add(1),
                            "JSON item count overflow",
                        )?;
                    }
                    selected
                } else if object && !schema.tag.is_empty() && key == schema.tag.as_bytes() && quoted
                {
                    next(input)?;
                    let (label, length, valid) = token(input)?;
                    if !valid {
                        return Ok((counts, false));
                    }
                    tag = Some(if tag.is_some() {
                        0
                    } else {
                        schema.mask(&label[..length])
                    });
                    let mut child = Branches::default();
                    if virtual_value {
                        child.always.nodes = 1;
                        child.always.depth = depth + 1;
                    }
                    child
                } else {
                    if object && key == schema.tag.as_bytes() && !schema.tag.is_empty() {
                        tag = Some(0);
                    }
                    let (mut selected, valid) =
                        value(input, child, depth + 1, virtual_value, charge)?;
                    if !valid {
                        return Ok((counts, false));
                    }
                    if object && index == 0 && key == RAW && quoted && std::ptr::eq(child, &UNKNOWN)
                    {
                        selected.always.unresolved = true;
                    }
                    selected
                };
                if std::ptr::eq(child, &NONE) {
                    selected = Branches::default();
                }
                counts.add(selected, gate)?;
                index = index.saturating_add(1);
            }
        }
        b',' | b':' | b'}' | b']' => Ok((counts, false)),
        _ => {
            while peek(input)?.is_some_and(|byte| {
                !matches!(byte, b',' | b'}' | b']' | b' ' | b'\r' | b'\n' | b'\t')
            }) {
                next(input)?;
            }
            Ok((counts, true))
        }
    }
}

#[derive(Default)]
pub(super) struct Lexical {
    items: u64,
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
    ) -> Result<()> {
        if self.string {
            if self.escape {
                self.escape = false;
            } else if byte == b'\\' {
                self.escape = true;
            } else if byte == b'"' {
                self.string = false;
            }
            return Ok(());
        }
        let depth = self.depth;
        let item = match byte {
            b'"' => {
                self.string = true;
                self.atom = false;
                true
            }
            b'[' | b'{' => {
                self.depth += 1;
                self.atom = false;
                true
            }
            b']' | b'}' => {
                self.depth = self
                    .depth
                    .checked_sub(1)
                    .ok_or_else(|| boundary("invalid JSON nesting", code))?;
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
            if depth > bounds.maximum_depth || self.items >= bounds.maximum_items {
                return Err(boundary("JSON exceeds declared structural bound", code));
            }
            charge.borrow_mut()(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            self.items += 1;
        }
        Ok(())
    }
}

pub(super) fn source(
    bytes: &[u8],
    mut bounds: JsonBounds,
    schema: Option<JsonValueSchema>,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
) -> Result<JsonValueShape> {
    bounds.maximum_depth = bounds.maximum_depth.min(256);
    if bytes.len() as u64 > bounds.maximum_bytes {
        return Err(boundary("JSON input exceeds declared byte bound", code));
    }
    let charge = RefCell::new(charge);
    let mut lexical = Lexical::default();
    let mut source = bytes.iter().map(|byte| {
        charge.borrow_mut()(JsonCodecCharge::Read { bytes: 1 })?;
        lexical.byte(*byte, bounds, code, &charge)?;
        Ok(*byte)
    });
    let mut reader: &mut Input<'_> = &mut source;
    let mut reader = reader.by_ref().peekable();
    let descriptor = schema.map_or(&UNKNOWN, JsonValueSchema::descriptor);
    let (mut branches, valid) = value(&mut reader, descriptor, 0, false, &charge)?;
    let ended = space(&mut reader)?.is_none();
    while next(&mut reader)?.is_some() {}
    drop(reader);
    drop(source);
    if lexical.string || lexical.depth != 0 {
        return Err(boundary("incomplete JSON document", code));
    }
    branches.always.unresolved |= !valid || !ended;
    finish(
        bytes.len() as u64,
        lexical.items,
        branches.always,
        bounds,
        Origin::Slice,
        code,
    )
}
fn finish(
    bytes: u64,
    items: u64,
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
        return Err(boundary("JSON nesting limit exceeded", code));
    }
    Ok(JsonValueShape {
        input_bytes: bytes,
        lexical_items: items,
        virtual_items: counts.nodes,
        raw_bytes: counts.bytes,
        raw_frames: counts.frames,
        unresolved_raw: counts.unresolved,
        origin,
    })
}
pub(crate) const fn lexical_json_diagnostic_bytes() -> u64 {
    "JSON exceeds declared structural bound".len() as u64
}
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
    schema: JsonValueSchema,
    charge: &mut F,
) -> Result<JsonValueShape> {
    source(bytes, bounds, Some(schema), None, charge)
}

pub(crate) fn json_owned_value_shape_bounded<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &Value,
    bounds: JsonBounds,
    schema: JsonValueSchema,
    charge: &mut F,
) -> Result<JsonValueShape> {
    owned::inspect(value, bounds, schema.descriptor(), charge)
}
mod owned;
