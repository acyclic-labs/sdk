//! Allocation-free admission of the Value fields Serde will actually decode.
use super::{
    CodecError, JsonBounds, JsonCodecCharge, JsonValueSchema, JsonValueShape, Result, ShapeOrigin,
};
mod decoder;
mod input;
use input::{Input, Slice, StringInput, Token};
mod owned;
pub(super) use owned::inspect_owned;
mod selection;
const RAW_KEY: &[u8] = b"$serde_json::private::RawValue";
const FRAMES: usize = 258 + usize::BITS as usize;
const ITEM_LIMIT: &str = "JSON item limit exceeded";
const DEPTH_LIMIT: &str = "JSON nesting limit exceeded";
#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    Ignore,
    Value,
    Unknown,
    Gate(u16, u8),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Node {
    Schema(JsonValueSchema),
    Unknown,
    Resolution,
    Events,
    Event,
    Projection,
    Resolutions,
    ResolutionTuple,
    Messages,
    Message,
    Model,
    Tools,
    Tool,
    Parts,
    StateRecord,
    Transition,
    Status,
    OperationSpec,
    EntrypointRef,
    Orchestration,
    ProviderProof,
    McpStdioRequest,
    ForkPreparationCommands,
    ForkPreparationCommandResult,
    ForkReportOutcome,
    Forks,
    ForkTuple,
    RecursivePlanPayload,
}
#[derive(Clone, Copy)]
struct Selection {
    node: Node,
    scope: Scope,
}
#[derive(Clone, Copy, Default)]
struct Stats {
    items: u64,
    bytes: u64,
    raw_frames: u64,
    depth: usize,
    unresolved: bool,
    overflow: bool,
}
impl Stats {
    const EMPTY: Self = Self {
        items: 0,
        bytes: 0,
        raw_frames: 0,
        depth: 0,
        unresolved: false,
        overflow: false,
    };
    fn add(value: &mut u64, overflow: &mut bool) {
        if let Some(next) = value.checked_add(1) {
            *value = next;
        } else {
            *overflow = true;
        }
    }
    fn merge(&mut self, other: Self) {
        for (target, source) in [
            (&mut self.items, other.items),
            (&mut self.bytes, other.bytes),
            (&mut self.raw_frames, other.raw_frames),
        ] {
            if let Some(sum) = target.checked_add(source) {
                *target = sum;
            } else {
                self.overflow = true;
            }
        }
        self.depth = self.depth.max(other.depth);
        self.unresolved |= other.unresolved;
        self.overflow |= other.overflow;
    }
}

#[derive(Clone, Copy)]
struct Frame {
    object: bool,
    selection: Selection,
    key: Token,
    array_index: u64,
    tag: Token,
    tag_seen: bool,
    tag_ambiguous: bool,
    candidates: [Stats; 3],
}
impl Frame {
    const EMPTY: Self = Self {
        object: false,
        selection: Selection {
            node: Node::Schema(JsonValueSchema::None),
            scope: Scope::Ignore,
        },
        key: Token::EMPTY,
        array_index: 0,
        tag: Token::EMPTY,
        tag_seen: false,
        tag_ambiguous: false,
        candidates: [Stats::EMPTY; 3],
    };
}
struct ValueInspector {
    bounds: JsonBounds,
    frames: [Frame; FRAMES],
    frame_len: usize,
    stats: Stats,
    potential_raw: bool,
}
impl ValueInspector {
    fn new(bounds: JsonBounds) -> Self {
        Self {
            bounds,
            frames: [Frame::EMPTY; FRAMES],
            frame_len: 0,
            stats: Stats::EMPTY,
            potential_raw: false,
        }
    }
    fn stats(&mut self, scope: Scope) -> Option<&mut Stats> {
        match scope {
            Scope::Value => Some(&mut self.stats),
            Scope::Gate(frame, branch) => {
                Some(&mut self.frames[frame as usize].candidates[branch as usize])
            }
            _ => None,
        }
    }
    fn item(&mut self, scope: Scope, virtual_value: bool) {
        let depth = self.frame_len;
        if let Some(stats) = self.stats(scope) {
            stats.depth = stats.depth.max(depth);
            if virtual_value {
                Stats::add(&mut stats.items, &mut stats.overflow);
            }
        }
    }
    fn tag(&mut self, tag: Token) {
        let frame = &mut self.frames[self.frame_len - 1];
        frame.tag_ambiguous |= frame.tag_seen;
        frame.tag_seen = true;
        frame.tag = tag;
    }
    fn enter(&mut self, mut selection: Selection, object: bool) -> bool {
        if self.frame_len == FRAMES {
            return false;
        }
        if selection.node == Node::Schema(JsonValueSchema::ModelContent) {
            selection.node = if object {
                Node::Schema(JsonValueSchema::ModelContentPart)
            } else {
                Node::Parts
            };
        }
        self.frames[self.frame_len] = Frame {
            object,
            selection,
            ..Frame::EMPTY
        };
        self.frame_len += 1;
        true
    }
    fn close(&mut self) {
        self.frame_len -= 1;
        let frame = self.frames[self.frame_len];
        let branches = if frame.tag_ambiguous {
            0
        } else {
            Self::tag_branches(frame.selection.node, frame.tag.bytes())
        };
        for branch in 0..3 {
            if branches & (1 << branch) != 0 {
                if let Some(stats) = self.stats(frame.selection.scope) {
                    stats.merge(frame.candidates[branch]);
                }
            }
        }
    }
    fn raw(
        &mut self,
        input: &mut dyn Input,
        scope: Scope,
        charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
    ) -> std::result::Result<(), CodecError> {
        if scope == Scope::Unknown {
            self.potential_raw = true;
        } else if scope != Scope::Ignore {
            if let Some(stats) = self.stats(scope) {
                Stats::add(&mut stats.raw_frames, &mut stats.overflow);
            }
            let base = self.frame_len;
            let valid = self.parse(
                input,
                Selection {
                    node: Node::Schema(JsonValueSchema::Root),
                    scope,
                },
                true,
                charge,
            )? && input.whitespace(charge)?.is_none();
            if !valid {
                if let Some(stats) = self.stats(scope) {
                    stats.unresolved = true;
                }
            }
            self.frame_len = base;
        }
        while input.next(charge)?.is_some() {}
        if let Some(stats) = self.stats(scope) {
            stats.unresolved |= !input.valid();
            stats.merge(Stats {
                bytes: input.decoded_bytes(),
                ..Stats::EMPTY
            });
        }
        Ok(())
    }
    fn parse(
        &mut self,
        input: &mut dyn Input,
        selection: Selection,
        virtual_value: bool,
        charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
    ) -> std::result::Result<bool, CodecError> {
        let Some(byte) = input.whitespace(charge)? else {
            return Ok(false);
        };
        let scope = Self::value_scope(selection);
        self.item(scope, virtual_value);
        input.next(charge)?;
        match byte {
            b'"' => {
                let (_, valid) = Token::read(input, charge)?;
                Ok(valid)
            }
            b'{' | b'[' => {
                let object = byte == b'{';
                if !self.enter(selection, object) {
                    return Ok(false);
                }
                let frame = self.frame_len - 1;
                let end = if object { b'}' } else { b']' };
                let mut first = true;
                loop {
                    let Some(next) = input.whitespace(charge)? else {
                        return Ok(false);
                    };
                    if next == end {
                        input.next(charge)?;
                        self.close();
                        return Ok(true);
                    }
                    if !first {
                        if next != b',' {
                            return Ok(false);
                        }
                        input.next(charge)?;
                    }
                    if object {
                        if input.whitespace(charge)? != Some(b'"') {
                            return Ok(false);
                        }
                        input.next(charge)?;
                        self.item(
                            Self::value_scope(self.frames[frame].selection),
                            virtual_value,
                        );
                        let (key, valid) = Token::read(input, charge)?;
                        if !valid || input.whitespace(charge)? != Some(b':') {
                            return Ok(false);
                        }
                        input.next(charge)?;
                        self.frames[frame].key = key;
                    }
                    let child = self.select(0);
                    let value = input.whitespace(charge)?;
                    let raw = object && first && self.frames[frame].key.bytes() == RAW_KEY;
                    let tag = object
                        && !Self::tag_key(self.frames[frame].selection.node).is_empty()
                        && self.frames[frame].key.bytes()
                            == Self::tag_key(self.frames[frame].selection.node);
                    if raw && Self::value_scope(child) != Scope::Ignore && value == Some(b'"') {
                        self.item(Self::value_scope(child), virtual_value);
                        input.next(charge)?;
                        let mut raw = StringInput::new(input);
                        raw.inspect = true;
                        self.raw(&mut raw, Self::value_scope(child), charge)?;
                    } else if tag && value == Some(b'"') {
                        input.next(charge)?;
                        self.item(Self::value_scope(child), virtual_value);
                        let (tag, valid) = Token::read(input, charge)?;
                        if !valid {
                            return Ok(false);
                        }
                        self.tag(tag);
                    } else {
                        if tag {
                            self.tag(Token::EMPTY);
                        }
                        if !self.parse(input, child, virtual_value, charge)? {
                            return Ok(false);
                        }
                    }
                    self.frames[frame].array_index =
                        self.frames[frame].array_index.saturating_add(1);
                    first = false;
                }
            }
            _ => {
                if matches!(byte, b',' | b':' | b'}' | b']') {
                    return Ok(false);
                }
                while let Some(next) = input.peek(charge)? {
                    if matches!(next, b',' | b'}' | b']' | b' ' | b'\r' | b'\n' | b'\t') {
                        break;
                    }
                    input.next(charge)?;
                }
                Ok(true)
            }
        }
    }
    fn finish(
        &self,
        input_bytes: u64,
        lexical_items: u64,
    ) -> std::result::Result<JsonValueShape, CodecError> {
        let stats = self.stats;
        if stats.overflow
            || lexical_items
                .checked_add(stats.items)
                .is_none_or(|items| items > self.bounds.maximum_items)
        {
            return Err(CodecError::Bound(ITEM_LIMIT));
        }
        if stats.depth > self.bounds.maximum_depth.min(256) as usize {
            return Err(CodecError::Bound(DEPTH_LIMIT));
        }
        Ok(JsonValueShape {
            input_bytes,
            lexical_items,
            virtual_items: stats.items,
            raw_bytes: stats.bytes,
            raw_frames: stats.raw_frames,
            unresolved_raw: stats.unresolved || self.potential_raw,
            origin: ShapeOrigin::Slice,
        })
    }
}
pub(super) fn inspect_source(
    bytes: &[u8],
    bounds: JsonBounds,
    schema: Option<JsonValueSchema>,
    charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
) -> std::result::Result<JsonValueShape, CodecError> {
    let mut inspector = ValueInspector::new(bounds);
    let mut source = Slice::new(bytes, Some(bounds));
    let node = schema.map_or(Node::Unknown, Node::Schema);
    let scope = if schema.is_none() {
        Scope::Unknown
    } else {
        Scope::Value
    };
    let valid = inspector.parse(&mut source, Selection { node, scope }, false, charge)?
        && source.whitespace(charge)?.is_none();
    if !valid {
        inspector.stats.unresolved = true;
    }
    while source.next(charge)?.is_some() {}
    source.scan.end_inline()?;
    inspector.finish(source.scan.bytes, source.scan.items)
}
