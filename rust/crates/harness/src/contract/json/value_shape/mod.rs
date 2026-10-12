//! Streaming extension of the canonical lexical admission pass.

use super::{
    CodecError, JsonBounds, JsonCodecCharge, JsonValueSchema, JsonValueShape, Result, ShapeOrigin,
};

mod decoder;
use decoder::Decoder;
mod owned;
mod selection;
pub(super) use owned::inspect_owned;

const RAW_KEY: &[u8] = b"$serde_json::private::RawValue";
const KEYS: [&[u8]; 76] = [
    RAW_KEY,
    b"type",
    b"schema",
    b"response_schema",
    b"result",
    b"question",
    b"form",
    b"kind",
    b"interaction_resolved",
    b"resolution",
    b"outcome",
    b"events",
    b"payload",
    b"projection",
    b"interactions",
    b"messages",
    b"content",
    b"context",
    b"model",
    b"options",
    b"tools",
    b"input_schema",
    b"output_schema",
    b"projection_schema",
    b"tool_call",
    b"arguments",
    b"tool_result",
    b"json",
    b"value",
    b"metadata",
    b"completed",
    b"input",
    b"prior",
    b"next",
    b"transition",
    b"state",
    b"status",
    b"initial",
    b"child",
    b"declared",
    b"orchestrated",
    b"spec",
    b"entrypoint",
    b"result_schema",
    b"orchestration",
    b"reduce",
    b"reducer",
    b"ready",
    b"source_pinned",
    b"waiting",
    b"receipt",
    b"phase",
    b"fork",
    b"plan",
    b"request",
    b"preparation",
    b"provider_proof",
    b"statement",
    b"Succeeded",
    b"complete",
    b"selected_context",
    b"mcp_stdio",
    b"params",
    b"authorized_source",
    b"native_snapshot",
    b"read_authorization",
    b"intent",
    b"report",
    b"commands",
    b"seed",
    b"fork_published",
    b"forks",
    b"project_merge_published",
    b"snapshot",
    b"merge",
    b"admission",
];
const _: () = assert!(KEYS.len() < u128::BITS as usize);
const FRAMES: usize = 258 + usize::BITS as usize;
const LAYERS: usize = usize::BITS as usize + 1;
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
impl Selection {
    const IGNORE: Self = Self {
        node: Node::Schema(JsonValueSchema::None),
        scope: Scope::Ignore,
    };
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
struct Match {
    candidates: u128,
    length: usize,
}
impl Match {
    const fn new() -> Self {
        Self {
            candidates: (1 << KEYS.len()) - 1,
            length: 0,
        }
    }
    fn push(&mut self, byte: u8) {
        let mut remaining = self.candidates;
        while remaining != 0 {
            let index = remaining.trailing_zeros() as usize;
            remaining &= remaining - 1;
            if KEYS[index].get(self.length) != Some(&byte) {
                self.candidates &= !(1 << index);
            }
        }
        if self.candidates != 0 {
            self.length += 1;
        }
    }
    fn key(&self) -> &'static [u8] {
        let mut remaining = self.candidates;
        while remaining != 0 {
            let index = remaining.trailing_zeros() as usize;
            remaining &= remaining - 1;
            if self.length == KEYS[index].len() {
                return KEYS[index];
            }
        }
        b""
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Key,
    Colon,
    Value,
    After,
}
#[derive(Clone, Copy)]
struct Frame {
    object: bool,
    phase: Phase,
    first: bool,
    raw: bool,
    key: &'static [u8],
    selection: Selection,
    array_index: u64,
    tag: &'static [u8],
    tag_seen: bool,
    tag_ambiguous: bool,
    candidates: [Stats; 3],
}
impl Frame {
    const EMPTY: Self = Self {
        object: false,
        phase: Phase::Value,
        first: true,
        raw: false,
        key: b"",
        selection: Selection::IGNORE,
        array_index: 0,
        tag: b"",
        tag_seen: false,
        tag_ambiguous: false,
        candidates: [Stats::EMPTY; 3],
    };
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Key,
    Tag,
    Raw,
    Other,
}
#[derive(Clone, Copy)]
struct StringToken {
    decoder: Decoder,
    matched: Match,
    role: Role,
    scope: Scope,
    raw_child: bool,
}
#[derive(Clone, Copy)]
struct Layer {
    base: usize,
    root: Selection,
    string: Option<StringToken>,
    atom: bool,
    started: bool,
    finished: bool,
    broken: bool,
    overflow_depth: usize,
}
impl Layer {
    const EMPTY: Self = Self {
        base: 0,
        root: Selection::IGNORE,
        string: None,
        atom: false,
        started: false,
        finished: false,
        broken: false,
        overflow_depth: 0,
    };
}
#[derive(Clone, Copy)]
struct Pending {
    layer: usize,
    bytes: [u8; 4],
    len: u8,
    position: u8,
}
impl Pending {
    const EMPTY: Self = Self {
        layer: 0,
        bytes: [0; 4],
        len: 0,
        position: 0,
    };
}

pub(super) struct ValueInspector {
    bounds: JsonBounds,
    frames: [Frame; FRAMES],
    frame_len: usize,
    layers: [Layer; LAYERS],
    stats: Stats,
    potential_raw: bool,
}

impl ValueInspector {
    pub(super) fn new(schema: Option<JsonValueSchema>, bounds: JsonBounds) -> Self {
        let node = schema.map_or(Node::Unknown, Node::Schema);
        let mut layers = [Layer::EMPTY; LAYERS];
        layers[0].root = Selection {
            node,
            scope: if node == Node::Unknown {
                Scope::Unknown
            } else {
                Scope::Value
            },
        };
        Self {
            bounds,
            frames: [Frame::EMPTY; FRAMES],
            frame_len: 0,
            layers,
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
    fn unresolved(&mut self, scope: Scope) {
        if let Some(stats) = self.stats(scope) {
            stats.unresolved = true;
        }
    }
    fn broken(&mut self, layer: usize, scope: Scope) {
        self.layers[layer].broken = true;
        self.unresolved(scope);
    }
    fn item(&mut self, layer: usize, scope: Scope) {
        let depth = self.frame_len;
        if let Some(stats) = self.stats(scope) {
            stats.depth = stats.depth.max(depth);
            if layer != 0 {
                Stats::add(&mut stats.items, &mut stats.overflow);
            }
        }
    }
    fn complete(&mut self, layer: usize) {
        if self.frame_len == self.layers[layer].base {
            self.layers[layer].finished = true;
        } else {
            let frame = &mut self.frames[self.frame_len - 1];
            frame.phase = Phase::After;
            if !frame.object {
                frame.array_index = frame.array_index.saturating_add(1);
            }
        }
    }
    fn set_tag(&mut self, tag: &'static [u8]) {
        let frame = &mut self.frames[self.frame_len - 1];
        if frame.tag_seen {
            frame.tag_ambiguous = true;
        }
        frame.tag_seen = true;
        frame.tag = tag;
    }
    fn close_frame(&mut self) {
        let index = self.frame_len - 1;
        let frame = &self.frames[index];
        let branches = if frame.tag_ambiguous {
            0
        } else {
            Self::tag_branches(frame.selection.node, frame.tag)
        };
        let scope = frame.selection.scope;
        self.frame_len -= 1;
        for branch in 0..3 {
            if branches & (1 << branch) != 0 {
                let summary = self.frames[index].candidates[branch];
                if let Some(destination) = self.stats(scope) {
                    destination.merge(summary);
                }
            }
        }
    }
    fn close_raw(&mut self, layer: usize, scope: Scope) {
        let child = &self.layers[layer + 1];
        if child.string.is_some() || self.frame_len != child.base || !child.started || child.broken
        {
            self.unresolved(scope);
        }
        self.frame_len = self.layers[layer + 1].base;
        self.layers[layer + 1] = Layer::EMPTY;
    }

    fn feed(&mut self, layer: usize, byte: u8) -> Option<Pending> {
        if let Some(mut string) = self.layers[layer].string {
            let decoded = string.decoder.push(byte);
            if decoded.invalid {
                self.broken(layer, string.scope);
            }
            if decoded.end {
                if string.role == Role::Raw && string.raw_child {
                    self.close_raw(layer, string.scope);
                }
                self.layers[layer].string = None;
                match string.role {
                    Role::Key => {
                        let frame = &mut self.frames[self.frame_len - 1];
                        frame.key = string.matched.key();
                        frame.raw = frame.first && frame.key == RAW_KEY;
                        frame.phase = Phase::Colon;
                    }
                    Role::Tag => {
                        self.set_tag(string.matched.key());
                        self.complete(layer);
                    }
                    _ => {
                        if self.layers[layer].overflow_depth == 0 {
                            self.complete(layer);
                        }
                    }
                }
                return None;
            }
            if matches!(string.role, Role::Key | Role::Tag) {
                for &decoded_byte in &decoded.bytes[..decoded.len as usize] {
                    string.matched.push(decoded_byte);
                }
            }
            self.layers[layer].string = Some(string);
            if string.role == Role::Raw && string.raw_child && decoded.len != 0 {
                return Some(Pending {
                    layer: layer + 1,
                    bytes: decoded.bytes,
                    len: decoded.len,
                    position: 0,
                });
            }
            return None;
        }

        let selection = self.select(layer);
        let scope = Self::value_scope(selection);
        if self.layers[layer].overflow_depth != 0 {
            match byte {
                b'"' => {
                    self.item(layer, scope);
                    self.layers[layer].string = Some(StringToken {
                        decoder: Decoder::new(),
                        matched: Match::new(),
                        role: Role::Other,
                        scope,
                        raw_child: false,
                    });
                    self.layers[layer].atom = false;
                }
                b'{' | b'[' => {
                    self.item(layer, scope);
                    self.layers[layer].overflow_depth =
                        self.layers[layer].overflow_depth.saturating_add(1);
                    self.layers[layer].atom = false;
                }
                b'}' | b']' => {
                    self.layers[layer].overflow_depth -= 1;
                    self.layers[layer].atom = false;
                    if self.layers[layer].overflow_depth == 0 {
                        self.complete(layer);
                    }
                }
                b',' | b':' | b' ' | b'\n' | b'\r' | b'\t' => self.layers[layer].atom = false,
                _ => {
                    if !self.layers[layer].atom {
                        self.item(layer, scope);
                    }
                    self.layers[layer].atom = true;
                }
            }
            return None;
        }
        if self.layers[layer].atom {
            if !matches!(byte, b',' | b'}' | b']' | b' ' | b'\r' | b'\n' | b'\t') {
                return None;
            }
            self.layers[layer].atom = false;
            self.complete(layer);
        }
        if matches!(byte, b' ' | b'\r' | b'\n' | b'\t') {
            return None;
        }
        let has_frame = self.frame_len > self.layers[layer].base;
        if byte == b',' {
            if has_frame {
                if self.frames[self.frame_len - 1].phase != Phase::After {
                    self.broken(layer, scope);
                }
                let frame = &mut self.frames[self.frame_len - 1];
                frame.phase = if frame.object {
                    Phase::Key
                } else {
                    Phase::Value
                };
                frame.first = false;
                frame.raw = false;
            } else {
                self.broken(layer, scope);
            }
            return None;
        }
        if byte == b':' {
            if has_frame && self.frames[self.frame_len - 1].phase == Phase::Colon {
                self.frames[self.frame_len - 1].phase = Phase::Value;
            } else {
                self.broken(layer, scope);
            }
            return None;
        }
        if byte == b'}' || byte == b']' {
            if has_frame {
                if self.frames[self.frame_len - 1].object != (byte == b'}') {
                    self.broken(layer, scope);
                }
                self.close_frame();
                self.complete(layer);
            } else {
                self.broken(layer, scope);
            }
            return None;
        }
        let key = has_frame
            && self.frames[self.frame_len - 1].object
            && self.frames[self.frame_len - 1].phase == Phase::Key;
        let raw = has_frame && self.frames[self.frame_len - 1].raw && !key;
        let tag = has_frame
            && !key
            && !Self::tag_key(self.frames[self.frame_len - 1].selection.node).is_empty()
            && self.frames[self.frame_len - 1].key
                == Self::tag_key(self.frames[self.frame_len - 1].selection.node);
        if self.layers[layer].finished {
            self.broken(layer, scope);
        }
        if has_frame && !key && self.frames[self.frame_len - 1].phase != Phase::Value {
            self.broken(layer, scope);
        }
        self.layers[layer].started = true;
        let scope = if key {
            Self::value_scope(self.frames[self.frame_len - 1].selection)
        } else {
            scope
        };
        self.item(layer, scope);
        if byte == b'"' {
            let mut role = if key {
                Role::Key
            } else if tag {
                Role::Tag
            } else {
                Role::Other
            };
            let mut raw_child = false;
            if raw {
                match scope {
                    Scope::Unknown => self.potential_raw = true,
                    Scope::Ignore => {}
                    _ => {
                        role = Role::Raw;
                        if let Some(stats) = self.stats(scope) {
                            Stats::add(&mut stats.raw_frames, &mut stats.overflow);
                        }
                        if layer + 1 < LAYERS {
                            self.layers[layer + 1] = Layer {
                                base: self.frame_len,
                                root: Selection {
                                    node: Node::Schema(JsonValueSchema::Root),
                                    scope,
                                },
                                ..Layer::EMPTY
                            };
                            raw_child = true;
                        } else if let Some(stats) = self.stats(scope) {
                            stats.overflow = true;
                            stats.unresolved = true;
                        }
                    }
                }
            }
            self.layers[layer].string = Some(StringToken {
                decoder: Decoder::new(),
                matched: Match::new(),
                role,
                scope,
                raw_child,
            });
        } else if byte == b'{' || byte == b'[' {
            if key || raw {
                self.broken(layer, scope);
            }
            if tag {
                self.set_tag(b"");
            }
            if self.frame_len == FRAMES {
                if let Some(stats) = self.stats(scope) {
                    stats.overflow = true;
                }
                self.layers[layer].overflow_depth = 1;
            } else {
                let object = byte == b'{';
                let node = if selection.node == Node::Schema(JsonValueSchema::ModelContent) {
                    if object {
                        Node::Schema(JsonValueSchema::ModelContentPart)
                    } else {
                        Node::Parts
                    }
                } else {
                    selection.node
                };
                self.frames[self.frame_len] = Frame {
                    object,
                    phase: if object { Phase::Key } else { Phase::Value },
                    selection: Selection { node, ..selection },
                    ..Frame::EMPTY
                };
                self.frame_len += 1;
            }
        } else {
            if key || raw {
                self.broken(layer, scope);
            }
            if tag {
                self.set_tag(b"");
            }
            self.layers[layer].atom = true;
        }
        None
    }

    pub(super) fn push<F: FnMut(JsonCodecCharge) -> Result<()>>(
        &mut self,
        byte: u8,
        charge: &mut F,
    ) -> std::result::Result<(), CodecError> {
        if self.layers[0].root.node == Node::Schema(JsonValueSchema::None) {
            return Ok(());
        }
        self.push_layer(0, byte, charge)
    }

    fn push_layer<F: FnMut(JsonCodecCharge) -> Result<()>>(
        &mut self,
        initial_layer: usize,
        byte: u8,
        charge: &mut F,
    ) -> std::result::Result<(), CodecError> {
        let mut pending = [Pending::EMPTY; LAYERS];
        pending[0] = Pending {
            layer: initial_layer,
            bytes: [byte, 0, 0, 0],
            len: 1,
            position: 0,
        };
        let mut length = 1;
        while length != 0 {
            let current = &mut pending[length - 1];
            if current.position == current.len {
                length -= 1;
                continue;
            }
            let byte = current.bytes[current.position as usize];
            let layer = current.layer;
            current.position += 1;
            if layer != 0 {
                charge(JsonCodecCharge::Inspect { items: 1 })?;
                if let Some(stats) = self.stats(self.layers[layer].root.scope) {
                    Stats::add(&mut stats.bytes, &mut stats.overflow);
                }
            }
            if let Some(emitted) = self.feed(layer, byte) {
                pending[length] = emitted;
                length += 1;
            }
        }
        Ok(())
    }

    pub(super) fn finish(
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
            unresolved_raw: stats.unresolved
                || (self.layers[0].root.node == Node::Unknown && self.potential_raw),
            origin: ShapeOrigin::Slice,
        })
    }
}
