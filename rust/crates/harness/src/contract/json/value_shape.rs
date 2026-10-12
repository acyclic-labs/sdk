//! Streaming extension of the canonical lexical admission pass.

use super::{CodecError, JsonBounds, JsonCodecCharge, JsonValueSchema, JsonValueShape, Result, ShapeOrigin};

const RAW_KEY: &[u8] = b"$serde_json::private::RawValue";
const KEYS: [&[u8]; 76] = [
    RAW_KEY, b"type", b"schema", b"response_schema", b"result", b"question", b"form",
    b"kind", b"interaction_resolved", b"resolution", b"outcome", b"events",
    b"payload", b"projection", b"interactions", b"messages", b"content", b"context",
    b"model", b"options", b"tools", b"input_schema", b"output_schema", b"projection_schema",
    b"tool_call", b"arguments", b"tool_result", b"json", b"value", b"metadata",
    b"completed", b"input", b"prior", b"next", b"transition", b"state", b"status",
    b"initial", b"child",
    b"declared", b"orchestrated", b"spec", b"entrypoint", b"result_schema",
    b"orchestration", b"reduce", b"reducer",
    b"ready", b"source_pinned", b"waiting", b"receipt", b"phase", b"fork", b"plan",
    b"request", b"preparation", b"provider_proof", b"statement", b"Succeeded", b"complete",
    b"selected_context", b"mcp_stdio", b"params",
    b"authorized_source", b"native_snapshot", b"read_authorization", b"intent", b"report",
    b"commands", b"seed", b"fork_published", b"forks", b"project_merge_published",
    b"snapshot", b"merge", b"admission",
];
const _: () = assert!(KEYS.len() < u128::BITS as usize);
const FRAMES: usize = 258 + usize::BITS as usize;
const LAYERS: usize = usize::BITS as usize + 1;
const ITEM_LIMIT: &str = "JSON item limit exceeded";
const DEPTH_LIMIT: &str = "JSON nesting limit exceeded";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope { Ignore, Value, Unknown, Gate(u16, u8) }

#[derive(Clone, Copy, PartialEq, Eq)]
enum Node {
    Schema(JsonValueSchema), Unknown, Resolution, Events, Event, Projection, Resolutions, ResolutionTuple, Messages, Message, Model, Tools, Tool, Parts, StateRecord, Transition, Status, OperationSpec, EntrypointRef, Orchestration, ProviderProof, McpStdioRequest, ForkPreparationCommands, ForkPreparationCommandResult, ForkReportOutcome, Forks, ForkTuple, RecursivePlanPayload,
}
#[derive(Clone, Copy)]
struct Selection { node: Node, scope: Scope }
impl Selection {
    const IGNORE: Self = Self { node: Node::Schema(JsonValueSchema::None), scope: Scope::Ignore };
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
    const EMPTY: Self = Self { items: 0, bytes: 0, raw_frames: 0, depth: 0, unresolved: false, overflow: false };
    fn add(value: &mut u64, overflow: &mut bool) {
        if let Some(next) = value.checked_add(1) { *value = next; } else { *overflow = true; }
    }
    fn merge(&mut self, other: Self) {
        for (target, source) in [(&mut self.items, other.items), (&mut self.bytes, other.bytes), (&mut self.raw_frames, other.raw_frames)] {
            if let Some(sum) = target.checked_add(source) { *target = sum; } else { self.overflow = true; }
        }
        self.depth = self.depth.max(other.depth);
        self.unresolved |= other.unresolved;
        self.overflow |= other.overflow;
    }
}

#[derive(Clone, Copy)]
struct Match { candidates: u128, length: usize }
impl Match {
    const fn new() -> Self { Self { candidates: (1 << KEYS.len()) - 1, length: 0 } }
    fn push(&mut self, byte: u8) {
        let mut remaining = self.candidates;
        while remaining != 0 {
            let index = remaining.trailing_zeros() as usize;
            remaining &= remaining - 1;
            if KEYS[index].get(self.length) != Some(&byte) { self.candidates &= !(1 << index); }
        }
        if self.candidates != 0 { self.length += 1; }
    }
    fn key(&self) -> &'static [u8] {
        let mut remaining = self.candidates;
        while remaining != 0 {
            let index = remaining.trailing_zeros() as usize;
            remaining &= remaining - 1;
            if self.length == KEYS[index].len() { return KEYS[index]; }
        }
        b""
    }
}

#[derive(Clone, Copy)]
enum Escape { Plain, Slash, Hex(u8, u16), LowSlash(u16), LowU(u16), LowHex(u16, u8, u16) }
#[derive(Clone, Copy)]
struct Decoder { escape: Escape, utf8_left: u8, utf8_low: u8, utf8_high: u8 }
#[derive(Clone, Copy)]
struct Decoded { bytes: [u8; 4], len: u8, end: bool, invalid: bool }
impl Decoded {
    const fn empty() -> Self { Self { bytes: [0; 4], len: 0, end: false, invalid: false } }
    fn byte(byte: u8) -> Self { Self { bytes: [byte, 0, 0, 0], len: 1, ..Self::empty() } }
    fn scalar(scalar: u32) -> Self {
        let mut decoded = Self::empty();
        if let Some(character) = char::from_u32(scalar) {
            decoded.len = character.encode_utf8(&mut decoded.bytes).len() as u8;
        } else { decoded.invalid = true; }
        decoded
    }
}
impl Decoder {
    const fn new() -> Self { Self { escape: Escape::Plain, utf8_left: 0, utf8_low: 0x80, utf8_high: 0xbf } }
    fn hex(byte: u8) -> Option<u16> {
        match byte { b'0'..=b'9' => Some((byte - b'0') as u16), b'a'..=b'f' => Some((byte - b'a' + 10) as u16), b'A'..=b'F' => Some((byte - b'A' + 10) as u16), _ => None }
    }
    fn push(&mut self, byte: u8) -> Decoded {
        let invalid = || Decoded { invalid: true, ..Decoded::empty() };
        match self.escape {
            Escape::Plain => {
                if self.utf8_left != 0 {
                    if !(self.utf8_low..=self.utf8_high).contains(&byte) { return invalid(); }
                    self.utf8_left -= 1;
                    self.utf8_low = 0x80;
                    self.utf8_high = 0xbf;
                    return Decoded::byte(byte);
                }
                match byte {
                    b'"' => Decoded { end: true, ..Decoded::empty() },
                    b'\\' => { self.escape = Escape::Slash; Decoded::empty() }
                    0..=0x1f => invalid(),
                    0x20..=0x7f => Decoded::byte(byte),
                    0xc2..=0xdf => { self.utf8_left = 1; Decoded::byte(byte) }
                    0xe0..=0xef => {
                        self.utf8_left = 2;
                        if byte == 0xe0 { self.utf8_low = 0xa0; }
                        if byte == 0xed { self.utf8_high = 0x9f; }
                        Decoded::byte(byte)
                    }
                    0xf0..=0xf4 => {
                        self.utf8_left = 3;
                        if byte == 0xf0 { self.utf8_low = 0x90; }
                        if byte == 0xf4 { self.utf8_high = 0x8f; }
                        Decoded::byte(byte)
                    }
                    _ => invalid(),
                }
            }
            Escape::Slash => {
                self.escape = Escape::Plain;
                match byte {
                    b'"' | b'\\' | b'/' => Decoded::byte(byte),
                    b'b' => Decoded::byte(8), b'f' => Decoded::byte(12),
                    b'n' => Decoded::byte(b'\n'), b'r' => Decoded::byte(b'\r'), b't' => Decoded::byte(b'\t'),
                    b'u' => { self.escape = Escape::Hex(0, 0); Decoded::empty() }
                    _ => invalid(),
                }
            }
            Escape::Hex(count, value) => {
                let Some(digit) = Self::hex(byte) else { self.escape = Escape::Plain; return invalid(); };
                let value = (value << 4) | digit;
                if count != 3 { self.escape = Escape::Hex(count + 1, value); return Decoded::empty(); }
                self.escape = Escape::Plain;
                if (0xd800..=0xdbff).contains(&value) {
                    self.escape = Escape::LowSlash(value);
                    Decoded::empty()
                } else { Decoded::scalar(value as u32) }
            }
            Escape::LowSlash(high) => {
                if byte != b'\\' { self.escape = Escape::Plain; return invalid(); }
                self.escape = Escape::LowU(high);
                Decoded::empty()
            }
            Escape::LowU(high) => {
                if byte != b'u' { self.escape = Escape::Plain; return invalid(); }
                self.escape = Escape::LowHex(high, 0, 0);
                Decoded::empty()
            }
            Escape::LowHex(high, count, value) => {
                let Some(digit) = Self::hex(byte) else { self.escape = Escape::Plain; return invalid(); };
                let value = (value << 4) | digit;
                if count != 3 { self.escape = Escape::LowHex(high, count + 1, value); return Decoded::empty(); }
                self.escape = Escape::Plain;
                if !(0xdc00..=0xdfff).contains(&value) { return invalid(); }
                Decoded::scalar(0x10000 + (((high as u32) - 0xd800) << 10) + value as u32 - 0xdc00)
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase { Key, Colon, Value, After }
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
    const EMPTY: Self = Self { object: false, phase: Phase::Value, first: true, raw: false,
        key: b"", selection: Selection::IGNORE, array_index: 0, tag: b"", tag_seen: false,
        tag_ambiguous: false, candidates: [Stats::EMPTY; 3] };
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role { Key, Tag, Raw, Other }
#[derive(Clone, Copy)]
struct StringToken { decoder: Decoder, matched: Match, role: Role, scope: Scope, raw_child: bool }
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
    const EMPTY: Self = Self { base: 0, root: Selection::IGNORE, string: None, atom: false, started: false, finished: false, broken: false, overflow_depth: 0 };
}
#[derive(Clone, Copy)]
struct Pending { layer: usize, bytes: [u8; 4], len: u8, position: u8 }
impl Pending { const EMPTY: Self = Self { layer: 0, bytes: [0; 4], len: 0, position: 0 }; }

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
        layers[0].root = Selection { node, scope: if node == Node::Unknown { Scope::Unknown } else { Scope::Value } };
        Self { bounds, frames: [Frame::EMPTY; FRAMES], frame_len: 0, layers, stats: Stats::EMPTY, potential_raw: false }
    }

    fn stats(&mut self, scope: Scope) -> Option<&mut Stats> {
        match scope {
            Scope::Value => Some(&mut self.stats),
            Scope::Gate(frame, branch) => Some(&mut self.frames[frame as usize].candidates[branch as usize]),
            _ => None,
        }
    }
    fn unresolved(&mut self, scope: Scope) {
        if let Some(stats) = self.stats(scope) { stats.unresolved = true; }
    }
    fn broken(&mut self, layer: usize, scope: Scope) {
        self.layers[layer].broken = true;
        self.unresolved(scope);
    }
    fn value_scope(selection: Selection) -> Scope {
        if matches!(selection.node, Node::Schema(JsonValueSchema::Root) | Node::Unknown) { selection.scope } else { Scope::Ignore }
    }
    fn tag_key(node: Node) -> &'static [u8] {
        match node {
            Node::Schema(JsonValueSchema::Interaction) | Node::Schema(JsonValueSchema::InteractionResponse) => b"type",
            Node::Schema(JsonValueSchema::EventPayload) | Node::Schema(JsonValueSchema::ModelContentPart) | Node::Schema(JsonValueSchema::ToolResultContent) => b"kind",
            Node::Schema(JsonValueSchema::ModelEvent) => b"kind",
            Node::Status => b"kind",
            Node::Schema(JsonValueSchema::SchedulerEvent) | Node::Orchestration | Node::Schema(JsonValueSchema::RecursivePlan) => b"kind",
            Node::Schema(JsonValueSchema::RecursiveState) => b"phase",
            _ => b"",
        }
    }
    fn tag_branches(node: Node, tag: &[u8]) -> u8 {
        match (node, tag) {
            (Node::Schema(JsonValueSchema::Interaction), b"question") | (Node::Schema(JsonValueSchema::EventPayload), b"interaction_resolved")
                | (Node::Schema(JsonValueSchema::ModelContentPart), b"tool_call") | (Node::Schema(JsonValueSchema::ToolResultContent), b"json")
                | (Node::Schema(JsonValueSchema::ModelEvent), b"tool_call") | (Node::Status, b"completed") => 1,
            (Node::Schema(JsonValueSchema::Interaction), b"form") | (Node::Schema(JsonValueSchema::ModelContentPart), b"tool_result")
                | (Node::Schema(JsonValueSchema::ModelEvent), b"completed") => 2,
            (Node::Schema(JsonValueSchema::EventPayload), b"fork_published") => 2,
            (Node::Schema(JsonValueSchema::EventPayload), b"project_merge_published") => 4,
            (Node::Schema(JsonValueSchema::SchedulerEvent), b"declared") | (Node::Orchestration, b"reduce") => 1,
            (Node::Schema(JsonValueSchema::SchedulerEvent), b"orchestrated") => 2,
            (Node::Schema(JsonValueSchema::InteractionResponse), b"question" | b"form") | (Node::Schema(JsonValueSchema::RecursivePlan), b"fork") => 1,
            (Node::Schema(JsonValueSchema::RecursivePlan), b"merge") => 2,
            (Node::Schema(JsonValueSchema::RecursiveState), b"ready" | b"source_pinned") => 1,
            (Node::Schema(JsonValueSchema::RecursiveState), b"waiting") => 3,
            (Node::Schema(JsonValueSchema::RecursiveState), b"complete") => 4,
            _ => 0,
        }
    }
    fn select(&self, layer: usize) -> Selection {
        if self.frame_len == self.layers[layer].base { return self.layers[layer].root; }
        let index = self.frame_len - 1;
        let frame = &self.frames[index];
        let scope = frame.selection.scope;
        let gate = |branch| Scope::Gate(index as u16, branch);
        let (node, scope) = match (frame.selection.node, frame.key) {
            (Node::Schema(JsonValueSchema::Root), _) => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Unknown, _) => (Node::Unknown, Scope::Unknown),
            (Node::Schema(JsonValueSchema::Interaction), b"response_schema") => (Node::Schema(JsonValueSchema::Root), gate(0)),
            (Node::Schema(JsonValueSchema::Interaction), b"schema") => (Node::Schema(JsonValueSchema::Root), gate(1)),
            (Node::Schema(JsonValueSchema::ControlResponse), b"result") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::EventPayload), b"resolution") => (Node::Resolution, gate(0)),
            (Node::Schema(JsonValueSchema::EventPayload), b"seed") => (Node::Schema(JsonValueSchema::ForkSeed), gate(1)),
            (Node::Schema(JsonValueSchema::EventPayload), b"receipt") => (Node::Schema(JsonValueSchema::ProjectMergeReceipt), gate(2)),
            (Node::Resolution, b"outcome") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::Snapshot), b"events") => (Node::Events, scope),
            (Node::Schema(JsonValueSchema::Snapshot), b"projection") => (Node::Projection, scope),
            (Node::Events, _) if !frame.object => (Node::Event, scope),
            (Node::Event, b"payload") => (Node::Schema(JsonValueSchema::EventPayload), scope),
            (Node::Projection, b"interactions") => (Node::Resolutions, scope),
            (Node::Projection, b"forks") => (Node::Forks, scope),
            (Node::Forks, _) if !frame.object => (Node::ForkTuple, scope),
            (Node::ForkTuple, _) if !frame.object && frame.array_index == 1 => (Node::Schema(JsonValueSchema::ForkSeed), scope),
            (Node::Resolutions, _) if frame.object => (Node::ResolutionTuple, scope),
            (Node::ResolutionTuple, _) if !frame.object && frame.array_index == 1 => (Node::Resolution, scope),
            (Node::Schema(JsonValueSchema::Context), b"messages") => (Node::Messages, scope),
            (Node::Messages, _) if !frame.object => (Node::Message, scope),
            (Node::Message, b"content") => (Node::Schema(JsonValueSchema::ModelContent), scope),
            (Node::Schema(JsonValueSchema::ResponseProjection), b"context") => (Node::Schema(JsonValueSchema::Context), scope),
            (Node::Schema(JsonValueSchema::ModelRequest), b"model") => (Node::Model, scope),
            (Node::Model, b"options") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::ModelRequest), b"messages") => (Node::Messages, scope),
            (Node::Schema(JsonValueSchema::ModelRequest), b"tools") => (Node::Tools, scope),
            (Node::Tools, _) if !frame.object => (Node::Tool, scope),
            (Node::Tool, b"input_schema" | b"output_schema" | b"projection_schema") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Parts, _) if !frame.object => (Node::Schema(JsonValueSchema::ModelContentPart), scope),
            (Node::Schema(JsonValueSchema::ModelContentPart), b"arguments") => (Node::Schema(JsonValueSchema::Root), gate(0)),
            (Node::Schema(JsonValueSchema::ModelContentPart), b"content") => (Node::Schema(JsonValueSchema::ToolResultContent), gate(1)),
            (Node::Schema(JsonValueSchema::ToolResultContent), b"value") => (Node::Schema(JsonValueSchema::Root), gate(0)),
            (Node::Schema(JsonValueSchema::ModelEvent), b"arguments") => (Node::Schema(JsonValueSchema::Root), gate(0)),
            (Node::Schema(JsonValueSchema::ModelEvent), b"metadata") => (Node::Schema(JsonValueSchema::Root), gate(1)),
            (Node::Schema(JsonValueSchema::ProjectedToolInvocation), b"arguments") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::WorkflowRecord), b"input") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::WorkflowRecord), b"prior" | b"next") => (Node::StateRecord, scope),
            (Node::Schema(JsonValueSchema::WorkflowRecord), b"transition") => (Node::Transition, scope),
            (Node::StateRecord | Node::Transition | Node::Schema(JsonValueSchema::MachineCheckpoint), b"state") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Transition, b"status") => (Node::Status, scope),
            (Node::Status, b"value") => (Node::Schema(JsonValueSchema::Root), gate(0)),
            (Node::Schema(JsonValueSchema::WorkflowAdmission), b"initial") => (Node::StateRecord, scope),
            (Node::Schema(JsonValueSchema::ResourcePreparationRequest), b"payload") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::ResourcePreparationRequest), b"child") => (Node::Schema(JsonValueSchema::TaskAdmissionRecord), scope),
            (Node::Schema(JsonValueSchema::ResourcePreparationRequest), b"read_authorization") => (Node::Schema(JsonValueSchema::ProjectSourceAuthorization), scope),
            (Node::Schema(JsonValueSchema::TaskAdmissionRecord), b"input" | b"input_schema" | b"output_schema") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::SchedulerEvent), b"spec") => (Node::OperationSpec, gate(0)),
            (Node::Schema(JsonValueSchema::SchedulerEvent), b"reducer") => (Node::EntrypointRef, gate(1)),
            (Node::OperationSpec, b"entrypoint") => (Node::EntrypointRef, scope),
            (Node::OperationSpec, b"orchestration") => (Node::Orchestration, scope),
            (Node::Orchestration, b"reducer") => (Node::EntrypointRef, gate(0)),
            (Node::EntrypointRef, b"result_schema") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::InteractionResponse), b"value") => (Node::Schema(JsonValueSchema::Root), gate(0)),
            (Node::Schema(JsonValueSchema::RecursiveForkToolPlan) | Node::RecursivePlanPayload, b"request") => (Node::Schema(JsonValueSchema::ResourcePreparationRequest), scope),
            (Node::Schema(JsonValueSchema::RecursiveForkToolPlan) | Node::RecursivePlanPayload, b"preparation") => (Node::Schema(JsonValueSchema::TaskAdmissionRecord), scope),
            (Node::Schema(JsonValueSchema::RecursivePlan), b"plan") => (Node::RecursivePlanPayload, gate(0)),
            (Node::Schema(JsonValueSchema::RecursiveInitialPlan), b"plan") => (Node::Schema(JsonValueSchema::RecursivePlan), scope),
            (Node::Schema(JsonValueSchema::RecursiveState), b"arguments") => (Node::Schema(JsonValueSchema::Root), gate(0)),
            (Node::Schema(JsonValueSchema::RecursiveState), b"intent") => (Node::Schema(JsonValueSchema::RecursiveForkSourceIntent), gate(0)),
            (Node::Schema(JsonValueSchema::RecursiveState), b"report") => (Node::Schema(JsonValueSchema::ForkReport), gate(1)),
            (Node::Schema(JsonValueSchema::RecursiveState), b"plan") => (Node::Schema(JsonValueSchema::RecursivePlan), gate(1)),
            (Node::Schema(JsonValueSchema::RecursiveState), b"receipt") => (Node::Schema(JsonValueSchema::ProjectMergeReceipt), gate(1)),
            (Node::Schema(JsonValueSchema::RecursiveState), b"value") => (Node::Schema(JsonValueSchema::Root), gate(2)),
            (Node::Schema(JsonValueSchema::RecursiveReceipt), b"value") => (Node::Schema(JsonValueSchema::TaskOutcome), scope),
            (Node::Schema(JsonValueSchema::TaskOutcome), b"Succeeded") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::ProjectMergeReceipt), b"provider_proof") => (Node::ProviderProof, scope),
            (Node::ProviderProof, b"statement") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::ProjectSourceAuthorization), b"native_snapshot") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::ForkRequest) | Node::Schema(JsonValueSchema::ForkSeed), b"authorized_source") => (Node::Schema(JsonValueSchema::ProjectSourceAuthorization), scope),
            (Node::Schema(JsonValueSchema::ForkReport) | Node::Schema(JsonValueSchema::RecursiveForkSourceIntent), b"request") => (Node::Schema(JsonValueSchema::ForkRequest), scope),
            (Node::Schema(JsonValueSchema::RecursiveSourcePin), b"intent") => (Node::Schema(JsonValueSchema::RecursiveForkSourceIntent), scope),
            (Node::Schema(JsonValueSchema::ForkPreparationCommandResults), b"commands") => (Node::ForkPreparationCommands, scope),
            (Node::ForkPreparationCommands, _) if !frame.object => (Node::ForkPreparationCommandResult, scope),
            (Node::ForkPreparationCommandResult, b"value") => (Node::ForkReportOutcome, scope),
            (Node::ForkReportOutcome, b"Succeeded") => (Node::Schema(JsonValueSchema::ForkReport), scope),
            (Node::Schema(JsonValueSchema::ConfiguredRecursiveSourceCheckpoint), b"snapshot") => (Node::Schema(JsonValueSchema::Snapshot), scope),
            (Node::Schema(JsonValueSchema::ConfiguredRecursiveSourceCheckpoint), b"request") => (Node::Schema(JsonValueSchema::ForkRequest), scope),
            (Node::Schema(JsonValueSchema::RecursiveMergeToolPlan), b"authorized_source") => (Node::Schema(JsonValueSchema::ProjectSourceAuthorization), scope),
            (Node::RecursivePlanPayload, b"authorized_source") => {
                let scope = match scope {
                    Scope::Gate(owner, _) => Scope::Gate(owner, 1),
                    _ => Scope::Ignore,
                };
                (Node::Schema(JsonValueSchema::ProjectSourceAuthorization), scope)
            }
            (Node::Schema(JsonValueSchema::ToolTaskCommand), b"arguments") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::TaskAdmitCommand), b"input") => (Node::Schema(JsonValueSchema::Root), scope),
            (Node::Schema(JsonValueSchema::PreparedTaskAdmitCommand), b"admission") => (Node::Schema(JsonValueSchema::TaskAdmissionRecord), scope),
            (Node::Schema(JsonValueSchema::ModelTaskCommand), b"input") => (Node::Schema(JsonValueSchema::ModelContent), scope),
            (Node::Schema(JsonValueSchema::ModelTaskCommand), b"selected_context") => (Node::Schema(JsonValueSchema::Context), scope),
            (Node::Schema(JsonValueSchema::NativeProcessRequest), b"mcp_stdio") => (Node::McpStdioRequest, scope),
            (Node::McpStdioRequest, b"params") => (Node::Schema(JsonValueSchema::Root), scope),
            _ => (Node::Schema(JsonValueSchema::None), Scope::Ignore),
        };
        Selection { node, scope }
    }
    fn item(&mut self, layer: usize, scope: Scope) {
        let depth = self.frame_len;
        if let Some(stats) = self.stats(scope) {
            stats.depth = stats.depth.max(depth);
            if layer != 0 { Stats::add(&mut stats.items, &mut stats.overflow); }
        }
    }
    fn complete(&mut self, layer: usize) {
        if self.frame_len == self.layers[layer].base { self.layers[layer].finished = true; }
        else {
            let frame = &mut self.frames[self.frame_len - 1];
            frame.phase = Phase::After;
            if !frame.object { frame.array_index = frame.array_index.saturating_add(1); }
        }
    }
    fn set_tag(&mut self, tag: &'static [u8]) {
        let frame = &mut self.frames[self.frame_len - 1];
        if frame.tag_seen { frame.tag_ambiguous = true; }
        frame.tag_seen = true;
        frame.tag = tag;
    }
    fn close_frame(&mut self) {
        let index = self.frame_len - 1;
        let frame = &self.frames[index];
        let branches = if frame.tag_ambiguous { 0 } else { Self::tag_branches(frame.selection.node, frame.tag) };
        let scope = frame.selection.scope;
        self.frame_len -= 1;
        for branch in 0..3 {
            if branches & (1 << branch) != 0 {
                let summary = self.frames[index].candidates[branch];
                if let Some(destination) = self.stats(scope) { destination.merge(summary); }
            }
        }
    }
    fn close_raw(&mut self, layer: usize, scope: Scope) {
        let child = &self.layers[layer + 1];
        if child.string.is_some() || self.frame_len != child.base || !child.started || child.broken {
            self.unresolved(scope);
        }
        self.frame_len = self.layers[layer + 1].base;
        self.layers[layer + 1] = Layer::EMPTY;
    }

    fn feed(&mut self, layer: usize, byte: u8) -> Option<Pending> {
        if let Some(mut string) = self.layers[layer].string {
            let decoded = string.decoder.push(byte);
            if decoded.invalid { self.broken(layer, string.scope); }
            if decoded.end {
                if string.role == Role::Raw && string.raw_child { self.close_raw(layer, string.scope); }
                self.layers[layer].string = None;
                match string.role {
                    Role::Key => {
                        let frame = &mut self.frames[self.frame_len - 1];
                        frame.key = string.matched.key();
                        frame.raw = frame.first && frame.key == RAW_KEY;
                        frame.phase = Phase::Colon;
                    }
                    Role::Tag => { self.set_tag(string.matched.key()); self.complete(layer); }
                    _ => { if self.layers[layer].overflow_depth == 0 { self.complete(layer); } }
                }
                return None;
            }
            if matches!(string.role, Role::Key | Role::Tag) {
                for &decoded_byte in &decoded.bytes[..decoded.len as usize] { string.matched.push(decoded_byte); }
            }
            self.layers[layer].string = Some(string);
            if string.role == Role::Raw && string.raw_child && decoded.len != 0 {
                return Some(Pending { layer: layer + 1, bytes: decoded.bytes, len: decoded.len, position: 0 });
            }
            return None;
        }

        let selection = self.select(layer);
        let scope = Self::value_scope(selection);
        if self.layers[layer].overflow_depth != 0 {
            match byte {
                b'"' => {
                    self.item(layer, scope);
                    self.layers[layer].string = Some(StringToken { decoder: Decoder::new(), matched: Match::new(), role: Role::Other, scope, raw_child: false });
                    self.layers[layer].atom = false;
                }
                b'{' | b'[' => { self.item(layer, scope); self.layers[layer].overflow_depth = self.layers[layer].overflow_depth.saturating_add(1); self.layers[layer].atom = false; }
                b'}' | b']' => {
                    self.layers[layer].overflow_depth -= 1;
                    self.layers[layer].atom = false;
                    if self.layers[layer].overflow_depth == 0 { self.complete(layer); }
                }
                b',' | b':' | b' ' | b'\n' | b'\r' | b'\t' => self.layers[layer].atom = false,
                _ => { if !self.layers[layer].atom { self.item(layer, scope); } self.layers[layer].atom = true; }
            }
            return None;
        }
        if self.layers[layer].atom {
            if !matches!(byte, b',' | b'}' | b']' | b' ' | b'\r' | b'\n' | b'\t') { return None; }
            self.layers[layer].atom = false;
            self.complete(layer);
        }
        if matches!(byte, b' ' | b'\r' | b'\n' | b'\t') { return None; }
        let has_frame = self.frame_len > self.layers[layer].base;
        if byte == b',' {
            if has_frame {
                if self.frames[self.frame_len - 1].phase != Phase::After { self.broken(layer, scope); }
                let frame = &mut self.frames[self.frame_len - 1];
                frame.phase = if frame.object { Phase::Key } else { Phase::Value };
                frame.first = false;
                frame.raw = false;
            } else { self.broken(layer, scope); }
            return None;
        }
        if byte == b':' {
            if has_frame && self.frames[self.frame_len - 1].phase == Phase::Colon {
                self.frames[self.frame_len - 1].phase = Phase::Value;
            } else { self.broken(layer, scope); }
            return None;
        }
        if byte == b'}' || byte == b']' {
            if has_frame {
                if self.frames[self.frame_len - 1].object != (byte == b'}') { self.broken(layer, scope); }
                self.close_frame();
                self.complete(layer);
            } else { self.broken(layer, scope); }
            return None;
        }
        let key = has_frame && self.frames[self.frame_len - 1].object && self.frames[self.frame_len - 1].phase == Phase::Key;
        let raw = has_frame && self.frames[self.frame_len - 1].raw && !key;
        let tag = has_frame && !key && !Self::tag_key(self.frames[self.frame_len - 1].selection.node).is_empty()
            && self.frames[self.frame_len - 1].key == Self::tag_key(self.frames[self.frame_len - 1].selection.node);
        if self.layers[layer].finished { self.broken(layer, scope); }
        if has_frame && !key && self.frames[self.frame_len - 1].phase != Phase::Value { self.broken(layer, scope); }
        self.layers[layer].started = true;
        let scope = if key { Self::value_scope(self.frames[self.frame_len - 1].selection) } else { scope };
        self.item(layer, scope);
        if byte == b'"' {
            let mut role = if key { Role::Key } else if tag { Role::Tag } else { Role::Other };
            let mut raw_child = false;
            if raw {
                match scope {
                    Scope::Unknown => self.potential_raw = true,
                    Scope::Ignore => {}
                    _ => {
                        role = Role::Raw;
                        if let Some(stats) = self.stats(scope) { Stats::add(&mut stats.raw_frames, &mut stats.overflow); }
                        if layer + 1 < LAYERS {
                            self.layers[layer + 1] = Layer { base: self.frame_len, root: Selection { node: Node::Schema(JsonValueSchema::Root), scope }, ..Layer::EMPTY };
                            raw_child = true;
                        } else if let Some(stats) = self.stats(scope) { stats.overflow = true; stats.unresolved = true; }
                    }
                }
            }
            self.layers[layer].string = Some(StringToken { decoder: Decoder::new(), matched: Match::new(), role, scope, raw_child });
        } else if byte == b'{' || byte == b'[' {
            if key || raw { self.broken(layer, scope); }
            if tag { self.set_tag(b""); }
            if self.frame_len == FRAMES {
                if let Some(stats) = self.stats(scope) { stats.overflow = true; }
                self.layers[layer].overflow_depth = 1;
            } else {
                let object = byte == b'{';
                let node = if selection.node == Node::Schema(JsonValueSchema::ModelContent) {
                    if object { Node::Schema(JsonValueSchema::ModelContentPart) } else { Node::Parts }
                } else { selection.node };
                self.frames[self.frame_len] = Frame { object, phase: if object { Phase::Key } else { Phase::Value },
                    selection: Selection { node, ..selection }, ..Frame::EMPTY };
                self.frame_len += 1;
            }
        } else {
            if key || raw { self.broken(layer, scope); }
            if tag { self.set_tag(b""); }
            self.layers[layer].atom = true;
        }
        None
    }

    pub(super) fn push<F: FnMut(JsonCodecCharge) -> Result<()>>(
        &mut self, byte: u8, charge: &mut F,
    ) -> std::result::Result<(), CodecError> {
        if self.layers[0].root.node == Node::Schema(JsonValueSchema::None) { return Ok(()); }
        self.push_layer(0, byte, charge)
    }

    fn push_layer<F: FnMut(JsonCodecCharge) -> Result<()>>(
        &mut self, initial_layer: usize, byte: u8, charge: &mut F,
    ) -> std::result::Result<(), CodecError> {
        let mut pending = [Pending::EMPTY; LAYERS];
        pending[0] = Pending { layer: initial_layer, bytes: [byte, 0, 0, 0], len: 1, position: 0 };
        let mut length = 1;
        while length != 0 {
            let current = &mut pending[length - 1];
            if current.position == current.len { length -= 1; continue; }
            let byte = current.bytes[current.position as usize];
            let layer = current.layer;
            current.position += 1;
            if layer != 0 {
                charge(JsonCodecCharge::Inspect { items: 1 })?;
                if let Some(stats) = self.stats(self.layers[layer].root.scope) { Stats::add(&mut stats.bytes, &mut stats.overflow); }
            }
            if let Some(emitted) = self.feed(layer, byte) {
                pending[length] = emitted;
                length += 1;
            }
        }
        Ok(())
    }

    fn owned_key(&mut self, key: &str) {
        let mut matched = Match::new();
        for &byte in key.as_bytes() { matched.push(byte); }
        let frame = &mut self.frames[self.frame_len - 1];
        frame.key = matched.key();
        frame.raw = frame.first && frame.key == RAW_KEY;
        frame.phase = Phase::Value;
    }

    fn owned_complete(&mut self) {
        self.complete(0);
        if self.frame_len != 0 {
            let frame = &mut self.frames[self.frame_len - 1];
            frame.first = false;
            frame.raw = false;
            frame.phase = if frame.object { Phase::Key } else { Phase::Value };
        }
    }

    fn owned_enter<'a, F: FnMut(JsonCodecCharge) -> Result<()>>(
        &mut self, value: &'a serde_json::Value, charge: &mut F,
    ) -> std::result::Result<Option<OwnedFrame<'a>>, CodecError> {
        let selection = self.select(0);
        let scope = Self::value_scope(selection);
        self.item(0, scope);
        let tag = self.frame_len != 0
            && !Self::tag_key(self.frames[self.frame_len - 1].selection.node).is_empty()
            && self.frames[self.frame_len - 1].key == Self::tag_key(self.frames[self.frame_len - 1].selection.node);
        if tag {
            let mut matched = Match::new();
            if let serde_json::Value::String(string) = value {
                for &byte in string.as_bytes() { matched.push(byte); }
            }
            self.set_tag(if value.is_string() { matched.key() } else { b"" });
        }
        match value {
            serde_json::Value::Object(values) => {
                let node = if selection.node == Node::Schema(JsonValueSchema::ModelContent) { Node::Schema(JsonValueSchema::ModelContentPart) } else { selection.node };
                self.frames[self.frame_len] = Frame { object: true, phase: Phase::Key,
                    selection: Selection { node, ..selection }, ..Frame::EMPTY };
                self.frame_len += 1;
                Ok(Some(OwnedFrame::Object(values.iter())))
            }
            serde_json::Value::Array(values) => {
                let node = if selection.node == Node::Schema(JsonValueSchema::ModelContent) { Node::Parts } else { selection.node };
                self.frames[self.frame_len] = Frame { selection: Selection { node, ..selection }, ..Frame::EMPTY };
                self.frame_len += 1;
                Ok(Some(OwnedFrame::Array(values.iter())))
            }
            serde_json::Value::String(string) => {
                let raw = self.frame_len != 0 && self.frames[self.frame_len - 1].raw;
                if raw && scope != Scope::Ignore {
                    if scope == Scope::Unknown { self.potential_raw = true; }
                    else {
                        if let Some(stats) = self.stats(scope) { Stats::add(&mut stats.raw_frames, &mut stats.overflow); }
                        self.layers[1] = Layer { base: self.frame_len, root: Selection { node: Node::Schema(JsonValueSchema::Root), scope }, ..Layer::EMPTY };
                        for &byte in string.as_bytes() { self.push_layer(1, byte, charge)?; }
                        self.close_raw(0, scope);
                    }
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    pub(super) fn finish(&self, input_bytes: u64, lexical_items: u64) -> std::result::Result<JsonValueShape, CodecError> {
        let stats = self.stats;
        if stats.overflow || lexical_items.checked_add(stats.items).is_none_or(|items| items > self.bounds.maximum_items) {
            return Err(CodecError::Bound(ITEM_LIMIT));
        }
        if stats.depth > self.bounds.maximum_depth.min(256) as usize {
            return Err(CodecError::Bound(DEPTH_LIMIT));
        }
        Ok(JsonValueShape { input_bytes, lexical_items, virtual_items: stats.items,
            raw_bytes: stats.bytes, raw_frames: stats.raw_frames,
            unresolved_raw: stats.unresolved || (self.layers[0].root.node == Node::Unknown && self.potential_raw), origin: ShapeOrigin::Slice })
    }
}

enum OwnedFrame<'a> {
    Array(std::slice::Iter<'a, serde_json::Value>),
    Object(serde_json::map::Iter<'a>),
}

struct OwnedMeter {
    bounds: JsonBounds,
    items: u64,
    bytes: u64,
}
impl OwnedMeter {
    fn visit<F: FnMut(JsonCodecCharge) -> Result<()>>(
        &mut self, depth: usize, payload: usize, charge: &mut F,
    ) -> std::result::Result<(), CodecError> {
        if depth > self.bounds.maximum_depth.min(256) as usize { return Err(CodecError::Bound(DEPTH_LIMIT)); }
        if self.items >= self.bounds.maximum_items { return Err(CodecError::Bound(ITEM_LIMIT)); }
        let bytes = self.bytes.checked_add(1).and_then(|bytes| bytes.checked_add(payload as u64))
            .ok_or(CodecError::Bound("JSON byte limit exceeded"))?;
        if bytes > self.bounds.maximum_bytes { return Err(CodecError::Bound("JSON byte limit exceeded")); }
        charge(JsonCodecCharge::Inspect { items: 1 })?;
        self.items += 1;
        self.bytes = bytes;
        Ok(())
    }
}

/// Borrowed resident-tree witness immediately before a genuine from_value conversion.
pub(super) fn inspect_owned<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &serde_json::Value, bounds: JsonBounds, schema: JsonValueSchema, charge: &mut F,
) -> std::result::Result<JsonValueShape, CodecError> {
    let mut inspector = ValueInspector::new(Some(schema), bounds);
    let mut meter = OwnedMeter { bounds, items: 0, bytes: 0 };
    let mut stack: [Option<OwnedFrame<'_>>; 257] = std::array::from_fn(|_| None);
    let mut length = 0;
    let mut next = Some(value);
    loop {
        if let Some(value) = next.take() {
            let payload = match value { serde_json::Value::String(string) => string.len(), _ => 0 };
            meter.visit(length, payload, charge)?;
            if let Some(frame) = inspector.owned_enter(value, charge)? {
                stack[length] = Some(frame);
                length += 1;
            } else { inspector.owned_complete(); }
        }
        if length == 0 { break; }
        let entry = match stack[length - 1].as_mut().expect("live fixed resident iterator") {
            OwnedFrame::Array(values) => values.next().map(|value| (None, value)),
            OwnedFrame::Object(values) => values.next().map(|(key, value)| (Some(key), value)),
        };
        if let Some((key, value)) = entry {
            if let Some(key) = key {
                meter.visit(length, key.len(), charge)?;
                inspector.owned_key(key);
            }
            next = Some(value);
        } else {
            stack[length - 1] = None;
            length -= 1;
            inspector.close_frame();
            inspector.owned_complete();
        }
    }
    let mut shape = inspector.finish(meter.bytes, meter.items)?;
    shape.origin = ShapeOrigin::OwnedConversion;
    Ok(shape)
}
