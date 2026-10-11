//! The canonical codec bounds logical data, not allocator metadata or RSS.
//! Custom Serialize/Deserialize code is trusted application code: serde cannot
//! prevent it allocating or looping before it calls a serializer/visitor.

use super::{Error, Result};
use serde::ser::{self, Serialize};
use serde_json::Value;

mod value_shape;
use value_shape::ValueInspector;
#[cfg(test)]
mod value_shape_tests;

/// Finite admission for one JSON document. Items include map keys and duplicate
/// values, even when canonical last-key-wins output discards an earlier value.
#[derive(Clone, Copy, Debug)]
pub struct JsonBounds {
    /// UTF-8 encoded/input byte limit; duplicate emitted entries count too.
    pub maximum_bytes: u64,
    /// Values and map keys visited, including replaced duplicate values.
    pub maximum_items: u64,
    /// Zero-based nesting depth. Bounded entry points support at most 256.
    /// Selected Value decoding includes simultaneously live ancestor containers
    /// across nested RawValue parsers, not each parser's reset local counter or
    /// only the final tree depth. This is not an exact Rust-stack byte bound.
    pub maximum_depth: u32,
}

impl JsonBounds {
    pub(crate) const IDENTITY: Self = Self {
        maximum_bytes: usize::MAX as u64,
        maximum_items: usize::MAX as u64,
        maximum_depth: u32::MAX,
    };
}

/// SDK-audited closed Value field reachability, not an authority or an arbitrary
/// user selector. Generic Deserialize implementations remain trusted; choose the
/// descriptor belonging to the actual audited concrete input type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonValueSchema {
    /// The closed concrete type has no serde_json::Value fields.
    None,
    /// The entire document is a serde_json::Value.
    Root,
    /// Question.response_schema or Form.schema, gated by the root type tag.
    Interaction,
    /// The control reply's optional result field only.
    ControlResponse,
    /// Interaction outcomes, fork source snapshots, and merge proof statements.
    EventPayload,
    /// Event payloads and retained interaction/fork projections.
    Snapshot,
    /// Conversation messages' model content.
    Context,
    /// A response projection's nested context.
    ResponseProjection,
    /// Tool arguments or completed metadata, gated by the event kind.
    ModelEvent,
    /// Model options, message content and tool schemas.
    ModelRequest,
    /// Untagged text, one model content part, or a parts array.
    ModelContent,
    /// Tool-call arguments or JSON tool-result content.
    ModelContentPart,
    /// Input, checkpoints and transition state/completed values.
    WorkflowRecord,
    /// The initial machine checkpoint state.
    WorkflowAdmission,
    /// The machine checkpoint state.
    MachineCheckpoint,
    /// Native projected tool invocation's arguments.
    ProjectedToolInvocation,
    /// Tool-result JSON value, gated by its kind.
    ToolResultContent,
    /// Resource payload, child admission schemas and source native snapshot.
    ResourcePreparationRequest,
    /// Declared/orchestrated reducer and entrypoint result schemas.
    SchedulerEvent,
    /// Question/Form response value, gated by the root type tag.
    InteractionResponse,
    /// Ordinary task admission input and input/output schemas.
    TaskAdmissionRecord,
    /// The actual recursive fork plan's request/preparation child admissions.
    RecursiveForkToolPlan,
    /// Adjacent fork/merge recursive plan.
    RecursivePlan,
    /// Internally phase-tagged recursive machine state.
    RecursiveState,
    /// Initial recursive plan and ref-only command.
    RecursiveInitialPlan,
    /// Recursive receipt's externally tagged successful Value.
    RecursiveReceipt,
    /// Provider join proof statement only.
    ProjectMergeReceipt,
    /// Stock tool task's arguments only.
    ToolTaskCommand,
    /// Stock task admission command's input only.
    TaskAdmitCommand,
    /// Stock model input and selected-context message content.
    ModelTaskCommand,
    /// Native process request's optional MCP stdio params only.
    NativeProcessRequest,
    /// Source authorization's optional native snapshot.
    ProjectSourceAuthorization,
    /// Fork request's optional authorized-source native snapshot.
    ForkRequest,
    /// Fork report's request and its authorized-source native snapshot.
    ForkReport,
    /// Fork seed's optional authorized-source native snapshot.
    ForkSeed,
    /// Recursive source intent's nested fork request.
    RecursiveForkSourceIntent,
    /// Recursive source pin's intent and its nested fork request.
    RecursiveSourcePin,
    /// Fork preparation commands' externally successful ForkReport values.
    ForkPreparationCommandResults,
    /// Configured recursive checkpoint's Snapshot and source fork request.
    ConfiguredRecursiveSourceCheckpoint,
}

/// Copied data issued by an SDK source pass or borrowed resident-tree inspection.
/// Private fields prevent manufacturing a RawValue-absence/origin assertion.
/// This is quota data, NOT permission or an arbitrary serde heap/CPU/RSS proof.
#[derive(Clone, Copy, Debug)]
pub struct JsonValueShape {
    input_bytes: u64,
    lexical_items: u64,
    virtual_items: u64,
    raw_bytes: u64,
    raw_frames: u64,
    unresolved_raw: bool,
    origin: ShapeOrigin,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShapeOrigin { Slice, OwnedConversion, BorrowedConversion }
impl JsonValueShape {
    /// Source bytes consumed once; for resident inspection, the owned UTF-8
    /// payload/node lower bound instead. Neither includes virtual inspection.
    pub const fn input_bytes(self) -> u64 { self.input_bytes }
    /// Original lexical or resident values/map keys, including source duplicates.
    pub const fn lexical_items(self) -> u64 { self.lexical_items }
    /// Additional lexical nodes in inspected decoded RawValue sources.
    pub const fn virtual_items(self) -> u64 { self.virtual_items }
    /// Whether a possible RawValue path requires conservative byte-based quota.
    pub const fn unresolved_raw(self) -> bool { self.unresolved_raw }
}

/// Called before admitted logical storage grows and after it is freed. Keep a
/// custody guard through the returned output's lifetime. Errors release every
/// codec Retain after its buffers drop; separate caller-owned guards still
/// belong to the caller. Allocator/node overhead and borrowed input are excluded.
#[derive(Clone, Copy, Debug)]
pub enum JsonCodecCharge {
    /// Encoded-byte/item work for byte producers (including sizing passes).
    /// Direct Value producers emit zero bytes; actual sizing CPU uses Inspect.
    /// Zero amounts are cancellation/fence checkpoints, not fabricated work.
    Work {
        /// Encoded bytes.
        bytes: u64,
        /// Structural items.
        items: u64,
    },
    /// Admit requested logical payload/slot storage before allocation. This is
    /// quota growth, NOT an allocator operation or observed allocated bytes.
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
    /// Preflight copy allowance before a potentially unnecessary allocation.
    /// Check the original ledger without spending bytes or making a receipt.
    CopyAdmission {
        /// Bytes that the subsequent SDK buffer copy would copy.
        bytes: u64,
    },
    /// Immediately before one genuine positive SDK String/Vec try_reserve call.
    /// Opaque map/serde allocations are not observed or invented here.
    AllocationOperation {
        /// Full requested new capacity in bytes, not the logical growth delta
        /// or an observation of allocator metadata/RSS.
        bytes: u64,
    },
    /// Bytes about to be copied into an already-reserved buffer. Once accepted,
    /// the copy is infallible: no intervening SDK allocation/failure/checkpoint.
    Copy {
        /// Copied bytes.
        bytes: u64,
    },
    /// Bytes consumed by lexical input scanning, not backend physical reads.
    Read {
        /// Borrowed input bytes consumed.
        bytes: u64,
    },
    /// Actual virtual decoded bytes, resident nodes/keys or sizing-writer chunks
    /// inspected by the SDK. Charge logical CPU items on the original ledger,
    /// not provider reads, owned wire bytes or opaque serde visits.
    Inspect {
        /// Actual inspection units, not a hypothetical maximum.
        items: u64,
    },
    /// Conservative schema/scratch preflight, NOT actual allocated bytes.
    Admission {
        /// Upper bound justified by the concrete audited schema.
        maximum_owned_bytes: u64,
        /// SDK-issued Value shape from the original source/tree pass. Concrete
        /// owners add its Value quota to their distinct tagged/typed ownership.
        value_shape: JsonValueShape,
    },
}

/// Logical owned capacities of an already admitted, finite-depth Value.
/// Excludes the root Value header and opaque map-node/allocator metadata. This
/// pure audit neither admits traversal nor charges quota, copies, or allocation.
pub(crate) fn owned_logical_bytes(value: &Value) -> u64 {
    match value {
        Value::String(value) => value.capacity() as u64,
        Value::Array(values) => (values.capacity() * std::mem::size_of::<Value>()) as u64
            + values.iter().map(owned_logical_bytes).sum::<u64>(),
        Value::Object(values) => (values.len() * std::mem::size_of::<(String, Value)>()) as u64
            + values.iter().map(|(key, value)| key.capacity() as u64 + owned_logical_bytes(value)).sum::<u64>(),
        _ => 0,
    }
}

// Callback interruption is inline. The first owned Root error lives in Meter;
// creating a serde_json custom-message String/Box here would allocate AFTER
// cancellation or quota denial. Ordinary serde errors retain their old carrier.
#[derive(Debug)]
enum CodecError {
    Interrupted,
    Bound(&'static str),
    Json(serde_json::Error),
    Allocation(std::collections::TryReserveError),
}
impl std::fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Interrupted => formatter.write_str("JSON admission interrupted"),
            Self::Bound(reason) => formatter.write_str(reason),
            Self::Json(error) => std::fmt::Display::fmt(error, formatter),
            Self::Allocation(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}
impl std::error::Error for CodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Interrupted | Self::Bound(_) => None,
            Self::Json(error) => Some(error),
            Self::Allocation(error) => Some(error),
        }
    }
}
impl ser::Error for CodecError {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self::Json(ser::Error::custom(message))
    }
}
impl From<serde_json::Error> for CodecError {
    fn from(error: serde_json::Error) -> Self { Self::Json(error) }
}
impl From<std::collections::TryReserveError> for CodecError {
    fn from(error: std::collections::TryReserveError) -> Self { Self::Allocation(error) }
}
impl CodecError {
    fn into_io(self) -> std::io::Error {
        match self {
            Self::Interrupted => std::io::ErrorKind::Interrupted.into(),
            Self::Bound(_) => std::io::ErrorKind::InvalidData.into(),
            Self::Json(error) => std::io::Error::other(error),
            Self::Allocation(_) => unreachable!("SDK reserve errors return directly to Root, not through serde's writer bridge"),
        }
    }
    fn into_root_with_code(self, native_code: Option<crate::wire::ErrorCode>) -> Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(code) = native_code {
            return match self {
                Self::Interrupted => unreachable!("interruption retains its original owned Root cause"),
                Self::Bound(reason) => Error::NativeBoundary { code, reason },
                Self::Json(source) => Error::NativeEncode { code, source, custody: None },
                Self::Allocation(error) => Error::Allocation(error),
            };
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = native_code;
        match self {
            Self::Interrupted => unreachable!("interruption retains its original owned Root cause"),
            Self::Bound(reason) => Error::Invalid(reason.into()),
            Self::Json(error) => Error::Invalid(error.to_string()),
            Self::Allocation(error) => Error::Allocation(error),
        }
    }
}

/// Closed SDK-generated encoder/Raw-Value diagnostic allowance only. Audited
/// against serde_json 1.0.151 ErrorCode/Display and Rust 1.98.1 TryReserveError
/// Display; upgrades require revisiting this inventory. Custom Serialize error
/// messages and arbitrary typed Deserialize diagnostics are NOT bounded here.
/// This is future logical quota (zero allocation observations), not RSS or
/// serde_json's private ErrorImpl/allocator-node footprint. Native callback
/// interruption is inline and needs none of this allowance.
pub const fn sdk_json_diagnostic_bytes() -> u64 {
    // Longest SDK/static-library label: all literal encoder/lexer labels and
    // JSON syntax labels are shorter than this actual allocation-error text.
    const CAPACITY_ERROR: &str = concat!("memory allocation failed",
        " because the computed capacity exceeded the collection's maximum");
    const SYNTAX_ERROR: &str =
        "control character (\\u0000-\\u001F) found while parsing a string";
    let largest = if CAPACITY_ERROR.len() > SYNTAX_ERROR.len() {
        CAPACITY_ERROR.len() as u64
    } else { SYNTAX_ERROR.len() as u64 };
    // Raw lex errors may cross Root Invalid's Display prefix. ErrorImpl renders
    // at most two usize positions; supported targets have <=64-bit usize.
    let rendered = largest + "invalid request: ".len() as u64
        + " at line ".len() as u64 + " column ".len() as u64
        + 2 * (u64::MAX.ilog10() as u64 + 1);
    // Two error-message buffers can overlap during ordinary conversions; each
    // formatter's Vec capacity is <=2*max(length,8). Include known carrier ABI,
    // but do not pretend to observe serde_json's hidden boxed ErrorImpl layout.
    let wrappers = std::mem::size_of::<CodecError>() as u64
        + std::mem::size_of::<serde_json::Error>() as u64
        + std::mem::size_of::<std::io::Error>() as u64
        + std::mem::size_of::<String>() as u64;
    4 * rendered + 2 * wrappers
}

/// Conservative logical quota for the CLOSED serde_json::Value parser, derived
/// from the SDK-issued shape in the ORIGINAL source pass. No scan, parser or
/// cloned tree occurs here. Ordinary non-Raw input retains the old slot/string
/// policy; selected Raw sources add witnessed virtual nodes and live-buffer
/// upper bounds. Unknown generic serde Value reachability uses an honest
/// byte-derived Raw worst case instead of manufacturing a Raw-absence proof.
/// Typed envelopes add their OWN tagged/typed/Box/diagnostic overlap. Opaque
/// allocator/map-node/RSS overhead and arbitrary custom serde are excluded.
pub fn json_value_decode_owned_bytes(shape: JsonValueShape) -> Result<u64> {
    value_full_bytes(shape).ok_or_else(|| shape_boundary("JSON owned bound overflow"))
}
fn value_full_bytes(shape: JsonValueShape) -> Option<u64> {
    value_storage_bytes(shape)
        .and_then(|bytes| shape.input_bytes.max(8).checked_mul(4).and_then(|strings| bytes.checked_add(strings)))
}

/// Inspection only before an SDK's existing consuming from_value conversion.
/// Uses actual map iterator order, with no cloned tree, serialization or source
/// reread. The selected closed DTO owner supplies its own typed/Content/Box and
/// dynamic diagnostic quota before invoking the original converter.
pub(crate) fn json_owned_value_shape_bounded<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &Value, bounds: JsonBounds, schema: JsonValueSchema, charge: &mut F,
) -> Result<JsonValueShape> {
    value_shape::inspect_owned(value, JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds }, schema, charge)
        .map_err(|failure| match failure {
            AdmissionFailure::Root(error) => error,
            AdmissionFailure::Bound(reason) => shape_boundary(reason),
        })
}

/// NEW logical Value-container/Raw replay/syntax diagnostic quota for a genuine
/// consuming SDK Value converter. Resident strings MOVE and remain covered by
/// the original input guard; this helper must not claim them a second time.
/// Keep that original guard until the converter physically consumes its input.
/// Custom DTO clones, Content storage, new strings and typed nodes are audited
/// separately by the concrete owner. Not a generic from_value heap proof.
pub(crate) fn json_owned_value_conversion_additional_bytes(shape: JsonValueShape) -> Result<u64> {
    if shape.origin != ShapeOrigin::OwnedConversion { return Err(shape_boundary("JSON conversion shape is not owned")); }
    value_storage_bytes(shape).ok_or_else(|| shape_boundary("JSON owned bound overflow"))
}

/// Pure arithmetic for an audited SDK BORROWED closed DTO. Its owner supplies
/// the finite maximum slot from actual typed nodes plus pinned serde Content
/// layer/cardinality proof; this is not an arbitrary T heap bound. Eight slots
/// per admitted resident/virtual node cover that owner's declared collection
/// growth policy. Shared Value strings/Raw/syntax allowance is included ONCE.
pub(crate) fn json_typed_value_decode_owned_bytes(shape: JsonValueShape, maximum_slot_bytes: u64) -> Result<u64> {
    if shape.origin != ShapeOrigin::BorrowedConversion {
        return Err(shape_boundary("JSON typed decode shape is not borrowed"));
    }
    shape_nodes(shape)
        .and_then(|nodes| nodes.checked_mul(8))
        .and_then(|slots| slots.checked_mul(maximum_slot_bytes))
        .and_then(|typed| value_full_bytes(shape).and_then(|value| typed.checked_add(value)))
        .ok_or_else(|| shape_boundary("JSON typed owned bound overflow"))
}

fn shape_nodes(shape: JsonValueShape) -> Option<u64> {
    if shape.unresolved_raw { Some(shape.input_bytes.max(shape.lexical_items)) }
    else { shape.lexical_items.checked_add(shape.virtual_items) }
}

// Extend existing concrete Event/Snapshot quota without changing their NORMAL
// policy or charging the same Raw buffer twice. This is arithmetic subtraction
// of a baseline, NOT an issued Raw-absence assertion or a second source pass.
pub(crate) fn json_value_virtual_owned_bytes(shape: JsonValueShape) -> Result<u64> {
    let mut normal = shape;
    normal.virtual_items = 0;
    normal.raw_bytes = 0;
    normal.raw_frames = 0;
    normal.unresolved_raw = false;
    value_storage_bytes(shape)
        .and_then(|actual| value_storage_bytes(normal).and_then(|baseline| actual.checked_sub(baseline)))
        .ok_or_else(|| shape_boundary("JSON virtual owned bound overflow"))
}

fn shape_boundary(reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason } }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { Error::Invalid(reason.into()) }
}

fn value_storage_bytes(shape: JsonValueShape) -> Option<u64> {
    let nodes = shape_nodes(shape)?;
    let slots = nodes.checked_mul((10 * std::mem::size_of::<Value>()
        + std::mem::size_of::<(String, Value)>()) as u64)?;
    let raw = if shape.unresolved_raw {
        // An actual input slice therefore admits at most usize::BITS layers.
        // Each layer holds raw payload, geometric parser scratch, and a
        // possible into_boxed_str/shrink overlap; two extra buffers cover
        // transitions. This is conservative future quota, not an observation.
        shape.input_bytes.max(8).checked_mul(4 * (u64::from(usize::BITS) + 2))?
    } else {
        // Sum decoded raw payload across ALL virtual frames is >= the maximum
        // simultaneously active boxed payload. Scratch is <=2*max(len,8);
        // include a further payload during String-to-box shrinking.
        shape.raw_bytes.checked_mul(4)
            .and_then(|bytes| shape.raw_frames.checked_mul(16).and_then(|minimum| bytes.checked_add(minimum)))
            ?
    };
    slots.checked_add(raw)
        .and_then(|bytes| bytes.checked_add(sdk_json_diagnostic_bytes()))
}

struct Meter<'a, F: FnMut(JsonCodecCharge) -> Result<()>> {
    bounds: JsonBounds,
    items: u64,
    bytes: u64,
    charge: &'a mut F,
    failure: Option<Error>,
    retained: u64,
    bound_failure: Option<&'static str>,
    native_code: Option<crate::wire::ErrorCode>,
    tree_only: bool,
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
    fn root_error(&mut self, error: CodecError) -> Error {
        self.first_error().unwrap_or_else(|| error.into_root_with_code(self.native_code))
    }
    fn first_error(&mut self) -> Option<Error> {
        self.failure.take().or_else(|| self.bound_failure.take()
            .map(|reason| CodecError::Bound(reason).into_root_with_code(self.native_code)))
    }
    fn bound(&mut self, reason: &'static str) -> CodecError {
        if self.failure.is_none() && self.bound_failure.is_none() { self.bound_failure = Some(reason); }
        CodecError::Bound(reason)
    }
    fn charge(&mut self, charge: JsonCodecCharge) -> JsonResult<()> {
        if self.failure.is_some() && !matches!(charge, JsonCodecCharge::Release { .. }) {
            return Err(CodecError::Interrupted);
        }
        if let Some(reason) = self.bound_failure {
            if !matches!(charge, JsonCodecCharge::Release { .. }) { return Err(CodecError::Bound(reason)); }
        }
        match (self.charge)(charge) {
            Ok(()) => Ok(()),
            Err(error) => {
                if self.failure.is_none() { self.failure = Some(error); }
                Err(CodecError::Interrupted)
            }
        }
    }


    fn item(&mut self, depth: u32) -> JsonResult<()> {
        if depth > self.bounds.maximum_depth || self.items >= self.bounds.maximum_items {
            return Err(CodecError::Bound("JSON exceeds declared structural bound"));
        }
        self.charge(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        self.items += 1;
        Ok(())
    }

    fn data(&mut self, bytes: u64) -> JsonResult<()> {
        let next = self.bytes.checked_add(bytes).ok_or_else(|| self.bound("JSON byte count overflow"))?;
        if next > self.bounds.maximum_bytes {
            return Err(self.bound("JSON exceeds declared byte bound"));
        }
        self.charge(JsonCodecCharge::Work { bytes: if self.tree_only { 0 } else { bytes }, items: 0 })?;
        self.bytes = next;
        Ok(())
    }

    fn size<T: Serialize + ?Sized>(&mut self, value: &T) -> JsonResult<u64> {
        if !self.tree_only { return encoded_length(value).map_err(CodecError::from); }
        struct Sizing<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> {
            meter: &'a mut Meter<'b, F>, bytes: u64,
        }
        impl<F: FnMut(JsonCodecCharge) -> Result<()>> std::io::Write for Sizing<'_, '_, F> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let next = self.bytes.checked_add(bytes.len() as u64)
                    .ok_or_else(|| self.meter.bound("JSON byte count overflow").into_io())?;
                self.meter.charge(JsonCodecCharge::Inspect { items: bytes.len() as u64 })
                    .map_err(CodecError::into_io)?;
                self.bytes = next;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }
        let mut sizing = Sizing { meter: self, bytes: 0 };
        serde_json::to_writer(&mut sizing, value)?;
        Ok(sizing.bytes)
    }

    fn retain(&mut self, bytes: usize) -> JsonResult<()> {
        let retained = self.retained.checked_add(bytes as u64)
            .ok_or_else(|| CodecError::Bound("JSON retention overflow"))?;
        self.charge(JsonCodecCharge::Retain { bytes: bytes as u64 })?;
        self.retained = retained;
        Ok(())
    }

    fn release(&mut self, bytes: u64) -> JsonResult<()> {
        let retained = self.retained.checked_sub(bytes)
            .ok_or_else(|| CodecError::Bound("JSON retention accounting differs"))?;
        self.charge(JsonCodecCharge::Release { bytes })?;
        self.retained = retained;
        Ok(())
    }
    fn allocation(&mut self, requested_capacity: usize) -> JsonResult<()> {
        if requested_capacity != 0 {
            self.charge(JsonCodecCharge::AllocationOperation { bytes: requested_capacity as u64 })?;
        }
        Ok(())
    }

    fn string(&mut self, value: &str) -> JsonResult<String> {
        // Charge before scanning/copying a potentially huge borrowed string.
        if value.len() as u64 > self.bounds.maximum_bytes.saturating_sub(self.bytes) {
            return Err(CodecError::Bound("JSON exceeds declared byte bound"));
        }
        self.charge(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
        struct StringCounter<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>>(&'a mut Meter<'b, F>);
        impl<F: FnMut(JsonCodecCharge) -> Result<()>> std::io::Write for StringCounter<'_, '_, F> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.data(bytes.len() as u64).map_err(CodecError::into_io)?;
                if self.0.tree_only {
                    self.0.charge(JsonCodecCharge::Inspect { items: bytes.len() as u64 }).map_err(CodecError::into_io)?;
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }
        serde_json::to_writer(StringCounter(self), value)?;
        self.retain(value.len())?;
        self.charge(JsonCodecCharge::CopyAdmission { bytes: value.len() as u64 })?;
        let mut output = String::new();
        if !value.is_empty() {
            self.allocation(value.len())?;
            output.try_reserve_exact(value.len()).map_err(CodecError::from)?;
        }
        self.charge(JsonCodecCharge::Copy { bytes: value.len() as u64 })?;
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

type JsonResult<T> = std::result::Result<T, CodecError>;

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

impl<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> ser::Serializer for Encoder<'a, 'b, F> {
    type Ok = Value;
    type Error = CodecError;
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
            return Err(text.error.unwrap_or_else(|| CodecError::Bound("Display failed")));
        }
        let value = Value::String(text.meter.string(&text.text)?);
        let released = text.text.len() as u64;
        drop(text.text);
        text.meter.release(released)?;
        Ok(value)
    }
}

struct BoundedText<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> { meter: &'a mut Meter<'b, F>, text: String, error: Option<CodecError> }
impl<F: FnMut(JsonCodecCharge) -> Result<()>> std::fmt::Write for BoundedText<'_, '_, F> {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        let result = (|| {
            // This transient formatting buffer is separate retained/work data;
            // final escaped bytes are charged by serialize_str.
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

impl<F: FnMut(JsonCodecCharge) -> Result<()>> Sequence<'_, '_, F> {
    fn element<T: Serialize + ?Sized>(&mut self, value: &T) -> JsonResult<()> {
        if !self.values.is_empty() { self.encoder.meter.data(1)?; }
        if self.encoder.meter.items >= self.encoder.meter.bounds.maximum_items {
            return Err(CodecError::Bound("JSON exceeds declared structural bound"));
        }
        // Geometric, explicitly admitted growth; ignore untrusted serde hints.
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
        impl<F: FnMut(JsonCodecCharge) -> Result<()>> ser::$trait for Sequence<'_, '_, F> {
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

impl<F: FnMut(JsonCodecCharge) -> Result<()>> Object<'_, '_, F> {
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
impl<F: FnMut(JsonCodecCharge) -> Result<()>> ser::SerializeMap for Object<'_, '_, F> {
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
        impl<F: FnMut(JsonCodecCharge) -> Result<()>> ser::$trait for Object<'_, '_, F> {
            type Ok = Value; type Error = CodecError;
            fn serialize_field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> JsonResult<()> { self.entry(key, value) }
            fn end(self) -> JsonResult<Value> { self.finish() }
        }
    };
}
impl<F: FnMut(JsonCodecCharge) -> Result<()>> ser::SerializeStruct for Object<'_, '_, F> {
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
            $(if !($finite)(value) { return Err(CodecError::Bound("float key must be finite (got NaN or +/-inf)")); })?
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
        fn $method(self $(, _: $ty)?) -> JsonResult<String> { Err(CodecError::Bound("key must be a string")) }
    };
}
impl<F: FnMut(JsonCodecCharge) -> Result<()>> ser::Serializer for Key<'_, '_, F> {
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

/// Allocates only after each logical payload/item has passed admission. A
/// canonical tree is necessary for sorted maps and last-duplicate-key wins;
/// streaming serde_json directly would change f32 and duplicate-key identities.
/// Byte admission counts all emitted entries (including discarded duplicates).
pub fn canonical_json_bytes_bounded<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, charge: &mut F,
) -> Result<Vec<u8>> {
    encode(value, JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds }, None, charge)
}

/// SDK-private native carrier for the SAME bounded canonical byte encoder.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn canonical_json_bytes_bounded_native_trusted<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, code: crate::wire::ErrorCode, charge: &mut F,
) -> Result<Vec<u8>> {
    encode(value, JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds }, Some(code), charge)
}

/// Native carrier for the legacy identity policy, restricted to an already
/// paid, trusted resident Value. Uses the SAME encoder and exact IDENTITY bounds
/// without the bounded entry points' 256-depth clamp. Legacy recursive-callstack
/// risk remains: finite work custody is not a universal stack/RSS guarantee.
/// Untrusted wire must pass its actual finite admission before this operation.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn canonical_json_bytes_identity_native_trusted<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &Value, code: crate::wire::ErrorCode, charge: &mut F,
) -> Result<Vec<u8>> {
    encode(value, JsonBounds::IDENTITY, Some(code), charge)
}

/// Produces the SAME canonical encoder's owned Value directly, without an
/// encoded buffer, parsing roundtrip or unchecked initial serde_json::to_value.
/// Encoded bytes (including duplicate entries), items and depth are admitted
/// before owned payload/slot growth. Retain/Release/Copy report logical storage,
/// not opaque allocator/map-node operations or process RSS. Custom Serialize
/// internals are trusted, not sandboxed. RawValue's separate Admission requires
/// an audited caller-owned parser quota; its subsequent Retain observes the
/// parsed tree and must adopt that quota without inventing an allocator attempt.
/// On success keep the actual returned-tree custody until its last use; all
/// temporary codec claims are released, and errors drop owned nodes before
/// unconditional cleanup while preserving the first owned callback failure.
pub fn json_value_bounded_trusted<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, charge: &mut F,
) -> Result<Value> {
    value_output(value, bounds, None, charge)
}

/// Native direct tree producer using the SAME canonical Encoder. Closed SDK
/// failures remain inline; actual serde failures move their original source
/// without formatting. Attach the original preclaimed diagnostic allocation to
/// NativeEncode before returning its error. Returned-tree custody is still the
/// original caller's responsibility; custom serde internals are trusted.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub fn json_value_bounded_native_trusted<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, code: crate::wire::ErrorCode, charge: &mut F,
) -> Result<Value> {
    value_output(value, bounds, Some(code), charge)
}

/// SDK-private sizing of an already bounded, audited closed source. No owned
/// Value/wire buffer is constructed; actual emitted sizing bytes/nodes are CPU
/// Inspect/Work items, never backend Read or owned-wire Work.bytes. The caller
/// admits resident data BEFORE invoking serde (for Value use resident inspection
/// with schema None first). Opaque custom Serialize code/CPU is not sandboxed,
/// and standard serde error allocation requires the original paid diagnostic
/// guard. This is not a deadline guarantee between opaque serde callbacks.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn validate_json_byte_bound_native_trusted<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, code: crate::wire::ErrorCode, charge: &mut F,
) -> Result<()> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    let mut meter = Meter { bounds, items: 0, bytes: 0, charge, failure: None,
        bound_failure: None, native_code: Some(code), retained: 0, tree_only: true };
    if let Err(error) = meter.charge(JsonCodecCharge::Work { bytes: 0, items: 0 }) {
        return Err(meter.root_error(error));
    }
    struct Sizing<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> {
        scan: Scan, meter: &'a mut Meter<'b, F>,
    }
    impl<F: FnMut(JsonCodecCharge) -> Result<()>> std::io::Write for Sizing<'_, '_, F> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let meter = &mut *self.meter;
            self.scan.push_inline(bytes, meter.bounds, &mut |event| {
                meter.charge(match event {
                    JsonCodecCharge::Read { bytes } => JsonCodecCharge::Inspect { items: bytes },
                    JsonCodecCharge::Work { items, .. } => JsonCodecCharge::Work { bytes: 0, items },
                    other => other,
                }).map_err(|error| meter.root_error(error))
            }).map_err(|failure| match failure {
                AdmissionFailure::Root(error) => { meter.failure = Some(error); CodecError::Interrupted.into_io() },
                AdmissionFailure::Bound(reason) => meter.bound(reason).into_io(),
            })?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let mut sizing = Sizing { scan: Scan::default(), meter: &mut meter };
    if let Err(error) = serde_json::to_writer(&mut sizing, value) {
        return Err(sizing.meter.root_error(error.into()));
    }
    if let Some(error) = sizing.meter.first_error() { return Err(error); }
    sizing.scan.end_inline().map_err(|failure| match failure {
        AdmissionFailure::Root(error) => error,
        AdmissionFailure::Bound(reason) => sizing.meter.root_error(CodecError::Bound(reason)),
    })?;
    if let Err(error) = sizing.meter.charge(JsonCodecCharge::Work { bytes: 0, items: 0 }) {
        return Err(sizing.meter.root_error(error));
    }
    Ok(())
}

fn value_output<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, native_code: Option<crate::wire::ErrorCode>, charge: &mut F,
) -> Result<Value> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    let mut meter = Meter { bounds, items: 0, bytes: 0, charge, failure: None,
        bound_failure: None, native_code, retained: 0, tree_only: true };
    let value = encode_value(value, &mut meter)?;
    // Observe this actual tree once: permissive custom serializers can discard
    // temporary keys/nodes before returning success. Their already-freed claims
    // must not be transferred as if they were part of the returned Value.
    let owned = owned_logical_bytes(&value);
    let released = meter.retained.checked_sub(owned)
        .ok_or_else(|| CodecError::Bound("JSON retention accounting differs").into_root_with_code(native_code))?;
    if released != 0 {
        if let Err(error) = meter.release(released) {
            return Err(meter.root_error(error));
        }
    }
    if let Err(error) = meter.charge(JsonCodecCharge::Work { bytes: 0, items: 0 }) {
        return Err(meter.root_error(error));
    }
    meter.retained = 0;
    Ok(value)
}

fn encode_value<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, meter: &mut Meter<'_, F>,
) -> Result<Value> {
    let mut value = match value.serialize(Encoder { meter: &mut *meter, depth: 0 }) {
        Ok(value) => value,
        Err(error) => return Err(meter.root_error(error)),
    };
    if let Some(error) = meter.first_error() { return Err(error); }
    // serde_json::Map may use insertion-order storage. Explicit sorting pins
    // the same raw UTF-8 key ordering for all feature configurations.
    if let Err(error) = meter.charge(JsonCodecCharge::Work { bytes: 0, items: 0 }) {
        return Err(meter.root_error(error));
    }
    value.sort_all_objects();
    Ok(value)
}

pub(super) fn canonical_json_bytes_trusted<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    encode(value, JsonBounds::IDENTITY, None, &mut |_| Ok(()))
}

fn encode<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, native_code: Option<crate::wire::ErrorCode>, charge: &mut F,
) -> Result<Vec<u8>> {
    let mut meter = Meter { bounds, items: 0, bytes: 0, charge, failure: None,
        bound_failure: None, native_code, retained: 0, tree_only: false };
    let value = encode_value(value, &mut meter)?;
    let size = meter.size(&value).map_err(|error| meter.root_error(error))?;
    if size > bounds.maximum_bytes {
        return Err(meter.root_error(CodecError::Bound("JSON exceeds declared byte bound")));
    }
    let size = usize::try_from(size)
        .map_err(|_| meter.root_error(CodecError::Bound("JSON output exceeds address space")))?;
    if let Err(error) = meter.retain(size) {
        return Err(meter.root_error(error));
    }
    (meter.charge)(JsonCodecCharge::Work { bytes: size as u64, items: 0 })?;
    if let Err(error) = meter.charge(JsonCodecCharge::CopyAdmission { bytes: size as u64 })
        .and_then(|()| meter.allocation(size)) {
        return Err(meter.root_error(error));
    }
    let mut bytes = Vec::new();
    if size != 0 { bytes.try_reserve_exact(size).map_err(Error::Allocation)?; }
    struct Output<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> { bytes: &'a mut Vec<u8>, maximum: usize, meter: &'a mut Meter<'b, F> }
    impl<F: FnMut(JsonCodecCharge) -> Result<()>> std::io::Write for Output<'_, '_, F> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.maximum - self.bytes.len() {
                return Err(self.meter.bound("JSON exceeds admitted output").into_io());
            }
            self.meter.charge(JsonCodecCharge::Work { bytes: bytes.len() as u64, items: 0 }).map_err(CodecError::into_io)?;
            self.meter.charge(JsonCodecCharge::Copy { bytes: bytes.len() as u64 }).map_err(CodecError::into_io)?;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    if let Err(error) = serde_json::to_writer(Output { bytes: &mut bytes, maximum: size, meter: &mut meter }, &value) {
        return Err(meter.root_error(error.into()));
    }
    let released = meter.retained - size as u64;
    drop(value);
    if let Err(error) = meter.release(released) {
        return Err(meter.root_error(error));
    }
    // Transfer only the returned output reservation to the caller. Every
    // temporary reservation has already been released after its data was freed.
    meter.retained = 0;
    Ok(bytes)
}

const LEXICAL_BYTE_ERROR: &str = "JSON input exceeds declared byte bound";
const LEXICAL_NESTING_ERROR: &str = "invalid JSON nesting";
const LEXICAL_ITEM_ERROR: &str = "JSON exceeds declared structural bound";
const LEXICAL_INCOMPLETE_ERROR: &str = "incomplete JSON document";

/// Maximum ONE newly owned SDK lexical-error String payload. These paths never
/// interpolate input, labels, line/column numbers or callback-owned diagnostics.
/// Wrapper/allocator metadata is excluded; any caller formatting adds its own
/// audited prefix/buffer allowance. Native callbacks forward their owned cause.
pub(crate) const fn lexical_json_diagnostic_bytes() -> u64 {
    let labels = [LEXICAL_BYTE_ERROR, LEXICAL_NESTING_ERROR,
        LEXICAL_ITEM_ERROR, LEXICAL_INCOMPLETE_ERROR];
    let mut maximum = 0;
    let mut index = 0;
    while index < labels.len() {
        let bytes = labels[index].len() as u64;
        if bytes > maximum { maximum = bytes; }
        index += 1;
    }
    maximum
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
    inspector: Option<ValueInspector>,
}
enum AdmissionFailure {
    Root(Error),
    Bound(&'static str),
}
impl From<Error> for AdmissionFailure {
    fn from(error: Error) -> Self { Self::Root(error) }
}
impl AdmissionFailure {
    fn into_root(self) -> Error {
        match self {
            Self::Root(error) => error,
            Self::Bound(reason) => Error::Invalid(reason.into()),
        }
    }
}
impl Scan {
    fn with_value_schema(schema: Option<JsonValueSchema>, bounds: JsonBounds) -> Self {
        Self { inspector: Some(ValueInspector::new(schema, bounds)), ..Self::default() }
    }
    fn value_shape(&self) -> std::result::Result<JsonValueShape, AdmissionFailure> {
        self.end_inline()?;
        self.inspector.as_ref().expect("Value shape requested only from configured source pass")
            .finish(self.bytes, self.items)
    }
    fn push<F: FnMut(JsonCodecCharge) -> Result<()>>(&mut self, bytes: &[u8], bounds: JsonBounds, charge: &mut F) -> Result<()> {
        self.push_inline(bytes, bounds, charge).map_err(AdmissionFailure::into_root)
    }
    fn push_inline<F: FnMut(JsonCodecCharge) -> Result<()>>(
        &mut self, bytes: &[u8], bounds: JsonBounds, charge: &mut F,
    ) -> std::result::Result<(), AdmissionFailure> {
        for &byte in bytes {
            if self.bytes >= bounds.maximum_bytes { return Err(AdmissionFailure::Bound(LEXICAL_BYTE_ERROR)); }
            charge(JsonCodecCharge::Read { bytes: 1 })?;
            self.bytes += 1;
            if let Some(inspector) = &mut self.inspector { inspector.push(byte, charge)?; }
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
                b']' | b'}' => { self.depth = self.depth.checked_sub(1).ok_or(AdmissionFailure::Bound(LEXICAL_NESTING_ERROR))?; self.atom = false; false }
                b':' | b',' | b' ' | b'\r' | b'\n' | b'\t' => { self.atom = false; false }
                _ => { let first = !self.atom; self.atom = true; first }
            };
            if item {
                if item_depth > bounds.maximum_depth || self.items >= bounds.maximum_items {
                    return Err(AdmissionFailure::Bound(LEXICAL_ITEM_ERROR));
                }
                charge(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                self.items += 1;
            }
        }
        Ok(())
    }
    fn end(&self) -> Result<()> {
        self.end_inline().map_err(AdmissionFailure::into_root)
    }
    fn end_inline(&self) -> std::result::Result<(), AdmissionFailure> {
        if self.string || self.depth != 0 { return Err(AdmissionFailure::Bound(LEXICAL_INCOMPLETE_ERROR)); }
        Ok(())
    }
}

/// Bounds lexical tokens, including duplicate keys. Complete grammar, UTF-8 and
/// numeric validation follows in the typed serde decoder.
pub(crate) fn admit_json<F: FnMut(JsonCodecCharge) -> Result<()>>(bytes: &[u8], bounds: JsonBounds, charge: &mut F) -> Result<()> {
    admit_json_trusted(bytes, JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds }, charge)
}

pub(super) fn admit_json_trusted<F: FnMut(JsonCodecCharge) -> Result<()>>(bytes: &[u8], bounds: JsonBounds, charge: &mut F) -> Result<()> {
    admit_json_inline(bytes, bounds, charge).map_err(AdmissionFailure::into_root)
}

fn admit_json_inline<F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8], bounds: JsonBounds, charge: &mut F,
) -> std::result::Result<(), AdmissionFailure> {
    if bytes.len() as u64 > bounds.maximum_bytes { return Err(AdmissionFailure::Bound(LEXICAL_BYTE_ERROR)); }
    let mut scan = Scan::default();
    scan.push_inline(bytes, bounds, charge)?;
    scan.end_inline()
}

fn admit_json_value_shape_inline<F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8], bounds: JsonBounds, schema: Option<JsonValueSchema>, charge: &mut F,
) -> std::result::Result<JsonValueShape, AdmissionFailure> {
    if bytes.len() as u64 > bounds.maximum_bytes { return Err(AdmissionFailure::Bound(LEXICAL_BYTE_ERROR)); }
    let mut scan = Scan::with_value_schema(schema, bounds);
    scan.push_inline(bytes, bounds, charge)?;
    scan.value_shape()
}

/// The original source pass with a real closed SDK Value field descriptor.
/// Callers keep their own schema/diagnostic custody and original serde decoder.
pub(crate) fn admit_json_value_shape<F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8], bounds: JsonBounds, schema: JsonValueSchema, charge: &mut F,
) -> Result<JsonValueShape> {
    admit_json_value_shape_inline(bytes, JsonBounds {
        maximum_depth: bounds.maximum_depth.min(256), ..bounds
    }, Some(schema), charge).map_err(AdmissionFailure::into_root)
}

/// Existing noncanonical record bytes (not identities). Serialize must be
/// trusted and deterministic across these two passes. Neither pass constructs
/// a Value tree; emitted bytes/items/depth are checked before owned output
/// growth. Inconsistent second-pass serialization errors rather than reallocs.
pub fn json_bytes_bounded_trusted<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, charge: &mut F,
) -> Result<Vec<u8>> {
    bytes_output(value, bounds, None, charge)
}

/// SDK-private native carrier for the SAME deterministic two-pass byte writer.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn json_bytes_bounded_native_trusted<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, code: crate::wire::ErrorCode, charge: &mut F,
) -> Result<Vec<u8>> {
    bytes_output(value, bounds, Some(code), charge)
}

fn bytes_output<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, native_code: Option<crate::wire::ErrorCode>, charge: &mut F,
) -> Result<Vec<u8>> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    let mut meter = Meter { bounds, items: 0, bytes: 0, charge, failure: None,
        bound_failure: None, native_code, retained: 0, tree_only: false };
    struct Writer<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> {
        scan: Scan,
        bounds: JsonBounds,
        meter: &'a mut Meter<'b, F>,
        output: Option<&'a mut Vec<u8>>,
        maximum: usize,
    }
    impl<F: FnMut(JsonCodecCharge) -> Result<()>> std::io::Write for Writer<'_, '_, F> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let meter = &mut *self.meter;
            self.scan.push_inline(bytes, self.bounds, &mut |event| {
                meter.charge(match event {
                    JsonCodecCharge::Read { bytes } => JsonCodecCharge::Work { bytes, items: 0 },
                    other => other,
                }).map_err(|error| meter.root_error(error))
            }).map_err(|failure| match failure {
                AdmissionFailure::Root(error) => { meter.failure = Some(error); CodecError::Interrupted.into_io() },
                AdmissionFailure::Bound(reason) => meter.bound(reason).into_io(),
            })?;
            if let Some(output) = self.output.as_mut() {
                if bytes.len() > self.maximum - output.len() {
                    return Err(meter.bound("JSON exceeds admitted output").into_io());
                }
                meter.charge(JsonCodecCharge::Copy { bytes: bytes.len() as u64 }).map_err(CodecError::into_io)?;
                output.extend_from_slice(bytes);
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let size = {
        let mut first = Writer { scan: Scan::default(), bounds, meter: &mut meter, output: None, maximum: 0 };
        if let Err(error) = serde_json::to_writer(&mut first, value) {
            return Err(first.meter.root_error(error.into()));
        }
        if let Some(error) = first.meter.first_error() { return Err(error); }
        first.scan.end_inline().map_err(|failure| match failure {
            AdmissionFailure::Root(error) => error,
            AdmissionFailure::Bound(reason) => first.meter.root_error(CodecError::Bound(reason)),
        })?;
        usize::try_from(first.scan.bytes)
            .map_err(|_| first.meter.root_error(CodecError::Bound("JSON output exceeds address space")))?
    };
    if let Err(error) = meter.retain(size) {
        return Err(meter.root_error(error));
    }
    if let Err(error) = meter.charge(JsonCodecCharge::CopyAdmission { bytes: size as u64 })
        .and_then(|()| meter.allocation(size)) {
        return Err(meter.root_error(error));
    }
    let mut output = Vec::new();
    if size != 0 { output.try_reserve_exact(size).map_err(Error::Allocation)?; }
    {
        let mut second = Writer { scan: Scan::default(), bounds, meter: &mut meter, output: Some(&mut output), maximum: size };
        if let Err(error) = serde_json::to_writer(&mut second, value) {
            return Err(second.meter.root_error(error.into()));
        }
        if let Some(error) = second.meter.first_error() { return Err(error); }
        second.scan.end_inline().map_err(|failure| match failure {
            AdmissionFailure::Root(error) => error,
            AdmissionFailure::Bound(reason) => second.meter.root_error(CodecError::Bound(reason)),
        })?;
        if second.scan.bytes != size as u64 {
            return Err(second.meter.root_error(CodecError::Bound("JSON serialization changed between admission and encoding")));
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
    decode_bounded(bytes, bounds, maximum_owned_bytes, None, charge).map_err(DecodeFailure::into_portable_root)
}

/// SDK-private portable closed-seed carrier using the SAME decode engine.
/// Finite SDK producers provide their actual Value schema even in owned/WASM
/// mode; legacy generic T admission keeps its separate unknown-schema policy.
/// The concrete seed/custody obligations are identical to the native companion.
pub(crate) fn json_from_slice_seed_bounded_trusted<'de, S, F>(
    seed: S, bytes: &'de [u8], bounds: JsonBounds, maximum_owned_bytes: u64,
    value_schema: JsonValueSchema, charge: &mut F,
) -> Result<S::Value>
where S: serde::de::DeserializeSeed<'de>, F: FnMut(JsonCodecCharge) -> Result<()> {
    decode_seed_bounded(seed, bytes, bounds, maximum_owned_bytes, Some(value_schema), charge)
        .map_err(DecodeFailure::into_portable_root)
}

/// Native carrier for the SAME trusted concrete-schema decoder. Serde syntax
/// and schema failures move their actual source without formatting/copying a
/// diagnostic after admission. The caller must attach its original preclaimed
/// diagnostic allocation to NativeDecode's custody before returning that error,
/// and keep successful typed payload custody through its last use. No arbitrary
/// Deserialize heap/CPU or opaque allocator/RSS guarantee is made.
/// `code` preserves the caller's wire classification (request Invalid or
/// persisted-record Storage) for serde failures. SDK lexical/bounds failures
/// are inline static NativeBoundary Invalid errors, before owned error growth.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub fn json_from_slice_bounded_native_trusted<T: serde::de::DeserializeOwned, F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8], bounds: JsonBounds, maximum_owned_bytes: u64, code: crate::wire::ErrorCode,
    value_schema: JsonValueSchema, charge: &mut F,
) -> Result<T> {
    decode_bounded(bytes, bounds, maximum_owned_bytes, Some(value_schema), charge).map_err(|failure| match failure {
        DecodeFailure::Root(error) => error,
        DecodeFailure::Bound(reason) => Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason },
        DecodeFailure::Serde(source) => Error::NativeDecode { code, source, custody: None },
    })
}

/// SDK-private closed seed boundary using the SAME original lexical/serde
/// engine. Manifest seeds enforce their member bound BEFORE next_element and
/// growth. Original SDK work failures stay in the seed's existing owned sidecar
/// and take precedence over its STATIC serde marker. No SDK format/to_string
/// or diagnostic allocation is permitted after denial; serde's foreign private
/// Message/ErrorImpl allocations remain opaque, not a zero-malloc claim. Hold
/// the original preparse guard until partial typed payload physically drops.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn json_from_slice_seed_bounded_native_trusted<'de, S, F>(
    seed: S, bytes: &'de [u8], bounds: JsonBounds, maximum_owned_bytes: u64,
    code: crate::wire::ErrorCode, value_schema: JsonValueSchema, charge: &mut F,
) -> Result<S::Value>
where S: serde::de::DeserializeSeed<'de>, F: FnMut(JsonCodecCharge) -> Result<()> {
    decode_seed_bounded(seed, bytes, bounds, maximum_owned_bytes, Some(value_schema), charge)
        .map_err(|failure| match failure {
            DecodeFailure::Root(error) => error,
            DecodeFailure::Bound(reason) => Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason },
            DecodeFailure::Serde(source) => Error::NativeDecode { code, source, custody: None },
        })
}

/// SDK-private BORROWED Value-to-closed-DTO boundary. Inspect the original paid
/// tree (including selected virtual Raw inputs), then admit the concrete output
/// BEFORE the genuine borrowed Value deserializer executes. No source clone,
/// JSON round trip, encoded-byte charge or invented backend read occurs.
/// The closed owner MUST audit borrowed/owned visitor acceptance equivalence
/// and its actual new String/Vec/Content/Box/diagnostic quota; arbitrary custom
/// Deserialize and visit_string-only visitors are explicitly unsupported.
/// Consuming from_value callers retain their original semantics and use the
/// separate inspection/additional-quota helpers instead.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn json_from_value_bounded_native_trusted<T: serde::de::DeserializeOwned, F: FnMut(JsonCodecCharge) -> Result<()>>(
    source: &Value, bounds: JsonBounds, maximum_owned_bytes: u64, code: crate::wire::ErrorCode,
    value_schema: JsonValueSchema, charge: &mut F,
) -> Result<T> {
    let mut value_shape = json_owned_value_shape_bounded(source, bounds, value_schema, charge)?;
    value_shape.origin = ShapeOrigin::BorrowedConversion;
    charge(JsonCodecCharge::Admission { maximum_owned_bytes, value_shape })?;
    T::deserialize(source).map_err(|source| Error::NativeDecode { code, source, custody: None })
}

enum DecodeFailure {
    Root(Error),
    Bound(&'static str),
    Serde(serde_json::Error),
}
impl DecodeFailure {
    fn into_portable_root(self) -> Error {
        match self {
            Self::Root(error) => error,
            Self::Bound(reason) => Error::Invalid(reason.into()),
            Self::Serde(error) => Error::Invalid(error.to_string()),
        }
    }
}
impl From<Error> for DecodeFailure {
    fn from(error: Error) -> Self { Self::Root(error) }
}
impl From<AdmissionFailure> for DecodeFailure {
    fn from(failure: AdmissionFailure) -> Self {
        match failure {
            AdmissionFailure::Root(error) => Self::Root(error),
            AdmissionFailure::Bound(reason) => Self::Bound(reason),
        }
    }
}

fn decode_bounded<T: serde::de::DeserializeOwned, F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8], bounds: JsonBounds, maximum_owned_bytes: u64,
    value_schema: Option<JsonValueSchema>, charge: &mut F,
) -> std::result::Result<T, DecodeFailure> {
    decode_seed_bounded(std::marker::PhantomData::<T>, bytes, bounds, maximum_owned_bytes, value_schema, charge)
}

fn decode_seed_bounded<'de, S, F>(
    seed: S, bytes: &'de [u8], bounds: JsonBounds, maximum_owned_bytes: u64,
    value_schema: Option<JsonValueSchema>, charge: &mut F,
) -> std::result::Result<S::Value, DecodeFailure>
where S: serde::de::DeserializeSeed<'de>, F: FnMut(JsonCodecCharge) -> Result<()> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    let value_shape = admit_json_value_shape_inline(bytes, bounds, value_schema, charge)?;
    // Vec scratch growth is geometric; reserve its conservative logical
    // capacity, not just string length. Borrowed input itself is not newly owned.
    let scratch = (bytes.len() as u64).max(8).checked_mul(2)
        .ok_or(DecodeFailure::Bound("JSON scratch bound overflow"))?;
    let maximum_owned_bytes = maximum_owned_bytes.checked_add(scratch)
        .ok_or(DecodeFailure::Bound("JSON owned bound overflow"))?;
    charge(JsonCodecCharge::Admission { maximum_owned_bytes, value_shape })?;
    charge(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let value = seed.deserialize(&mut decoder).map_err(DecodeFailure::Serde)?;
    decoder.end().map_err(DecodeFailure::Serde)?;
    drop(decoder);
    Ok(value)
}

struct Raw<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> { encoder: Encoder<'a, 'b, F> }
macro_rules! bad_raw {
    ($method:ident $(, $ty:ty)?) => {
        fn $method(self $(, _: $ty)?) -> JsonResult<Value> { Err(CodecError::Bound("expected value")) }
    };
}
impl<F: FnMut(JsonCodecCharge) -> Result<()>> ser::Serializer for Raw<'_, '_, F> {
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
        // Legacy convenience retains its existing trusted unbounded policy.
        // Finite producers inspect genuine Value virtual sources before growth.
        let schema = if meter.bounds.maximum_depth == u32::MAX { None } else { Some(JsonValueSchema::Root) };
        let mut scan = Scan::with_value_schema(schema, bounds);
        scan.push_inline(value.as_bytes(), bounds, &mut |event| {
            let items = match event { JsonCodecCharge::Work { items, .. } => items, _ => 0 };
            meter.charge(event).map_err(|error| meter.root_error(error))?;
            meter.items += items;
            Ok(())
        }).map_err(|failure| match failure {
            AdmissionFailure::Root(error) => { meter.failure = Some(error); CodecError::Interrupted },
            AdmissionFailure::Bound(reason) => CodecError::Bound(reason),
        })?;
        let value_shape = scan.value_shape().map_err(|failure| match failure {
            AdmissionFailure::Root(error) => { meter.failure = Some(error); CodecError::Interrupted },
            AdmissionFailure::Bound(reason) => CodecError::Bound(reason),
        })?;
        meter.items = meter.items.checked_add(value_shape.virtual_items)
            .ok_or_else(|| CodecError::Bound("JSON item count overflow"))?;
        let maximum_owned_bytes = value_storage_bytes(value_shape)
            .and_then(|bytes| value_shape.input_bytes.max(8).checked_mul(4).and_then(|strings| bytes.checked_add(strings)))
            .ok_or(CodecError::Bound("JSON owned bound overflow"))?;
        meter.charge(JsonCodecCharge::Admission { maximum_owned_bytes, value_shape })?;
        // Keep serde_json::to_value(RawValue)'s existing number/grammar/recursion
        // semantics, rather than treating its private token as an ordinary map.
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
    fn direct_value_preserves_canonical_semantics_and_only_actual_tree_custody() -> Result<()> {
        let record = (Pairs(vec![("z".into(), u64::MAX), ("a".into(), 1), ("z".into(), 2)]),
            vec![i64::MIN, i64::MAX], 0.1f32, -0.0f64, f64::NAN, "escape\n\"\\\u{1f600}");
        let mut expected = serde_json::to_value(&record)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        expected.sort_all_objects();
        let mut live = 0u64;
        let mut peak = 0u64;
        let value = json_value_bounded_trusted(&record, BOUNDS, &mut |event| {
            match event {
                JsonCodecCharge::Retain { bytes } => { live += bytes; peak = peak.max(live); }
                JsonCodecCharge::Release { bytes } => live = live.checked_sub(bytes).expect("conserved quota"),
                _ => {}
            }
            Ok(())
        })?;
        assert_eq!(value, expected);
        assert_eq!(live, owned_logical_bytes(&value));
        assert!(peak >= live);
        let retained = live;
        drop(value);
        live = live.checked_sub(retained).expect("caller releases returned-tree custody");
        assert_eq!(live, 0);
        Ok(())
    }

    #[test]
    fn direct_value_all_cancellation_schedules_preserve_owned_cause_and_release() -> Result<()> {
        let record = (Pairs(vec![("same".into(), 1), ("same".into(), 2)]),
            vec!["escape\n\"\\", "another"], u64::MAX, 0.1f32);
        let mut callbacks = 0;
        let baseline = json_value_bounded_trusted(&record, BOUNDS, &mut |event| {
            if !matches!(event, JsonCodecCharge::Release { .. }) { callbacks += 1; }
            Ok(())
        })?;
        drop(baseline);
        for cancel_at in 0..callbacks {
            let operation = crate::OperationId::from_bytes((cancel_at as u128).to_le_bytes());
            let mut cause = Some(Error::Indeterminate(operation));
            let mut index = 0;
            let mut live = 0u64;
            let result = json_value_bounded_trusted(&record, BOUNDS, &mut |event| {
                if let JsonCodecCharge::Release { bytes } = event {
                    live = live.checked_sub(bytes).expect("cleanup remains allowed");
                    return Ok(());
                }
                if index == cancel_at { return Err(cause.take().expect("one original owned cause")); }
                index += 1;
                if let JsonCodecCharge::Retain { bytes } = event { live += bytes; }
                Ok(())
            });
            assert!(matches!(result, Err(Error::Indeterminate(id)) if id == operation));
            assert_eq!(live, 0);
        }
        Ok(())
    }

    #[test]
    fn callback_interruption_is_inline_not_a_serde_custom_message() {
        let operation = crate::OperationId::from_bytes([42; 16]);
        let mut original = Some(Error::Indeterminate(operation));
        let mut charge = |_| Err(original.take().expect("one cause"));
        let mut meter = Meter { bounds: BOUNDS, items: 0, bytes: 0,
            charge: &mut charge, failure: None, retained: 0, bound_failure: None, native_code: None, tree_only: false };
        let interrupted = meter.item(0).unwrap_err();
        assert!(matches!(interrupted, CodecError::Interrupted));
        assert!(std::error::Error::source(&interrupted).is_none());
        assert!(matches!(meter.failure.take(), Some(Error::Indeterminate(id)) if id == operation));
        assert_eq!(interrupted.into_io().kind(), std::io::ErrorKind::Interrupted);
    }

    #[test]
    fn direct_value_bounds_and_discarded_temporary_keys_conserve_custody() -> Result<()> {
        struct ReplacedKey;
        impl Serialize for ReplacedKey {
            fn serialize<S: ser::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_key("discarded")?;
                map.serialize_key("kept")?;
                map.serialize_value(&u64::MAX)?;
                map.end()
            }
        }
        let mut live = 0u64;
        let value = json_value_bounded_trusted(&ReplacedKey, BOUNDS, &mut |event| {
            match event {
                JsonCodecCharge::Retain { bytes } => live += bytes,
                JsonCodecCharge::Release { bytes } => live = live.checked_sub(bytes).expect("no double release"),
                _ => {}
            }
            Ok(())
        })?;
        assert_eq!(value["kept"], u64::MAX);
        assert_eq!(live, owned_logical_bytes(&value));
        drop(value);
        let record = vec![vec!["escape\n\"\\"]];
        for bounds in [JsonBounds { maximum_bytes: 1, ..BOUNDS },
            JsonBounds { maximum_items: 1, ..BOUNDS }, JsonBounds { maximum_depth: 1, ..BOUNDS }] {
            let mut active = 0u64;
            assert!(json_value_bounded_trusted(&record, bounds, &mut |event| {
                match event {
                    JsonCodecCharge::Retain { bytes } => active += bytes,
                    JsonCodecCharge::Release { bytes } => active = active.checked_sub(bytes).expect("released after drop"),
                    _ => {}
                }
                Ok(())
            }).is_err());
            assert_eq!(active, 0);
        }
        Ok(())
    }

    #[test]
    fn closed_lexical_diagnostic_witness_matches_real_owned_payloads() {
        for (bytes, bounds) in [
            (b"0".as_slice(), JsonBounds { maximum_bytes: 0, ..BOUNDS }),
            (b"]".as_slice(), BOUNDS),
            (b"0".as_slice(), JsonBounds { maximum_items: 0, ..BOUNDS }),
            (b"[".as_slice(), BOUNDS),
        ] {
            let error = admit_json(bytes, bounds, &mut |_| Ok(())).unwrap_err();
            match error {
                Error::Invalid(message) => assert!(message.capacity() as u64 <= lexical_json_diagnostic_bytes()),
                other => panic!("unexpected lexical cause: {other}"),
            }
        }
        assert!(sdk_json_diagnostic_bytes() > lexical_json_diagnostic_bytes());
    }

    #[test]
    fn closed_value_quota_uses_the_actual_single_lexical_pass() -> Result<()> {
        for bytes in [
            b"[]".as_slice(),
            br#"[1]"#.as_slice(),
            br#"{"x":"escape\n\u0041","x":[18446744073709551615,-9223372036854775808,0.1,-0.0]}"#.as_slice(),
        ] {
            let mut input_bytes = 0u64;
            let mut items = 0u64;
            let mut quota = None;
            let value: Value = json_from_slice_bounded_trusted(bytes, BOUNDS, 0, &mut |event| {
                match event {
                    JsonCodecCharge::Read { bytes } => input_bytes += bytes,
                    JsonCodecCharge::Work { items: visited, .. } => items += visited,
                    JsonCodecCharge::Admission { value_shape, .. } => {
                        assert_eq!(input_bytes, bytes.len() as u64);
                        assert_eq!(value_shape.lexical_items(), items);
                        quota = Some(json_value_decode_owned_bytes(value_shape)?);
                    }
                    _ => {}
                }
                Ok(())
            })?;
            assert!(owned_logical_bytes(&value) <= quota.expect("quota precedes parser"));
            drop(value);
        }
        for (input_bytes, lexical_items) in [(u64::MAX, 1), (1, u64::MAX)] {
            let overflow_shape = JsonValueShape { input_bytes, lexical_items, virtual_items: 0,
                raw_bytes: 0, raw_frames: 0, unresolved_raw: false, origin: ShapeOrigin::Slice };
            assert!(json_value_decode_owned_bytes(overflow_shape).is_err());
        }
        let mut admission = false;
        let cause = Uuid::new_v4();
        let error = json_from_slice_bounded_trusted::<Value, _>(br#"["owned"]"#, BOUNDS, 0, &mut |event| {
            if matches!(event, JsonCodecCharge::Admission { .. }) {
                admission = true;
                return Err(Error::Indeterminate(cause));
            }
            Ok(())
        }).unwrap_err();
        assert!(admission);
        assert!(matches!(error, Error::Indeterminate(id) if id == cause));
        Ok(())
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
    fn copy_preflight_allocator_and_copy_preserve_the_real_prefix() -> Result<()> {
        // One direct String avoids opaque map/parser allocations entirely.
        for denied in 0..3 {
            let mut live = 0u64;
            let mut attempted = 0u64;
            let mut copied = 0u64;
            let cause = Uuid::new_v4();
            let result = json_value_bounded_trusted("SDK bytes", BOUNDS, &mut |event| {
                match event {
                    JsonCodecCharge::Retain { bytes } => live += bytes,
                    JsonCodecCharge::Release { bytes } => live = live.checked_sub(bytes).expect("drop before release"),
                    JsonCodecCharge::CopyAdmission { bytes } => {
                        assert_eq!(bytes, 9);
                        assert_eq!(attempted, 0);
                        assert_eq!(copied, 0);
                        if denied == 0 { return Err(Error::Indeterminate(cause)); }
                    }
                    JsonCodecCharge::AllocationOperation { bytes } => {
                        assert_eq!(bytes, 9);
                        assert_eq!(copied, 0);
                        if denied == 1 { return Err(Error::Indeterminate(cause)); }
                        attempted += 1;
                    }
                    JsonCodecCharge::Copy { bytes } => {
                        assert_eq!(attempted, 1);
                        assert_eq!(bytes, 9);
                        if denied == 2 { return Err(Error::Indeterminate(cause)); }
                        copied += bytes;
                    }
                    _ => {}
                }
                Ok(())
            });
            assert!(matches!(result, Err(Error::Indeterminate(id)) if id == cause));
            assert_eq!(copied, 0);
            assert_eq!(attempted, u64::from(denied == 2));
            assert_eq!(live, 0);
        }
        let mut sequence = Vec::new();
        let value = json_value_bounded_trusted("SDK bytes", BOUNDS, &mut |event| {
            match event {
                JsonCodecCharge::CopyAdmission { bytes } => sequence.push((0, bytes)),
                JsonCodecCharge::AllocationOperation { bytes } => sequence.push((1, bytes)),
                JsonCodecCharge::Copy { bytes } => sequence.push((2, bytes)),
                _ => {}
            }
            Ok(())
        })?;
        assert_eq!(sequence, [(0, 9), (1, 9), (2, 9)]);
        assert_eq!(value, "SDK bytes");
        // Empty strings have no allocation/reservation operation.
        json_value_bounded_trusted("", BOUNDS, &mut |event| {
            assert!(!matches!(event, JsonCodecCharge::AllocationOperation { .. }));
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn actual_reserve_rejection_moves_its_typed_cause_without_formatting() {
        let mut operations = 0;
        let mut meter = Meter { bounds: BOUNDS, items: 0, bytes: 0, retained: 0,
            failure: None, bound_failure: None, native_code: None, tree_only: false, charge: &mut |event| {
                if let JsonCodecCharge::AllocationOperation { bytes } = event {
                    assert_eq!(bytes, usize::MAX as u64);
                    operations += 1;
                }
                Ok(())
            } };
        meter.allocation(usize::MAX).expect("operation admitted");
        let mut buffer = Vec::<u8>::new();
        let cause = buffer.try_reserve_exact(usize::MAX).expect_err("actual capacity-overflow rejection");
        let root = CodecError::from(cause).into_root_with_code(None);
        assert!(matches!(root, Error::Allocation(_)));
        assert_eq!(buffer.capacity(), 0);
        drop(meter);
        assert_eq!(operations, 1);
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
            let operation = crate::OperationId::from_bytes((stop as u128).to_le_bytes());
            let mut error = Some(Error::Indeterminate(operation));
            let result = canonical_json_bytes_bounded(&value, BOUNDS, &mut |event| {
                if let JsonCodecCharge::Release { bytes } = event {
                    live = live.checked_sub(bytes).expect("release only admitted quota");
                    return Ok(());
                }
                if seen == stop {
                    return Err(error.take().expect("codec must stop after its first failure"));
                }
                seen += 1;
                if let JsonCodecCharge::Retain { bytes } = event { live += bytes; }
                Ok(())
            });
            assert!(matches!(result, Err(Error::Indeterminate(original)) if original == operation));
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
            let operation = crate::OperationId::from_bytes((stop as u128).to_le_bytes());
            let mut error = Some(Error::Indeterminate(operation));
            let result = json_bytes_bounded_trusted(&value, BOUNDS, &mut |event| {
                if let JsonCodecCharge::Release { bytes } = event {
                    live = live.checked_sub(bytes).expect("release only admitted quota");
                    return Ok(());
                }
                if seen == stop {
                    return Err(error.take().expect("codec must stop after its first failure"));
                }
                seen += 1;
                if let JsonCodecCharge::Retain { bytes } = event { live += bytes; }
                Ok(())
            });
            assert!(matches!(result, Err(Error::Indeterminate(original)) if original == operation));
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
        assert!(matches!(failure, Err(Error::Invalid(_))));
        assert_eq!(live, 0);
        let operation = crate::OperationId::from_bytes([91; 16]);
        let mut first = Some(Error::Indeterminate(operation));
        let mut copy_failures = 0;
        let failure = canonical_json_bytes_bounded(&IgnoredErrors, BOUNDS, &mut |event| {
            match event {
                JsonCodecCharge::Retain { bytes } => live += bytes,
                JsonCodecCharge::Release { bytes } => live = live.checked_sub(bytes).expect("balanced release"),
                JsonCodecCharge::Copy { .. } => {
                    copy_failures += 1;
                    return Err(first.take().expect("codec must stop after its first failure"));
                }
                _ => {}
            }
            Ok(())
        });
        assert!(matches!(failure, Err(Error::Indeterminate(original)) if original == operation));
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_trusted_decoder_preserves_actual_serde_cause_and_callback_primary() -> Result<()> {
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Record { text: String }
        let valid = br#"{"text":"escape\n\u0041"}"#;
        let record: Record = json_from_slice_bounded_native_trusted(valid, BOUNDS, 128, crate::wire::ErrorCode::Invalid, &mut |_| Ok(()))?;
        assert_eq!(record.text, "escape\nA");
        for bytes in [
            br#"{"text":"\q"}"#.as_slice(),
            br#"{"unknown\"field":"large diagnostic","text":"value"}"#.as_slice(),
            br#"{"text":"value"} {"text":"extra"}"#.as_slice(),
        ] {
            let expected = serde_json::from_slice::<Record>(bytes).unwrap_err();
            let error = json_from_slice_bounded_native_trusted::<Record, _>(bytes, BOUNDS, 128, crate::wire::ErrorCode::Storage, &mut |_| Ok(())).unwrap_err();
            match error {
                Error::NativeDecode { code: crate::wire::ErrorCode::Storage, source, custody: None } => {
                    assert_eq!(source.classify(), expected.classify());
                    assert_eq!(source.line(), expected.line());
                    assert_eq!(source.column(), expected.column());
                    assert_eq!(source.to_string(), expected.to_string());
                }
                other => panic!("native producer replaced the actual serde cause: {other}"),
            }
        }
        let cause = Uuid::new_v4();
        let error = json_from_slice_bounded_native_trusted::<Record, _>(valid, BOUNDS, 128, crate::wire::ErrorCode::Invalid, &mut |event| {
            if matches!(event, JsonCodecCharge::Admission { .. }) {
                return Err(Error::Indeterminate(cause));
            }
            Ok(())
        }).unwrap_err();
        assert!(matches!(error, Error::Indeterminate(id) if id == cause));
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_lexical_and_overflow_denials_are_inline_before_schema_growth() {
        for (bytes, bounds, owned, label) in [
            (b"0".as_slice(), JsonBounds { maximum_bytes: 0, ..BOUNDS }, 0, LEXICAL_BYTE_ERROR),
            (b"]".as_slice(), BOUNDS, 0, LEXICAL_NESTING_ERROR),
            (b"0".as_slice(), JsonBounds { maximum_items: 0, ..BOUNDS }, 0, LEXICAL_ITEM_ERROR),
            (b"[".as_slice(), BOUNDS, 0, LEXICAL_INCOMPLETE_ERROR),
            (b"0".as_slice(), BOUNDS, u64::MAX, "JSON owned bound overflow"),
        ] {
            let mut admitted = false;
            let error = json_from_slice_bounded_native_trusted::<Value, _>(
                bytes, bounds, owned, crate::wire::ErrorCode::Storage, &mut |event| {
                    admitted |= matches!(event, JsonCodecCharge::Admission { .. });
                    Ok(())
                },
            ).unwrap_err();
            assert!(!admitted);
            assert!(matches!(error, Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid, reason
            } if reason == label));
        }
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
            let mut retained = 0u64;
            let direct = json_value_bounded_trusted(&value, BOUNDS, &mut |event| {
                match event {
                    JsonCodecCharge::Retain { bytes } => retained += bytes,
                    JsonCodecCharge::Release { bytes } => retained = retained.checked_sub(bytes).expect("conserved tree quota"),
                    _ => {}
                }
                Ok(())
            }).expect("admitted direct Value");
            proptest::prop_assert_eq!(&direct, &old);
            proptest::prop_assert_eq!(retained, owned_logical_bytes(&direct));
        }
    }
}
