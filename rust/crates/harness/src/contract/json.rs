//! The canonical codec bounds logical data, not allocator metadata or RSS.

use super::{Error, Result};
use serde::ser::{self, Serialize};
use serde_json::Value;

mod value_shape;
use value_shape::ValueInspector;
#[cfg(test)]
mod value_shape_tests;

/// Finite admission for one JSON document.
#[derive(Clone, Copy, Debug)]
pub struct JsonBounds {
    /// UTF-8 encoded/input byte limit; duplicate emitted entries count too.
    pub maximum_bytes: u64,
    /// Values and map keys visited, including replaced duplicate values.
    pub maximum_items: u64,
    /// Zero-based nesting depth.
    pub maximum_depth: u32,
}

impl JsonBounds {
    pub(crate) const IDENTITY: Self = Self {
        maximum_bytes: usize::MAX as u64,
        maximum_items: usize::MAX as u64,
        maximum_depth: u32::MAX,
    };
}

/// SDK-audited closed Value field reachability, not an authority or an arbitrary user selector.
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
    /// The SDK Outcome<Value> root's externally tagged Succeeded payload only.
    TaskOutcome,
    /// Provider join proof statement only.
    ProjectMergeReceipt,
    /// Stock tool task's arguments only.
    ToolTaskCommand,
    /// Stock task admission command's input only.
    TaskAdmitCommand,
    /// Prepared task admission's nested input and input/output schemas.
    PreparedTaskAdmitCommand,
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
    /// Recursive merge plan's optional authorized-source native snapshot.
    RecursiveMergeToolPlan,
}

/// Copied data issued by an SDK source pass or borrowed resident-tree inspection.
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
    /// Source bytes consumed once; for resident inspection, the owned UTF-8 payload/node lower bound instead.
    pub const fn input_bytes(self) -> u64 { self.input_bytes }
    /// Original lexical or resident values/map keys, including source duplicates.
    pub const fn lexical_items(self) -> u64 { self.lexical_items }
    /// Additional lexical nodes in inspected decoded RawValue sources.
    pub const fn virtual_items(self) -> u64 { self.virtual_items }
    /// Whether a possible RawValue path requires conservative byte-based quota.
    pub const fn unresolved_raw(self) -> bool { self.unresolved_raw }
}

/// Called before admitted logical storage grows and after it is freed.
#[derive(Clone, Copy, Debug)]
pub enum JsonCodecCharge {
    /// Encoded-byte/item work for byte producers (including sizing passes).
    Work {
        /// Encoded bytes.
        bytes: u64,
        /// Structural items.
        items: u64,
    },
    /// Admit requested logical payload/slot storage before allocation.
    Retain {
        /// Logical storage bytes, excluding opaque allocator/map node overhead.
        bytes: u64,
    },
    /// Release storage after its codec-owned buffer/tree has been freed.
    Release {
        /// Previously admitted logical storage bytes.
        bytes: u64,
    },
    /// Preflight copy allowance before a potentially unnecessary allocation.
    CopyAdmission {
        /// Bytes that the subsequent SDK buffer copy would copy.
        bytes: u64,
    },
    /// Immediately before one genuine positive SDK String/Vec try_reserve call.
    AllocationOperation {
        /// Full requested new capacity in bytes, not the logical growth delta or an observation of allocator metadata/RSS.
        bytes: u64,
    },
    /// Bytes about to be copied into an already-reserved buffer.
    Copy {
        /// Copied bytes.
        bytes: u64,
    },
    /// Bytes consumed by lexical input scanning, not backend physical reads.
    Read {
        /// Borrowed input bytes consumed.
        bytes: u64,
    },
    /// Actual virtual decoded bytes, resident nodes/keys or sizing-writer chunks inspected by the SDK.
    Inspect {
        /// Actual inspection units, not a hypothetical maximum.
        items: u64,
    },
    /// Conservative schema/scratch preflight, NOT actual allocated bytes.
    Admission {
        /// Upper bound justified by the concrete audited schema.
        maximum_owned_bytes: u64,
        /// SDK-issued Value shape from the original source/tree pass.
        value_shape: JsonValueShape,
    },
}

/// Logical owned capacities of an already admitted, finite-depth Value.
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

#[derive(Debug)]
enum CodecError {
    Root(Error),
    Interrupted,
    Bound(&'static str),
    Json(serde_json::Error),
    Allocation(std::collections::TryReserveError),
}
impl std::fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Root(error) => std::fmt::Display::fmt(error, formatter),
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
            Self::Root(error) => Some(error),
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
impl From<Error> for CodecError {
    fn from(error: Error) -> Self { Self::Root(error) }
}
impl CodecError {
    fn into_io(self) -> std::io::Error {
        match self {
            Self::Root(_) => unreachable!("original errors stay outside serde's writer bridge"),
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
                Self::Root(error) => error,
                Self::Interrupted => unreachable!("interruption retains its original owned Root cause"),
                Self::Bound(reason) => Error::NativeBoundary { code, reason },
                Self::Json(source) => Error::NativeEncode { code, source, custody: None },
                Self::Allocation(error) => Error::Allocation(error),
            };
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = native_code;
        match self {
            Self::Root(error) => error,
            Self::Interrupted => unreachable!("interruption retains its original owned Root cause"),
            Self::Bound(reason) => Error::Invalid(reason.into()),
            Self::Json(error) => Error::Invalid(error.to_string()),
            Self::Allocation(error) => Error::Allocation(error),
        }
    }
    fn into_error(self, code: Option<crate::wire::ErrorCode>) -> Error {
        self.into_root_with_code(code.map(|_| crate::wire::ErrorCode::Invalid))
    }
}

/// Closed SDK-generated encoder/Raw-Value diagnostic allowance only.
pub const fn sdk_json_diagnostic_bytes() -> u64 {
    const CAPACITY_ERROR: &str = concat!("memory allocation failed",
        " because the computed capacity exceeded the collection's maximum");
    const SYNTAX_ERROR: &str =
        "control character (\\u0000-\\u001F) found while parsing a string";
    let largest = if CAPACITY_ERROR.len() > SYNTAX_ERROR.len() {
        CAPACITY_ERROR.len() as u64
    } else { SYNTAX_ERROR.len() as u64 };
    let rendered = largest + "invalid request: ".len() as u64
        + " at line ".len() as u64 + " column ".len() as u64
        + 2 * (u64::MAX.ilog10() as u64 + 1);
    let wrappers = std::mem::size_of::<CodecError>() as u64
        + std::mem::size_of::<serde_json::Error>() as u64
        + std::mem::size_of::<std::io::Error>() as u64
        + std::mem::size_of::<String>() as u64;
    4 * rendered + 2 * wrappers
}

/// Conservative logical quota for the CLOSED serde_json::Value parser, derived from the SDK-issued shape in the ORIGINAL source pass.
pub fn json_value_decode_owned_bytes(shape: JsonValueShape) -> Result<u64> {
    value_full_bytes(shape).ok_or_else(|| shape_boundary("JSON owned bound overflow"))
}
fn value_full_bytes(shape: JsonValueShape) -> Option<u64> {
    value_storage_bytes(shape)
        .and_then(|bytes| shape.input_bytes.max(8).checked_mul(4).and_then(|strings| bytes.checked_add(strings)))
}

/// Inspection only before an SDK's existing consuming from_value conversion.
pub(crate) fn json_owned_value_shape_bounded<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &Value, bounds: JsonBounds, schema: JsonValueSchema, charge: &mut F,
) -> Result<JsonValueShape> {
    value_shape::inspect_owned(value, JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds }, schema, charge)
        .map_err(|error| error.into_error(Some(crate::wire::ErrorCode::Invalid)))
}

/// NEW logical Value-container/Raw replay/scratch/static diagnostic quota for a genuine consuming Value converter, using the original SDK-issued owned shape.
pub fn json_owned_value_conversion_additional_bytes(shape: JsonValueShape) -> Result<u64> {
    if shape.origin != ShapeOrigin::OwnedConversion { return Err(shape_boundary("JSON conversion shape is not owned")); }
    value_storage_bytes(shape).ok_or_else(|| shape_boundary("JSON owned bound overflow"))
}

/// Pure arithmetic for an audited SDK BORROWED closed DTO.
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
        shape.input_bytes.max(8).checked_mul(4 * (u64::from(usize::BITS) + 2))?
    } else {
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
        let retained = std::mem::take(&mut self.retained);
        if retained != 0 {
            let _ = (self.charge)(JsonCodecCharge::Release { bytes: retained });
        }
    }
}

impl<F: FnMut(JsonCodecCharge) -> Result<()>> Meter<'_, F> {
    fn forward(&mut self, error: CodecError) -> CodecError {
        match error {
            CodecError::Root(error) => { self.failure = Some(error); CodecError::Interrupted },
            error => error,
        }
    }
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

/// Allocates only after each logical payload/item has passed admission.
pub fn canonical_json_bytes_bounded<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, code: Option<crate::wire::ErrorCode>, charge: &mut F,
) -> Result<Vec<u8>> {
    encode(value, JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds }, code, charge)
}

/// Native carrier for the legacy identity policy, restricted to an already paid, trusted resident Value.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn canonical_json_bytes_identity_native_trusted<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &Value, code: crate::wire::ErrorCode, charge: &mut F,
) -> Result<Vec<u8>> {
    encode(value, JsonBounds::IDENTITY, Some(code), charge)
}

/// SDK-private sizing of an already bounded, audited closed source.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn validate_json_byte_bound_native_trusted<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, code: crate::wire::ErrorCode, charge: &mut F,
) -> Result<()> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    count_json_bytes_trusted(value, bounds, Some(code), charge).map(|_| ())
}

pub(super) fn count_json_bytes_trusted<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, native_code: Option<crate::wire::ErrorCode>, charge: &mut F,
) -> Result<u64> {
    let mut meter = Meter { bounds, items: 0, bytes: 0, charge, failure: None,
        bound_failure: None, native_code, retained: 0, tree_only: true };
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
            }).map_err(|error| match meter.forward(error) {
                CodecError::Bound(reason) => meter.bound(reason).into_io(),
                error => error.into_io(),
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
    sizing.scan.end_inline().map_err(|error| sizing.meter.root_error(error))?;
    if let Err(error) = sizing.meter.charge(JsonCodecCharge::Work { bytes: 0, items: 0 }) {
        return Err(sizing.meter.root_error(error));
    }
    Ok(sizing.scan.bytes)
}

/// Produces an admitted JSON value.
pub fn json_value_bounded<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, native_code: Option<crate::wire::ErrorCode>, charge: &mut F,
) -> Result<Value> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    let mut meter = Meter { bounds, items: 0, bytes: 0, charge, failure: None,
        bound_failure: None, native_code, retained: 0, tree_only: true };
    let value = encode_value(value, &mut meter)?;
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
    if let Err(error) = meter.charge(JsonCodecCharge::Work { bytes: 0, items: 0 }) {
        return Err(meter.root_error(error));
    }
    value.sort_all_objects();
    Ok(value)
}

/// Finish an SDK-owned, already canonical immutable journal template in its caller-owned prepaid output.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn canonical_json_value_into_prepaid_native_trusted<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &Value, output: &mut Vec<u8>, maximum_bytes: u64,
    code: crate::wire::ErrorCode, charge: &mut F,
) -> Result<()> {
    let maximum = usize::try_from(maximum_bytes)
        .map_err(|_| Error::NativeBoundary { code, reason: "JSON output exceeds address space" })?;
    if !output.is_empty() {
        return Err(Error::NativeBoundary { code, reason: "JSON prepaid output must be empty" });
    }
    if output.capacity() < maximum {
        return Err(Error::NativeBoundary { code, reason: "JSON prepaid output capacity is insufficient" });
    }
    let mut meter = Meter { bounds: JsonBounds { maximum_bytes, ..JsonBounds::IDENTITY },
        items: 0, bytes: 0, charge, failure: None, bound_failure: None,
        native_code: Some(code), retained: 0, tree_only: false };
    if let Err(error) = meter.charge(JsonCodecCharge::Work { bytes: 0, items: 0 }) {
        return Err(meter.root_error(error));
    }
    write_canonical_output(value, output, maximum, &mut meter)
}

struct Output<'a, 'b, F: FnMut(JsonCodecCharge) -> Result<()>> {
    bytes: &'a mut Vec<u8>,
    maximum: usize,
    meter: &'a mut Meter<'b, F>,
}

impl<F: FnMut(JsonCodecCharge) -> Result<()>> std::io::Write for Output<'_, '_, F> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let remaining = self.maximum.checked_sub(self.bytes.len())
            .ok_or_else(|| self.meter.bound("JSON exceeds admitted output").into_io())?;
        if bytes.len() > remaining {
            return Err(self.meter.bound("JSON exceeds admitted output").into_io());
        }
        self.meter.charge(JsonCodecCharge::Work { bytes: bytes.len() as u64, items: 0 }).map_err(CodecError::into_io)?;
        self.meter.charge(JsonCodecCharge::Copy { bytes: bytes.len() as u64 }).map_err(CodecError::into_io)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

fn write_canonical_output<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &Value, bytes: &mut Vec<u8>, maximum: usize, meter: &mut Meter<'_, F>,
) -> Result<()> {
    if let Err(error) = serde_json::to_writer(Output { bytes, maximum, meter }, value) {
        return Err(meter.root_error(error.into()));
    }
    Ok(())
}

pub(super) fn encode<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
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
    write_canonical_output(&value, &mut bytes, size, &mut meter)?;
    let released = meter.retained - size as u64;
    drop(value);
    if let Err(error) = meter.release(released) {
        return Err(meter.root_error(error));
    }
    meter.retained = 0;
    Ok(bytes)
}

const LEXICAL_BYTE_ERROR: &str = "JSON input exceeds declared byte bound";
const LEXICAL_NESTING_ERROR: &str = "invalid JSON nesting";
const LEXICAL_ITEM_ERROR: &str = "JSON exceeds declared structural bound";
const LEXICAL_INCOMPLETE_ERROR: &str = "incomplete JSON document";

/// Maximum ONE newly owned SDK lexical-error String payload.
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
impl Scan {
    fn with_value_schema(schema: Option<JsonValueSchema>, bounds: JsonBounds) -> Self {
        Self { inspector: Some(ValueInspector::new(schema, bounds)), ..Self::default() }
    }
    fn value_shape(&self) -> std::result::Result<JsonValueShape, CodecError> {
        self.end_inline()?;
        self.inspector.as_ref().expect("Value shape requested only from configured source pass")
            .finish(self.bytes, self.items)
    }
    fn push<F: FnMut(JsonCodecCharge) -> Result<()>>(&mut self, bytes: &[u8], bounds: JsonBounds, charge: &mut F) -> Result<()> {
        self.push_inline(bytes, bounds, charge).map_err(|failure| failure.into_error(None))
    }
    fn push_inline<F: FnMut(JsonCodecCharge) -> Result<()>>(
        &mut self, bytes: &[u8], bounds: JsonBounds, charge: &mut F,
    ) -> std::result::Result<(), CodecError> {
        for &byte in bytes {
            if self.bytes >= bounds.maximum_bytes { return Err(CodecError::Bound(LEXICAL_BYTE_ERROR)); }
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
                b']' | b'}' => { self.depth = self.depth.checked_sub(1).ok_or(CodecError::Bound(LEXICAL_NESTING_ERROR))?; self.atom = false; false }
                b':' | b',' | b' ' | b'\r' | b'\n' | b'\t' => { self.atom = false; false }
                _ => { let first = !self.atom; self.atom = true; first }
            };
            if item {
                if item_depth > bounds.maximum_depth || self.items >= bounds.maximum_items {
                    return Err(CodecError::Bound(LEXICAL_ITEM_ERROR));
                }
                charge(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                self.items += 1;
            }
        }
        Ok(())
    }
    fn end(&self) -> Result<()> {
        self.end_inline().map_err(|failure| failure.into_error(None))
    }
    fn end_inline(&self) -> std::result::Result<(), CodecError> {
        if self.string || self.depth != 0 { return Err(CodecError::Bound(LEXICAL_INCOMPLETE_ERROR)); }
        Ok(())
    }
}

/// Bounds lexical tokens, including duplicate keys.
pub(crate) fn admit_json<F: FnMut(JsonCodecCharge) -> Result<()>>(bytes: &[u8], bounds: JsonBounds, charge: &mut F) -> Result<()> {
    admit_json_trusted(bytes, JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds }, charge)
}

pub(super) fn admit_json_trusted<F: FnMut(JsonCodecCharge) -> Result<()>>(bytes: &[u8], bounds: JsonBounds, charge: &mut F) -> Result<()> {
    admit_json_inline(bytes, bounds, charge).map_err(|failure| failure.into_error(None))
}

fn admit_json_inline<F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8], bounds: JsonBounds, charge: &mut F,
) -> std::result::Result<(), CodecError> {
    if bytes.len() as u64 > bounds.maximum_bytes { return Err(CodecError::Bound(LEXICAL_BYTE_ERROR)); }
    let mut scan = Scan::default();
    scan.push_inline(bytes, bounds, charge)?;
    scan.end_inline()
}

fn admit_json_value_shape_inline<F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8], bounds: JsonBounds, schema: Option<JsonValueSchema>, charge: &mut F,
) -> std::result::Result<JsonValueShape, CodecError> {
    if bytes.len() as u64 > bounds.maximum_bytes { return Err(CodecError::Bound(LEXICAL_BYTE_ERROR)); }
    let mut scan = Scan::with_value_schema(schema, bounds);
    scan.push_inline(bytes, bounds, charge)?;
    scan.value_shape()
}

/// The original source pass with a real closed SDK Value field descriptor.
pub(crate) fn admit_json_value_shape<F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8], bounds: JsonBounds, schema: JsonValueSchema, charge: &mut F,
) -> Result<JsonValueShape> {
    admit_json_value_shape_inline(bytes, JsonBounds {
        maximum_depth: bounds.maximum_depth.min(256), ..bounds
    }, Some(schema), charge).map_err(|failure| failure.into_error(None))
}

/// Existing noncanonical record bytes (not identities).
pub fn json_bytes_bounded<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
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
            }).map_err(|error| match meter.forward(error) {
                CodecError::Bound(reason) => meter.bound(reason).into_io(),
                error => error.into_io(),
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
        first.scan.end_inline().map_err(|error| first.meter.root_error(error))?;
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
        second.scan.end_inline().map_err(|error| second.meter.root_error(error))?;
        if second.scan.bytes != size as u64 {
            return Err(second.meter.root_error(CodecError::Bound("JSON serialization changed between admission and encoding")));
        }
    }
    meter.retained = 0;
    Ok(output)
}

/// Decodes an admitted schema with the supplied Serde seed.
pub fn json_from_slice_bounded<'de, S, F>(
    seed: S, bytes: &'de [u8], bounds: JsonBounds, maximum_owned_bytes: u64,
    code: Option<crate::wire::ErrorCode>, value_schema: Option<JsonValueSchema>, charge: &mut F,
) -> Result<S::Value>
where S: serde::de::DeserializeSeed<'de>, F: FnMut(JsonCodecCharge) -> Result<()> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    let value_shape = admit_json_value_shape_inline(bytes, bounds, value_schema, charge)
        .map_err(|failure| failure.into_error(code))?;
    let scratch = (bytes.len() as u64).max(8).checked_mul(2)
        .ok_or_else(|| CodecError::Bound("JSON scratch bound overflow").into_error(code))?;
    let maximum_owned_bytes = maximum_owned_bytes.checked_add(scratch)
        .ok_or_else(|| CodecError::Bound("JSON owned bound overflow").into_error(code))?;
    charge(JsonCodecCharge::Admission { maximum_owned_bytes, value_shape })?;
    charge(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let value = seed.deserialize(&mut decoder).map_err(|error| decode_error(error, code))?;
    decoder.end().map_err(|error| decode_error(error, code))?;
    drop(decoder);
    Ok(value)
}

/// Converts an admitted borrowed or owned JSON value.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub fn json_from_value_bounded<T: serde::de::DeserializeOwned, F: FnMut(JsonCodecCharge) -> Result<()>>(
    source: std::borrow::Cow<'_, Value>, bounds: JsonBounds, maximum_owned_bytes: u64, code: crate::wire::ErrorCode,
    value_schema: JsonValueSchema, charge: &mut F,
) -> Result<T> {
    let mut value_shape = json_owned_value_shape_bounded(source.as_ref(), bounds, value_schema, charge)?;
    if matches!(source, std::borrow::Cow::Borrowed(_)) {
        value_shape.origin = ShapeOrigin::BorrowedConversion;
    }
    charge(JsonCodecCharge::Admission { maximum_owned_bytes, value_shape })?;
    match source {
        std::borrow::Cow::Borrowed(value) => T::deserialize(value),
        std::borrow::Cow::Owned(value) => T::deserialize(value),
    }.map_err(|source| decode_error(source, Some(code)))
}

fn decode_error(source: serde_json::Error, code: Option<crate::wire::ErrorCode>) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(code) = code { return Error::NativeDecode { code, source, custody: None }; }
    let _ = code;
    Error::Invalid(source.to_string())
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    const BOUNDS: JsonBounds = JsonBounds { maximum_bytes: 65_536, maximum_items: 4_096, maximum_depth: 32 };
    #[test]
    fn exact_byte_item_depth_and_escape_admission() -> Result<()> {
        let value = serde_json::json!({"z": ["\u{0}\"\\\n", u64::MAX], "a": i64::MIN});
        let expected = super::super::canonical_json_bytes(&value)?;
        let bounds = JsonBounds { maximum_bytes: expected.len() as u64, maximum_items: 7, maximum_depth: 2 };
        assert_eq!(canonical_json_bytes_bounded(&value, bounds, None, &mut |_| Ok(()))?, expected);
        assert!(canonical_json_bytes_bounded(&value, JsonBounds { maximum_bytes: bounds.maximum_bytes - 1, ..bounds }, None, &mut |_| Ok(())).is_err());
        assert!(canonical_json_bytes_bounded(&value, JsonBounds { maximum_items: 6, ..bounds }, None, &mut |_| Ok(())).is_err());
        assert!(canonical_json_bytes_bounded(&value, JsonBounds { maximum_depth: 1, ..bounds }, None, &mut |_| Ok(())).is_err());
        Ok(())
    }

    #[test]
    fn every_codec_cancellation_schedule_preserves_original_error() -> Result<()> {
        let value = serde_json::json!({"b": ["escape\n", 0.1], "a": {"later": 2, "earlier": 1}});
        let mut calls = 0usize;
        canonical_json_bytes_bounded(&value, BOUNDS, None, &mut |event| {
            if !matches!(event, JsonCodecCharge::Release { .. }) { calls += 1; }
            Ok(())
        })?;
        for stop in 0..calls {
            let mut seen = 0;
            let mut live = 0u64;
            let operation = crate::OperationId::from_bytes((stop as u128).to_le_bytes());
            let mut error = Some(Error::Indeterminate(operation));
            let result = canonical_json_bytes_bounded(&value, BOUNDS, None, &mut |event| {
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
    fn ordinary_record_bytes_and_raw_value_identity_remain_exact() -> Result<()> {
        #[derive(Serialize)]
        struct Record { z: f32, a: u64 }
        let record = Record { z: 0.1, a: u64::MAX };
        let expected = serde_json::to_vec(&record).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(json_bytes_bounded(&record, BOUNDS, None, &mut |_| Ok(()))?, expected);
        let raw = serde_json::value::RawValue::from_string(r#"{"z":[0.1,-0.0],"a":18446744073709551615,"z":[2]}"#.into())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let mut old = serde_json::to_value(&raw).map_err(|error| Error::Invalid(error.to_string()))?;
        old.sort_all_objects();
        let expected = serde_json::to_vec(&old).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(canonical_json_bytes_bounded(&raw, BOUNDS, None, &mut |_| Ok(()))?, expected);
        Ok(())
    }

}
