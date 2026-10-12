//! The canonical codec bounds logical data, not allocator metadata or RSS.

use super::{Error, Result, json_value_logical_owned_bytes};
use serde::ser::{self, Serialize};
use serde_json::Value;

mod meter;
use meter::*;
mod encoder;
use encoder::*;
mod quota;
use quota::value_storage_bytes;
pub use quota::{
    json_owned_value_conversion_additional_bytes, json_value_decode_owned_bytes,
    sdk_json_diagnostic_bytes,
};
pub(crate) use quota::{
    json_owned_value_shape_bounded, json_typed_value_decode_owned_bytes,
    json_value_virtual_owned_bytes,
};
mod lexical;
use lexical::{Scan, admit_json_value_shape_inline};
pub(crate) use lexical::{admit_json, admit_json_value_shape, lexical_json_diagnostic_bytes};
mod canonical;
pub use canonical::{canonical_json_bytes_bounded, json_value_bounded};
mod records;
pub(super) use records::count_json_bytes_trusted;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use records::json_from_value_bounded;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use records::validate_json_byte_bound_native_trusted;
pub use records::{json_bytes_bounded, json_from_slice_bounded};
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
enum ShapeOrigin {
    Slice,
    OwnedConversion,
    BorrowedConversion,
}
impl JsonValueShape {
    /// Source bytes consumed once; for resident inspection, the owned UTF-8 payload/node lower bound instead.
    pub const fn input_bytes(self) -> u64 {
        self.input_bytes
    }
    /// Original lexical or resident values/map keys, including source duplicates.
    pub const fn lexical_items(self) -> u64 {
        self.lexical_items
    }
    /// Additional lexical nodes in inspected decoded RawValue sources.
    pub const fn virtual_items(self) -> u64 {
        self.virtual_items
    }
    /// Whether a possible RawValue path requires conservative byte-based quota.
    pub const fn unresolved_raw(self) -> bool {
        self.unresolved_raw
    }
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

#[cfg(test)]
mod tests;
