#![deny(unsafe_code)]
#![cfg_attr(
    target_arch = "wasm32",
    allow(
        clippy::arc_with_non_send_sync,
        reason = "public shared handles retain uniform ownership while browser providers stay on their event loop"
    )
)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::too_many_lines,
        clippy::cognitive_complexity,
        reason = "unit tests run ordered scenarios over fixtures they built"
    )
)]
//! Runtime, contracts, storage and execution behind `acyclic-harness`.
//!
//! Depend on `acyclic-harness`; it re-exports every public item of this crate
//! at the same path and adds the client, TUI, local and browser compositions.

#[cfg(any(
    test,
    feature = "test-support",
    all(
        not(target_arch = "wasm32"),
        any(
            feature = "filesystem-local",
            feature = "hooks",
            feature = "mcp-stdio",
            feature = "mcp-http",
            feature = "search-http"
        )
    )
))]
#[doc(hidden)]
pub mod native_environment;
#[doc(hidden)]
pub mod sync;
#[cfg(all(
    test,
    debug_assertions,
    feature = "filesystem",
    not(target_arch = "wasm32")
))]
mod accounting_trace {
    include!("memory_store/retention_scan_trace.rs");
    include!("memory_store/retention_scan_capture.rs");
}
pub mod agent_loop;
pub mod bundle;
#[doc(hidden)]
pub mod command_request;
pub mod context;
#[doc(hidden)]
pub mod contract;
pub mod conversation;
pub mod core;
pub mod distributed;
pub mod durable_host;
pub mod durable_tool;
pub mod effect_host;
pub mod effects;
pub mod executor;
pub mod extension;
#[cfg(feature = "filesystem")]
pub mod filesystem;
pub mod fork;
pub mod goal;
#[cfg(feature = "grpc")]
pub mod grpc;
#[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
pub mod hooks;
#[doc(hidden)]
pub mod identity;
pub mod interaction;
mod json_literal;
pub mod live;
#[cfg(feature = "machines")]
pub mod machines;
pub mod mcp;
#[cfg(any(
    test,
    feature = "filesystem",
    all(feature = "wasm", target_arch = "wasm32")
))]
#[doc(hidden)]
pub mod memory_store;
pub mod merge;
pub mod model;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native_future;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http")
))]
pub mod native_settings;
#[cfg(feature = "objects")]
pub mod objects;
mod obs;
pub mod projection;
pub mod registry;
pub mod resource_preparation;
pub mod resources;
pub mod runtime;
pub mod scheduler;
pub mod source_authorization;
pub mod store;
pub mod task_volume;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod tool;
pub mod tools;
pub mod turn;
pub mod wire_api;
pub mod wire_codec;
pub mod workspace;
pub use wire_codec::encode_error;
#[cfg(test)]
#[path = "../../harness/tests/support/stream/mod.rs"]
mod test_stream;
pub mod wire_validation;
pub mod wire_values;
pub mod workflow;

/// Optional original native work; `None` selects unbounded work.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub type NativeWork<'a> = Option<&'a acyclic_fs::NativeMaterializationWork>;
/// Optional original native work; `None` selects unbounded work.
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub type NativeWork<'a> = Option<&'a ()>;

/// Optional custody retained for an original native allocation.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub type NativeCustody = Option<acyclic_fs::NativeAllocation>;
/// Optional custody retained for an original native allocation.
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub type NativeCustody = Option<()>;

use build_metadata::generated;

/// Generated Protobuf envelopes shared by every transport: the harness contract and the protocol handshake it negotiates with.
pub mod wire {
    pub use super::generated::acyclic::harness::v2::*;
    pub use super::generated::acyclic::protocol::v1::*;
}

/// Canonical harness descriptor set used for transport compatibility.
pub const FILE_DESCRIPTOR_SET: &[u8] = build_metadata::FILE_DESCRIPTOR_SET;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use crate::executor::json::json_from_value_bounded;
pub use crate::executor::json::{
    JsonBounds, JsonCodecCharge, JsonValueSchema, JsonValueShape, canonical_json_bytes_bounded,
    json_bytes_bounded, json_from_slice_bounded, json_owned_value_conversion_additional_bytes,
    json_typed_diagnostic_owned_bytes, json_value_bounded, json_value_decode_owned_bytes,
    preparation_serde_diagnostic_label_bytes, sdk_json_diagnostic_bytes,
};
pub use bundle::{HarnessBuilder, HarnessBundle as Harness};
pub use contract::{
    Admission, AgentId, AuthorityLevel, AuthorityPolicy, BatchId, COMPONENT_LABEL_FORBIDDEN_EXACT,
    COMPONENT_LABEL_FORBIDDEN_SEPARATORS, COMPONENT_LABEL_MAX_BYTES, Capabilities,
    CommandIndeterminacy, CommandIndeterminacyReason, ConversationId, EffectAttemptId, EffectId,
    Error, GroupId, IdempotencyKey, IndeterminateCause, InteractionId, InteractionRejection,
    ModelTransportFailure, OperationId, Outcome, PolicyLayer, ProtocolIdentity, Result, SessionId,
    TaskId, TurnId, is_valid_component_label, resolve_policies, resolve_policy_layers,
};
pub use extension::{
    ExtensionIdentity, ExtensionLease, ExtensionRegistry, ExtensionRuntime, NativeExtension,
};
pub use json_literal::deserialize as deserialize_json_value;
pub use live::{TaskGroup, TaskHandle, completion_stream, first_success, join_all, ordered_reduce};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use native_future::NativeFutureBundle;
#[cfg(feature = "filesystem")]
pub use resource_preparation::ChildScratchCreationProof;

mod process_error;
pub use process_error::ProcessError;

/// Cargo-provided build metadata, isolated from ambient user environment discovery.
#[doc(hidden)]
pub mod build_metadata;
