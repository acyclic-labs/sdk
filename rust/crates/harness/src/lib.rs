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
#![doc = include_str!("../README.md")]

mod sync;
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
pub mod client;
pub mod context;
mod contract;
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
pub mod interaction;
mod json_literal;
pub mod live;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub mod local;
#[cfg(feature = "machines")]
pub mod machines;
pub mod mcp;
#[cfg(any(
    test,
    feature = "filesystem",
    all(feature = "wasm", target_arch = "wasm32")
))]
pub(crate) mod memory_store;
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
pub mod search;
pub mod source_authorization;
pub mod store;
pub mod task_volume;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod tool;
pub mod tools;
#[cfg(feature = "tui")]
pub mod tui;
#[cfg(feature = "tui")]
pub mod tui_config;
#[cfg(feature = "tui")]
pub mod tui_files;
pub mod turn;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use wasm::BrowserContentReader;
pub mod wire_api;
pub mod wire_codec;
pub mod workspace;
pub use wire_codec::encode_error;
#[cfg(test)]
#[path = "../tests/support/stream/mod.rs"]
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

/// Generated Protobuf packages, nested as their package names are, so the harness messages resolve the shared protocol handshake they import.
#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::large_enum_variant,
    reason = "generated prost bindings"
)]
mod generated {
    pub mod acyclic {
        pub mod harness {
            pub mod v2 {
                include!(concat!(env!("OUT_DIR"), "/acyclic.harness.v2.rs"));
            }
        }
        pub mod protocol {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/acyclic.protocol.v1.rs"));
            }
        }
    }
}

/// Generated Protobuf envelopes shared by every transport: the harness contract and the protocol handshake it negotiates with.
pub mod wire {
    pub use super::generated::acyclic::harness::v2::*;
    pub use super::generated::acyclic::protocol::v1::*;
}

/// Canonical harness descriptor set used for transport compatibility.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/harness_descriptor.bin"));

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
    COMPONENT_LABEL_FORBIDDEN_SEPARATORS, COMPONENT_LABEL_MAX_BYTES, Capabilities, ConversationId,
    EffectAttemptId, EffectId, Error, GroupId, IdempotencyKey, InteractionId, InteractionRejection,
    OperationId, Outcome, PolicyLayer, ProtocolIdentity, Result, SessionId, TaskId, TurnId,
    is_valid_component_label, resolve_policies, resolve_policy_layers,
};
pub use extension::{
    ExtensionIdentity, ExtensionLease, ExtensionRegistry, ExtensionRuntime, NativeExtension,
};
pub use json_literal::deserialize as deserialize_json_value;
pub use live::{TaskGroup, TaskHandle, completion_stream, first_success, join_all, ordered_reduce};
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub use local::{LocalHarness, LocalHarnessConfig};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use native_future::NativeFutureBundle;
#[cfg(feature = "filesystem")]
pub use resource_preparation::ChildScratchCreationProof;

mod process_error;
pub use process_error::ProcessError;
