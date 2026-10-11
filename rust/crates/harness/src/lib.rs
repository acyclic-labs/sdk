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
#[cfg(feature = "grpc")]
pub mod grpc;
mod handles;
pub mod interaction;
pub mod live;
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
#[cfg(feature = "objects")]
pub mod objects;
mod obs;
mod platform_time;
pub mod projection;
pub mod registry;
pub mod resources;
pub mod runtime;
pub mod scheduler;
pub mod store;
pub mod tool;
pub mod turn;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use wasm::{
    decode_execution_record_json, decode_turn_input_json, decode_turn_output_json,
    encode_canonical_json,
};
pub mod wire_api;
mod wire_codec;
pub use wire_codec::encode_error;
#[cfg(test)]
#[path = "../tests/support/stream.rs"]
mod test_stream;
pub mod wire_validation;
pub mod wire_values;
pub mod workflow;

/// Generated Protobuf packages, nested as their package names are, so the
/// harness messages resolve the shared protocol handshake they import.
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

/// Generated Protobuf envelopes shared by every transport: the harness
/// contract and the protocol handshake it negotiates with.
pub mod wire {
    pub use super::generated::acyclic::harness::v2::*;
    pub use super::generated::acyclic::protocol::v1::*;
}

/// Canonical harness descriptor set used for transport compatibility.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/harness_descriptor.bin"));

pub use bundle::{HarnessBuilder, HarnessBundle as Harness};
pub use contract::{
    Admission, AgentId, AuthorityLevel, AuthorityPolicy, BatchId, COMPONENT_LABEL_FORBIDDEN_EXACT,
    COMPONENT_LABEL_FORBIDDEN_SEPARATORS, COMPONENT_LABEL_MAX_BYTES, Capabilities, ConversationId,
    EffectAttemptId, EffectId, Error, GroupId, IdempotencyKey, InteractionId, InteractionRejection,
    OperationId, Outcome, PolicyLayer, ProtocolIdentity, Result, SessionId, TaskId, TurnId,
    is_valid_component_label, resolve_policies, resolve_policy_layers,
};
pub use extension::{
    ExtensionIdentity, ExtensionLease, ExtensionLeases, ExtensionLinker, ExtensionRegistry,
    ExtensionRuntime, NativeExtension, NativeExtensionBundle,
};
pub use handles::{Agent, Conversation, Session, Task, Turn};
pub use live::{
    TaskGroup, TaskHandle, completion_stream, first_success, join_all, ordered_reduce, quorum,
    race, recursive_sum,
};
