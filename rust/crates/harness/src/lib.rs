#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )
)]
#![doc = include_str!("../README.md")]

pub mod agent_loop;
pub mod bundle;
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
pub mod merge;
pub mod model;
#[cfg(feature = "objects")]
pub mod objects;
pub mod projection;
pub mod registry;
pub mod resources;
pub mod runtime;
pub mod scheduler;
pub mod store;
pub mod tool;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
pub mod wire_api;
mod wire_codec;
pub use wire_codec::encode_error;
pub mod wire_values;
pub mod workflow;

/// Generated Protobuf envelopes shared by every transport.
#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::too_many_lines,
    clippy::large_enum_variant
)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/acyclic.harness.v2.rs"));
}

/// Canonical harness descriptor set used for transport compatibility.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/harness_descriptor.bin"));

pub use bundle::{HarnessBuilder, HarnessBundle as Harness};
pub use contract::{
    Admission, AgentId, AuthorityLevel, AuthorityPolicy, BatchId, Capabilities, ConversationId,
    EffectAttemptId, EffectId, Error, GroupId, IdempotencyKey, InteractionId, InteractionRejection,
    OperationId, Outcome, PolicyLayer, ProtocolIdentity, Result, SessionId, TaskId, TurnId,
    resolve_policies, resolve_policy_layers,
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
