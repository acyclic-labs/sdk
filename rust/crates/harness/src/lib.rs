#![deny(unsafe_code)]
#![doc = include_str!("../README.md")]

#[cfg(feature = "host")]
pub mod bundle;
#[cfg(feature = "host")]
pub mod context;
mod contract;
pub mod core;
#[cfg(feature = "host")]
pub mod distributed;
#[cfg(feature = "host")]
pub mod effects;
#[cfg(feature = "host")]
pub mod executor;
pub mod fork;
#[cfg(feature = "host")]
mod handles;
pub mod interaction;
#[cfg(feature = "host")]
pub mod live;
#[cfg(feature = "host")]
pub mod model;
#[cfg(feature = "host")]
pub mod registry;
pub mod resources;
pub mod scheduler;
pub mod speculation;
#[cfg(feature = "host")]
pub mod store;
#[cfg(feature = "host")]
pub mod tool;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
#[cfg(feature = "host")]
pub mod wire_api;
#[cfg(any(feature = "host", feature = "wasm"))]
mod wire_codec;
#[cfg(any(feature = "host", feature = "wasm"))]
pub use wire_codec::encode_error;
pub mod workflow;

/// Generated Protobuf envelopes shared by every transport.
#[allow(missing_docs, clippy::pedantic, clippy::too_many_lines)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/acyclic.harness.v1.rs"));
}

/// Canonical harness descriptor set used for transport compatibility.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/harness_descriptor.bin"));

pub use contract::{
    Admission, AgentId, AuthorityLevel, AuthorityPolicy, Capabilities, ConversationId,
    EffectAttemptId, EffectId, Error, IdempotencyKey, InteractionId, OperationId, Outcome,
    PolicyLayer, ProtocolIdentity, Result, SessionId, TaskId, TurnId, resolve_policies,
    resolve_policy_layers,
};
#[cfg(feature = "host")]
pub use handles::{Agent, Conversation, Session, Task, Turn};
#[cfg(feature = "host")]
pub use live::{
    TaskGroup, TaskHandle, completion_stream, first_success, join_all, quorum, recursive_sum,
};
