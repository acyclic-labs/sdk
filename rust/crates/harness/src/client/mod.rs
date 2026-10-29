//! Client helpers and the shared Rust client surface.

#![doc = include_str!("../../docs/client/README.md")]

pub use acyclic_harness_core::preset::Preset;

mod command_result;
pub use command_result::{HeadlessOutcome, encode_command_result};
pub mod commands;
mod mentions;
pub use mentions::*;
mod diff;
pub use diff::*;
mod task_list;
#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub use acyclic_harness_client::{
    AgentClient, AgentClientError, AgentEvent, AgentSelection, StreamView, StreamViewPolicy,
    WireSession, WireSessionError,
};
pub use acyclic_harness_client::{
    CREDENTIAL_ENV, ConnectionConfigError, ConnectionOptions, DEFAULT_ENDPOINT, DemandError,
    DemandPolicy, ENDPOINT_ENV, ViewDemand, resolve_connection_options,
};
pub use task_list::{TaskItem, TaskList, TaskState, parse_task_list};
