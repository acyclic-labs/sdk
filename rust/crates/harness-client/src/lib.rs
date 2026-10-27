//! Bounded view demand and provider-owned pages shared by client adapters, and
//! the lazy stream [`reader`] every client renders from.

mod connection;
pub use connection::{
    CREDENTIAL_ENV, ConnectionConfigError, ConnectionOptions, DEFAULT_ENDPOINT, ENDPOINT_ENV,
    EndpointTransport, resolve_connection_options,
};

/// The same preset and defaults applied by local and hosted harnesses.
#[cfg(any(feature = "agents", feature = "wasm"))]
pub use acyclic_harness_core::preset::Preset;

#[cfg(any(feature = "agents", feature = "wasm"))]
mod control;
#[cfg(any(feature = "agents", feature = "wasm"))]
pub use control::{
    AgentBinding, AgentCall, AgentControls, AgentExchangeError, PreparedAgentInvocation,
};

#[cfg(any(feature = "agents", feature = "wasm"))]
mod command;
#[cfg(any(feature = "agents", feature = "wasm"))]
pub use command::{
    AgentCommand, AgentPageRequest, CommandError, MAX_COMMAND_BYTES as MAX_AGENT_COMMAND_BYTES,
    MAX_READ_BYTES, PreparedCommand, ReadTarget,
};
#[cfg(any(feature = "agents", feature = "wasm"))]
mod reply;
#[cfg(any(feature = "agents", feature = "wasm"))]
pub use reply::{
    AgentControlFailure, AgentReply, ChildAgent, MAX_AGENT_REPLY_BYTES, PendingApproval,
    decode_agent_reply, encode_agent_reply,
};

#[cfg(any(feature = "agents", feature = "wasm"))]
mod agents;
#[cfg(any(feature = "agents", feature = "wasm"))]
pub use acyclic_harness_core::agent::{
    AgentId, AgentPath, AgentRecord, ConversationEntry, Delivered, Error as AgentError, IntentId,
    Phase, Reply, StatusRecord,
};
#[cfg(any(feature = "agents", feature = "wasm"))]
pub use acyclic_harness_core::{
    interaction::{ApprovalBinding, Interaction},
    model::ModelContent,
};
#[cfg(any(feature = "agents", feature = "wasm"))]
pub use agents::{AgentClient, AgentClientError, AgentEvent, AgentSelection};
pub mod reader;
pub use reader::{Reader, ReaderPolicy};

/// Agent tree and history renders over the reader.
#[cfg(any(feature = "agents", feature = "wasm"))]
pub mod explorer;
#[cfg(any(feature = "agents", feature = "wasm"))]
pub use explorer::Explorer;

mod demand;
pub use demand::{DemandError, DemandPolicy, ViewDemand};

// Agent controls include Actors' wasm runtime. Session-only clients retain
// their native transport boundary without implicitly selecting browser I/O.
#[cfg(any(
    feature = "agents",
    feature = "wasm",
    all(feature = "session", not(target_arch = "wasm32"))
))]
mod session;
#[cfg(any(
    feature = "agents",
    feature = "wasm",
    all(feature = "session", not(target_arch = "wasm32"))
))]
pub use session::{WireSession, WireSessionError};

#[cfg(any(
    feature = "agents",
    feature = "wasm",
    all(feature = "session", not(target_arch = "wasm32"))
))]
mod stream_view;
#[cfg(any(
    feature = "agents",
    feature = "wasm",
    all(feature = "session", not(target_arch = "wasm32"))
))]
pub use stream_view::{StreamView, StreamViewPolicy};

#[cfg(test)]
mod tests;
