//! Bounded view demand and provider-owned pages shared by client adapters, and
//! the lazy stream [`reader`] every client renders from.

#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

mod connection;
pub use connection::{
    CREDENTIAL_ENV, ConnectionConfigError, ConnectionOptions, DEFAULT_ENDPOINT, ENDPOINT_ENV,
    EndpointTransport, resolve_connection_options,
};

/// The same preset and defaults applied by local and hosted harnesses.
pub use acyclic_harness_core::preset::Preset;

mod control;
pub use control::{
    AgentBinding, AgentCall, AgentControls, AgentExchangeError, PreparedAgentInvocation,
};

mod command;
pub use command::{
    AgentCommand, AgentPageRequest, CommandError, MAX_COMMAND_BYTES as MAX_AGENT_COMMAND_BYTES,
    MAX_READ_BYTES, PreparedCommand, ReadTarget,
};
mod reply;
pub use reply::{
    AgentControlFailure, AgentReply, ChildAgent, MAX_AGENT_REPLY_BYTES, PendingApproval,
    decode_agent_reply, encode_agent_reply, encoded_agent_reply_size,
};

mod agents;
pub use acyclic_harness_core::agent::{
    AgentId, AgentPath, AgentRecord, ConversationEntry, Delivered, Error as AgentError, IntentId,
    Phase, Reply, StatusRecord,
};
pub use acyclic_harness_core::{
    interaction::{ApprovalBinding, ApprovalVerdict, Interaction},
    model::ModelContent,
};
pub use agents::{AgentClient, AgentClientError, AgentDelivery, AgentEvent, AgentSelection};
pub mod acp;
mod event_projection;
pub use event_projection::{
    ApprovalPayloadEvent, EventCursor, EventDrain, EventProjection, ExecutionPayloadEvent,
    ExecutionPosition, ExecutionQuery, ExecutionStreamEvent, StreamEvent,
};
pub mod reader;
pub use reader::{Reader, ReaderPolicy};

/// Agent tree and history renders over the reader.
pub mod explorer;
/// Lazy private-volume UI projection and ordinary inbox replies.
pub mod ui;
pub use explorer::Explorer;

mod demand;
pub use demand::{DemandError, DemandPolicy, ViewDemand};

// Agent controls include Actors' wasm runtime. Session-only clients retain
// their native transport boundary without implicitly selecting browser I/O.
mod session;
pub use session::{WireSession, WireSessionError};

mod stream_view;
pub use stream_view::{StreamView, StreamViewPolicy};

#[cfg(test)]
mod tests;
