//! JavaScript bindings for the shared agent runtime and authority.
#![allow(
    clippy::needless_pass_by_value,
    reason = "wasm-bindgen's exported ABI owns JavaScript values and byte buffers"
)]

use crate::{
    AgentId, BatchId, Capabilities, ConversationId, EffectId, GroupId, OperationId, SessionId,
    TaskId, TurnId,
    conversation::{
        BoundedContentFuture, ContentIo, ContentResidencyVerifier, ConversationMessage,
        FileDescriptor, FileRef, Limits, ReferencedAttachments, TaskOutcomeRecord, VolumeRef,
    },
    core::{
        AggregateKind, Authority, ExtensionAdmission, ExtensionConfiguration, ExtensionDependency,
        ExtensionRecord, ExtensionStateMigration, RuntimeAuthority, Scope,
    },
    executor::ModelEventAdmission,
    interaction::{ApprovalBinding, InteractionResolution, InteractionTicket},
    model::{ModelContent, ModelEvent, ModelMessage},
    resources::{ProviderRef, ResourceRef},
    tool::ToolDefinition,
};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use tsify::Tsify;
use wasm_bindgen::prelude::*;

mod cancel;
mod reader;
#[cfg(feature = "browser")]
pub use agents::{WasmAgentProviders, WasmAgentRegistry, WasmMachinesWorkspace, WasmObjects};
pub use reader::BrowserContentReader;
mod context;
use context::*;
mod contract;
mod declarations;
mod identity;
mod json;
use json::*;
mod model;
use model::*;
mod agent_authority;
mod tool;
use tool::*;
mod agent_client;
mod agent_command;
mod client_connection;
mod explorer;
mod wire_session;

#[cfg(feature = "browser")]
mod agents;
mod context_discovery;
mod mcp;

/// Host-managed signing authority for browser and guest agent admission.
#[wasm_bindgen]
#[derive(Clone)]
pub struct WasmAgentAuthority {
    issuer: RuntimeAuthority,
    caller: acyclic_auth::contract::CallerContext,
}
