//! Thin JavaScript host for the native authority and content contracts.
#![allow(
    clippy::needless_pass_by_value,
    reason = "wasm-bindgen's exported ABI owns JavaScript values and byte buffers"
)]

use crate::wire_codec::{decode_event_payload as decode_payload_wire, protocol_identity};
use crate::{
    AgentId, BatchId, Capabilities, ConversationId, EffectId, GroupId, OperationId, PolicyLayer,
    SessionId, TaskId, TurnId,
    conversation::{
        Attachment, BoundedContentFuture, ContentGrant, ContentIo, ContentResidencyVerifier,
        ConversationMessage, ConversationState, FileDescriptor, FileRef, Limits,
        ModelContextSelection, ReferencedAttachments, TaskOutcomeRecord, VolumeOperation,
        VolumeRef, decode_attachment_manifest, encode_attachment_manifest,
    },
    core::{
        AggregateKind, Authority, AuthorityRoot, ExtensionAdmission, ExtensionConfiguration,
        ExtensionDependency, ExtensionForkPolicy, ExtensionRecord, ExtensionStateMigration,
        SchemaRegistry, Scope,
    },
    executor::ModelEventAdmission,
    interaction::{ApprovalBinding, InteractionResolution, InteractionTicket, ResolutionReceipt},
    model::{ModelContent, ModelEvent, ModelMessage},
    projection::{SelectedModelContext, select_model_context, validate_model_context_selection},
    resources::{ProviderRef, ResourceRef},
    tool::ToolDefinition,
    turn::prepare_turn,
};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tsify::Tsify;
use wasm_bindgen::prelude::*;

mod bytes;
mod content;
use content::*;
mod reader;
#[cfg(feature = "browser")]
pub use agents::WasmAgentProviders;
pub use reader::BrowserContentReader;
mod context;
use context::*;
mod contract;
mod declarations;
mod identity;
use identity::*;
mod json;
use json::*;
mod model;
use model::*;
mod reducer;
mod tool;
use tool::*;
#[cfg(target_arch = "wasm32")]
mod agent_client;
#[cfg(target_arch = "wasm32")]
mod agent_command;
mod client_connection;
mod client_preset;
#[cfg(target_arch = "wasm32")]
mod explorer;
mod wire;
#[cfg(target_arch = "wasm32")]
mod wire_session;

#[cfg(feature = "browser")]
mod agents;
#[cfg(target_arch = "wasm32")]
mod context_discovery;
mod mcp;

/// Host-managed authority root hosted in WebAssembly: it issues and verifies
/// the scopes of browser agents and their content grants.
#[wasm_bindgen]
pub struct WasmReducer {
    issuer: AuthorityRoot,
}

/// Bounded Rust-owned content state for the WASM `MemoryConversation` adapter.
#[wasm_bindgen]
pub struct WasmContentStore {
    store: crate::memory_store::MemoryStore,
}
