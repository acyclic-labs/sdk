//! JavaScript bindings for the shared agent runtime and authority.
#![allow(
    clippy::needless_pass_by_value,
    reason = "wasm-bindgen's exported ABI owns JavaScript values and byte buffers"
)]

use crate::wire_codec::protocol_identity;
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
        ExtensionDependency, ExtensionRecord, ExtensionStateMigration, Scope,
    },
    executor::ModelEventAdmission,
    interaction::{ApprovalBinding, InteractionResolution, InteractionTicket},
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
mod agent_authority;
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

/// Host-managed signing authority for browser and guest agent admission.
#[wasm_bindgen]
pub struct WasmAgentAuthority {
    issuer: AuthorityRoot,
}
