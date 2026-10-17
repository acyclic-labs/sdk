//! Thin JavaScript host for the exact native reducer.
#![allow(
    clippy::needless_pass_by_value,
    reason = "wasm-bindgen's exported ABI owns JavaScript values and byte buffers"
)]

use crate::runtime::dependencies::{dependency_target, validate_task_dependencies};
use crate::wire_codec::{
    decode_command, decode_event_payload as decode_payload_wire, encode_apply_result,
    protocol_identity,
};
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
        AggregateKind, ApplyResult, Authority, AuthorityIssuer, Command, ExtensionAdmission,
        ExtensionConfiguration, ExtensionDependency, ExtensionForkPolicy, ExtensionRecord,
        ExtensionStateMigration, Reducer, SchemaRegistry, Scope, Snapshot,
    },
    executor::ModelEventAdmission,
    fork::{ForkReport, ForkRequest, ForkSeed, ReferenceGrant, ResourceRevision},
    interaction::{ApprovalBinding, InteractionResolution, InteractionTicket, ResolutionReceipt},
    merge::ProjectMergeReceipt,
    model::{ModelContent, ModelEvent, ModelMessage},
    projection::{SelectedModelContext, select_model_context, validate_model_context_selection},
    resources::{ProviderRef, ResourceRef},
    runtime::{
        BatchGroupPolicy, DurableBatchRequest, TaskAdmissionRecord, TaskChild, TaskChildrenPage,
        TaskRunLimits, batch_member_operation_id, task_admission_identities,
        task_definition_digest, validate_children_page, validate_children_request,
    },
    tool::ToolDefinition,
    turn::prepare_turn,
};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;
use tsify::Tsify;
use wasm_bindgen::prelude::*;

mod admission;
mod bytes;
use admission::*;
mod content;
use content::*;
mod reader;
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
use reducer::schema_registry;
mod tool;
use tool::*;
#[cfg(target_arch = "wasm32")]
mod file_client;
mod file_worker;
#[cfg(target_arch = "wasm32")]
mod query;
#[cfg(target_arch = "wasm32")]
pub(crate) mod swarm;
mod task_views;
mod wire;

#[cfg(target_arch = "wasm32")]
mod context_discovery;
mod history;
mod mcp;
#[cfg(feature = "browser")]
mod task_runtime;

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WasmTaskChild {
    slot: String,
    task_id: TaskId,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WasmTaskChildrenPage {
    revision: u64,
    entries: Vec<WasmTaskChild>,
    next_after: Option<String>,
}

/// Opaque synchronous reducer hosted in WebAssembly.
#[wasm_bindgen]
pub struct WasmReducer {
    reducer: Reducer,
    issuer: AuthorityIssuer,
}

/// Bounded Rust-owned content state for the WASM `MemoryConversation` adapter.
#[wasm_bindgen]
pub struct WasmContentStore {
    store: crate::memory_store::MemoryStore,
}
