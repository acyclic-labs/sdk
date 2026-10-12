//! Thin JavaScript host for the exact native reducer.
#![allow(
    clippy::needless_pass_by_value,
    reason = "wasm-bindgen's exported ABI owns JavaScript values and byte buffers"
)]

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
        TaskDependencyEnvironment, TaskRunLimits, batch_member_operation_id,
        task_admission_identities, task_definition_digest, validate_children_page,
        validate_children_request, validate_task_requirements,
    },
    tool::ToolDefinition,
    turn::prepare_turn,
};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;
use tsify::{Ts, Tsify};
use wasm_bindgen::prelude::*;

mod admission;
use admission::*;
mod content;
use content::*;
mod context;
use context::*;
mod contract;
use contract::*;
mod declarations;
use declarations::*;
mod identity;
use identity::*;
mod json;
use json::*;
mod model;
use model::*;
mod reducer;
use reducer::*;
mod tool;
use tool::*;
mod wire;
use wire::*;

#[cfg(target_arch = "wasm32")]
#[path = "../wasm/context_discovery.rs"]
mod context_discovery;
#[path = "../wasm/history.rs"]
mod history;
#[path = "../wasm/mcp.rs"]
mod mcp;

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
/// The native filesystem provider uses the same crate-level core while
/// retaining its signed provider-generation proof around delegated reads.
#[wasm_bindgen]
pub struct WasmContentStore {
    store: crate::memory_store::MemoryStore,
}

#[cfg(feature = "browser")]
#[path = "../wasm/task_runtime/mod.rs"]
mod task_runtime;
