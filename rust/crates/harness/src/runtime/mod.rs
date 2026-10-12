//! Typed, version-pinned task admission over the live and durable primitives.

mod native_startup;
use native_startup::StartupIo;
use crate::contract::capability;
use crate::{
    Admission, BatchId, Capabilities, EffectId, Error, GroupId, InteractionId, OperationId,
    Outcome, Result, TaskId,
    context::ContextPipeline,
    conversation::{
        ContentPublisher, ContentResidencyVerifier, FileRef, Limits, MAX_PRIVATE_DIRECTORY_PAGE,
        PrivateDirectoryPage, VolumeOperation, VolumeRef, verified_content_bytes,
    },
    core::{ExtensionAdmission, Reducer, SchemaRegistry, Scope},
    durable_tool::ResumableToolRegistry,
    executor::ModelEventAdmission,
    extension::ExtensionRuntime,
    interaction::{
        Interaction, InteractionOutcome, InteractionResolution, InteractionResponse,
        InteractionTicket, ResolutionReceipt,
    },
    live::{TaskGroup, TaskHandle},
    model::{Model, ModelEvent, ModelMessage, ModelProvider, ModelRequest},
    registry::{ComponentIdentity, validate_component_label},
    resources::{ArtifactRef, GenerationRef, SandboxRef},
    scheduler::InboxItem,
    tool::{ToolDefinition, ToolInvocation, ToolRegistry},
    workflow::{
        DurableWorkflowHost, MachineCheckpoint, MachineIdentity, MachineRegistry, MachineStatus,
        MachineTransition, ResumableMachine, WorkflowAdmission, WorkflowJournal,
    },
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use futures::{StreamExt as _, stream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
/// Default number of direct children requested by the SDK facade.

pub const DEFAULT_CHILD_PAGE: usize = 256;
/// Default number of entries requested by the private-directory SDK facade.

pub const DEFAULT_PRIVATE_DIRECTORY_PAGE: usize = 256;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod native_context;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use native_context::NativeTaskContext;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod native_volume_callback;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_volume_callback::NativeVolumeCallbackGuard;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_volume_callback::NativeVolumePublicationGuard;

mod tasks;
pub use tasks::*;
mod registry;
pub use registry::*;
mod task_workflow;
pub use task_workflow::*;
mod task_identity;
pub use task_identity::*;
mod admission;
pub use admission::*;
mod providers;
pub use providers::*;
mod scope;
pub use scope::*;
mod joins;
pub use joins::*;
mod bindings;
pub use bindings::*;
mod tools;
pub use tools::*;
mod dependencies;
pub use dependencies::*;
mod children;
pub use children::*;
mod context_open;
pub use context_open::*;
mod host_binding;
mod fork_binding;
mod startup;
mod admission_execution;
mod reattachment;
pub use reattachment::*;
mod task_context;
pub use task_context::*;
mod content_read;
pub use content_read::*;
mod content_write;
mod model_context;
mod tool_dispatch;
mod interactions;
mod groups;
pub use groups::*;

#[cfg(test)]
mod tests;

