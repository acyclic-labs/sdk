//! The authority, content bindings and tool context of one agent turn.

use crate::core::contains_content_grant_with_work;
use crate::{
    Capabilities, Error, NativeCustody, OperationId, Result, TaskId,
    conversation::{
        ContentPublisher, ContentResidencyVerifier, FileRef, Limits, MAX_PRIVATE_DIRECTORY_PAGE,
        PrivateDirectoryPage, VolumeRef, verified_content_bytes,
    },
    core::{ExtensionAdmission, Reducer, SchemaRegistry},
    extension::ExtensionRuntime,
    interaction::{ApprovalSubject, Interaction, InteractionOutcome},
    registry::ComponentIdentity,
    resources::GenerationRef,
    tool::{ToolDefinition, ToolInvocation},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
/// Default number of entries requested by the private-directory SDK facade.
pub const DEFAULT_PRIVATE_DIRECTORY_PAGE: usize = 256;

mod scope;
pub use scope::*;
mod bindings;
pub use bindings::*;
mod tools;
pub use tools::*;
mod task_context;
pub use task_context::*;
mod content_read;
mod content_write;
pub(crate) use content_write::{PrivateSummaries, SummaryDestination};
mod model_context;
