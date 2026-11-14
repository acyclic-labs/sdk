//! Ordered function-based context assembly.

use crate::{
    Result,
    conversation::{ContentPublisher, ContentResidencyVerifier, FileRef, Limits},
    model::{ModelContent, ModelMessage, ModelRole},
    projection::SelectedModelContext,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};
mod accounting;
pub use accounting::*;
mod selection;
pub use selection::*;
mod discovery;
#[doc(hidden)]
pub use discovery::frontmatter_end;
pub use discovery::*;
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub use discovery::{DiscoveryWork, discover_skills_at};
mod compaction;
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub mod native_discovery;
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub mod native_skills;
#[cfg(not(target_arch = "wasm32"))]
mod skill_materializer;
pub use compaction::*;
pub(crate) use compaction::{clone_context_range, compact_projection, prefix_digest};
pub(crate) use compaction::{mandatory_message_count, mandatory_messages};
mod snapshot;
pub use snapshot::*;
mod epoch;
pub use epoch::{CONTEXT_CHANGED, ContextChange, ContextEpoch};
mod manifest;
pub use manifest::ContextManifest;
mod documents;

pub use documents::capture_working_documents;

mod checkpoint;
pub use checkpoint::*;
mod validation;
pub(crate) use validation::{
    plan_compaction_budget, restrict_context_limits, validate_compaction,
    validate_projected_context, validate_projected_request,
};
mod value;
pub use value::*;
mod environment;
mod text;
pub use environment::EnvironmentFacts;
mod pipeline;
pub use pipeline::*;
mod source;
pub use source::*;
mod allocations;
use allocations::append_context_messages;
use allocations::context_contract;
#[cfg(not(target_arch = "wasm32"))]
use allocations::reserve_context_vec_native;
use validation::{validate_context_refs, validate_source_messages};
use value::{BorrowedContext, ContextMessages};
#[cfg(test)]
mod tests;

mod diff;
pub use diff::{DiffCursor, DiffFilePage, DiffKind, DiffPath, DiffPublication};
