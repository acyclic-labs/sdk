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
#[cfg(feature = "filesystem-local")]
#[doc(hidden)]
pub use discovery::{DiscoveryWork, discover_skills_at};
mod compaction;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
#[doc(hidden)]
pub mod native_discovery;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
#[doc(hidden)]
pub mod native_skills;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
mod skill_materializer;
pub(crate) use compaction::mandatory_messages;
pub use compaction::*;
pub(crate) use compaction::{clone_context, compact_projection};
mod snapshot;
pub use snapshot::*;
mod manifest;
pub use manifest::ContextManifest;
#[doc(hidden)]
pub use manifest::DEFAULT_CONVENTIONS_JSON;

mod checkpoint;
pub use checkpoint::*;
mod validation;
pub(crate) use validation::{
    plan_compaction_budget, restrict_context_limits, validate_compaction,
    validate_projected_context, validate_projected_messages,
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
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use allocations::reserve_context_vec_native;
pub(crate) use validation::verify_file;
use validation::{validate_context_refs, validate_source_messages};
use value::{BorrowedContext, ContextMessages};
#[cfg(test)]
mod tests;

mod diff;
pub use diff::{DiffCursor, DiffFilePage, DiffKind, DiffPath, DiffPublication};
