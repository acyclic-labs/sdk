//! Independently replaceable tool definitions, executors, and projections.

use crate::{
    Error, InteractionId, OperationId, Result,
    core::{AuthorityVerifier, Scope},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

pub mod edit;
pub mod files;
pub mod patch;
pub mod schema;
pub mod text;
pub mod text_files;

mod definition;
pub use definition::{ToolDefinition, json_projection_schema};
mod invocation;
pub use invocation::{ToolInvocation, ToolResult};
mod recorded_invocation;
pub(crate) use recorded_invocation::{RecordedInvocation, validate_model_call_recorded};
mod executor;
pub use executor::ToolExecutor;
mod projection;
pub use projection::*;
mod approval;
pub use approval::ToolApprovalVerifier;
mod registry;
pub use registry::{Tool, ToolRegistry};
mod codec;
#[cfg(feature = "filesystem")]
pub(crate) use codec::TOOL_SERDE_DIAGNOSTIC_LABEL_BYTES;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use codec::clone_value;
pub(crate) use codec::{decode, value};

#[cfg(test)]
mod tests;
