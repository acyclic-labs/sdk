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
mod executor;
pub use executor::ToolExecutor;
mod projection;
pub use projection::*;
mod approval;
pub use approval::ToolApprovalVerifier;
mod registry;
pub use registry::{Tool, ToolRegistry};
mod codec;
pub(crate) use crate::contract::validate_json_schema_value as validate_value;
pub(crate) use codec::decode;
#[cfg(feature = "filesystem")]
pub(crate) use codec::tool_serde_diagnostic_label_bytes;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) use codec::{clone_value, value};

#[cfg(test)]
mod tests;
