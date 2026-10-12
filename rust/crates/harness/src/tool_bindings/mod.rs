//! Independently replaceable tool definitions, executors, and projections.

use crate::{
    Error, InteractionId, OperationId, Result,
    core::{AuthorityVerifier, Scope},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

#[path = "../tool/edit.rs"]
pub mod edit;
#[path = "../tool/files.rs"]
pub mod files;
#[path = "../tool/patch.rs"]
pub mod patch;
#[path = "../tool/schema.rs"]
pub mod schema;
#[path = "../tool/text.rs"]
pub mod text;
#[path = "../tool/text_files.rs"]
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
use codec::*;
pub(crate) use crate::contract::validate_json_schema_value as validate_value;

#[cfg(test)]
mod tests;
