//! Native workspace materialization and path restoration contracts.
#![cfg(not(target_arch = "wasm32"))]

use crate::selected_budget;
use crate::transaction;
#[path = "workspace_materialization/workspace/mod.rs"]
mod workspace;
