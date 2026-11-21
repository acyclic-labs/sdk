//! Deterministic filesystem workspace histories.
#![cfg(not(target_arch = "wasm32"))]

use crate::fixtures;
#[path = "support/simulation_image.rs"]
mod simulation_image;
use crate::transaction;
#[path = "workspace_simulation/workspace/mod.rs"]
mod workspace;
