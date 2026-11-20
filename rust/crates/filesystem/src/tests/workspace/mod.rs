use super::*;

use crate::model::{Lifecycle, VolumeConfig};

use crate::{CancellationToken, Fs, WorkBudget};

use acyclic_stream::OwnedBytes;

use std::error::Error;

use std::sync::Arc;

macro_rules! value {
    ($result:expr) => {
        $result
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    };
}

use crate as acyclic_fs;

#[path = "../../../tests/support/workspace.rs"]
mod fixtures;
use crate::transaction_fixture;
use fixtures::fork_workspace;
use transaction_fixture::allow;
use transaction_fixture::commit;

mod access_policy;
mod append_segments;
mod change_sets;
mod composition;
mod content_equality;
mod discovery;
mod fork_selection;
mod generation;
mod identity;
mod joins;
mod merge_drivers;
mod populated_creation;
mod projection_policy;
mod proof_cache;
mod reads;
mod rebase;
mod restore;
mod transactions;

mod publication;

#[cfg(not(target_arch = "wasm32"))]
#[path = "../../../tests/support/simulation_image.rs"]
mod simulation_image;

#[path = "../../../tests/support/selected_budget.rs"]
mod selected_budget;
use selected_budget::selected_transaction_budget;
