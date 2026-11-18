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

#[macro_use]
#[path = "../../../tests/support/unbounded.rs"]
mod unbounded;

use crate as acyclic_fs;

#[path = "../../../tests/support/workspace.rs"]
mod fixtures;
use fixtures::{allow, commit, fork_workspace};

mod access_policy;
mod append_segments;
mod change_sets;
mod content_equality;
mod projection_policy;

mod discovery;

mod fork_selection;

mod generation;

mod identity;

mod joins;

mod merge_drivers;

mod composition;

mod populated_creation;

mod proof_cache;

mod publication;

mod reads;

mod rebase;

mod restore;

mod transactions;

#[cfg(not(target_arch = "wasm32"))]
#[path = "../../../tests/support/simulation_image.rs"]
mod simulation_image;

#[path = "../../../tests/support/selected_budget.rs"]
mod selected_budget;
use selected_budget::selected_transaction_budget;
