use super::*;

#[cfg(any(not(unit_test_split), unit_test_workspace, unit_test_workspace_merge))]
use crate::model::{Lifecycle, VolumeConfig};

use crate::{CancellationToken, Fs, WorkBudget};

use acyclic_stream::OwnedBytes;

use std::error::Error;

#[cfg(not(unit_test_workspace_accounting))]
use std::sync::Arc;

#[cfg(not(unit_test_workspace_accounting))]
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

#[cfg(not(unit_test_workspace_accounting))]
#[path = "../../../tests/support/workspace.rs"]
mod fixtures;
#[path = "../../../tests/support/transaction.rs"]
mod transaction_fixture;
#[cfg(not(unit_test_workspace_accounting))]
use fixtures::fork_workspace;
#[cfg(any(not(unit_test_split), unit_test_workspace, unit_test_workspace_merge))]
use transaction_fixture::allow;
use transaction_fixture::commit;

#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod access_policy;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod append_segments;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod change_sets;
#[cfg(any(not(unit_test_split), unit_test_workspace_merge))]
mod composition;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod content_equality;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod discovery;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod fork_selection;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod generation;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod identity;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod joins;
#[cfg(any(not(unit_test_split), unit_test_workspace_merge))]
mod merge_drivers;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod populated_creation;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod projection_policy;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod proof_cache;
#[cfg(any(not(unit_test_split), unit_test_workspace_accounting))]
mod reads;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod rebase;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod restore;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
mod transactions;

#[cfg(any(not(unit_test_split), unit_test_workspace_recovery))]
mod publication;

#[cfg(all(
    not(target_arch = "wasm32"),
    any(not(unit_test_split), unit_test_workspace_recovery)
))]
#[path = "../../../tests/support/simulation_image.rs"]
mod simulation_image;

#[cfg(any(not(unit_test_split), unit_test_workspace))]
#[path = "../../../tests/support/selected_budget.rs"]
mod selected_budget;
#[cfg(any(not(unit_test_split), unit_test_workspace))]
use selected_budget::selected_transaction_budget;
