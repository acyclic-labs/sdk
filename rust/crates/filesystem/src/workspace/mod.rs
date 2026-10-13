//! Workspace identities, immutable generations, and atomic mutations.

use crate::CancellationToken;
use crate::foundation::{FileId, GenerationId, OperationId, VolumeId};
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::kernel::binary_search_with_accounting;
use crate::kernel::{
    ExtentKind, FileKind, FileMetadata, FilePayload, LogicalName, MetadataField, NamespacePath,
};
use crate::model::{CheckoutMode, GenerationSelector};
use crate::path::PortablePath;
use crate::performance::WorkBudget;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, Checkout, ConflictKey, ConflictKind,
    ConflictSide, ConflictValue, FsError, GenerationDiff, LivePublicationObservation,
    MergeConflict, MergeDriverRegistry, MergePlan, MergePlanResolutionError, MergeResolution,
    MergeResolutionCache, UnpublishedMergeCandidate, Volume, resolve_merge_plan,
};
use acyclic_stream::ProviderTask;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use unicode_normalization::UnicodeNormalization;

const WORKSPACE_ID_DOMAIN: &[u8] = b"acyclic-fs-workspace-id-v1\0";
const MAXIMUM_WORKSPACE_NAME_BYTES: usize = 255;

mod checkout;
mod commit;
mod content;
mod diff;
mod errors;
mod extents;
mod fork_options;
mod generation;
mod handle;
mod identity;
mod join;
mod join_apply;
mod join_helpers;
#[cfg(not(target_arch = "wasm32"))]
mod join_native;
mod join_resolution;
mod lifecycle;
mod materialize;
mod metadata;
mod outcomes;
mod path_apply;
mod paths;
mod publication;
mod read_core;
mod reads;
mod restore;
mod reverse_paths;
mod reverse_traversal;
mod transaction;
mod transaction_create;
mod transaction_ranges;
mod transaction_write;
mod writes;

pub use commit::{TransactionCommit, TransactionRebase};
pub use diff::{ChangeSet, ChangedPath};
pub(crate) use errors::workspace_bounded_error;
pub use errors::{WorkspaceError, WorkspaceNameError};
pub use extents::{WorkspaceExtentKind, WorkspaceExtentPlan, WorkspaceExtentSpan};
pub use fork_options::ForkOptions;
pub use generation::Generation;
pub use handle::Workspace;
pub use identity::{IdempotencyKey, WorkspaceId, WorkspaceName};
pub use join::{JoinBuilder, JoinHistory, JoinPlan};
pub use join_apply::DrivenJoinError;
use join_helpers::{
    binding_conflict_view, conflict_file_id, kernel_conflict, logical_name_text,
    namespace_path_text, record_conflict_view, resolution_matches_conflict, side_resolution,
};
pub use metadata::{WorkspaceMetadata, WorkspaceStat};
pub use outcomes::{
    ApplyOptions, JoinApplication, JoinOutcome, WorkspaceDelete, WorkspacePathApply,
    WorkspacePathConflict, WorkspaceRebase, WorkspaceRestore,
};
use paths::{
    copy_workspace_label, merge_workspace_backend, path_conflict_kind, regular_file_bytes,
    restore_absolute_path, workspace_allocation, workspace_allocation_error,
};
pub(crate) use paths::{
    customer_path, customer_path_with_accounting, workspace_owned_strings_bytes,
    workspace_vec_bytes,
};
use read_core::{list, plan, read_link, read_range, read_selected, stat};
pub use transaction::Transaction;

#[cfg(test)]
#[path = "../tests/workspace/mod.rs"]
mod tests;

pub(crate) use reverse_traversal::reverse_paths_to_file_root_with_accounting;
use reverse_traversal::{
    NamespaceRecordSearch, NamespaceRecords, namespace_records_bytes,
    namespace_records_to_file_root_with_accounting, reverse_drop, reverse_grow,
    sorted_changed_file_ids,
};

async fn publication_ready(
    _: WorkBudget,
) -> crate::MeasuredResult<crate::OperationReceipt<()>, WorkspaceError> {
    Ok(crate::OperationReceipt {
        value: (),
        work: crate::WorkCounters::UNCHARGED,
    })
}

fn copy_work(bytes: u64) -> crate::WorkCounters {
    crate::WorkCounters {
        bytes_copied: bytes,
        ..crate::WorkCounters::UNCHARGED
    }
}
fn item_work(items: u64) -> crate::WorkCounters {
    crate::WorkCounters {
        items_examined: items,
        ..crate::WorkCounters::UNCHARGED
    }
}
