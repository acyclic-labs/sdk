//! Bounded host-state capture into one atomic sparse checkout transaction.

use crate::demand::{DemandError, native_demand_error};
use crate::kernel::{
    FileKind, FileMetadata, FileRecord, LogicalName, MetadataField, NameEncoding, NamespacePath,
};
use crate::model::FilesystemProfile;
use crate::native_host::NativeHostWork as _;
use crate::native_host::{HostDataRange, HostRoot, HostStat, allocated_data_ranges};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, CancellationToken, Checkout,
    NativeRootIdentity, OperationFailure, OperationReceipt, StagedContent, WatchBatch, WatchChange,
    WatchInvalidationReason, WorkBudget, WorkCounters, WorkError,
};
use std::path::{Path, PathBuf};
use thiserror::Error;

mod accounting;
#[cfg(test)]
mod capture_policy_tests;
mod entry;
mod model;
#[cfg(test)]
mod native_view_baseline_tests;
use accounting::*;
mod sparse;
use sparse::*;
mod paths;
use paths::*;
mod names;
use names::*;
mod host_walk;
use host_walk::*;
#[cfg(test)]
mod host_observation_tests;
mod reconcile;
use reconcile::*;
mod checkout_walk;
use checkout_walk::*;
mod watch_replay;
use watch_replay::*;
#[cfg(test)]
mod shared_watch_capture_tests;
mod watch_paths;
use watch_paths::*;
mod transaction;
use transaction::*;
mod stage;
use stage::*;
mod metadata;
use metadata::*;
#[cfg(test)]
mod host_file_race_tests;
#[cfg(test)]
mod native_range_tests;
#[cfg(test)]
mod root_capture_regression;
#[cfg(test)]
mod unix_name_tests;
#[cfg(test)]
mod windows_name_tests;
#[cfg(test)]
mod windows_sparse_tests;

#[cfg(feature = "native-mount")]
pub(crate) use entry::capture_paths_batched;
pub(crate) use entry::capture_subtrees_with_native_work;
pub use entry::{
    capture_baseline, capture_paths, capture_root_identity, capture_subtree, capture_watch_batch,
};
pub(crate) use metadata::system_time;
#[cfg(feature = "native-mount")]
pub(crate) use model::MAX_NATIVE_EXACT_CAPTURE_PATHS;
pub(crate) use model::NativeViewBaseline;
pub use model::{CaptureError, CaptureOptions, CapturePolicy, CaptureReceipt, WatchCaptureReceipt};
#[cfg(windows)]
pub(crate) use names::namespace_component_wide;
pub use names::{host_path_to_namespace, namespace_to_host_path};
pub(crate) use names::{namespace_host_units_with_work, namespace_to_host_path_with_work};
