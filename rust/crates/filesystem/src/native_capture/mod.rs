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
    NativeMaterializationWork, NativeRootIdentity, OperationFailure, OperationReceipt,
    StagedContent, WatchBatch, WatchChange, WatchInvalidationReason, WorkBudget, WorkCounters,
    WorkError,
};
use std::path::{Path, PathBuf};
use thiserror::Error;

mod accounting;
#[cfg(test)]
mod capture_policy_tests;
mod entry;
mod model;
#[cfg(all(test, unix))]
mod native_view_baseline_tests;
#[cfg(all(test, feature = "memory"))]
mod reserve_tests;
use accounting::*;
#[cfg(windows)]
mod sparse;
#[cfg(any(windows, test))]
mod sparse_scan;
#[cfg(windows)]
use sparse::*;
mod paths;
use paths::*;
mod names;
#[cfg(all(test, feature = "memory"))]
mod path_error_tests;
use names::*;
mod host_walk;
#[cfg(all(test, feature = "memory"))]
mod leaf_path_tests;
use host_walk::*;
#[cfg(test)]
mod host_observation_tests;
mod reconcile;
use reconcile::*;
mod state_order;
use state_order::*;
mod checkout_walk;
#[cfg(test)]
mod state_order_tests;
use checkout_walk::*;
mod watch_replay;
pub use watch_replay::capture_watch_batch;
mod watch_paths;
use watch_paths::*;
mod transaction;
use transaction::*;
mod stage;
use stage::*;
mod metadata;
#[cfg(all(test, any(target_os = "linux", windows), feature = "memory"))]
mod rescan_probe_tests;
#[cfg(all(test, target_os = "linux", feature = "memory"))]
mod stat_probe_tests;
use metadata::*;
#[cfg(all(test, feature = "memory"))]
mod empty_stage_tests;
#[cfg(test)]
mod host_file_race_tests;
#[cfg(test)]
mod native_range_tests;
#[cfg(all(test, feature = "memory", any(windows, target_os = "linux")))]
mod owner_allocation_tests;
#[cfg(all(test, target_os = "linux"))]
mod reuse_properties;
#[cfg(all(test, unix, feature = "native-mount"))]
mod root_capture_regression;
#[cfg(all(test, unix))]
mod unix_name_tests;
#[cfg(all(test, windows, feature = "memory"))]
mod windows_dense_tests;
#[cfg(all(test, windows))]
mod windows_name_tests;
#[cfg(all(test, windows))]
mod windows_sparse_tests;

#[cfg(feature = "native-mount")]
pub(crate) use entry::capture_paths_batched;
pub(crate) use entry::capture_subtrees_with_native_work;
pub use entry::{capture_baseline, capture_paths, capture_root_identity, capture_subtree};
#[cfg(feature = "native-mount")]
pub(crate) use model::MAX_NATIVE_EXACT_CAPTURE_PATHS;
pub(crate) use model::NativeViewBaseline;
pub use model::{CaptureError, CaptureOptions, CapturePolicy, CaptureReceipt, WatchCaptureReceipt};
pub(crate) use names::namespace_to_host_path_with_work;
pub use names::{host_path_to_namespace, namespace_to_host_path};
#[cfg(windows)]
pub(crate) use names::{namespace_component_wide, namespace_host_units_with_work};
