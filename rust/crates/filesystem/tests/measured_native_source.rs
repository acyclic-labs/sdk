//! Production native-root admission and source-bound attachment failure receipts.
#![cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;

use acyclic_fs::demand::{
    DemandDirectoryObserver, DemandError, DemandSource, SourceReference, native::NativeDemandSource,
};
use acyclic_fs::facade::FsWork;
use acyclic_fs::kernel::{
    KernelAccounting, KernelOwnedAllocation, LogicalName, NameEncoding, NamespacePath,
};
use acyclic_fs::model::{FilesystemProfile, Lifecycle, VolumeConfig, VolumeLimits};
use acyclic_fs::{
    CancellationToken, Fs, IdempotencyKey, LazyOverlay, LazyOverlayId, LazyShadow, LazyShadowId,
    LazyWorkspace, LazyWorkspaceError, LazyWorkspaceState, LazyWorkspaceStore,
    MemoryLazyWorkspaceStore, OperationId, WorkBudget, WorkCounters, WorkspaceId,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const FINITE_BUDGET: WorkBudget = acyclic_fs::performance::work_budget(100_000_000, 100_000_000);

async fn open_source(
    path: &std::path::Path,
) -> Result<NativeDemandSource, Box<dyn std::error::Error>> {
    NativeDemandSource::open(
        std::borrow::Cow::Owned(path.to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error.into())
}

#[derive(Default)]
struct DirectoryObserver(AtomicUsize);

impl DemandDirectoryObserver for DirectoryObserver {
    fn observe_directory(
        &self,
        _directory: &NamespacePath,
        accounting: &mut acyclic_fs::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        accounting
            .charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })
            .map_err(DemandError::from)?;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn requested(name: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    let limits = VolumeLimits::default();
    let components = if name.is_empty() {
        Vec::new()
    } else {
        vec![LogicalName::new(
            NameEncoding::Utf8,
            name.as_bytes().to_vec(),
            limits.maximum_component_bytes,
        )?]
    };
    Ok(NamespacePath::new(components, limits)?)
}

#[path = "measured_native_source/admission.rs"]
mod admission;
#[path = "measured_native_source/companion.rs"]
mod companion;
#[path = "measured_native_source/cursor.rs"]
mod cursor;
#[path = "measured_native_source/epoch.rs"]
mod epoch;
#[path = "measured_native_source/requests.rs"]
mod requests;
#[cfg(windows)]
#[path = "measured_native_source/utf16.rs"]
mod utf16;
