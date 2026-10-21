use super::*;
use crate::facade::FsWork;
use crate::lock_order::{CURSOR, Mutex as RankedMutex};
use crate::model::{FilesystemProfile, VolumeLimits};
use crate::native_host::HostListedEntry;
use crate::native_host::NativeHostWork;
use crate::native_host::{HostRoot, HostStat, HostStatReader};
use crate::source_watch::CallbackWork;
#[cfg(feature = "test-support")]
use acyclic_native_runtime::testing::spawn_blocking;
use std::collections::BTreeMap;
#[cfg(windows)]
use std::os::windows::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Semaphore;
#[cfg(not(feature = "test-support"))]
use tokio::task::spawn_blocking;

const MAXIMUM_NATIVE_REQUESTS: usize = 64;

include!("root.rs");
include!("directory.rs");
include!("excluded.rs");
include!("file.rs");
include!("provider.rs");
include!("watch.rs");
include!("metadata.rs");

mod queue;

#[cfg(unix)]
type DemandPath = PathBuf;
#[cfg(windows)]
type DemandPath = Vec<u16>;

fn account_source<T>(
    context: &mut impl SourceDemandReadWork,
    operation: impl FnOnce(
        &mut crate::kernel::KernelAccounting<'_>,
        &CancellationToken,
    ) -> Result<T, DemandError>,
) -> Result<T, DemandError> {
    if let Some(native) = context.native_source_work() {
        let stop = native.cancellation();
        return operation(
            &mut crate::kernel::KernelAccounting::native(&native, stop),
            stop,
        );
    }
    context.with_original_leaf(|work| {
        let (ledger, counters, budget, stop) = work.allocation_parts();
        operation(
            &mut crate::kernel::KernelAccounting::borrowed_standalone(
                ledger, counters, budget, stop,
            ),
            stop,
        )
    })
}
