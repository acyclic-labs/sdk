use super::*;
use crate::facade::FsWork;
use crate::lock_order::{CURSOR, Mutex as RankedMutex};
use crate::model::{FilesystemProfile, VolumeLimits};
use crate::native_host::HostListedEntry;
use crate::native_host::NativeHostWork;
use crate::native_host::{HostRoot, HostStat, HostStatReader};
use crate::source_watch::CallbackWork;
use std::collections::BTreeMap;
#[cfg(windows)]
use std::os::windows::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Semaphore;

const MAXIMUM_NATIVE_REQUESTS: usize = 64;

include!("root.rs");
include!("directory.rs");
include!("excluded.rs");
include!("file.rs");
include!("provider.rs");
include!("watch.rs");
include!("metadata.rs");

#[cfg(test)]
#[path = "read_work.rs"]
pub(crate) mod demand_read_work;
mod queue;
mod remote;
use crate::native_host::charged::Charged;

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

#[cfg(test)]
mod read_baseline;

#[cfg(test)]
pub(super) fn baseline_file(
    source: NativeDemandSource,
    path: &NamespacePath,
    expected: SourceVersion,
) -> Result<Box<dyn DemandFile>, DemandError> {
    let reference = source.reference();
    let mut context = CallbackWork::Ordinary(DemandWork::new(
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ));
    Ok(Box::new(read_baseline::BaselineDemandFile::open(
        source,
        reference,
        path,
        expected,
        &mut context,
    )?))
}
