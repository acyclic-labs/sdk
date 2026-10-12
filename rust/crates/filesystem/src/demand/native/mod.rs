use super::*;

use crate::model::{FilesystemProfile, VolumeLimits};

use crate::native_host::{HostRoot, HostStat, HostStatReader};

use crate::native_host::NativeHostWork as _;

#[cfg(unix)]
use crate::native_host::HostListedEntry;

#[cfg(windows)]
use crate::native_host::HostListedWideEntry as HostListedEntry;

use std::path::{Path, PathBuf};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use std::sync::{Arc, Mutex};

use std::time::Duration;

use tokio::sync::Semaphore;

const MAXIMUM_NATIVE_REQUESTS: usize = 64;

mod changes;
mod directory;
mod file;
mod metadata;
mod queue;
mod root;
mod watch;
pub(in crate::demand) use changes::*;
pub(in crate::demand) use directory::*;
pub(in crate::demand) use file::*;
pub(in crate::demand) use metadata::*;
pub(in crate::demand) use root::*;
pub(in crate::demand) use watch::*;

/// One native root. Opening it does not enumerate any descendants.
#[derive(Clone)]
pub struct NativeDemandSource {
    pub(in crate::demand) inner: Arc<NativeDemandInner>,
    /// Whether a test let this source be watched, so tests cover the path
    #[cfg(test)]
    pub(in crate::demand) watchable: bool,
}

pub(in crate::demand) struct NativeDemandInner {
    pub(in crate::demand) path: DemandPath,
    pub(in crate::demand) root: HostRoot,
    /// Whether local host I/O can run on the native callback thread.
    pub(in crate::demand) inline: bool,
    pub(in crate::demand) identity: [u8; 16],
    pub(in crate::demand) epoch: AtomicU64,
    pub(in crate::demand) profile: FilesystemProfile,
    pub(in crate::demand) limits: VolumeLimits,
    pub(in crate::demand) cursors: Mutex<CursorSlots>,
    pub(in crate::demand) next_cursor: AtomicU64,
    pub(in crate::demand) requests: Arc<Semaphore>,
    pub(in crate::demand) observer: Option<Arc<dyn DemandDirectoryObserver>>,
    /// Live change watches: on Linux, inotify admits directories to them
    #[cfg(any(target_os = "linux", windows))]
    pub(in crate::demand) watches: std::sync::RwLock<NativeSourceWatches>,
    // The native path is physically dropped before its custody retires.
    pub(in crate::demand) root_custody: SourceRootPathCustody,
}

impl DemandSource for NativeDemandSource {
    fn reference(&self) -> SourceReference {
        SourceReference {
            identity: self.inner.identity,
            epoch: self.inner.epoch.load(Ordering::Acquire),
        }
    }

    fn watch<W: crate::facade::FsWork + Send>(
        &self,
        sink: std::sync::Arc<dyn SourceChangeSink>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<Box<dyn SourceWatch>>, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| {
            if let Some(original) = accounting.native_work() {
                return self.subscribe(
                    sink,
                    &mut NativeSourceWork::from_native_materialization_work(original),
                );
            }
            let budget = accounting
                .remaining()
                .map_err(SourceCursor::cursor_accounting_error)?;
            let mut work = DemandWork::new(budget, cancellation);
            let answer = self.subscribe(sink, &mut work);
            let answer = answer
                .map(|value| OperationReceipt {
                    value,
                    work: work.work,
                })
                .map_err(|error| OperationFailure::new(error, work.work));
            accept_source_result(answer, &mut accounting)
        })();
        work.finish(result, &accounting)
    }

    async fn lookup<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<SourceNode>, DemandError> {
        self.request(path, work, cancellation, move |provider, path, work| {
            provider.lookup_node(source, path, work)
        })
        .await
    }

    async fn list_page<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum_entries: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<SourceDirectoryPage, DemandError> {
        if maximum_entries == 0 || maximum_entries > 4096 {
            return work.finish(
                Err(DemandError::InvalidRequest),
                &work.accounting(cancellation),
            );
        }
        self.request(
            directory,
            work,
            cancellation,
            move |provider, directory, work| {
                provider.directory_page(source, directory, cursor, maximum_entries, work)
            },
        )
        .await
    }

    async fn read_range<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        offset: u64,
        length: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
        if let Err(error) = check_read_range(offset, length) {
            return work.finish(Err(error), &work.accounting(cancellation));
        }
        self.request(path, work, cancellation, move |provider, path, work| {
            NativeDemandFile::open(provider, source, path, expected, work)?
                .read(offset, length, work)
        })
        .await
    }

    async fn open_file<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Box<dyn DemandFile>, DemandError> {
        self.request(path, work, cancellation, move |provider, path, work| {
            NativeDemandFile::open_boxed(provider, source, path, expected, work)
        })
        .await
    }

    async fn read_link<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
        self.request(path, work, cancellation, move |provider, path, work| {
            provider.link_target(source, path, expected, maximum_bytes, work)
        })
        .await
    }
}
