use super::*;
use crate::demand::{DemandFile, SourceDirectoryEntry, SourceDirectoryPage, SourceMetadata};
use crate::kernel::{NameEncoding, OwnedSourceChunk};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

macro_rules! accounting {
    () => {
        &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())
    };
}
async fn attach<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
>(
    fs: &Fs<A, O>,
    name: &str,
    source: Arc<D>,
    store: S,
) -> Result<LazyWorkspace<A, O, D, S>, LazyWorkspaceError> {
    LazyWorkspace::attach(
        fs,
        name,
        source,
        store,
        VolumeConfig::portable(if fs.capabilities().durable {
            crate::Lifecycle::Durable
        } else {
            crate::Lifecycle::Ephemeral
        }),
        crate::OperationId::new(),
        accounting!(),
    )
    .await
    .map(|(lazy, _)| lazy)
}
#[derive(Default)]
struct SourceCounts {
    lookups: AtomicUsize,
    pages: AtomicUsize,
    ranges: AtomicUsize,
    cancel_range: AtomicBool,
}
struct CountingSource {
    epoch: AtomicU64,
    node: SourceNode,
    versions: Mutex<BTreeMap<u64, Bytes>>,
    directory_entries: usize,
    counts: Arc<SourceCounts>,
    cancel_on_page: AtomicBool,
}
impl CountingSource {
    fn new(bytes: Bytes) -> Self {
        Self {
            epoch: AtomicU64::new(3),
            node: SourceNode {
                kind: SourceNodeKind::RegularFile,
                file_identity: [8; 32],
                link_count: Some(1),
                device: None,
                logical_bytes: Some(bytes.len() as u64),
                version: SourceVersion([9; 32]),
                metadata: SourceMetadata::default(),
            },
            versions: Mutex::new(BTreeMap::from([(3, bytes)])),
            directory_entries: 1,
            counts: Arc::default(),
            cancel_on_page: AtomicBool::new(false),
        }
    }
    fn invalidate(&self) {
        let prior = self.epoch.fetch_add(1, Ordering::Relaxed);
        let mut versions = self.versions.lock().unwrap_or_else(|p| p.into_inner());
        let bytes = versions.get(&prior).cloned().unwrap_or_default();
        versions.insert(prior + 1, bytes);
    }
    fn replace(&self, bytes: Bytes) {
        let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
        self.versions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(epoch, bytes);
    }
    fn versioned_node(&self, source: SourceReference) -> Option<SourceNode> {
        if source.identity != [7; 16] {
            return None;
        }
        let versions = self.versions.lock().unwrap_or_else(|p| p.into_inner());
        let bytes = versions.get(&source.epoch)?;
        Some(SourceNode {
            logical_bytes: Some(bytes.len() as u64),
            version: SourceVersion(*blake3::hash(bytes).as_bytes()),
            ..self.node
        })
    }
    fn pinned(
        &self,
        source: SourceReference,
        version: SourceVersion,
    ) -> Result<Bytes, DemandError> {
        let node = self
            .versioned_node(source)
            .ok_or(DemandError::StaleSource)?;
        if node.version != version {
            return Err(DemandError::StaleVersion);
        }
        self.versions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&source.epoch)
            .cloned()
            .ok_or(DemandError::StaleSource)
    }
    fn with_directory_entries(mut self, entries: usize) -> Self {
        self.directory_entries = entries;
        self
    }
    fn cancel_next_page(&self) {
        self.cancel_on_page.store(true, Ordering::Relaxed);
    }
    #[cfg(feature = "native-mount")]
    fn cancel_next_range(&self) {
        self.counts.cancel_range.store(true, Ordering::Relaxed);
    }
}
struct PinnedFile {
    bytes: Bytes,
    counts: Arc<SourceCounts>,
    _allocation: KernelOwnedAllocation,
}
impl DemandFile for PinnedFile {
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
    fn read_range(
        &self,
        offset: u64,
        length: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, DemandError> {
        if self.counts.cancel_range.swap(false, Ordering::Relaxed) {
            cancellation(ctx).cancel();
        }
        ctx.check().map_err(SourceCursor::cursor_accounting_error)?;
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(self.bytes.len());
        let end = start
            .saturating_add(usize::try_from(length).unwrap_or(usize::MAX))
            .min(self.bytes.len());
        let read = WorkCounters {
            backend_read_operations: 1,
            source_bytes_read: (end - start) as u64,
            ..WorkCounters::UNCHARGED
        };
        ctx.admit(read)
            .map_err(SourceCursor::cursor_accounting_error)?;
        let mut out = Owned::<Vec<u8>>::allocate(end - start, ctx)
            .map_err(SourceCursor::cursor_accounting_error)?;
        self.counts.ranges.fetch_add(1, Ordering::Relaxed);
        out.value.extend_from_slice(&self.bytes[start..end]);
        ctx.record_completed(read)
            .map_err(SourceCursor::cursor_accounting_error)?;
        let bytes = crate::kernel::owned_read_bytes((out.value, out.allocation).into())
            .map_err(SourceCursor::cursor_accounting_error)?;
        #[cfg(not(target_arch = "wasm32"))]
        if ctx.native_work().is_some() {
            return Ok(OwnedSourceChunk::intrinsic(bytes, out.bytes));
        }
        Ok(OwnedSourceChunk::claimed(bytes, out.bytes))
    }
}
impl DemandSource for CountingSource {
    fn reference(&self) -> SourceReference {
        SourceReference {
            identity: [7; 16],
            epoch: self.epoch.load(Ordering::Relaxed),
        }
    }
    async fn lookup<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        _path: &NamespacePath,
        work: W,
        cancel: &CancellationToken,
    ) -> W::Output<Option<SourceNode>, DemandError> {
        let mut ctx = work.accounting(cancel);
        let result = (|| {
            ctx.charge(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::UNCHARGED
            })
            .map_err(SourceCursor::cursor_accounting_error)?;
            self.counts.lookups.fetch_add(1, Ordering::Relaxed);
            self.versioned_node(source)
                .map(Some)
                .ok_or(DemandError::StaleSource)
        })();
        work.finish(result, &ctx)
    }
    async fn list_page<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        _directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        _maximum: u32,
        work: W,
        cancel: &CancellationToken,
    ) -> W::Output<SourceDirectoryPage, DemandError> {
        let mut ctx = work.accounting(cancel);
        let result = (|| {
            self.counts.pages.fetch_add(1, Ordering::Relaxed);
            if self.cancel_on_page.swap(false, Ordering::Relaxed) {
                cancel.cancel();
                return Err(DemandError::Cancelled);
            }
            if cursor.is_some() {
                return Err(DemandError::StaleCursor);
            }
            let node = self
                .versioned_node(source)
                .ok_or(DemandError::StaleSource)?;
            ctx.charge(WorkCounters {
                backend_read_operations: 1,
                source_entries_visited: self.directory_entries as u64,
                ..WorkCounters::UNCHARGED
            })
            .map_err(SourceCursor::cursor_accounting_error)?;
            let mut entries = Owned::allocate(self.directory_entries, &mut ctx)
                .map_err(SourceCursor::cursor_accounting_error)?;
            for index in 0..self.directory_entries {
                let name = if self.directory_entries == 1 {
                    "file.txt".to_owned()
                } else {
                    format!("file-{index:04}.txt")
                };
                let mut guard = ctx
                    .claim_owned(name.capacity() as u64, 1)
                    .map_err(SourceCursor::cursor_accounting_error)?;
                let name = LogicalName::new(NameEncoding::Utf8, name.into_bytes(), 255)
                    .map_err(|_| DemandError::InvalidRequest)?;
                entries
                    .allocation
                    .absorb(&mut guard)
                    .map_err(SourceCursor::cursor_accounting_error)?;
                entries.value.push(SourceDirectoryEntry::new(name, node));
            }
            Ok(SourceDirectoryPage::stateless(
                entries.value,
                None,
                SourceVersion([4; 32]),
            ))
        })();
        work.finish(result, &ctx)
    }
    async fn read_range<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        _path: &NamespacePath,
        version: SourceVersion,
        offset: u64,
        length: u64,
        work: W,
        cancel: &CancellationToken,
    ) -> W::Output<OwnedSourceChunk, DemandError> {
        let mut ctx = work.accounting(cancel);
        let result = self.pinned(source, version).and_then(|bytes| {
            PinnedFile {
                bytes,
                counts: Arc::clone(&self.counts),
                _allocation: KernelOwnedAllocation::Standalone,
            }
            .read_range(offset, length, &mut ctx)
        });
        work.finish(result, &ctx)
    }
    async fn open_file<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        _path: &NamespacePath,
        version: SourceVersion,
        work: W,
        cancel: &CancellationToken,
    ) -> W::Output<Box<dyn DemandFile>, DemandError> {
        let mut ctx = work.accounting(cancel);
        let result = (|| {
            let bytes = self.pinned(source, version)?;
            let guard = ctx
                .claim_owned(std::mem::size_of::<PinnedFile>() as u64, 1)
                .map_err(SourceCursor::cursor_accounting_error)?;
            Ok(Box::new(PinnedFile {
                bytes,
                counts: Arc::clone(&self.counts),
                _allocation: guard,
            }) as Box<dyn DemandFile>)
        })();
        work.finish(result, &ctx)
    }
    async fn read_link<W: crate::facade::FsWork + Send>(
        &self,
        _source: SourceReference,
        _path: &NamespacePath,
        _version: SourceVersion,
        _maximum: u64,
        work: W,
        cancel: &CancellationToken,
    ) -> W::Output<OwnedSourceChunk, DemandError> {
        let ctx = work.accounting(cancel);
        work.finish(Err(DemandError::InvalidRequest), &ctx)
    }
}
mod binding;
#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
async fn native_source(
    path: &std::path::Path,
    profile: crate::model::FilesystemProfile,
) -> Arc<crate::demand::native::NativeDemandSource> {
    Arc::new(
        crate::demand::native::NativeDemandSource::open(
            std::borrow::Cow::Owned(path.to_path_buf()),
            profile,
            crate::model::VolumeLimits::default(),
            None,
            None,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .unwrap()
        .value,
    )
}
mod exactify;
mod lookup;
mod mount;
mod recovery;
mod snapshot;
