use super::*;
use crate::demand::{
    DemandFile, SourceCursor, SourceDirectoryEntry, SourceDirectoryPage, SourceMetadata,
    SourceVersion,
};
use crate::kernel::NameEncoding;
use crate::performance::WorkCounters;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

macro_rules! accounting {
    () => {
        &mut crate::kernel::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
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
    let cancellation = CancellationToken::new();
    let mut accounting =
        crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let lifecycle = if fs.capabilities().durable {
        crate::Lifecycle::Durable
    } else {
        crate::Lifecycle::Ephemeral
    };
    LazyWorkspace::attach(
        fs,
        name,
        source,
        store,
        VolumeConfig::portable(lifecycle),
        crate::OperationId::new(),
        &mut accounting,
    )
    .await
    .map(|(lazy, _)| lazy)
}

#[derive(Default)]
struct SourceCounts {
    lookups: AtomicUsize,
    pages: AtomicUsize,
    ranges: AtomicUsize,
}

struct CountingSource {
    identity: [u8; 16],
    epoch: AtomicU64,
    node: SourceNode,
    versions: Mutex<BTreeMap<u64, Bytes>>,
    directory_entries: usize,
    counts: Arc<SourceCounts>,
    cancel_on_page: AtomicBool,
    cancel_on_range: Arc<AtomicBool>,
}

impl CountingSource {
    fn new(bytes: Bytes) -> Self {
        let logical_bytes = bytes.len() as u64;
        let versions = BTreeMap::from([(3, bytes)]);
        Self {
            identity: [7; 16],
            epoch: AtomicU64::new(3),
            node: SourceNode {
                kind: SourceNodeKind::RegularFile,
                file_identity: [8; 32],
                link_count: Some(1),
                device: None,
                logical_bytes: Some(logical_bytes),
                version: SourceVersion([9; 32]),
                metadata: SourceMetadata::default(),
            },
            versions: Mutex::new(versions),
            directory_entries: 1,
            counts: Arc::default(),
            cancel_on_page: AtomicBool::new(false),
            cancel_on_range: Arc::new(AtomicBool::new(false)),
        }
    }

    fn invalidate(&self) {
        let prior = self.epoch.fetch_add(1, Ordering::Relaxed);
        let mut versions = self
            .versions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let bytes = versions.get(&prior).cloned().unwrap_or_default();
        versions.insert(prior + 1, bytes);
    }

    fn replace(&self, bytes: Bytes) {
        let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
        self.versions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(epoch, bytes);
    }

    fn pinned(
        &self,
        source: SourceReference,
        expected: SourceVersion,
    ) -> Result<Bytes, DemandError> {
        let Some(node) = self.versioned_node(source) else {
            return Err(DemandError::StaleSource);
        };
        if expected != node.version {
            return Err(DemandError::StaleVersion);
        }
        self.versions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&source.epoch)
            .cloned()
            .ok_or(DemandError::StaleSource)
    }

    fn versioned_node(&self, source: SourceReference) -> Option<SourceNode> {
        if source.identity != self.identity {
            return None;
        }
        let versions = self
            .versions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let bytes = versions.get(&source.epoch)?;
        Some(SourceNode {
            logical_bytes: Some(bytes.len() as u64),
            version: SourceVersion(*blake3::hash(bytes).as_bytes()),
            ..self.node
        })
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
        self.cancel_on_range.store(true, Ordering::Relaxed);
    }
}

struct PinnedFile {
    bytes: Bytes,
    counts: Arc<SourceCounts>,
    cancel: Arc<AtomicBool>,
}

impl DemandFile for PinnedFile {
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
    fn read_range(
        &self,
        offset: u64,
        length: u64,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
        let token = cancellation(accounting);
        if self.cancel.swap(false, Ordering::Relaxed) {
            token.cancel();
        }
        accounting
            .check()
            .map_err(DemandError::from)?;
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(self.bytes.len());
        let end = start
            .saturating_add(usize::try_from(length).unwrap_or(usize::MAX))
            .min(self.bytes.len());
        let delta = WorkCounters {
            backend_read_operations: 1,
            source_bytes_read: (end - start) as u64,
            ..WorkCounters::UNCHARGED
        };
        accounting
            .admit(delta)
            .map_err(DemandError::from)?;
        let mut bytes = crate::kernel::Owned::<Vec<u8>>::allocate(end - start, accounting)
            .map_err(DemandError::from)?;
        self.counts.ranges.fetch_add(1, Ordering::Relaxed);
        bytes.value.extend_from_slice(&self.bytes[start..end]);
        accounting
            .record_completed(delta)
            .map_err(DemandError::from)?;
        Ok(crate::kernel::OwnedSourceChunk::claimed(
            bytes.value.into(),
            bytes.bytes,
        ))
    }
}

impl DemandSource for CountingSource {
    fn reference(&self) -> SourceReference {
        SourceReference {
            identity: self.identity,
            epoch: self.epoch.load(Ordering::Relaxed),
        }
    }
    async fn lookup<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        _path: &NamespacePath,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<SourceNode>, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| {
            accounting
                .charge(WorkCounters {
                    backend_read_operations: 1,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(DemandError::from)?;
            self.counts.lookups.fetch_add(1, Ordering::Relaxed);
            self.versioned_node(source)
                .map(Some)
                .ok_or(DemandError::StaleSource)
        })();
        work.finish(result, &accounting)
    }
    async fn list_page<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        _directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        _maximum_entries: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<SourceDirectoryPage, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| {
            self.counts.pages.fetch_add(1, Ordering::Relaxed);
            if self.cancel_on_page.swap(false, Ordering::Relaxed) {
                cancellation.cancel();
                return Err(DemandError::Cancelled);
            }
            if cursor.is_some() {
                return Err(DemandError::StaleCursor);
            }
            let node = self
                .versioned_node(source)
                .ok_or(DemandError::StaleSource)?;
            accounting
                .charge(WorkCounters {
                    backend_read_operations: 1,
                    source_entries_visited: self.directory_entries as u64,
                    ..WorkCounters::UNCHARGED
                })
                .map_err(DemandError::from)?;
            let mut entries =
                crate::kernel::Owned::allocate(self.directory_entries, &mut accounting)
                    .map_err(DemandError::from)?;
            for index in 0..self.directory_entries {
                let name = if self.directory_entries == 1 {
                    "file.txt".to_owned()
                } else {
                    format!("file-{index:04}.txt")
                };
                let mut allocation = accounting
                    .claim_owned(name.len() as u64, 1)
                    .map_err(DemandError::from)?;
                let name = LogicalName::new(NameEncoding::Utf8, name.into_bytes(), 255)
                    .map_err(|_| DemandError::InvalidRequest)?;
                entries
                    .allocation
                    .absorb(&mut allocation)
                    .map_err(DemandError::from)?;
                entries.value.push(SourceDirectoryEntry::new(name, node));
            }
            Ok(SourceDirectoryPage::stateless(
                entries.value,
                None,
                SourceVersion([4; 32]),
            ))
        })();
        work.finish(result, &accounting)
    }
    async fn read_range<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        _path: &NamespacePath,
        expected: SourceVersion,
        offset: u64,
        length: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.pinned(source, expected).and_then(|bytes| {
            PinnedFile {
                bytes,
                counts: Arc::clone(&self.counts),
                cancel: Arc::clone(&self.cancel_on_range),
            }
            .read_range(offset, length, &mut accounting)
        });
        work.finish(result, &accounting)
    }
    async fn open_file<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        _path: &NamespacePath,
        expected: SourceVersion,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Box<dyn DemandFile>, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| {
            let bytes = self.pinned(source, expected)?;
            accounting
                .claim_owned(std::mem::size_of::<PinnedFile>() as u64, 1)
                .map_err(DemandError::from)?;
            Ok(Box::new(PinnedFile {
                bytes,
                counts: Arc::clone(&self.counts),
                cancel: Arc::clone(&self.cancel_on_range),
            }) as Box<dyn DemandFile>)
        })();
        work.finish(result, &accounting)
    }
    async fn read_link<W: crate::facade::FsWork + Send>(
        &self,
        _source: SourceReference,
        _path: &NamespacePath,
        _expected: SourceVersion,
        _maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
        let accounting = work.accounting(cancellation);
        work.finish(Err(DemandError::InvalidRequest), &accounting)
    }
}

mod binding;
mod exactify;
mod lookup;
mod mount;
mod recovery;
