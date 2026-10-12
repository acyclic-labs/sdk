
    use super::*;
    use crate::demand::{
        DemandFile, DemandResult, SourceCursor, SourceDirectoryEntry, SourceDirectoryPage,
        SourceMetadata, SourceVersion,
    };
    use crate::kernel::NameEncoding;
    use crate::performance::{OperationFailure, OperationReceipt, WorkCounters};
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

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
        counts: SourceCounts,
        cancel_on_page: AtomicBool,
        cancel_on_range: AtomicBool,
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
                counts: SourceCounts::default(),
                cancel_on_page: AtomicBool::new(false),
                cancel_on_range: AtomicBool::new(false),
            }
        }

        fn receipt<T>(value: T) -> DemandResult<T> {
            Ok(OperationReceipt {
                value,
                work: WorkCounters::default(),
            })
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
        ) -> Result<Bytes, OperationFailure<DemandError>> {
            let Some(node) = self.versioned_node(source) else {
                return Err(OperationFailure::before_work(DemandError::StaleSource));
            };
            if expected != node.version {
                return Err(OperationFailure::before_work(DemandError::StaleVersion));
            }
            self.versions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&source.epoch)
                .cloned()
                .ok_or_else(|| OperationFailure::before_work(DemandError::StaleSource))
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

    /// One source version's bytes, pinned when the file was opened.
    struct PinnedFile(Bytes);

    impl DemandFile for PinnedFile {
        fn read_range(
            &self,
            offset: u64,
            length: u64,
            _cancellation: &CancellationToken,
        ) -> DemandResult<Bytes> {
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(self.0.len());
            let end = start
                .saturating_add(usize::try_from(length).unwrap_or(usize::MAX))
                .min(self.0.len());
            CountingSource::receipt(self.0.slice(start..end))
        }
    }

    #[async_trait]
    impl DemandSource for CountingSource {
        fn reference(&self) -> SourceReference {
            SourceReference {
                identity: self.identity,
                epoch: self.epoch.load(Ordering::Relaxed),
            }
        }

        async fn lookup(
            &self,
            source: SourceReference,
            _path: &NamespacePath,
            _cancellation: &CancellationToken,
        ) -> DemandResult<Option<SourceNode>> {
            self.counts.lookups.fetch_add(1, Ordering::Relaxed);
            let Some(node) = self.versioned_node(source) else {
                return Err(OperationFailure::before_work(DemandError::StaleSource));
            };
            Self::receipt(Some(node))
        }

        async fn list_page(
            &self,
            source: SourceReference,
            _directory: &NamespacePath,
            cursor: Option<SourceCursor>,
            _maximum_entries: u32,
            cancellation: &CancellationToken,
        ) -> DemandResult<SourceDirectoryPage> {
            self.counts.pages.fetch_add(1, Ordering::Relaxed);
            if self.cancel_on_page.swap(false, Ordering::Relaxed) {
                cancellation.cancel();
                return Err(OperationFailure::before_work(DemandError::Cancelled));
            }
            if cursor.is_some() {
                return Err(OperationFailure::before_work(DemandError::StaleCursor));
            }
            let Some(node) = self.versioned_node(source) else {
                return Err(OperationFailure::before_work(DemandError::StaleSource));
            };
            let entries = if self.directory_entries == 1 {
                vec![SourceDirectoryEntry {
                    name: LogicalName::new(NameEncoding::Utf8, b"file.txt".to_vec(), 255)
                        .expect("name"),
                    node,
                }]
            } else {
                (0..self.directory_entries)
                    .map(|index| SourceDirectoryEntry {
                        name: LogicalName::new(
                            NameEncoding::Utf8,
                            format!("file-{index:04}.txt").into_bytes(),
                            255,
                        )
                        .expect("name"),
                        node,
                    })
                    .collect()
            };
            Self::receipt(SourceDirectoryPage {
                entries,
                next: None,
                version: SourceVersion([4; 32]),
            })
        }

        async fn read_range(
            &self,
            source: SourceReference,
            _path: &NamespacePath,
            expected: SourceVersion,
            offset: u64,
            length: u64,
            cancellation: &CancellationToken,
        ) -> DemandResult<Bytes> {
            self.counts.ranges.fetch_add(1, Ordering::Relaxed);
            if self.cancel_on_range.swap(false, Ordering::Relaxed) {
                cancellation.cancel();
            }
            PinnedFile(self.pinned(source, expected)?).read_range(offset, length, cancellation)
        }

        async fn open_file(
            &self,
            source: SourceReference,
            _path: &NamespacePath,
            expected: SourceVersion,
            _cancellation: &CancellationToken,
        ) -> DemandResult<Box<dyn DemandFile>> {
            Self::receipt(Box::new(PinnedFile(self.pinned(source, expected)?)))
        }

        async fn read_link(
            &self,
            _source: SourceReference,
            _path: &NamespacePath,
            _expected: SourceVersion,
            _cancellation: &CancellationToken,
        ) -> DemandResult<Bytes> {
            Err(OperationFailure::before_work(DemandError::InvalidRequest))
        }
    }


mod binding;
mod mount;
mod lookup;
mod exactify;
mod recovery;
