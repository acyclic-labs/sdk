use super::*;
use crate::kernel::{
    KernelAccounting, KernelOwnedAllocation, LogicalName, NameEncoding, Owned, OwnedSourceChunk,
};
use crate::{
    Fs, IdempotencyKey, LazyWorkspace, MemoryLazyWorkspaceStore, PublicationPermit, VolumeConfig,
    WorkBudget,
};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

#[derive(Default)]
struct Counts {
    snapshots: AtomicUsize,
    nodes: AtomicUsize,
    lookups: AtomicUsize,
    ranges: Mutex<Vec<(u64, u64)>>,
}
#[derive(Clone)]
struct Provider {
    data: Arc<Mutex<BTreeMap<u64, Bytes>>>,
    epoch: Arc<AtomicU64>,
    counts: Arc<Counts>,
    sink: Arc<Mutex<Option<Arc<dyn ViewChangeSink<u64>>>>>,
}
impl Provider {
    fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(BTreeMap::from([(
                0,
                Bytes::from_static(b"abcdef"),
            )]))),
            epoch: Arc::new(AtomicU64::new(0)),
            counts: Arc::default(),
            sink: Arc::default(),
        }
    }
    fn append(&self, bytes: Bytes, ctx: &mut KernelAccounting<'_>) {
        let epoch = self.epoch.load(Ordering::SeqCst) + 1;
        self.data.lock().unwrap().insert(epoch, bytes);
        self.epoch.store(epoch, Ordering::SeqCst);
        if let Some(sink) = self.sink.lock().unwrap().as_ref() {
            sink.changed(&epoch, ctx).unwrap();
        }
    }
}
impl ViewProvider for Provider {
    type Snapshot = Bytes;
    type Record = u64;
    fn reference(&self) -> SourceReference {
        SourceReference {
            identity: [7; 16],
            epoch: self.epoch.load(Ordering::SeqCst),
        }
    }
    fn snapshot(
        &self,
        source: SourceReference,
        _: &mut KernelAccounting<'_>,
    ) -> Result<(ViewCut, Bytes), DemandError> {
        self.counts.snapshots.fetch_add(1, Ordering::SeqCst);
        if source.identity != [7; 16] {
            return Err(DemandError::StaleSource);
        }
        let bytes = self
            .data
            .lock()
            .unwrap()
            .get(&source.epoch)
            .cloned()
            .ok_or(DemandError::StaleSource)?;
        Ok((
            ViewCut {
                sequence: source.epoch,
                generation: [0; 32],
            },
            bytes,
        ))
    }
    fn watch(
        &self,
        sink: Arc<dyn ViewChangeSink<u64>>,
        _: &mut KernelAccounting<'_>,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
        assert!(
            self.sink.lock().unwrap().replace(sink).is_none(),
            "one subscription in this fixture"
        );
        Ok(Some(Box::new(Watch(self.sink.clone()))))
    }
}
struct Watch(Arc<Mutex<Option<Arc<dyn ViewChangeSink<u64>>>>>);
impl SourceWatch for Watch {
    fn fence(&self, _: &mut KernelAccounting<'_>) -> Result<SourceFence, DemandError> {
        Ok(SourceFence::Confirmed)
    }
    fn stop(&self, _: &mut KernelAccounting<'_>) -> Result<(), DemandError> {
        self.0.lock().unwrap().take();
        Ok(())
    }
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
    fn is_exact(&self) -> bool {
        true
    }
}
struct Projection(Arc<Counts>);
fn name() -> LogicalName {
    LogicalName::new(NameEncoding::Utf8, b"index.md".to_vec()).unwrap()
}
fn metadata(bytes: Option<u64>) -> SourceNode {
    SourceNode {
        kind: if bytes.is_some() {
            SourceNodeKind::RegularFile
        } else {
            SourceNodeKind::Directory
        },
        file_identity: [u8::from(bytes.is_some()); 32],
        link_count: Some(1),
        device: None,
        logical_bytes: bytes,
        version: SourceVersion([0; 32]),
        metadata: SourceMetadata::default(),
    }
}
impl View for Projection {
    type Snapshot = Bytes;
    type Record = u64;
    fn identity(&self) -> [u8; 32] {
        [9; 32]
    }
    fn lookup(
        &self,
        _: ViewCut,
        bytes: &Bytes,
        path: &NamespacePath,
        _: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceNode>, DemandError> {
        self.0.lookups.fetch_add(1, Ordering::SeqCst);
        Ok(if path.is_root() {
            Some(metadata(None))
        } else if path.components() == [name()] {
            Some(metadata(Some(bytes.len() as u64)))
        } else {
            None
        })
    }
    fn nodes(
        &self,
        _: ViewCut,
        bytes: &Bytes,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        _: u32,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<SourceDirectoryPage, DemandError> {
        self.0.nodes.fetch_add(1, Ordering::SeqCst);
        if !directory.is_root() {
            return Err(DemandError::NotDirectory);
        }
        if cursor.is_some() {
            return Err(DemandError::StaleCursor);
        }
        SourceDirectoryPage::copied(
            std::iter::once((&name(), metadata(Some(bytes.len() as u64)))),
            None,
            SourceVersion([0; 32]),
            ctx,
        )
    }
    fn bytes(
        &self,
        _: ViewCut,
        bytes: &Bytes,
        _: &NamespacePath,
        offset: u64,
        length: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, DemandError> {
        self.0.ranges.lock().unwrap().push((offset, length));
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(bytes.len());
        let end = start
            .saturating_add(usize::try_from(length).unwrap_or(usize::MAX))
            .min(bytes.len());
        let data = Owned::copy_from_slice(&bytes[start..end], ctx)?;
        let capacity = data.bytes;
        let native = !matches!(data.allocation, KernelOwnedAllocation::Standalone);
        let bytes = crate::kernel::owned_read_bytes((data.value, data.allocation).into())?;
        #[cfg(not(target_arch = "wasm32"))]
        if native {
            return Ok(OwnedSourceChunk::intrinsic(bytes, capacity));
        }
        let _ = native;
        Ok(OwnedSourceChunk::claimed(bytes, capacity))
    }
    fn affected(
        &self,
        _: &u64,
        sink: &dyn SourceChangeSink,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        let cancel = ctx.cancellation();
        let (path, allocation) = NamespacePath::parse_in_profile(
            "/index.md",
            crate::FilesystemProfile::Portable,
            crate::VolumeLimits::default(),
            ctx,
            cancel,
        )?;
        let bytes = path.logical_owned_bytes();
        let result = sink.source_changed(&[SourceChange::Entry(path)], ctx);
        crate::kernel::retire_allocation(allocation, bytes, ctx)?;
        result
    }
}
fn path(text: &str) -> NamespacePath {
    NamespacePath::from_text_in_profile(
        text,
        crate::FilesystemProfile::Portable,
        crate::VolumeLimits::default(),
    )
    .unwrap()
}
macro_rules! ctx {
    () => {
        &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &CancellationToken::new())
    };
}
fn computed(provider: &Provider) -> Computed<Projection, Provider> {
    Computed::new(
        Arc::new(Projection(provider.counts.clone())),
        provider.clone(),
    )
}

#[tokio::test]
async fn computed_lazy_overlay_and_fork_use_one_demand_path() {
    let provider = Provider::new();
    let source = Arc::new(Routed::new(
        Empty,
        LogicalName::new(NameEncoding::Utf8, b"ui".to_vec()).unwrap(),
        computed(&provider),
        crate::VolumeLimits::default(),
    ));
    let fs = Fs::memory();
    let (root, _) = LazyWorkspace::attach(
        &fs,
        "computed-root",
        source,
        MemoryLazyWorkspaceStore::default(),
        crate::VolumeInitialization {
            config: VolumeConfig::portable(crate::Lifecycle::Ephemeral),
            access_policy: None,
        },
        crate::OperationId::new(),
        ctx!(),
    )
    .await
    .unwrap();
    assert_eq!(provider.counts.snapshots.load(Ordering::SeqCst), 0);
    let bytes = root.read_range("/ui/index.md", 2, 2, ctx!()).await.unwrap();
    assert_eq!(bytes.bytes.as_ref(), b"cd");
    assert_eq!(*provider.counts.ranges.lock().unwrap(), [(2, 2)]);
    assert_eq!(provider.counts.nodes.load(Ordering::SeqCst), 0);
    root.write(
        "/ui/index.md",
        acyclic_stream::OwnedBytes::from_static(b"authored"),
        IdempotencyKey::new(),
        PublicationPermit::Unrestricted,
        ctx!(),
    )
    .await
    .unwrap();
    let before = provider.counts.snapshots.load(Ordering::SeqCst);
    assert_eq!(
        root.read("/ui/index.md", 64, ctx!())
            .await
            .unwrap()
            .bytes
            .as_ref(),
        b"authored"
    );
    assert_eq!(provider.counts.snapshots.load(Ordering::SeqCst), before);
    let (child, cut) = root
        .fork(
            "computed-child",
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            ctx!(),
        )
        .await
        .unwrap();
    assert_eq!(
        child
            .read("/ui/index.md", 64, ctx!())
            .await
            .unwrap()
            .bytes
            .as_ref(),
        b"authored"
    );
    assert_eq!(child.state(ctx!()).await.unwrap().source, cut.source);
    // A view-only authored path does not need to exist in the source.
    child
        .write(
            "/ui/custom.md",
            acyclic_stream::OwnedBytes::from_static(b"custom"),
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            ctx!(),
        )
        .await
        .unwrap();
    assert_eq!(
        child
            .read("/ui/custom.md", 64, ctx!())
            .await
            .unwrap()
            .bytes
            .as_ref(),
        b"custom"
    );
    assert!(root.read("/ui/custom.md", 64, ctx!()).await.is_err());
    child.remove("/ui/index.md", None, ctx!()).await.unwrap();
    assert_eq!(
        child
            .read_latest("/ui/index.md", 64, ctx!())
            .await
            .unwrap()
            .bytes
            .as_ref(),
        b"abcdef"
    );
    child.remove("/ui/index.md", None, ctx!()).await.unwrap();
    assert!(child.read_latest("/ui/index.md", 64, ctx!()).await.is_err());
}

#[tokio::test]
async fn computed_exact_versions_ranges_and_routed_root_pages() {
    let provider = Provider::new();
    let view = computed(&provider);
    let source = view.reference();
    let cancel = CancellationToken::new();
    let node = view
        .lookup(source, &path("/index.md"), WorkBudget::UNBOUNDED, &cancel)
        .await
        .unwrap()
        .value
        .unwrap();
    let page = view
        .list_page(source, &path("/"), None, 1, WorkBudget::UNBOUNDED, &cancel)
        .await
        .unwrap()
        .value;
    assert_eq!(page.entries[0].node, node);
    let file = view
        .open_file(
            source,
            &path("/index.md"),
            node.version,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .unwrap()
        .value;
    provider.append(Bytes::from_static(b"changed"), ctx!());
    assert_eq!(file.read_range(1, 2, ctx!()).unwrap().bytes.as_ref(), b"bc");
    assert!(matches!(
        view.read_range(
            view.reference(),
            &path("/index.md"),
            node.version,
            0,
            1,
            WorkBudget::UNBOUNDED,
            &cancel
        )
        .await
        .unwrap_err()
        .error,
        DemandError::StaleVersion
    ));
    let routed = Routed::new(
        Empty,
        LogicalName::new(NameEncoding::Utf8, b"ui".to_vec()).unwrap(),
        view,
        crate::VolumeLimits::default(),
    );
    let first = routed
        .list_page(
            routed.reference(),
            &path("/"),
            None,
            1,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .unwrap()
        .value;
    assert_eq!(first.entries.len(), 1);
    assert_eq!(first.entries[0].name.as_bytes(), b"ui");
    let last = routed
        .list_page(
            routed.reference(),
            &path("/"),
            first.next,
            1,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .unwrap()
        .value;
    assert!(last.entries.is_empty());
    assert!(last.next.is_none());
}

#[derive(Default)]
struct Sink(Mutex<Vec<SourceChange>>);
impl SourceChangeSink for Sink {
    fn source_changed(
        &self,
        changes: &[SourceChange],
        _: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        self.0.lock().unwrap().extend_from_slice(changes);
        Ok(())
    }
    fn delivery_lost(&self) {}
}
#[test]
fn computed_changes_deliver_without_a_timer_or_readiness_probe() {
    let provider = Provider::new();
    let view = computed(&provider);
    let sink = Arc::new(Sink::default());
    let watch = view
        .watch(
            sink.clone(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .unwrap()
        .value
        .unwrap();
    provider.append(Bytes::from_static(b"new"), ctx!());
    assert_eq!(
        *sink.0.lock().unwrap(),
        [SourceChange::Entry(path("/index.md"))]
    );
    assert_eq!(watch.fence(ctx!()).unwrap(), SourceFence::Confirmed);
    watch.stop(ctx!()).unwrap();
    assert_eq!(provider.counts.snapshots.load(Ordering::SeqCst), 0);
}

#[test]
fn computed_idle_cost_is_independent_of_agent_count() {
    let provider = Provider::new();
    let view = Arc::new(Projection(provider.counts.clone()));
    for count in [10, 100_000] {
        let allocated = acyclic_test_support::measure(|| {
            for _ in 0..count {
                std::hint::black_box(Computed::new(view.clone(), provider.clone()));
            }
        });
        assert_eq!((allocated.count_total, allocated.bytes_total), (0, 0));
        assert_eq!(provider.counts.snapshots.load(Ordering::SeqCst), 0);
    }
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn computed_mounted_override_removal_preserves_the_original_data_decision() {
    for committed in [false, true] {
        let provider = Provider::new();
        let source = Arc::new(Routed::new(
            Empty,
            LogicalName::new(NameEncoding::Utf8, b"ui".to_vec()).unwrap(),
            computed(&provider),
            crate::VolumeLimits::default(),
        ));
        let fs = Fs::memory();
        let (root, _) = LazyWorkspace::attach(
            &fs,
            "mounted-computed",
            source,
            MemoryLazyWorkspaceStore::default(),
            crate::VolumeInitialization {
                config: VolumeConfig::portable(crate::Lifecycle::Ephemeral),
                access_policy: None,
            },
            crate::OperationId::new(),
            ctx!(),
        )
        .await
        .unwrap();
        root.write(
            "/ui/index.md",
            acyclic_stream::OwnedBytes::from_static(b"authored"),
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            ctx!(),
        )
        .await
        .unwrap();
        let key = IdempotencyKey::new();
        root.prepare_mount_removals(&["/ui/index.md".to_owned()], &[], &[], key, ctx!())
            .await
            .unwrap();
        if committed {
            let cancel = CancellationToken::new();
            let mut transaction = root
                .workspace()
                .begin_transaction(key, WorkBudget::UNBOUNDED, &cancel)
                .await
                .unwrap()
                .value;
            transaction
                .remove("/ui/index.md", WorkBudget::UNBOUNDED, &cancel)
                .await
                .unwrap();
            let outcome = transaction
                .commit(
                    None,
                    PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &cancel,
                    &mut |_| async {
                        Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                            crate::OperationReceipt {
                                value: (),
                                work: WorkCounters::UNCHARGED,
                            },
                        )
                    },
                )
                .await
                .unwrap()
                .value;
            assert!(matches!(
                outcome,
                crate::TransactionCommit::Committed(_)
                    | crate::TransactionCommit::AlreadyCommitted(_)
            ));
        }
        assert_eq!(root.finish_mount_removals(ctx!()).await.unwrap(), committed);
        let bytes = root.read("/ui/index.md", 64, ctx!()).await.unwrap().bytes;
        assert_eq!(
            bytes.as_ref(),
            if committed {
                b"abcdef".as_slice()
            } else {
                b"authored".as_slice()
            }
        );
        if committed {
            // Removing a projected default itself still records its deletion.
            root.remove("/ui/index.md", None, ctx!()).await.unwrap();
            assert!(root.read("/ui/index.md", 64, ctx!()).await.is_err());
        }
    }
}

#[tokio::test]
async fn computed_routed_hash_work_is_admitted_before_lookup_and_paging() {
    let provider = Provider::new();
    let view = computed(&provider);
    let source = view.reference();
    let cancel = CancellationToken::new();
    let direct_file = view
        .lookup(source, &path("/index.md"), WorkBudget::UNBOUNDED, &cancel)
        .await
        .unwrap();
    let direct_root = view
        .lookup(source, &path("/"), WorkBudget::UNBOUNDED, &cancel)
        .await
        .unwrap();
    let direct_page = view
        .list_page(source, &path("/"), None, 1, WorkBudget::UNBOUNDED, &cancel)
        .await
        .unwrap();
    let routed = Routed::new(
        Empty,
        LogicalName::new(NameEncoding::Utf8, b"ui".to_vec()).unwrap(),
        view,
        crate::VolumeLimits::default(),
    );
    let source = routed.reference();
    let file = routed
        .lookup(
            source,
            &path("/ui/index.md"),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .unwrap();
    // Every scoped identity hashes its 16-byte source and 32-byte identity once.
    assert_eq!(file.work.bytes_hashed, direct_file.work.bytes_hashed + 48);
    let page = routed
        .list_page(
            source,
            &path("/ui"),
            None,
            1,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .unwrap();
    assert_eq!(
        page.work.bytes_hashed,
        direct_page.work.bytes_hashed + 48 * page.value.entries.len() as u64
    );
    let root = routed
        .lookup(source, &path("/"), WorkBudget::UNBOUNDED, &cancel)
        .await
        .unwrap();
    let root_page = routed
        .list_page(source, &path("/"), None, 1, WorkBudget::UNBOUNDED, &cancel)
        .await
        .unwrap();
    let root_hash = (b"acyclic:routed:directory:v1".len() + 32 + 32 + b"ui".len()) as u64;
    assert_eq!(
        root.work.bytes_hashed,
        direct_root.work.bytes_hashed + 48 + root_hash
    );
    assert_eq!(
        root_page.work.bytes_hashed,
        direct_root.work.bytes_hashed + 48 + root_hash
    );
    for lookup in [true, false] {
        let limit = if lookup {
            file.work.bytes_hashed
        } else {
            page.work.bytes_hashed
        } - 1;
        let budget = WorkBudget {
            bytes_hashed: limit,
            ..WorkBudget::UNBOUNDED
        };
        let error = if lookup {
            routed
                .lookup(source, &path("/ui/index.md"), budget, &cancel)
                .await
                .unwrap_err()
                .error
        } else {
            routed
                .list_page(source, &path("/ui"), None, 1, budget, &cancel)
                .await
                .unwrap_err()
                .error
        };
        assert!(matches!(
            error,
            DemandError::Work(crate::WorkError::BudgetExceeded {
                counter: "bytes_hashed",
                ..
            })
        ));
    }
}
