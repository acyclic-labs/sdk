use super::*;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

struct PausedObjects {
    inner: crate::MemoryObjectBackend,
    paused: AtomicBool,
    reads: AtomicUsize,
}

impl PausedObjects {
    async fn pause(&self) {
        if self.paused.load(Ordering::Relaxed) {
            self.reads.fetch_add(1, Ordering::Relaxed);
            std::future::pending::<()>().await;
        }
    }
}

impl AsyncObjectStore for PausedObjects {
    fn supports_decoded_cache(&self) -> bool {
        false
    }
    async fn put(
        &self,
        id: ObjectId,
        bytes: acyclic_stream::OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put(id, bytes, work).await
    }
    async fn put_many(
        &self,
        writes: &[crate::storage::ObjectWrite],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.inner.put_many(writes, work).await
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, ObjectStoreError> {
        self.pause().await;
        self.inner.read(id, maximum, work).await
    }
    async fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        self.pause().await;
        self.inner.read_many(requests, work).await
    }
    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        self.inner.contains(id, work).await
    }
}

type TestWorkspace = crate::Workspace<crate::MemoryAuthorityBackend, PausedObjects>;

async fn fixture(cancellation: &CancellationToken) -> TestWorkspace {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
    let fs = Fs::new(
        crate::distributed::StreamAuthorityStore::new(Arc::new(
            acyclic_stream::MemoryStream::default(),
        )),
        PausedObjects {
            inner: crate::LogicalObjectStore::new(Arc::new(objects), bucket),
            paused: AtomicBool::new(false),
            reads: AtomicUsize::new(0),
        },
        crate::EmbeddedCapabilities::MEMORY,
    );
    fs.create_workspace("frame-cancellation", WorkBudget::UNBOUNDED, cancellation)
        .await
        .unwrap()
        .value
}

fn poll_pending<F: Future>(future: std::pin::Pin<&mut F>) {
    assert!(matches!(
        future.poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
}

#[tokio::test]
async fn dropped_borrowed_directory_list_retires_its_frames_on_the_same_ledger() {
    let cancellation = CancellationToken::new();
    let workspace = fixture(&cancellation).await;
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap()
        .value;
    let mut setup = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let path = NamespacePath::new(Vec::new(), checkout.volume_config().limits).unwrap();
    let record = checkout
        .lookup_no_follow_with_accounting(&path, &mut setup, &cancellation, false)
        .await
        .unwrap()
        .0
        .record
        .unwrap();
    workspace
        .volume
        .fs
        .inner
        .objects
        .paused
        .store(true, Ordering::Relaxed);
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    // Admit the one ledger-lifetime control cell before comparing frame bytes.
    let ((), reservation) =
        crate::heap_future::in_heap_metered(&mut accounting, |_| std::future::ready(()))
            .unwrap()
            .await;
    crate::heap_future::FrameLedger::release_frame(&mut accounting, reservation).unwrap();
    // This control isolates future-frame custody. Raw query payload backing
    // has a separate cancellation-retirement defect, tracked independently.
    let baseline = accounting.standalone_parts().unwrap().0.live_frame_bytes();
    for _ in 0..2 {
        let mut operation = Box::pin(checkout.list_directory_record_with_accounting(
            record,
            std::ops::Bound::Unbounded,
            1,
            &mut accounting,
            &cancellation,
        ));
        poll_pending(operation.as_mut());
        drop(operation);
        assert_eq!(
            accounting.standalone_parts().unwrap().0.live_frame_bytes(),
            baseline,
            "cancelled directory list retained its frame"
        );
    }
    assert_eq!(
        workspace
            .volume
            .fs
            .inner
            .objects
            .reads
            .load(Ordering::Relaxed),
        2
    );
}

#[tokio::test]
async fn dropped_reads_and_forks_return_frames_to_the_original_native_ledger() {
    let cancellation = CancellationToken::new();
    let workspace = fixture(&cancellation).await;
    let generation = workspace
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .unwrap()
        .value;
    workspace
        .volume
        .fs
        .inner
        .objects
        .paused
        .store(true, Ordering::Relaxed);
    let work = crate::NativeMaterializationWork::new(
        crate::performance::work_budget(1_000_000, 64_000_000),
        &cancellation,
    )
    .unwrap();
    let baseline = work.remaining().unwrap().peak_allocation_bytes;
    for _ in 0..2 {
        let mut read = Box::pin(workspace.read("/file", 16, &work, &cancellation));
        poll_pending(read.as_mut());
        drop(read);
        assert_eq!(
            work.remaining().unwrap().peak_allocation_bytes,
            baseline,
            "cancelled read retained allocation"
        );
        let mut fork = Box::pin(workspace.fork::<_, _, WorkspaceError>(
            "cancelled-fork",
            crate::ForkOptions::from_generation(generation.clone(), crate::IdempotencyKey::new()),
            crate::PublicationPermit::Unrestricted,
            &work,
            &cancellation,
            crate::NativeAuthorityGuard(|_: &crate::NativeMaterializationWork| {
                std::future::ready(Ok::<(), FsError>(()))
            }),
        ));
        poll_pending(fork.as_mut());
        drop(fork);
        assert_eq!(
            work.remaining().unwrap().peak_allocation_bytes,
            baseline,
            "cancelled fork retained allocation"
        );
    }
    assert_eq!(
        workspace
            .volume
            .fs
            .inner
            .objects
            .reads
            .load(Ordering::Relaxed),
        4
    );
}
