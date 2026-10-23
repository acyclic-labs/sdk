use super::*;
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct PausedObjects {
    inner: crate::memory::MemoryObjectStore,
    paused: AtomicBool,
    entered: AtomicUsize,
    released: tokio::sync::Notify,
}

impl PausedObjects {
    async fn before_write(&self) {
        if self.paused.load(Ordering::Acquire) {
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            self.entered.fetch_add(1, Ordering::AcqRel);
            released.await;
        }
    }

    fn release(&self) {
        self.paused.store(false, Ordering::Release);
        self.released.notify_waiters();
    }
}

impl AsyncObjectStore for PausedObjects {
    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.before_write().await;
        self.inner.put_hashed(object, work).await
    }

    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.before_write().await;
        self.inner.put(id, bytes, work).await
    }

    async fn put_many(
        &self,
        writes: &[crate::storage::ObjectWrite],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.before_write().await;
        self.inner.put_many(writes, work).await
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        self.inner.read(id, maximum, work).await
    }

    async fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        self.inner.read_many(requests, work).await
    }

    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        self.inner.contains(id, work).await
    }
}

type PausedSource = CheckoutMountSource<crate::memory::MemoryAuthorityStore, Arc<PausedObjects>>;

fn paused_source() -> Result<(PausedSource, Arc<PausedObjects>), Box<dyn std::error::Error>> {
    let objects = Arc::new(PausedObjects::default());
    let fs = Fs::new(
        crate::memory::MemoryAuthorityStore::default(),
        Arc::clone(&objects),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let cancellation = CancellationToken::new();
    let checkout = runtime.block_on(async {
        let volume = fs
            .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map(|receipt| receipt.value)
    })?;
    let source = CheckoutMountSource::new(
        Arc::new(SharedCheckout::with_publication(
            checkout,
            MountPublication::Manual,
        )),
        config,
    )?;
    Ok((bind_test_access(source, vec![], None)?, objects))
}

async fn write(
    source: &PausedSource,
    file: FileId,
    offset: u64,
    bytes: &'static [u8],
    work: &crate::NativeMaterializationWork,
) -> Result<(), MountSourceError> {
    source
        .checkout
        .change_file_content(
            file,
            ContentChange::Write {
                offset,
                bytes: OwnedBytes::from_static(bytes),
            },
            false,
            ContentChangeContext {
                deadline: None,
                work,
                lifetime: source.cancellation(),
                clock: &source.runtime.clock,
                guard: None,
            },
        )
        .await
}

#[test]
fn disjoint_writes_overlap_in_object_store() -> Result<(), Box<dyn std::error::Error>> {
    let (source, objects) = paused_source()?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let paths = ["left", "right"].map(native_test_path);
    let left = source
        .create_file(&paths[0], metadata(), None, &work)?
        .node
        .file_id;
    let right = source
        .create_file(&paths[1], metadata(), None, &work)?
        .node
        .file_id;
    objects.paused.store(true, Ordering::Release);
    source.runtime.wait(|| async {
        let left_write = write(&source, left, 0, b"left", &work);
        let right_write = write(&source, right, 0, b"right", &work);
        tokio::pin!(left_write, right_write);
        assert!(futures::poll!(left_write.as_mut()).is_pending());
        assert_eq!(objects.entered.load(Ordering::Acquire), 1);
        assert!(futures::poll!(right_write.as_mut()).is_pending());
        assert_eq!(
            objects.entered.load(Ordering::Acquire),
            2,
            "both writes must reach object I/O before either completes"
        );
        objects.release();
        futures::try_join!(left_write, right_write).map(|_| ())
    })?;
    assert_eq!(
        source.open_file(&paths[0])?.read_up_to(0, 16)?.as_ref(),
        b"left"
    );
    assert_eq!(
        source.open_file(&paths[1])?.read_up_to(0, 16)?.as_ref(),
        b"right"
    );
    Ok(())
}

#[test]
fn same_file_writes_wait_and_both_land_in_order() -> Result<(), Box<dyn std::error::Error>> {
    for (offset, expected) in [(4, b"lefttail".as_slice()), (0, b"tail".as_slice())] {
        let (source, objects) = paused_source()?;
        let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
        let path = native_test_path("shared");
        let file = source
            .create_file(&path, metadata(), None, &work)?
            .node
            .file_id;
        objects.paused.store(true, Ordering::Release);
        source.runtime.wait(|| async {
            let first = write(&source, file, 0, b"left", &work);
            let second = write(&source, file, offset, b"tail", &work);
            tokio::pin!(first, second);
            assert!(futures::poll!(first.as_mut()).is_pending());
            assert_eq!(objects.entered.load(Ordering::Acquire), 1);
            assert!(futures::poll!(second.as_mut()).is_pending());
            assert_eq!(
                objects.entered.load(Ordering::Acquire),
                1,
                "same-file successor must wait before staging"
            );
            objects.release();
            futures::try_join!(first, second).map(|_| ())
        })?;
        assert_eq!(
            source.open_file(&path)?.read_up_to(0, 16)?.as_ref(),
            expected
        );
        assert!(
            source
                .checkout
                .file_mutations
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entries
                .is_empty()
        );
    }
    Ok(())
}

#[test]
fn cancelling_a_same_file_waiter_keeps_the_next_write_live()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, objects) = paused_source()?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let path = native_test_path("cancelled-waiter");
    let file = source
        .create_file(&path, metadata(), None, &work)?
        .node
        .file_id;
    let cancellation = CancellationToken::new();
    let waiting_work = capture_meter(BOUNDARY_BUDGET, &cancellation)?;
    objects.paused.store(true, Ordering::Release);
    source.runtime.wait(|| async {
        let first = write(&source, file, 0, b"left", &work);
        let cancelled = write(&source, file, 4, b"lost", &waiting_work);
        tokio::pin!(first, cancelled);
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert!(futures::poll!(cancelled.as_mut()).is_pending());
        assert_eq!(objects.entered.load(Ordering::Acquire), 1);
        cancellation.cancel();
        assert!(matches!(cancelled.await, Err(MountSourceError::Cancelled)));
        objects.release();
        first.await?;
        write(&source, file, 4, b"tail", &work).await
    })?;
    assert_eq!(
        source.open_file(&path)?.read_up_to(0, 16)?.as_ref(),
        b"lefttail"
    );
    assert!(
        source
            .checkout
            .file_mutations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .is_empty()
    );
    Ok(())
}
