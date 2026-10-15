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

#[test]
fn disjoint_writes_overlap_in_object_store() -> Result<(), Box<dyn std::error::Error>> {
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
        let write = |file, bytes| {
            let checkout = &source.checkout;
            let work = &work;
            let cancellation = source.cancellation();
            async move {
                let candidate = staged_write(checkout, file, bytes, work).await?;
                checkout
                    .install_change(candidate, ViewChange::Node(file), work, cancellation, None)
                    .await
            }
        };
        let left_write = write(left, b"left");
        let right_write = write(right, b"right");
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
