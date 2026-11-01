use super::*;
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct PausedObjects {
    inner: crate::memory::MemoryObjectStore,
    paused: AtomicBool,
    paused_reads: AtomicBool,
    entered: AtomicUsize,
    released: tokio::sync::Notify,
}

impl PausedObjects {
    async fn before_write(&self) {
        self.before_io(&self.paused).await;
    }

    async fn before_io(&self, paused: &AtomicBool) {
        if paused.load(Ordering::Acquire) {
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            self.entered.fetch_add(1, Ordering::AcqRel);
            released.await;
        }
    }

    fn release(&self) {
        self.paused.store(false, Ordering::Release);
        self.paused_reads.store(false, Ordering::Release);
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
        self.before_io(&self.paused_reads).await;
        self.inner.read(id, maximum, work).await
    }

    async fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        self.before_io(&self.paused_reads).await;
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
) -> Result<crate::native_mount::MountWriteResult, MountSourceError> {
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
                facts: true,
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
    source.runtime.wait(|| async {
        objects.paused_reads.store(true, Ordering::Release);
        let mut first_carrier = NativeDemandCarrier::Native(&work);
        let mut second_carrier = NativeDemandCarrier::Native(&work);
        let first = source
            .checkout
            .candidate(None, &mut first_carrier, work.cancellation(), None);
        let second =
            source
                .checkout
                .candidate(None, &mut second_carrier, work.cancellation(), None);
        tokio::pin!(first, second);
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert_eq!(objects.entered.load(Ordering::Acquire), 1);
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert_eq!(
            objects.entered.load(Ordering::Acquire),
            2,
            "a private policy read must not exclude another candidate"
        );
        let state = source
            .checkout
            .state
            .try_write()
            .expect("checkout unlocked during policy I/O");
        let root = state.checkout.root().file_table;
        drop(state);
        objects.release();
        let (first, second) = futures::try_join!(first, second)?;
        assert_eq!(first.base.root().file_table, root);
        assert_eq!(second.base.root().file_table, root);
        assert_eq!(first.epoch, second.epoch);
        assert_eq!(first.stamp, second.stamp);
        drop((first, second));

        // Abandon the original read while its provider is still delayed. Its
        // publication lease and private proof must not exclude the successor.
        objects.paused_reads.store(true, Ordering::Release);
        let mut carrier = NativeDemandCarrier::Native(&work);
        let mut abandoned = Box::pin(source.checkout.candidate(
            None,
            &mut carrier,
            work.cancellation(),
            None,
        ));
        assert!(futures::poll!(abandoned.as_mut()).is_pending());
        drop(abandoned);
        assert!(source.checkout.publication_fence.try_write().is_ok());
        assert!(source.checkout.state.try_write().is_ok());
        objects.release();
        drop(candidate_now(&source.checkout, &work).await?);
        objects.entered.store(0, Ordering::Release);
        Ok(())
    })?;
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
        let (left_result, right_result) = futures::try_join!(left_write, right_write)?;
        for (result, id, length) in [(left_result, left, 4), (right_result, right, 5)] {
            assert_eq!((result.file_id, result.written), (id, length));
            let facts = result.facts.expect("requested final write facts");
            assert_eq!(facts.lookup.node.file_id, id);
            assert_eq!(facts.lookup.node.logical_bytes, length);
            assert_eq!(facts.lookup.node.link_count, 1);
            assert!(source.node_unchanged_since_with_meter(id, facts.stamp, &work)?);
        }
        Ok(())
    })?;
    assert_eq!(
        source.open_file(&paths[0])?.read_up_to(0, 16)?.as_ref(),
        b"left"
    );
    assert_eq!(
        source.open_file(&paths[1])?.read_up_to(0, 16)?.as_ref(),
        b"right"
    );

    // A link adopted after staging changes the final record without changing
    // this write's content. Pre-rebase facts would still report one binding.
    let candidate = source
        .runtime
        .wait(|| staged_write(&source.checkout, left, b"rebased", &work))?;
    source.hard_link(&paths[0], &native_test_path("alias"), &work)?;
    let facts = source
        .runtime
        .wait(|| {
            source.checkout.install_change(
                candidate,
                ViewChange::Node(left),
                Some(left),
                &work,
                source.cancellation(),
                None,
            )
        })?
        .expect("adopted facts");
    assert_eq!(facts.lookup.node.file_id, left);
    assert_eq!(facts.lookup.node.logical_bytes, 7);
    assert_eq!(facts.lookup.node.link_count, 2);
    assert_eq!(
        source
            .open_file(&native_test_path("alias"))?
            .read_up_to(0, 16)?
            .as_ref(),
        b"rebased"
    );

    // Force an actual merge against a newer root before delaying the provider.
    // Each exit retains the original pin and mutation/publication leases.
    for exit in ["return", "superseded", "abandoned", "cancelled"] {
        source.runtime.wait(|| async {
            let stop = CancellationToken::new();
            let pending_work = capture_meter(BOUNDARY_BUDGET, &stop)?;
            let bytes = match exit {
                "superseded" => b"refused",
                "abandoned" => b"abandon",
                "cancelled" => b"cancel!",
                _ => b"outside",
            };
            let pending = staged_write(&source.checkout, left, bytes, &pending_work).await?;
            let seed = match exit {
                "superseded" => b"seed-b",
                "abandoned" => b"seed-c",
                "cancelled" => b"seed-d",
                _ => b"seed-a",
            };
            write(&source, right, 0, seed, &work).await?;
            let winner = staged_write(&source.checkout, right, b"newest", &work).await?;
            let reads = objects.entered.load(Ordering::Acquire);
            objects.paused_reads.store(true, Ordering::Release);
            let mut rebase = Box::pin(source.checkout.install_change(
                pending,
                ViewChange::Node(left),
                None,
                &pending_work,
                source.cancellation(),
                None,
            ));
            assert!(futures::poll!(rebase.as_mut()).is_pending());
            assert_eq!(objects.entered.load(Ordering::Acquire), reads + 1);
            assert!(
                source.checkout.state.try_write().is_ok(),
                "rebase I/O retains no shared checkout guard"
            );
            match exit {
                "superseded" => {
                    source
                        .checkout
                        .install_change(
                            winner,
                            ViewChange::Node(right),
                            None,
                            &work,
                            source.cancellation(),
                            None,
                        )
                        .await?;
                    objects.release();
                    assert!(matches!(rebase.await, Err(MountSourceError::Stale)));
                }
                "abandoned" => {
                    drop(winner);
                    drop(rebase);
                    objects.release();
                }
                "cancelled" => {
                    drop(winner);
                    stop.cancel();
                    objects.release();
                    assert!(matches!(rebase.await, Err(MountSourceError::Cancelled)));
                }
                _ => {
                    drop(winner);
                    objects.release();
                    rebase.await?;
                }
            }
            assert!(source.checkout.state.try_write().is_ok());
            assert!(source.checkout.publication_fence.try_write().is_ok());
            Ok(())
        })?;
        assert_eq!(
            source.open_file(&paths[0])?.read_up_to(0, 16)?.as_ref(),
            b"outside"
        );
    }

    // Project the original private candidate while a disjoint candidate wins.
    // The original read is released by its provider; adoption must then refuse
    // the changed base instead of returning facts for a different root.
    source.runtime.wait(|| async {
        let pending = staged_write(&source.checkout, left, b"stale!!", &work).await?;
        let winner = staged_write(&source.checkout, right, b"winner", &work).await?;
        objects.paused_reads.store(true, Ordering::Release);
        let projection = source.checkout.install_change(
            pending,
            ViewChange::Node(left),
            Some(left),
            &work,
            source.cancellation(),
            None,
        );
        tokio::pin!(projection);
        assert!(futures::poll!(projection.as_mut()).is_pending());
        assert!(
            source.checkout.state.try_write().is_ok(),
            "fact I/O retains no shared checkout guard"
        );
        source
            .checkout
            .install_change(
                winner,
                ViewChange::Node(right),
                None,
                &work,
                source.cancellation(),
                None,
            )
            .await?;
        objects.release();
        assert!(matches!(projection.await, Err(MountSourceError::Stale)));
        Ok(())
    })?;
    assert_eq!(
        source.open_file(&paths[0])?.read_up_to(0, 16)?.as_ref(),
        b"outside"
    );
    assert_eq!(
        source.open_file(&paths[1])?.read_up_to(0, 16)?.as_ref(),
        b"winner"
    );
    assert!(!source.node_unchanged_since_with_meter(left, facts.stamp, &work)?);
    source.runtime.wait(|| async {
        let candidate = staged_write(&source.checkout, left, b"dropped", &work).await?;
        objects.paused_reads.store(true, Ordering::Release);
        let mut projection = Box::pin(source.checkout.install_change(
            candidate,
            ViewChange::Node(left),
            Some(left),
            &work,
            source.cancellation(),
            None,
        ));
        assert!(futures::poll!(projection.as_mut()).is_pending());
        drop(projection);
        assert!(source.checkout.state.try_write().is_ok());
        assert!(source.checkout.publication_fence.try_write().is_ok());
        objects.release();
        Ok(())
    })?;

    let detached_path = native_test_path("detached");
    let created = source.create_file(&detached_path, metadata(), None, &work)?;
    let detached = source.detach_file(&detached_path)?;
    source.remove(&detached_path, Some(created.node.file_id), &work)?;
    let result = detached.write_range_with_meter(
        0,
        crate::kernel::OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"unlinked"), 0),
        true,
        &work,
    )?;
    assert_eq!((result.file_id, result.written), (created.node.file_id, 8));
    let detached_facts = result.facts.expect("detached authorized facts");
    assert_eq!(detached_facts.lookup, detached.lookup_with_meter(&work)?);
    assert_eq!(detached_facts.lookup.node.link_count, 0);
    assert!(source.lookup(&detached_path)?.is_none());

    use super::super::access::deny;
    use crate::access::Action;
    let append_path = native_test_path("append");
    source.create_file(&append_path, metadata(), None, &work)?;
    source.write_range(&append_path, 0, Bytes::from_static(b"old"))?;
    let source = bind_test_access(
        source,
        vec![
            deny(
                "private-inbox",
                &[Action::List, Action::Read, Action::Write, Action::Append],
                "inbox",
            ),
            deny("private-append", &[Action::List, Action::Write], "append"),
        ],
        None,
    )?;
    let inbox_path = native_test_path("inbox");
    let created = source.create_file(&inbox_path, metadata(), None, &work)?;
    let inbox = source.open_created(&inbox_path, &created)?;
    let result = inbox.write_range_with_meter(
        0,
        crate::kernel::OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"delivery"), 0),
        true,
        &work,
    )?;
    assert_eq!((result.file_id, result.written), (created.node.file_id, 8));
    assert!(result.facts.is_none(), "Create does not disclose metadata");
    let append = source.open_file(&append_path)?;
    let result = append.write_range_with_meter(
        3,
        crate::kernel::OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"!"), 0),
        true,
        &work,
    )?;
    assert_eq!(result.written, 1);
    assert!(result.facts.is_none(), "Append does not imply List");
    assert!(matches!(
        append.write_range_with_meter(
            0,
            crate::kernel::OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"bad"), 0),
            true,
            &work
        ),
        Err(MountSourceError::AccessDenied)
    ));
    assert_eq!(append.read_up_to(0, 8)?.as_ref(), b"old!");
    let stop = CancellationToken::new();
    let cancelled_work = capture_meter(BOUNDARY_BUDGET, &stop)?;
    stop.cancel();
    assert!(
        inbox
            .write_range_with_meter(
                0,
                crate::kernel::OwnedSourceChunk::intrinsic(OwnedBytes::from_static(b"lost"), 0),
                true,
                &cancelled_work
            )
            .is_err()
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
