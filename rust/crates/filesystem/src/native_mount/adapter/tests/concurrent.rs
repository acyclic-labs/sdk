use super::*;

mod overlap;

async fn staged_write<'a, A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &'a SharedCheckout<A, O>,
    file: FileId,
    bytes: &'static [u8],
    work: &crate::NativeMaterializationWork,
) -> Result<CheckoutCandidate<'a, A, O>, MountSourceError> {
    let mut carrier = NativeDemandCarrier::Native(work);
    let mut candidate = checkout
        .candidate(None, &mut carrier, work.cancellation(), None)
        .await?;
    candidate
        .checkout
        .change_content(
            file,
            ContentChange::Write {
                offset: 0,
                bytes: Bytes::from_static(bytes).into(),
            },
            ContentTimes::Preserve,
            work,
            work.cancellation(),
        )
        .await?;
    Ok(candidate)
}

#[test]
fn native_candidates_merge_disjoint_changes_and_cancel_without_blocking_successors()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = sources(
        FilesystemProfile::Portable,
        ConsistencyMode::Pinned,
        MountPublication::Manual,
    )?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let paths = ["first", "second"].map(native_test_path);
    let first = source.create_file(&paths[0], metadata(), None, &work)?;
    let second = source.create_file(&paths[1], metadata(), None, &work)?;
    let handles = [source.open_file(&paths[0])?, source.open_file(&paths[1])?];
    source.runtime.wait(|| async {
        let first_candidate =
            staged_write(&source.checkout, first.node.file_id, b"first", &work).await?;
        let cancelled = CancellationToken::new();
        let waiting_work = capture_meter(BOUNDARY_BUDGET, &cancelled)?;
        let abandoned = staged_write(
            &source.checkout,
            second.node.file_id,
            b"cancelled",
            &waiting_work,
        )
        .await?;
        let held = source.checkout.lock().await;
        let waiting = source.checkout.install_change(
            abandoned,
            ViewChange::Node(second.node.file_id),
            &waiting_work,
            &cancelled,
            None,
        );
        tokio::pin!(waiting);
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        cancelled.cancel();
        assert!(matches!(waiting.await, Err(MountSourceError::Cancelled)));
        drop(held);
        assert!(handles[1].read_up_to(0, 64)?.is_empty());
        let successor =
            staged_write(&source.checkout, second.node.file_id, b"second", &work).await?;
        source
            .checkout
            .install_change(
                first_candidate,
                ViewChange::Node(first.node.file_id),
                &work,
                source.cancellation(),
                None,
            )
            .await?;
        source
            .checkout
            .install_change(
                successor,
                ViewChange::Node(second.node.file_id),
                &work,
                source.cancellation(),
                None,
            )
            .await
    })?;
    assert_eq!(handles[0].read_up_to(0, 64)?.as_ref(), b"first");
    assert_eq!(handles[1].read_up_to(0, 64)?.as_ref(), b"second");
    Ok(())
}

#[test]
fn native_publication_waits_for_in_flight_candidate() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let path = native_test_path("written-before-sync");
    let created = source.create_file(&path, metadata(), None, &work)?;
    let file = source.open_created(&path, &created)?;
    source.runtime.wait(|| async {
        let candidate =
            staged_write(&source.checkout, created.node.file_id, b"published", &work).await?;
        let publication = source.sync_async(crate::PublicationPermit::Unrestricted);
        tokio::pin!(publication);
        assert!(futures::poll!(publication.as_mut()).is_pending());
        source
            .checkout
            .install_change(
                candidate,
                ViewChange::Node(created.node.file_id),
                &work,
                source.cancellation(),
                None,
            )
            .await?;
        publication.await
    })?;
    assert_eq!(file.read_up_to(0, 64)?.as_ref(), b"published");
    assert!(!checkout_state(&source).1);
    Ok(())
}

#[test]
fn overlapping_native_writes_remain_fenced() -> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = sources(
        FilesystemProfile::Portable,
        ConsistencyMode::Pinned,
        MountPublication::Manual,
    )?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let path = native_test_path("overlap");
    let file = source
        .create_file(&path, metadata(), None, &work)?
        .node
        .file_id;
    source.runtime.wait(|| async {
        let first = staged_write(&source.checkout, file, b"first", &work).await?;
        let second = staged_write(&source.checkout, file, b"other", &work).await?;
        source
            .checkout
            .install_change(
                first,
                ViewChange::Node(file),
                &work,
                source.cancellation(),
                None,
            )
            .await?;
        assert!(matches!(
            source
                .checkout
                .install_change(
                    second,
                    ViewChange::Node(file),
                    &work,
                    source.cancellation(),
                    None
                )
                .await,
            Err(MountSourceError::Stale)
        ));
        Ok(())
    })?;
    assert_eq!(
        source.open_file(&path)?.read_up_to(0, 64)?.as_ref(),
        b"first"
    );
    Ok(())
}

#[test]
fn overlapping_native_creates_never_coalesce_file_identities()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = sources(
        FilesystemProfile::Portable,
        ConsistencyMode::Pinned,
        MountPublication::Manual,
    )?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let path = native_test_path("same-name");
    let namespace = source.path(&path)?;
    source.runtime.wait(|| async {
        let mut carrier = NativeDemandCarrier::Native(&work);
        let mut first = source
            .checkout
            .candidate(None, &mut carrier, work.cancellation(), None)
            .await?;
        let mut second = source
            .checkout
            .candidate(None, &mut carrier, work.cancellation(), None)
            .await?;
        for candidate in [&mut first, &mut second] {
            apply_authored(
                &mut candidate.checkout,
                &AuthoredMutation::CreateFile {
                    path: namespace.clone(),
                    metadata: metadata(),
                    bytes: OwnedBytes::new(),
                },
                &work,
            )
            .await?;
        }
        source
            .checkout
            .install_change(
                first,
                ViewChange::Bound(&namespace),
                &work,
                source.cancellation(),
                None,
            )
            .await?;
        assert!(matches!(
            source
                .checkout
                .install_change(
                    second,
                    ViewChange::Bound(&namespace),
                    &work,
                    source.cancellation(),
                    None
                )
                .await,
            Err(MountSourceError::Stale)
        ));
        Ok(())
    })?;
    assert!(source.lookup(&path)?.is_some());
    Ok(())
}

#[test]
fn parallel_clients_create_and_write_without_stale_handles()
-> Result<(), Box<dyn std::error::Error>> {
    for publication in [
        MountPublication::Manual,
        MountPublication::CloseAndSync,
        MountPublication::PerMutation,
    ] {
        let (first, second) = sources(
            FilesystemProfile::Portable,
            ConsistencyMode::Pinned,
            publication,
        )?;
        let sources = [Arc::new(first), Arc::new(second)];
        let start = std::sync::Barrier::new(4);
        std::thread::scope(|scope| {
            let workers = (0..4)
                .map(|client| {
                    let source = &sources[client % 2];
                    let start = &start;
                    scope.spawn(move || -> Result<(), MountSourceError> {
                        let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
                        start.wait();
                        for round in 0..8 {
                            let path = native_test_path(&format!("client-{client}-{round}"));
                            let created = source.create_file(&path, metadata(), None, &work)?;
                            let file = source.open_created(&path, &created)?;
                            file.write_range(0, Bytes::from_static(b"own file"))?;
                            let reopened = source.open_file(&path)?;
                            file.write_range(8, Bytes::from_static(b" tail"))?;
                            assert_eq!(file.lookup()?.node.file_id, created.node.file_id);
                            assert_eq!(reopened.read_up_to(0, 64)?.as_ref(), b"own file tail");
                            if publication == MountPublication::CloseAndSync {
                                source.sync()?;
                            }
                        }
                        Ok(())
                    })
                })
                .collect::<Vec<_>>();
            for worker in workers {
                worker.join().expect("parallel client panicked")?;
            }
            Ok::<_, MountSourceError>(())
        })?;
        sources[0].sync()?;
    }
    Ok(())
}
