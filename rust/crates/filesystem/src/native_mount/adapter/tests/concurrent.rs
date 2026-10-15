#[test]
fn native_candidates_wait_for_adoption_and_cancel_without_blocking_successors()
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
        let mut first_carrier = NativeDemandCarrier::Native(&work);
        let mut first_candidate = source
            .checkout
            .candidate(
                None,
                &mut first_carrier,
                work.cancellation(),
                Some(source.cancellation()),
            )
            .await?;
        first_candidate
            .checkout
            .change_content(
                first.node.file_id,
                ContentChange::Write {
                    offset: 0,
                    bytes: Bytes::from_static(b"first").into(),
                },
                ContentTimes::Preserve,
                &work,
                work.cancellation(),
            )
            .await?;
        let cancelled = CancellationToken::new();
        let waiting_work = capture_meter(BOUNDARY_BUDGET, &cancelled)?;
        let mut carrier = NativeDemandCarrier::Native(&waiting_work);
        let waiting = source
            .checkout
            .candidate(None, &mut carrier, &cancelled, None);
        tokio::pin!(waiting);
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        cancelled.cancel();
        assert!(matches!(waiting.await, Err(MountSourceError::Cancelled)));
        let mut carrier = NativeDemandCarrier::Native(&work);
        let successor = source
            .checkout
            .candidate(None, &mut carrier, work.cancellation(), None);
        tokio::pin!(successor);
        assert!(futures::poll!(successor.as_mut()).is_pending());
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
        let mut successor = successor.await?;
        successor
            .checkout
            .change_content(
                second.node.file_id,
                ContentChange::Write {
                    offset: 0,
                    bytes: Bytes::from_static(b"second").into(),
                },
                ContentTimes::Preserve,
                &work,
                work.cancellation(),
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
        let mut carrier = NativeDemandCarrier::Native(&work);
        let mut candidate = source
            .checkout
            .candidate(None, &mut carrier, work.cancellation(), None)
            .await?;
        candidate
            .checkout
            .change_content(
                created.node.file_id,
                ContentChange::Write {
                    offset: 0,
                    bytes: Bytes::from_static(b"published").into(),
                },
                ContentTimes::Preserve,
                &work,
                work.cancellation(),
            )
            .await?;
        let publication = source.sync_async(crate::PublicationPermit::Unrestricted);
        tokio::pin!(publication);
        assert!(futures::poll!(publication.as_mut()).is_pending());
        // Install directly: the queued sync is the publication for this mutation.
        source
            .checkout
            .install_candidate(
                candidate,
                &mut carrier,
                work.cancellation(),
                None,
                |carrier| {
                    record_view_change(
                        &source.checkout.ledger,
                        &ViewChange::Node(created.node.file_id),
                        carrier,
                    )
                },
                |checkout, candidate, prepared| {
                    Ok(checkout.adopt_prepared_candidate(
                        &candidate.base,
                        candidate.checkout,
                        prepared,
                    ))
                },
            )
            .await?
            .ok_or(MountSourceError::Stale)?;
        publication.await
    })?;
    assert_eq!(file.read_up_to(0, 64)?.as_ref(), b"published");
    assert!(!checkout_state(&source).1);
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
