use super::*;

#[tokio::test]
async fn native_continuation_replays_consumed_entries_after_budget_denial() -> TestResult {
    let directory = tempfile::tempdir()?;
    for index in 0..8 {
        std::fs::write(directory.path().join(format!("entry-{index}")), b"body")?;
    }
    let source = open_source(directory.path()).await?;
    let path = requested("")?;
    let token = CancellationToken::new();
    let budget = FINITE_BUDGET;
    let mut first = source
        .list_page(source.reference(), &path, None, 1, budget, &token)
        .await?;
    {
        let allowance = first.work.remaining(budget)?;
        let cancellation = &token;
        let mut accounting = allowance.accounting(cancellation);
        let result = match first.value.publication.as_deref_mut() {
            Some(publication) => publication.accept(&mut accounting),
            None => Ok(()),
        };
        allowance.finish(result, &accounting)
    }?;
    let cursor = first.value.next.ok_or("expected continuation")?;
    let mut limited = budget;
    limited.source_entries_visited = 0;
    let failure = source
        .list_page(
            source.reference(),
            &path,
            Some(cursor.clone()),
            2,
            limited,
            &token,
        )
        .await
        .err()
        .ok_or("cursor advanced beyond its source-item allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.source_entries_visited, 0);
    failure.work.verify(limited)?;
    let mut names = first
        .value
        .entries
        .iter()
        .map(|entry| entry.name.as_bytes().to_vec())
        .collect::<Vec<_>>();
    let mut next = Some(cursor);
    while let Some(cursor) = next {
        let mut page = source
            .list_page(source.reference(), &path, Some(cursor), 2, budget, &token)
            .await?;
        page.work.verify(budget)?;
        {
            let allowance = page.work.remaining(budget)?;
            let cancellation = &token;
            let mut accounting = allowance.accounting(cancellation);
            let result = match page.value.publication.as_deref_mut() {
                Some(publication) => publication.accept(&mut accounting),
                None => Ok(()),
            };
            allowance.finish(result, &accounting)
        }?;
        names.extend(
            page.value
                .entries
                .iter()
                .map(|entry| entry.name.as_bytes().to_vec()),
        );
        next = page.value.next;
    }
    names.sort();
    let expected = (0..8)
        .map(|index| format!("entry-{index}").into_bytes())
        .collect::<Vec<_>>();
    assert_eq!(names, expected);
    Ok(())
}

#[tokio::test]
async fn filtered_page_reserves_comparisons_before_committing_native_cursor() -> TestResult {
    let directory = tempfile::tempdir()?;
    for name in ["a", "b", "c"] {
        std::fs::write(directory.path().join(name), b"actual")?;
    }
    let native = NativeDemandSource::open(
        std::borrow::Cow::Owned(std::path::Path::new(directory.path()).to_path_buf()),
        FilesystemProfile::Portable,
        VolumeLimits::default(),
        None,
        None,
        acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)?;
    let source = native
        .exclude(
            vec![requested("private")?, requested("excluded")?],
            &WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )?
        .value;
    let root = requested("")?;
    let token = CancellationToken::new();
    let budget = FINITE_BUDGET;
    let mut first = source
        .list_page(source.reference(), &root, None, 1, budget, &token)
        .await?;
    {
        let allowance = first.work.remaining(budget)?;
        let cancellation = &token;
        let mut accounting = allowance.accounting(cancellation);
        let result = match first.value.publication.as_deref_mut() {
            Some(publication) => publication.accept(&mut accounting),
            None => Ok(()),
        };
        allowance.finish(result, &accounting)
    }?;
    let cursor = first.value.next.ok_or("native continuation absent")?;
    let mut denied = budget;
    denied.items_examined = 2;
    let failure = source
        .list_page(
            source.reference(),
            &root,
            Some(cursor.clone()),
            8,
            denied,
            &token,
        )
        .await
        .err()
        .ok_or("filter advanced native cursor without projection allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.source_entries_visited, 0);
    assert_eq!(failure.work.allocation_operations, 0);
    failure.work.verify(denied)?;
    let mut next = source
        .list_page(source.reference(), &root, Some(cursor), 8, budget, &token)
        .await?;
    {
        let allowance = next.work.remaining(budget)?;
        let cancellation = &token;
        let mut accounting = allowance.accounting(cancellation);
        let result = match next.value.publication.as_deref_mut() {
            Some(publication) => publication.accept(&mut accounting),
            None => Ok(()),
        };
        allowance.finish(result, &accounting)
    }?;
    next.work.verify(budget)?;
    assert!(next.value.next.is_none());
    let mut names = first
        .value
        .entries
        .iter()
        .chain(next.value.entries.iter())
        .map(|entry| entry.name.as_bytes().to_vec())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    Ok(())
}

#[tokio::test]
async fn native_unpublished_pages_restore_initial_and_continuation_positions() -> TestResult {
    let directory = tempfile::tempdir()?;
    for name in ["a", "b", "c", "d"] {
        std::fs::write(directory.path().join(name), b"body")?;
    }
    let source = open_source(directory.path()).await?;
    let root = requested("")?;
    let token = CancellationToken::new();
    let budget = FINITE_BUDGET;
    let mut position = None;
    for _ in 0..2 {
        let page = source
            .list_page(
                source.reference(),
                &root,
                position.clone(),
                1,
                budget,
                &token,
            )
            .await?;
        page.work.verify(budget)?;
        assert_eq!(page.work.source_bytes_read, 0);
        assert!(page.value.retained_bytes() > 0);
        let expected = page
            .value
            .entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();
        drop(page);
        let mut retry = source
            .list_page(
                source.reference(),
                &root,
                position.clone(),
                1,
                budget,
                &token,
            )
            .await?;
        assert_eq!(
            retry
                .value
                .entries
                .iter()
                .map(|entry| entry.name.clone())
                .collect::<Vec<_>>(),
            expected
        );
        let aborted = {
            let allowance = retry.work.remaining(budget)?;
            let cancellation = &CancellationToken::new();
            let mut accounting = allowance.accounting(cancellation);
            let result = match retry.value.publication.as_deref_mut() {
                Some(publication) => publication.abort(&mut accounting),
                None => Ok(()),
            };
            allowance.finish(result, &accounting)
        }?;
        aborted.work.verify(retry.work.remaining(budget)?)?;
        retry.work.checked_add(aborted.work)?.verify(budget)?;
        let mut accepted = source
            .list_page(
                source.reference(),
                &root,
                position.clone(),
                1,
                budget,
                &token,
            )
            .await?;
        assert_eq!(
            accepted
                .value
                .entries
                .iter()
                .map(|entry| entry.name.clone())
                .collect::<Vec<_>>(),
            expected
        );
        let committed = {
            let allowance = accepted.work.remaining(budget)?;
            let cancellation = &token;
            let mut accounting = allowance.accounting(cancellation);
            let result = match accepted.value.publication.as_deref_mut() {
                Some(publication) => publication.accept(&mut accounting),
                None => Ok(()),
            };
            allowance.finish(result, &accounting)
        }?;
        accepted.work.checked_add(committed.work)?.verify(budget)?;
        position = accepted.value.next;
        assert!(position.is_some());
    }
    Ok(())
}

#[tokio::test]
async fn native_late_publication_cancellation_and_cleanup_denial_preserve_retry() -> TestResult {
    let directory = tempfile::tempdir()?;
    for name in ["a", "b", "c"] {
        std::fs::write(directory.path().join(name), b"actual")?;
    }
    let source = open_source(directory.path()).await?;
    let root = requested("")?;
    let live = CancellationToken::new();
    let budget = FINITE_BUDGET;
    for cancelled in [false, true] {
        let mut page = source
            .list_page(source.reference(), &root, None, 2, budget, &live)
            .await?;
        assert!(page.work.backend_read_operations > 0);
        assert!(page.work.items_examined > 0);
        assert!(page.work.allocation_operations > 0);
        page.work.verify(budget)?;
        let expected = page
            .value
            .entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>();
        let projected = CancellationToken::new();
        let mut remaining = page.work.remaining(budget)?;
        if cancelled {
            projected.cancel();
        } else {
            remaining.items_examined = 0;
        }
        let failure = {
            let allowance = remaining;
            let cancellation = &projected;
            let mut accounting = allowance.accounting(cancellation);
            let result = match page.value.publication.as_deref_mut() {
                Some(publication) => publication.accept(&mut accounting),
                None => Ok(()),
            };
            allowance.finish(result, &accounting)
        }
        .err()
        .ok_or("unadmitted publication committed native cursor")?;
        if cancelled {
            assert!(matches!(failure.error, DemandError::Cancelled));
        } else {
            assert!(matches!(failure.error, DemandError::Work(_)));
        }
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.allocation_operations, 0);
        failure.work.verify(remaining)?;
        // The original source receipt remains observable after late projection failure.
        assert!(page.work.backend_read_operations > 0);
        let mut cleanup_budget = page.work.remaining(budget)?;
        cleanup_budget.items_examined = 1;
        let aborted = {
            let allowance = cleanup_budget;
            let cancellation = &CancellationToken::new();
            let mut accounting = allowance.accounting(cancellation);
            let result = match page.value.publication.as_deref_mut() {
                Some(publication) => publication.abort(&mut accounting),
                None => Ok(()),
            };
            allowance.finish(result, &accounting)
        }?;
        aborted.work.verify(cleanup_budget)?;
        let retained = page
            .work
            .checked_add(*failure.work)?
            .checked_add(aborted.work)?;
        retained.verify(budget)?;
        let mut retry = source
            .list_page(source.reference(), &root, None, 2, budget, &live)
            .await?;
        assert_eq!(
            retry
                .value
                .entries
                .iter()
                .map(|entry| entry.name.clone())
                .collect::<Vec<_>>(),
            expected
        );
        {
            let allowance = retry.work.remaining(budget)?;
            let cancellation = &CancellationToken::new();
            let mut accounting = allowance.accounting(cancellation);
            let result = match retry.value.publication.as_deref_mut() {
                Some(publication) => publication.abort(&mut accounting),
                None => Ok(()),
            };
            allowance.finish(result, &accounting)
        }?;
    }
    let mut committed = source
        .list_page(source.reference(), &root, None, 2, budget, &live)
        .await?;
    let mut cleanup_budget = committed.work.remaining(budget)?;
    cleanup_budget.items_examined = 1;
    let accepted = {
        let allowance = cleanup_budget;
        let cancellation = &live;
        let mut accounting = allowance.accounting(cancellation);
        let result = match committed.value.publication.as_deref_mut() {
            Some(publication) => publication.accept(&mut accounting),
            None => Ok(()),
        };
        allowance.finish(result, &accounting)
    }?;
    accepted.work.verify(cleanup_budget)?;
    committed.work.checked_add(accepted.work)?.verify(budget)?;
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn native_filter_admission_precedes_request_transport_custody() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    let source = open_source(directory.path())
        .await?
        .exclude(
            vec![requested("private")?, requested("excluded")?],
            &FINITE_BUDGET,
            &CancellationToken::new(),
        )?
        .value;
    let root = requested("")?;
    for maximum in [0, 4097, 8] {
        let token = CancellationToken::new();
        let budget = WorkBudget {
            items_examined: 2,
            ..FINITE_BUDGET
        };
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &token)
            .map_err(|(error, _)| error)?;
        let before = work.receipt();
        let live = work.live_allocation_bytes();
        let failure = source
            .list_page(source.reference(), &root, None, maximum, &work, &token)
            .await
            .err()
            .ok_or("unadmitted directory request reached transport")?;
        if maximum == 8 {
            assert!(matches!(failure, DemandError::Work(_)));
        } else {
            assert!(matches!(failure, DemandError::InvalidRequest));
        }
        assert_eq!(work.receipt(), before);
        assert_eq!(work.live_allocation_bytes(), live);
    }
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn request_transport_copy_denial_retires_and_reports_its_completed_prefix() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    let source = open_source(directory.path()).await?;
    let path = requested("child")?;
    let token = CancellationToken::new();
    let budget = WorkBudget {
        bytes_copied: 0,
        ..FINITE_BUDGET
    };
    let failure = source
        .list_page(source.reference(), &path, None, 1, budget, &token)
        .await
        .err()
        .ok_or("path copy was not denied")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.allocation_operations, 1);
    assert_eq!(
        failure.work.peak_allocation_bytes,
        std::mem::size_of::<crate::source_watch::CallbackWork>() as u64
    );
    assert_eq!(failure.work.bytes_copied, 0);
    assert_eq!(failure.work.backend_read_operations, 0);
    assert_eq!(failure.work.backend_write_operations, 0);
    failure.work.verify(budget)?;
    let work =
        acyclic_fs::NativeMaterializationWork::new(budget, &token).map_err(|(error, _)| error)?;
    let before = work.receipt();
    let live = work.live_allocation_bytes();
    let failure = source
        .list_page(source.reference(), &path, None, 1, &work, &token)
        .await
        .err()
        .ok_or("native path copy was not denied")?;
    assert!(matches!(failure, DemandError::Work(_)));
    assert_eq!(work.live_allocation_bytes(), live);
    assert_eq!(
        work.receipt().allocation_operations - before.allocation_operations,
        1
    );
    assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
    assert_eq!(
        work.receipt().backend_read_operations,
        before.backend_read_operations
    );
    assert_eq!(
        work.receipt().backend_write_operations,
        before.backend_write_operations
    );
    Ok(())
}

#[cfg(feature = "native-mount")]
#[tokio::test]
async fn request_semantic_denial_preserves_an_existing_original_ledger_failure() -> TestResult {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    let source = open_source(directory.path()).await?;
    let root = requested("")?;
    for maximum in [0, 1] {
        for cancelled in [false, true] {
            let token = CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(FINITE_BUDGET, &token)
                .map_err(|(error, _)| error)?;
            work.with_original_leaf(|leaf| leaf.poison_native_original_work());
            let before = work.receipt();
            if cancelled {
                token.cancel();
            }
            let failure = source
                .list_page(source.reference(), &root, None, maximum, &work, &token)
                .await
                .err()
                .ok_or("poisoned request was admitted")?;
            assert!(matches!(failure, DemandError::OriginalWorkPoisoned));
            assert_eq!(work.receipt(), before);
        }
    }
    Ok(())
}
