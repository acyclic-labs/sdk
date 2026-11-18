use super::*;

#[path = "../../../tests/support/append.rs"]
mod fixture;
use fixture::stage;

#[tokio::test]
async fn one_empty_append_seals_its_transaction_without_changing_file_bytes()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("append-one-empty"))?;
    workspace.write_text("/log", "prefix").await?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let empty = stage(&transaction, b"").await?;
    assert_eq!(
        unbounded!(transaction, append_staged("/log", &[empty]))?,
        crate::ByteRange {
            offset: 6,
            length: 0
        }
    );
    assert!(!transaction.checkout.has_pending_mutations());
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::AlreadyCommitted(_)
    ));
    assert_eq!(unbounded!(workspace, read("/log", 16))?.as_ref(), b"prefix");
    Ok(())
}

#[tokio::test]
async fn append_segments_empty_parts_and_repeated_appends_have_explicit_ranges()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("append-empty"))?;
    workspace.write_text("/existing", "base").await?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let empty = stage(&transaction, b"").await?;
    assert_eq!(
        unbounded!(transaction, append_staged("/existing", &[empty, empty]))?,
        crate::ByteRange {
            offset: 4,
            length: 0
        }
    );
    // Empty segments leave the authored tree unchanged.
    assert!(!transaction.checkout.has_pending_mutations());
    assert_eq!(
        unbounded!(transaction, append_staged("/new", &[empty]))?,
        crate::ByteRange {
            offset: 0,
            length: 0
        }
    );
    let a = stage(&transaction, b"A").await?;
    let b = stage(&transaction, b"B").await?;
    assert_eq!(
        unbounded!(transaction, append_staged("/existing", &[a]))?,
        crate::ByteRange {
            offset: 4,
            length: 1
        }
    );
    assert_eq!(
        unbounded!(transaction, append_staged("/existing", &[empty, b]))?,
        crate::ByteRange {
            offset: 5,
            length: 1
        }
    );
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/existing", 16))?.as_ref(),
        b"baseAB"
    );
    assert!(unbounded!(workspace, read("/new", 1))?.is_empty());
    Ok(())
}

#[tokio::test]
async fn append_segments_fence_same_length_alias_edits_and_delete_recreate()
-> Result<(), Box<dyn Error>> {
    for recreate in [false, true] {
        let fs = Fs::memory();
        let workspace = unbounded!(fs, create_workspace("append-record-fence"))?;
        workspace.write_text("/log", "base").await?;
        let mut links = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(links, hard_link("/log", "/alias"))?;
        commit(&mut links).await?;
        let mut appender = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        let suffix = stage(&appender, b"suffix").await?;
        unbounded!(appender, append_staged("/log", &[suffix]))?;
        let mut editor = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        if recreate {
            unbounded!(editor, remove("/log"))?;
            unbounded!(
                editor,
                create_file(
                    "/log",
                    OwnedBytes::from_static(b"next"),
                    FileMetadata::default()
                )
            )?;
        } else {
            unbounded!(
                editor,
                write_range("/alias", 0, OwnedBytes::from_static(b"next"))
            )?;
        }
        assert!(matches!(
            commit(&mut editor).await?,
            TransactionCommit::Committed(_)
        ));
        assert!(matches!(
            appender
                .rebase(16, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await?
                .value,
            TransactionRebase::Conflicted { .. }
        ));
        assert!(matches!(
            commit(&mut appender).await?,
            TransactionCommit::Conflict { .. }
        ));
        assert_eq!(unbounded!(workspace, read("/log", 32))?.as_ref(), b"next");
        assert_eq!(
            unbounded!(workspace, read("/alias", 32))?.as_ref(),
            if recreate { b"base" } else { b"next" }
        );
    }
    Ok(())
}

#[cfg(all(not(target_arch = "wasm32"), any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_segments_lost_reply_reopens_with_the_original_range_and_identity()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let key = IdempotencyKey::from_bytes([0x73; 16]);
    let (base, committed, file_id);
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let workspace = unbounded!(fs, create_workspace("append-reopen"))?;
        workspace.write_text("/log", "base").await?;
        base = unbounded!(workspace, head())?.id();
        file_id = unbounded!(workspace, stat("/log"))?.file_id;
        let mut transaction = unbounded!(workspace, begin_transaction(key))?;
        let suffix = stage(&transaction, b"suffix").await?;
        assert_eq!(
            unbounded!(transaction, append_staged("/log", &[suffix]))?,
            crate::ByteRange {
                offset: 4,
                length: 6
            }
        );
        // Drop the publication response before releasing the process-owned root.
        let TransactionCommit::Committed(result) = commit(&mut transaction).await? else {
            return Err("first append did not commit".into());
        };
        committed = result.id();
        let released = fs
            .local_root_release_barrier()
            .ok_or("missing local root custody")?;
        drop((result, transaction, workspace, fs));
        tokio::task::spawn_blocking(move || released.wait()).await?;
    }
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let workspace = unbounded!(fs, open_workspace("append-reopen"))?;
        let original = unbounded!(workspace, generation(base))?;
        let mut transaction = unbounded!(workspace, begin_transaction_at(&original, key))?;
        let suffix = stage(&transaction, b"suffix").await?;
        assert_eq!(
            unbounded!(transaction, append_staged("/log", &[suffix]))?,
            crate::ByteRange {
                offset: 4,
                length: 6
            }
        );
        let TransactionCommit::AlreadyCommitted(result) = commit(&mut transaction).await? else {
            return Err("lost reply did not resolve the original publication".into());
        };
        assert_eq!(result.id(), committed);
        assert_eq!(unbounded!(workspace, head())?.id(), committed);
        assert_eq!(unbounded!(workspace, stat("/log"))?.file_id, file_id);
        assert_eq!(
            unbounded!(workspace, read("/log", 32))?.as_ref(),
            b"basesuffix"
        );
        let released = fs
            .local_root_release_barrier()
            .ok_or("missing local root custody")?;
        drop((result, transaction, original, workspace, fs));
        tokio::task::spawn_blocking(move || released.wait()).await?;
    }
    Ok(())
}

#[tokio::test]
async fn append_segments_budget_cuts_leave_the_candidate_and_identity_unpublished()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("append-budget-cuts"))?;
    workspace.write_text("/log", "base").await?;
    let head = unbounded!(workspace, head())?.id();
    let mut probe = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let parts = [
        stage(&probe, b"one").await?,
        stage(&probe, b"two").await?,
        stage(&probe, b"three").await?,
    ];
    let full = probe
        .append_staged(
            "/log",
            &parts,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert!(full.work.items_examined > parts.len() as u64);
    let mut refused = 0;
    for limit in 0..full.work.items_examined {
        let key = IdempotencyKey::new();
        let mut transaction = unbounded!(workspace, begin_transaction(key))?;
        let root = transaction.checkout.root().clone();
        let result = transaction
            .append_staged(
                "/log",
                &parts,
                WorkBudget {
                    items_examined: limit,
                    ..WorkBudget::UNBOUNDED
                },
                &CancellationToken::new(),
            )
            .await;
        if result.is_err() {
            refused += 1;
            assert_eq!(
                transaction.checkout.root(),
                &root,
                "partial append at item budget {limit}"
            );
            assert!(!transaction.checkout.has_pending_mutations());
            assert!(unbounded!(workspace, operation_generation(key))?.is_none());
        }
        assert_eq!(unbounded!(workspace, head())?.id(), head);
    }
    assert!(refused > parts.len());
    assert_eq!(unbounded!(workspace, read("/log", 16))?.as_ref(), b"base");
    Ok(())
}
