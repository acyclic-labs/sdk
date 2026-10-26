use super::*;
use proptest::prelude::*;

async fn stage<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    transaction: &Transaction<A, O>,
    bytes: &[u8],
) -> Result<crate::StagedContent, Box<dyn Error>> {
    let mut source = bytes;
    Ok(unbounded!(
        transaction,
        stage_content(&mut source, (bytes.len() as u64).max(1))
    )?)
}

#[tokio::test]
async fn append_segments_preserve_prefix_and_share_one_publication() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("append-segments"))?;
    workspace.write_text("/log", "prefix:").await?;
    let before = unbounded!(workspace, head())?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let parts = [
        stage(&transaction, b"first").await?,
        stage(&transaction, b"").await?,
        stage(&transaction, b":second").await?,
    ];
    let appended = unbounded!(transaction, append_staged("/log", &parts))?;
    assert_eq!(
        appended,
        crate::ByteRange {
            offset: 7,
            length: 12
        }
    );
    assert_eq!(
        unbounded!(workspace, read("/log", 64))?.as_ref(),
        b"prefix:"
    );
    let TransactionCommit::Committed(after) = commit(&mut transaction).await? else {
        return Err("append batch did not commit".into());
    };
    assert_eq!(
        unbounded!(after, read("/log", 64))?.as_ref(),
        b"prefix:first:second"
    );
    assert_eq!(unbounded!(before, read("/log", 64))?.as_ref(), b"prefix:");
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::AlreadyCommitted(_)
    ));
    // Replacement uses the same implementation but must retain replace semantics.
    let mut replace = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(replace, write_staged("/log", &parts))?;
    assert!(matches!(
        commit(&mut replace).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/log", 64))?.as_ref(),
        b"first:second"
    );
    Ok(())
}

#[tokio::test]
async fn append_segments_create_and_extend_without_reading_the_existing_body()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("append-shared-body"))?;
    let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let prefix = vec![b'p'; 1_048_576];
    let body = stage(&seed, &prefix).await?;
    unbounded!(seed, append_staged("/log", &[body]))?;
    assert!(matches!(
        commit(&mut seed).await?,
        TransactionCommit::Committed(_)
    ));
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let suffix = stage(&transaction, b"suffix").await?;
    let appended = transaction
        .append_staged(
            "/log",
            &[suffix],
            WorkBudget {
                object_bytes_read: 131_072,
                ..WorkBudget::UNBOUNDED
            },
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        appended.value,
        crate::ByteRange {
            offset: prefix.len() as u64,
            length: 6
        }
    );
    assert!(appended.work.object_bytes_read < prefix.len() as u64);
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read_range("/log", prefix.len() as u64 - 1, 7))?
            .bytes
            .as_ref(),
        b"psuffix"
    );
    Ok(())
}

#[tokio::test]
async fn append_segments_reject_symlinks_and_denied_work_without_partial_edits()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("append-refusals"))?;
    workspace.write_text("/log", "original").await?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(
        transaction,
        create_symbolic_link("/link", OwnedBytes::from_static(b"log"))
    )?;
    unbounded!(transaction, create_directory("/directory"))?;
    let suffix = stage(&transaction, b"suffix").await?;
    for path in ["/link", "/directory"] {
        assert!(matches!(
            unbounded!(transaction, append_staged(path, &[suffix])),
            Err(WorkspaceError::NotRegularFile)
        ));
    }
    assert!(matches!(
        unbounded!(transaction, append_staged("/log", &[])),
        Err(WorkspaceError::EmptyContentSet)
    ));
    assert!(
        transaction
            .append_staged(
                "/log",
                &[suffix],
                WorkBudget::UNCHARGED,
                &CancellationToken::new()
            )
            .await
            .is_err()
    );
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        transaction
            .append_staged("/log", &[suffix], WorkBudget::UNBOUNDED, &cancel)
            .await
            .is_err()
    );
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/log", 32))?.as_ref(),
        b"original"
    );
    Ok(())
}

#[tokio::test]
async fn append_segments_conflict_on_the_same_eof_even_through_an_alias()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("append-eof-fence"))?;
    workspace.write_text("/log", "base").await?;
    let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(seed, hard_link("/log", "/alias"))?;
    commit(&mut seed).await?;
    let mut first = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let mut second = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let a = stage(&first, b"A").await?;
    let b = stage(&second, b"B").await?;
    unbounded!(first, append_staged("/log", &[a]))?;
    unbounded!(second, append_staged("/alias", &[b]))?;
    assert!(matches!(
        commit(&mut first).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        commit(&mut second).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert_eq!(unbounded!(workspace, read("/log", 16))?.as_ref(), b"baseA");
    assert_eq!(
        unbounded!(workspace, read("/alias", 16))?.as_ref(),
        b"baseA"
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn append_segments_retain_the_original_native_work() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("append-native"))?;
    workspace.write_text("/log", "prefix").await?;
    let cancel = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
        .map_err(|(error, _)| error)?;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), &original, &cancel)
        .await?;
    let mut source = b"suffix".as_slice();
    let suffix = transaction
        .stage_content(&mut source, 6, &original, &cancel)
        .await?;
    let appended = transaction
        .append_staged("/log", &[suffix], &original, &cancel)
        .await?;
    assert_eq!(
        appended,
        crate::ByteRange {
            offset: 6,
            length: 6
        }
    );
    let mut ready =
        |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), FsError>(()));
    assert!(matches!(
        transaction
            .commit_with_native_work::<FsError, _, _>(
                None,
                crate::PublicationPermit::Unrestricted,
                &original,
                &cancel,
                &mut ready,
            )
            .await?,
        TransactionCommit::Committed(_)
    ));
    assert!(original.receipt().items_examined > 0);
    assert_eq!(
        unbounded!(workspace, read("/log", 32))?.as_ref(),
        b"prefixsuffix"
    );
    Ok(())
}

#[test]
fn append_segments_equal_serial_concatenation() -> Result<(), Box<dyn Error>> {
    let mut runner =
        proptest::test_runner::TestRunner::new(proptest::test_runner::Config::with_cases(16));
    runner.run(
        &(
            proptest::collection::vec(any::<u8>(), 0..512),
            proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..128), 1..6),
        ),
        |(prefix, segments)| {
            let result: Result<(), Box<dyn Error>> = futures::executor::block_on(async {
                let fs = Fs::memory();
                let workspace = unbounded!(fs, create_workspace("append-serial"))?;
                workspace
                    .write("/log", bytes::Bytes::from(prefix.clone()))
                    .await?;
                let mut transaction =
                    unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
                let mut parts = Vec::new();
                let mut expected = prefix;
                for segment in segments {
                    parts.push(stage(&transaction, &segment).await?);
                    expected.extend(segment);
                }
                unbounded!(transaction, append_staged("/log", &parts))?;
                assert!(matches!(
                    commit(&mut transaction).await?,
                    TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_)
                ));
                assert_eq!(
                    unbounded!(workspace, read("/log", 2048))?.as_ref(),
                    expected
                );
                Ok(())
            });
            prop_assert!(result.is_ok(), "{result:?}");
            Ok(())
        },
    )?;
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
    // An existing file with only empty segments has no publication to acknowledge.
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

#[cfg(all(feature = "local", any(unix, windows)))]
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
