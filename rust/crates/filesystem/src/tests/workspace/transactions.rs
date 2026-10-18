use super::*;

#[tokio::test]
async fn conditional_remove_preserves_typed_stale_identity() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("conditional-remove"))?;
    let mut create = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    create.write_text("/file.txt", "current").await?;
    assert!(matches!(
        commit(&mut create).await?,
        TransactionCommit::Committed(_)
    ));
    let actual = unbounded!(workspace, stat("/file.txt"))?.file_id;
    let mut stale = FileId::new();
    while stale == actual {
        stale = FileId::new();
    }
    let mut remove = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    assert!(matches!(
        unbounded!(remove, remove_if("/file.txt", stale)),
        Err(WorkspaceError::StaleIdentity)
    ));
    assert_eq!(
        unbounded!(workspace, read("/file.txt", 64))?.as_ref(),
        b"current"
    );
    Ok(())
}

#[tokio::test]
async fn transaction_is_atomic_idempotent_and_visible_to_forks() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("repo"))?;
    let key = IdempotencyKey::new();
    let mut transaction = unbounded!(workspace, begin_transaction(key))?;
    unbounded!(transaction, create_dir_all("/output/nested"))?;
    transaction
        .write_text("/output/nested/status", "ready")
        .await?;
    let TransactionCommit::Committed(generation) = commit(&mut transaction).await? else {
        return Err("first publication did not commit".into());
    };
    assert_eq!(
        unbounded!(workspace, read("/output/nested/status", 64))?,
        OwnedBytes::from_static(b"ready")
    );
    let reopened = unbounded!(workspace, generation(generation.id()))?;
    assert_eq!(
        unbounded!(reopened, read("/output/nested/status", 64))?,
        OwnedBytes::from_static(b"ready")
    );
    let fork = fork_workspace(
        &workspace,
        "copy",
        ForkOptions::from_generation(generation.clone(), IdempotencyKey::new()),
    )
    .await?;
    assert_eq!(
        unbounded!(fork, read("/output/nested/status", 64))?,
        OwnedBytes::from_static(b"ready")
    );

    let mut exact_retry = unbounded!(workspace, begin_transaction(key))?;
    unbounded!(exact_retry, create_dir_all("/output/nested"))?;
    exact_retry
        .write_text("/output/nested/status", "ready")
        .await?;
    assert!(matches!(
        commit(&mut exact_retry).await?,
        TransactionCommit::AlreadyCommitted(retried) if retried.id() == generation.id()
    ));

    let mut conflicting_retry = unbounded!(workspace, begin_transaction(key))?;
    conflicting_retry
        .write_text("/output/nested/status", "different")
        .await?;
    assert!(matches!(
        commit(&mut conflicting_retry).await?,
        TransactionCommit::IdempotencyConflict
    ));
    Ok(())
}

#[tokio::test]
async fn retained_transaction_rebases_disjoint_work_and_rejects_overlap()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("transaction-rebase"))?;

    let mut first = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let mut disjoint = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    first.write_text("/first", "one").await?;
    disjoint.write_text("/second", "two").await?;
    assert!(matches!(
        commit(&mut first).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        commit(&mut disjoint).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        unbounded!(disjoint, rebase(16))?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        commit(&mut disjoint).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/first", 8))?,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        unbounded!(workspace, read("/second", 8))?,
        OwnedBytes::from_static(b"two")
    );

    let mut winner = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let mut overlap = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    winner.write_text("/first", "winner").await?;
    overlap.write_text("/first", "loser").await?;
    assert!(matches!(
        commit(&mut winner).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        commit(&mut overlap).await?,
        TransactionCommit::Conflict { .. }
    ));
    let TransactionRebase::Conflicted {
        conflicts,
        truncated,
    } = unbounded!(overlap, rebase(16))?
    else {
        return Err("overlapping mutation rebased unsafely".into());
    };
    assert!(!conflicts.is_empty());
    assert!(!truncated);
    assert_eq!(
        unbounded!(workspace, read("/first", 8))?,
        OwnedBytes::from_static(b"winner")
    );
    Ok(())
}

#[tokio::test]
async fn exact_generation_transaction_is_stateless_and_rebases_safely() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("exact-transaction"))?;
    let base = unbounded!(workspace, head())?;

    workspace.write_text("/upstream", "one").await?;
    let mut transaction = unbounded!(
        workspace,
        begin_transaction_at(&base, IdempotencyKey::new())
    )?;
    transaction.write_text("/local", "two").await?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        unbounded!(transaction, rebase(16))?,
        TransactionRebase::Rebased(_)
    ));
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/upstream", 8))?,
        OwnedBytes::from_static(b"one")
    );
    assert_eq!(
        unbounded!(workspace, read("/local", 8))?,
        OwnedBytes::from_static(b"two")
    );

    let other = unbounded!(fs, create_workspace("other-exact-transaction"))?;
    let foreign = unbounded!(other, head())?;
    assert!(matches!(
        unbounded!(
            workspace,
            begin_transaction_at(&foreign, IdempotencyKey::new())
        ),
        Err(WorkspaceError::ForeignGeneration)
    ));
    Ok(())
}

#[tokio::test]
async fn transaction_preserves_links_sparse_ranges_metadata_and_cow_clones()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("shapes"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(transaction, create_directory("/tree"))?;
    unbounded!(
        transaction,
        write("/tree/source", OwnedBytes::from_static(b"abcdef"))
    )?;
    unbounded!(
        transaction,
        write("/tree/destination", OwnedBytes::from_static(b"......"))
    )?;
    unbounded!(
        transaction,
        create_symbolic_link("/tree/symlink", OwnedBytes::from_static(b"source"))
    )?;
    unbounded!(transaction, hard_link("/tree/source", "/tree/hard-link"))?;
    unbounded!(
        transaction,
        write_range("/tree/source", 1, OwnedBytes::from_static(b"Z"))
    )?;
    unbounded!(
        transaction,
        zero_range(
            "/tree/source",
            crate::ByteRange {
                offset: 2,
                length: 2,
            },
            false,
            false
        )
    )?;
    unbounded!(
        transaction,
        preallocate(
            "/tree/source",
            crate::ByteRange {
                offset: 0,
                length: 6,
            },
            true
        )
    )?;
    unbounded!(
        transaction,
        clone_range("/tree/source", 0, "/tree/destination", 1, 5)
    )?;
    unbounded!(
        transaction,
        set_metadata("/tree/source", FileMetadata::default())
    )?;
    unbounded!(transaction, resize("/tree/destination", 8))?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    let source = OwnedBytes::from_static(b"aZ\0\0ef");
    assert_eq!(unbounded!(workspace, read("/tree/source", 32))?, source);
    assert_eq!(unbounded!(workspace, read("/tree/hard-link", 32))?, source);
    assert_eq!(
        unbounded!(workspace, read("/tree/destination", 32))?,
        OwnedBytes::from_static(b".aZ\0\0e\0\0")
    );
    Ok(())
}

#[tokio::test]
async fn transaction_rename_follows_posix_kind_rules() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("rename-kinds"))?;
    let mut setup = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(setup, create_dir_all("/a"))?;
    unbounded!(setup, create_dir_all("/e"))?;
    unbounded!(setup, create_dir_all("/full"))?;
    unbounded!(setup, write("/full/x", OwnedBytes::from_static(b"x")))?;
    unbounded!(
        setup,
        create_symbolic_link("/d", OwnedBytes::from_static(b"/\x08"))
    )?;
    unbounded!(setup, write("/f", OwnedBytes::from_static(b"file")))?;
    let TransactionCommit::Committed(_) = commit(&mut setup).await? else {
        return Err("setup did not commit".into());
    };
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    assert!(matches!(
        unbounded!(transaction, rename("/a", "/d", true)),
        Err(WorkspaceError::NotDirectory)
    ));
    assert!(matches!(
        unbounded!(transaction, rename("/a", "/f", true)),
        Err(WorkspaceError::NotDirectory)
    ));
    assert!(matches!(
        unbounded!(transaction, rename("/d", "/a", true)),
        Err(WorkspaceError::IsDirectory)
    ));
    assert!(matches!(
        unbounded!(transaction, rename("/f", "/a", true)),
        Err(WorkspaceError::IsDirectory)
    ));
    // Nor does anything replace a directory that is not empty (`ENOTEMPTY`).
    assert!(matches!(
        unbounded!(transaction, rename("/e", "/full", true)),
        Err(WorkspaceError::DirectoryNotEmpty)
    ));
    unbounded!(transaction, rename("/a", "/a", true))?;
    unbounded!(transaction, rename("/a", "/a", false))
        .err()
        .ok_or("RENAME_NOREPLACE onto itself succeeded")?;
    drop(transaction);
    let end = unbounded!(workspace, head())?;
    assert_eq!(unbounded!(end, stat("/a"))?.kind, FileKind::Directory);
    assert_eq!(unbounded!(end, stat("/d"))?.kind, FileKind::SymbolicLink);
    assert_eq!(unbounded!(end, stat("/f"))?.kind, FileKind::Regular);
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // mkdir observes prefix bindings, not directory contents, whether it is
    // staged alone or in a batch. Explicit directory observations and replaced
    // bindings must still prevent a safe rebase.
    #[test]
    fn create_dirs_rebase_preserves_exact_bindings(
        name in "entry[a-z]{1,4}",
        payload in proptest::collection::vec(proptest::prelude::any::<u8>(), 1..17),
    ) {
        #[derive(Clone, Copy, Debug)]
        enum WriteMode {
            Serial,
            #[cfg(not(target_arch = "wasm32"))]
            Batched,
        }
        let result: Result<(), Box<dyn Error>> = futures::executor::block_on(async {
            for mode in [
                WriteMode::Serial,
                #[cfg(not(target_arch = "wasm32"))]
                WriteMode::Batched,
            ] {
            for inside in [false, true] {
            for observe_directory in [false, true] {
                for replace_prefix in [false, true] {
                    if inside && replace_prefix {
                        continue;
                    }
                    // The other writer adds a sibling under this directory.
                    let (directory, components): (&str, &[&[u8]]) = if inside {
                        ("/shared/nested", &[b"shared", b"nested"])
                    } else {
                        ("/shared", &[b"shared"])
                    };
                    let sibling = format!("{directory}/{name}");
                    let fs = Fs::memory();
                    let workspace = unbounded!(fs, create_workspace("mkdir-dependencies"))?;
                    let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
                    unbounded!(seed, create_dir_all("/shared/nested"))?;
                    commit(&mut seed).await?;
                    let mut pending = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
                    if observe_directory {
                        let limits = pending.checkout.volume_config().limits;
                        let path = crate::kernel::NamespacePath::new(components.iter().map(|component| crate::kernel::LogicalName::new(
                            crate::kernel::NameEncoding::Utf8, component.to_vec(), limits.maximum_component_bytes,
                        )).collect::<Result<_, _>>()?, limits)?;
                        unbounded!(pending.checkout, lookup_no_follow(&path))?;
                    }
                    match mode {
                        WriteMode::Serial => {
                            unbounded!(pending, create_dir_all("/shared/nested"))?;
                            pending.write_text("/shared/nested/local", "local").await?;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        WriteMode::Batched => {
                            unbounded!(pending, write_batch(&["/shared/nested"], &[("/shared/nested/local", OwnedBytes::copy_from_slice(b"local"))]))?;
                        }
                    }
                    let mut other = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
                    if replace_prefix {
                        unbounded!(other, remove("/shared/nested"))?;
                        other.write_text("/shared/nested", "replacement").await?;
                    }
                    unbounded!(other, write(&sibling, OwnedBytes::copy_from_slice(&payload)))?;
                    assert!(matches!(commit(&mut other).await?, TransactionCommit::Committed(_)));
                    // Negative control: publishing the stale original candidate
                    // without a dependency-checked rebase must fail.
                    assert!(matches!(commit(&mut pending).await?, TransactionCommit::Conflict { .. }));
                    let rebased = unbounded!(pending, rebase(1))?;
                    if observe_directory || replace_prefix {
                        assert!(matches!(rebased, TransactionRebase::Conflicted { .. }));
                    } else {
                        assert!(
                            matches!(rebased, TransactionRebase::Rebased(_)),
                            "mode={mode:?} inside={inside}"
                        );
                        assert!(matches!(commit(&mut pending).await?, TransactionCommit::Committed(_)));
                        assert_eq!(unbounded!(workspace, read("/shared/nested/local", 16))?.as_ref(), b"local");
                    }
                    assert_eq!(unbounded!(workspace, read(&sibling, 32))?.as_ref(), payload.as_slice());
                }
            }
            }
            }
            Ok(())
        });
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }
}

// Both writers stage against exactly the same generation, then publish in a
// fixed order. No scheduler timing is needed to reproduce the host upload race.
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn shared_directory_writers_rebase_only_semantically_disjoint_batches()
-> Result<(), Box<dyn Error>> {
    #[derive(Clone, Copy, Debug)]
    enum WriteMode {
        Serial,
        Batched,
        PendingBatch,
    }
    for mode in [
        WriteMode::Serial,
        WriteMode::Batched,
        WriteMode::PendingBatch,
    ] {
        let batched = !matches!(mode, WriteMode::Serial);
        for observe_directory in [false, true] {
            for replace_prefix in [false, true] {
                let fs = Fs::memory();
                let workspace = unbounded!(fs, create_workspace("shared-directory-writers"))?;
                let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
                unbounded!(seed, create_dir_all("/.system/harness-uploads"))?;
                commit(&mut seed).await?;
                let mut pending = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
                let mut winner = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
                if matches!(mode, WriteMode::PendingBatch) {
                    // A prior mutation forces write_batch's serial fallback.
                    pending.write_text("/staged", "pending").await?;
                }
                if observe_directory {
                    let limits = pending.checkout.volume_config().limits;
                    let path = crate::kernel::NamespacePath::new(
                        [".system", "harness-uploads"]
                            .into_iter()
                            .map(|name| {
                                crate::kernel::LogicalName::new(
                                    crate::kernel::NameEncoding::Utf8,
                                    name.as_bytes().to_vec(),
                                    limits.maximum_component_bytes,
                                )
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                        limits,
                    )?;
                    unbounded!(pending.checkout, lookup_no_follow(&path))?;
                }
                if batched {
                    unbounded!(
                        pending,
                        write_batch(
                            &["/.system/harness-uploads"],
                            &[(
                                "/.system/harness-uploads/local",
                                OwnedBytes::from_static(b"local")
                            )]
                        )
                    )?;
                } else {
                    unbounded!(pending, create_dir_all("/.system/harness-uploads"))?;
                    pending
                        .write_text("/.system/harness-uploads/local", "local")
                        .await?;
                }
                if replace_prefix {
                    unbounded!(winner, remove("/.system/harness-uploads"))?;
                    winner
                        .write_text("/.system/harness-uploads", "replacement")
                        .await?;
                } else {
                    unbounded!(
                        winner,
                        write_batch(
                            &["/.system/harness-uploads"],
                            &[(
                                "/.system/harness-uploads/remote",
                                OwnedBytes::from_static(b"remote")
                            )]
                        )
                    )?;
                }
                assert!(matches!(
                    commit(&mut winner).await?,
                    TransactionCommit::Committed(_)
                ));
                assert!(matches!(
                    commit(&mut pending).await?,
                    TransactionCommit::Conflict { .. }
                ));
                let outcome = unbounded!(pending, rebase(16))?;
                if observe_directory || replace_prefix {
                    assert!(
                        matches!(outcome, TransactionRebase::Conflicted { .. }),
                        "batched={batched}, observed={observe_directory}, replaced={replace_prefix}"
                    );
                } else {
                    assert!(
                        matches!(outcome, TransactionRebase::Rebased(_)),
                        "mode={mode:?} failed to rebase disjoint sibling writes"
                    );
                    assert!(matches!(
                        commit(&mut pending).await?,
                        TransactionCommit::Committed(_)
                    ));
                    assert_eq!(
                        unbounded!(workspace, read("/.system/harness-uploads/local", 16))?.as_ref(),
                        b"local"
                    );
                    assert_eq!(
                        unbounded!(workspace, read("/.system/harness-uploads/remote", 16))?
                            .as_ref(),
                        b"remote"
                    );
                }
            }
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn directory_batches_still_observe_replaced_file_contents() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("batch-file-observation"))?;
    let mut seed = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(seed, create_dir_all("/shared"))?;
    seed.write_text("/shared/file", "base").await?;
    commit(&mut seed).await?;
    let mut pending = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(
        pending,
        write_batch(
            &["/shared"],
            &[("/shared/file", OwnedBytes::from_static(b"replacement"))]
        )
    )?;
    workspace.write_text("/shared/file", "concurrent").await?;
    assert!(matches!(
        commit(&mut pending).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert!(matches!(
        unbounded!(pending, rebase(16))?,
        TransactionRebase::Conflicted { .. }
    ));
    assert_eq!(
        unbounded!(workspace, read("/shared/file", 16))?.as_ref(),
        b"concurrent"
    );
    Ok(())
}
