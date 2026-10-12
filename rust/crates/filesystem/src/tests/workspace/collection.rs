use super::*;

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_local_fork_survives_power_loss_at_every_journal_cut() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let journal_path = root.join("stream").join("stream.journal");
    let key = IdempotencyKey::from_bytes([0x61; 16]);
    let source_id;
    {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = fs
            .create_workspace(
                "repo",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value;
        main.write_text("/base.txt", "base").await?;
        source_id = main
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value
            .id();
        drop(main);
        close_local(fs).await?;
    }
    let before = std::fs::metadata(&journal_path)?.len();
    let forked_head;
    {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = fs
            .open_workspace(
                "repo",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value;
        let source = main
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value;
        assert_eq!(source.id(), source_id);
        let fork = main
            .fork::<_, _, crate::WorkspaceError>(
                "agent",
                ForkOptions::from_generation(source, key),
                crate::PublicationPermit::Unrestricted,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                &mut |_| {
                    std::future::ready(Ok(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    }))
                },
            )
            .await?
            .value;
        forked_head = fork
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value
            .id();
        drop((main, fork));
        close_local(fs).await?;
    }
    let journal = std::fs::read(&journal_path)?;
    let before = usize::try_from(before)?;
    // Each frame the fork appended ends at a power-loss point, and a frame
    // torn anywhere, in its length, command, or checksum, is another. A
    // frame is a 4-byte length, the command, and a 32-byte checksum.
    let mut cuts = vec![before];
    let mut frame = before;
    while frame < journal.len() {
        let length = journal
            .get(frame..frame + 4)
            .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
            .map(u32::from_le_bytes)
            .ok_or("fork journal frame is truncated")?;
        let end = frame + 4 + usize::try_from(length)? + 32;
        cuts.extend([
            frame + 2,
            frame + 4 + usize::try_from(length)? / 2,
            end - 1,
            end,
        ]);
        frame = end;
    }
    assert_eq!(frame, journal.len(), "fork frames do not end the journal");
    // The fork of the published head is one commit.
    assert_eq!(cuts.len(), 1 + 4, "the fork did not append one frame");
    for length in cuts {
        cut_journal(root, &journal, length)?;
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = fs
            .open_workspace(
                "repo",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value;
        let source = main
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value;
        assert_eq!(source.id(), source_id);
        if let Ok(agent) = fs
            .open_workspace(
                "agent",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
        {
            assert_eq!(
                agent
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new()
                    )
                    .await?
                    .value
                    .id(),
                forked_head
            );
            assert_eq!(
                agent
                    .read(
                        "/base.txt",
                        16,
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new()
                    )
                    .await?
                    .value,
                OwnedBytes::from_static(b"base")
            );
        }
        let retried = main
            .fork::<_, _, crate::WorkspaceError>(
                "agent",
                ForkOptions::from_generation(source, key),
                crate::PublicationPermit::Unrestricted,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                &mut |_| {
                    std::future::ready(Ok(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    }))
                },
            )
            .await?
            .value;
        assert_eq!(
            retried
                .head(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new()
                )
                .await?
                .value
                .id(),
            forked_head
        );
        assert_eq!(
            retried
                .read(
                    "/base.txt",
                    16,
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new()
                )
                .await?
                .value,
            OwnedBytes::from_static(b"base")
        );
        drop((main, retried));
        close_local(fs).await?;
    }
    Ok(())
}

/// Deleting a local workspace releases its authority for good; a collection
/// then releases the fork base its creation retained and reclaims what only
/// the deleted workspace held, while its source reads as before.
#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_local_fork_releases_its_authority_and_content() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let cancellation = CancellationToken::new();
    {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = fs
            .create_workspace(
                "repo",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value;
        main.write_text("/base.txt", "base").await?;
        let base = main
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value;
        let agent = main
            .fork::<_, _, crate::WorkspaceError>(
                "agent",
                ForkOptions::from_generation(base, IdempotencyKey::new()),
                crate::PublicationPermit::Unrestricted,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                &mut |_| {
                    std::future::ready(Ok(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    }))
                },
            )
            .await?
            .value;
        agent
            .write_text("/agent.txt", &"only the agent ".repeat(8_192))
            .await?;
        assert_eq!(fs.authority().authorities(64).await?.len(), 3);
        let key = IdempotencyKey::new();
        assert_eq!(agent.delete(key).await?, WorkspaceDelete::Deleted);
        assert_eq!(agent.delete(key).await?, WorkspaceDelete::AlreadyDeleted);
        assert_eq!(
            fs.delete_workspace("agent", key).await?,
            WorkspaceDelete::AlreadyDeleted
        );
        assert!(
            fs.open_workspace(
                "agent",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
        );
        assert!(
            fs.create_workspace(
                "agent",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .is_err()
        );
        assert_eq!(
            fs.authority().authorities(64).await?.len(),
            2,
            "the fork's authority is gone; its base is still retained"
        );
        // The collection runs while the source workspace is open.
        let collected = fs.collect_local_garbage(None, &cancellation).await?;
        assert!(collected.removed >= 1, "{collected:?}");
        let again = fs.collect_local_garbage(None, &cancellation).await?;
        assert_eq!(again.removed, 0);
        assert_eq!(
            main.read(
                "/base.txt",
                64,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new()
            )
            .await?
            .value
            .as_ref(),
            b"base"
        );
        drop((main, agent));
        close_local(fs).await?;
    }

    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    assert_eq!(
        fs.authority().authorities(64).await?.len(),
        1,
        "the fork base went with its fork"
    );
    let main = fs
        .open_workspace(
            "repo",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    assert_eq!(
        main.read(
            "/base.txt",
            64,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )
        .await?
        .value
        .as_ref(),
        b"base"
    );
    // Content the collection removed is stored again when written again.
    let content = "only the agent ".repeat(8_192);
    main.write_text("/again.txt", &content).await?;
    drop(main);
    close_local(fs).await?;
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let main = fs
        .open_workspace(
            "repo",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    assert_eq!(
        main.read(
            "/again.txt",
            1 << 20,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )
        .await?
        .value
        .as_ref(),
        content.as_bytes()
    );
    Ok(())
}

/// Collections run back to back while a workspace keeps publishing and its
/// forks come and go: every publication succeeds, what the deleted forks
/// held is reclaimed, and everything live reads back after a reopen.
#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collections_run_alongside_publications() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let cancellation = CancellationToken::new();
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    fs.create_workspace(
        "repo",
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await?
    .value;
    let writer = tokio::spawn({
        let fs = fs.clone();
        async move {
            let main = fs
                .open_workspace(
                    "repo",
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)?;
            for round in 0..24_u32 {
                let TransactionCommit::Committed(generation) = main
                    .write_text("/churn.txt", &round.to_string().repeat(20_000))
                    .await?
                else {
                    return Err(WorkspaceError::Engine(
                        "churn fixture did not commit".to_owned(),
                    ));
                };
                generation
                    .pin(
                        format!("churn-{round}"),
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)?;
                let agent = main
                    .fork::<_, _, crate::WorkspaceError>(
                        &format!("agent-{round}"),
                        ForkOptions::from_generation(generation, IdempotencyKey::new()),
                        crate::PublicationPermit::Unrestricted,
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                        &mut |_| {
                            std::future::ready(Ok(crate::OperationReceipt {
                                value: (),
                                work: crate::WorkCounters::UNCHARGED,
                            }))
                        },
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)?;
                agent
                    .write_text("/agent.txt", &format!("agent {round} ").repeat(8_192))
                    .await?;
                agent.delete(IdempotencyKey::new()).await?;
            }
            Ok::<_, WorkspaceError>(())
        }
    });
    let mut removed = 0_u64;
    while !writer.is_finished() {
        removed += fs.collect_local_garbage(None, &cancellation).await?.removed;
    }
    writer.await??;
    removed += fs.collect_local_garbage(None, &cancellation).await?.removed;
    assert!(removed > 0);
    close_local(fs).await?;

    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let main = fs
        .open_workspace(
            "repo",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    assert_eq!(
        main.read(
            "/churn.txt",
            1 << 20,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new()
        )
        .await?
        .value
        .as_ref(),
        "23".repeat(20_000).as_bytes()
    );
    assert_eq!(
        fs.collect_local_garbage(None, &cancellation).await?.removed,
        0
    );
    Ok(())
}

/// A collection forgets a deleted workspace's core-state records unless a
/// live workspace descends from it.
#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_collection_forgets_a_deleted_workspaces_records() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let fs = Fs::local(crate::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await?;
    let store = crate::LocalCoreStateStore::open_owned(directory.path().join("core-state"))?;
    let graph = crate::WorkspaceGraph::new(store.clone());
    let main = fs
        .create_workspace(
            "repo",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    main.write_text("/base.txt", "base").await?;
    let mut children = Vec::new();
    for name in ["kept", "deleted"] {
        let base = main
            .head(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await?
            .value;
        let child = main
            .fork::<_, _, crate::WorkspaceError>(
                name,
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
                crate::PublicationPermit::Unrestricted,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                &mut |_| {
                    std::future::ready(Ok(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    }))
                },
            )
            .await?
            .value;
        graph
            .register_existing_child(&main, &child, base.id())
            .await?;
        children.push(child);
    }
    let deleted = children.pop().ok_or("deleted child")?;
    let kept = children.pop().ok_or("kept child")?;
    // A deleted parent of a live child keeps its records.
    let grandchild_base = kept
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let grandchild = kept
        .fork::<_, _, crate::WorkspaceError>(
            "grandchild",
            ForkOptions::from_generation(grandchild_base.clone(), IdempotencyKey::new()),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    graph
        .register_existing_child(&kept, &grandchild, grandchild_base.id())
        .await?;
    kept.delete(IdempotencyKey::new()).await?;
    deleted.delete(IdempotencyKey::new()).await?;
    let records = store.workspace_records().await?;
    assert!(records.contains_key(&deleted.id()) && records.contains_key(&kept.id()));

    fs.collect_local_garbage(Some(&store), &CancellationToken::new())
        .await?;
    let records = store.workspace_records().await?;
    assert!(!records.contains_key(&deleted.id()));
    assert!(
        records.contains_key(&kept.id()),
        "a live child descends from it"
    );
    assert!(records.contains_key(&grandchild.id()));
    Ok(())
}
