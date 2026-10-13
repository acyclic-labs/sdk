use super::*;

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_local_fork_releases_its_authority_and_content() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let cancellation = CancellationToken::new();
    {
        let fs = Fs::local(crate::LocalOptions::new(root)).await?;
        let main = unbounded!(fs, create_workspace("repo"))?;
        main.write_text("/base.txt", "base").await?;
        let base = unbounded!(main, head())?;
        let agent = fork_workspace(
            &main,
            "agent",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        )
        .await?;
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
        assert!(unbounded!(fs, open_workspace("agent")).is_err());
        assert!(unbounded!(fs, create_workspace("agent")).is_err());
        assert_eq!(
            fs.authority().authorities(64).await?.len(),
            2,
            "the fork's authority is gone; its base is still retained"
        );
        let collected = fs.collect_local_garbage(None, &cancellation).await?;
        assert!(collected.removed >= 1, "{collected:?}");
        let again = fs.collect_local_garbage(None, &cancellation).await?;
        assert_eq!(again.removed, 0);
        assert_eq!(unbounded!(main, read("/base.txt", 64))?, "base");
        drop((main, agent));
        close_local(fs).await?;
    }

    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    assert_eq!(
        fs.authority().authorities(64).await?.len(),
        1,
        "the fork base went with its fork"
    );
    let main = unbounded!(fs, open_workspace("repo"))?;
    assert_eq!(unbounded!(main, read("/base.txt", 64))?, "base");
    let content = "only the agent ".repeat(8_192);
    main.write_text("/again.txt", &content).await?;
    drop(main);
    close_local(fs).await?;
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let main = unbounded!(fs, open_workspace("repo"))?;
    assert_eq!(
        unbounded!(main, read("/again.txt", 1 << 20))?,
        content.as_str()
    );
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collections_run_alongside_publications() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let cancellation = CancellationToken::new();
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    unbounded!(fs, create_workspace("repo"))?;
    let writer = tokio::spawn({
        let fs = fs.clone();
        async move {
            let main = unbounded!(fs, open_workspace("repo"))?;
            for round in 0..24_u32 {
                main.write_text("/churn.txt", &round.to_string().repeat(20_000))
                    .await?;
                let agent = fork_workspace(
                    &main,
                    &format!("agent-{round}"),
                    ForkOptions::from_generation(unbounded!(main, head())?, IdempotencyKey::new()),
                )
                .await?;
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
    let main = unbounded!(fs, open_workspace("repo"))?;
    assert_eq!(
        unbounded!(main, read("/churn.txt", 1 << 20))?,
        "23".repeat(20_000).as_str()
    );
    assert_eq!(
        fs.collect_local_garbage(None, &cancellation).await?.removed,
        0
    );
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_collection_forgets_a_deleted_workspaces_records() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let fs = Fs::local(crate::LocalOptions::new(
        directory.path().join("filesystem"),
    ))
    .await?;
    let store = crate::LocalCoreStateStore::open_owned(directory.path().join("core-state"))?;
    let distributed = crate::DistributedFs::new(fs.clone(), store.clone());
    let main = unbounded!(fs, create_workspace("repo"))?;
    main.write_text("/base.txt", "base").await?;
    let mut children = Vec::new();
    for name in ["kept", "deleted"] {
        let base = unbounded!(main, head())?;
        let child = fork_workspace(
            &main,
            name,
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        )
        .await?;
        distributed
            .lineage()
            .register_existing_child(&main, &child, base.id())
            .await?;
        children.push(child);
    }
    let deleted = children.pop().ok_or("deleted child")?;
    let kept = children.pop().ok_or("kept child")?;
    let grandchild_base = unbounded!(kept, head())?;
    let grandchild = fork_workspace(
        &kept,
        "grandchild",
        ForkOptions::from_generation(grandchild_base.clone(), IdempotencyKey::new()),
    )
    .await?;
    distributed
        .lineage()
        .register_existing_child(&kept, &grandchild, grandchild_base.id())
        .await?;
    kept.delete(IdempotencyKey::new()).await?;
    deleted.delete(IdempotencyKey::new()).await?;
    let records = store.workspace_records().await?;
    assert!(records.contains_key(&deleted.id()) && records.contains_key(&kept.id()));

    distributed
        .collect_garbage(&CancellationToken::new())
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
