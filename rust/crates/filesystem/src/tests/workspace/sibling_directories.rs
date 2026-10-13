use super::*;

#[tokio::test]
async fn sibling_directories_created_independently_merge_by_path() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("fold-main"))?;
    main.write_text("/README.md", "base\n").await?;
    let base = unbounded!(main, head())?;
    let mut forks = Vec::new();
    for (name, file) in [("fold-first", "a.rs"), ("fold-second", "b.rs")] {
        let fork = fork_workspace(
            &main,
            name,
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        )
        .await?;
        let mut transaction = unbounded!(fork, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(transaction, create_dir_all("/pkg/nested"))?;
        commit(&mut transaction).await?;
        fork.write_text(&format!("/pkg/{file}"), file).await?;
        fork.write_text(&format!("/pkg/nested/{file}"), file)
            .await?;
        forks.push(fork);
    }
    for fork in &forks {
        let plan = fork.join_into(&main).plan().await?;
        match plan
            .apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                },
                crate::PublicationPermit::Unrestricted,
            )
            .await?
        {
            JoinOutcome::Applied(_) => {}
            JoinOutcome::Conflicted { conflicts, .. } => {
                let described = plan.describe_conflicts(&conflicts, false).await?;
                panic!("join conflicted: {:?}", described.conflicts);
            }
            _ => panic!("join did not apply"),
        }
    }
    for path in [
        "/pkg/a.rs",
        "/pkg/b.rs",
        "/pkg/nested/a.rs",
        "/pkg/nested/b.rs",
    ] {
        let name = path.rsplit('/').next().expect("file name");
        assert_eq!(
            unbounded!(main, read(path, 16))?,
            OwnedBytes::copy_from_slice(name.as_bytes())
        );
    }
    Ok(())
}

#[tokio::test]
async fn sibling_directory_times_reconcile_but_authored_metadata_conflicts()
-> Result<(), Box<dyn Error>> {
    use crate::kernel::MetadataField;

    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("sibling-times-main"))?;
    main.write_text("/README.md", "base\n").await?;
    {
        let mut transaction = unbounded!(main, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(transaction, create_dir_all("/pkg"))?;
        commit(&mut transaction).await?;
    }
    let touch = |modified_ns: i64, posix_mode: u32| FileMetadata {
        modified_ns: MetadataField::Value(modified_ns),
        posix_mode: MetadataField::Value(posix_mode),
        ..FileMetadata::default()
    };
    {
        let mut transaction = unbounded!(main, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(transaction, set_metadata("/pkg", touch(10, 0o755)))?;
        commit(&mut transaction).await?;
    }
    let base = unbounded!(main, head())?;
    let fork = |name: &'static str, base: &Generation<_, _>| {
        fork_workspace(
            &main,
            name,
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        )
    };
    let first = fork("sibling-times-first", &base).await?;
    let second = fork("sibling-times-second", &base).await?;
    for (workspace, file, modified_ns) in [(&first, "/pkg/a.rs", 20), (&second, "/pkg/b.rs", 30)] {
        let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        transaction.write_text(file, "x\n").await?;
        unbounded!(transaction, set_metadata("/pkg", touch(modified_ns, 0o755)))?;
        commit(&mut transaction).await?;
    }
    for fork in [&first, &second] {
        let plan = fork.join_into(&main).plan().await?;
        let outcome = plan
            .apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                },
                crate::PublicationPermit::Unrestricted,
            )
            .await?;
        if let JoinOutcome::Conflicted { conflicts, .. } = outcome {
            let described = plan.describe_conflicts(&conflicts, false).await?;
            panic!("sibling join conflicted: {:?}", described.conflicts);
        }
    }
    assert_eq!(
        unbounded!(main, read("/pkg/a.rs", 16))?,
        OwnedBytes::from_static(b"x\n")
    );
    assert_eq!(
        unbounded!(main, read("/pkg/b.rs", 16))?,
        OwnedBytes::from_static(b"x\n")
    );
    assert_eq!(
        unbounded!(main, stat("/pkg"))?.metadata.modified_ns,
        Some(30)
    );
    let base = unbounded!(main, head())?;
    let third = fork("sibling-times-third", &base).await?;
    for (workspace, mode) in [(&main, 0o700), (&third, 0o770)] {
        let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(transaction, set_metadata("/pkg", touch(40, mode)))?;
        commit(&mut transaction).await?;
    }
    let plan = third.join_into(&main).plan().await?;
    let JoinOutcome::Conflicted {
        conflicts,
        truncated,
    } = plan
        .apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            },
            crate::PublicationPermit::Unrestricted,
        )
        .await?
    else {
        return Err("a mode conflict on the shared directory was not reported".into());
    };
    let described = plan.describe_conflicts(&conflicts, truncated).await?;
    assert_eq!(described.conflicts.len(), 1);
    assert_eq!(described.conflicts[0].kind, crate::ConflictKind::Metadata);
    Ok(())
}
