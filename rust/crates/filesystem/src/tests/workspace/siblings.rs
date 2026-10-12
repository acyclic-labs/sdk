use super::*;

/// Reopening local storage between edits and promotions preserves the base
/// objects needed to return a typed conflict for a second sibling.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn reopened_sibling_promotions_return_conflict_without_missing_objects()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let directory = tempfile::tempdir()?;
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let main = fs.create_workspace("promote-main", B, &cancel).await?.value;
        main.write_text("/README.md", "base\n").await?;
        let mut transaction = main
            .begin_transaction(IdempotencyKey::new(), B, &cancel)
            .await?
            .value;
        transaction.create_dir_all("/src", B, &cancel).await?.value;
        transaction
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>,
            )
            .await?
            .value;
        main.write_text("/src/main.rs", "fn main() {}\n").await?;
        let base = main.head(B, &cancel).await?.value;
        main.fork::<_, _, crate::WorkspaceError>(
            "promote-first",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
        main.fork::<_, _, crate::WorkspaceError>(
            "promote-second",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    }
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        fs.open_workspace("promote-first", B, &cancel)
            .await?
            .value
            .write_text("/README.md", "first\n")
            .await?;
    }
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let main = fs.open_workspace("promote-main", B, &cancel).await?.value;
        let first = fs.open_workspace("promote-first", B, &cancel).await?.value;
        let plan = first
            .join_into(&main)
            .history(JoinHistory::Merge)
            .plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        assert!(matches!(
            plan.apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new()
                }, std::collections::BTreeMap::new(),
                PublicationPermit::Unrestricted
            )
            .await?,
            JoinOutcome::Applied(_)
        ));
        assert_eq!(
            main.read("/README.md", 64, B, &cancel).await?.value,
            OwnedBytes::from_static(b"first\n")
        );
    }
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        fs.open_workspace("promote-second", B, &cancel)
            .await?
            .value
            .write_text("/README.md", "second\n")
            .await?;
    }
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let main = fs.open_workspace("promote-main", B, &cancel).await?.value;
        let second = fs.open_workspace("promote-second", B, &cancel).await?.value;
        let plan = second
            .join_into(&main)
            .history(JoinHistory::Merge)
            .plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        assert!(matches!(
            plan.apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new()
                }, std::collections::BTreeMap::new(),
                PublicationPermit::Unrestricted
            )
            .await?,
            JoinOutcome::Conflicted { .. }
        ));
        assert_eq!(
            main.read("/README.md", 64, B, &cancel).await?.value,
            OwnedBytes::from_static(b"first\n")
        );
    }
    Ok(())
}

/// Siblings forked from one head, each adding its own file, join back one
/// after the other. The second join is a real three-way merge (its base is
/// the pre-first-join head) and must apply: nothing in it conflicts.
#[tokio::test]
async fn sibling_forks_adding_distinct_files_join_in_sequence() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let main = fs
        .create_workspace("siblings-main", B, &cancel)
        .await?
        .value;
    main.write_text("/README.md", "base\n").await?;
    let base = main.head(B, &cancel).await?.value;
    let first = main
        .fork::<_, _, crate::WorkspaceError>(
            "siblings-first",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    let second = main
        .fork::<_, _, crate::WorkspaceError>(
            "siblings-second",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    first.write_text("/a.rs", "a\n").await?;
    second.write_text("/b.rs", "b\n").await?;

    for (label, fork) in [("first", &first), ("second", &second)] {
        let plan = fork.join_into(&main).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let outcome = plan
            .apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                }, std::collections::BTreeMap::new(),
                PublicationPermit::Unrestricted,
            )
            .await?;
        let applied = match outcome {
            JoinOutcome::Applied(_) => true,
            JoinOutcome::Conflicted { conflicts, .. } => {
                let described = plan.describe_conflicts(conflicts.to_vec(), false).await?;
                panic!("{label} join conflicted: {:?}", described.conflicts);
            }
            _ => false,
        };
        assert!(applied, "{label} join did not apply");
    }
    assert_eq!(
        main.read("/a.rs", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"a\n")
    );
    assert_eq!(
        main.read("/b.rs", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"b\n")
    );
    assert_eq!(
        main.read("/README.md", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"base\n")
    );
    Ok(())
}

/// Two siblings each create the same directory (under different identities)
/// and add a file to it. Directories merge by path, as in Git: the second join
/// folds its directory into the first's instead of conflicting on the name,
/// including a nested directory both created.
#[tokio::test]
async fn sibling_directories_created_independently_merge_by_path() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let main = fs.create_workspace("fold-main", B, &cancel).await?.value;
    main.write_text("/README.md", "base\n").await?;
    let base = main.head(B, &cancel).await?.value;
    let mut forks = Vec::new();
    for (name, file) in [("fold-first", "a.rs"), ("fold-second", "b.rs")] {
        let fork = main
            .fork::<_, _, crate::WorkspaceError>(
                name,
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                &mut allow::<FsError>,
            )
            .await?
            .value;
        let mut transaction = fork
            .begin_transaction(IdempotencyKey::new(), B, &cancel)
            .await?
            .value;
        transaction
            .create_dir_all("/pkg/nested", B, &cancel)
            .await?
            .value;
        transaction
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>,
            )
            .await?
            .value;
        fork.write_text(&format!("/pkg/{file}"), file).await?;
        fork.write_text(&format!("/pkg/nested/{file}"), file)
            .await?;
        forks.push(fork);
    }
    for fork in &forks {
        let plan = fork.join_into(&main).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        match plan
            .apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                }, std::collections::BTreeMap::new(),
                PublicationPermit::Unrestricted,
            )
            .await?
        {
            JoinOutcome::Applied(_) => {}
            JoinOutcome::Conflicted { conflicts, .. } => {
                let described = plan.describe_conflicts(conflicts.to_vec(), false).await?;
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
            main.read(path, 16, B, &cancel).await?.value,
            OwnedBytes::copy_from_slice(name.as_bytes())
        );
    }
    Ok(())
}

/// Two siblings each create the same file (under different identities, as two
/// captures of one host file do). Files merge by path like directories: the
/// same content folds into one file, and only different content conflicts.
#[tokio::test]
async fn sibling_files_created_independently_merge_by_path() -> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let main = fs
        .create_workspace("file-fold-main", B, &cancel)
        .await?
        .value;
    main.write_text("/README.md", "base\n").await?;
    let base = main.head(B, &cancel).await?.value;
    let mut forks = Vec::new();
    for (name, other) in [
        ("file-fold-first", "first\n"),
        ("file-fold-second", "second\n"),
    ] {
        let fork = main
            .fork::<_, _, crate::WorkspaceError>(
                name,
                ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                &mut allow::<FsError>,
            )
            .await?
            .value;
        fork.write_text("/same.rs", "same\n").await?;
        fork.write_text("/other.rs", other).await?;
        forks.push(fork);
    }
    let mut outcomes = Vec::new();
    for fork in &forks {
        let plan = fork.join_into(&main).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        outcomes.push(
            match plan
                .apply(
                    ApplyOptions {
                        if_target: plan.target_head(),
                        idempotency_key: IdempotencyKey::new(),
                    }, std::collections::BTreeMap::new(),
                    PublicationPermit::Unrestricted,
                )
                .await?
            {
                JoinOutcome::Applied(_) => Vec::new(),
                JoinOutcome::Conflicted { conflicts, .. } => plan
                    .describe_conflicts(conflicts.to_vec(), false)
                    .await?
                    .conflicts
                    .into_iter()
                    .map(|conflict| conflict.path)
                    .collect(),
                _ => panic!("join did not apply"),
            },
        );
    }
    assert_eq!(outcomes, [Vec::new(), vec![Some("/other.rs".to_owned())]]);

    let fork = main
        .fork::<_, _, crate::WorkspaceError>(
            "file-fold-third",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
            PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut allow::<FsError>,
        )
        .await?
        .value;
    fork.write_text("/same.rs", "same\n").await?;
    let plan = fork.join_into(&main).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    match plan
        .apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            }, std::collections::BTreeMap::new(),
            PublicationPermit::Unrestricted,
        )
        .await?
    {
        // Folded into the file main already holds: nothing left to publish.
        JoinOutcome::NoChanges(_) => {}
        JoinOutcome::Conflicted { conflicts, .. } => {
            let described = plan.describe_conflicts(conflicts.to_vec(), false).await?;
            panic!(
                "an identical independent creation must fold: {:?}",
                described.conflicts
            );
        }
        _ => panic!("an identical independent creation changes nothing"),
    }
    assert_eq!(
        main.read("/same.rs", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"same\n")
    );
    assert_eq!(
        main.read("/other.rs", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"first\n")
    );
    Ok(())
}

/// The mount bumps a directory's modification time whenever a child is
/// added, so two siblings adding files to one directory always diverge on its
/// metadata. That must reconcile; only authored metadata (the mode) conflicts.
#[tokio::test]
async fn sibling_directory_times_reconcile_but_authored_metadata_conflicts()
-> Result<(), Box<dyn Error>> {
    let cancel = CancellationToken::new();
    use crate::kernel::MetadataField;

    let fs = Fs::memory();
    let main = fs
        .create_workspace("sibling-times-main", B, &cancel)
        .await?
        .value;
    main.write_text("/README.md", "base\n").await?;
    {
        let mut transaction = main
            .begin_transaction(IdempotencyKey::new(), B, &cancel)
            .await?
            .value;
        transaction.create_dir_all("/pkg", B, &cancel).await?.value;
        transaction
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>,
            )
            .await?
            .value;
    }
    let touch = |modified_ns: i64, posix_mode: u32| FileMetadata {
        modified_ns: MetadataField::Value(modified_ns),
        posix_mode: MetadataField::Value(posix_mode),
        ..FileMetadata::default()
    };
    {
        let mut transaction = main
            .begin_transaction(IdempotencyKey::new(), B, &cancel)
            .await?
            .value;
        transaction
            .set_metadata("/pkg", touch(10, 0o755), B, &cancel)
            .await?
            .value;
        transaction
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>,
            )
            .await?
            .value;
    }
    let base = main.head(B, &cancel).await?.value;
    let fork = |name: &'static str, base: &Generation<_, _>| {
        let options = ForkOptions::from_generation(base.clone(), IdempotencyKey::new());
        let main = &main;
        let cancel = &cancel;
        async move {
            main.fork::<_, _, crate::WorkspaceError>(
                name,
                options,
                PublicationPermit::Unrestricted,
                B,
                cancel,
                &mut allow::<FsError>,
            ).await
        }
    };
    let first = fork("sibling-times-first", &base).await?.value;
    let second = fork("sibling-times-second", &base).await?.value;
    for (workspace, file, modified_ns) in [(&first, "/pkg/a.rs", 20), (&second, "/pkg/b.rs", 30)] {
        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::new(), B, &cancel)
            .await?
            .value;
        transaction.write_text(file, "x\n").await?;
        transaction
            .set_metadata("/pkg", touch(modified_ns, 0o755), B, &cancel)
            .await?
            .value;
        transaction
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>,
            )
            .await?
            .value;
    }
    for fork in [&first, &second] {
        let plan = fork.join_into(&main).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let outcome = plan
            .apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                }, std::collections::BTreeMap::new(),
                PublicationPermit::Unrestricted,
            )
            .await?;
        if let JoinOutcome::Conflicted { conflicts, .. } = outcome {
            let described = plan.describe_conflicts(conflicts.to_vec(), false).await?;
            panic!("sibling join conflicted: {:?}", described.conflicts);
        }
    }
    assert_eq!(
        main.read("/pkg/a.rs", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"x\n")
    );
    assert_eq!(
        main.read("/pkg/b.rs", 16, B, &cancel).await?.value,
        OwnedBytes::from_static(b"x\n")
    );
    assert_eq!(
        main.stat("/pkg", B, &cancel)
            .await?
            .value
            .metadata
            .modified_ns,
        Some(30)
    );

    // Authored metadata changed both ways is a conflict, reported as such.
    let base = main.head(B, &cancel).await?.value;
    let third = fork("sibling-times-third", &base).await?.value;
    for (workspace, mode) in [(&main, 0o700), (&third, 0o770)] {
        let mut transaction = workspace
            .begin_transaction(IdempotencyKey::new(), B, &cancel)
            .await?
            .value;
        transaction
            .set_metadata("/pkg", touch(40, mode), B, &cancel)
            .await?
            .value;
        transaction
            .commit(
                None,
                PublicationPermit::Unrestricted,
                B,
                &cancel,
                allow::<WorkspaceError>,
            )
            .await?
            .value;
    }
    let plan = third.join_into(&main).plan(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    let JoinOutcome::Conflicted {
        conflicts,
        truncated,
    } = plan
        .apply(
            ApplyOptions {
                if_target: plan.target_head(),
                idempotency_key: IdempotencyKey::new(),
            }, std::collections::BTreeMap::new(),
            PublicationPermit::Unrestricted,
        )
        .await?
    else {
        return Err("a mode conflict on the shared directory was not reported".into());
    };
    let described = plan.describe_conflicts(conflicts.to_vec(), truncated).await?;
    assert_eq!(described.conflicts.len(), 1);
    assert_eq!(described.conflicts[0].kind, crate::ConflictKind::Metadata);
    Ok(())
}
