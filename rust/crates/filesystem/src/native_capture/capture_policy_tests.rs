use super::*;

#[cfg(test)]
mod capture_policy_tests {
    use super::*;

    fn path(value: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
        let limits = crate::model::VolumeLimits::default();
        Ok(NamespacePath::from_portable(
            &crate::path::PortablePath::parse(value, limits)?,
            limits,
        )?)
    }

    fn batch(changes: Vec<WatchChange>) -> WatchBatch {
        WatchBatch::Changes {
            original: Default::default(),
            epoch: 1,
            first_sequence: 1,
            next_sequence: 1 + changes.len() as u64,
            changes,
        }
    }

    fn options(root: &Path, maximum_paths: u32) -> Result<CaptureOptions, CaptureError> {
        Ok(CaptureOptions {
            source_root: root.to_path_buf(),
            expected_root_identity: capture_root_identity(root)?,
            maximum_paths,
            maximum_extent_spans: 8,
        })
    }

    #[test]
    fn canonical_policy_uses_the_nearest_sorted_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let policy = CapturePolicy::excluding(vec![
            path("/z/private/nested")?,
            path("/a/cache")?,
            path("/z/private")?,
            path("/a/cache/deeper")?,
            path("/m")?,
        ])?;

        assert!(policy.excludes(&path("/a/cache/object")?));
        assert!(policy.excludes(&path("/m")?));
        assert!(policy.excludes(&path("/z/private/nested/object")?));
        assert!(!policy.excludes(&path("/a/cached")?));
        assert!(!policy.excludes(&path("/n")?));
        assert!(!policy.excludes(&path("/z/public")?));
        Ok(())
    }

    #[tokio::test]
    async fn nested_hints_hydrate_missing_or_stale_ancestors_without_expanding_siblings()
    -> Result<(), Box<dyn std::error::Error>> {
        for (workspace_name, child_name, contents, prior) in [
            ("watch-ancestor", "/sub/changed.txt", "changed", None),
            ("watch-stale-ancestor", "/sub/new.txt", "new", Some("old")),
        ] {
            let temporary = tempfile::tempdir()?;
            std::fs::create_dir(temporary.path().join("sub"))?;
            std::fs::write(temporary.path().join(&child_name[1..]), contents.as_bytes())?;
            if prior.is_none() {
                std::fs::write(temporary.path().join("sub/unrelated.txt"), b"unrelated")?;
            }
            let workspace = crate::Fs::memory()
                .create_workspace(
                    workspace_name,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)?;
            if let Some(contents) = prior {
                workspace.write_text("/sub", contents).await?;
            }
            let mut checkout = workspace
                .checkout(
                    crate::model::GenerationSelector::Head,
                    crate::model::CheckoutMode::tracking_transaction(),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)?;
            let child = path(child_name)?;
            let hint = if prior.is_some() {
                WatchChange::Created(child.clone())
            } else {
                WatchChange::Modified(child.clone())
            };
            capture_watch_batch(
                &mut checkout,
                batch(vec![hint]),
                &options(temporary.path(), 2)?,
                &CapturePolicy::default(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
            let token = CancellationToken::new();
            let parent = checkout
                .lookup_no_follow(&path("/sub")?, WorkBudget::UNBOUNDED, &token)
                .await?;
            assert_eq!(
                parent.value.record.map(|record| record.kind),
                Some(FileKind::Directory)
            );
            let file = checkout
                .lookup_no_follow(&child, WorkBudget::UNBOUNDED, &token)
                .await?;
            assert_eq!(
                file.value.record.map(|record| record.kind),
                Some(FileKind::Regular)
            );
            if prior.is_none() {
                let unrelated = checkout
                    .lookup_no_follow(&path("/sub/unrelated.txt")?, WorkBudget::UNBOUNDED, &token)
                    .await?;
                assert!(unrelated.value.record.is_none());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_modified_directory_keeps_what_it_holds() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("pkg"))?;
        std::fs::write(temporary.path().join("pkg/shared.rs"), b"VALUE = 1")?;
        let workspace = crate::Fs::memory()
            .create_workspace(
                "watch-directory",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        let options = options(temporary.path(), 8)?;
        capture_baseline(
            &mut checkout,
            &options,
            &crate::CapturePolicy::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        capture_watch_batch(
            &mut checkout,
            batch(vec![WatchChange::Modified(path("/pkg")?)]),
            &options,
            &crate::CapturePolicy::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        let file = checkout
            .lookup_no_follow(
                &path("/pkg/shared.rs")?,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            file.value.record.map(|record| record.kind),
            Some(FileKind::Regular)
        );
        Ok(())
    }

    #[tokio::test]
    async fn watch_directory_rename_then_child_change_keeps_the_renamed_directory()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("old"))?;
        std::fs::write(temporary.path().join("old/file.txt"), b"old")?;
        let workspace = crate::Fs::memory()
            .create_workspace(
                "watch-renamed-directory",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        let mut transaction = workspace
            .begin_transaction(
                crate::IdempotencyKey::new(),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        transaction
            .create_dir_all(
                "/old",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        transaction.write_text("/old/file.txt", "old").await?;
        transaction
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                        crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::default(),
                        },
                    )
                },
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        assert!(
            checkout
                .lookup_no_follow(
                    &path("/old")?,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new()
                )
                .await?
                .value
                .record
                .is_some()
        );
        std::fs::rename(temporary.path().join("old"), temporary.path().join("new"))?;
        std::fs::write(temporary.path().join("new/file.txt"), b"new")?;
        let old = path("/old")?;
        let new = path("/new")?;
        let child = path("/new/file.txt")?;
        capture_watch_batch(
            &mut checkout,
            batch(vec![
                WatchChange::Renamed {
                    from: old.clone(),
                    to: new.clone(),
                },
                WatchChange::Modified(child.clone()),
            ]),
            &options(temporary.path(), 3)?,
            &crate::CapturePolicy::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        let token = CancellationToken::new();
        assert!(
            checkout
                .lookup_no_follow(&old, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .is_none()
        );
        assert_eq!(
            checkout
                .lookup_no_follow(&new, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .map(|record| record.kind),
            Some(FileKind::Directory)
        );
        assert_eq!(
            checkout
                .lookup_no_follow(&child, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .map(|record| record.kind),
            Some(FileKind::Regular)
        );
        Ok(())
    }

    /// A directory modified in place is covered by its entries' own events: its unnamed entries (unobserved in a lazy checkout) stay out, while a directory that arrived by rename brings its whole subtree.
    #[cfg(not(target_os = "macos"))]
    #[tokio::test]
    async fn watch_expands_only_directories_that_appeared() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        for directory in ["modified", "arrived"] {
            std::fs::create_dir(temporary.path().join(directory))?;
            std::fs::write(temporary.path().join(directory).join("inner.txt"), b"x")?;
        }
        let workspace = crate::Fs::memory()
            .create_workspace(
                "watch-expansion",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        capture_watch_batch(
            &mut checkout,
            batch(vec![
                WatchChange::Modified(path("/modified")?),
                WatchChange::Arrived(path("/arrived")?),
            ]),
            &options(temporary.path(), 8)?,
            &CapturePolicy::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        let token = CancellationToken::new();
        let mut captured = Vec::new();
        for candidate in [
            "/modified",
            "/modified/inner.txt",
            "/arrived",
            "/arrived/inner.txt",
        ] {
            if checkout
                .lookup_no_follow(&path(candidate)?, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .is_some()
            {
                captured.push(candidate);
            }
        }
        assert_eq!(captured, ["/modified", "/arrived", "/arrived/inner.txt"]);
        Ok(())
    }

    #[tokio::test]
    async fn watch_rename_chain_observes_intermediate_parent()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("sub"))?;
        std::fs::write(temporary.path().join("final.txt"), b"old")?;
        let workspace = crate::Fs::memory()
            .create_workspace(
                "watch-rename-chain",
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        workspace.write_text("/old.txt", "old").await?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        capture_watch_batch(
            &mut checkout,
            batch(vec![
                WatchChange::Renamed {
                    from: path("/old.txt")?,
                    to: path("/sub/intermediate.txt")?,
                },
                WatchChange::Renamed {
                    from: path("/sub/intermediate.txt")?,
                    to: path("/final.txt")?,
                },
            ]),
            &options(temporary.path(), 3)?,
            &crate::CapturePolicy::default(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        let token = CancellationToken::new();
        assert!(
            checkout
                .lookup_no_follow(&path("/old.txt")?, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .is_none()
        );
        assert!(
            checkout
                .lookup_no_follow(&path("/final.txt")?, WorkBudget::UNBOUNDED, &token)
                .await?
                .value
                .record
                .is_some()
        );
        Ok(())
    }
}
