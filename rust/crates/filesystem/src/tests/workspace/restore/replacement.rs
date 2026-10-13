use super::*;

#[tokio::test]
async fn selected_restore_replaces_both_shapes_atomically_and_preserves_outside_aliases()
-> Result<(), Box<dyn Error>> {
    for directory in [false, true] {
        let fs = Fs::memory();
        let source_workspace = unbounded!(fs, create_workspace("replacement-source"))?;
        let workspace = unbounded!(fs, create_workspace("replacement-target"))?;
        let paths;
        let retained_path;
        let retained_text;
        if directory {
            let mut source =
                unbounded!(source_workspace, begin_transaction(IdempotencyKey::new()))?;
            unbounded!(source, create_dir_all("/hot/empty/nested"))?;
            unbounded!(
                source,
                set_metadata(
                    "/hot",
                    FileMetadata {
                        posix_mode: MetadataField::Value(0o750),
                        ..FileMetadata::default()
                    }
                )
            )?;
            source.write_text("/hot/value", "selected source").await?;
            unbounded!(source, hard_link("/hot/value", "/hot/alias"))?;
            commit(&mut source).await?;
            workspace.write_text("/hot", "original file").await?;
            retained_path = "/hot";
            retained_text = "original file";
            paths = vec![
                "hot",
                "hot/empty",
                "hot/empty/nested",
                "hot/value",
                "hot/alias",
            ];
        } else {
            source_workspace
                .write_text("/hot", "replacement file")
                .await?;
            let mut target = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
            unbounded!(target, create_dir_all("/hot/nested/empty"))?;
            target
                .write_text("/hot/nested/old", "retained outside alias")
                .await?;
            target.write_text("/hot/top", "old top").await?;
            commit(&mut target).await?;
            retained_path = "/hot/nested/old";
            retained_text = "retained outside alias";
            paths = vec![
                "hot/nested/empty",
                "hot/nested/old",
                "hot/nested",
                "hot/top",
                "hot",
            ];
        }
        let mut target = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(target, hard_link(retained_path, "/outside-linked"))?;
        target.write_text("/outside", "user").await?;
        commit(&mut target).await?;
        let source = unbounded!(source_workspace, head())?;
        let original = unbounded!(workspace, head())?;
        let paths: Vec<_> = paths.into_iter().map(str::to_owned).collect();
        let budget = selected_transaction_budget();
        let cancel = CancellationToken::new();
        let denied = workspace
            .restore_paths_from(
                &source,
                &paths,
                original.id(),
                IdempotencyKey::new(),
                crate::PublicationPermit::Unrestricted,
                budget,
                &cancel,
                |_| {
                    std::future::ready(Err(crate::OperationFailure::before_work(
                        FsError::MutationNotAllowed,
                    )))
                },
            )
            .await
            .err()
            .ok_or("denied replacement published")?;
        denied.work.verify(budget)?;
        assert_eq!(unbounded!(workspace, head())?.id(), original.id());
        assert_eq!(
            unbounded!(workspace, read(retained_path, 64))?.as_ref(),
            retained_text.as_bytes()
        );
        assert_eq!(
            unbounded!(workspace, stat(retained_path))?.file_id,
            unbounded!(workspace, stat("/outside-linked"))?.file_id
        );
        assert_eq!(
            unbounded!(workspace, stat("/outside-linked"))?.link_count,
            2
        );
        let volume = workspace.volume.clone();
        let guard_cancel = cancel.clone();
        let original_id = original.id();
        let restored = workspace
            .restore_paths_from(
                &source,
                &paths,
                original_id,
                IdempotencyKey::new(),
                crate::PublicationPermit::Unrestricted,
                budget,
                &cancel,
                move |allowance| {
                    let volume = volume.clone();
                    let cancel = guard_cancel.clone();
                    async move {
                        let checked = volume
                            .checkout(
                                GenerationSelector::Exact(original_id),
                                CheckoutMode::read_only_pinned(),
                                allowance,
                                &cancel,
                            )
                            .await?;
                        Ok(crate::OperationReceipt {
                            value: (),
                            work: checked.work,
                        })
                    }
                },
            )
            .await?;
        restored.work.verify(budget)?;
        assert!(matches!(restored.value, TransactionCommit::Committed(_)));
        if directory {
            assert_eq!(
                unbounded!(workspace, stat("/hot"))?.metadata.posix_mode,
                Some(0o750)
            );
            assert_eq!(
                unbounded!(workspace, stat("/hot/empty/nested"))?.kind,
                FileKind::Directory
            );
            assert!(
                unbounded!(workspace, list_directory("/hot/empty/nested", None, 1))?
                    .0
                    .entries
                    .is_empty()
            );
            for path in ["/hot/value", "/hot/alias"] {
                assert_eq!(
                    unbounded!(workspace, read(path, 32))?.as_ref(),
                    b"selected source"
                );
            }
            assert_eq!(
                unbounded!(workspace, stat("/hot/value"))?.file_id,
                unbounded!(workspace, stat("/hot/alias"))?.file_id
            );
        } else {
            assert_eq!(
                unbounded!(workspace, read("/hot", 64))?.as_ref(),
                b"replacement file"
            );
            assert_eq!(unbounded!(workspace, stat("/hot"))?.kind, FileKind::Regular);
        }
        assert_eq!(
            unbounded!(workspace, read("/outside-linked", 64))?.as_ref(),
            retained_text.as_bytes()
        );
        assert_eq!(
            unbounded!(workspace, stat("/outside-linked"))?.link_count,
            1
        );
        assert_eq!(
            unbounded!(workspace, read("/outside", 64))?.as_ref(),
            b"user"
        );
    }
    Ok(())
}
