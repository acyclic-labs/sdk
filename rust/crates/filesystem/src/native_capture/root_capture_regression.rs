use super::*;

#[cfg(all(test, unix, feature = "native-mount"))]
mod root_capture_regression {
    use super::*;

    #[tokio::test]
    async fn root_capture_publishes_metadata_and_children() -> Result<(), Box<dyn std::error::Error>>
    {
        use std::os::unix::fs::PermissionsExt;
        let source = tempfile::tempdir()?;
        std::fs::set_permissions(source.path(), std::fs::Permissions::from_mode(0o750))?;
        std::fs::write(source.path().join("binary"), [0, 255, 10])?;
        let workspace = crate::Fs::memory()
            .create_workspace(
                "root-capture-regression",
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
        let root = NamespacePath::new(Vec::new(), checkout.volume_config().limits)?;
        let cancellation = CancellationToken::new();
        capture_subtree(
            &mut checkout,
            root.clone(),
            &CaptureOptions {
                source_root: source.path().to_path_buf(),
                expected_root_identity: capture_root_identity(source.path())?,
                maximum_paths: 100,
                maximum_extent_spans: 100,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        crate::seal_checkout(
            &mut checkout,
            crate::OperationId::new(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let output = tempfile::tempdir()?;
        let mut reader = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::read_only_pinned(),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        crate::materialize_checkout(
            &mut reader,
            &crate::MaterializeOptions {
                destination: output.path().to_path_buf(),
                expected_root_identity: None,
                maximum_directory_entries: 2,
                maximum_extent_spans: 100,
                transfer_bytes: 1024,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert_eq!(std::fs::read(output.path().join("binary"))?, [0, 255, 10]);
        assert_eq!(
            std::fs::metadata(output.path())?.permissions().mode() & 0o777,
            0o750
        );
        Ok(())
    }
}
