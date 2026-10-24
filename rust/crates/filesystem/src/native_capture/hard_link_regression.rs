use super::*;

#[tokio::test]
async fn recaptured_hard_links_preserve_identity_after_an_alias_rename()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("a"), b"linked")?;
    std::fs::hard_link(source.path().join("a"), source.path().join("b"))?;
    let cancel = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancel)
        .map_err(|(error, _)| error)?;
    let workspace = crate::Fs::memory()
        .create_workspace("postimage-links", work.budget(), &cancel)
        .await?
        .value;
    let mut checkout = workspace
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            &work,
            &cancel,
        )
        .await?;
    assert!(checkout.volume_config().hard_links);
    let root = crate::native_host::HostRoot::open(source.path(), &mut &work)
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
    let options = crate::CaptureOptions {
        source_root: source.path().to_owned(),
        expected_root_identity: root.identity(),
        maximum_paths: 16,
        maximum_extent_spans: 8,
    };
    let limits = checkout.volume_config().limits;
    let path = |text| -> Result<NamespacePath, Box<dyn std::error::Error>> {
        Ok(NamespacePath::from_portable(
            &crate::path::PortablePath::parse(text, limits)?,
            limits,
        )?)
    };
    let paths = [path("/")?];
    crate::native_capture::capture_subtrees_with_native_work(
        &mut checkout,
        &paths,
        &options,
        &crate::CapturePolicy::default(),
        &cancel,
        None,
        &work,
    )
    .await?;
    let record = checkout
        .lookup_no_follow(&path("/a")?, &work, &cancel)
        .await?
        .record
        .ok_or("missing captured original")?;
    std::fs::rename(source.path().join("b"), source.path().join("c"))?;
    let policy = crate::CapturePolicy::default();
    crate::native_capture::capture_subtrees_with_native_work(
        &mut checkout,
        &paths,
        &options,
        &policy,
        &cancel,
        None,
        &work,
    )
    .await?;
    let alias = checkout
        .lookup_no_follow(&path("/c")?, &work, &cancel)
        .await?
        .record
        .ok_or("missing renamed alias")?;
    assert_eq!(
        alias.file_id, record.file_id,
        "recaptured paths must retain the hard-link anchor"
    );
    Ok(())
}
