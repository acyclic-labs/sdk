use super::*;

#[tokio::test]
async fn materialized_baseline_ignores_host_defaults_and_keeps_content_edits()
-> Result<(), Box<dyn std::error::Error>> {
    check_materialized_metadata().await
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn materialized_baseline_survives_publication_without_birth_times()
-> Result<(), Box<dyn std::error::Error>> {
    let _statx = crate::native_host::stat_probe::StatxOptions::enter(false, true);
    check_materialized_metadata().await
}

async fn check_materialized_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let state = tempfile::tempdir()?;
    let host = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let budget = WorkBudget::UNBOUNDED;
    let workspace = crate::Fs::local(crate::LocalOptions::new(state.path()))
        .await?
        .create_workspace("materialized-view", budget, &cancellation)
        .await?
        .value;
    for index in 0..8 {
        workspace
            .write_text(&format!("/file-{index}"), &"original content ".repeat(512))
            .await?;
    }
    let mut reader = workspace
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::read_only_pinned(),
            budget,
            &cancellation,
        )
        .await?
        .value;
    let metadata = crate::LocalCoreStateStore::new(state.path().join("metadata"));
    let mut materialization = crate::MaterializeOptions::native(host.path());
    materialization.materialized_view = Some(NativeViewBaseline::recording(&metadata));
    crate::materialize_checkout(&mut reader, &materialization, budget, &cancellation).await?;
    let identity = capture_root_identity(host.path())?;
    let mut options = CaptureOptions {
        materialized_view: None,
        source_root: host.path().to_path_buf(),
        expected_root_identity: identity,
        source_namespace: None,
        maximum_paths: None,
        maximum_extent_spans: None,
    };
    for normalized in [false, true] {
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
                budget,
                &cancellation,
            )
            .await?
            .value;
        options.materialized_view =
            normalized.then(|| NativeViewBaseline::new(identity, false).recorded_in(&metadata));
        let receipt = capture_baseline(
            &mut checkout,
            &options,
            &CapturePolicy::default(),
            budget,
            &cancellation,
        )
        .await?
        .value;
        if normalized {
            assert_eq!(receipt.changed_paths, 0);
            assert!(!checkout.has_pending_mutations());
        } else {
            assert!(
                receipt.changed_paths > 0,
                "host defaults are the negative control"
            );
            assert!(checkout.has_pending_mutations());
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let file = host.path().join("file-4");
        let original = std::fs::metadata(&file)?.permissions();
        std::fs::set_permissions(
            &file,
            std::fs::Permissions::from_mode(original.mode() ^ 0o100),
        )?;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
                budget,
                &cancellation,
            )
            .await?
            .value;
        let receipt = capture_baseline(
            &mut checkout,
            &options,
            &CapturePolicy::default(),
            budget,
            &cancellation,
        )
        .await?
        .value;
        assert_eq!(
            receipt.changed_paths, 1,
            "permission edits must survive normalization"
        );
        assert!(checkout.has_pending_mutations());
        std::fs::set_permissions(file, original)?;
    }
    let file = host.path().join("file-2");
    let original = std::fs::metadata(&file)?;
    let modified = original.modified()?;
    let created = original.created().ok();
    let handle = std::fs::OpenOptions::new().write(true).open(&file)?;
    handle.set_times(
        std::fs::FileTimes::new().set_modified(modified + std::time::Duration::from_secs(37)),
    )?;
    let mut checkout = workspace
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            budget,
            &cancellation,
        )
        .await?
        .value;
    let changed = capture_baseline(
        &mut checkout,
        &options,
        &CapturePolicy::default(),
        budget,
        &cancellation,
    )
    .await?
    .value;
    assert_eq!(
        changed.changed_paths, 1,
        "explicit modification times must survive normalization"
    );
    handle.set_times(std::fs::FileTimes::new().set_modified(modified))?;
    drop(handle);
    assert_eq!(
        std::fs::metadata(&file)?.created().ok(),
        created,
        "the modification-time fixture preserves creation time"
    );
    std::fs::write(host.path().join("file-3"), b"updated content")?;
    let mut checkout = workspace
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            budget,
            &cancellation,
        )
        .await?
        .value;
    let receipt = capture_baseline(
        &mut checkout,
        &options,
        &CapturePolicy::default(),
        budget,
        &cancellation,
    )
    .await?
    .value;
    assert_eq!(receipt.changed_paths, 1);
    assert!(checkout.has_pending_mutations());
    let previous_entries = std::fs::read_dir(host.path())?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    crate::restore_checkout_host_path(
        &mut reader,
        Path::new("file-3"),
        crate::HostPathReplacement::Atomic,
        &materialization,
        crate::performance::work_budget(100_000, 64 * 1024 * 1024),
        &cancellation,
    )
    .await?;
    let mut restored = workspace
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            budget,
            &cancellation,
        )
        .await?
        .value;
    let selected = host_path_to_namespace(
        Path::new("file-3"),
        reader.volume_config().profile,
        reader.volume_config().limits,
    )?;
    let recaptured = capture_paths(&mut restored, &[selected], &options, budget, &cancellation)
        .await?
        .value;
    assert_eq!(
        recaptured.changed_paths, 0,
        "restored defaults belong to the final root"
    );
    assert!(!restored.has_pending_mutations());
    let added = std::fs::read_dir(host.path())?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|name| !previous_entries.contains(name))
        .collect::<Vec<_>>();
    assert_eq!(added.len(), 1, "restore retains one process-safe lock file");
    let complete = capture_baseline(
        &mut restored,
        &options,
        &CapturePolicy::default(),
        budget,
        &cancellation,
    )
    .await?
    .value;
    assert_eq!(
        complete.changed_paths, 1,
        "full capture preserves new files"
    );
    let added = host_path_to_namespace(
        Path::new(&added[0]),
        reader.volume_config().profile,
        reader.volume_config().limits,
    )?;
    assert!(
        restored
            .lookup_no_follow(&added, budget, &cancellation)
            .await?
            .value
            .record
            .is_some()
    );
    let other = tempfile::tempdir()?;
    options.materialized_view = Some(NativeViewBaseline::new(
        capture_root_identity(other.path())?,
        false,
    ));
    let mut clean = workspace
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            budget,
            &cancellation,
        )
        .await?
        .value;
    let failure = capture_baseline(
        &mut clean,
        &options,
        &CapturePolicy::default(),
        budget,
        &cancellation,
    )
    .await
    .unwrap_err();
    assert!(matches!(failure.error, CaptureError::RootChanged));
    assert!(!clean.has_pending_mutations());
    Ok(())
}
