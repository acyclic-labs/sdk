use super::*;

fn path(value: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    let limits = crate::model::VolumeLimits::default();
    Ok(NamespacePath::from_portable(
        &crate::path::PortablePath::parse(value, limits)?,
        limits,
    )?)
}

fn options(root: &Path, maximum_paths: u32) -> Result<CaptureOptions, CaptureError> {
    Ok(CaptureOptions {
        source_root: root.to_path_buf(),
        expected_root_identity: capture_root_identity(root)?,
        maximum_paths,
        maximum_extent_spans: 8,
    })
}

async fn checkout(
    files: &[(&str, &str)],
) -> Result<crate::facade::MemoryCheckout, Box<dyn std::error::Error>> {
    let workspace = crate::Fs::memory()
        .create_workspace(
            "capture-policy",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    for (name, contents) in files {
        workspace.write_text(name, contents).await?;
    }
    Ok(workspace
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value)
}

async fn replay(
    checkout: &mut crate::facade::MemoryCheckout,
    options: &CaptureOptions,
    changes: Vec<WatchChange>,
) -> Result<(), Box<dyn std::error::Error>> {
    capture_watch_batch(
        checkout,
        WatchBatch::Changes {
            original: Default::default(),
            epoch: 1,
            first_sequence: 1,
            next_sequence: 1 + changes.len() as u64,
            changes,
        },
        options,
        &CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    Ok(())
}

async fn assert_records(
    checkout: &mut crate::facade::MemoryCheckout,
    expected: &[(&str, Option<FileKind>)],
) -> Result<(), Box<dyn std::error::Error>> {
    for (name, kind) in expected {
        let record = checkout
            .lookup_no_follow(
                &path(name)?,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            record.value.record.map(|record| record.kind),
            *kind,
            "{name}"
        );
    }
    Ok(())
}

#[test]
fn excluded_prefixes_do_not_exclude_adjacent_names() -> Result<(), Box<dyn std::error::Error>> {
    let policy = CapturePolicy::excluding(vec![
        path("/z/private/nested")?,
        path("/a/cache")?,
        path("/z/private")?,
        path("/a/cache/deeper")?,
        path("/m")?,
    ])?;
    for (name, excluded) in [
        ("/a/cache/object", true),
        ("/m", true),
        ("/z/private/nested/object", true),
        ("/a/cached", false),
        ("/n", false),
        ("/z/public", false),
    ] {
        assert_eq!(policy.excludes(&path(name)?), excluded, "{name}");
    }
    Ok(())
}

#[tokio::test]
async fn nested_hints_hydrate_missing_or_stale_ancestors_without_expanding_siblings()
-> Result<(), Box<dyn std::error::Error>> {
    for stale in [false, true] {
        let source = tempfile::tempdir()?;
        std::fs::create_dir(source.path().join("sub"))?;
        std::fs::write(source.path().join("sub/changed.txt"), b"changed")?;
        std::fs::write(source.path().join("sub/unrelated.txt"), b"unrelated")?;
        let mut checkout = checkout(if stale { &[("/sub", "old")] } else { &[] }).await?;
        let child = path("/sub/changed.txt")?;
        replay(
            &mut checkout,
            &options(source.path(), 2)?,
            vec![if stale {
                WatchChange::Created(child)
            } else {
                WatchChange::Modified(child)
            }],
        )
        .await?;
        assert_records(
            &mut checkout,
            &[
                ("/sub", Some(FileKind::Directory)),
                ("/sub/changed.txt", Some(FileKind::Regular)),
                ("/sub/unrelated.txt", None),
            ],
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_modified_directory_keeps_what_it_holds() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("pkg"))?;
    std::fs::write(source.path().join("pkg/shared.rs"), b"VALUE = 1")?;
    let mut checkout = checkout(&[]).await?;
    let options = options(source.path(), 8)?;
    capture_baseline(
        &mut checkout,
        &options,
        &CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    replay(
        &mut checkout,
        &options,
        vec![WatchChange::Modified(path("/pkg")?)],
    )
    .await?;
    assert_records(
        &mut checkout,
        &[("/pkg/shared.rs", Some(FileKind::Regular))],
    )
    .await
}

#[tokio::test]
async fn watch_directory_rename_then_child_change_keeps_the_renamed_directory()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("old"))?;
    std::fs::write(source.path().join("old/file.txt"), b"old")?;
    let mut checkout = checkout(&[]).await?;
    let options = options(source.path(), 3)?;
    capture_baseline(
        &mut checkout,
        &options,
        &CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    std::fs::rename(source.path().join("old"), source.path().join("new"))?;
    std::fs::write(source.path().join("new/file.txt"), b"new")?;
    replay(
        &mut checkout,
        &options,
        vec![
            WatchChange::Renamed {
                from: path("/old")?,
                to: path("/new")?,
            },
            WatchChange::Modified(path("/new/file.txt")?),
        ],
    )
    .await?;
    assert_records(
        &mut checkout,
        &[
            ("/old", None),
            ("/new", Some(FileKind::Directory)),
            ("/new/file.txt", Some(FileKind::Regular)),
        ],
    )
    .await
}

#[cfg(not(target_os = "macos"))]
#[tokio::test]
async fn watch_expands_only_directories_that_appeared() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    for directory in ["modified", "arrived"] {
        std::fs::create_dir(source.path().join(directory))?;
        std::fs::write(source.path().join(directory).join("inner.txt"), b"x")?;
    }
    let mut checkout = checkout(&[]).await?;
    replay(
        &mut checkout,
        &options(source.path(), 8)?,
        vec![
            WatchChange::Modified(path("/modified")?),
            WatchChange::Arrived(path("/arrived")?),
        ],
    )
    .await?;
    assert_records(
        &mut checkout,
        &[
            ("/modified", Some(FileKind::Directory)),
            ("/modified/inner.txt", None),
            ("/arrived", Some(FileKind::Directory)),
            ("/arrived/inner.txt", Some(FileKind::Regular)),
        ],
    )
    .await
}

#[tokio::test]
async fn watch_rename_chain_observes_intermediate_parent() -> Result<(), Box<dyn std::error::Error>>
{
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("sub"))?;
    std::fs::write(source.path().join("final.txt"), b"old")?;
    let mut checkout = checkout(&[("/old.txt", "old")]).await?;
    replay(
        &mut checkout,
        &options(source.path(), 3)?,
        vec![
            WatchChange::Renamed {
                from: path("/old.txt")?,
                to: path("/sub/intermediate.txt")?,
            },
            WatchChange::Renamed {
                from: path("/sub/intermediate.txt")?,
                to: path("/final.txt")?,
            },
        ],
    )
    .await?;
    assert_records(
        &mut checkout,
        &[("/old.txt", None), ("/final.txt", Some(FileKind::Regular))],
    )
    .await
}
