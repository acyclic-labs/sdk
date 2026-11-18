use acyclic_fs::model::VolumeLimits;
use acyclic_fs::path::PortablePath;
use acyclic_fs::{
    CancellationToken, Fs, LocalOptions, ReconcileOutcome, SourceError, SourceOptions, SourceState,
};
use acyclic_fs_work::performance::WorkBudget;

#[tokio::test]
async fn attached_directory_rescans_and_seals_through_one_workspace()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("before.txt"), b"before")?;
    let workspace =
        Box::pin(Fs::memory().attach_directory("repo", root.path(), SourceOptions::default()))
            .await?;
    assert_eq!(
        unbounded!(workspace, read("/before.txt", 16))?,
        acyclic_stream::OwnedBytes::from_static(b"before")
    );
    let source = workspace.source().ok_or("source handle missing")?;
    assert_eq!(source.state().await, SourceState::Clean);

    std::fs::write(root.path().join("before.txt"), b"after")?;
    std::fs::write(root.path().join("added.txt"), b"added")?;
    assert!(matches!(
        Box::pin(source.rescan()).await?,
        ReconcileOutcome::Clean(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/before.txt", 16))?,
        acyclic_stream::OwnedBytes::from_static(b"after")
    );
    assert_eq!(
        unbounded!(workspace, read("/added.txt", 16))?,
        acyclic_stream::OwnedBytes::from_static(b"added")
    );
    let sealed = Box::pin(workspace.seal()).await?;
    assert_eq!(source.state().await, SourceState::Sealed);
    std::fs::remove_dir_all(root.path())?;
    assert_eq!(
        unbounded!(sealed, read("/before.txt", 16))?,
        acyclic_stream::OwnedBytes::from_static(b"after")
    );
    Ok(())
}

#[tokio::test]
async fn attached_directory_excludes_private_prefixes_from_all_rescans()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join(".git"))?;
    std::fs::write(root.path().join(".git").join("config"), b"private")?;
    std::fs::write(root.path().join("visible.txt"), b"visible")?;
    let options = SourceOptions {
        excluded_paths: vec![PortablePath::parse("/.git", VolumeLimits::default())?],
        ..SourceOptions::default()
    };
    let workspace =
        Box::pin(Fs::memory().attach_directory("filtered", root.path(), options)).await?;
    assert_eq!(
        unbounded!(workspace, read("/visible.txt", 16))?,
        acyclic_stream::OwnedBytes::from_static(b"visible")
    );
    assert!(unbounded!(workspace, read("/.git/config", 16)).is_err());

    std::fs::write(root.path().join(".git").join("config"), b"changed")?;
    let source = workspace.source().ok_or("source handle missing")?;
    assert!(matches!(
        Box::pin(source.rescan()).await?,
        ReconcileOutcome::Clean(_)
    ));
    assert!(unbounded!(workspace, read("/.git/config", 16)).is_err());
    Ok(())
}

#[tokio::test]
async fn durable_source_restarts_fail_closed_and_remains_gc_classifiable()
-> Result<(), Box<dyn std::error::Error>> {
    let storage = tempfile::tempdir()?;
    let source_root = tempfile::tempdir()?;
    std::fs::write(source_root.path().join("durable.txt"), b"one")?;
    let local_options = LocalOptions::new(storage.path());

    {
        let fs = Fs::local(local_options.clone()).await?;
        let workspace =
            Box::pin(fs.attach_directory("durable", source_root.path(), SourceOptions::default()))
                .await?;
        let source = workspace.source().ok_or("source missing")?;
        assert_eq!(source.state().await, SourceState::Clean);
    }

    std::fs::write(source_root.path().join("durable.txt"), b"two")?;
    {
        let fs = Fs::local(local_options.clone()).await?;
        let workspace =
            Box::pin(fs.attach_directory("durable", source_root.path(), SourceOptions::default()))
                .await?;
        assert_eq!(
            Box::pin(workspace.read(
                "/durable.txt",
                8,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            ))
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?,
            acyclic_stream::OwnedBytes::from_static(b"two")
        );
        let wrong_root = tempfile::tempdir()?;
        assert!(matches!(
            Box::pin(fs.attach_directory("durable", wrong_root.path(), SourceOptions::default()))
                .await,
            Err(SourceError::BindingMismatch)
        ));
    }

    Ok(())
}
