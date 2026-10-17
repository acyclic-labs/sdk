use super::*;

#[tokio::test]
async fn concurrent_initializers_publish_one_complete_checkout() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let destination = directory.path().join("project");
    let a = staging(directory.path())?;
    let b = staging(directory.path())?;
    assert_ne!(a, b);
    let attempt = |path: PathBuf, contents: &'static str| {
        let destination = &destination;
        let directory = directory.path();
        async move {
            initialize(
                async {
                    std::fs::write(path.join("first"), contents).map_err(storage)?;
                    tokio::task::yield_now().await;
                    std::fs::write(path.join("second"), contents).map_err(storage)?;
                    Ok(())
                },
                || {
                    acyclic_native_runtime::durable_rename(
                        &path,
                        destination,
                        acyclic_native_runtime::RenameMode::NoReplace,
                        None,
                    )
                },
                || {
                    acyclic_native_runtime::remove_child_directory(directory, &path)
                        .map_err(storage)
                },
            )
            .await
        }
    };
    let (a_result, b_result) = tokio::join!(attempt(a.clone(), "a"), attempt(b.clone(), "b"));
    a_result?;
    b_result?;
    assert!(!a.exists() && !b.exists());
    assert_eq!(
        std::fs::read(destination.join("first")).map_err(storage)?,
        std::fs::read(destination.join("second")).map_err(storage)?
    );
    // A later first-open attempt must not replace an empty winning directory.
    let destination = directory.path().join("empty-winner");
    std::fs::create_dir(&destination).map_err(storage)?;
    let another = staging(directory.path())?;
    std::fs::write(another.join("file"), "loser").map_err(storage)?;
    initialize(
        std::future::ready(Ok(())),
        || {
            acyclic_native_runtime::durable_rename(
                &another,
                &destination,
                acyclic_native_runtime::RenameMode::NoReplace,
                None,
            )
        },
        || {
            acyclic_native_runtime::remove_child_directory(directory.path(), &another)
                .map_err(storage)
        },
    )
    .await?;
    assert!(!destination.join("file").exists());
    Ok(())
}
