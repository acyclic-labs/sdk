#[tokio::test]
#[ignore = "requires a host that permits mounting a writable ProjFS provider"]
async fn enumeration_survives_concurrent_projected_writes() -> Result<(), Box<dyn std::error::Error>>
{
    let work = setup_work()?;
    let root = tempfile::tempdir()?;
    let workspace = local_workspace(root.path(), "enumeration-rebase", &work).await?;
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), &work, work.cancellation())
        .await?;
    for index in 0..300 {
        transaction
            .create_file(
                &format!("/entry{index:03}"),
                Bytes::from_static(b"initial"),
                crate::kernel::FileMetadata::default(),
                &work,
                work.cancellation(),
            )
            .await?;
    }
    for name in ["Alpha", "Zebra", "éclair"] {
        transaction
            .create_file(
                &format!("/{name}"),
                Bytes::from_static(b"initial"),
                crate::kernel::FileMetadata::default(),
                &work,
                work.cancellation(),
            )
            .await?;
    }
    transaction
        .commit::<crate::WorkspaceError, _, _>(
            None,
            crate::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            work.cancellation(),
            |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::default(),
                }))
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    let path = root.path().join("mount");
    std::fs::create_dir(&path)?;
    let mount = workspace.mount(&path, manual_mount_options()).await?;
    let initial = std::fs::read_dir(&path)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    let initial_count = initial.len();
    let barrier = Arc::new(Barrier::new(2));
    let writer_barrier = Arc::clone(&barrier);
    let writer_path = path.join("entry000");
    let writer = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        writer_barrier.wait();
        for index in 0..32 {
            std::fs::write(&writer_path, format!("update-{index}"))?;
        }
        Ok(())
    });
    let enumeration = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        barrier.wait();
        for _ in 0..64 {
            let names = std::fs::read_dir(&path)?
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect::<std::io::Result<Vec<_>>>()?;
            let entries = names.iter().cloned().collect::<HashSet<_>>();
            assert_eq!(
                names.len(),
                303,
                "projected entries changed or duplicated: {:?}",
                names
                    .iter()
                    .filter(|name| names.iter().filter(|other| other == name).count() > 1)
                    .collect::<HashSet<_>>()
            );
            assert_eq!(entries.len(), 303, "projected entries were duplicated");
            for index in 0..300 {
                assert!(entries.contains(std::ffi::OsStr::new(&format!("entry{index:03}"))));
            }
            for name in ["Alpha", "Zebra", "éclair"] {
                assert!(entries.contains(std::ffi::OsStr::new(name)));
            }
        }
        Ok(())
    });
    let (written, enumerated) = tokio::join!(writer, enumeration);
    let synchronized = mount.sync().await;
    let unmounted = mount.unmount().await;
    assert_eq!(
        initial_count, 303,
        "static projected enumeration is incomplete"
    );
    written??;
    enumerated??;
    synchronized?;
    unmounted?;
    Ok(())
}
