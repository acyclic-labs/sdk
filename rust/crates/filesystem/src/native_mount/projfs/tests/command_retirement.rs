use super::*;

async fn retire_after_cancellation(
    fresh_cleanup_credit: bool,
) -> Result<u64, Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let filesystem = Fs::memory();
    let workspace = filesystem
        .create_workspace(
            "accepted-command-retirement",
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    workspace.write_text("/result.txt", "durable").await?;
    let parent = tempfile::tempdir()?;
    let destination = parent.path().join("projection");
    std::fs::create_dir(&destination)?;
    let mount = workspace
        .mount(
            &destination,
            crate::MountOptions::read_write()
                .publication(crate::MountPublication::Manual)
                .native_context(&work, None, None),
        )
        .await?;
    mount.sync().await?;
    assert_eq!(std::fs::read(destination.join("result.txt"))?, b"durable");
    let [fence, registry] = crate::native_mount_control_paths(&destination)?;
    let fresh =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    {
        let mut owner = mount.session.lock().unwrap();
        let session = owner.as_mut().unwrap();
        session.prepare_destination_cleanup(&work)?;
        session.prepare_projection_cleanup()?;
        if fresh_cleanup_credit {
            // Restore the old fresh-ledger cleanup while keeping the real
            // driver, projected handles, publication and physical output.
            session
                .driver
                .as_mut()
                .unwrap()
                .projection_cleanup
                .as_mut()
                .unwrap()
                .3 = fresh.reserve_work(crate::WorkCounters {
                backend_write_operations: 3,
                ..crate::WorkCounters::UNCHARGED
            })?;
        }
    }
    let before = work.receipt();
    cancellation.cancel();
    mount.abandon()?;
    assert!(!destination.exists());
    assert!(!fence.exists());
    assert!(
        registry.is_file(),
        "retirement unlinked the shared registry"
    );
    assert!(mount.session.lock().unwrap().is_none());
    Ok(work.receipt().backend_write_operations - before.backend_write_operations)
}

#[tokio::test(flavor = "multi_thread")]
async fn accepted_command_cleanup_finishes_on_original_credit_after_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    // SDK stop + physical worker handle close + one projected file + root
    // tag + exact empty destination + registry lock/fence unlink/unlock.
    let expected = 8;
    assert_eq!(retire_after_cancellation(false).await?, expected);
    assert_ne!(
        retire_after_cancellation(true).await?,
        expected,
        "fresh cleanup ledger escaped the original-credit oracle",
    );
    Ok(())
}
