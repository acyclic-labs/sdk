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

#[tokio::test]
async fn failed_projection_cleanup_refunds_no_work_and_retries_on_original_budget()
-> Result<(), Box<dyn std::error::Error>> {
    use super::super::cleanup::{finish_projection_cleanup, prepare_projection_cleanup};
    // Empty and populated projections must both recover after root-tag failure.
    for count in 0..3 {
        let cancellation = CancellationToken::new();
        let ledger = Arc::new(crate::demand::NativeDemandReadMeter::new(
            crate::demand::DemandWork::new(
                WorkBudget {
                    backend_write_operations: 256,
                    ..WorkBudget::UNBOUNDED
                },
                &cancellation,
            ),
        ));
        let work = crate::NativeMaterializationWork::from_meter(Arc::clone(&ledger));
        let source = windows_checkout_source().await?;
        for index in 0..count {
            source.create_file(
                &windows_path(&format!("file-{index}")),
                crate::kernel::FileMetadata::default(),
                None,
                &work,
            )?;
        }
        let (_root, destination, mut session) = mount_source(&source)?;
        let mut cleanup = prepare_projection_cleanup(
            &destination,
            session.native_root_identity().unwrap(),
            source.as_ref(),
            &work,
        )?;
        session.stop()?;
        // An ordinary file cannot accept FSCTL_DELETE_REPARSE_POINT. Retain
        // the authentic root handle while forcing actual Windows failures.
        let authentic_root = std::mem::replace(&mut cleanup.1, tempfile::tempfile()?);
        cancellation.cancel();
        let allocations = work.receipt().allocation_operations;
        for attempt in 0..4 {
            let before = work.receipt().backend_write_operations;
            let error = finish_projection_cleanup(&mut cleanup).unwrap_err();
            assert!(matches!(error, NativeMountError::Io(_)), "{error:?}");
            assert!(cleanup.0.is_empty());
            if attempt > 0 {
                assert_eq!(work.receipt().backend_write_operations, before + 1);
            }
        }
        // Negative control: the former fixed reservation denies the next
        // write instead of attempting Windows again, despite original budget.
        assert!(
            cleanup
                .3
                .started_io(WriteOperations.delta(1), None)
                .is_err()
        );
        let before = work.receipt();
        ledger.lock().budget.backend_write_operations = before.backend_write_operations;
        assert!(finish_projection_cleanup(&mut cleanup).is_err());
        assert_eq!(work.receipt(), before, "denied refill performed work");
        ledger.lock().budget.backend_write_operations = 256;
        cleanup.1 = authentic_root;
        finish_projection_cleanup(&mut cleanup)?;
        assert_eq!(
            work.receipt().backend_write_operations,
            before.backend_write_operations + 1
        );
        assert_eq!(work.receipt().allocation_operations, allocations);
        drop(cleanup);
        assert_eq!(super::reparse_tag(&destination)?.0, None);
        assert!(sorted_names(&destination)?.is_empty());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn command_retirement_retries_the_physical_owner_before_live_controls()
-> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let filesystem = Fs::memory();
    let workspace = filesystem
        .create_workspace("retry-command-owner", WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value;
    workspace.write_text("/result.txt", "published").await?;
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
    let authentic_root = {
        let mut owner = mount.session.lock().unwrap();
        let session = owner.as_mut().unwrap();
        session.prepare_projection_cleanup()?;
        let cleanup = session
            .driver
            .as_mut()
            .unwrap()
            .projection_cleanup
            .as_mut()
            .unwrap();
        std::mem::replace(&mut cleanup.1, tempfile::tempfile()?)
    };
    // Actual Windows root-tag deletion fails only after the runtime and all
    // cleanup ownership move into the physical worker.
    assert!(mount.complete_command(true).await.is_err());
    assert!(mount.is_retiring());
    {
        let mut session = mount.session.lock().unwrap();
        let driver = session.as_mut().unwrap().driver.as_mut().unwrap();
        assert!(driver.runtime.is_none());
        let controller = driver.controller.as_ref().unwrap();
        let mut lifecycle = super::super::lock_recover(&controller.lifecycle);
        let physical = lifecycle.owner.as_mut().unwrap();
        physical.projection_cleanup.as_mut().unwrap().1 = authentic_root;
    }
    // Negative control: the former retry begins with a live callback flush,
    // although the runtime now belongs to the physical retirement worker.
    assert!(mount.sync().await.is_err());
    mount.complete_command(true).await?;
    assert!(mount.session.lock().unwrap().is_none());
    assert!(!destination.exists());
    mount.complete_command(true).await?;
    Ok(())
}
