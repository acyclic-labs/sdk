use super::*;

struct RunningControlGate {
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    released: Mutex<std::sync::mpsc::Receiver<()>>,
    exited: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl<S> tracing_subscriber::Layer<S> for RunningControlGate
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_enter(&self, id: &tracing::span::Id, context: tracing_subscriber::layer::Context<'_, S>) {
        if context.span(id).unwrap().metadata().name() == "acyclic.runtime.blocking_io"
            && let Some(entered) = self.entered.lock().unwrap().take()
        {
            let _ = entered.send(());
            // Gate the actual native worker, after it enters the control's
            // originating span. Dropping the sender also releases a failed test.
            let _ = self.released.lock().unwrap().recv();
        }
    }

    fn on_exit(&self, id: &tracing::span::Id, context: tracing_subscriber::layer::Context<'_, S>) {
        if context.span(id).unwrap().metadata().name() == "acyclic.runtime.blocking_io"
            && let Some(exited) = self.exited.lock().unwrap().take()
        {
            let _ = exited.send(());
        }
    }
}

#[tokio::test]
async fn running_async_controls_keep_original_work_after_caller_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::native_mount::session::NativeSessionControl;
    use tracing_subscriber::prelude::*;

    crate::initialize_native_mount_services()?;
    let source = windows_checkout_source().await?;
    let (_root, _destination, mut session) = mount_source(&source)?;
    let work = session.driver.as_ref().unwrap().original_stop_work.clone();
    let foreign =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    for control in [
        NativeSessionControl::FlushCallbacks,
        NativeSessionControl::Revalidate,
    ] {
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let (exited, finished) = tokio::sync::oneshot::channel();
        let subscriber = tracing_subscriber::registry().with(RunningControlGate {
            entered: Mutex::new(Some(entered)),
            released: Mutex::new(released),
            exited: Mutex::new(Some(exited)),
        });
        let mut completion = tracing::subscriber::with_default(subscriber, || {
            session.start_control(&work, control)
        })?
        .ok_or("live control has no completion")?;
        tokio::select! {
            started = started => started?,
            _ = &mut completion => panic!("control completed before its worker gate released"),
        }
        drop(completion);
        assert!(matches!(
            session.stop_with_cleanup_wait(&CancellationToken::new()),
            Err(NativeMountError::ControlPending)
        ));
        assert!(session.begin_original_work(&foreign).is_err());
        release.send(())?;
        finished.await?;
        drop(session.begin_original_work(&foreign)?);
    }
    session.stop()?;
    Ok(())
}

#[tokio::test]
async fn async_controls_retain_original_work_until_completion_or_drop()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::native_mount::session::NativeSessionControl;

    crate::initialize_native_mount_services()?;
    let source = windows_checkout_source().await?;
    let (_root, _destination, mut session) = mount_source(&source)?;
    let work = session.driver.as_ref().unwrap().original_stop_work.clone();
    let foreign =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    for control in [
        NativeSessionControl::FlushCallbacks,
        NativeSessionControl::Revalidate,
    ] {
        for finish in [false, true] {
            let completion = session.start_control(&work, control)?.unwrap();
            // Submission owns the invocation before its first poll. A stop
            // cannot take the SDK runtime away from this physical operation.
            assert!(matches!(
                session.stop_with_cleanup_wait(&CancellationToken::new()),
                Err(NativeMountError::ControlPending)
            ));
            assert!(session.begin_original_work(&foreign).is_err());
            if finish {
                completion.await??;
            } else {
                drop(completion);
            }
            // Both completion and cancellation release exactly that claim.
            drop(session.begin_original_work(&foreign)?);
        }
    }
    session.stop()?;
    assert!(
        session
            .start_control(&work, NativeSessionControl::FlushCallbacks)?
            .is_none()
    );
    let error = session
        .start_control(&work, NativeSessionControl::Revalidate)
        .err()
        .ok_or("retired session accepted revalidation")?;
    assert!(
        matches!(error, NativeMountError::Io(error) if error.kind() == std::io::ErrorKind::NotConnected)
    );
    Ok(())
}

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
