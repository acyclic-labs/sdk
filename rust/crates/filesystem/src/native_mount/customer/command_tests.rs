use super::tests::test_work;
use super::*;
use std::cell::RefCell;

thread_local! {
    static TRACE: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

pub(super) fn record(operation: &'static str) {
    TRACE.with_borrow_mut(|trace| trace.push(operation));
}

fn take_trace() -> Vec<&'static str> {
    TRACE.with_borrow_mut(std::mem::take)
}

type MemoryCommand = Mount<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>;

async fn command_final_join(revoke: bool, guarded: bool) -> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    let cancellation = crate::CancellationToken::new();
    let work = test_work(crate::WorkBudget::UNBOUNDED, &cancellation)?;
    let filesystem = crate::Fs::memory();
    let workspace = filesystem
        .create_workspace(
            "command-final-join",
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    workspace.write_text("/result", "original").await?;
    let destination = tempfile::tempdir()?;
    let command = private_command(&workspace, &work, destination.path(), false).await?;
    let path = crate::native_mount::adapter::tests::native_test_path("result");
    command.source.open_file(&path)?.write_range_with_meter(
        0,
        crate::kernel::OwnedSourceChunk::claimed(
            acyclic_stream::OwnedBytes::from_static(b"verified"),
            0,
        ),
        false,
        &work,
    )?;
    let (fork, custody) = command
        .complete_private_command()
        .await?
        .ok_or("missing fork")?;
    if revoke {
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
                &work,
                &cancellation,
            )
            .await?;
        let policy = crate::access::fixtures::denied(crate::access::Action::Write, "result");
        checkout
            .write_access_policy(
                &serde_json::to_vec(&policy)?,
                crate::NamedAttributeWriteMode::Upsert,
                &work,
                &cancellation,
            )
            .await?;
        checkout
            .commit(
                crate::OperationId::new(),
                None,
                None,
                None,
                crate::PublicationPermit::Unrestricted,
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::FsError>>(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    })
                },
            )
            .await
            .map_err(|failure| failure.error)?;
    }
    // Replan after revocation so a stale target alone cannot make this proof
    // pass: the real original-volume authorization seal must refuse it.
    let plan = fork
        .join_into(&workspace)
        .git_metadata()
        .plan(crate::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value;
    let options = crate::ApplyOptions {
        if_target: plan.target_head(),
        idempotency_key: crate::IdempotencyKey::new(),
    };
    if guarded {
        let outcome = command
            .apply_private_command_join(&plan, options, None)
            .await;
        if revoke {
            assert!(matches!(
                outcome,
                Err(MountLifecycleError::Source(MountSourceError::AccessDenied))
            ));
        } else {
            let (outcome, _allocation) = outcome?;
            assert!(matches!(outcome, crate::JoinOutcome::Applied(_)));
        }
    } else {
        plan.apply(
            options,
            Default::default(),
            crate::PublicationPermit::Unrestricted,
        )
        .await?;
    }
    let bytes = workspace.read("/result", 8, &work, &cancellation).await?;
    if revoke {
        assert_eq!(
            bytes.as_ref(),
            b"original",
            "revoked command reached original volume"
        );
    } else {
        assert_eq!(bytes.as_ref(), b"verified");
    }
    custody.settle().await?;
    Ok(())
}

#[tokio::test]
async fn command_final_join_rechecks_original_authority() -> Result<(), Box<dyn std::error::Error>>
{
    command_final_join(false, true).await?;
    command_final_join(true, true).await
}

#[tokio::test]
#[should_panic(expected = "revoked command reached original volume")]
async fn command_final_join_negative_control_detects_unrestricted_apply() {
    command_final_join(true, false).await.unwrap();
}

async fn private_command(
    workspace: &Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>,
    work: &NativeMaterializationWork,
    destination: &Path,
    read_only: bool,
) -> Result<MemoryCommand, Box<dyn std::error::Error>> {
    let mut options =
        super::tests::fixture_options(workspace, MountOptions::read_write().private_command())
            .await?
            .native_context(work, None, None);
    if read_only {
        options = options.restrict_to_read_only();
    }
    let source = workspace.native_source(&options).await?;
    Ok(Mount {
        source,
        session: CustomerSession::new(None, work)?,
        destination: destination.to_owned(),
        _destination_custody: work.path(destination, None)?.1,
        retiring: std::sync::atomic::AtomicBool::new(false),
    })
}

#[tokio::test]
async fn private_commands_fork_at_first_write_and_keep_the_authorized_source()
-> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    let cancellation = crate::CancellationToken::new();
    let work = test_work(crate::WorkBudget::UNBOUNDED, &cancellation)?;
    let filesystem = crate::Fs::memory();
    let workspace = filesystem
        .create_workspace(
            "private-command",
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    workspace.write_text("/result.txt", "original").await?;
    let origin = workspace.head(&work, &cancellation).await?.id();
    let destination = tempfile::tempdir()?;
    let path = crate::native_mount::adapter::tests::native_test_path("result.txt");
    let write = |mount: &MemoryCommand, bytes: &'static [u8]| {
        mount.source.open_file(&path)?.write_range_with_meter(
            0,
            crate::kernel::OwnedSourceChunk::claimed(
                acyclic_stream::OwnedBytes::from_static(bytes),
                0,
            ),
            false,
            &work,
        )
    };

    let no_write = private_command(&workspace, &work, destination.path(), false).await?;
    assert_eq!(no_write.source.volume_id()?, workspace.id().volume_id());
    assert_eq!(
        no_write.source.read_range(&path, 0, 8)?.as_ref(),
        b"original"
    );
    assert!(
        no_write.complete_private_command().await?.is_none(),
        "read-only work created a fork"
    );

    let first = private_command(&workspace, &work, destination.path(), false).await?;
    let second = private_command(&workspace, &work, destination.path(), false).await?;
    write(&first, b"first!!!")?;
    write(&first, b"again!!!")?;
    write(&second, b"second!!")?;
    assert_eq!(first.source.volume_id()?, workspace.id().volume_id());
    assert_eq!(first.source.read_range(&path, 0, 8)?.as_ref(), b"again!!!");
    assert_eq!(second.source.read_range(&path, 0, 8)?.as_ref(), b"second!!");
    assert_eq!(workspace.head(&work, &cancellation).await?.id(), origin);
    let (first_fork, first_custody) = first
        .complete_private_command()
        .await?
        .ok_or("missing first fork")?;
    let (second_fork, second_custody) = second
        .complete_private_command()
        .await?
        .ok_or("missing second fork")?;
    assert_ne!(first_fork.id(), second_fork.id());
    assert_eq!(
        first_fork
            .read("/result.txt", 8, &work, &cancellation)
            .await?
            .as_ref(),
        b"again!!!"
    );
    assert_eq!(
        second_fork
            .read("/result.txt", 8, &work, &cancellation)
            .await?
            .as_ref(),
        b"second!!"
    );
    assert_eq!(workspace.head(&work, &cancellation).await?.id(), origin);
    first_custody.settle().await?;
    second_custody.settle().await?;

    let read_only = private_command(&workspace, &work, destination.path(), true).await?;
    assert!(matches!(
        write(&read_only, b"forbidden"),
        Err(MountSourceError::ReadOnly)
    ));
    assert!(read_only.complete_private_command().await?.is_none());

    // Independent ordinary-view control proves the source-head oracle sees
    // publication, rather than merely observing an unchanging fixture.
    let options = super::tests::fixture_options(
        &workspace,
        MountOptions::read_write().publication(MountPublication::Manual),
    )
    .await?
    .native_context(&work, None, None);
    let control = workspace.native_source(&options).await?;
    control.open_file(&path)?.write_range_with_meter(
        0,
        crate::kernel::OwnedSourceChunk::claimed(
            acyclic_stream::OwnedBytes::from_static(b"control!"),
            0,
        ),
        false,
        &work,
    )?;
    control.sync_async(PublicationPermit::Unrestricted).await?;
    assert_ne!(workspace.head(&work, &cancellation).await?.id(), origin);
    Ok(())
}

// Each candidate/control has its own filesystem, checkout, source and journal.
// No native driver is needed to test the real command publication dispatch;
// destination retirement is proved separately against the actual fence packet.
async fn publish_command(
    duplicate_sync: bool,
    retire: bool,
) -> Result<(Vec<u8>, Vec<&'static str>), Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    let cancellation = crate::CancellationToken::new();
    let work = test_work(crate::WorkBudget::UNBOUNDED, &cancellation)?;
    let filesystem = crate::Fs::memory();
    let workspace = filesystem
        .create_workspace(
            "command-publication",
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace.write_text("/result.txt", "original").await?;
    let original = workspace.head(&work, &cancellation).await?;
    let destination = tempfile::tempdir()?;
    let working = original
        .prepare_native_working_set(
            &super::tests::fixture_options(
                &workspace,
                MountOptions::read_write().publication(MountPublication::Manual),
            )
            .await?,
            &recording_options(destination.path()),
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let mount = Mount {
        source: working.source.clone(),
        session: CustomerSession::new(None, &work)?,
        destination: destination.path().to_owned(),
        _destination_custody: work.path(destination.path(), None)?.1,
        retiring: std::sync::atomic::AtomicBool::new(false),
    };
    let path = {
        #[cfg(unix)]
        let name = b"result.txt".to_vec();
        #[cfg(windows)]
        let name = "result.txt"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        MountPath::root().child(name)
    };
    mount.source.open_file(&path)?.write_range_with_meter(
        0,
        crate::kernel::OwnedSourceChunk::claimed(
            acyclic_stream::OwnedBytes::from_static(b"updated!"),
            0,
        ),
        false,
        &work,
    )?;
    take_trace();
    if duplicate_sync {
        // Restore exactly the previous sync -> advance -> unmount path.
        mount.sync().await?;
        mount.advance_to_head().await?;
        mount.unmount().await?;
    } else {
        mount.complete_command(retire).await?;
    }
    let trace = take_trace();
    let reopened = workspace.head(&work, &cancellation).await?;
    assert_ne!(
        reopened.id(),
        original.id(),
        "command was acknowledged before publication"
    );
    let bytes = workspace
        .read("/result.txt", 64, &work, &cancellation)
        .await?;
    Ok((bytes.as_ref().to_vec(), trace))
}

#[tokio::test]
async fn command_completion_publishes_once_with_an_independent_duplicate_sync_control()
-> Result<(), Box<dyn std::error::Error>> {
    let (bytes, trace) = publish_command(false, true).await?;
    assert_eq!(bytes, b"updated!");
    assert_eq!(trace, ["sync", "detach"]);
    let (baseline_bytes, duplicate_trace) = publish_command(true, true).await?;
    assert_eq!(baseline_bytes, bytes);
    assert_eq!(duplicate_trace, ["sync", "advance", "sync", "detach"]);
    assert_ne!(
        duplicate_trace, trace,
        "duplicate sync escaped the production trace oracle"
    );
    let (retained_bytes, retained_trace) = publish_command(false, false).await?;
    assert_eq!(retained_bytes, bytes);
    assert_eq!(retained_trace, ["sync", "advance"]);
    Ok(())
}

// Every task-view backend: Linux shares one instance, macOS mounts each view
// at a directory issued for it. Each view's retirement removes only its path.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn view_completion_unmount_and_abandon_retire_only_their_view()
-> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    let cancellation = crate::CancellationToken::new();
    let work = test_work(crate::WorkBudget::UNBOUNDED, &cancellation)?;
    let filesystem = crate::Fs::memory();
    let workspace = filesystem
        .create_workspace(
            "shared-command",
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace.write_text("/result.txt", "original").await?;
    let root = tempfile::tempdir()
        .inspect_err(|error| eprintln!("view temporary directory: {error:?}"))?;
    let admission = Arc::new(crate::hold_root(root.path())?);
    let options = super::tests::fixture_options(
        &workspace,
        MountOptions::read_write()
            .publication(MountPublication::Manual)
            .view_root(admission.clone()),
    )
    .await?;
    let retained = workspace.mount_view(options.clone()).await?;
    // A startup that fails after issuance leaves nothing behind: the view's
    // issuance owns its directory from creation, not from a successful mount.
    let listed = || {
        std::fs::read_dir(root.path())
            .inspect_err(|error| eprintln!("view admission directory listing: {error:?}"))
            .map(Iterator::count)
    };
    let before = listed()?;
    let unauthorized = MountOptions::read_write().view_root(admission);
    assert!(workspace.mount_view(unauthorized).await.is_err());
    assert_eq!(listed()?, before);
    let completed = workspace.mount_view(options.clone()).await?;
    assert_eq!(retained.path().parent(), completed.path().parent());
    std::fs::write(retained.path().join("still-live.txt"), b"retained")
        .inspect_err(|error| eprintln!("retained view kernel write: {error:?}"))?;
    std::fs::write(completed.path().join("result.txt"), b"captured")
        .inspect_err(|error| eprintln!("completed view kernel write: {error:?}"))?;
    {
        let mut owner = completed
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let session = owner.as_mut().ok_or("missing native session")?;
        assert!(session.destination_guard.is_none());
        // Both dispatch layers accept cleanup without claiming physical-root
        // custody. Only retirement of this lease may remove its namespace.
        let driver = session.driver.as_mut().ok_or("missing native driver")?;
        driver.prepare_destination_cleanup(completed.path(), &work)?;
        assert!(completed.path().exists());
    }
    let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::new();
    let completion = schedule.scope(completed.complete_command(true));
    tokio::pin!(completion);
    loop {
        tokio::select! {
            result = &mut completion => { result?; break; }
            boundary = events.recv() => {
                boundary.ok_or("retirement boundary producer dropped")?.release();
            }
        }
    }
    schedule.drained().await;
    assert!(!completed.path().exists());
    assert_eq!(
        workspace
            .read("/result.txt", 64, &work, &cancellation)
            .await?
            .as_ref(),
        b"captured"
    );
    assert_eq!(
        std::fs::read(retained.path().join("still-live.txt"))
            .inspect_err(|error| eprintln!("retained view read after completion: {error:?}"))?,
        b"retained"
    );

    let abandoned = workspace.mount_view(options.clone()).await?;
    assert_eq!(retained.path().parent(), abandoned.path().parent());
    std::fs::write(abandoned.path().join("result.txt"), b"unverified")
        .inspect_err(|error| eprintln!("abandoned view kernel write: {error:?}"))?;
    abandoned.abandon_command().await?;
    assert!(!abandoned.path().exists());
    assert_eq!(
        workspace
            .read("/result.txt", 64, &work, &cancellation)
            .await?
            .as_ref(),
        b"captured"
    );
    assert_eq!(
        std::fs::read(retained.path().join("still-live.txt"))
            .inspect_err(|error| eprintln!("retained view read after abandonment: {error:?}"))?,
        b"retained"
    );
    let cancelled = workspace.mount_view(options.clone()).await?;
    let cancelled_path = cancelled.path().to_owned();
    let cancelled_session = Arc::downgrade(&cancelled.session);
    let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::new();
    let mut retirement = Box::pin(schedule.scope(cancelled.abandon_command()));
    let accepted = loop {
        tokio::select! {
            result = &mut retirement => {
                result?;
                return Err("retirement completed before its submission boundary".into());
            }
            boundary = events.recv() => {
                let boundary = boundary.ok_or("retirement submission producer dropped")?;
                if boundary.name == "blocking:submitted" && cancelled.is_retiring() {
                    break boundary;
                }
                boundary.release();
            }
        }
    };
    assert!(cancelled.is_retiring());
    assert!(schedule.pending_native_work() > 0);
    schedule.cancel();
    drop(retirement);
    drop(cancelled);
    assert!(cancelled_session.upgrade().is_some());
    accepted.release();
    let drained = schedule.drained();
    tokio::pin!(drained);
    loop {
        tokio::select! {
            () = &mut drained => break,
            boundary = events.recv() => {
                boundary.ok_or("accepted retirement producer dropped")?.release();
            }
        }
    }
    assert!(cancelled_session.upgrade().is_none());
    assert!(!cancelled_path.exists());
    assert_eq!(
        std::fs::read(retained.path().join("still-live.txt"))
            .inspect_err(|error| eprintln!("retained view read after cancellation: {error:?}"))?,
        b"retained"
    );
    let unmounted = workspace.mount_view(options).await?;
    assert_eq!(retained.path().parent(), unmounted.path().parent());
    assert!(unmounted.path().is_dir());
    unmounted.unmount().await?;
    assert!(!unmounted.path().exists());
    retained.abandon_command().await?;
    assert!(!retained.path().exists());
    Ok(())
}

// Both callbacks begin before either is joined. The negative control gives
// them independent command candidates, so the survivor oracle must see a lost
// mutation even though both writes themselves returned success.
async fn concurrent_command_first_writes(shared: bool) -> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    let cancellation = crate::CancellationToken::new();
    let work = test_work(crate::WorkBudget::UNBOUNDED, &cancellation)?;
    let filesystem = crate::Fs::memory();
    let workspace = filesystem
        .create_workspace(
            "concurrent-command-promotion",
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    workspace.write_text("/left", "original").await?;
    workspace.write_text("/right", "original").await?;
    let origin = workspace.head(&work, &cancellation).await?.id();
    let destination = tempfile::tempdir()?;
    let command = private_command(&workspace, &work, destination.path(), false).await?;
    let independent = if shared {
        None
    } else {
        Some(private_command(&workspace, &work, destination.path(), false).await?)
    };
    let left = crate::native_mount::adapter::tests::native_test_path("left");
    let right = crate::native_mount::adapter::tests::native_test_path("right");
    let handles = [
        command.source.open_file(&left)?,
        independent
            .as_ref()
            .unwrap_or(&command)
            .source
            .open_file(&right)?,
    ];
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|threads| -> Result<(), Box<dyn std::error::Error>> {
        let tasks = handles.map(|handle| {
            let barrier = &barrier;
            let work = &work;
            threads.spawn(move || {
                barrier.wait();
                handle.write_range_with_meter(
                    0,
                    crate::kernel::OwnedSourceChunk::claimed(
                        acyclic_stream::OwnedBytes::from_static(b"updated!"),
                        0,
                    ),
                    false,
                    work,
                )
            })
        });
        for task in tasks {
            task.join().map_err(|_| "first-write callback panicked")??;
        }
        Ok(())
    })?;
    assert_eq!(workspace.head(&work, &cancellation).await?.id(), origin);
    for path in [&left, &right] {
        assert_eq!(
            command.source.read_range(path, 0, 8)?.as_ref(),
            b"updated!",
            "concurrent first writes lost a mutation"
        );
    }
    let (fork, custody) = command
        .complete_private_command()
        .await?
        .ok_or("missing fork")?;
    for path in ["/left", "/right"] {
        assert_eq!(
            fork.read(path, 8, &work, &cancellation).await?.as_ref(),
            b"updated!"
        );
    }
    custody.settle().await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_command_first_writes_share_one_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    concurrent_command_first_writes(true).await
}

#[tokio::test]
#[should_panic(expected = "concurrent first writes lost a mutation")]
async fn concurrent_command_first_writes_negative_control_detects_split_candidates() {
    concurrent_command_first_writes(false).await.unwrap();
}
