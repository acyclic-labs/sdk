use super::*;

pub(super) const FINITE_MATERIALIZATION_BUDGET: WorkBudget =
    crate::performance::work_budget(100_000, 64 * 1024 * 1024);

#[test]
fn replaced_root_is_rejected_before_destination_enumeration()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("root");
    std::fs::create_dir(&destination)?;
    let original = HostRoot::open(&destination, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?
        .identity();
    std::fs::rename(&destination, temporary.path().join("original"))?;
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("foreign"), b"untouched")?;
    let actual = HostRoot::open(&destination, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?
        .identity();
    assert_ne!(original, actual);
    let cancellation = CancellationToken::new();
    let mut receipt = GenerationReceipt::new(FINITE_MATERIALIZATION_BUDGET, &cancellation)
        .map_err(|failure| failure.0)?;
    let failure = validate_options(
        &MaterializeOptions {
            expected_root_identity: Some(original),
            ..MaterializeOptions::native(&destination)
        },
        &mut receipt,
    )
    .err()
    .ok_or("replacement was accepted")?;
    assert!(
        matches!(failure, MaterializeError::AllocationChanged { expected, actual: found }
        if expected == original && found == actual)
    );
    assert_eq!(std::fs::read(destination.join("foreign"))?, b"untouched");
    Ok(())
}

#[tokio::test]
async fn multipath_failure_retains_completed_host_and_lookup_work()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::model::{Lifecycle, VolumeConfig, VolumeLimits};
    acyclic_native_runtime::initialize_native_io_services()?;
    let cancellation = CancellationToken::new();
    let limits = VolumeLimits::default();
    let volume = super::super::tests::memory_volume(
        crate::VolumeId::new(),
        VolumeConfig {
            limits,
            ..VolumeConfig::portable(Lifecycle::Ephemeral)
        },
        &cancellation,
    )
    .await?;
    let mut checkout = super::super::tests::private_checkout(&volume, &cancellation).await?;
    let first = NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::Utf8,
            b"a-present".to_vec(),
            limits.maximum_component_bytes,
        )?],
        limits,
    )?;
    let missing = NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::Utf8,
            b"z-missing".to_vec(),
            limits.maximum_component_bytes,
        )?],
        limits,
    )?;
    checkout
        .create(
            crate::AuthoredMutation::CreateFile {
                path: first.clone(),
                bytes: (Bytes::from_static(b"first")).into(),
                metadata: crate::kernel::FileMetadata::default(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let zero_output = tempfile::tempdir()?;
    let zero = materialize_checkout(
        &mut checkout,
        &MaterializeOptions::native(zero_output.path()),
        WorkBudget::default(),
        &cancellation,
    )
    .await
    .err()
    .ok_or("zero materialization budget was accepted")?;
    assert!(matches!(zero.error, MaterializeError::Work(_)));
    assert_eq!(*zero.work, WorkCounters::UNCHARGED);
    assert!(std::fs::read_dir(zero_output.path())?.next().is_none());

    let cancelled_output = tempfile::tempdir()?;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled = materialize_checkout(
        &mut checkout,
        &MaterializeOptions::native(cancelled_output.path()),
        FINITE_MATERIALIZATION_BUDGET,
        &cancelled,
    )
    .await
    .err()
    .ok_or("cancelled materialization was accepted")?;
    assert!(matches!(cancelled.error, MaterializeError::Cancelled(_)));
    assert_eq!(*cancelled.work, WorkCounters::UNCHARGED);
    assert!(std::fs::read_dir(cancelled_output.path())?.next().is_none());

    let no_write_output = tempfile::tempdir()?;
    let no_write = materialize_checkout(
        &mut checkout,
        &MaterializeOptions::native(no_write_output.path()),
        WorkBudget {
            backend_write_operations: 0,
            ..FINITE_MATERIALIZATION_BUDGET
        },
        &cancellation,
    )
    .await
    .err()
    .ok_or("zero native-write budget was accepted")?;
    assert!(matches!(no_write.error, MaterializeError::Work(_)));
    assert_eq!(no_write.work.backend_write_operations, 0);
    assert_eq!(no_write.work.output_bytes, 0);
    assert!(no_write.work.backend_read_operations > 0);
    assert!(no_write.work.allocation_operations > 0);
    assert!(std::fs::read_dir(no_write_output.path())?.next().is_none());

    let baseline = tempfile::tempdir()?;
    let completed = materialize_checkout_paths(
        &mut checkout,
        std::slice::from_ref(&first),
        &MaterializeOptions::native(baseline.path()),
        FINITE_MATERIALIZATION_BUDGET,
        &cancellation,
    )
    .await?;
    let output = tempfile::tempdir()?;
    let failed = materialize_checkout_paths(
        &mut checkout,
        &[first.clone(), missing.clone()],
        &MaterializeOptions::native(output.path()),
        FINITE_MATERIALIZATION_BUDGET,
        &cancellation,
    )
    .await
    .err()
    .ok_or("missing second path was accepted")?;
    assert!(matches!(failed.error, MaterializeError::MissingPath));
    assert_eq!(std::fs::read(output.path().join("a-present"))?, b"first");
    assert!(failed.work.backend_read_operations > completed.work.backend_read_operations);
    let mut original = GenerationReceipt::new(FINITE_MATERIALIZATION_BUDGET, &cancellation)
        .map_err(|failure| failure.0)?;
    let candidate = checkout.private_candidate(original.custody.work(), &cancellation);
    let candidate = candidate?;
    let reader =
        candidate.into_pinned_reader_with_native_work(original.custody.work(), &cancellation);
    let reader = reader?;
    let prefix = original.custody.work().receipt();
    let original_work = original.custody.work().clone();
    let original_output = tempfile::tempdir()?;
    let original_failed = materialize_reader_paths(
        &reader,
        &[first.clone(), missing.clone()],
        &MaterializeOptions::native(original_output.path()),
        MaterializeMode::DurableOutput,
        &original_work,
    )
    .await
    .err()
    .ok_or("original source accepted a missing second path")?;
    assert!(matches!(original_failed, MaterializeError::MissingPath));
    assert_eq!(
        std::fs::read(original_output.path().join("a-present"))?,
        b"first"
    );
    let original_spent = original.custody.work().receipt();
    assert!(original_spent.allocation_operations > prefix.allocation_operations);
    assert!(original_spent.backend_read_operations > prefix.backend_read_operations);
    assert_eq!(original_spent.output_bytes, 5);

    #[derive(Debug, Error)]
    enum TaskFenceFailure {
        #[error("task fence expired before the next logical action")]
        Owner,
        #[error(transparent)]
        Materialize(#[from] MaterializeError),
    }
    for deny_before_output in [true, false] {
        let guarded_output = tempfile::tempdir()?;
        let before = original.custody.work().receipt();
        let mut task_fence = |phase| {
            let denied = phase == NativeMaterializationActionPhase::Before
                && (deny_before_output
                    || original.custody.work().receipt().output_bytes > before.output_bytes);
            std::future::ready(if denied {
                Err(TaskFenceFailure::Owner)
            } else {
                Ok(())
            })
        };
        let error = materialize_checkout_host_paths_with_work_guarded(
            &reader,
            &["a-present".to_owned()],
            &MaterializeOptions::native(guarded_output.path()),
            &original_work,
            &mut task_fence,
        )
        .await
        .err()
        .ok_or("logical action bypassed the original task fence")?;
        assert!(matches!(error, TaskFenceFailure::Owner));
        let after = original.custody.work().receipt();
        after.verify(FINITE_MATERIALIZATION_BUDGET)?;
        assert!(after.backend_read_operations > before.backend_read_operations);
        if deny_before_output {
            assert!(!guarded_output.path().join("a-present").exists());
            assert_eq!(after.output_bytes, before.output_bytes);
        } else {
            assert_eq!(
                std::fs::read(guarded_output.path().join("a-present"))?,
                b"first"
            );
            assert!(after.backend_write_operations > before.backend_write_operations);
            assert!(after.output_bytes >= before.output_bytes + 5);
        }
    }

    let mut no_write = GenerationReceipt::new(
        WorkBudget {
            backend_write_operations: 0,
            ..FINITE_MATERIALIZATION_BUDGET
        },
        &cancellation,
    )
    .map_err(|failure| failure.0)?;
    let candidate = checkout.private_candidate(no_write.custody.work(), &cancellation);
    let candidate = candidate?;
    let reader =
        candidate.into_pinned_reader_with_native_work(no_write.custody.work(), &cancellation);
    let reader = reader?;
    let no_write_output = tempfile::tempdir()?;
    let no_write_work = no_write.custody.work().clone();
    let original_no_write = materialize_reader_paths(
        &reader,
        std::slice::from_ref(&first),
        &MaterializeOptions::native(no_write_output.path()),
        MaterializeMode::DurableOutput,
        &no_write_work,
    )
    .await
    .err()
    .ok_or("original source ignored zero native-write allowance")?;
    assert!(matches!(original_no_write, MaterializeError::Work(_)));
    let original_spent = no_write.custody.work().receipt();
    assert_eq!(original_spent.backend_write_operations, 0);
    assert_eq!(original_spent.output_bytes, 0);
    assert!(std::fs::read_dir(no_write_output.path())?.next().is_none());

    let finite = tempfile::tempdir()?;
    let finite_failure = materialize_checkout_paths(
        &mut checkout,
        &[first, missing],
        &MaterializeOptions::native(finite.path()),
        *failed.work,
        &cancellation,
    )
    .await
    .err()
    .ok_or("missing second path was accepted under finite budget")?;
    assert!(matches!(
        finite_failure.error,
        MaterializeError::MissingPath
    ));
    Ok(())
}

#[tokio::test]
async fn restore_removes_missing_path() -> Result<(), Box<dyn std::error::Error>> {
    use crate::model::{Lifecycle, VolumeConfig};
    acyclic_native_runtime::initialize_native_io_services()?;
    let cancellation = CancellationToken::new();
    let volume = super::super::tests::memory_volume(
        crate::VolumeId::new(),
        VolumeConfig::portable(Lifecycle::Ephemeral),
        &cancellation,
    )
    .await?;
    let mut checkout = super::super::tests::private_checkout(&volume, &cancellation).await?;
    let temporary = tempfile::tempdir()?;
    let relative = Path::new("private-restore-path-sentinel");
    std::fs::write(temporary.path().join(relative), b"private-content-sentinel")?;
    let receipt = restore_checkout_host_path(
        &mut checkout,
        relative,
        HostPathReplacement::Atomic,
        &MaterializeOptions::native(temporary.path()),
        FINITE_MATERIALIZATION_BUDGET,
        &cancellation,
    )
    .await?;
    assert_eq!(receipt.value, HostPathRestore::Removed);
    assert!(!temporary.path().join(relative).exists());
    let invalid = restore_checkout_host_path(
        &mut checkout,
        Path::new(""),
        HostPathReplacement::Atomic,
        &MaterializeOptions::native(temporary.path()),
        FINITE_MATERIALIZATION_BUDGET,
        &cancellation,
    )
    .await;
    assert!(
        matches!(invalid, Err(failure) if matches!(failure.error, MaterializeError::InvalidPath))
    );
    Ok(())
}

/// A file created through a Linux mount records "no flags" (and its own birth and change times); materializing it must not refuse any of those, or no fork's new file could ever reach the root.
#[cfg(unix)]
#[test]
fn host_projection_drops_what_linux_cannot_or_need_not_apply() {
    use crate::kernel::MetadataField;
    let created = FileMetadata {
        posix_flags: MetadataField::Value(0),
        created_ns: MetadataField::Value(1),
        changed_ns: MetadataField::Value(2),
        ..FileMetadata::default()
    };
    let projected = host_metadata(created).0;
    assert_eq!(projected.changed_ns, MetadataField::Unavailable);
    #[cfg(target_os = "linux")]
    {
        assert_eq!(projected.posix_flags, MetadataField::Unavailable);
        assert_eq!(projected.created_ns, MetadataField::Unavailable);
        let flagged = FileMetadata {
            posix_flags: MetadataField::Value(1),
            ..FileMetadata::default()
        };
        assert_eq!(
            host_metadata(flagged).0.posix_flags,
            MetadataField::Value(1)
        );
    }
}

#[tokio::test]
async fn native_materialization_rejects_unrepresentable_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::kernel::MetadataField;

    acyclic_native_runtime::initialize_native_io_services()?;
    let temporary = tempfile::tempdir()?;
    std::fs::write(temporary.path().join("file"), b"data")?;
    let cancellation = CancellationToken::new();
    let mut receipt = GenerationReceipt::new(FINITE_MATERIALIZATION_BUDGET, &cancellation)
        .map_err(|failure| failure.0)?;
    let root = receipt.run(|work| HostRoot::open(temporary.path(), work))?;
    let root = receipt.owned_arc(root, 0)?;
    let limits = crate::model::VolumeLimits::default();
    let namespace = NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::Utf8,
            b"file".to_vec(),
            limits.maximum_component_bytes,
        )?],
        limits,
    )?;
    let host_path = GenerationHostPath::new(None, namespace.components(), &mut receipt)?;
    #[cfg(unix)]
    for (metadata, expected) in [
        (
            FileMetadata {
                posix_flags: MetadataField::Value(1),
                ..FileMetadata::default()
            },
            "posix_flags",
        ),
        (
            FileMetadata {
                changed_ns: MetadataField::Value(123),
                ..FileMetadata::default()
            },
            "changed_ns without native-view baseline",
        ),
    ] {
        assert!(matches!(
            apply_host_metadata_offloaded(&root, &host_path, metadata, &mut receipt).await,
            Err(failure) if matches!(failure, MaterializeError::UnsupportedMetadata(field) if field == expected)
        ));
    }
    #[cfg(windows)]
    let metadata = FileMetadata {
        posix_mode: MetadataField::Value(0o644),
        ..FileMetadata::default()
    };
    #[cfg(windows)]
    assert!(matches!(
        apply_host_metadata_offloaded(&root, &host_path, metadata, &mut receipt).await,
        Err(failure) if matches!(failure, MaterializeError::UnsupportedMetadata(_))
    ));
    Ok(())
}

fn restoration_work() -> crate::demand::DemandWork {
    crate::demand::DemandWork::new(
        WorkBudget {
            backend_read_operations: 10_000,
            backend_write_operations: 10_000,
            durability_operations: 10_000,
            bytes_hashed: 1_000_000,
            bytes_copied: 1_000_000,
            bytes_encoded: 1_000_000,
            source_bytes_read: 1_000_000,
            source_path_components: 10_000,
            source_entries_visited: 10_000,
            output_bytes: 1_000_000,
            items_examined: 10_000,
            allocation_operations: 10_000,
            peak_allocation_bytes: 1_000_000,
            ..WorkCounters::UNCHARGED
        },
        &CancellationToken::new(),
    )
}

#[test]
fn restore_preparation_rejects_a_swapped_root_before_any_write()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("root");
    std::fs::create_dir(&destination)?;
    let expected = HostRoot::open(&destination, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?
        .identity();
    std::fs::rename(&destination, temporary.path().join("original"))?;
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("foreign"), b"untouched")?;
    let original = Arc::new(NativeDemandReadMeter(std::sync::Mutex::new(
        restoration_work(),
    )));
    let native_work = crate::NativeMaterializationWork::from_meter(Arc::clone(&original));
    let allocation_work = (&native_work).clone();
    let mut work = NativeSourceWork::new(&original);
    let failure = prepare_restore(
        allocation_work
            .path(&destination, None)
            .map_err(materialize_native_error)?,
        allocation_work
            .path(Path::new("nested/entry"), None)
            .map_err(materialize_native_error)?,
        Some(expected),
        HostPathReplacement::Atomic,
        &mut work,
    )
    .err()
    .ok_or("swapped restore root was accepted")?;
    assert!(
        matches!(failure, MaterializeError::AllocationChanged { expected: found, .. } if found == expected)
    );
    let spent = original.with_original_leaf(|work| work.work);
    assert!(spent.backend_read_operations > 0);
    assert_eq!(spent.backend_write_operations, 0);
    assert_eq!(spent.durability_operations, 0);
    assert_eq!(allocation_work.live_allocation_bytes(), 0);
    assert!(!destination.join("nested").exists());
    assert_eq!(std::fs::read(destination.join("foreign"))?, b"untouched");
    Ok(())
}

#[test]
fn restore_removal_retains_the_original_physical_parent() -> Result<(), Box<dyn std::error::Error>>
{
    let temporary = tempfile::tempdir()?;
    let destination = temporary.path().join("root");
    let original = temporary.path().join("original");
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("entry"), b"old")?;
    let mut work = restoration_work();
    let root = HostRoot::open(&destination, &mut work)?;
    let parent = held_parent(&root, Path::new("entry"), &mut work)?;
    std::fs::rename(&destination, &original)?;
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("entry"), b"foreign")?;
    remove_restored_path(&parent, Path::new("entry"), Path::new("entry"), &mut work)?;
    assert!(!original.join("entry").exists());
    assert_eq!(std::fs::read(destination.join("entry"))?, b"foreign");
    assert!(work.work.backend_write_operations > 0);
    assert!(work.work.durability_operations > 0);
    Ok(())
}

#[test]
fn same_path_restores_are_serialized_by_a_process_crash_safe_lock()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("root");
    std::fs::create_dir(&root)?;
    let relative = Path::new("entry");
    let mut work = restoration_work();
    let held_root = HostRoot::open(&root, &mut work)?;
    let parent = held_parent(&held_root, relative, &mut work)?;
    let first = acquire_restore_lock(&parent, relative, true, &mut work)?;

    assert!(acquire_restore_lock(&parent, relative, false, &mut work).is_err());
    drop(first);
    // Blocking: a child another test forks shares the lock's open file
    // until it execs, so a release can take a moment to be observed.
    let _next = acquire_restore_lock(&parent, relative, true, &mut work)?;
    Ok(())
}

#[tokio::test]
async fn cancelled_restore_waiter_keeps_lock_until_host_io_finishes()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let temporary = tempfile::tempdir()?;
    let relative = Path::new("entry");
    let mut work = restoration_work();
    let held_root = HostRoot::open(temporary.path(), &mut work)?;
    let parent = held_parent(&held_root, relative, &mut work)?;
    let lock = acquire_restore_lock(&parent, relative, true, &mut work)?;
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let (finished, completion) = tokio::sync::oneshot::channel();
    let original = Arc::new(NativeDemandReadMeter(std::sync::Mutex::new(
        restoration_work(),
    )));
    let waiter = tokio::spawn(restore_io(Some(lock), Arc::clone(&original), move |_| {
        let _ = started.send(());
        // Dropping release on an early test exit also unblocks the worker.
        blocked.recv().map_err(std::io::Error::other)?;
        let _ = finished.send(());
        Ok::<_, MaterializeError>(())
    }));
    entered.await?;
    // Native waiting must not hold the original accounting leaf.
    original.with_original_leaf(|work| {
        work.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })
    })?;
    assert_eq!(
        original.with_original_leaf(|work| work.work.items_examined),
        1
    );
    waiter.abort();
    let cancelled = waiter.await;
    let competing = acquire_restore_lock(&parent, relative, false, &mut work);
    let fenced = matches!(
        &competing,
        Err(MaterializeError::Io(error))
            if error.raw_os_error() == fs2::lock_contended_error().raw_os_error()
    );
    drop(competing);
    // Release before assertions: even the broken-lock mutation must finish.
    release.send(())?;
    completion.await?;
    // Acquiring is the completion barrier for the worker's final lock drop.
    let _next = acquire_restore_lock(&parent, relative, true, &mut work)?;
    assert!(cancelled.is_err_and(|error| error.is_cancelled()));
    assert!(
        fenced,
        "cancelled waiter released a still-running restore's lock"
    );
    Ok(())
}

#[test]
fn live_mount_recovery_preserves_the_correct_entry() -> Result<(), Box<dyn std::error::Error>> {
    for published in [false, true] {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("root");
        std::fs::create_dir(&root)?;
        let relative = Path::new("entry");
        let destination = root.join(relative);
        let expected = if published {
            b"after".as_slice()
        } else {
            b"before"
        };
        std::fs::write(&destination, expected)?;
        let mut work = restoration_work();
        let held_root = HostRoot::open(&root, &mut work)?;
        let parent = held_parent(&held_root, relative, &mut work)?;
        let backup = restore_artifact_name(".acyclic-restore-backup-", relative, &mut work)?;
        let witness = restore_artifact_name(".acyclic-restore-live-witness-", relative, &mut work)?;
        create_restore_witness(&parent, &witness, LIVE_RESTORE_WITNESS, &mut work)?;
        if published {
            std::fs::write(root.join(&backup), b"before")?;
        } else {
            parent.rename_to(relative, &parent, &backup, &mut work)?;
        }
        recover_live_mount_replacement(&parent, relative, relative, &mut work)?;
        assert_eq!(std::fs::read(&destination)?, expected);
        assert!(!root.join(backup).exists());
    }
    Ok(())
}

#[test]
fn live_mount_recovery_preserves_an_unowned_backup_collision()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("root");
    std::fs::create_dir(&root)?;
    let relative = Path::new("entry");
    let destination = root.join(relative);
    std::fs::write(&destination, b"live")?;
    let mut work = restoration_work();
    let held_root = HostRoot::open(&root, &mut work)?;
    let parent = held_parent(&held_root, relative, &mut work)?;
    let backup = restore_artifact_name(".acyclic-restore-backup-", relative, &mut work)?;
    std::fs::write(root.join(&backup), b"unrelated")?;

    assert!(recover_live_mount_replacement(&parent, relative, relative, &mut work).is_err());
    assert_eq!(std::fs::read(&destination)?, b"live");
    assert_eq!(std::fs::read(root.join(backup))?, b"unrelated");
    Ok(())
}

#[test]
fn absent_restore_moves_then_reclaims_the_old_entry() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path().join("root");
    std::fs::create_dir(&root)?;
    let relative = Path::new("entry");
    std::fs::write(root.join(relative), b"old")?;
    let mut work = restoration_work();
    let held_root = HostRoot::open(&root, &mut work)?;
    let parent = held_parent(&held_root, relative, &mut work)?;

    remove_restored_path(&parent, relative, relative, &mut work)?;

    assert!(!root.join(relative).exists());
    assert!(
        !root
            .join(restore_artifact_name(
                ".acyclic-restore-removed-",
                relative,
                &mut work
            )?)
            .exists()
    );
    Ok(())
}
