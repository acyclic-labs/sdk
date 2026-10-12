use super::*;

pub(super) fn posix_config() -> crate::model::VolumeConfig {
    let mut config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    config.profile = crate::model::FilesystemProfile::Posix;
    config
}

pub(super) fn memory_checkout(
    config: crate::model::VolumeConfig,
    mode: crate::model::CheckoutMode,
    count: usize,
) -> Result<(Vec<MemoryCheckout>, crate::model::VolumeConfig), Box<dyn std::error::Error>> {
    use crate::model::GenerationSelector;
    let fs = crate::Fs::memory();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let checkouts = runtime.block_on(async {
        let cancellation = crate::CancellationToken::new();
        let volume = fs
            .create_volume(config, crate::WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let mut checkouts = Vec::with_capacity(count);
        for _ in 0..count {
            checkouts.push(
                volume
                    .checkout(
                        GenerationSelector::Head,
                        mode,
                        crate::WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await?
                    .value,
            );
        }
        Ok::<_, crate::OperationFailure<crate::FsError>>(checkouts)
    })?;
    Ok((checkouts, config))
}

pub(super) fn tracking_sources() -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>>
{
    use crate::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};
    let (checkouts, config) = memory_checkout(
        posix_config(),
        CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::TrackingSafe,
            mutations: MutationMode::PrivateOverlay,
        },
        2,
    )?;
    let mut sources = checkouts.into_iter().map(|checkout| {
        MemorySource::new(
            Arc::new(crate::native_mount::SharedCheckout::new(checkout)),
            config,
        )
    });
    match (sources.next(), sources.next()) {
        (Some(reader), Some(writer)) => Ok((reader?, writer?)),
        _ => Err("two checkouts were requested".into()),
    }
}

pub(super) fn shared_sources() -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
    shared_sources_of(posix_config())
}

pub(super) fn shared_sources_of(
    config: crate::model::VolumeConfig,
) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
    use crate::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};
    acyclic_native_runtime::initialize_native_io_services()?;
    let (checkouts, config) = memory_checkout(
        config,
        CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::Pinned,
            mutations: MutationMode::PrivateOverlay,
        },
        1,
    )?;
    let checkout = checkouts.into_iter().next().ok_or("one checkout")?;
    let shared = Arc::new(crate::native_mount::SharedCheckout::new(checkout));
    Ok((
        MemorySource::new(Arc::clone(&shared), config)?,
        MemorySource::new(shared, config)?,
    ))
}

#[derive(Debug, thiserror::Error)]
#[error("installed FUSE callback owner has no authored WRITE permission")]
pub(super) struct FuseWriteDenied;

pub(super) struct FuseWriteGuard {
    pub(super) original: crate::NativeMaterializationWork,
    pub(super) writable: std::sync::atomic::AtomicBool,
}

impl crate::native_mount::NativeMountCallbackGuard for FuseWriteGuard {
    fn check<'a>(
        &'a self,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, crate::NativeMountError> {
        if !self.original.shares_ledger(&work) {
            return Err(crate::NativeMountError::ControlPending);
        }
        crate::native_mount::NativeMountCallbackFuture::new(work, || async move {
            work.admit(crate::WorkCounters::UNCHARGED)?;
            Ok(())
        })
    }

    fn check_write<'a>(
        &'a self,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, crate::NativeMountError> {
        if !self.original.shares_ledger(&work) {
            return Err(crate::NativeMountError::ControlPending);
        }
        if !self.writable.load(std::sync::atomic::Ordering::Acquire) {
            return Err(
                crate::native_mount::NativeMountCallbackReservation::prepare(work)?
                    .into_error(FuseWriteDenied),
            );
        }
        crate::native_mount::NativeMountCallbackFuture::new_guarded(work, move || async move {
            if self.writable.load(std::sync::atomic::Ordering::Acquire) {
                Ok(())
            } else {
                Err(FuseWriteDenied)
            }
        })
    }
}

#[test]
pub(super) fn installed_write_denial_preserves_reads_and_post_effect_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let (checkouts, config) = memory_checkout(
        posix_config(),
        crate::model::CheckoutMode::tracking_transaction(),
        1,
    )?;
    let shared = Arc::new(crate::native_mount::SharedCheckout::new(
        checkouts.into_iter().next().ok_or("one checkout")?,
    ));
    let around = MemorySource::new(Arc::clone(&shared), config)?;
    let path = name("write-authority");
    let created = crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(&path, FileMetadata::default(), None, work)
    })?;
    let original = crate::NativeMaterializationWork::new(
        FINITE_HANDLER_POLICY,
        &crate::CancellationToken::new(),
    )
    .map_err(|(cause, _)| cause)?;
    let guard = Arc::new(FuseWriteGuard {
        original: original.clone(),
        writable: std::sync::atomic::AtomicBool::new(false),
    });
    let source = MemorySource::new_at_meter(
        shared,
        config,
        crate::kernel::NamespacePath::new(Vec::new(), config.limits)?,
        original.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
        &original,
        Some(&original),
    )?
    .with_native_callback_guard(guard.clone());
    let source = source.into_arc_meter(&original)?;
    let core = Arc::new(crate::native_mount::fuse::ProjectionCore::new(
        source.clone(),
        empty_projection_state(),
        FINITE_HANDLER_POLICY,
        &crate::CancellationToken::new(),
    )?);
    let projection = crate::native_mount::fuse::FuseProjection {
        core: Arc::clone(&core),
        skips_opendir: false,
        skips_open: false,
    };
    let work = core.callback_meter()?.ok_or("installed original")?;
    assert!(
        projection
            .open_source(
                source.as_ref(),
                &path,
                created.node.file_id,
                libc::O_RDONLY,
                None,
                &work
            )
            .is_ok(),
        "READ does not require authored WRITE"
    );
    assert!(
        projection
            .open_source(
                source.as_ref(),
                &path,
                created.node.file_id,
                libc::O_TRUNC,
                None,
                &work
            )
            .is_err()
    );
    assert!(around.lookup(&path)?.is_some());
    let denied = |result: Result<(), crate::native_mount::fuse::FuseRequestError>| {
        assert!(matches!(
            result,
            Err(crate::native_mount::fuse::FuseRequestError::Native(
                crate::NativeMountError::CallbackDenied { .. }
            ))
        ));
    };
    denied(projection.remove_name(ROOT_INODE, std::ffi::OsStr::new("write-authority"), &work));
    denied(projection.rename_name(
        ROOT_INODE,
        std::ffi::OsStr::new("write-authority"),
        ROOT_INODE,
        std::ffi::OsStr::new("renamed"),
        0,
        &work,
    ));
    denied(
        projection
            .link_name(0, ROOT_INODE, std::ffi::OsStr::new("linked"), &work)
            .map(|_| ()),
    );
    denied(
        projection
            .write_handle(0, 0, 0, b"denied", &work)
            .map(|_| ()),
    );
    denied(projection.allocate(0, 0, 0, 1, 0, &work));
    denied(
        projection
            .copy_range(0, 0, 0, 0, 0, 0, 1, 0, &work)
            .map(|_| ()),
    );
    denied(
        projection
            .create_node(
                ROOT_INODE,
                std::ffi::OsStr::new("denied-directory"),
                |path, work| {
                    source
                        .create_directory(path, FileMetadata::default(), work)
                        .map_err(crate::native_mount::fuse::FuseRequestError::from)
                },
                &work,
            )
            .map(|_| ()),
    );
    denied(projection.write_named_attribute(
        0,
        std::ffi::OsStr::new("user.denied"),
        b"denied",
        0,
        0,
        &work,
    ));
    denied(projection.remove_named_attribute(0, std::ffi::OsStr::new("user.denied"), &work));
    assert!(
        projection
            .source_write(&work, || source
                .remove(&path, Some(created.node.file_id), &work,)
                .map_err(crate::native_mount::fuse::FuseRequestError::from))
            .is_err()
    );
    assert!(
        around.lookup(&path)?.is_some(),
        "pre-effect refusal cannot remove the file"
    );
    guard
        .writable
        .store(true, std::sync::atomic::Ordering::Release);
    let removed = projection.source_write(&work, || {
        source
            .remove(&path, Some(created.node.file_id), &work)
            .map_err(crate::native_mount::fuse::FuseRequestError::from)?;
        guard
            .writable
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    });
    assert!(
        removed.is_err(),
        "revocation after the real remove cannot report success"
    );
    assert!(around.lookup(&path)?.is_none(), "no fictitious rollback");
    let retained = core
        .take_handler_failure()
        .ok_or("post-effect refusal custody")?;
    assert!(matches!(
        retained.0,
        crate::NativeMountError::CallbackDenied { .. }
    ));
    assert_eq!(retained.1, original.receipt());
    assert_eq!(
        core.completed_callback_work(),
        crate::WorkCounters::UNCHARGED
    );
    Ok(())
}

pub(super) const FINITE_HANDLER_POLICY: crate::WorkBudget =
    crate::performance::work_budget(1 << 20, 1 << 26);

#[test]
pub(super) fn actual_source_effect_precedes_callback_cancellation_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, around) = shared_sources()?;
    let path = name("removed-before-cancellation");
    let created = crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(&path, FileMetadata::default(), None, work)
    })?;
    let source = Arc::new(source);
    let cancellation = crate::CancellationToken::new();
    let core = crate::native_mount::fuse::ProjectionCore::new(
        source.clone(),
        empty_projection_state(),
        FINITE_HANDLER_POLICY,
        &cancellation,
    )?;
    let result = core.request(|work| {
        source.remove(&path, Some(created.node.file_id), work)?;
        cancellation.cancel();
        source
            .lookup_with_meter(&path, work)
            .map_err(crate::native_mount::fuse::FuseRequestError::from)
    });
    let Err(failure) = result else {
        return Err("post-effect cancellation was reported as success".into());
    };
    assert_ne!(failure.1, crate::WorkCounters::UNCHARGED);
    assert_eq!(
        crate::native_mount::fuse::native_callback_errno(&failure.0),
        libc::ECANCELED
    );
    let prefix = failure.1;
    assert_eq!(
        core.finish::<Option<MountLookup>>(Err(failure)),
        Err(libc::ECANCELED)
    );
    assert!(
        core.handler_failed
            .load(std::sync::atomic::Ordering::Acquire)
    );
    let retained = core
        .take_handler_failure()
        .ok_or("actual fatal request failure missing")?;
    assert!(
        core.handler_failed
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(retained.1, prefix);
    assert_eq!(core.completed_callback_work(), prefix);
    assert!(
        around.lookup(&path)?.is_none(),
        "the completed SDK remove is not rolled back or hidden"
    );
    Ok(())
}

#[test]
pub(super) fn authentic_missing_source_name_does_not_poison_handler_health()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources()?;
    let source = Arc::new(source);
    let cancellation = crate::CancellationToken::new();
    let core = crate::native_mount::fuse::ProjectionCore::new(
        source.clone(),
        empty_projection_state(),
        FINITE_HANDLER_POLICY,
        &cancellation,
    )?;
    let path = name("authentically-absent");
    let result = core.request(|work| {
        let path = crate::native_mount::fuse::fuse_clone_path(&path, work)?;
        source
            .lookup_with_meter(&path, work)?
            .ok_or(MountSourceError::NotFound)
            .map_err(crate::native_mount::fuse::FuseRequestError::from)
    });
    let Err(failure) = &result else {
        return Err("real absent lookup lost its original request packet".into());
    };
    let prefix = failure.1;
    assert_eq!(core.completed_callback_work(), prefix);
    assert_eq!(core.finish(result).err(), Some(libc::ENOENT));
    assert!(core.take_handler_failure().is_none());
    assert_eq!(
        core.completed_callback_work(),
        prefix,
        "SDK projection does not fold twice"
    );
    assert!(
        !core
            .handler_failed
            .load(std::sync::atomic::Ordering::Acquire)
    );
    Ok(())
}

#[test]
pub(super) fn active_callback_borrows_original_meter_without_future_receipt_double_count()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, around) = shared_sources()?;
    let path = name("shared-original-callback");
    crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(&path, FileMetadata::default(), None, work)
    })?;
    let source: Arc<dyn MountFilesystem> = Arc::new(source);
    let cancellation = crate::CancellationToken::new();
    let receipt = crate::native_mount::run_native_mount_handler(
        FINITE_HANDLER_POLICY,
        &cancellation,
        |work| {
            let root = source
                .lookup_with_meter(&MountPath::root(), work)?
                .ok_or(MountSourceError::NotFound)?;
            let stamp = source.view_stamp_with_meter(work)?;
            let core = crate::native_mount::fuse::ProjectionCore::root(
                Arc::clone(&source),
                root,
                stamp,
                true,
                0,
                0,
                work,
            )?;
            let _active = crate::native_mount::fuse::NativeMountWorkScope::new(&core, work)?;
            let result = core.request(|callback_work| {
                assert!(Arc::ptr_eq(
                    callback_work.native_read_meter(),
                    work.native_read_meter()
                ));
                let path = crate::native_mount::fuse::fuse_clone_path(&path, callback_work)?;
                source
                    .lookup_with_meter(&path, callback_work)
                    .map_err(crate::native_mount::fuse::FuseRequestError::from)
            });
            assert!(
                core.finish(result)
                    .map_err(std::io::Error::from_raw_os_error)?
                    .is_some()
            );
            assert_eq!(
                core.completed_callback_work(),
                crate::WorkCounters::UNCHARGED
            );
            assert!(work.native_read_meter().lock().work.bytes_copied != 0);
            Ok(())
        },
    )
    .map_err(|failure| failure.0)?;
    assert!(receipt.work.bytes_copied != 0);
    Ok(())
}

#[test]
#[ignore = "requires a live Linux FUSE mount"]
pub(super) fn linux_installed_callbacks_observe_original_finite_mount_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, around) = shared_sources()?;
    crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(
            &name("cancelled-native-lookup"),
            FileMetadata::default(),
            None,
            work,
        )
    })?;
    let volume_id = source.volume_id()?;
    let destination = tempfile::tempdir()?;
    let cancellation = crate::CancellationToken::new();
    let mut session = crate::native_mount::mount_native_measured(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id,
            destination: destination.path().to_path_buf(),
            writable: true,
        },
        Arc::new(source),
        FINITE_HANDLER_POLICY,
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?
    .value;
    cancellation.cancel();
    let error = std::fs::metadata(destination.path().join("cancelled-native-lookup"))
        .expect_err("installed callback cannot replace the original cancellation root");
    assert_eq!(error.raw_os_error(), Some(libc::ECANCELED));
    session
        .stop_measured(FINITE_HANDLER_POLICY, &crate::CancellationToken::new())
        .map_err(|(cause, _)| cause)?;
    Ok(())
}

#[test]
pub(super) fn measured_write_copy_refusal_precedes_the_real_source_write()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources()?;
    let path = name("copy-denied");
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&path, FileMetadata::default(), None, work)
    })?;
    let opened = source.open_file(&path)?;
    opened.write_range(0, Bytes::from_static(b"original"))?;
    let cancellation = crate::CancellationToken::new();
    let mut budget = FINITE_HANDLER_POLICY;
    budget.bytes_copied = 0;
    let failure = crate::native_mount::run_native_mount_handler(budget, &cancellation, |work| {
        let bytes = crate::native_mount::fuse::fuse_source_chunk(b"replacement", work)
            .map_err(crate::NativeMountError::from)?;
        opened
            .write_range_with_meter(0, bytes, work)
            .map_err(crate::NativeMountError::Source)
    })
    .expect_err("zero copied-byte authority must refuse the packet before source mutation");
    assert_eq!(failure.1.bytes_copied, 0);
    assert_eq!(failure.1.backend_write_operations, 0);
    assert_eq!(opened.read_range(0, 8)?.as_ref(), b"original");
    Ok(())
}

#[test]
pub(super) fn measured_deferred_open_refuses_an_actual_rebound_file_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources()?;
    let source: Arc<dyn MountFilesystem> = Arc::new(source);
    let path = name("deferred-rebound");
    let opened = crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&path, FileMetadata::default(), None, work)
    })?;
    let cancellation = crate::CancellationToken::new();
    let deferred = crate::native_mount::run_native_mount_handler(
        FINITE_HANDLER_POLICY,
        &cancellation,
        |work| {
            let path = crate::native_mount::fuse::fuse_clone_path(&path, work)
                .map_err(crate::NativeMountError::from)?;
            let allocation = crate::native_mount::fuse::fuse_initialized(
                (),
                std::mem::size_of::<crate::native_mount::fuse::DeferredOpenFile>() as u64,
                work,
            )
            .map_err(crate::NativeMountError::from)?;
            Ok(Arc::new(crate::native_mount::fuse::DeferredOpenFile {
                source: crate::native_mount::fuse::fuse_arc_clone(&source, work)
                    .map_err(crate::NativeMountError::from)?,
                path,
                opened,
                file: std::sync::Mutex::new(None),
                _allocation: allocation,
            }))
        },
    )
    .map_err(|failure| failure.0)?
    .value;
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.remove(&path, Some(opened.node.file_id), work)
    })?;
    let rebound = crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&path, FileMetadata::default(), None, work)
    })?;
    assert_ne!(rebound.node.file_id, opened.node.file_id);
    let failure = crate::native_mount::run_native_mount_handler(
        FINITE_HANDLER_POLICY,
        &cancellation,
        |work| {
            deferred
                .file(work)?
                .lookup_with_meter(work)
                .map_err(crate::NativeMountError::Source)
        },
    )
    .expect_err("a lazy open must not attach its inode to a replacement");
    assert!(matches!(
        failure.0,
        crate::NativeMountError::Source(crate::native_mount::MountSourceError::Stale)
    ));
    assert!(
        deferred
            .file
            .lock()
            .map_err(|_| "poisoned deferred file")?
            .is_none()
    );
    assert!(failure.1.items_examined > 0);
    Ok(())
}

pub(super) fn mount(
    source: Arc<dyn MountFilesystem>,
    volume_id: crate::VolumeId,
    destination: &std::path::Path,
) -> Result<crate::NativeMountSession, crate::NativeMountError> {
    crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id,
            destination: destination.to_path_buf(),
            writable: true,
        },
        source,
    )
}

pub(super) fn listing(directory: &std::path::Path) -> std::io::Result<Vec<std::ffi::OsString>> {
    let mut names = std::fs::read_dir(directory)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<Vec<_>, _>>()?;
    names.sort();
    Ok(names)
}

#[test]
pub(super) fn last_facade_drop_signals_the_original_keeper_and_retires_its_actual_root()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources()?;
    let core = Arc::new(crate::native_mount::fuse::ProjectionCore::new(
        Arc::new(source),
        empty_projection_state(),
        FINITE_HANDLER_POLICY,
        &crate::CancellationToken::new(),
    )?);
    let cancellation = crate::CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 0,
            backend_write_operations: 1,
            ..FINITE_HANDLER_POLICY
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    let birth_cleanup = original.reserve_work(crate::WorkCounters {
        backend_write_operations: 1,
        ..crate::WorkCounters::UNCHARGED
    })?;
    let directory = tempfile::tempdir()?;
    let root = cap_std::fs::Dir::open_ambient_dir(directory.path(), cap_std::ambient_authority())?;
    let control_allocation = original.claim(
        std::mem::size_of::<crate::native_mount::fuse::FuseDriverControl>() as u64,
        u64::from(
            (std::mem::size_of::<crate::native_mount::fuse::FuseDriverControl>() as u64) != 0,
        ),
        crate::WorkCounters::UNCHARGED,
    )?;
    let control = Arc::new(crate::native_mount::fuse::FuseDriverControl {
        state: crate::native_mount::fuse::Mutex::new(
            crate::native_mount::fuse::FuseDriverControlState {
                sdk_session: Some(None),
                physical: Some(crate::native_mount::fuse::FusePhysicalSession {
                    session: None,
                    sdk_worker: None,
                    active_root: Some(root),
                    native_root_identity: None,
                    retirement_helper: None,
                    birth_cleanup,
                }),
                retirement: Some(crate::native_mount::NativeMountRetirement {
                    retained_source: None,
                    destination_guard: None,
                    startup_source_watch: None,
                }),
                loan_custody: (false, false),
                startup_finished: true,
                stop_requested: false,
                retired: false,
                failure: None,
            },
        ),
        changed: crate::native_mount::fuse::Condvar::new(),
        core: Arc::clone(&core),
        _allocation: control_allocation,
    });
    let job_allocation = original.claim(
        std::mem::size_of::<(
            Arc<crate::native_mount::fuse::FuseDriverControl>,
            crate::NativeAllocation,
        )>() as u64,
        u64::from(
            (std::mem::size_of::<(
                Arc<crate::native_mount::fuse::FuseDriverControl>,
                crate::NativeAllocation,
            )>() as u64)
                != 0,
        ),
        crate::WorkCounters::UNCHARGED,
    )?;
    let job = (Arc::clone(&control), job_allocation);
    let (done, terminal) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        crate::native_mount::fuse::run_fuse_retirement(job);
        done.send(()).unwrap();
    });
    let facade = crate::native_mount::fuse::FuseSession {
        control: Arc::clone(&control),
        invalidator: None,
    };
    let before = original.receipt();
    cancellation.cancel();
    drop(facade);
    terminal.recv_timeout(std::time::Duration::from_secs(10))?;
    worker.join().map_err(|_| "the actual keeper panicked")?;
    let state = control
        .state
        .lock()
        .unwrap_or_else(crate::native_mount::fuse::PoisonError::into_inner);
    assert!(state.retired && state.failure.is_none());
    let physical = state
        .physical
        .as_ref()
        .ok_or("the actual physical holder disappeared")?;
    assert!(physical.active_root.is_none());
    assert_eq!(
        physical.birth_cleanup.remaining_work(),
        crate::WorkCounters::UNCHARGED
    );
    assert!(
        state
            .retirement
            .as_ref()
            .is_some_and(crate::native_mount::NativeMountRetirement::is_retired)
    );
    assert_eq!(
        original.receipt().allocation_operations,
        before.allocation_operations,
        "last facade drop and the real keeper terminal phase allocate no new controls"
    );
    assert!(original.cancellation().is_cancelled());
    Ok(())
}

#[test]
pub(super) fn preborn_helper_frame_reaps_real_child_after_original_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = crate::CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 16_004,
            backend_write_operations: 13,
            ..FINITE_HANDLER_POLICY
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    let mut helper =
        crate::native_mount::fuse::FuseRetirementHelper::prepare_program(c"/bin/true", &original)?;
    let before = original.receipt();
    cancellation.cancel();
    helper.retire(c"/unused", None)?;
    assert!(helper.helper.is_none() && helper.frame.is_none());
    let after = original.receipt();
    assert_eq!(
        after.allocation_operations, before.allocation_operations,
        "all known argv/environment/control backing existed before birth"
    );
    assert!(original.cancellation().is_cancelled());
    helper.retire(c"/unused", None)?;
    assert_eq!(
        original.receipt(),
        after,
        "terminal replay cannot spawn another process"
    );
    Ok(())
}

#[test]
pub(super) fn preborn_helper_keeps_original_spawn_cause_after_genuine_sdk_refusal()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = crate::CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 16_004,
            backend_write_operations: 13,
            ..FINITE_HANDLER_POLICY
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    let mut helper = crate::native_mount::fuse::FuseRetirementHelper::prepare_program(
        c"/this-native-helper-program-authentically-does-not-exist",
        &original,
    )?;
    let before = original.receipt();
    cancellation.cancel();
    let error = helper
        .retire(c"/unused", None)
        .expect_err("the real posix_spawn must report ENOENT");
    assert!(matches!(error, crate::NativeMountError::Materialization(
        crate::NativeTreeMaterializationError::Io(error)
    ) if error.raw_os_error() == Some(libc::ENOENT)));
    assert!(helper.helper.is_none() && helper.frame.is_none());
    assert_eq!(
        original.receipt().allocation_operations,
        before.allocation_operations
    );
    Ok(())
}
