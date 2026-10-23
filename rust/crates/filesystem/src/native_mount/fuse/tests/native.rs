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
) -> Result<Vec<MemoryCheckout>, Box<dyn std::error::Error>> {
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
    Ok(checkouts)
}

pub(super) fn tracking_sources() -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>>
{
    use crate::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};
    let config = posix_config();
    let checkouts = memory_checkout(
        config,
        CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::TrackingSafe,
            mutations: MutationMode::PrivateOverlay,
        },
        2,
    )?;
    let mut sources = checkouts.into_iter().map(|checkout| {
        crate::native_mount::adapter::tests::access::bind_test_access(
            MemorySource::new(
                Arc::new(crate::native_mount::SharedCheckout::new(checkout)),
                config,
            )?,
            vec![],
            None,
        )
    });
    match (sources.next(), sources.next()) {
        (Some(reader), Some(writer)) => Ok((reader?, writer?)),
        _ => Err("two checkouts were requested".into()),
    }
}

pub(super) fn shared_sources(
    config: crate::model::VolumeConfig,
) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
    use crate::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};
    let checkouts = memory_checkout(
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
        crate::native_mount::adapter::tests::access::bind_test_access(
            MemorySource::new(Arc::clone(&shared), config)?,
            vec![],
            None,
        )?,
        crate::native_mount::adapter::tests::access::bind_test_access(
            MemorySource::new(shared, config)?,
            vec![],
            None,
        )?,
    ))
}

pub(super) struct FuseWriteGuard {
    pub(super) writable: AtomicBool,
}

impl crate::native_mount::NativeMountCallbackGuard for FuseWriteGuard {
    fn check<'a>(
        &'a self,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, crate::NativeMountError> {
        crate::native_mount::NativeMountCallbackFuture::new(work, || async move {
            work.admit(crate::WorkCounters::UNCHARGED)?;
            Ok(())
        })
    }

    fn check_write<'a>(
        &'a self,
        work: &'a crate::NativeMaterializationWork,
    ) -> Result<crate::native_mount::NativeMountCallbackFuture<'a>, crate::NativeMountError> {
        crate::native_mount::NativeMountCallbackFuture::new_guarded(work, move || async move {
            if self.writable.load(Ordering::Acquire) {
                Ok(())
            } else {
                Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
            }
        })
    }
}

#[test]
pub(super) fn installed_write_denial_preserves_reads_and_post_effect_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let config = posix_config();
    let checkouts = memory_checkout(
        config,
        crate::model::CheckoutMode::tracking_transaction(),
        1,
    )?;
    let shared = Arc::new(crate::native_mount::SharedCheckout::new(
        checkouts.into_iter().next().ok_or("one checkout")?,
    ));
    let around = crate::native_mount::adapter::tests::access::bind_test_access(
        MemorySource::new(Arc::clone(&shared), config)?,
        vec![],
        None,
    )?;
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
        writable: AtomicBool::new(false),
    });
    let source = crate::native_mount::adapter::tests::access::bind_test_access(
        MemorySource::new_at_meter(
            shared,
            config,
            crate::kernel::NamespacePath::new(Vec::new(), config.limits)?,
            original.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
            &original,
            Some(&original),
        )?,
        vec![],
        None,
    )?
    .with_native_callback_guard(guard.clone());
    let source = source.into_arc_meter(&original)?;
    let core = projection_core(
        source.clone(),
        FINITE_HANDLER_POLICY,
        &crate::CancellationToken::new(),
    )?;
    let projection = crate::native_mount::fuse::FuseProjection {
        core: Arc::clone(&core),
    };
    let work = core.callback_meter()?.ok_or("installed original")?;
    assert!(
        projection
            .open_source(&path, created.node.file_id, libc::O_RDONLY, None, &work)
            .is_ok(),
        "READ does not require authored WRITE"
    );
    assert!(
        projection
            .open_source(&path, created.node.file_id, libc::O_TRUNC, None, &work)
            .is_err()
    );
    assert!(around.lookup(&path)?.is_some());
    let denied = |result: Result<(), crate::NativeMountError>| {
        assert!(matches!(
            result,
            Err(crate::NativeMountError::CallbackDenied { .. })
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
            .create_node(
                ROOT_INODE,
                std::ffi::OsStr::new("denied-directory"),
                |path, work| {
                    source
                        .create_directory(path, FileMetadata::default(), work)
                        .map_err(crate::NativeMountError::from)
                },
                &work,
            )
            .map(|_| ()),
    );
    assert!(
        projection
            .source_write(&work, || source
                .remove(&path, Some(created.node.file_id), &work,)
                .map_err(crate::NativeMountError::from))
            .is_err()
    );
    assert!(
        around.lookup(&path)?.is_some(),
        "pre-effect refusal cannot remove the file"
    );
    guard.writable.store(true, Ordering::Release);
    let removed = projection.source_write(&work, || {
        source.remove(&path, Some(created.node.file_id), &work)?;
        guard.writable.store(false, Ordering::Release);
        Ok(())
    });
    assert!(
        removed.is_err(),
        "revocation after the real remove cannot report success"
    );
    assert!(around.lookup(&path)?.is_none(), "no fictitious rollback");
    let first_prefix = original.receipt();
    let second = name("second-write-authority");
    let created = crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(&second, FileMetadata::default(), None, work)
    })?;
    guard.writable.store(true, Ordering::Release);
    assert!(
        super::mount::finishes_within(Duration::from_secs(5), move || {
            let removed = projection.source_write(&work, || {
                source.remove(&second, Some(created.node.file_id), &work)?;
                guard.writable.store(false, Ordering::Release);
                Ok(())
            });
            assert!(removed.is_err());
            assert!(around.lookup(&second).unwrap().is_none());
        }),
        "discarding a later typed refusal must not hold the original meter lock"
    );
    let retained = core
        .take_handler_failure()
        .ok_or("post-effect refusal custody")?;
    assert!(matches!(
        retained.0,
        crate::NativeMountError::CallbackDenied { .. }
    ));
    assert_eq!(retained.1, first_prefix);
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
    let (source, around) = shared_sources(posix_config())?;
    let path = name("removed-before-cancellation");
    let created = crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(&path, FileMetadata::default(), None, work)
    })?;
    let source: Arc<dyn MountFilesystem> = Arc::new(source);
    let cancellation = crate::CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(FINITE_HANDLER_POLICY, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let root = source
        .lookup_with_meter(&MountPath::root(), &original)?
        .ok_or(MountSourceError::NotFound)?;
    let core = ProjectionCore::new(
        Arc::clone(&source),
        root,
        source.view_stamp_with_meter(&original)?,
        true,
        0,
        0,
        &original,
    )?;
    {
        let _active = super::super::NativeMountWorkScope::new(&core, &original)?;
        let result = core.request(|work| {
            assert!(Arc::ptr_eq(&work.state, &original.state));
            let path = super::super::fuse_clone_path(&path, work)?;
            Ok(source.lookup_with_meter(&path, work)?)
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
    }
    assert!(original.receipt().bytes_copied != 0);
    let missing = core.request(|work| {
        let path = super::super::fuse_clone_path(&name("authentically-absent"), work)?;
        source
            .lookup_with_meter(&path, work)?
            .ok_or(MountSourceError::NotFound)
            .map_err(Into::into)
    });
    let missing_prefix = missing.as_ref().expect_err("actual absent lookup").1;
    assert_eq!(core.completed_callback_work(), missing_prefix);
    assert_eq!(core.finish(missing).err(), Some(libc::ENOENT));
    assert_eq!(core.completed_callback_work(), missing_prefix);
    assert!(core.take_handler_failure().is_none());
    assert!(!core.handler_failed.load(Ordering::Acquire));
    let result = core.request(|work| {
        source.remove(&path, Some(created.node.file_id), work)?;
        cancellation.cancel();
        source
            .lookup_with_meter(&path, work)
            .map_err(crate::NativeMountError::from)
    });
    let Err(failure) = result else {
        return Err("post-effect cancellation was reported as success".into());
    };
    assert_ne!(failure.1, crate::WorkCounters::UNCHARGED);
    let prefix = failure.1;
    assert_eq!(
        core.finish::<Option<MountLookup>>(Err(failure)),
        Err(libc::ESTALE)
    );
    assert!(core.handler_failed.load(Ordering::Acquire));
    let retained = core
        .take_handler_failure()
        .ok_or("actual fatal request failure missing")?;
    assert!(core.handler_failed.load(Ordering::Acquire));
    assert_eq!(retained.1, prefix);
    assert_eq!(
        core.completed_callback_work(),
        missing_prefix.checked_add(prefix)?
    );
    assert!(
        around.lookup(&path)?.is_none(),
        "the completed SDK remove is not rolled back or hidden"
    );
    Ok(())
}

#[test]
#[ignore = "requires a live Linux FUSE mount"]
pub(super) fn linux_installed_callbacks_observe_original_finite_mount_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, around) = shared_sources(posix_config())?;
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
    let work = crate::NativeMaterializationWork::new(FINITE_HANDLER_POLICY, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let prepared = crate::native_mount::prepare_native_mount_start(&work)?;
    let mut session = prepared.start(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id,
            destination: destination.path().to_path_buf(),
            writable: true,
        },
        Arc::new(source),
        &work,
        None,
    )?;
    cancellation.cancel();
    let error = std::fs::metadata(destination.path().join("cancelled-native-lookup"))
        .expect_err("installed callback cannot replace the original cancellation root");
    assert_eq!(error.raw_os_error(), Some(libc::ECANCELED));
    let cleanup = crate::NativeMaterializationWork::new(
        FINITE_HANDLER_POLICY,
        &crate::CancellationToken::new(),
    )
    .map_err(|(cause, _)| cause)?;
    session.stop_with_meter(&cleanup)?;
    Ok(())
}

#[test]
pub(super) fn measured_write_copy_refusal_precedes_the_real_source_write()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
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
        let bytes = crate::native_mount::fuse::fuse_source_chunk(b"replacement", work)?;
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
pub(super) fn native_access_fuse_open_refuses_an_actual_rebound_file_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
    let source = Arc::new(source);
    let path = name("rebound");
    let created = crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&path, FileMetadata::default(), None, work)
    })?;
    let core = projection_core(
        source.clone(),
        FINITE_HANDLER_POLICY,
        &crate::CancellationToken::new(),
    )?;
    let projection = crate::native_mount::fuse::FuseProjection { core };
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.remove(&path, Some(created.node.file_id), work)?;
        let rebound = source.create_file(&path, FileMetadata::default(), None, work)?;
        assert_ne!(rebound.node.file_id, created.node.file_id);
        assert!(
            matches!(projection.open_source(&path, created.node.file_id, libc::O_RDONLY, None, work), Err(crate::NativeMountError::Io(error)) if error.raw_os_error() == Some(libc::ESTALE))
        );
        Ok(())
    })?;
    Ok(())
}

#[test]
fn an_open_drops_the_size_of_any_version_it_does_not_read() -> Result<(), Box<dyn std::error::Error>>
{
    let (source, around) = shared_sources(posix_config())?;
    let path = name("versioned-read");
    crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        around.create_file(&path, FileMetadata::default(), None, work)
    })?;
    let core = projection_core(
        Arc::new(source),
        FINITE_HANDLER_POLICY,
        &crate::CancellationToken::new(),
    )?;
    let projection = crate::native_mount::fuse::FuseProjection {
        core: Arc::clone(&core),
    };
    let work = fuse_fixture_work();
    let inode = projection
        .lookup_entry(ROOT_INODE, std::ffi::OsStr::new("versioned-read"), &work)?
        .attr
        .ino
        .0;
    {
        let _names = core.names().map_err(std::io::Error::from_raw_os_error)?;
        let (handle, _) = projection.open_node(inode, libc::O_RDONLY, &work)?;
        core.state_to_finish()
            .discard_file_handle(inode, handle)
            .map_err(std::io::Error::from_raw_os_error)?;
    }
    crate::native_mount::adapter::boundary(around.cancellation(), |work| {
        let (file, _) = around.open_file_with_meter(&path, work)?;
        file.resize_with_meter(7, work)
    })?;
    // The kernel still holds the empty size; the open is answered with the
    // new version and that size is dropped before the reply, so the kernel
    // asks for attributes again before it reads through the handle.
    crate::native_mount::fuse::core::DROPPED_ATTRIBUTES.with_borrow_mut(Vec::clear);
    let handle = {
        let _names = core.names().map_err(std::io::Error::from_raw_os_error)?;
        projection.open_node(inode, libc::O_RDONLY, &work)?.0
    };
    assert!(
        crate::native_mount::fuse::core::DROPPED_ATTRIBUTES
            .with_borrow(|dropped| dropped.contains(&inode)),
        "an open of a newer version left the kernel's size in place"
    );
    assert_eq!(
        projection
            .read_handle(inode, handle, 0, 64, &work)?
            .bytes
            .len(),
        7
    );
    assert_eq!(projection.node_attributes(inode, None, &work)?.0.size, 7);
    core.state_to_finish()
        .discard_file_handle(inode, handle)
        .map_err(std::io::Error::from_raw_os_error)?;
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
pub(super) fn last_facade_drop_retires_the_original_source_after_cancellation()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
    let core = projection_core(
        Arc::new(source),
        FINITE_HANDLER_POLICY,
        &crate::CancellationToken::new(),
    )?;
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
    let control_allocation = original.claim(
        std::mem::size_of::<Mutex<FuseSessionState>>() as u64,
        1,
        crate::WorkCounters::UNCHARGED,
    )?;
    let state = Arc::new(Mutex::new(FuseSessionState {
        path: super::super::FuseOwned::empty(Vec::new()),
        before: None,
        mount: super::super::mount::Mount::default(),
        worker: None,
        device: None,
        notifier: None,
        identity: None,
        cleanup: birth_cleanup,
        retirement: Some(crate::native_mount::NativeMountRetirement {
            retained_source: Some(Arc::clone(&core.source)),
            destination_guard: None,
            startup_source_watch: None,
        }),
        failure: None,
        _allocation: control_allocation,
    }));
    let job_allocation = original.claim(
        std::mem::size_of::<(
            Arc<ProjectionCore>,
            Arc<Mutex<FuseSessionState>>,
            crate::NativeAllocation,
        )>() as u64,
        1,
        crate::WorkCounters::UNCHARGED,
    )?;
    let job = (Arc::clone(&core), Arc::clone(&state));
    let (done, terminal) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let _storage = job_allocation;
        run_fuse_session(job);
        done.send(()).unwrap();
    });
    let facade = crate::native_mount::fuse::FuseSession {
        core: Arc::clone(&core),
        state: Arc::clone(&state),
        invalidator: None,
    };
    let before = original.receipt();
    cancellation.cancel();
    drop(facade);
    terminal.recv_timeout(std::time::Duration::from_secs(10))?;
    worker.join().map_err(|_| "the actual keeper panicked")?;
    let state = state
        .lock()
        .unwrap_or_else(crate::native_mount::fuse::PoisonError::into_inner);
    assert!(state.is_retired() && state.failure.is_none());
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
fn native_access_fuse_cached_content_cannot_bypass_new_open_revocation()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::access::Action;
    use crate::native_mount::adapter::tests::access::{deny, publish_test_access};
    let (source, _) = shared_sources(posix_config())?;
    let source = Arc::new(source);
    let path = name("cached");
    let work = fuse_fixture_work();
    source.create_file(&path, FileMetadata::default(), None, &work)?;
    source.write_range(&path, 0, Bytes::from_static(b"data"))?;
    let core = projection_core(
        source.clone(),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let projection = crate::native_mount::fuse::FuseProjection { core };
    let entry = projection.lookup_entry(ROOT_INODE, std::ffi::OsStr::new("cached"), &work)?;
    let inode = entry.attr.ino.0;
    let (handle, _) = projection.open_node(inode, libc::O_RDONLY, &work)?;
    assert_eq!(
        projection
            .read_handle(inode, handle, 0, 4, &work)?
            .bytes
            .as_ref(),
        b"data"
    );
    publish_test_access(&source, vec![deny("revoked", &[Action::Read], "cached")])?;
    assert!(matches!(
        projection.open_node(inode, libc::O_RDONLY, &work),
        Err(crate::NativeMountError::Source(
            MountSourceError::AccessDenied
        ))
    ));
    assert!(matches!(
        projection.read_handle(inode, handle, 0, 4, &work),
        Err(NativeMountError::Source(MountSourceError::AccessDenied))
    ));
    Ok(())
}

#[test]
fn native_access_fuse_cached_metadata_requires_current_list_authority()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::access::Action;
    use crate::native_mount::adapter::tests::access::{deny, publish_test_access};
    let (source, _) = shared_sources(posix_config())?;
    let source = Arc::new(source);
    let path = name("cached");
    let work = fuse_fixture_work();
    source.create_file(&path, FileMetadata::default(), None, &work)?;
    let core = projection_core(
        source.clone(),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )?;
    let projection = crate::native_mount::fuse::FuseProjection { core };
    let inode = projection
        .lookup_entry(ROOT_INODE, std::ffi::OsStr::new("cached"), &work)?
        .attr
        .ino
        .0;
    let (handle, _) = projection.open_node(inode, libc::O_RDONLY, &work)?;
    projection.node_attributes(inode, None, &work)?;
    let stamp = source.view_stamp_with_meter(&work)?;
    publish_test_access(
        &source,
        vec![deny("undisclosed", &[Action::List], "cached")],
    )?;
    assert_eq!(source.view_stamp_with_meter(&work)?, stamp);
    for result in [
        projection
            .lookup_entry(ROOT_INODE, std::ffi::OsStr::new("cached"), &work)
            .map(|_| ()),
        projection.node_attributes(inode, None, &work).map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(NativeMountError::Source(MountSourceError::AccessDenied))
        ));
    }
    assert!(matches!(
        projection.node_attributes(inode, Some(handle), &work),
        Err(NativeMountError::Source(MountSourceError::AccessDenied))
    ));
    Ok(())
}

#[test]
fn native_access_fuse_detaching_preserves_each_open_handle_policy()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::access::Action;
    use crate::native_mount::adapter::tests::access::{deny, publish_test_access};
    for rename in [false, true] {
        let (source, _) = shared_sources(posix_config())?;
        let source = Arc::new(source);
        let work = fuse_fixture_work();
        let path = name("retained");
        source.create_file(&path, FileMetadata::default(), None, &work)?;
        source.write_range(&path, 0, Bytes::from_static(b"original"))?;
        source.create_file(&name("replacement"), FileMetadata::default(), None, &work)?;
        publish_test_access(
            &source,
            vec![deny(
                "read-only",
                &[Action::Write, Action::Append],
                "retained",
            )],
        )?;
        let projection = crate::native_mount::fuse::FuseProjection {
            core: projection_core(
                source.clone(),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )?,
        };
        let entry = projection.lookup_entry(ROOT_INODE, std::ffi::OsStr::new("retained"), &work)?;
        let inode = entry.attr.ino.0;
        let (handle, _) = projection.open_node(inode, libc::O_RDONLY, &work)?;
        publish_test_access(
            &source,
            vec![deny("revoked-read", &[Action::Read], "retained")],
        )?;
        if rename {
            projection.rename_name(
                ROOT_INODE,
                std::ffi::OsStr::new("replacement"),
                ROOT_INODE,
                std::ffi::OsStr::new("retained"),
                0,
                &work,
            )?;
        } else {
            projection.remove_name(ROOT_INODE, std::ffi::OsStr::new("retained"), &work)?;
        }
        let file = projection
            .core
            .state()
            .map_err(NativeMountError::from)?
            .file_handle(inode, handle)
            .map_err(NativeMountError::from)?
            .open_file
            .clone();
        assert!(matches!(
            file.file().read_range(0, 8),
            Err(MountSourceError::AccessDenied)
        ));
        assert!(matches!(
            file.file().write_range(0, Bytes::from_static(b"denied")),
            Err(MountSourceError::AccessDenied)
        ));
    }
    Ok(())
}

#[test]
fn full_case_fold_fuse_preserves_native_names_for_long_expanding_keys()
-> Result<(), Box<dyn std::error::Error>> {
    let mut config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    config.case_sensitivity = crate::model::CaseSensitivity::ProfileFolded;
    let (source, _) = shared_sources(config)?;
    let source = Arc::new(source);
    let work = fuse_fixture_work();
    let long = "ΐ".repeat(100);
    source.create_file(&name(&long), FileMetadata::default(), None, &work)?;
    source.write_range(&name(&long), 0, Bytes::from_static(b"body"))?;
    let projection = super::super::FuseProjection {
        core: projection_core(
            source.clone(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?,
    };
    let first = projection.lookup_entry(ROOT_INODE, std::ffi::OsStr::new(&long), &work)?;
    let inode = first.attr.ino.0;
    assert_eq!(
        projection
            .lookup_entry(ROOT_INODE, std::ffi::OsStr::new(&long), &work)?
            .attr
            .ino
            .0,
        inode
    );
    let (handle, _) = projection.open_node(inode, libc::O_RDONLY | libc::O_DIRECT, &work)?;
    let opened = projection
        .core
        .state()
        .map_err(NativeMountError::from)?
        .file_handle(inode, handle)
        .map_err(NativeMountError::from)?
        .open_file
        .clone();
    assert_eq!(opened.file().read_range(0, 4)?.as_ref(), b"body");
    projection.rename_name(
        ROOT_INODE,
        std::ffi::OsStr::new(&long),
        ROOT_INODE,
        std::ffi::OsStr::new("renamed"),
        0,
        &work,
    )?;
    assert_eq!(
        projection
            .core
            .state()
            .map_err(NativeMountError::from)?
            .path(inode)
            .map_err(NativeMountError::from)?,
        &name("renamed")
    );
    assert!(source.lookup(&name(&long))?.is_none());
    let (handle, _) = projection.open_node(inode, libc::O_RDONLY | libc::O_DIRECT, &work)?;
    let reopened = projection
        .core
        .state()
        .map_err(NativeMountError::from)?
        .file_handle(inode, handle)
        .map_err(NativeMountError::from)?
        .open_file
        .clone();
    assert_eq!(reopened.file().read_range(0, 4)?.as_ref(), b"body");
    projection.rename_name(
        ROOT_INODE,
        std::ffi::OsStr::new("renamed"),
        ROOT_INODE,
        std::ffi::OsStr::new(&long),
        0,
        &work,
    )?;
    assert_eq!(
        projection
            .core
            .state()
            .map_err(NativeMountError::from)?
            .path(inode)
            .map_err(NativeMountError::from)?,
        &name(&long)
    );
    let (handle, _) = projection.open_node(inode, libc::O_RDONLY | libc::O_DIRECT, &work)?;
    let restored = projection
        .core
        .state()
        .map_err(NativeMountError::from)?
        .file_handle(inode, handle)
        .map_err(NativeMountError::from)?
        .open_file
        .clone();
    assert_eq!(restored.file().read_range(0, 4)?.as_ref(), b"body");
    Ok(())
}
