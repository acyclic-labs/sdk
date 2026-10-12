#[cfg(target_os = "macos")]
#[test]
#[allow(unsafe_code, reason = "calls the NFS transport's C test hook")]
fn exclusive_replay_is_bound_to_the_opened_file_handle() {
    // SAFETY: the test hook has no arguments and owns all callback state.
    assert_eq!(unsafe { nfs4_test_exclusive_replay_identity() }, 0);
}

#[cfg(target_os = "macos")]
#[test]
#[allow(unsafe_code, reason = "calls the NFS transport's C test hook")]
fn named_attribute_exclusive_replay_is_bound_to_owner_and_name() {
    assert_eq!(
        // SAFETY: the test hook has no arguments and owns all callback state.
        unsafe { nfs4_test_namedattr_exclusive_replay_identity() },
        0
    );
}

#[cfg(target_os = "macos")]
#[test]
#[allow(unsafe_code, reason = "calls the NFS transport's C test hook")]
fn readdir_continuations_are_bound_to_the_namespace_revision() {
    // SAFETY: the test hook has no arguments and mutates no shared state.
    assert_eq!(unsafe { nfs4_test_readdir_cookie_verifier() }, 0);
}

#[cfg(target_os = "macos")]
#[test]
#[allow(unsafe_code, reason = "calls the NFS transport's C test hook")]
fn nfs_sync_acknowledgement_requires_successful_callback() {
    // SAFETY: the test hook owns its callback state.
    assert_eq!(unsafe { nfs4_test_sync_acknowledgement() }, 0);
}

#[cfg(target_os = "macos")]
#[test]
#[ignore = "requires a live macOS NFS mount"]
fn macos_explicit_file_fsync_reaches_mount_source() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write as _;

    let source = Arc::new(source(FilesystemProfile::Posix)?);
    let temporary = tempfile::tempdir()?;
    let mut mount = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: temporary.path().to_path_buf(),
            writable: true,
        },
        Arc::clone(&source) as Arc<dyn MountFilesystem>,
    )?;
    let path = temporary.path().join("explicit-fsync");
    let mut file = std::fs::File::create(&path)?;
    file.write_all(b"durable")?;
    file.sync_all()?;
    assert_eq!(std::fs::read(&path)?, b"durable");
    drop(file);
    assert!(mount.stop()?);
    Ok(())
}

/// Objects in memory, refusing metadata while `refuse_metadata` is set.
#[derive(Default)]
struct MetadataRefusingObjects {
    inner: crate::memory::MemoryObjectStore,
    refuse_metadata: AtomicBool,
}

impl MetadataRefusingObjects {
    fn admit(&self, object_id: ObjectId) -> Result<(), crate::storage::ObjectFailure> {
        if object_id.kind == crate::storage::ObjectKind::Metadata
            && self.refuse_metadata.load(Ordering::Acquire)
        {
            return Err(crate::storage::ObjectFailure::before_work(
                crate::storage::ObjectStoreError::Corrupt,
            ));
        }
        Ok(())
    }
}

impl crate::storage::ObjectStore for MetadataRefusingObjects {
    fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
    ) -> crate::storage::ObjectResult<()> {
        self.admit(object_id)?;
        crate::storage::ObjectStore::put(&self.inner, object_id, bytes, budget)
    }

    fn put_many(
        &self,
        writes: &[crate::storage::ObjectWrite],
        budget: WorkBudget,
    ) -> crate::storage::ObjectResult<()> {
        for write in writes {
            self.admit(write.object_id)?;
        }
        crate::storage::ObjectStore::put_many(&self.inner, writes, budget)
    }

    fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
    ) -> crate::storage::ObjectResult<crate::storage::ObjectRead> {
        crate::storage::ObjectStore::read(&self.inner, object_id, maximum_bytes, budget)
    }

    fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        budget: WorkBudget,
    ) -> crate::storage::ObjectResult<Vec<crate::storage::ObjectRead>> {
        crate::storage::ObjectStore::read_many(&self.inner, requests, budget)
    }

    fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
    ) -> crate::storage::ObjectResult<bool> {
        crate::storage::ObjectStore::contains(&self.inner, object_id, budget)
    }
}

impl crate::async_storage::ImmediateObjectStore for MetadataRefusingObjects {}

#[test]
fn callbacks_run_on_their_driver_threads_without_helper_threads()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = CallbackRuntime::create()?;
    let callers = (0..32)
        .map(|index| {
            let runtime = runtime.clone();
            std::thread::Builder::new()
                .name(format!("native-driver-{index}"))
                .spawn(move || {
                    runtime.wait(|| async {
                        // A real reactor wait parks the driver thread
                        // until a runtime worker wakes it.
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        Ok::<_, MountSourceError>(
                            std::thread::current().name().unwrap_or_default().to_owned(),
                        )
                    })
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (index, caller) in callers.into_iter().enumerate() {
        assert_eq!(
            caller
                .join()
                .map_err(|_| std::io::Error::other("native driver thread panicked"))??,
            format!("native-driver-{index}")
        );
    }
    Ok(())
}

#[test]
fn nested_callbacks_reenter_on_the_same_driver_thread() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = CallbackRuntime::create()?;
    let nested = runtime.clone();
    let observed = std::thread::Builder::new()
        .name("native-driver".to_owned())
        .spawn(move || {
            runtime.wait(|| async {
                tokio::task::yield_now().await;
                nested.wait(|| async {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    Ok(std::thread::current().name().unwrap_or_default().to_owned())
                })
            })
        })?
        .join()
        .map_err(|_| "native driver callback panicked")??;
    assert_eq!(observed, "native-driver");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn in_place_callback_outlasts_its_callers_cooperative_budget()
-> Result<(), Box<dyn std::error::Error>> {
    let runtime = CallbackRuntime::create()?;
    let lock = tokio::sync::Mutex::new(());
    // Each acquisition spends cooperative budget; far more than one
    // task's budget must complete while the calling task is blocked.
    let acquired = runtime.wait(|| async {
        let mut acquired = 0_u32;
        for _ in 0..4_096 {
            drop(lock.lock().await);
            acquired += 1;
        }
        Ok(acquired)
    });
    assert_eq!(acquired?, 4_096);
    Ok(())
}

#[test]
fn an_abandoned_grouped_change_never_applies() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let abandoned = native_test_path("abandoned.bin");
    let kept = native_test_path("kept.bin");
    let create = |path: &MountPath| -> Result<GroupedChange, MountSourceError> {
        Ok(GroupedChange::CreateFile {
            path: source.path(path)?,
            metadata: Box::new(metadata()),
            file_id: FileId::new(),
            bytes: acyclic_stream::OwnedBytes::new(),
        })
    };
    let (abandoned_change, kept_change) = (create(&abandoned)?, create(&kept)?);
    source.runtime.block_on(|| async {
        // While another holder keeps the checkout, one caller queues its
        // change and gives up waiting, as a callback deadline does.
        let held = source.checkout.lock().await;
        let gave_up = tokio::time::timeout(
            Duration::from_millis(20),
            source
                .checkout
                .apply_grouped(abandoned_change, &source.cancellation),
        )
        .await;
        assert!(gave_up.is_err(), "the checkout was held");
        drop(held);
        source
            .checkout
            .apply_grouped(kept_change, &source.cancellation)
            .await
    })?;
    assert!(source.lookup(&kept)?.is_some());
    assert!(
        source.lookup(&abandoned)?.is_none(),
        "a change its caller saw fail applied later"
    );
    Ok(())
}

#[test]
fn abandoning_a_blocked_caller_leaves_other_callers_live() -> Result<(), Box<dyn std::error::Error>>
{
    let source = source(FilesystemProfile::Portable)?;
    let abandoned = native_test_path("drainer.bin");
    let kept = native_test_path("waiter.bin");
    let create = |path: &MountPath| -> Result<GroupedChange, MountSourceError> {
        Ok(GroupedChange::CreateFile {
            path: source.path(path)?,
            metadata: Box::new(metadata()),
            file_id: FileId::new(),
            bytes: acyclic_stream::OwnedBytes::new(),
        })
    };
    let (abandoned_change, kept_change) = (create(&abandoned)?, create(&kept)?);
    source.runtime.block_on(|| async {
        let held = source.checkout.lock().await;
        // Both callers wait for the same held checkout.
        let mut drainer = Box::pin(
            source
                .checkout
                .apply_grouped(abandoned_change, &source.cancellation),
        );
        assert!(futures::poll!(drainer.as_mut()).is_pending());
        let checkout = Arc::clone(&source.checkout);
        let cancellation = source.cancellation.clone();
        let waiter =
            tokio::spawn(async move { checkout.apply_grouped(kept_change, &cancellation).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Abandoning the first caller must not stop the second.
        drop(drainer);
        drop(held);
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .map_err(|_| MountSourceError::Stale)?
            .map_err(|_| MountSourceError::Stale)?
    })?;
    assert!(source.lookup(&kept)?.is_some());
    assert!(source.lookup(&abandoned)?.is_none());
    Ok(())
}

#[test]
fn callback_runtime_is_safe_inside_multi_and_current_thread_runtimes()
-> Result<(), Box<dyn std::error::Error>> {
    let callback = CallbackRuntime::create()?;
    let multi = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build()?;
    let current = tokio::runtime::Builder::new_current_thread().build()?;
    let profile = if cfg!(target_os = "windows") {
        FilesystemProfile::Windows
    } else {
        FilesystemProfile::Posix
    };
    for runtime in [&multi, &current] {
        let source = source(profile)?;
        runtime.block_on(async {
            let value = callback.block_on(|| async {
                // This future is deliberately non-Send across a real reactor wait.
                let local = std::rc::Rc::new(42_u8);
                tokio::time::sleep(Duration::from_millis(1)).await;
                *local
            });
            assert_eq!(value, 42);
            let path = native_test_path("runtime.bin");
            source.volume_id()?;
            let created = source.create_file(&path, metadata())?;
            source.write_range(&path, 0, Bytes::from_static(b"base"))?;
            let attached = source.open_file(&path)?;
            attached.write_range(4, Bytes::from_static(b"-open"))?;
            assert_eq!(attached.read_range(0, 9)?.as_ref(), b"base-open");
            let detached = source.detach_file(&path)?;
            source.remove(&path, Some(created.node.file_id))?;
            detached.write_range(0, Bytes::from_static(b"live"))?;
            assert_eq!(detached.read_range(0, 9)?.as_ref(), b"live-open");
            source.sync()?;
            Ok::<(), MountSourceError>(())
        })?;
    }
    Ok(())
}
