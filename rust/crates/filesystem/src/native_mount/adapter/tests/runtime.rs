#[cfg(target_os = "macos")]
#[test]
#[allow(unsafe_code, reason = "calls the NFS transport's C test hooks")]
fn nfs_callback_protocol_cases() {
    for hook in [
        nfs4_test_exclusive_replay_identity,
        nfs4_test_namedattr_exclusive_replay_identity,
        nfs4_test_readdir_cookie_verifier,
        nfs4_test_sync_acknowledgement,
    ] {
        // SAFETY: each hook owns its callback state and takes no arguments.
        assert_eq!(unsafe { hook() }, 0);
    }
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
    fn admit(&self, object_id: ObjectId) -> Result<(), crate::ObjectStoreError> {
        if object_id.kind == crate::storage::ObjectKind::Metadata
            && self.refuse_metadata.load(Ordering::Acquire)
        {
            return Err(crate::ObjectStoreError::Corrupt);
        }
        Ok(())
    }
}

impl AsyncObjectStore for MetadataRefusingObjects {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.admit(object_id)?;
        self.inner.put(object_id, bytes, context).await
    }
    async fn put_many(
        &self,
        writes: &[crate::storage::ObjectWrite],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        for write in writes {
            self.admit(write.object_id)?;
        }
        self.inner.put_many(writes, context).await
    }
    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        self.inner.read(object_id, maximum_bytes, context).await
    }
    async fn read_many(
        &self,
        requests: &[crate::storage::ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        self.inner.read_many(requests, context).await
    }
    async fn contains(
        &self,
        object_id: ObjectId,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        self.inner.contains(object_id, context).await
    }
}

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
fn abandoning_a_blocked_change_never_applies_and_leaves_other_callers_live()
-> Result<(), Box<dyn std::error::Error>> {
    let source = Arc::new(source(FilesystemProfile::Portable)?);
    let abandoned = native_test_path("abandoned");
    let kept = native_test_path("kept");
    let work = capture_meter(boundary_budget(), source.cancellation())?;
    let create = |path| AuthoredMutation::CreateFile {
        path,
        metadata: metadata(),
        bytes: OwnedBytes::new(),
    };
    source.runtime.wait(|| async {
        let held = source.checkout.lock().await;
        let mut drainer = Box::pin(source.create(&abandoned, &work, create));
        assert!(futures::poll!(drainer.as_mut()).is_pending());
        let waiter_source = Arc::clone(&source);
        let waiter_work = work.clone();
        let waiter_path = kept.clone();
        let waiter = tokio::spawn(async move {
            waiter_source
                .create(&waiter_path, &waiter_work, create)
                .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), drainer.as_mut())
                .await
                .is_err()
        );
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
            let value = callback
                .block_on(
                    || async {
                        // This future is deliberately non-Send across a real reactor wait.
                        let local = std::rc::Rc::new(42_u8);
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        *local
                    },
                    None,
                )
                .map_err(capture_io_error)?;
            assert_eq!(value, 42);
            let path = native_test_path("runtime.bin");
            source.volume_id()?;
            let created = boundary(source.cancellation(), |work| {
                source.create_file(&path, metadata(), None, work)
            })?;
            source.write_range(&path, 0, Bytes::from_static(b"base"))?;
            let attached = source.open_file(&path)?;
            attached.write_range(4, Bytes::from_static(b"-open"))?;
            assert_eq!(attached.read_range(0, 9)?.as_ref(), b"base-open");
            let detached = source.detach_file(&path)?;
            boundary(source.cancellation(), |work| {
                source.remove(&path, Some(created.node.file_id), work)
            })?;
            detached.write_range(0, Bytes::from_static(b"live"))?;
            assert_eq!(detached.read_range(0, 9)?.as_ref(), b"live-open");
            source.sync()?;
            Ok::<(), MountSourceError>(())
        })?;
    }
    Ok(())
}
