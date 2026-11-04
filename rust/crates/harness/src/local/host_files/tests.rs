use super::*;
use crate::local::host_owner::SessionAction;
use std::time::Duration;

fn request_storage_cause(error: &Error, operation: OperationId) -> &str {
    assert_eq!(
        error.request_operation_id(),
        Some(operation),
        "observation failure must retain its typed request identity"
    );
    let Error::Command(failure) = error else {
        panic!("original request cause was discarded")
    };
    let original = failure
        .original()
        .expect("local observation retains its actual cause");
    assert_eq!(
        error.classification(),
        original.classification(),
        "request identity must not change the original failure classification"
    );
    let Error::Storage(cause) = original else {
        panic!("original storage cause was replaced")
    };
    cause
}

fn reply_path(status: &Path, operation: OperationId) -> PathBuf {
    status.join(format!("{operation}.json"))
}

fn remove_projection(path: &Path) -> Result<()> {
    let (parent, leaf) = projection_parent(path)?;
    remove_held_projection(&parent, &leaf)
}

fn regular_bytes(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let (parent, leaf) = projection_parent(path)?;
    held_bytes(&parent, &leaf, maximum)
}

fn immutable(path: &Path, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 > MAXIMUM_REQUEST_BYTES {
        return Err(Error::Invalid(
            "native file projection exceeds its bound".into(),
        ));
    }
    let (parent, leaf) = projection_parent(path)?;
    parent
        .publish_immutable(&leaf, bytes, acyclic_fs::NativeDurability::Durable)
        .map_err(native_file_error)
}

fn projection_parent(path: &Path) -> Result<(acyclic_fs::HeldRoot, PathBuf)> {
    let endpoint = path
        .ancestors()
        .find(|ancestor| ancestor.ends_with(DIRECTORY))
        .ok_or_else(|| Error::Invalid("native projection is outside its workspace".into()))?;
    let workspace = endpoint
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| Error::Invalid("native projection workspace absent".into()))?;
    let parent = path
        .parent()
        .ok_or_else(|| Error::Invalid("native projection parent absent".into()))?;
    let leaf = PathBuf::from(
        path.file_name()
            .ok_or_else(|| Error::Invalid("native projection leaf absent".into()))?,
    );
    let root = acyclic_fs::hold_root(workspace).map_err(native_file_error)?;
    let parent = root
        .open_directory(parent.strip_prefix(workspace).map_err(storage)?)
        .map_err(native_file_error)?;
    Ok((parent, leaf))
}

fn carrier_io() -> Result<(tempfile::TempDir, CarrierIo, std::fs::File)> {
    use acyclic_fs::NativeDurability::Visibility;

    let checkout = tempfile::tempdir().map_err(storage)?;
    let root = acyclic_fs::hold_root(checkout.path()).map_err(native_file_error)?;
    // The fixture is discarded after its live-handle ownership controls.
    let directory = root
        .create_directories(Path::new(DIRECTORY), Visibility)
        .map_err(native_file_error)?;
    let requests = directory
        .create_directories(Path::new("requests"), Visibility)
        .map_err(native_file_error)?;
    let status = directory
        .create_directories(Path::new("status"), Visibility)
        .map_err(native_file_error)?;
    let lock_file = || {
        directory
            .open_file(Path::new("owner.lock"), true)
            .map_err(native_file_error)
    };
    let ownership = acyclic_native_runtime::FileLock::try_acquire(
        lock_file()?,
        acyclic_native_runtime::FileLockMode::Exclusive,
    )
    .map_err(storage)?;
    let probe = lock_file()?;
    Ok((
        checkout,
        CarrierIo {
            served: Served::default(),
            requests: Arc::new(requests),
            status,
            _ownership: Arc::new(ownership),
        },
        probe,
    ))
}

#[tokio::test(flavor = "current_thread")]
async fn carrier_io_publishes_without_blocking_its_coordinator() -> Result<()> {
    use tracing_subscriber::layer::SubscriberExt as _;

    let (_checkout, mut files, probe) = carrier_io()?;
    let operation = OperationId::new();
    let report = acyclic_native_runtime::obs::WaitReport::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
    let coordinator = std::thread::current().id();
    files
        .served
        .known
        .insert(operation, Tracked::Running { withdrawn: false });
    let status = files.status.path().to_path_buf();
    let (started, began) = tokio::sync::oneshot::channel();
    let (release, released) = mpsc::sync_channel(1);
    let mut pending = Box::pin(files.apply(operation, move |files| {
        assert_ne!(
            std::thread::current().id(),
            coordinator,
            "native carrier ran on its coordinator"
        );
        let _ = started.send(std::thread::current().id());
        released.recv().map_err(storage)?;
        files.served.answer(
            &files.status,
            operation,
            Ok(serde_json::json!({"reply": 7})),
        )
    }));
    let mut inline_caller = Box::pin(std::future::poll_fn(|context| {
        let _inline = acyclic_native_runtime::InlineBlocking::enter();
        tracing::dispatcher::with_default(&dispatch, || {
            std::future::Future::poll(pending.as_mut(), context)
        })
    }));
    let producer = tokio::select! {
        producer = began => producer.map_err(storage)?,
        _ = &mut inline_caller => panic!("the original write must wait for its release event"),
    };
    drop(inline_caller);
    assert_ne!(producer, std::thread::current().id());
    // The coordinator runs while its original native producer is blocked.
    let sibling = tokio::spawn(async { 7 });
    assert_eq!(sibling.await.map_err(storage)?, 7);
    assert!(
        report.snapshot().iter().any(|wait| {
            wait.primitive == "native-workspace-files" && wait.key == operation.to_string()
        }),
        "the blocked original publication must retain its report identity"
    );
    let error = acyclic_native_runtime::FileLock::try_acquire(
        probe.try_clone().map_err(storage)?,
        acyclic_native_runtime::FileLockMode::Shared,
    )
    .expect_err("an accepted write retains its original owner");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    release.send(()).map_err(storage)?;
    let mut files = pending.await?;
    assert!(
        report.snapshot().is_empty(),
        "physical completion retires its wait"
    );
    let reply: Value =
        serde_json::from_slice(&regular_bytes(&reply_path(&status, operation), u64::MAX)?)
            .map_err(storage)?;
    assert_eq!(reply["ok"], serde_json::json!({"reply": 7}));
    // Publishing a diagnostic does not retire its original allocation. Use a
    // real rejected directory read and keep that exact cause until withdrawal.
    let original =
        regular_bytes(files.requests.path(), u64::MAX).expect_err("a directory is not a reply");
    let diagnostic = |cause: &Error| match cause {
        Error::Invalid(message) | Error::Storage(message) => message.as_ptr(),
        cause => panic!("directory refusal must retain its diagnostic: {cause}"),
    };
    let original_diagnostic = diagnostic(&original);
    let original_code = original.code();
    let failed = OperationId::new();
    files
        .served
        .known
        .insert(failed, Tracked::Running { withdrawn: false });
    files = files
        .apply(failed, move |files| {
            files.served.answer(&files.status, failed, Err(original))
        })
        .await?;
    let Some(Tracked::Answered(Some(cause))) = files.served.known.get(&failed) else {
        panic!("published failure must retain its original cause until withdrawal");
    };
    assert_eq!(cause.code(), original_code);
    assert_eq!(diagnostic(cause), original_diagnostic);
    files = files
        .apply(failed, move |files| {
            files.served.withdraw(&files.status, failed)
        })
        .await?;
    assert!(!files.served.known.contains_key(&failed));
    assert!(matches!(
        regular_bytes(&reply_path(&status, failed), u64::MAX),
        Err(Error::NotFound(_))
    ));
    drop(files);
    acyclic_native_runtime::FileLock::try_acquire(
        probe,
        acyclic_native_runtime::FileLockMode::Shared,
    )
    .map_err(storage)?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn carrier_io_observer_loss_keeps_the_accepted_physical_owner() -> Result<()> {
    let (_checkout, files, probe) = carrier_io()?;
    let (started, began) = tokio::sync::oneshot::channel();
    let (release, released) = mpsc::sync_channel(1);
    let mut pending = Box::pin(files.apply(OperationId::new(), move |_| {
        let _ = started.send(());
        released.recv().map_err(storage)
    }));
    tokio::select! {
        started = began => started.map_err(storage)?,
        _ = &mut pending => panic!("the original job must wait for its release event"),
    }
    drop(pending);
    let error = acyclic_native_runtime::FileLock::try_acquire(
        probe.try_clone().map_err(storage)?,
        acyclic_native_runtime::FileLockMode::Shared,
    )
    .expect_err("dropping observation cannot release accepted physical custody");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    release.send(()).map_err(storage)?;
    // The OS lock waits for this same accepted producer to finish and release
    // the original guard; no observer, retry or replacement producer is used.
    acyclic_native_runtime::FileLock::acquire(probe, acyclic_native_runtime::FileLockMode::Shared)
        .map_err(storage)?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn carrier_io_producer_loss_keeps_its_failure_and_releases_its_files() -> Result<()> {
    let (_checkout, files, probe) = carrier_io()?;
    let (producer, receiver) = mpsc::channel::<()>();
    drop(producer);
    let result = files
        .apply(OperationId::new(), move |_| {
            receiver.recv().map_err(storage)
        })
        .await;
    assert!(matches!(result, Err(Error::Storage(cause)) if cause == mpsc::RecvError.to_string()));
    acyclic_native_runtime::FileLock::try_acquire(
        probe,
        acyclic_native_runtime::FileLockMode::Shared,
    )
    .map_err(storage)?;

    let (_checkout, mut files, probe) = carrier_io()?;
    let operation = OperationId::new();
    let original =
        regular_bytes(files.requests.path(), u64::MAX).expect_err("a directory is not a reply");
    let message = original.to_string();
    let code = original.code();
    files
        .served
        .known
        .insert(operation, Tracked::Running { withdrawn: false });
    files = files
        .apply(operation, move |files| {
            files
                .served
                .answer(&files.status, operation, Err(original))?;
            let path = reply_path(files.status.path(), operation);
            remove_projection(&path)?;
            std::fs::create_dir(path).map_err(storage)
        })
        .await?;
    let cleanup = remove_projection(&reply_path(files.status.path(), operation))
        .expect_err("a directory cannot be retired as a reply file");
    let result = files
        .apply(operation, move |files| {
            files.served.withdraw(&files.status, operation)
        })
        .await;
    let Err(Error::Cleanup(causes)) = result else {
        panic!("failed withdrawal must retain the original reply and retirement causes");
    };
    assert_eq!(causes.primary().code(), code);
    assert_eq!(causes.primary().to_string(), message);
    assert_eq!(causes.cleanup().code(), cleanup.code());
    assert_eq!(causes.cleanup().to_string(), cleanup.to_string());
    acyclic_native_runtime::FileLock::try_acquire(
        probe,
        acyclic_native_runtime::FileLockMode::Shared,
    )
    .map_err(storage)?;
    Ok(())
}

#[test]
fn owner_exit_retires_only_its_original_registry_entries() -> Result<()> {
    fn ownership(workspace: &Path) -> Result<acyclic_native_runtime::FileLock> {
        let directory =
            acyclic_fs::hold_root(&workspace.join(DIRECTORY)).map_err(native_file_error)?;
        acyclic_native_runtime::FileLock::try_acquire(
            directory
                .open_file(Path::new("owner.lock"), true)
                .map_err(native_file_error)?,
            acyclic_native_runtime::FileLockMode::Exclusive,
        )
        .map_err(storage)
    }
    fn owner() -> Result<(tempfile::TempDir, PathBuf, acyclic_native_runtime::FileLock)> {
        let checkout = tempfile::tempdir().map_err(storage)?;
        let workspace = std::fs::canonicalize(checkout.path()).map_err(storage)?;
        let directory = workspace.join(DIRECTORY);
        std::fs::create_dir_all(directory.join("requests")).map_err(storage)?;
        std::fs::create_dir(directory.join("status")).map_err(storage)?;
        let ownership = ownership(&workspace)?;
        immutable(
            &directory.join("owner.json"),
            &serde_json::to_vec(&Endpoint {
                version: 3,
                session: uuid::Uuid::new_v4(),
                workspace: workspace.clone(),
            })
            .map_err(storage)?,
        )?;
        Ok((checkout, workspace, ownership))
    }
    fn observer(bridge: &Bridge) -> std::thread::JoinHandle<()> {
        lock(&bridge.observer)
            .take()
            .expect("the original owner observer")
    }

    let registry = Arc::new(Mutex::<Registry>::default());
    let (_checkout, cwd, owner_lock) = owner()?;
    let (_other_checkout, other_cwd, other_lock) = owner()?;
    let child = cwd.join("child");
    std::fs::create_dir(&child).map_err(storage)?;
    let first = Registry::bridge(&registry, &cwd, Bridge::open)?;
    let alias = Registry::bridge(&registry, &child, Bridge::open)?;
    assert!(Arc::ptr_eq(&first, &alias));
    let other = Registry::bridge(&registry, &other_cwd, Bridge::open)?;
    let original = lock(&first.shared.waiters).retirement.clone().unwrap();
    let original_connection = original.connection.upgrade().unwrap();
    let first_observer = observer(&first);
    let other_observer = observer(&other);
    let (delivery, response) = mpsc::sync_channel(1);
    let waiting = first.wait(OperationId::new(), delivery)?;
    drop(owner_lock);
    first_observer
        .join()
        .expect("the original owner terminal must return");
    assert!(matches!(response.try_recv(), Ok(Delivery::Gone)));
    drop(waiting);
    let (delivery, _) = mpsc::sync_channel(1);
    assert!(matches!(
        first.wait(OperationId::new(), delivery),
        Err(Error::NotFound(_))
    ));
    {
        let index = lock(&registry);
        assert_eq!(
            index.bridges.len(),
            1,
            "no lookup may be needed to retire a dead owner"
        );
        assert_eq!(
            index.located.len(),
            1,
            "all and only the original owner's aliases retire"
        );
    }
    assert!(Arc::ptr_eq(
        &Registry::live(&registry, &other_cwd).unwrap(),
        &other
    ));

    // A new admitted owner at the same path gets its own slot. A delayed old
    // terminal and a borrowed old slot must not remove or reopen that owner.
    let replacement_lock = ownership(&cwd)?;
    let replacement = Registry::bridge(&registry, &cwd, Bridge::open)?;
    let replacement_alias = Registry::bridge(&registry, &child, Bridge::open)?;
    assert!(!Arc::ptr_eq(&replacement, &first));
    assert!(Arc::ptr_eq(&replacement, &replacement_alias));
    assert!(lock(&original_connection).retired);
    original.retire();
    assert!(Arc::ptr_eq(
        &Registry::live(&registry, &cwd).unwrap(),
        &replacement
    ));
    assert!(Arc::ptr_eq(
        &Registry::live(&registry, &child).unwrap(),
        &replacement
    ));
    assert!(Arc::ptr_eq(
        &Registry::live(&registry, &other_cwd).unwrap(),
        &other
    ));
    let weak = Arc::downgrade(&first);
    drop((first, alias, original_connection));
    assert!(
        weak.upgrade().is_none(),
        "only original caller custody may retain the retired bridge"
    );
    let replacement_observer = observer(&replacement);
    drop(replacement_lock);
    replacement_observer
        .join()
        .expect("replacement owner terminal must return");

    // Real owner loss before registration is not allowed to publish an alias
    // with no future producer. Join that same native observer inside setup.
    let early_lock = ownership(&cwd)?;
    let original_directory = cwd.join(DIRECTORY);
    let retained_directory = original_directory.with_file_name("retained-host-files");
    let result = Registry::bridge(&registry, &cwd, |directory, endpoint| {
        // Discovery has admitted the descriptor. Connection must use that
        // same directory even if its name changes before the owner probe.
        std::fs::rename(&original_directory, &retained_directory).map_err(storage)?;
        std::fs::create_dir(&original_directory).map_err(storage)?;
        let bridge = Bridge::open(directory, endpoint)
            .expect("discovery retains its original directory through connection admission");
        std::fs::remove_dir(&original_directory).map_err(storage)?;
        std::fs::rename(&retained_directory, &original_directory).map_err(storage)?;
        let observing = observer(&bridge);
        drop(early_lock);
        observing.join().expect("early owner terminal must return");
        Ok(bridge)
    });
    assert!(matches!(result, Err(Error::NotFound(_))));
    assert_eq!(lock(&registry).bridges.len(), 1);
    assert_eq!(lock(&registry).located.len(), 1);
    assert!(Arc::ptr_eq(
        &Registry::live(&registry, &other_cwd).unwrap(),
        &other
    ));

    // A terminal observation error is not owner death. It must still release
    // registry custody, while original callers retain its exact first cause.
    let (_failed_checkout, failed_cwd, failed_lock) = owner()?;
    let failed_child = failed_cwd.join("child");
    std::fs::create_dir(&failed_child).map_err(storage)?;
    let failed = Registry::bridge(&registry, &failed_cwd, Bridge::open)?;
    let failed_alias = Registry::bridge(&registry, &failed_child, Bridge::open)?;
    let failed_observer = observer(&failed);
    let retired = lock(&failed.shared.waiters).retirement.clone().unwrap();
    let failed_connection = retired.connection.upgrade().unwrap();
    let operation = OperationId::new();
    let (delivery, response) = mpsc::sync_channel(1);
    let waiting = failed.wait(operation, delivery)?;
    failed
        .shared
        .owner_finished(Err(std::io::ErrorKind::BrokenPipe.into()));
    let failure = lock(&failed.shared.waiters).failure.clone().unwrap();
    assert!(matches!(response.try_recv(),
        Ok(Delivery::Reply(Err(error)))
            if request_storage_cause(&error, operation) == failure));
    assert!(!lock(&failed.shared.waiters).gone);
    assert!(lock(&failed.shared.waiters).retirement.is_none());
    assert_eq!(lock(&registry).bridges.len(), 1);
    assert_eq!(lock(&registry).located.len(), 1);
    assert!(lock(&failed_connection).retired);
    let (delivery, _) = mpsc::sync_channel(1);
    assert!(matches!(failed.wait(OperationId::new(), delivery),
        Err(Error::Storage(error)) if error == failure));
    drop(waiting);
    assert!(lock(&failed.watching).watches.is_none());
    // This is a new caller's admission, not a retry of the failed operation.
    let fresh = Registry::bridge(&registry, &failed_cwd, Bridge::open)?;
    let fresh_alias = Registry::bridge(&registry, &failed_child, Bridge::open)?;
    assert!(!Arc::ptr_eq(&failed, &fresh));
    assert!(Arc::ptr_eq(&fresh, &fresh_alias));
    retired.retire();
    assert!(Arc::ptr_eq(
        &Registry::live(&registry, &failed_cwd).unwrap(),
        &fresh
    ));
    let weak = Arc::downgrade(&failed);
    drop((failed, failed_alias));
    assert!(weak.upgrade().is_none());
    let fresh_observer = observer(&fresh);
    drop(failed_lock);
    failed_observer
        .join()
        .expect("original native observer ends");
    fresh_observer.join().expect("fresh native observer ends");
    assert_eq!(lock(&registry).bridges.len(), 1);
    assert_eq!(lock(&registry).located.len(), 1);

    // A failure delivered before the registry can attach retirement cannot
    // leave a connection or alias with no original observation producer.
    let early_lock = ownership(&cwd)?;
    let result = Registry::bridge(&registry, &cwd, |directory, endpoint| {
        let bridge = Bridge::open(directory, endpoint)?;
        bridge
            .shared
            .owner_finished(Err(std::io::ErrorKind::BrokenPipe.into()));
        drop(early_lock);
        observer(&bridge)
            .join()
            .expect("early native observer ends");
        Ok(bridge)
    });
    assert!(matches!(result, Err(Error::Storage(ref error))
        if error.contains("step=observe native workspace owner exit:")));
    assert_eq!(lock(&registry).bridges.len(), 1);
    assert_eq!(lock(&registry).located.len(), 1);

    let refused = Arc::new(Mutex::<Registry>::default());
    let failed = Registry::bridge(&refused, &other_cwd, |_, _| {
        Err(Error::Unsupported(
            "original native admission refused".into(),
        ))
    });
    assert!(matches!(failed, Err(Error::Unsupported(_))));
    assert!(lock(&refused).bridges.is_empty());
    assert!(lock(&refused).located.is_empty());
    drop(other_lock);
    other_observer
        .join()
        .expect("unrelated original owner terminal must return");
    assert!(lock(&registry).bridges.is_empty());
    assert!(lock(&registry).located.is_empty());
    Ok(())
}

fn rejected_requests_end_without_owner_exit() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(storage)?;
    let workspace = std::fs::canonicalize(checkout.path()).map_err(storage)?;
    let directory = workspace.join(DIRECTORY);
    let requests = directory.join("requests");
    std::fs::create_dir_all(&requests).map_err(storage)?;
    std::fs::create_dir(directory.join("status")).map_err(storage)?;
    let root = acyclic_fs::hold_root(&directory).map_err(native_file_error)?;
    let request_root = root
        .open_directory(Path::new("requests"))
        .map_err(native_file_error)?;
    let status = root
        .open_directory(Path::new("status"))
        .map_err(native_file_error)?;
    let owner = acyclic_native_runtime::FileLock::try_acquire(
        root.open_file(Path::new("owner.lock"), true)
            .map_err(native_file_error)?,
        acyclic_native_runtime::FileLockMode::Exclusive,
    )
    .map_err(storage)?;
    let session = uuid::Uuid::new_v4();
    let mut endpoint = Endpoint {
        version: 2,
        session,
        workspace,
    };
    let descriptor = directory.join("owner.json");
    immutable(
        &descriptor,
        &serde_json::to_vec(&endpoint).map_err(storage)?,
    )?;
    assert!(matches!(
        discover(&endpoint.workspace),
        Err(Error::Unsupported(reason)) if reason.contains("requires 3")
    ));
    remove_projection(&descriptor)?;
    endpoint.version = 3;
    immutable(
        &descriptor,
        &serde_json::to_vec(&endpoint).map_err(storage)?,
    )?;
    let (directory, endpoint) = discover(&endpoint.workspace)?;
    let bridge = Arc::new(Bridge::open(directory, endpoint)?);
    let mut served = Served::default();
    for fault in 0..3 {
        let operation = OperationId::new();
        let path = requests.join(format!("{operation}.json"));
        let (delivery, response) = mpsc::sync_channel(1);
        let waiting = bridge.wait(operation, delivery)?;
        // An old removal observed before publication cannot reject new work.
        bridge
            .shared
            .note(Ok(notify::Event::new(notify::EventKind::Remove(
                notify::event::RemoveKind::File,
            ))
            .add_path(path.clone())));
        assert!(matches!(
            response.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        let mut request = Request {
            session,
            operation,
            action: HostAction::Open {
                host: "codex".into(),
                session: "must-not-dispatch".into(),
            },
        };
        immutable(&path, &serde_json::to_vec(&request).map_err(storage)?)?;
        bridge.shared.published(operation);
        assert!(matches!(
            response.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        let bytes = match fault {
            0 => b"{malformed request".to_vec(),
            1 => {
                request.session = uuid::Uuid::new_v4();
                serde_json::to_vec(&request).map_err(storage)?
            }
            _ => {
                request.operation = OperationId::new();
                serde_json::to_vec(&request).map_err(storage)?
            }
        };
        std::fs::write(&path, bytes).map_err(storage)?;
        served.touch(
            &request_root,
            &status,
            session,
            Touched {
                names: BTreeSet::from([OsString::from(format!("{operation}.json"))]),
                rescan: false,
                failure: None,
            },
        )?;
        assert!(
            served.start().is_empty(),
            "rejected bytes must never dispatch"
        );
        assert!(served.known.is_empty());
        assert_eq!(served.rescans, 0);
        assert!(
            matches!(response.recv_timeout(Duration::from_secs(2)).map_err(storage)?,
            Delivery::Reply(Err(Error::Indeterminate(id))) if id == operation)
        );
        assert!(
            !lock(&bridge.shared.waiters).gone,
            "the real owner lock remains held"
        );
        drop(waiting);
        // No new notification is needed by a waiter arriving after rejection.
        let (delivery, response) = mpsc::sync_channel(1);
        let late = bridge.wait(operation, delivery)?;
        bridge.shared.published(operation);
        assert!(matches!(response.try_recv(),
            Ok(Delivery::Reply(Err(Error::Indeterminate(id)))) if id == operation));
        drop(late);
        assert!(lock(&bridge.shared.waiters).waiting.is_empty());
        assert_eq!(lock(&bridge.shared.waiters).registered, 0);
        assert!(lock(&bridge.watching).watches.is_none());
    }
    // Exercise caller cleanup, not just delivery: the rejecting owner has
    // already removed the input when request_operation withdraws it again.
    let operation = OperationId::new();
    let leaf = OsString::from(format!("{operation}.json"));
    let (changed, changes) = mpsc::channel();
    let mut watcher = notify::recommended_watcher(move |event| {
        if projection_changed(&event) {
            let _ = changed.send(event);
        }
    })
    .map_err(storage)?;
    watcher
        .watch(&requests, RecursiveMode::NonRecursive)
        .map_err(storage)?;
    let registry = Arc::new(Mutex::<Registry>::default());
    let (completed, completion) = mpsc::sync_channel(1);
    let result = std::thread::scope(|scope| {
        scope.spawn(|| {
            let _ = completed.send(request_operation(
                &registry,
                &bridge.workspace,
                operation,
                HostAction::Open {
                    host: "codex".into(),
                    session: "rejected-caller-cleanup".into(),
                },
            ));
        });
        let observation = (|| {
            loop {
                let event = changes
                    .recv_timeout(Duration::from_secs(2))
                    .map_err(storage)?;
                let event = event.map_err(storage)?;
                if event
                    .paths
                    .iter()
                    .any(|path| path.file_name() == Some(&leaf))
                {
                    break;
                }
            }
            served.observe(&request_root, &status, uuid::Uuid::new_v4(), &leaf)?;
            assert!(served.start().is_empty());
            assert!(!lock(&bridge.shared.waiters).gone);
            completion
                .recv_timeout(Duration::from_secs(2))
                .map_err(storage)
        })();
        // Even a failed assertion guard must release the original owner so
        // the scoped caller can finish; it must not leave a joining thread.
        drop(owner);
        observation
    })?;
    let error = result.expect_err("a rejected request has no completed reply");
    assert_eq!(error.request_operation_id(), Some(operation));
    assert_eq!(
        error.classification(),
        Error::Indeterminate(operation).classification()
    );
    let Error::Command(failure) = error else {
        panic!("the request lost its original terminal cause")
    };
    assert!(matches!(failure.original(), Some(Error::Indeterminate(id)) if *id == operation));
    Ok(())
}

async fn projection_event(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
    owner: &mut tokio::task::JoinHandle<Result<()>>,
) -> Result<()> {
    let failed = |cause| {
        Error::Storage(format!(
            "step=observe workspace conformance: {cause}; recovery=cargo test --locked -p acyclic-harness --no-default-features --features filesystem-local,native-execution --lib local::host_files::tests::workspace_projection_uses_shared_results_without_client_state -- --exact"
        ))
    };
    tokio::select! {
        event = events.recv() => match event {
            Some(Ok(_)) => Ok(()),
            Some(Err(error)) => Err(failed(error.to_string())),
            None => Err(failed("notification producer closed".into())),
        },
        result = owner => Err(failed(format!("workspace owner ended: {result:?}"))),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn workspace_shutdown_retains_its_cause_when_retirement_refuses() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let local = Arc::new(
        LocalHarness::open(
            LocalHarnessConfig::native_host(checkout.path())
                .state_directory(state.path().join("owner")),
        )
        .await?,
    );
    let mut failures = Vec::new();
    let operation = OperationId::from_bytes([7; 16]);
    let descriptor = checkout.path().join(DIRECTORY).join("owner.json");
    for (ready, shutdown) in [
        (Ok(()), Err(Error::Cancelled)),
        (
            Ok(()),
            Err(Error::Unsupported("original shutdown producer".into())),
        ),
        (Ok(()), Err(Error::Indeterminate(operation))),
        (Ok(()), Ok(())),
        (
            Err(Error::Unsupported("original readiness output".into())),
            Ok(()),
        ),
    ] {
        let expected = ready
            .as_ref()
            .err()
            .or_else(|| shutdown.as_ref().err())
            .map(Error::code);
        let refused = ready.is_err();
        let stale = descriptor.is_file();
        let (mut announced, mut owned, mut shutdown_polled) = (0, false, false);
        // The extra public harness handle makes the original retirement
        // refuse disconnect after it has joined view cleanup. No replacement
        // producer or injected filesystem result supplies that refusal.
        let failure = serve_until(
            Arc::clone(&local),
            || {
                announced += 1;
                owned = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(checkout.path().join(DIRECTORY).join("owner.lock"))
                    .and_then(|file| {
                        acyclic_native_runtime::FileLock::try_acquire(
                            file,
                            acyclic_native_runtime::FileLockMode::Exclusive,
                        )
                    })
                    .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock);
                ready
            },
            async {
                shutdown_polled = true;
                shutdown
            },
        )
        .await
        .expect_err("the retained harness handle must refuse retirement");
        failures.push((
            expected,
            failure,
            announced,
            owned,
            stale,
            refused,
            shutdown_polled,
        ));
    }
    Arc::try_unwrap(local)
        .map_err(|_| Error::Conflict("workspace owner retained after retirement".into()))?
        .disconnect()
        .await?;
    // Release the actual harness before any regression assertion can fail.
    for (index, (expected, failure, announced, owned, stale, refused, shutdown_polled)) in
        failures.into_iter().enumerate()
    {
        assert_eq!(
            announced, 1,
            "each serving lifetime must announce readiness"
        );
        assert!(owned, "readiness must retain the original exclusive owner");
        assert_eq!(
            stale,
            index != 0,
            "restart retains its discovery descriptor"
        );
        assert_eq!(
            shutdown_polled, !refused,
            "readiness refusal must retire without starting request serving"
        );
        assert_eq!(
            failure.code(),
            expected.unwrap_or(crate::wire::ErrorCode::Conflict),
            "retirement must retain the original shutdown classification: {failure}"
        );
        let retirement = if let Error::Cleanup(ref causes) = failure {
            assert_eq!(Some(causes.primary().code()), expected);
            if let Error::Indeterminate(id) = causes.primary() {
                assert_eq!(*id, operation);
            }
            causes.cleanup()
        } else {
            assert!(expected.is_none(), "the original failure must survive");
            &failure
        };
        assert!(matches!(retirement, Error::Conflict(cause)
            if cause == "native owner still has live harness handles"));
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn saved_command_result_follows_its_original_request_directory() -> Result<()> {
    let (checkout, mut files, _probe) = carrier_io()?;
    let state = tempfile::tempdir().map_err(storage)?;
    std::fs::write(checkout.path().join("source.txt"), b"first").map_err(storage)?;
    let local = Arc::new(
        LocalHarness::open(
            LocalHarnessConfig::native_host(checkout.path())
                .state_directory(state.path().join("owner")),
        )
        .await?,
    );
    let directory = checkout.path().join(DIRECTORY);
    let requests = Arc::clone(&files.requests);
    let owner = HostWorkspaceOwner::new(Arc::clone(&local))?.with_requests(Arc::clone(&requests));
    let opened = owner
        .dispatch(
            OperationId::new(),
            HostAction::Open {
                host: "codex".into(),
                session: "request-retention".into(),
            },
        )
        .await?;
    let task: crate::agent::AgentPath =
        serde_json::from_value(opened["task"].clone()).map_err(storage)?;
    let command = |command: &str| HostAction::Fs {
        task: task.clone(),
        command: command.into(),
    };
    owner
        .dispatch(OperationId::new(), command("add source.txt"))
        .await?;
    let operation = OperationId::new();
    let session = uuid::Uuid::new_v4();
    let action = command("commit -m first");
    let leaf = format!("{operation}.json");
    immutable(
        &files.requests.path().join(&leaf),
        &serde_json::to_vec(&Request {
            session,
            operation,
            action: action.clone(),
        })
        .map_err(storage)?,
    )?;
    // Admit the real encoded request before losing its reply.
    files.served.observe(
        &files.requests,
        &files.status,
        session,
        std::ffi::OsStr::new(&leaf),
    )?;
    let request = files
        .served
        .start()
        .pop()
        .ok_or_else(|| Error::NotFound("admitted native request".into()))?;
    let committed = owner.dispatch(request.operation, request.action).await?;
    assert!(committed.get("Committed").is_some());
    // Replacing the directory name does not withdraw the original request.
    let retained = directory.join("retained-requests");
    std::fs::rename(files.requests.path(), &retained).map_err(storage)?;
    std::fs::create_dir(files.requests.path()).map_err(storage)?;
    assert!(retained.join(&leaf).is_file());
    assert!(!files.requests.path().join(&leaf).exists());
    std::fs::write(checkout.path().join("source.txt"), b"second").map_err(storage)?;
    owner
        .dispatch(OperationId::new(), command("commit -m second"))
        .await?;
    let workspace = local
        .workspace()
        .ok_or_else(|| Error::NotFound("native workspace".into()))?;
    let repository = acyclic_fs::GitCompatRepository::new(
        workspace.view().workspace().id(),
        workspace.store.clone(),
    );
    let saved = repository.load().await.map_err(storage)?;
    let identity = acyclic_fs::OperationId::from_bytes(operation.into_bytes());
    assert_eq!(
        serde_json::to_value(saved.request(identity)).map_err(storage)?,
        committed
    );
    assert_eq!(owner.dispatch(operation, action).await?, committed);
    // The same notification owner must still answer through its admitted
    // directories after both names are occupied by unrelated replacements.
    let original_status = directory.join("retained-status");
    std::fs::rename(files.status.path(), &original_status).map_err(storage)?;
    std::fs::create_dir(files.status.path()).map_err(storage)?;
    let replacement_reply = files.status.path().join(&leaf);
    std::fs::write(&replacement_reply, b"replacement reply").map_err(storage)?;
    let next = OperationId::new();
    let next_leaf = format!("{next}.json");
    let pending = Request {
        session,
        operation: next,
        action: command("tag retained-notification"),
    };
    let pending_bytes = serde_json::to_vec(&pending).map_err(storage)?;
    requests
        .publish_immutable(
            Path::new(&next_leaf),
            &pending_bytes,
            acyclic_fs::NativeDurability::Durable,
        )
        .map_err(native_file_error)?;
    // The replacement's invalid bytes must not replace this admitted input.
    std::fs::write(files.requests.path().join(&next_leaf), b"{unrelated").map_err(storage)?;
    files.served.touch(
        &files.requests,
        &files.status,
        session,
        Touched {
            names: BTreeSet::new(),
            rescan: true,
            failure: None,
        },
    )?;
    files
        .served
        .answer(&files.status, operation, Ok(committed.clone()))?;
    let original_reply = original_status.join(&leaf);
    assert_eq!(
        std::fs::read(&original_reply).map_err(storage)?,
        crate::client::encode_command_result(0, operation, &Ok(committed.clone()), u64::MAX)?
    );
    let ready = files.served.start();
    assert_eq!(ready.len(), 1);
    let pending = ready
        .into_iter()
        .next()
        .expect("the original pending input");
    assert_eq!(
        serde_json::to_vec(&pending).map_err(storage)?,
        pending_bytes
    );
    let outcome = owner.dispatch(pending.operation, pending.action).await;
    files.served.answer(&files.status, next, outcome)?;
    // An invalid leaf cannot prove that the client withdrew its request.
    let parked = retained.join("pending.json");
    std::fs::rename(retained.join(&leaf), &parked).map_err(storage)?;
    std::fs::create_dir(retained.join(&leaf)).map_err(storage)?;
    let original_error = native_file_error(
        requests
            .open_file(Path::new(&leaf), false)
            .expect_err("a directory cannot be an original regular request"),
    );
    let observation = files
        .served
        .touch(
            &files.requests,
            &files.status,
            session,
            Touched {
                names: BTreeSet::from([OsString::from(&leaf)]),
                rescan: false,
                failure: None,
            },
        )
        .expect_err("an uncertain original query must retain its cause");
    assert_eq!(observation.code(), original_error.code());
    assert_eq!(observation.to_string(), original_error.to_string());
    assert!(original_reply.is_file());
    owner
        .dispatch(OperationId::new(), command("tag uncertain"))
        .await?;
    assert_eq!(
        serde_json::to_value(repository.load().await.map_err(storage)?.request(identity))
            .map_err(storage)?,
        committed
    );
    std::fs::remove_dir(retained.join(&leaf)).map_err(storage)?;
    std::fs::rename(&parked, retained.join(&leaf)).map_err(storage)?;
    // Only removal in the original held directory proves withdrawal.
    requests
        .remove_file(Path::new(&leaf), acyclic_fs::NativeDurability::Durable)
        .map_err(native_file_error)?;
    files.served.touch(
        &files.requests,
        &files.status,
        session,
        Touched {
            names: BTreeSet::from([OsString::from(&leaf)]),
            rescan: false,
            failure: None,
        },
    )?;
    assert!(!original_reply.exists());
    assert_eq!(
        std::fs::read(&replacement_reply).map_err(storage)?,
        b"replacement reply"
    );
    owner
        .dispatch(OperationId::new(), command("tag answered"))
        .await?;
    assert!(
        repository
            .load()
            .await
            .map_err(storage)?
            .request(identity)
            .is_none()
    );
    drop((repository, owner, files));
    Arc::try_unwrap(local)
        .map_err(|_| Error::Conflict("native result owner retained after shutdown".into()))?
        .disconnect()
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn workspace_projection_uses_shared_results_without_client_state() -> Result<()> {
    rejected_requests_end_without_owner_exit()?;
    let registry = Arc::new(Mutex::<Registry>::default());
    let checkout = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let (changed, mut changes) = tokio::sync::mpsc::unbounded_channel();
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        if projection_changed(&event) {
            let _ = changed.send(event);
        }
    })
    .map_err(storage)?;
    let local = Arc::new(
        LocalHarness::open(
            LocalHarnessConfig::native_host(checkout.path())
                .state_directory(state.path().join("owner")),
        )
        .await?,
    );
    let (ready, started) = tokio::sync::oneshot::channel();
    let mut owner = tokio::spawn(Box::pin(serve_until(
        Arc::clone(&local),
        move || ready.send(()).map_err(|()| Error::Cancelled),
        std::future::pending(),
    )));
    started.await.map_err(storage)?;
    // Follow only coordination files once published, before any native view
    // is admitted. The observer never traverses mounted agent filesystems.
    watcher
        .watch(&checkout.path().join(DIRECTORY), RecursiveMode::Recursive)
        .map_err(storage)?;
    assert!(
        serve(Arc::clone(&local)).await.is_err(),
        "a second workspace owner must not dispatch the same requests"
    );
    let requests = checkout.path().join(DIRECTORY).join("requests");
    // A stale or untrusted status entry must neither select another reply nor
    // be pruned by publication. Its name sorts after every former sequence.
    let debris = checkout
        .path()
        .join(DIRECTORY)
        .join("status/18446744073709551615.json");
    std::fs::create_dir_all(debris.parent().expect("status parent")).map_err(storage)?;
    std::fs::write(&debris, b"unrelated status is not a reply").map_err(storage)?;
    std::fs::write(requests.join("malformed.json"), b"{not a request").map_err(storage)?;
    let foreign = OperationId::new();
    std::fs::write(
        requests.join(format!("{foreign}.json")),
        serde_json::to_vec(&Request {
            session: uuid::Uuid::new_v4(),
            operation: foreign,
            action: HostAction::Open {
                host: "codex".into(),
                session: "must-not-dispatch".into(),
            },
        })
        .map_err(storage)?,
    )
    .map_err(storage)?;
    #[cfg(unix)]
    let outside = {
        let outside = tempfile::NamedTempFile::new().map_err(storage)?;
        std::fs::write(outside.path(), b"outside remains untouched").map_err(storage)?;
        std::os::unix::fs::symlink(outside.path(), requests.join("symlink.json"))
            .map_err(storage)?;
        outside
    };
    let cwd = std::fs::canonicalize(checkout.path()).map_err(storage)?;
    let other_checkout = tempfile::tempdir().map_err(storage)?;
    let other_local = Arc::new(
        LocalHarness::open(
            LocalHarnessConfig::native_host(other_checkout.path())
                .state_directory(state.path().join("other-owner")),
        )
        .await?,
    );
    let (ready, started) = tokio::sync::oneshot::channel();
    let other_owner = tokio::spawn(Box::pin(serve_until(
        Arc::clone(&other_local),
        move || ready.send(()).map_err(|()| Error::Cancelled),
        std::future::pending(),
    )));
    started.await.map_err(storage)?;
    let other_cwd = std::fs::canonicalize(other_checkout.path()).map_err(storage)?;
    let opening_registry = Arc::clone(&registry);
    let opening_cwd = cwd.clone();
    let connected = tokio::task::spawn_blocking(move || {
        std::thread::scope(|scope| -> Result<_> {
            let registry = &opening_registry;
            let cwd = &opening_cwd;
            // Dropping the sole release producer also ends the paused open,
            // so a failed assertion cannot leave the scoped thread waiting.
            let (release, resume) = mpsc::sync_channel(1);
            let (started, opening) = mpsc::sync_channel(1);
            let first = scope.spawn(move || {
                Registry::bridge(registry, cwd, |directory, endpoint| {
                    started.send(()).map_err(storage)?;
                    resume.recv().map_err(storage)?;
                    Bridge::open(directory, endpoint)
                })
            });
            opening.recv().map_err(storage)?;
            assert!(
                registry.try_lock().is_ok(),
                "native connection setup must not hold the registry for unrelated owners"
            );
            let connection = lock(registry).bridges[&cwd.join(DIRECTORY)].clone();
            assert!(
                connection.try_lock().is_err(),
                "the connecting owner must retain its single initialization slot"
            );
            let (started, overlapping) = mpsc::sync_channel(1);
            let second = scope.spawn(move || {
                started.send(()).map_err(storage)?;
                Registry::bridge(registry, cwd, |_, _| {
                    panic!("an overlapping connection must reuse the first native open")
                })
            });
            overlapping.recv().map_err(storage)?;
            let other = Registry::bridge(registry, &other_cwd, Bridge::open)?;
            assert_eq!(other.workspace, other_cwd);
            release.send(()).map_err(storage)?;
            let first = first.join().expect("the first native open must settle")?;
            let second = second
                .join()
                .expect("the overlapping connection must settle")?;
            assert!(Arc::ptr_eq(&first, &second));
            Ok(first)
        })
    })
    .await
    .map_err(storage)??;
    other_owner.abort();
    assert!(other_owner.await.unwrap_err().is_cancelled());
    Arc::try_unwrap(other_local)
        .map_err(|_| Error::Conflict("other workspace owner retained after shutdown".into()))?
        .disconnect()
        .await?;
    assert!(Arc::ptr_eq(
        &Registry::bridge(&registry, &cwd, |_, _| {
            panic!("a warm connection must not repeat native setup")
        })?,
        &connected,
    ));
    // Obtain an actual committed workspace result. On this current-thread
    // runtime the owner cannot process withdrawal between the removal below
    // and the late read: an available result must win over absent input.
    let operation = OperationId::new();
    let (delivery, response) = mpsc::sync_channel(1);
    let waiting = connected.wait(operation, delivery)?;
    let recovery_path = requests.join(format!("{operation}.json"));
    immutable(
        &recovery_path,
        &serde_json::to_vec(&Request {
            session: connected.session,
            operation,
            action: HostAction::Open {
                host: "codex".into(),
                session: "reply-withdrawal-recovery".into(),
            },
        })
        .map_err(storage)?,
    )?;
    connected.shared.published(operation);
    let committed =
        tokio::task::spawn_blocking(move || response.recv_timeout(Duration::from_secs(2)))
            .await
            .map_err(storage)?
            .map_err(storage)?;
    let Delivery::Reply(Ok(committed)) = committed else {
        panic!("the actual owner must publish its committed result");
    };
    let _: crate::agent::AgentPath =
        serde_json::from_value(committed["task"].clone()).map_err(storage)?;
    assert!(Path::new(committed["cwd"].as_str().expect("committed workspace path")).is_absolute());
    drop(waiting);
    // Re-admission after the last waiter retires must still observe the
    // original directory. Move the actual committed bytes away before
    // arming: neither publication's initial read nor unrelated request
    // events may stand in for the original reply notification.
    let status = checkout.path().join(DIRECTORY).join("status");
    let retained_status = checkout.path().join(DIRECTORY).join("retained-status");
    std::fs::rename(&status, &retained_status).map_err(storage)?;
    std::fs::create_dir(&status).map_err(storage)?;
    let original_reply = reply_path(&retained_status, operation);
    let parked_reply = retained_status.join("parked-reply");
    std::fs::rename(&original_reply, &parked_reply).map_err(storage)?;
    let (delivery, response) = mpsc::sync_channel(1);
    let readmitted = connected.wait(operation, delivery)?;
    connected.shared.published(operation);
    assert!(matches!(
        response.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    std::fs::rename(&parked_reply, &original_reply).map_err(storage)?;
    let recovered =
        tokio::task::spawn_blocking(move || response.recv_timeout(Duration::from_secs(2)))
            .await
            .map_err(storage)?
            .map_err(storage)?;
    let Delivery::Reply(Ok(recovered)) = recovered else {
        panic!("the original reply producer must survive idle watch re-admission");
    };
    assert_eq!(recovered, committed);
    assert_eq!(std::fs::read_dir(&status).map_err(storage)?.count(), 0);
    drop(readmitted);
    std::fs::remove_dir(&status).map_err(storage)?;
    std::fs::rename(&retained_status, &status).map_err(storage)?;
    remove_projection(&recovery_path)?;
    let (delivery, response) = mpsc::sync_channel(1);
    let late = connected.wait(operation, delivery)?;
    // The admitted client must read the committed reply in its original
    // directory even when an unrelated directory takes over the same name.
    // Arm the waiter first so all subsequent changes have a real producer.
    let status = checkout.path().join(DIRECTORY).join("status");
    let retained_status = checkout.path().join(DIRECTORY).join("retained-status");
    std::fs::rename(&status, &retained_status).map_err(storage)?;
    std::fs::create_dir(&status).map_err(storage)?;
    let replacement = reply_path(&status, operation);
    std::fs::write(&replacement, b"{unrelated replacement reply").map_err(storage)?;
    connected.shared.published(operation);
    let Delivery::Reply(Ok(recovered)) = response
        .try_recv()
        .expect("committed reply is immediately available")
    else {
        panic!("withdrawal must not replace an available committed outcome");
    };
    assert_eq!(recovered, committed);
    assert_eq!(
        std::fs::read(&replacement).map_err(storage)?,
        b"{unrelated replacement reply"
    );
    std::fs::remove_file(&replacement).map_err(storage)?;
    std::fs::remove_dir(&status).map_err(storage)?;
    std::fs::rename(&retained_status, &status).map_err(storage)?;
    drop(late);
    let requests_registry = Arc::clone(&registry);
    let result = tokio::task::spawn_blocking(move || {
        // A caller can occupy its exact writable reply name before watching.
        // The publisher must isolate that conflict and the caller must finish
        // without requiring a new notification for the pre-existing bytes.
        let operation = OperationId::new();
        let directory = cwd.join(DIRECTORY);
        let path = reply_path(&directory.join("status"), operation);
        std::fs::write(&path, b"{conflicting reply").map_err(storage)?;
        let mut served = Served::default();
        served.known.insert(
            operation,
            Tracked::Running { withdrawn: false },
        );
        let held = acyclic_fs::hold_root(&directory.join("status")).map_err(native_file_error)?;
        served.answer(&held, operation, Ok(serde_json::json!({"ok": null})))?;
        assert_eq!(std::fs::read(&path).map_err(storage)?, b"{conflicting reply");
        assert!(matches!(
            request_operation(
                &requests_registry,
                &cwd,
                operation,
                HostAction::Open {
                    host: "codex".into(),
                    session: "conflicting-reply".into(),
                },
            ),
            Err(error) if error.request_operation_id() == Some(operation) && error.code() == crate::wire::ErrorCode::Indeterminate
        ));
        served.withdraw(&held, operation)?;
        // The actual shared owner must still complete an unrelated request.
        let request = |cwd: &Path, action| request_with(&requests_registry, cwd, action);
        let result = request(
            &cwd,
            HostAction::Open {
                host: "codex".into(),
                session: "fixture-native-session".into(),
            },
        )?;
        let parent: crate::agent::AgentPath =
            serde_json::from_value(result["task"].clone()).map_err(storage)?;
        let session = |action| {
            request(
                &cwd,
                HostAction::Session {
                    parent: parent.clone(),
                    reference: "endpoint-boundary".into(),
                    action,
                    next: false,
                },
            )
        };
        let spawned = session(SessionAction::Spawn {
            prompt: "fixture prompt".into(),
        })?;
        let child = PathBuf::from(spawned["cwd"].as_str().unwrap());
        assert_eq!(workspace_root(&child)?, cwd);
        assert!(Arc::ptr_eq(
            &Registry::bridge(&requests_registry, &child, |_, _| {
                panic!("another directory of the same owner must reuse its connection")
            })?,
            &connected,
        ));
        assert!(!child.join(DIRECTORY).join("owner.json").exists());
        assert_eq!(spawned["pending"], 1, "the first prompt is pending");
        // Spawning the same reference again names the same child.
        assert_eq!(
            session(SessionAction::Spawn {
                prompt: "fixture prompt".into(),
            })?["task"],
            spawned["task"]
        );
        let page = request(
            &cwd,
            HostAction::Sessions {
                parent: parent.clone(),
            },
        )?;
        assert_eq!(page["references"], serde_json::json!(["endpoint-boundary"]));
        let turn = session(SessionAction::Next)?;
        assert_eq!(turn["prompt"], "fixture prompt");
        assert_eq!(turn["task"], spawned["task"]);
        assert!(
            session(SessionAction::Next).is_err(),
            "a claimed input cannot be claimed twice"
        );
        assert!(
            request(
                &cwd,
                HostAction::Fs {
                    task: parent.clone(),
                    command: "merge agents/endpoint-boundary".into(),
                },
            )
            .is_err(),
            "a running session cannot merge"
        );
        let claim: crate::local::host_owner::TurnClaim =
            serde_json::from_value(turn["claim"].clone()).map_err(storage)?;
        let outcome = crate::Outcome::Succeeded(crate::executor::TurnOutput::new(
            "saved outcome ".repeat(3_000),
            serde_json::json!({"source": "native", "details": "\\\"\n".repeat(256)}),
            1,
        ));
        let expected = serde_json::to_value(&outcome).map_err(storage)?;
        let complete = |claim| {
            session(SessionAction::Complete {
                claim,
                outcome: outcome.clone(),
            })
        };
        let settled = complete(claim.clone())?;
        assert_eq!(settled["status"]["settled"], true);
        assert_eq!(settled["pending"], 0);
        assert_eq!(settled["results"][0]["outcome"], expected);
        let reply = session(SessionAction::Reply {
            id: serde_json::from_value(spawned["id"].clone()).map_err(storage)?,
            inbox: spawned["inbox"].as_u64().unwrap(),
        })?;
        assert_eq!(reply["reply"]["outcome"], expected);
        assert!(reply.get("results").is_none(), "a reply carries its outcome once");
        assert_eq!(session(SessionAction::Observe)?["results"][0]["outcome"], expected);
        // A replayed completion of the settled turn is idempotent.
        assert_eq!(complete(claim)?["status"]["turn"], 1);
        assert!(session(SessionAction::Next)?.is_null());
        // A completion fits Event admission even when its real file request
        // envelope and resulting session projection exceed that payload size.
        let sent = session(SessionAction::Send { prompt: "near-bound outcome".into() })?;
        let turn = session(SessionAction::Next)?;
        let outcome = crate::Outcome::Succeeded(crate::executor::TurnOutput::new(
            "x".repeat(65_336), Value::Null, 1,
        ));
        let expected = serde_json::to_value(&outcome).map_err(storage)?;
        let operation = OperationId::new();
        let action = HostAction::Session {
            parent: parent.clone(), reference: "endpoint-boundary".into(), next: false,
            action: SessionAction::Complete {
                claim: serde_json::from_value(turn["claim"].clone()).map_err(storage)?,
                outcome,
            },
        };
        assert!(serde_json::to_vec(&Request {
            session: connected.session, operation, action: action.clone(),
        }).map_err(storage)?.len() > 65_536);
        let settled = request_operation(&requests_registry, &cwd, operation, action)?;
        assert_eq!(settled["results"][0]["outcome"], expected);
        let reply = session(SessionAction::Reply {
            id: serde_json::from_value(sent["id"].clone()).map_err(storage)?,
            inbox: sent["inbox"].as_u64().unwrap(),
        })?;
        assert_eq!(reply["reply"]["outcome"], expected);
        request(&cwd, HostAction::Hook {
            host: "pi".into(), event: "NativeSession".into(),
            input: serde_json::json!({"task":parent,"native_session":{"id":"fixture","file":cwd.join("native")}}),
        })?;
        assert_eq!(request(&cwd, HostAction::Hook {
            host: "pi".into(), event: "NativeSession".into(),
            input: serde_json::json!({"task":parent}),
        })?["id"], "fixture");
        // Requests of one process wait concurrently, each its own keyed
        // waiter, sharing one watch per awaited status key.
        std::thread::scope(|scope| {
            let readers = (0..8)
                .map(|reader| {
                    let (cwd, parent) = (&cwd, &parent);
                    scope.spawn(move || {
                        let action = if reader % 2 == 0 {
                            HostAction::Sessions { parent: parent.clone() }
                        } else {
                            HostAction::Locate { cwd: cwd.clone() }
                        };
                        request(cwd, action).map(|_| ())
                    })
                })
                .collect::<Vec<_>>();
            readers.into_iter().try_for_each(|reader| {
                reader
                    .join()
                    .map_err(|_| Error::Conflict("concurrent request panicked".into()))?
                    .or_else(|error| match error {
                        // Locating the root may be refused; it must still
                        // be answered, never left waiting.
                        error if matches!(error.code(), crate::wire::ErrorCode::Conflict
                            | crate::wire::ErrorCode::Invalid | crate::wire::ErrorCode::NotFound) => Ok(()),
                        error => Err(error),
                    })
            })
        })?;
        let agents = request(&cwd, HostAction::Agents { task: parent.clone() })?;
        assert_eq!(agents["agents"][0]["reference"], "endpoint-boundary");
        assert_eq!(request(&cwd, HostAction::Open {
            host: "codex".into(), session: "fixture-native-session".into(),
        })?["task"], result["task"]);
        Ok::<_, Error>(result)
    })
    .await
    .map_err(storage)??;
    assert_eq!(
        result.get("cwd").unwrap_or(&Value::Null),
        std::fs::canonicalize(checkout.path())
            .map_err(storage)?
            .to_string_lossy()
            .as_ref()
    );
    assert!(result.get("task").is_some_and(Value::is_string));
    tokio::time::timeout(Duration::from_secs(2), async {
        while std::fs::read_dir(&requests)
            .map_err(storage)?
            .next()
            .is_some()
        {
            projection_event(&mut changes, &mut owner).await?;
        }
        Ok::<_, Error>(())
    })
    .await
    .map_err(storage)??;
    assert!(
        !owner.is_finished(),
        "invalid writable projections must not stop the owner"
    );
    #[cfg(unix)]
    assert_eq!(
        std::fs::read(outside.path()).map_err(storage)?,
        b"outside remains untouched"
    );
    assert_eq!(
        std::fs::read_dir(checkout.path().join(DIRECTORY).join("requests"))
            .map_err(storage)?
            .count(),
        0
    );
    // Every consumed operation's reply is retired, without touching debris.
    let statuses = checkout.path().join(DIRECTORY).join("status");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let mut pending = 0;
            for entry in std::fs::read_dir(&statuses).map_err(storage)? {
                if entry.map_err(storage)?.path() != debris {
                    pending += 1;
                }
            }
            if pending == 0 {
                return Ok::<_, Error>(());
            }
            projection_event(&mut changes, &mut owner).await?;
        }
    })
    .await
    .map_err(storage)??;
    assert_eq!(
        std::fs::read(&debris).map_err(storage)?,
        b"unrelated status is not a reply",
        "publishing or retiring one reply must never prune unrelated leaves"
    );
    // Every registration retired, and with the last the directory watches.
    let cwd = std::fs::canonicalize(checkout.path()).map_err(storage)?;
    let bridge = Registry::live(&registry, &cwd)
        .ok_or_else(|| Error::Conflict("client bridge absent".into()))?;
    assert!(lock(&bridge.shared.waiters).waiting.is_empty());
    assert!(
        lock(&bridge.watching).watches.is_none(),
        "a connection nothing awaits must not stay watched"
    );
    // The real connection has completed ordinary requests. Lose its
    // notification producer while two registered replies remain outstanding:
    // both must finish without publishing a reply or waiting for owner death.
    let (first, first_reply) = mpsc::sync_channel(1);
    let first = bridge.wait(OperationId::new(), first)?;
    let (second, second_reply) = mpsc::sync_channel(1);
    let second = bridge.wait(OperationId::new(), second)?;
    assert!(matches!(
        first_reply.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    bridge
        .shared
        .note(Err(notify::Error::io(std::io::Error::from(
            std::io::ErrorKind::BrokenPipe,
        ))));
    let first_failure = match first_reply
        .try_recv()
        .expect("watch failure settles the first reply")
    {
        Delivery::Reply(Err(error)) => error,
        _ => panic!("watch failure must retain its storage cause"),
    };
    let failure = lock(&bridge.shared.waiters)
        .failure
        .clone()
        .expect("the first notification cause remains available");
    assert_eq!(
        request_storage_cause(&first_failure, first.operation),
        failure
    );
    assert!(first_failure.uncertain());
    assert!(failure.contains("step=observe native workspace files:"));
    assert!(failure.ends_with("recovery=acyclic agents"));
    assert!(matches!(
        second_reply.try_recv(),
        Ok(Delivery::Reply(Err(ref error)))
            if request_storage_cause(error, second.operation) == failure
    ));
    drop((first, second));
    bridge
        .shared
        .note(Err(notify::Error::io(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied,
        ))));
    assert!(
        Arc::ptr_eq(&Registry::bridge(&registry, &cwd, Bridge::open)?, &bridge),
        "a failed live connection must not reopen its notification source"
    );
    let refused_operation = OperationId::new();
    assert!(
        matches!(request_operation(&registry, &cwd, refused_operation, HostAction::Agents {
        task: serde_json::from_value(result["task"].clone()).map_err(storage)?,
    }), Err(ref error) if request_storage_cause(error, refused_operation) == failure)
    );
    assert_eq!(
        std::fs::read_dir(&requests).map_err(storage)?.count(),
        0,
        "a refused connection must not publish a request with no response producer"
    );
    assert!(lock(&bridge.shared.waiters).waiting.is_empty());
    assert!(lock(&bridge.watching).watches.is_none());
    assert!(
        !owner.is_finished(),
        "client failure must not kill the workspace owner"
    );
    // The owner is still alive: failure of its native exit observation must
    // retain that cause rather than falsely declaring the owner gone.
    let (directory, endpoint) = discover(&cwd)?;
    let observed = Arc::new(Bridge::open(directory, endpoint)?);
    let (delivery, response) = mpsc::sync_channel(1);
    let waiting = observed.wait(OperationId::new(), delivery)?;
    observed
        .shared
        .owner_finished(Err(std::io::ErrorKind::BrokenPipe.into()));
    let first = match response.try_recv().expect("owner observer must settle") {
        Delivery::Reply(Err(error)) => error,
        _ => panic!("owner observation failure must preserve its native cause"),
    };
    let failure = lock(&observed.shared.waiters)
        .failure
        .clone()
        .expect("owner observation failure remains available");
    assert_eq!(request_storage_cause(&first, waiting.operation), failure);
    assert!(failure.contains("step=observe native workspace owner exit:"));
    assert!(failure.ends_with("recovery=graphcoder --host-workspace"));
    assert!(!lock(&observed.shared.waiters).gone);
    drop(waiting);
    observed
        .shared
        .owner_finished(Err(std::io::ErrorKind::PermissionDenied.into()));
    let (late, _) = mpsc::sync_channel(1);
    assert!(matches!(
        observed.wait(OperationId::new(), late),
        Err(Error::Storage(error)) if error == failure
    ));
    assert!(lock(&observed.shared.waiters).waiting.is_empty());
    assert!(lock(&observed.watching).watches.is_none());
    let (producer, mut ended) = tokio::sync::mpsc::unbounded_channel();
    drop(producer);
    assert!(matches!(
        projection_event(&mut ended, &mut owner).await,
        Err(Error::Storage(ref error)) if error.contains("notification producer closed")
    ));
    assert!(local.agent.is_none());
    assert!(local.tools().definitions(None)?.is_empty());
    // Retirement requires the caller to release its harness handle before
    // the original owner joins physical view cleanup and disconnects it.
    drop(local);
    // A rejected projection that cannot be unlinked has no withdrawal
    // producer. Its error must retire the original owner and end the call.
    let (directory, endpoint) = discover(&cwd)?;
    let blocked = Arc::new(Bridge::open(directory, endpoint)?);
    let operation = OperationId::new();
    let (delivery, response) = mpsc::sync_channel(1);
    let waiting = blocked.wait(operation, delivery)?;
    // A directory at a canonical request leaf is rejected by the no-follow
    // reader and cannot be unlinked as a file on any supported platform.
    let rejected = requests.join(format!("{operation}.json"));
    std::fs::create_dir(&rejected).map_err(storage)?;
    blocked.shared.published(operation);
    let failure = tokio::time::timeout(Duration::from_secs(2), &mut owner)
        .await
        .map_err(storage)?
        .map_err(storage)?
        .unwrap_err();
    let Error::Cleanup(causes) = failure else {
        panic!("rejected request must retain admission and withdrawal causes: {failure}");
    };
    // Compare both original native failures, without pinning diagnostic wording.
    // Neither operation can remove this directory, including during retirement.
    let admission = regular_bytes(&rejected, MAXIMUM_REQUEST_BYTES)
        .expect_err("a directory cannot be admitted as a request");
    let withdrawal =
        remove_projection(&rejected).expect_err("file withdrawal cannot remove a directory");
    assert_eq!(causes.primary().to_string(), admission.to_string());
    assert_eq!(causes.cleanup().to_string(), withdrawal.to_string());
    let Delivery::Reply(Err(observed)) = response
        .try_recv()
        .expect("the rejected reply must settle its original waiter")
    else {
        panic!("the rejected reply must retain its admission cause");
    };
    assert_eq!(observed.request_operation_id(), Some(operation));
    let Error::Command(failure) = observed else {
        panic!("the rejected reply must retain its typed request identity");
    };
    assert_eq!(
        failure
            .original()
            .expect("original admission cause")
            .to_string(),
        admission.to_string()
    );
    // Client observation already retained the original admission cause. Join
    // the actual owner-lock producer before asserting its separate retirement.
    lock(&blocked.observer)
        .take()
        .expect("original owner-lock observer")
        .join()
        .expect("original owner-lock producer retires");
    drop(waiting);
    assert!(lock(&blocked.shared.waiters).gone);
    let (delivery, _) = mpsc::sync_channel(1);
    assert!(matches!(
        blocked.wait(operation, delivery),
        Err(Error::NotFound(_))
    ));
    Ok(())
}

#[test]
fn absent_owner_never_creates_a_service_or_state_directory() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(storage)?;
    assert!(matches!(
        request(
            checkout.path(),
            HostAction::Open {
                host: "codex".into(),
                session: "fixture".into()
            }
        ),
        Err(error) if error.code() == crate::wire::ErrorCode::NotFound
    ));
    assert_eq!(
        std::fs::read_dir(checkout.path()).map_err(storage)?.count(),
        0
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn endpoint_projection_cannot_follow_a_workspace_symlink() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(storage)?;
    let outside = tempfile::tempdir().map_err(storage)?;
    std::os::unix::fs::symlink(outside.path(), checkout.path().join(".acyclic-sdk"))
        .map_err(storage)?;
    assert!(discover(checkout.path()).is_err());
    assert_eq!(
        std::fs::read_dir(outside.path()).map_err(storage)?.count(),
        0
    );
    Ok(())
}

#[test]
fn notifications_name_requests_and_only_overflow_rescans() -> Result<()> {
    use notify::event::{AccessKind, CreateKind, Flag, RemoveKind};
    let root = tempfile::tempdir().map_err(storage)?;
    let requests = std::fs::canonicalize(root.path()).map_err(storage)?;
    let notices = Notices::new(requests.clone());
    let held = acyclic_fs::hold_root(&requests).map_err(native_file_error)?;
    // Requests published before the owner ran are found by one startup scan.
    assert!(notices.take().rescan);
    let named = |kind, leaf: &str| Ok(notify::Event::new(kind).add_path(requests.join(leaf)));
    notices.note(&named(
        notify::EventKind::Create(CreateKind::File),
        "a.json",
    ));
    notices.note(&named(
        notify::EventKind::Remove(RemoveKind::File),
        "b.json",
    ));
    notices.note(&named(
        notify::EventKind::Access(AccessKind::Read),
        "c.json",
    ));
    let touched = notices.take();
    assert!(!touched.rescan, "named notifications never rescan");
    assert_eq!(
        touched.names,
        BTreeSet::from(["a.json".into(), "b.json".into()])
    );
    // Negative controls: an overflow or a foreign path cannot name
    // every changed request, so each forces exactly one rescan.
    for event in [
        Ok(notify::Event::new(notify::EventKind::Other).set_flag(Flag::Rescan)),
        Ok(notify::Event::new(notify::EventKind::Any).add_path(root.path().join("..").join("x"))),
        Ok(notify::Event::new(notify::EventKind::Any)),
    ] {
        notices.note(&event);
        assert!(notices.take().rescan);
    }
    // A named pass touches only its names; only a rescan lists the directory.
    let mut served = Served::default();
    let session = uuid::Uuid::new_v4();
    served.touch(
        &held,
        &held,
        session,
        Touched {
            names: BTreeSet::from(["absent.json".into()]),
            rescan: false,
            failure: None,
        },
    )?;
    assert_eq!(served.rescans, 0);
    served.touch(
        &held,
        &held,
        session,
        Touched {
            names: BTreeSet::new(),
            rescan: true,
            failure: None,
        },
    )?;
    assert_eq!(served.rescans, 1);
    // Notification errors mean the producer is unavailable, unlike a
    // successful overflow event. Do not scan or dispatch after that failure.
    notices.note(&Err(notify::Error::io(std::io::Error::from(
        std::io::ErrorKind::BrokenPipe,
    ))));
    let failure = notices.take();
    assert!(!failure.rescan);
    let error = served
        .touch(&held, &held, session, failure)
        .unwrap_err()
        .to_string();
    assert!(error.contains("step=observe native workspace files:"));
    notices.note(&named(notify::EventKind::Any, "later.json"));
    let later = notices.take();
    assert!(later.names.is_empty());
    assert_eq!(
        served
            .touch(&held, &held, session, later)
            .unwrap_err()
            .to_string(),
        error
    );
    assert_eq!(
        served.rescans, 1,
        "producer failure must not trigger another scan"
    );
    Ok(())
}
