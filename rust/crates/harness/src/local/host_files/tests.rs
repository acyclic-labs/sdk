use super::*;
use crate::local::host_owner::SessionAction;

fn rejected_requests_end_without_owner_exit() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(storage)?;
    let workspace = std::fs::canonicalize(checkout.path()).map_err(storage)?;
    let directory = workspace.join(DIRECTORY);
    let requests = directory.join("requests");
    std::fs::create_dir_all(&requests).map_err(storage)?;
    std::fs::create_dir(directory.join("status")).map_err(storage)?;
    let root = acyclic_fs::hold_root(&directory).map_err(native_file_error)?;
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
    let (_, endpoint) = discover(&endpoint.workspace)?;
    let bridge = Arc::new(Bridge::open(&directory, endpoint)?);
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
            &requests,
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
        assert!(!lock(&bridge.watching).watched);
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
    let registry = Mutex::<Registry>::default();
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
            served.observe(&requests, uuid::Uuid::new_v4(), &leaf)?;
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
    assert!(matches!(result, Err(Error::Indeterminate(id)) if id == operation));
    Ok(())
}

async fn projection_event(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
    owner: &mut tokio::task::JoinHandle<Result<()>>,
) -> Result<()> {
    tokio::select! {
        event = events.recv() => match event {
            Some(Ok(_)) => Ok(()),
            Some(Err(error)) => Err(projection_failure(error)),
            None => Err(projection_failure("notification producer closed")),
        },
        result = owner => Err(projection_failure(format!("workspace owner ended: {result:?}"))),
    }
}

fn projection_failure(cause: impl std::fmt::Display) -> Error {
    Error::Storage(format!(
        "step=observe workspace conformance: {cause}; recovery=cargo test --locked -p acyclic-harness --no-default-features --features filesystem-local,native-execution --lib local::host_files::tests::workspace_projection_uses_shared_results_without_client_state -- --exact"
    ))
}

#[test]
fn projection_reads_cannot_wake_either_endpoint() {
    use notify::event::{AccessKind, AccessMode, CreateKind, ModifyKind, RemoveKind};
    for kind in [
        AccessKind::Any,
        AccessKind::Read,
        AccessKind::Open(AccessMode::Any),
        AccessKind::Open(AccessMode::Read),
        AccessKind::Open(AccessMode::Write),
        AccessKind::Open(AccessMode::Execute),
        AccessKind::Close(AccessMode::Any),
        AccessKind::Close(AccessMode::Read),
        AccessKind::Close(AccessMode::Write),
        AccessKind::Close(AccessMode::Execute),
        AccessKind::Other,
    ] {
        assert!(
            !projection_changed(&Ok(notify::Event::new(notify::EventKind::Access(kind)))),
            "access-only notifications must not schedule another projection scan"
        );
    }
    for kind in [
        notify::EventKind::Any,
        notify::EventKind::Create(CreateKind::Any),
        notify::EventKind::Modify(ModifyKind::Any),
        notify::EventKind::Remove(RemoveKind::Any),
        notify::EventKind::Other,
    ] {
        assert!(projection_changed(&Ok(notify::Event::new(kind))));
    }
    assert!(projection_changed(&Err(notify::Error::generic(
        "watch failed"
    ))));
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
    watcher
        .watch(checkout.path(), RecursiveMode::Recursive)
        .map_err(storage)?;
    let local = Arc::new(
        LocalHarness::open(
            LocalHarnessConfig::native_host(checkout.path())
                .state_directory(state.path().join("owner")),
        )
        .await?,
    );
    let mut serving = Box::pin(serve(Arc::clone(&local)));
    // Endpoint publication and request-watch registration precede its first
    // suspension, just as in the native child-process fixture.
    assert!(futures::poll!(serving.as_mut()).is_pending());
    let mut owner = tokio::spawn(serving);
    let descriptor = checkout.path().join(DIRECTORY).join("owner.json");
    assert!(descriptor.is_file());
    // Follow only coordination files once published, before any native view
    // is admitted. The observer never traverses mounted agent filesystems.
    watcher.unwatch(checkout.path()).map_err(storage)?;
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
    watcher
        .watch(other_checkout.path(), RecursiveMode::Recursive)
        .map_err(storage)?;
    let other_local = Arc::new(
        LocalHarness::open(
            LocalHarnessConfig::native_host(other_checkout.path())
                .state_directory(state.path().join("other-owner")),
        )
        .await?,
    );
    let mut other_owner = tokio::spawn(Box::pin(serve(Arc::clone(&other_local))));
    let other_descriptor = other_checkout.path().join(DIRECTORY).join("owner.json");
    projection_settled(&mut changes, &mut other_owner, || {
        Ok(other_descriptor.is_file())
    })
    .await?;
    watcher.unwatch(other_checkout.path()).map_err(storage)?;
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
    assert!(committed.get("ok").is_some());
    remove_projection(&recovery_path)?;
    drop(waiting);
    let (delivery, response) = mpsc::sync_channel(1);
    let late = connected.wait(operation, delivery)?;
    connected.shared.published(operation);
    let Delivery::Reply(Ok(recovered)) = response
        .try_recv()
        .expect("committed reply is immediately available")
    else {
        panic!("withdrawal must not replace an available committed outcome");
    };
    assert_eq!(recovered, committed);
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
        let held = acyclic_fs::hold_root(&directory).map_err(native_file_error)?;
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
            Err(Error::Storage(_))
        ));
        served.withdraw(&directory.join("requests"), operation)?;
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
        let complete = |claim| {
            session(SessionAction::Complete {
                claim,
                outcome: crate::Outcome::Succeeded(crate::executor::TurnOutput {
                    text: "done".into(),
                    metadata: Value::Null,
                    steps: 1,
                    attachments: Vec::new(),
                }),
            })
        };
        let settled = complete(claim.clone())?;
        assert_eq!(settled["status"]["settled"], true);
        assert_eq!(settled["pending"], 0);
        assert!(settled["results"][0].to_string().contains("done"));
        // A replayed completion of the settled turn is idempotent.
        assert_eq!(complete(claim)?["status"]["turn"], 1);
        assert!(session(SessionAction::Next)?.is_null());
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
                        Error::Conflict(_) | Error::Invalid(_) | Error::NotFound(_) => Ok(()),
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
    projection_settled(&mut changes, &mut owner, || {
        Ok(std::fs::read_dir(&requests)
            .map_err(storage)?
            .next()
            .is_none())
    })
    .await?;
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
    // The original watcher has followed every status since before the
    // requests were submitted; early withdrawals cannot lose their wake.
    projection_settled(&mut changes, &mut owner, || {
        for entry in std::fs::read_dir(&statuses).map_err(storage)? {
            if entry.map_err(storage)?.path() != debris {
                return Ok(false);
            }
        }
        Ok(true)
    })
    .await?;
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
        !lock(&bridge.watching).watched,
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
        Delivery::Reply(Err(Error::Storage(failure))) => failure,
        _ => panic!("watch failure must retain its storage cause"),
    };
    let failure = lock(&bridge.shared.waiters)
        .failure
        .clone()
        .expect("the first notification cause remains available");
    assert_eq!(
        first_failure,
        format!("operation={}; {failure}", first.operation)
    );
    assert!(Error::Storage(first_failure).uncertain());
    assert!(failure.contains("step=observe native workspace files:"));
    assert!(failure.ends_with("recovery=acyclic agents"));
    assert!(matches!(
        second_reply.try_recv(),
        Ok(Delivery::Reply(Err(Error::Storage(ref error))))
            if error == &format!("operation={}; {failure}", second.operation)
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
    assert!(matches!(request_with(&registry, &cwd, HostAction::Agents {
        task: serde_json::from_value(result["task"].clone()).map_err(storage)?,
    }), Err(Error::Storage(ref error)) if error == &failure));
    assert_eq!(
        std::fs::read_dir(&requests).map_err(storage)?.count(),
        0,
        "a refused connection must not publish a request with no response producer"
    );
    assert!(lock(&bridge.shared.waiters).waiting.is_empty());
    assert!(!lock(&bridge.watching).watched);
    assert!(
        futures::poll!(&mut owner).is_pending(),
        "client failure must not kill the workspace owner"
    );
    // The owner is still alive: failure of its native exit observation must
    // retain that cause rather than falsely declaring the owner gone.
    let (directory, endpoint) = discover(&cwd)?;
    let observed = Arc::new(Bridge::open(&directory, endpoint)?);
    let (delivery, response) = mpsc::sync_channel(1);
    let waiting = observed.wait(OperationId::new(), delivery)?;
    observed
        .shared
        .owner_finished(Err(std::io::ErrorKind::BrokenPipe.into()));
    let first = match response.try_recv().expect("owner observer must settle") {
        Delivery::Reply(Err(Error::Storage(error))) => error,
        _ => panic!("owner observation failure must preserve its native cause"),
    };
    let failure = lock(&observed.shared.waiters)
        .failure
        .clone()
        .expect("owner observation failure remains available");
    assert_eq!(first, format!("operation={}; {failure}", waiting.operation));
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
    assert!(!lock(&observed.watching).watched);
    let (producer, mut ended) = tokio::sync::mpsc::unbounded_channel();
    drop(producer);
    assert!(matches!(
        projection_settled(&mut ended, &mut owner, || Ok(false)).await,
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
    let blocked = Arc::new(Bridge::open(&directory, endpoint)?);
    let operation = OperationId::new();
    let (delivery, response) = mpsc::sync_channel(1);
    let waiting = blocked.wait(operation, delivery)?;
    // A directory at a canonical request leaf is rejected by the no-follow
    // reader and cannot be unlinked as a file on any supported platform.
    std::fs::create_dir(requests.join(format!("{operation}.json"))).map_err(storage)?;
    blocked.shared.published(operation);
    let failure = tokio::time::timeout(Duration::from_secs(2), &mut owner)
        .await
        .map_err(storage)?
        .map_err(storage)?
        .unwrap_err()
        .to_string();
    assert!(
        failure.contains(&format!(
            "step=withdraw rejected native request {operation}:"
        )),
        "the original rejection cause must survive retirement: {failure}"
    );
    assert!(failure.ends_with("recovery=graphcoder --host-workspace"));
    assert!(matches!(
        response
            .recv_timeout(Duration::from_secs(2))
            .map_err(storage)?,
        Delivery::Gone
    ));
    drop(waiting);
    assert!(lock(&blocked.shared.waiters).gone);
    let (delivery, _) = mpsc::sync_channel(1);
    assert!(matches!(
        blocked.wait(operation, delivery),
        Err(Error::NotFound(_))
    ));
    Ok(())
}

/// Reuses the original watcher armed before publication and observes its
/// producer and workspace-owner terminal events while waiting for settlement.
async fn projection_settled(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
    owner: &mut tokio::task::JoinHandle<Result<()>>,
    mut settled: impl FnMut() -> Result<bool>,
) -> Result<()> {
    loop {
        if let std::task::Poll::Ready(result) = futures::poll!(&mut *owner) {
            return Err(projection_failure(format!(
                "workspace owner ended: {result:?}"
            )));
        }
        if settled()? {
            return Ok(());
        }
        projection_event(events, owner).await?;
    }
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
        Err(Error::NotFound(_))
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
        &requests,
        session,
        Touched {
            names: BTreeSet::from(["absent.json".into()]),
            rescan: false,
            failure: None,
        },
    )?;
    assert_eq!(served.rescans, 0);
    served.touch(
        &requests,
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
        .touch(&requests, session, failure)
        .unwrap_err()
        .to_string();
    assert!(error.contains("step=observe native workspace files:"));
    notices.note(&named(notify::EventKind::Any, "later.json"));
    let later = notices.take();
    assert!(later.names.is_empty());
    assert_eq!(
        served
            .touch(&requests, session, later)
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
