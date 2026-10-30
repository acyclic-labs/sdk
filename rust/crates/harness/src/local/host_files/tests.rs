use super::*;
use crate::local::host_owner::SessionAction;
use std::time::Duration;

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
    let mut owner = tokio::spawn(Box::pin(serve(Arc::clone(&local))));
    let descriptor = checkout.path().join(DIRECTORY).join("owner.json");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !descriptor.is_file() {
            projection_event(&mut changes, &mut owner).await?;
        }
        Ok::<_, Error>(())
    })
    .await
    .map_err(storage)??;
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
    std::fs::write(requests.join("malformed.json"), b"{not a request").map_err(storage)?;
    std::fs::write(
        requests.join("oversized.json"),
        vec![b'x'; usize::try_from(MAXIMUM_BYTES).map_err(storage)? + 1],
    )
    .map_err(storage)?;
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
    let result = tokio::task::spawn_blocking(move || {
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
    // Every read reply is dropped from its agent's latest status.
    let statuses = checkout.path().join(DIRECTORY).join("status");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let mut pending = 0;
            for entry in std::fs::read_dir(&statuses).map_err(storage)? {
                if let Some((_, path)) = latest(&entry.map_err(storage)?.path())? {
                    // A status superseded while read is counted as pending.
                    pending += std::fs::read(path).ok().map_or(1, |bytes| {
                        serde_json::from_slice::<Status>(&bytes)
                            .map_or(1, |status| status.replies.len())
                    });
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
    // Every registration retired, and with the last of each key its watch.
    let directory = std::fs::canonicalize(checkout.path())
        .map_err(storage)?
        .join(DIRECTORY);
    let bridge = lock(&REGISTRY)
        .bridges
        .get(&directory)
        .cloned()
        .ok_or_else(|| Error::Conflict("client bridge absent".into()))?;
    assert!(lock(&bridge.shared.waiters).keys.is_empty());
    assert!(
        lock(&bridge.watching).watched.is_empty(),
        "a key nothing awaits must not stay watched"
    );
    // The real connection has completed ordinary requests. Lose its
    // notification producer while two registered replies remain outstanding:
    // both must finish without publishing a reply or waiting for owner death.
    let (first, first_reply) = mpsc::sync_channel(1);
    let first = bridge.wait(WORKSPACE, OperationId::new(), first)?;
    let (second, second_reply) = mpsc::sync_channel(1);
    let second = bridge.wait(WORKSPACE, OperationId::new(), second)?;
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
    let cwd = std::fs::canonicalize(checkout.path()).map_err(storage)?;
    assert!(
        Arc::ptr_eq(&lock(&REGISTRY).bridge(&cwd)?, &bridge),
        "a failed live connection must not reopen its notification source"
    );
    assert!(matches!(request(&cwd, HostAction::Agents {
        task: serde_json::from_value(result["task"].clone()).map_err(storage)?,
    }), Err(Error::Storage(ref error)) if error == &failure));
    assert_eq!(
        std::fs::read_dir(&requests).map_err(storage)?.count(),
        0,
        "a refused connection must not publish a request with no response producer"
    );
    assert!(lock(&bridge.shared.waiters).keys.is_empty());
    assert!(lock(&bridge.watching).watched.is_empty());
    assert!(
        !owner.is_finished(),
        "client failure must not kill the workspace owner"
    );
    let (producer, mut ended) = tokio::sync::mpsc::unbounded_channel();
    drop(producer);
    assert!(matches!(
        projection_event(&mut ended, &mut owner).await,
        Err(Error::Storage(ref error)) if error.contains("notification producer closed")
    ));
    assert!(local.agent.is_none());
    assert!(local.tools().definitions(None)?.is_empty());
    owner.abort();
    assert!(owner.await.unwrap_err().is_cancelled());
    Arc::try_unwrap(local)
        .map_err(|_| Error::Conflict("workspace owner retained after shutdown".into()))?
        .disconnect()
        .await?;
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
