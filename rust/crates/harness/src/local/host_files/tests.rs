use super::*;

#[tokio::test]
async fn codex_interrupted_claim_replies_uncertain_without_reexecution() -> Result<()> {
    interrupted_claim_replies_uncertain("codex").await
}
#[tokio::test]
async fn claude_interrupted_claim_replies_uncertain_without_reexecution() -> Result<()> {
    interrupted_claim_replies_uncertain("claude-code").await
}
#[tokio::test]
async fn pi_interrupted_claim_replies_uncertain_without_reexecution() -> Result<()> {
    interrupted_claim_replies_uncertain("pi").await
}

async fn interrupted_claim_replies_uncertain(host: &str) -> Result<()> {
    let checkout = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let local = Arc::new(
        LocalHarness::open(
            LocalHarnessConfig::native_host(checkout.path())
                .state_directory(state.path().join("owner")),
        )
        .await?,
    );
    let mut owner = HostWorkspaceOwner::new(Arc::clone(&local))?;
    let root = owner
        .dispatch(
            OperationId::new(),
            HostAction::Open {
                host: host.into(),
                session: "interrupted-command".into(),
            },
        )
        .await?;
    let parent = serde_json::from_value(root["task"].clone()).map_err(storage)?;
    let session = uuid::Uuid::new_v4();
    let operation = OperationId::new();
    let action = HostAction::Fork {
        parent,
        reference: "once".into(),
    };
    interrupt_claim(&local, &mut owner, session, operation, action.clone()).await?;
    drop(owner);
    let directory = checkout.path().join(DIRECTORY);
    std::fs::create_dir_all(directory.join("results")).map_err(storage)?;
    for _ in 0..2 {
        assert_uncertain_restart(
            &local,
            &directory,
            Request {
                session,
                operation,
                action: action.clone(),
            },
            parent,
        )
        .await?;
    }
    Arc::try_unwrap(local)
        .map_err(|_| Error::Conflict("interrupted fixture retained owner".into()))?
        .disconnect()
        .await?;
    Ok(())
}

async fn assert_uncertain_restart(
    local: &Arc<LocalHarness>,
    directory: &Path,
    request: Request,
    parent: TaskId,
) -> Result<()> {
    let Request {
        session,
        operation,
        action,
    } = request;
    let session_component = Component::new(session.to_string()).map_err(storage)?;
    let component = Component::new(operation.to_string()).map_err(storage)?;
    let mut owner = HostWorkspaceOwner::new(Arc::clone(local))?;
    let mut commands = CommandSession::open_local(local, session, MAXIMUM_BYTES, 4096).await?;
    let before = local.task_files.inner.head().await.map_err(storage)?;
    process_request(
        local,
        &mut owner,
        &mut commands,
        &session_component,
        directory,
        Request {
            session,
            operation,
            action: action.clone(),
        },
    )
    .await?;
    let bytes = regular_bytes(&directory.join("results").join(format!("{operation}.json")))?;
    let uncertain: Value = serde_json::from_slice(&bytes).map_err(storage)?;
    assert_eq!(uncertain["completion"], "indeterminate", "{uncertain}");
    assert_eq!(
        before,
        local.task_files.inner.head().await.map_err(storage)?,
        "replay dispatched the already-claimed effect"
    );
    let durable = local
        .task_files
        .snapshot(
            &Query::File(FileKey::Session {
                session: session_component.clone(),
                file: TaskFile::ExecutionResult(component.clone()),
            }),
            1,
        )
        .await
        .map_err(storage)?;
    assert!(
        durable.files.is_empty(),
        "an uncertain projection settled the canonical claim"
    );
    remove_projection(&directory.join("results").join(format!("{operation}.json")))?;
    let next = OperationId::new();
    process_request(
        local,
        &mut owner,
        &mut commands,
        &session_component,
        directory,
        Request {
            session,
            operation: next,
            action: HostAction::Sessions {
                parent,
                after: None,
            },
        },
    )
    .await?;
    let next: Value = serde_json::from_slice(&regular_bytes(
        &directory.join("results").join(format!("{next}.json")),
    )?)
    .map_err(storage)?;
    assert_eq!(
        next["ok"],
        serde_json::json!(["once"]),
        "owner stopped serving"
    );
    Ok(())
}

async fn interrupt_claim(
    local: &LocalHarness,
    owner: &mut HostWorkspaceOwner,
    session: uuid::Uuid,
    operation: OperationId,
    action: HostAction,
) -> Result<()> {
    let file = FileValue::new(
        FileKey::Session {
            session: Component::new(session.to_string()).map_err(storage)?,
            file: TaskFile::Request(Component::new(operation.to_string()).map_err(storage)?),
        },
        serde_json::to_vec(&CommandResolution::Sdk {
            command: "workspace".into(),
            arguments: serde_json::to_string(&action).map_err(storage)?,
        })
        .map_err(storage)?,
    );
    local
        .task_files
        .inner
        .append(local.task_files.inner.head().await.map_err(storage)?, &file)
        .await
        .map_err(storage)?;
    let mut commands = CommandSession::open_local(local, session, MAXIMUM_BYTES, 4096).await?;
    let (reached, admitted) = tokio::sync::oneshot::channel();
    let consuming = commands.consume(&file, |operation, _, _| async move {
        let effect = owner.dispatch(operation, action).await?;
        reached.send(effect).map_err(storage)?;
        std::future::pending::<Result<Value>>().await
    });
    tokio::pin!(consuming);
    tokio::select! {
        result = &mut consuming => panic!("interrupted effect unexpectedly settled: {result:?}"),
        effect = admitted => { effect.map_err(storage)?; }
    }
    // Drop the actual consumer after the claim and effect, before its durable
    // result. Reopening must never grant this effect again.
    Ok(())
}

#[tokio::test]
async fn every_host_observes_live_files_without_journaling_queries() -> Result<()> {
    for host in ["codex", "claude-code", "pi"] {
        let checkout = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        let local = Arc::new(
            LocalHarness::open(
                LocalHarnessConfig::native_host(checkout.path())
                    .state_directory(state.path().join("owner")),
            )
            .await?,
        );
        let mut owner = HostWorkspaceOwner::new(Arc::clone(&local))?;
        let root = owner
            .dispatch(
                OperationId::new(),
                HostAction::Open {
                    host: host.into(),
                    session: "live-query-root".into(),
                },
            )
            .await?;
        let parent = serde_json::from_value(root["task"].clone()).map_err(storage)?;
        owner
            .dispatch(
                OperationId::new(),
                HostAction::Session {
                    parent,
                    reference: "worker".into(),
                    action: super::super::host_owner::SessionAction::Spawn {
                        prompt: "input".into(),
                    },
                },
            )
            .await?;
        let session = uuid::Uuid::new_v4();
        let mut commands = CommandSession::open_local(&local, session, MAXIMUM_BYTES, 4096).await?;
        let directory = checkout.path().join(DIRECTORY);
        std::fs::create_dir_all(directory.join("results")).map_err(storage)?;
        let before = local.task_files.inner.head().await.map_err(storage)?;
        for action in [
            HostAction::Session {
                parent,
                reference: "worker".into(),
                action: super::super::host_owner::SessionAction::Observe,
            },
            HostAction::Sessions {
                parent,
                after: None,
            },
            HostAction::Locate {
                cwd: PathBuf::from(root["cwd"].as_str().unwrap()),
            },
            HostAction::Query {
                task: parent,
                query: Query::File(FileKey::Task {
                    task: Component::new(parent.to_string()).map_err(storage)?,
                    file: TaskFile::Context(Component::new("native-session").map_err(storage)?),
                }),
            },
        ] {
            let operation = OperationId::new();
            process_request(
                &local,
                &mut owner,
                &mut commands,
                &Component::new(session.to_string()).map_err(storage)?,
                &directory,
                Request {
                    session,
                    operation,
                    action,
                },
            )
            .await?;
            let result =
                regular_bytes(&directory.join("results").join(format!("{operation}.json")))?;
            let result: Value = serde_json::from_slice(&result).map_err(storage)?;
            assert!(result.get("ok").is_some(), "{host}: {result}");
            assert_eq!(
                before,
                local.task_files.inner.head().await.map_err(storage)?,
                "{host}: polling published a durable command"
            );
        }
        drop(commands);
        drop(owner);
        Arc::try_unwrap(local)
            .map_err(|_| Error::Conflict("query fixture retained the workspace owner".into()))?
            .disconnect()
            .await?;
    }
    Ok(())
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
    let local = Arc::new(
        LocalHarness::open(
            LocalHarnessConfig::native_host(checkout.path())
                .state_directory(state.path().join("owner")),
        )
        .await?,
    );
    let owner = tokio::spawn(Box::pin(serve(Arc::clone(&local))));
    let descriptor = checkout.path().join(DIRECTORY).join("owner.json");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !descriptor.is_file() {
            assert!(
                !owner.is_finished(),
                "workspace owner stopped before readiness"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
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
        let child = request(
            &cwd,
            HostAction::Fork {
                parent: serde_json::from_value(result["task"].clone()).map_err(storage)?,
                reference: "endpoint-boundary".into(),
            },
        )?;
        let child = PathBuf::from(child["cwd"].as_str().unwrap());
        assert_eq!(workspace_root(&child)?, cwd);
        assert!(!child.join(DIRECTORY).join("owner.json").exists());
        let task = serde_json::from_value(result["task"].clone()).map_err(storage)?;
        // The reference fits a request, but the byte-array query encoding does
        // not fit a response. The shared command consumer settles it without
        // stranding the claim or terminating the workspace owner.
        request(&cwd, HostAction::Hook {
            host: "pi".into(), event: "NativeSession".into(),
            input: serde_json::json!({"task":task,"native_session":{"id":"fixture","file":cwd.join("x".repeat(20_000))}}),
        })?;
        assert!(request(&cwd, HostAction::Query {
            task, query: Query::File(FileKey::Task {
                task: Component::new(task.to_string()).map_err(storage)?,
                file: TaskFile::Context(Component::new("native-session").map_err(storage)?),
            }),
        }).is_err(), "oversized live queries must settle as omitted output");
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
            assert!(!owner.is_finished(), "invalid request stopped the owner");
            tokio::task::yield_now().await;
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
    assert_eq!(
        std::fs::read_dir(checkout.path().join(DIRECTORY).join("results"))
            .map_err(storage)?
            .count(),
        0
    );
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
