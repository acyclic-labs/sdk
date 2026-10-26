use super::*;
use crate::local::host_owner::SessionAction;

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
        let parent: crate::agent::AgentPath =
            serde_json::from_value(result["task"].clone()).map_err(storage)?;
        let session = |action| {
            request(
                &cwd,
                HostAction::Session {
                    parent: parent.clone(),
                    reference: "endpoint-boundary".into(),
                    action,
                },
            )
        };
        let spawned = session(SessionAction::Spawn {
            prompt: "fixture prompt".into(),
        })?;
        let child = PathBuf::from(spawned["cwd"].as_str().unwrap());
        assert_eq!(workspace_root(&child)?, cwd);
        assert!(!child.join(DIRECTORY).join("owner.json").exists());
        assert_eq!(spawned["pending"], 1, "the brief is the first input");
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
                after: None,
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
