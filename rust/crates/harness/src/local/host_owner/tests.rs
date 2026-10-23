use super::*;

#[tokio::test]
async fn pi_native_cwd_preserves_tool_inputs_and_rejects_a_wrong_view() -> Result<()> {
    let mut fixture = Fixture::open().await?;
    let root = fixture
        .owner
        .dispatch(
            OperationId::new(),
            HostAction::Open {
                host: "pi".into(),
                session: "native-root".into(),
            },
        )
        .await?;
    let parent = serde_json::from_value(root["task"].clone()).map_err(storage)?;
    let child = fixture
        .owner
        .dispatch(
            OperationId::new(),
            HostAction::Fork {
                parent,
                reference: "native-pi".into(),
            },
        )
        .await?;
    let task = serde_json::from_value(child["task"].clone()).map_err(storage)?;
    let shell = json!({"command":format!("printf '{}'", fixture.checkout.path().display())});
    assert!(matches!(
        fixture
            .event_for(
                "pi",
                "PreToolUse",
                json!({"task":task,"tool_name":"bash","tool_use_id":"wrong-cwd","tool_input":shell})
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(fixture.event_for("pi", "PreToolUse",
        json!({"task":task,"cwd":child["cwd"],"tool_name":"bash","tool_use_id":"native-write","tool_input":shell})).await?, json!({}));
    assert!(fixture.owner.quiescent(task).await.is_err());
    fixture
        .event_for(
            "pi",
            "PostToolUse",
            json!({"task":task,"cwd":child["cwd"],"tool_name":"bash","tool_use_id":"native-write"}),
        )
        .await?;
    fixture.owner.quiescent(task).await?;
    Ok(())
}

#[tokio::test]
async fn pi_session_references_survive_owner_recreation_without_reading_transcripts() -> Result<()>
{
    let mut fixture = Fixture::open().await?;
    let root = fixture
        .owner
        .dispatch(
            OperationId::new(),
            HostAction::Open {
                host: "pi".into(),
                session: "native-root".into(),
            },
        )
        .await?;
    let parent: TaskId = serde_json::from_value(root["task"].clone()).map_err(storage)?;
    let child = fixture
        .owner
        .dispatch(
            OperationId::new(),
            HostAction::Fork {
                parent,
                reference: "pi-child".into(),
            },
        )
        .await?;
    let task: TaskId = serde_json::from_value(child["task"].clone()).map_err(storage)?;
    assert!(matches!(
        fixture
            .event_for("pi", "NativeSession", json!({"task":task}))
            .await,
        Err(Error::NotFound(_))
    ));
    let absent = fixture
        .checkout
        .path()
        .join("never-opened-native-transcript.jsonl");
    assert!(!absent.exists());
    let saved = json!({"id":"native-child-session","file":absent});
    assert_eq!(
        fixture
            .event_for(
                "pi",
                "NativeSession",
                json!({"task":task,"native_session":saved})
            )
            .await?,
        saved
    );
    assert_eq!(
        fixture
            .event_for("pi", "NativeSession", json!({"task":task}))
            .await?,
        saved
    );
    assert!(fixture.owner.records(task).await?.contains(&saved));
    assert!(!fixture.owner.records(parent).await?.contains(&saved));
    for invalid in [
        json!({"id":"","file":null}),
        json!({"id":"child","file":"relative.jsonl"}),
        json!({"id":"child","file":null,"transcript":"forbidden"}),
    ] {
        assert!(
            fixture
                .event_for(
                    "pi",
                    "NativeSession",
                    json!({"task":task,"native_session":invalid})
                )
                .await
                .is_err()
        );
    }
    assert!(matches!(
        fixture
            .event_for(
                "pi",
                "NativeSession",
                json!({"session_id":"another-root","task":task})
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    let local = Arc::clone(&fixture.owner.local);
    drop(fixture.owner);
    fixture.owner = HostWorkspaceOwner::new(local)?;
    fixture
        .owner
        .dispatch(OperationId::new(), HostAction::Reopen { task })
        .await?;
    assert_eq!(
        fixture
            .event_for("pi", "NativeSession", json!({"task":task}))
            .await?,
        saved
    );
    assert!(!absent.exists());
    fixture
        .owner
        .dispatch(OperationId::new(), HostAction::Cancel { task })
        .await?;
    assert!(
        fixture
            .event_for("pi", "NativeSession", json!({"task":task}))
            .await
            .is_err()
    );
    assert!(
        fixture
            .event_for(
                "pi",
                "NativeSession",
                json!({"task":task,"native_session":saved})
            )
            .await
            .is_err()
    );
    Ok(())
}

struct Fixture {
    checkout: tempfile::TempDir,
    _state: tempfile::TempDir,
    owner: HostWorkspaceOwner,
}

impl Fixture {
    async fn open() -> Result<Self> {
        let checkout = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        std::fs::write(checkout.path().join("source.txt"), b"source").map_err(storage)?;
        let local = Arc::new(
            LocalHarness::open(
                LocalHarnessConfig::native_host(checkout.path())
                    .state_directory(state.path().join("owner")),
            )
            .await?,
        );
        let owner = HostWorkspaceOwner::new(local)?;
        Ok(Self {
            checkout,
            _state: state,
            owner,
        })
    }

    async fn event(&mut self, event: &str, fields: Value) -> Result<Value> {
        self.event_for("codex", event, fields).await
    }

    async fn event_for(&mut self, host: &str, event: &str, fields: Value) -> Result<Value> {
        let mut input = json!({"session_id":"native-root","thread_id":"native-root","cwd":self.checkout.path(),"turn_id":"root-turn"});
        input
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("fixture input is not an object".into()))?
            .extend(
                fields
                    .as_object()
                    .ok_or_else(|| Error::Invalid("fixture fields are not an object".into()))?
                    .clone(),
            );
        self.owner
            .dispatch(
                OperationId::new(),
                HostAction::Hook {
                    host: host.into(),
                    event: event.into(),
                    input,
                },
            )
            .await
    }
    async fn start_child(&mut self) -> Result<TaskId> {
        self.event("SessionStart", json!({})).await?;
        std::fs::write(self.checkout.path().join("parent.txt"), b"native parent")
            .map_err(storage)?;
        self.event("PreToolUse",json!({"tool_name":"spawn_agent","tool_use_id":"spawn-one","tool_input":{"message":"child work"}})).await?;
        self.event(
            "SubagentStart",
            json!({"agent_id":"child-one","agent_type":"default"}),
        )
        .await?;
        let records = self
            .owner
            .records(
                self.owner
                    .views
                    .iter()
                    .find(|(_, view)| view.workspace.path() == self.checkout.path())
                    .ok_or_else(|| Error::NotFound("fixture root".into()))?
                    .0
                    .to_owned(),
            )
            .await?;
        let child: TaskId = serde_json::from_value(
            records
                .iter()
                .find(|record| record.get("kind").unwrap_or(&Value::Null) == "agent")
                .ok_or_else(|| Error::NotFound("fixture agent".into()))?
                .get("task")
                .unwrap_or(&Value::Null)
                .clone(),
        )
        .map_err(storage)?;

        Ok(child)
    }
}

#[tokio::test]
async fn native_hook_fork_captures_and_merges_through_shared_task_files() -> Result<()> {
    let mut fixture = Fixture::open().await?;
    let child = fixture.start_child().await?;
    let path = fixture
        .owner
        .attached(child)?
        .workspace
        .path()
        .to_path_buf();
    assert!(path.starts_with(fixture.checkout.path().join(host_workspace::HOST_VIEW_ROOT)));
    assert_eq!(
        std::fs::read(path.join("parent.txt")).map_err(storage)?,
        b"native parent"
    );
    let answer = fixture.event("PreToolUse",json!({"thread_id":"child-one","tool_name":"Bash","tool_use_id":"write-one","tool_input":{"command":"echo child > child.txt"}})).await?;
    assert_eq!(
        answer
            .get("hookSpecificOutput")
            .unwrap_or(&Value::Null)
            .get("permissionDecision")
            .unwrap_or(&Value::Null),
        "allow"
    );
    assert!(
        answer
            .get("hookSpecificOutput")
            .unwrap_or(&Value::Null)
            .get("updatedInput")
            .unwrap_or(&Value::Null)
            .get("command")
            .unwrap_or(&Value::Null)
            .as_str()
            .ok_or_else(|| Error::Invalid("fixture command missing".into()))?
            .contains(&path.to_string_lossy().to_string())
    );
    assert!(
        fixture
            .owner
            .dispatch(OperationId::new(), HostAction::Capture { task: child })
            .await
            .is_err()
    );
    std::fs::write(path.join("child.txt"), b"native child").map_err(storage)?;
    fixture
        .event(
            "PostToolUse",
            json!({"thread_id":"child-one","tool_name":"Bash","tool_use_id":"write-one"}),
        )
        .await?;
    assert!(!fixture.checkout.path().join("child.txt").exists());
    let parent = fixture
        .owner
        .require_native(child)
        .await?
        .ok_or_else(|| Error::NotFound("fixture parent".into()))?;
    fixture
        .owner
        .dispatch(
            OperationId::new(),
            HostAction::Fs {
                task: parent,
                command: "merge agents/child-one".into(),
            },
        )
        .await?;
    assert_eq!(
        std::fs::read(fixture.checkout.path().join("child.txt")).map_err(storage)?,
        b"native child"
    );
    assert!(fixture.owner.local.agent.is_none());
    assert!(fixture.owner.local.tools().definitions(None)?.is_empty());
    Ok(())
}

#[tokio::test]
async fn unknown_threads_and_ambiguous_forks_cannot_fall_back_to_the_root() -> Result<()> {
    let mut fixture = Fixture::open().await?;
    fixture.event("SessionStart", json!({})).await?;
    let unknown = json!({"thread_id":"unbound-child","tool_name":"Bash","tool_use_id":"unknown","tool_input":{"command":"echo outside"}});
    assert!(matches!(
        fixture.event("PreToolUse", unknown).await,
        Err(Error::Unauthorized(_))
    ));
    assert!(
        fixture
            .event(
                "SubagentStart",
                json!({"agent_id":"unprepared-child","agent_type":"default"})
            )
            .await
            .is_err()
    );
    fixture
        .event(
            "PreToolUse",
            json!({"tool_name":"spawn_agent","tool_use_id":"pending-one","tool_input":{}}),
        )
        .await?;
    std::fs::write(
        fixture.checkout.path().join("changed.txt"),
        b"different cut",
    )
    .map_err(storage)?;
    assert!(matches!(
        fixture
            .event(
                "PreToolUse",
                json!({"tool_name":"spawn_agent","tool_use_id":"pending-two","tool_input":{}})
            )
            .await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[tokio::test]
async fn coordination_commands_do_not_wait_on_their_own_write_marker() -> Result<()> {
    let mut fixture = Fixture::open().await?;
    let child = fixture.start_child().await?;
    fixture.event("PreToolUse", json!({"thread_id":"child-one","tool_name":"Bash","tool_use_id":"coordination","tool_input":{"command":"acyclic fs log"}})).await?;
    fixture.owner.quiescent(child).await?;
    fixture.event("PreToolUse", json!({"thread_id":"child-one","tool_name":"Bash","tool_use_id":"compound","tool_input":{"command":"acyclic fs log && echo changed > child.txt"}})).await?;
    assert!(fixture.owner.quiescent(child).await.is_err());
    fixture
        .event(
            "PostToolUse",
            json!({"thread_id":"child-one","tool_name":"Bash","tool_use_id":"compound"}),
        )
        .await?;
    fixture.owner.quiescent(child).await?;
    Ok(())
}

#[tokio::test]
async fn claude_worktree_hook_returns_the_admitted_view_without_permission_override() -> Result<()>
{
    let mut fixture = Fixture::open().await?;
    fixture
        .event_for("claude-code", "SessionStart", json!({}))
        .await?;
    assert!(
        fixture
            .event_for(
                "claude-code",
                "WorktreeCreate",
                json!({"name":"unprepared"})
            )
            .await
            .is_err()
    );
    let answer = fixture.event_for("claude-code", "PreToolUse", json!({"tool_name":"Agent","tool_use_id":"claude-spawn","tool_input":{"prompt":"child"}})).await?;
    assert_eq!(
        answer.pointer("/hookSpecificOutput/updatedInput/isolation"),
        Some(&json!("worktree"))
    );
    assert!(
        answer
            .pointer("/hookSpecificOutput/permissionDecision")
            .is_none()
    );
    let location = fixture
        .event_for("claude-code", "WorktreeCreate", json!({"name":"prepared"}))
        .await?;
    let cwd = location
        .get("cwd")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Invalid("fixture cwd missing".into()))?;
    let path = PathBuf::from(cwd);
    assert!(path.starts_with(fixture.checkout.path().join(host_workspace::HOST_VIEW_ROOT)));
    fixture
        .event_for(
            "claude-code",
            "SubagentStart",
            json!({"agent_id":"claude-child","cwd":path}),
        )
        .await?;
    let shell = json!({"command":format!("printf '{}'", fixture.checkout.path().display())});
    assert!(matches!(fixture.event_for("claude-code", "PreToolUse",
        json!({"agent_id":"claude-child","cwd":fixture.checkout.path(),"tool_name":"Bash","tool_use_id":"wrong-cwd","tool_input":shell})).await,
        Err(Error::Unauthorized(_))));
    assert_eq!(fixture.event_for("claude-code", "PreToolUse",
        json!({"agent_id":"claude-child","cwd":path,"tool_name":"Bash","tool_use_id":"native-write","tool_input":shell})).await?, json!({}),
        "a native worktree command retains its exact arguments and permission checks");
    std::fs::write(path.join("native.txt"), b"native").map_err(storage)?;
    fixture.event_for("claude-code", "PostToolUse",
        json!({"agent_id":"claude-child","cwd":path,"tool_name":"Bash","tool_use_id":"native-write"})).await?;
    fixture
        .event_for(
            "claude-code",
            "WorktreeRemove",
            json!({"worktree_path":path}),
        )
        .await?;
    assert!(!fixture.checkout.path().join("native.txt").exists());
    let task = serde_json::from_value(
        location
            .get("task")
            .cloned()
            .ok_or_else(|| Error::Invalid("fixture task missing".into()))?,
    )
    .map_err(storage)?;
    fixture
        .owner
        .dispatch(OperationId::new(), HostAction::Reopen { task })
        .await?;
    assert_eq!(
        std::fs::read(
            fixture
                .owner
                .attached(task)?
                .workspace
                .path()
                .join("native.txt")
        )
        .map_err(storage)?,
        b"native"
    );
    Ok(())
}

#[tokio::test]
async fn claude_children_bind_by_worktree_even_when_their_start_order_changes() -> Result<()> {
    let mut fixture = Fixture::open().await?;
    let mut locations = Vec::new();
    for name in ["first", "second"] {
        fixture
            .event_for(
                "claude-code",
                "PreToolUse",
                json!({"tool_name":"Agent","tool_use_id":name,"tool_input":{"prompt":name}}),
            )
            .await?;
        locations.push(
            fixture
                .event_for("claude-code", "WorktreeCreate", json!({"name":name}))
                .await?,
        );
    }
    assert!(
        fixture
            .event_for(
                "claude-code",
                "SubagentStart",
                json!({"agent_id":"wrong-cwd"})
            )
            .await
            .is_err()
    );
    for (index, location) in locations.iter().enumerate().rev() {
        let agent = format!("claude-child-{index}");
        let cwd = location
            .get("cwd")
            .cloned()
            .ok_or_else(|| Error::Invalid("fixture cwd absent".into()))?;
        fixture
            .event_for(
                "claude-code",
                "SubagentStart",
                json!({"agent_id":agent,"cwd":cwd}),
            )
            .await?;
        let root = *fixture
            .owner
            .views
            .iter()
            .find(|(_, view)| view.workspace.path() == fixture.checkout.path())
            .ok_or_else(|| Error::NotFound("fixture root absent".into()))?
            .0;
        let records = fixture.owner.records(root).await?;
        let bound = records
            .iter()
            .find(|record| record.get("agent") == Some(&json!(agent)))
            .ok_or_else(|| Error::NotFound("fixture child binding absent".into()))?;
        assert_eq!(bound.get("task"), location.get("task"));
    }
    Ok(())
}
