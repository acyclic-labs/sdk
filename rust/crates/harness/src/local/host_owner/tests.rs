use super::super::host_workspace::tests::native_write;
use super::*;

struct Fixture {
    // Retire retained views while their checkout and state still exist.
    owner: HostWorkspaceOwner,
    checkout: tempfile::TempDir,
    _state: tempfile::TempDir,
    parent: AgentPath,
}

impl Fixture {
    async fn for_host(host: &str) -> Result<Self> {
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
        let mut owner = HostWorkspaceOwner::new(local)?;
        let root = owner
            .dispatch(
                OperationId::new(),
                HostAction::Open {
                    host: host.into(),
                    session: "native-root".into(),
                },
            )
            .await?;
        let parent = serde_json::from_value(root["task"].clone()).map_err(storage)?;
        Ok(Self {
            checkout,
            _state: state,
            owner,
            parent,
        })
    }

    async fn session(&mut self, reference: &str, action: SessionAction) -> Result<Value> {
        self.owner
            .dispatch(
                OperationId::new(),
                HostAction::Session {
                    parent: self.parent.clone(),
                    reference: reference.into(),
                    action,
                },
            )
            .await
    }

    async fn merge(&mut self, reference: &str) -> Result<Value> {
        self.owner
            .dispatch(
                OperationId::new(),
                HostAction::Fs {
                    task: self.parent.clone(),
                    command: format!("merge agents/{reference}"),
                },
            )
            .await
    }
}

fn claim(turn: &Value) -> Result<TurnClaim> {
    serde_json::from_value(turn["claim"].clone()).map_err(storage)
}

fn cwd(value: &Value) -> Result<PathBuf> {
    Ok(PathBuf::from(value["cwd"].as_str().ok_or_else(|| {
        Error::Invalid("fixture view absent".into())
    })?))
}

#[tokio::test]
async fn codex_native_session_lifecycle() -> Result<()> {
    native_session_lifecycle("codex").await
}
#[tokio::test]
async fn claude_native_session_lifecycle() -> Result<()> {
    native_session_lifecycle("claude-code").await
}
#[tokio::test]
async fn pi_native_session_lifecycle() -> Result<()> {
    native_session_lifecycle("pi").await
}

/// One child through spawn, follow-up, a failed write, an owner restart,
/// interruption and merge: every turn is the child agent's own commit.
async fn native_session_lifecycle(host: &str) -> Result<()> {
    let mut fixture = Fixture::for_host(host).await?;
    std::fs::write(
        fixture.checkout.path().join("instructions.md"),
        b"discovered in child view",
    )
    .map_err(storage)?;
    let spawned = fixture
        .session(
            "child",
            SessionAction::Spawn {
                prompt: "initial".into(),
            },
        )
        .await?;
    assert_eq!(spawned["control"], "resume");
    assert_eq!(spawned["pending"], 1);
    let child: AgentPath = serde_json::from_value(spawned["task"].clone()).map_err(storage)?;
    assert_eq!(child.parent().as_ref(), Some(&fixture.parent));
    let sent = fixture
        .session(
            "child",
            SessionAction::Send {
                prompt: "follow-up".into(),
            },
        )
        .await?;
    assert_eq!(sent["pending"], 2);
    let first = fixture.session("child", SessionAction::Next).await?;
    assert_eq!(first["prompt"], "initial");
    assert_eq!(first["turn"], 1);
    assert!(fixture.session("child", SessionAction::Next).await.is_err());
    assert!(
        fixture.merge("child").await.is_err(),
        "a running session cannot merge"
    );
    let view = cwd(&first)?;
    assert_eq!(
        std::fs::read(view.join("instructions.md")).map_err(storage)?,
        b"discovered in child view"
    );
    fixture
        .session(
            "child",
            SessionAction::SaveSession {
                id: "opaque-native-resume-id".into(),
            },
        )
        .await?;
    native_write(&view.join("child.txt"), b"native")?;
    let failed = fixture
        .session(
            "child",
            SessionAction::Complete {
                claim: claim(&first)?,
                outcome: crate::Outcome::Failed {
                    message: "native failed after write".into(),
                },
            },
        )
        .await?;
    assert_eq!(failed.pointer("/status/settled"), Some(&json!(true)));
    // A new owner over the same harness reopens the same agent and view.
    let local = Arc::clone(&fixture.owner.local);
    let previous = std::mem::replace(&mut fixture.owner, HostWorkspaceOwner::new(local)?);
    drop(previous);
    fixture
        .owner
        .dispatch(
            OperationId::new(),
            HostAction::Reopen {
                task: child.clone(),
            },
        )
        .await?;
    let next = fixture.session("child", SessionAction::Next).await?;
    assert_eq!(next["turn"], 2);
    assert_eq!(next["prompt"], "follow-up");
    assert_eq!(
        next.pointer("/session/id"),
        Some(&json!("opaque-native-resume-id"))
    );
    assert_eq!(
        std::fs::read(cwd(&next)?.join("child.txt")).map_err(storage)?,
        b"native"
    );
    fixture
        .session(
            "child",
            SessionAction::Control {
                control: Control::Interrupt,
            },
        )
        .await?;
    let settled = fixture
        .session(
            "child",
            SessionAction::Complete {
                claim: claim(&next)?,
                outcome: crate::Outcome::Cancelled,
            },
        )
        .await?;
    assert_eq!(settled.pointer("/status/settled"), Some(&json!(true)));
    assert_eq!(settled["control"], "interrupt");
    fixture
        .session(
            "child",
            SessionAction::Control {
                control: Control::Resume,
            },
        )
        .await?;
    assert!(
        fixture
            .session("child", SessionAction::Next)
            .await?
            .is_null()
    );
    fixture.merge("child").await?;
    assert_eq!(
        std::fs::read(fixture.checkout.path().join("child.txt")).map_err(storage)?,
        b"native"
    );
    Ok(())
}

#[tokio::test]
async fn native_cancel_interrupts_the_claim_and_archive_settles_unstarted_inputs() -> Result<()> {
    let mut fixture = Fixture::for_host("pi").await?;
    fixture
        .session(
            "active",
            SessionAction::Spawn {
                prompt: "work".into(),
            },
        )
        .await?;
    let turn = fixture.session("active", SessionAction::Next).await?;
    let task = serde_json::from_value(turn["task"].clone()).map_err(storage)?;
    let intent = fixture
        .owner
        .dispatch(OperationId::new(), HostAction::Cancel { task })
        .await?;
    assert_eq!(intent["control"], "interrupt");
    assert_eq!(intent.pointer("/status/settled"), Some(&json!(false)));
    // Recovery settles the orphaned claim once; the original claim then replays.
    let recovered = fixture.session("active", SessionAction::Recover).await?;
    assert_eq!(recovered.pointer("/status/settled"), Some(&json!(true)));
    fixture
        .session(
            "active",
            SessionAction::Complete {
                claim: claim(&turn)?,
                outcome: crate::Outcome::Cancelled,
            },
        )
        .await?;
    fixture
        .session(
            "unstarted",
            SessionAction::Spawn {
                prompt: "never launched".into(),
            },
        )
        .await?;
    let archived = fixture
        .session(
            "unstarted",
            SessionAction::Control {
                control: Control::Archive,
            },
        )
        .await?;
    assert_eq!(archived["control"], "archive");
    assert_eq!(archived["pending"], 0);
    assert_eq!(archived.pointer("/status/turn"), Some(&json!(1)));
    assert!(
        fixture
            .session(
                "unstarted",
                SessionAction::Control {
                    control: Control::Resume
                }
            )
            .await
            .is_err(),
        "an archived session stays archived"
    );
    let agents = fixture
        .owner
        .dispatch(
            OperationId::new(),
            HostAction::Agents {
                task: fixture.parent.clone(),
            },
        )
        .await?;
    assert_eq!(agents["agents"].as_array().map(Vec::len), Some(2));
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn native_child_binding_root_refuses_a_symlink_escape() -> Result<()> {
    let mut fixture = Fixture::for_host("codex").await?;
    let outside = tempfile::tempdir().map_err(storage)?;
    std::os::unix::fs::symlink(
        outside.path(),
        fixture
            .checkout
            .path()
            .join(host_workspace::HOST_VIEW_ROOT)
            .join(".bindings"),
    )
    .map_err(storage)?;
    assert!(
        fixture
            .session(
                "escape",
                SessionAction::Spawn {
                    prompt: "work".into(),
                },
            )
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read_dir(outside.path()).map_err(storage)?.count(),
        0
    );
    Ok(())
}

#[tokio::test]
async fn native_owner_teardown_removes_storage_after_retiring_retained_views() -> Result<()> {
    let mut fixture = Fixture::for_host("codex").await?;
    let child = fixture
        .session(
            "retained-at-shutdown",
            SessionAction::Spawn {
                prompt: "work".into(),
            },
        )
        .await?;
    native_write(&cwd(&child)?.join("retained.txt"), b"native child")?;
    let checkout = fixture.checkout.path().to_path_buf();
    let state = fixture._state.path().to_path_buf();
    let Fixture {
        mut owner,
        checkout: checkout_fixture,
        _state: state_fixture,
        ..
    } = fixture;
    let coordination_thread = std::thread::current().id();
    let (observed, retirement) = std::sync::mpsc::sync_channel(1);
    owner.retirement_observer = Some(observed);
    owner.retire().await?;
    assert_ne!(
        retirement.recv().map_err(storage)?,
        coordination_thread,
        "native view retirement blocked its callback coordination thread"
    );
    drop(checkout_fixture);
    drop(state_fixture);
    assert!(
        !checkout.exists(),
        "checkout cleanup traversed a live native view"
    );
    assert!(!state.exists(), "native state outlived fixture shutdown");
    Ok(())
}
