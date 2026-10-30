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
        let owner = HostWorkspaceOwner::new(local)?;
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
        self.exchange(reference, action, false).await
    }

    /// One exchange that also claims the next input under its fence.
    async fn handoff(&mut self, reference: &str, action: SessionAction) -> Result<Value> {
        self.exchange(reference, action, true).await
    }

    async fn exchange(
        &mut self,
        reference: &str,
        action: SessionAction,
        next: bool,
    ) -> Result<Value> {
        self.owner
            .dispatch(
                OperationId::new(),
                HostAction::Session {
                    parent: self.parent.clone(),
                    reference: reference.into(),
                    action,
                    next,
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
    // The turn's custodied event log names its resume id; settlement derives
    // it, so the running turn spends no exchange on it.
    let custody = fixture
        .checkout
        .path()
        .join(".acyclic-sdk/host-processes")
        .join(child.id().as_str());
    std::fs::create_dir_all(&custody).map_err(storage)?;
    std::fs::write(
        custody.join(format!("events-{}.jsonl", claim(&first)?.operation)),
        b"{\"type\":\"thread.started\",\"thread_id\":\"superseded-id\"}
{\"thread_id\":\"opaque-native-resume-id\"}
not json naming a session_id
",
    )
    .map_err(storage)?;
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
    let interrupt = SessionAction::Control {
        control: Control::Interrupt,
    };
    let intent = fixture.session("active", interrupt).await?;
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
    // A cancel drops the input queued behind a claimed turn at once.
    let spawn = SessionAction::Spawn {
        prompt: "claimed".into(),
    };
    fixture.session("claimed", spawn).await?;
    fixture.session("claimed", SessionAction::Next).await?;
    let behind = SessionAction::Send {
        prompt: "behind".into(),
    };
    assert_eq!(fixture.session("claimed", behind).await?["pending"], 1);
    let cancel = SessionAction::Control {
        control: Control::Cancel,
    };
    assert_eq!(fixture.session("claimed", cancel).await?["pending"], 0);
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

/// Settled and archived children retire their views, so residency follows
/// running or waiting sessions only; a later request reattaches lazily over
/// the same captured fork, and a claimed child stays resident and fenced.
#[tokio::test]
async fn idle_native_views_retire_and_reattach_lazily() -> Result<()> {
    let mut fixture = Fixture::for_host("codex").await?;
    for reference in ["settled", "archived"] {
        let spawn = SessionAction::Spawn {
            prompt: "work".into(),
        };
        fixture.session(reference, spawn).await?;
    }
    assert_eq!(
        fixture.owner.views().unwrap().len(),
        3,
        "waiting children stay resident"
    );
    let turn = fixture.session("settled", SessionAction::Next).await?;
    native_write(&cwd(&turn)?.join("kept.txt"), b"kept")?;
    let outcome = crate::Outcome::Failed {
        message: "settled after write".into(),
    };
    let complete = SessionAction::Complete {
        claim: claim(&turn)?,
        outcome,
    };
    fixture.session("settled", complete).await?;
    let archive = SessionAction::Control {
        control: Control::Archive,
    };
    fixture.session("archived", archive).await?;
    assert_eq!(
        fixture.owner.views().unwrap().len(),
        1,
        "idle children retired"
    );
    let observed = fixture.session("settled", SessionAction::Observe).await?;
    assert_eq!(
        fixture.owner.views().unwrap().len(),
        2,
        "observe reattaches lazily"
    );
    assert_eq!(
        std::fs::read(cwd(&observed)?.join("kept.txt")).map_err(storage)?,
        b"kept"
    );
    let send = SessionAction::Send {
        prompt: "again".into(),
    };
    fixture.session("settled", send).await?;
    fixture.session("settled", SessionAction::Next).await?;
    let agents = HostAction::Agents {
        task: fixture.parent.clone(),
    };
    fixture.owner.dispatch(OperationId::new(), agents).await?;
    assert_eq!(
        fixture.owner.views().unwrap().len(),
        2,
        "a claimed child stays resident"
    );
    assert!(
        fixture
            .session("settled", SessionAction::Next)
            .await
            .is_err(),
        "a resident claimed child keeps exclusive custody"
    );
    Ok(())
}

fn prompt(value: &Value) -> Result<SessionAction> {
    Ok(SessionAction::Reply {
        id: serde_json::from_value(value["id"].clone()).map_err(storage)?,
        inbox: value["inbox"]
            .as_u64()
            .ok_or_else(|| Error::Invalid("fixture prompt has no inbox".into()))?,
    })
}

/// Every prompt, the spawn's first included, is awaited through the shared
/// committed reply: pending until its own turn settles, answered with that
/// turn's outcome, and dropped when a cancel clears it from the queue.
#[tokio::test]
async fn native_prompts_resolve_through_the_committed_reply() -> Result<()> {
    use tracing_subscriber::layer::SubscriberExt as _;

    let mut fixture = Fixture::for_host("codex").await?;
    let spawn = || SessionAction::Spawn {
        prompt: "first".into(),
    };
    let spawned = fixture.session("child", spawn()).await?;
    let replayed = fixture.session("child", spawn()).await?;
    assert_eq!(
        (&spawned["id"], &spawned["inbox"]),
        (&replayed["id"], &replayed["inbox"]),
        "a replayed spawn names the same prompt"
    );
    let first = prompt(&spawned)?;
    let send = SessionAction::Send {
        prompt: "second".into(),
    };
    let second = prompt(&fixture.session("child", send).await?)?;
    let report = acyclic_native_runtime::obs::WaitReport::default();
    let subscriber = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
    let snapshot = || -> Result<String> {
        let mut bytes = Vec::new();
        report.report(&mut bytes).map_err(storage)?;
        String::from_utf8(bytes).map_err(storage)
    };
    let action = || HostAction::Session {
        parent: fixture.parent.clone(),
        reference: "child".into(),
        action: first.clone(),
        next: false,
    };
    let operation = OperationId::new();
    let reply = tracing::dispatcher::with_default(&subscriber, || {
        fixture.owner.dispatch(operation, action())
    });
    // The real dispatch has the same identity as its eventual bridge reply;
    // observing it adds no request and does not consume the pending prompt.
    assert!(snapshot()?.contains(&format!(
        "waiting: native-workspace-dispatch {operation} since "
    )));
    assert_eq!(reply.await?["reply"]["kind"], "pending");
    assert!(
        snapshot()?.is_empty(),
        "a returned pending reply is complete"
    );
    let withdrawn = tracing::dispatcher::with_default(&subscriber, || {
        fixture.owner.dispatch(OperationId::new(), action())
    });
    assert!(!snapshot()?.is_empty());
    drop(withdrawn);
    assert!(
        snapshot()?.is_empty(),
        "dropping dispatch retires its observation"
    );
    let turn = fixture.session("child", SessionAction::Next).await?;
    assert_eq!(turn["prompt"], "first");
    let complete = SessionAction::Complete {
        claim: claim(&turn)?,
        outcome: crate::Outcome::Succeeded(crate::executor::TurnOutput {
            text: "done".into(),
            metadata: Value::Null,
            steps: 1,
            attachments: Vec::new(),
        }),
    };
    fixture.session("child", complete).await?;
    let answered = fixture.session("child", first).await?;
    assert_eq!(answered["reply"]["kind"], "answered");
    assert_eq!(
        answered.pointer("/reply/outcome/Succeeded/text"),
        Some(&json!("done"))
    );
    let send = SessionAction::Send {
        prompt: "third".into(),
    };
    let third = prompt(&fixture.session("child", send).await?)?;
    assert_eq!(
        fixture.session("child", second).await?["reply"]["kind"],
        "pending"
    );
    let running = fixture.session("child", SessionAction::Next).await?;
    assert_eq!(running["prompt"], "second");
    // A cancel drops the prompt queued behind the claimed turn at once.
    let cancel = SessionAction::Control {
        control: Control::Cancel,
    };
    fixture.session("child", cancel).await?;
    assert_eq!(
        fixture.session("child", third).await?["reply"]["kind"],
        "dropped"
    );
    Ok(())
}

/// Acceptance and settlement each carry the next claimed input under their
/// own physical fence: no Observe or Next exchange, a replay of either hands
/// out the identical claim, a plain claim stays refused, and a live process
/// holding the fence receives no work.
#[tokio::test]
async fn native_handoff_carries_next_work_under_the_settlement_fence() -> Result<()> {
    let mut fixture = Fixture::for_host("codex").await?;
    let spawn = || SessionAction::Spawn {
        prompt: "first".into(),
    };
    let accepted = fixture.handoff("child", spawn()).await?;
    let first = accepted["next"].clone();
    assert_eq!(first["prompt"], "first");
    assert_eq!(
        (&first["task"], &first["cwd"]),
        (&accepted["task"], &accepted["cwd"])
    );
    assert!(
        fixture.session("child", SessionAction::Next).await.is_err(),
        "a competitor's plain claim is refused"
    );
    // A replayed acceptance re-hands the identical claim under the fence.
    assert_eq!(
        fixture.handoff("child", spawn()).await?["next"]["claim"],
        first["claim"]
    );
    let send = SessionAction::Send {
        prompt: "second".into(),
    };
    // While a process holds the fence, acceptance carries no work.
    let child: AgentPath = serde_json::from_value(accepted["task"].clone()).map_err(storage)?;
    let workspace = fixture.checkout.path().to_path_buf();
    let held = super::super::host_process::HostProcessLease::acquire(
        &workspace,
        &child,
        &cwd(&accepted)?,
    )?;
    assert!(fixture.handoff("child", send).await?["next"].is_null());
    drop(held);
    let first = claim(&first)?;
    let complete = || SessionAction::Complete {
        claim: first.clone(),
        outcome: crate::Outcome::Cancelled,
    };
    let settled = fixture.handoff("child", complete()).await?;
    assert_eq!(settled["status"]["turn"], 1);
    let second = settled["next"].clone();
    assert_eq!(second["prompt"], "second");
    assert_eq!(second["turn"], 2);
    // An unknown settlement outcome is resent exactly: the same next claim.
    let replayed = fixture.handoff("child", complete()).await?;
    assert_eq!(replayed["next"]["claim"], second["claim"]);
    assert_eq!(replayed["status"]["turn"], 1);
    Ok(())
}
