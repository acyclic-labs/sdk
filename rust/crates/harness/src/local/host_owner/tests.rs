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
    // Replay unchanged installed-host output, including the original usage
    // and refusal fields. Settlement must retain more than the answer text.
    let captures: Value = serde_json::from_str(include_str!(
        "../../../../../../plugin/tests/hosts/fixtures/native-completion.json"
    ))
    .map_err(storage)?;
    let (events, resume) = match host {
        "codex" => (
            captures["records"][0]["stdout"].as_str(),
            Some("01a193eb-fb8f-77c3-bf63-f2cb89618c45"),
        ),
        "claude-code" => (
            captures["records"][1]["stdout"].as_str(),
            Some("a14f2590-4397-48a1-a076-1f2dac2d5349"),
        ),
        _ => (
            Some(include_str!(
                "../host_process/fixtures/pi-assistant-error.jsonl"
            )),
            None,
        ),
    };
    let events = events.ok_or_else(|| Error::Invalid(
        "step=read native lifecycle capture: stdout absent; recovery=cargo test --locked -p acyclic-harness --no-default-features --features filesystem-local,native-execution --lib native_session_lifecycle".into()
    ))?.as_bytes();
    let custody = fixture
        .checkout
        .path()
        .join(".acyclic-sdk/host-processes")
        .join(child.id().as_str());
    std::fs::create_dir_all(&custody).map_err(storage)?;
    std::fs::write(
        custody.join(format!("events-{}.jsonl", claim(&first)?.operation)),
        events,
    )
    .map_err(storage)?;
    // Exercise the original stable content operation before Complete. Both
    // first publication and replay must consume the same source through EOF,
    // preserving its resume identity without a second file read.
    let staged = {
        use crate::local::host_process::{HostProcessLease, children::SessionId};
        let quiescent =
            HostProcessLease::acquire(fixture.checkout.path(), &child, &view)?.quiescent();
        let mut staged = None;
        for _ in 0..2 {
            let mut observed = SessionId::default();
            let file = {
                let mut source = quiescent
                    .events(claim(&first)?.operation, &mut observed)?
                    .ok_or_else(|| Error::Invalid("recorded native log absent".into()))?;
                fixture
                    .owner
                    .native
                    .stage_events(&child, claim(&first)?.operation, &mut source)
                    .await?
            };
            assert_eq!(
                observed.id.as_deref(),
                resume,
                "publication replay skipped original source EOF"
            );
            if let Some(previous) = &staged {
                assert_eq!(previous, &file, "same claim published another artifact");
            }
            staged = Some(file);
        }
        staged
    };
    // A real source-open/read failure must leave the original claim unsettled
    // and its log recoverable. Exercise the deployed completion entry point.
    let log = custody.join(format!("events-{}.jsonl", claim(&first)?.operation));
    let retained = log.with_extension("retained");
    std::fs::rename(&log, &retained).map_err(storage)?;
    std::fs::create_dir(&log).map_err(storage)?;
    let refused = fixture
        .session(
            "child",
            SessionAction::Complete {
                claim: claim(&first)?,
                outcome: crate::Outcome::Cancelled,
            },
        )
        .await;
    assert!(refused.is_err(), "unreadable native log settled its claim");
    assert_eq!(
        fixture.owner.native.session(&child).await?.1.claimed(),
        Some(claim(&first)?.operation)
    );
    assert!(
        retained.is_file(),
        "failed publication retired original events"
    );
    std::fs::remove_dir(&log).map_err(storage)?;
    std::fs::rename(&retained, &log).map_err(storage)?;
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
    let file: crate::conversation::FileRef =
        serde_json::from_value(failed["events"].clone()).map_err(storage)?;
    assert_eq!(staged.as_ref(), Some(&file));
    let (state, _) = fixture.owner.native.session(&child).await?;
    assert_eq!(fixture.owner.native.read(&state, &file).await?, events);
    assert!(
        !custody
            .join(format!("events-{}.jsonl", claim(&first)?.operation))
            .exists()
    );
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
    let (state, reopened) = fixture.owner.native.session(&child).await?;
    assert_eq!(reopened.events.as_ref(), Some(&file));
    assert_eq!(fixture.owner.native.read(&state, &file).await?, events);
    let next = fixture.session("child", SessionAction::Next).await?;
    assert_eq!(next["turn"], 2);
    assert_eq!(next["prompt"], "follow-up");
    assert_eq!(next.pointer("/session/id").and_then(Value::as_str), resume);
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
    assert!(!fixture.owner.views()?.contains_key(&child));
    let session = fixture
        .owner
        .dispatch(
            OperationId::new(),
            HostAction::Hook {
                host: host.into(),
                event: "NativeSession".into(),
                input: json!({"task": child}),
            },
        )
        .await;
    if let Some(resume) = resume {
        assert_eq!(session?["id"], resume);
    } else {
        assert!(matches!(session, Err(Error::NotFound(_))));
    }
    assert!(
        !fixture.owner.views()?.contains_key(&child),
        "reading committed session metadata must not reattach its retired view"
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
    let missing = fixture
        .owner
        .dispatch(
            OperationId::new(),
            HostAction::Hook {
                host: "codex".into(),
                event: "NativeSession".into(),
                input: json!({"task": agents::child(&fixture.parent, "settled")?}),
            },
        )
        .await;
    assert!(
        matches!(missing, Err(Error::NotFound(ref cause)) if cause == "native session reference absent")
    );
    assert_eq!(
        fixture.owner.views()?.len(),
        1,
        "a missing native reference must not reattach a retired child view"
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
            #[cfg(feature = "research")]
            journal_cut: None,
            text: "done".into(),
            metadata: Value::Null,
            steps: 1,
            transcript: Vec::new(),
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

/// Only spawn reaches the approval chain. Allow, deny and human escalation
/// preserve the host's decision for every other tool and namespace.
#[tokio::test]
async fn permission_prompts_gate_spawn_and_preserve_host_tool_decisions() -> Result<()> {
    use crate::approvals::{ApprovalPolicy, ApprovalRule, Approver, RuleVerdict};
    let rules = |tool: &str, verdict| Approver::Rules {
        principal: format!("rules-{tool}"),
        rules: vec![ApprovalRule {
            tool: Some(tool.into()),
            arguments: None,
            selectors: Vec::new(),
            target: None,
            verdict,
        }],
        fallback: RuleVerdict::Ask,
    };
    let prompt = |owner: &HostWorkspaceOwner, host: &str, tool: &str| {
        let answer = owner.permission(
            host,
            &json!({"tool_name": tool, "tool_input": {"reference": "child"}}),
        );
        answer
            .pointer("/hookSpecificOutput/decision/behavior")
            .cloned()
    };
    let owner = |policy: ApprovalPolicy| async move {
        let (checkout, state) = (
            tempfile::tempdir().map_err(storage)?,
            tempfile::tempdir().map_err(storage)?,
        );
        let local = LocalHarness::open(
            LocalHarnessConfig::native_host(checkout.path())
                .state_directory(state.path().join("owner"))
                .approval_policy(policy),
        )
        .await?;
        Ok::<_, Error>((HostWorkspaceOwner::new(Arc::new(local))?, (checkout, state)))
    };
    for (policy, decision) in [
        (
            ApprovalPolicy {
                approvers: vec![rules("spawn", RuleVerdict::Allow)],
            },
            Some(json!("allow")),
        ),
        (ApprovalPolicy { approvers: vec![] }, Some(json!("deny"))),
        (ApprovalPolicy::default(), None),
    ] {
        let (owner, _checkout) = owner(policy).await?;
        for (host, prefix) in [
            ("codex", "mcp__acyclic-sessions__"),
            ("codex", "mcp__acyclic_sessions__"),
            ("claude-code", "mcp__plugin_acyclic_acyclic-sessions__"),
        ] {
            assert_eq!(prompt(&owner, host, &format!("{prefix}spawn")), decision);
            for tool in ["send", "wait", "archive", "cancel", "resume", "spawn_extra"] {
                assert_eq!(prompt(&owner, host, &format!("{prefix}{tool}")), None);
            }
        }
        for (host, tool) in [
            ("codex", "shell"),
            ("codex", "read"),
            ("codex", "write"),
            ("codex", "edit"),
            ("codex", "mcp__other__spawn"),
            ("codex", "mcp__acyclic-sessions__unknown"),
            ("codex", "mcp__plugin_acyclic_acyclic-sessions__spawn"),
            ("claude-code", "mcp__acyclic-sessions__spawn"),
            ("pi", "mcp__acyclic-sessions__spawn"),
        ] {
            assert_eq!(prompt(&owner, host, tool), None, "{host} {tool}");
        }
    }
    Ok(())
}
