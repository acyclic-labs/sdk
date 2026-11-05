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

    async fn restart(&mut self, child: &AgentPath) -> Result<()> {
        let local = Arc::clone(&self.owner.local);
        let previous = std::mem::replace(&mut self.owner, HostWorkspaceOwner::new(local)?);
        drop(previous);
        self.owner
            .dispatch(
                OperationId::new(),
                HostAction::Reopen {
                    task: child.clone(),
                },
            )
            .await?;
        Ok(())
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
    let operation = OperationId::new();
    let parent = fixture.parent.clone();
    let next = || HostAction::Session {
        parent: parent.clone(),
        reference: "child".into(),
        action: SessionAction::Next,
        next: false,
    };
    let first = fixture.owner.dispatch(operation, next()).await?;
    assert_eq!(first["prompt"], "initial");
    assert_eq!(first["turn"], 1);
    assert_eq!(fixture.owner.dispatch(operation, next()).await?, first);
    let retired = cwd(&first)?;
    // Recover an accepted claim whose reply was lost, using its original
    // operation after the workspace owner has been replaced.
    let local = Arc::clone(&fixture.owner.local);
    let previous = std::mem::replace(&mut fixture.owner, HostWorkspaceOwner::new(local)?);
    drop(previous);
    let recovered = fixture.owner.dispatch(operation, next()).await?;
    let view = cwd(&recovered)?;
    // A remounted view has a fresh, never-reused name. Only the physical
    // location changes; the accepted claim and its entire payload survive.
    assert_ne!(view, retired);
    assert!(matches!(
        std::fs::metadata(&retired),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound
    ));
    let mut expected = first;
    expected["cwd"] = json!(view);
    assert_eq!(recovered, expected);
    let first = recovered;
    assert!(matches!(
        fixture.session("child", SessionAction::Next).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(fixture.owner.dispatch(operation, next()).await?, first);
    assert!(
        fixture.merge("child").await.is_err(),
        "a running session cannot merge"
    );
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
    let (events, native_id) = if host == "claude-code" {
        let original = include_str!("../host_process/fixtures/claude-code-completed.jsonl");
        // A representation mutation of the original capture, not another
        // native response: JSON member spelling cannot change recovery.
        let escaped = original.replace(r#""session_id""#, r#""sess\u0069on\u005fid""#);
        assert_eq!(
            serde_json::from_str::<Value>(&escaped).map_err(storage)?,
            serde_json::from_str::<Value>(original).map_err(storage)?
        );
        (escaped, "a14f2590-4397-48a1-a076-1f2dac2d5349")
    } else {
        (
            "{\"type\":\"thread.started\",\"thread_id\":\"superseded-id\"}
{\"thread_id\":\"opaque-native-resume-id\"}
not json naming a session_id
"
            .into(),
            "opaque-native-resume-id",
        )
    };
    std::fs::write(
        custody.join(format!("events-{}.jsonl", claim(&first)?.operation)),
        events,
    )
    .map_err(storage)?;
    native_write(&view.join("child.txt"), b"native")?;
    // The original scale child emits a full retained stderr tail after its
    // exit description. Settlement must preserve both through the owner.
    let diagnostic = format!(
        "native failed after write\n{}\ndiagnostic-tail-{host}",
        "e".repeat(4096)
    );
    let failed = fixture
        .session(
            "child",
            SessionAction::Complete {
                claim: claim(&first)?,
                outcome: crate::Outcome::Failed {
                    message: diagnostic.clone(),
                },
            },
        )
        .await?;
    assert_eq!(failed.pointer("/status/settled"), Some(&json!(true)));
    assert_eq!(
        failed.pointer("/results/0/outcome/Failed/message"),
        Some(&json!(diagnostic))
    );
    // A new owner over the same harness reopens the same agent and view.
    fixture.restart(&child).await?;
    assert_eq!(
        fixture
            .owner
            .session_value(&child, true)
            .await?
            .pointer("/results/0/outcome/Failed/message"),
        Some(&json!(diagnostic)),
        "owner restart must preserve the complete native outcome"
    );
    let next = fixture.session("child", SessionAction::Next).await?;
    assert_eq!(next["turn"], 2);
    assert_eq!(next["prompt"], "follow-up");
    assert_eq!(
        next.pointer("/session/id"),
        Some(&json!(native_id)),
        "settlement must preserve the captured native resume id after an owner restart"
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
    fixture
        .session(
            "child",
            SessionAction::Send {
                prompt: "complete output".into(),
            },
        )
        .await?;
    let final_turn = fixture.session("child", SessionAction::Next).await?;
    let final_claim = claim(&final_turn)?;
    // Use the original staged event log as an attachment, not a fabricated file.
    let mut output = crate::executor::TurnOutput::new(String::new(), json!({"native": host}), 1);
    output.attachments.push(crate::conversation::Attachment {
        file: serde_json::from_value(failed["events"].clone()).map_err(storage)?,
        label: Some("original native log".into()),
    });
    let overhead = serde_json::to_vec(&agents::Event::Complete {
        operation: final_claim.operation,
        outcome: crate::Outcome::Succeeded(output.clone()),
        events: None,
    })
    .map_err(storage)?
    .len();
    output.text = "s".repeat(agents::MAXIMUM_BYTES as usize - overhead);
    assert_eq!(
        serde_json::to_vec(&agents::Event::Complete {
            operation: final_claim.operation,
            outcome: crate::Outcome::Succeeded(output.clone()),
            events: None,
        })
        .map_err(storage)?
        .len(),
        agents::MAXIMUM_BYTES as usize
    );
    let mut oversized = output.clone();
    oversized.text.push('x');
    assert!(matches!(
        fixture
            .session(
                "child",
                SessionAction::Complete {
                    claim: final_claim.clone(),
                    outcome: crate::Outcome::Succeeded(oversized),
                }
            )
            .await,
        Err(Error::Invalid(_))
    ));
    let outcome = crate::Outcome::Succeeded(output);
    let expected = serde_json::to_value(&outcome).map_err(storage)?;
    let completed = fixture
        .session(
            "child",
            SessionAction::Complete {
                claim: final_claim,
                outcome,
            },
        )
        .await?;
    assert_eq!(completed.pointer("/results/0/outcome"), Some(&expected));
    let (state, stored) = fixture.owner.native.session(&child).await?;
    let stored_bytes = serde_json::to_vec(&stored).map_err(storage)?;
    agents::validation::check_native_session_storage(&stored_bytes);
    if let Some(agents::StoredOutcome::Complete(file)) = &stored.result {
        let event_bytes = fixture.owner.native.read(&state, file).await?;
        agents::validation::check_native_session_storage(&event_bytes);
    } else {
        panic!("completed session did not retain its admitted event");
    }
    fixture.restart(&child).await?;
    assert_eq!(
        fixture
            .owner
            .session_value(&child, true)
            .await?
            .pointer("/results/0/outcome"),
        Some(&expected)
    );
    let sent = fixture
        .session(
            "child",
            SessionAction::Send {
                prompt: "after complete output".into(),
            },
        )
        .await?;
    assert_eq!(sent.pointer("/results/0/outcome"), Some(&expected));
    let after = fixture.session("child", SessionAction::Next).await?;
    assert_eq!(after["turn"], 4);
    assert_eq!(after["prompt"], "after complete output");
    assert_eq!(
        fixture
            .owner
            .session_value(&child, true)
            .await?
            .pointer("/results/0/outcome"),
        Some(&expected),
        "an open follow-up must retain the complete previous outcome"
    );
    fixture
        .session(
            "child",
            SessionAction::Complete {
                claim: claim(&after)?,
                outcome: crate::Outcome::Cancelled,
            },
        )
        .await?;
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
        .await?;
    assert_eq!(session["id"], native_id);
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
    let shared = |state: crate::agent::AgentState| {
        state
            .volumes
            .into_iter()
            .find(|binding| binding.name == super::super::SHARED_VOLUME)
            .expect("native session must retain its admitted shared volume")
            .volume
    };
    let admitted = shared(fixture.owner.native.state(&fixture.parent).await?);
    assert_eq!(admitted, fixture.owner.local.shared);
    for reference in ["settled", "archived"] {
        let spawn = SessionAction::Spawn {
            prompt: "work".into(),
        };
        fixture.session(reference, spawn).await?;
        let child = agents::child(&fixture.parent, reference)?;
        assert_eq!(shared(fixture.owner.native.state(&child).await?), admitted);
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
    let child = agents::child(&fixture.parent, "settled")?;
    assert_eq!(shared(fixture.owner.native.state(&child).await?), admitted);
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
        outcome: crate::Outcome::Succeeded(crate::executor::TurnOutput::new(
            "done".into(),
            Value::Null,
            1,
        )),
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
        (
            ApprovalPolicy {
                approvers: vec![rules("spawn", RuleVerdict::Deny)],
            },
            Some(json!("deny")),
        ),
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
