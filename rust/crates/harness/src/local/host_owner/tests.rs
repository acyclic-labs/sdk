use super::super::host_workspace::tests::native_write;
use super::*;
use crate::Outcome;
use crate::task_volume::{ChildFile, ChildState, ChildStatus, Component, FileKey, FileValue};

struct Fixture {
    // Retire retained views while their checkout and state still exist.
    owner: HostWorkspaceOwner,
    checkout: tempfile::TempDir,
    _state: tempfile::TempDir,
    parent: TaskId,
}

impl Fixture {
    async fn open() -> Result<Self> {
        Self::for_host("pi").await
    }

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
                    parent: self.parent,
                    reference: reference.into(),
                    action,
                },
            )
            .await
    }
}

// Each host runs every fence against one admitted root. Reusing that root
// avoids repeating durable harness/mount setup for independent mailbox checks;
// each invariant still owns a distinct child and retains its negative controls.
#[tokio::test]
async fn codex_native_coordination_contract() -> Result<()> {
    native_fence_contract("codex").await
}
#[tokio::test]
async fn claude_native_coordination_contract() -> Result<()> {
    native_fence_contract("claude-code").await
}
#[tokio::test]
async fn pi_native_coordination_contract() -> Result<()> {
    native_fence_contract("pi").await
}
async fn native_fence_contract(host: &str) -> Result<()> {
    let started = std::time::Instant::now();
    let mut fixture = Fixture::for_host(host).await?;
    eprintln!("native contract {host}: admitted {:?}", started.elapsed());
    native_claim_fence_and_read_only_observation_cover_every_host(&mut fixture).await?;
    eprintln!(
        "native contract {host}: claim fence {:?}",
        started.elapsed()
    );
    native_completion_fences_physical_custody_through_every_host(&mut fixture, host).await?;
    super::sessions::assert_lifecycle_projection(&mut fixture.owner, fixture.parent, "physical")
        .await?;
    eprintln!(
        "native contract {host}: physical fence {:?}",
        started.elapsed()
    );
    split_status_without_an_atomic_claim_cannot_authorize_recovery(&mut fixture).await?;
    eprintln!(
        "native contract {host}: atomic mailbox {:?}",
        started.elapsed()
    );
    shared_native_session_lifecycle(&mut fixture, host).await?;
    eprintln!(
        "native contract {host}: resumed lifecycle {:?}",
        started.elapsed()
    );
    failed_and_cancelled_tools_leave_mergeable_views(&mut fixture, host).await?;
    eprintln!(
        "native contract {host}: failed/cancelled writes {:?}",
        started.elapsed()
    );
    live_observation_keeps_latest_result_and_retains_shared_history(&mut fixture).await
}
async fn native_completion_fences_physical_custody_through_every_host(
    fixture: &mut Fixture,
    host: &str,
) -> Result<()> {
    use super::super::host_process::HostProcessLease;

    let observed = fixture
        .session(
            "physical",
            SessionAction::Spawn {
                prompt: "physical process ownership".into(),
            },
        )
        .await?;
    let child: TaskId = serde_json::from_value(observed["task"].clone()).map_err(storage)?;
    let view = PathBuf::from(
        observed["cwd"]
            .as_str()
            .ok_or_else(|| Error::Invalid("physical child cwd absent".into()))?,
    );
    let lease = HostProcessLease::acquire(fixture.checkout.path(), child, &view)?;
    let claimed = fixture.session("physical", SessionAction::Next).await?;
    assert_eq!(claimed["turn"], 1);
    let claim = serde_json::from_value(claimed["claim"].clone()).map_err(storage)?;
    native_write(&view.join("partial.txt"), b"write before failure")?;
    managed_boundaries_only_observe_admission(fixture, host, child).await?;
    let action = SessionAction::Complete {
        claim,
        outcome: Outcome::Failed {
            message: "x".repeat(24 * 1024),
        },
    };
    let error = match fixture.session("physical", action.clone()).await {
        Err(error) => error,
        Ok(result) => {
            return Err(Error::Conflict(format!(
                "a JSON result cannot release a live physical owner: {result}"
            )));
        }
    };
    assert!(
        error
            .to_string()
            .contains("step=acquire native process custody"),
        "{error}"
    );
    assert!(
        fixture
            .session("physical", SessionAction::Recover)
            .await
            .is_err()
    );
    assert!(
        fixture
            .owner
            .dispatch(OperationId::new(), HostAction::Capture { task: child })
            .await
            .is_err()
    );
    let status = fixture.session("physical", SessionAction::Observe).await?;
    assert_eq!(status.pointer("/status/settled"), Some(&json!(false)));
    drop(lease);
    fixture.session("physical", action).await?;
    fixture
        .owner
        .dispatch(
            OperationId::new(),
            HostAction::Fs {
                task: fixture.parent,
                command: "merge agents/physical".into(),
            },
        )
        .await?;
    assert_eq!(
        std::fs::read(fixture.checkout.path().join("partial.txt")).map_err(storage)?,
        b"write before failure"
    );
    let proof = HostProcessLease::acquire(fixture.checkout.path(), child, &view)?.quiescent();
    assert!(proof.verify(fixture.parent, &view).is_err());
    assert!(proof.verify(child, fixture.checkout.path()).is_err());
    proof.verify(child, &view)?;
    Ok(())
}

async fn managed_boundaries_only_observe_admission(
    fixture: &mut Fixture,
    host: &str,
    child: TaskId,
) -> Result<()> {
    let (canonical, expected_reads) =
        crate::test_support::admission_work::measure(fixture.owner.require_native(child)).await;
    assert!(canonical?.is_some());
    assert!(expected_reads > 0);
    for event in ["Stop", "SessionEnd"] {
        let (result, reads) = crate::test_support::admission_work::measure(fixture.owner.dispatch(
            OperationId::new(),
            HostAction::Hook {
                host: host.into(),
                event: event.into(),
                input: json!({"task": child}),
            },
        ))
        .await;
        assert_eq!(
            result?,
            json!({}),
            "managed capture belongs to the physical owner"
        );
        assert_eq!(
            reads, expected_reads,
            "managed boundary repeated native admission"
        );
    }
    Ok(())
}

async fn native_claim_fence_and_read_only_observation_cover_every_host(
    fixture: &mut Fixture,
) -> Result<()> {
    let parent = fixture.owner.attached(fixture.parent)?;
    fixture
        .owner
        .local
        .admit_host_child(&parent.context, "unentered")
        .await?;
    let before = fixture
        .owner
        .local
        .task_files
        .inner
        .head()
        .await
        .map_err(storage)?;
    let located = fixture.owner.locate(fixture.checkout.path()).await?;
    assert_eq!(located["task"], json!(fixture.parent));
    assert_eq!(
        fixture.owner.views.len(),
        1,
        "cwd discovery attached an unentered task"
    );
    assert_eq!(
        before,
        fixture
            .owner
            .local
            .task_files
            .inner
            .head()
            .await
            .map_err(storage)?
    );
    let unknown = fixture
        .checkout
        .path()
        .join(host_workspace::HOST_VIEW_ROOT)
        .join("unadmitted");
    std::fs::create_dir(&unknown).map_err(storage)?;
    assert!(
        fixture.owner.locate(&unknown).await.is_err(),
        "unknown agent cwd fell back to its parent"
    );
    fixture
        .session(
            "worker",
            SessionAction::Spawn {
                prompt: "initial".into(),
            },
        )
        .await?;
    let child = fixture.session("worker", SessionAction::Observe).await?;
    let cwd = Path::new(
        child["cwd"]
            .as_str()
            .ok_or_else(|| Error::Invalid("child cwd absent".into()))?,
    );
    assert_eq!(fixture.owner.locate(cwd).await?["task"], child["task"]);
    let files = fixture.owner.files(fixture.parent).await?;
    let before = files.inner.head().await.map_err(storage)?;
    fixture.session("worker", SessionAction::Observe).await?;
    assert_eq!(
        before,
        files.inner.head().await.map_err(storage)?,
        "observation published or re-admitted a child"
    );
    let next = fixture.session("worker", SessionAction::Next).await?;
    let original = serde_json::from_value(next["claim"].clone()).map_err(storage)?;
    let mut forged = next["claim"].clone();
    forged["operation"] = serde_json::to_value(OperationId::new()).map_err(storage)?;
    let before = files.inner.head().await.map_err(storage)?;
    assert!(
        fixture
            .session(
                "worker",
                SessionAction::Complete {
                    claim: serde_json::from_value(forged).map_err(storage)?,
                    outcome: Outcome::Cancelled,
                }
            )
            .await
            .is_err()
    );
    assert_eq!(
        before,
        files.inner.head().await.map_err(storage)?,
        "foreign claim changed the mailbox"
    );
    fixture
        .session(
            "worker",
            SessionAction::Complete {
                claim: original,
                outcome: Outcome::Cancelled,
            },
        )
        .await?;
    Ok(())
}

async fn split_status_without_an_atomic_claim_cannot_authorize_recovery(
    fixture: &mut Fixture,
) -> Result<()> {
    let child = fixture
        .session(
            "split-worker",
            SessionAction::Spawn {
                prompt: "initial".into(),
            },
        )
        .await?;
    let task = serde_json::from_value(child["task"].clone()).map_err(storage)?;
    fixture
        .owner
        .publish(
            fixture.parent,
            FileValue::new(
                FileKey::Child {
                    reference: Component::new("split-worker").map_err(storage)?,
                    file: ChildFile::Status,
                },
                serde_json::to_vec(&ChildStatus {
                    task,
                    state: ChildState::Running,
                    settled: false,
                    inputs: 1,
                })
                .map_err(storage)?,
            ),
        )
        .await?;
    let files = fixture.owner.files(fixture.parent).await?;
    let before = files.inner.head().await.map_err(storage)?;
    assert!(
        fixture
            .session("split-worker", SessionAction::Recover)
            .await
            .is_err()
    );
    assert_eq!(before, files.inner.head().await.map_err(storage)?);
    Ok(())
}

#[tokio::test]
async fn codex_native_owner_teardown_removes_storage_after_retiring_retained_views() -> Result<()> {
    native_owner_teardown("codex").await
}

#[tokio::test]
async fn native_retirement_rejects_the_former_inline_drop_before_view_admission() -> Result<()> {
    let coordination_thread = std::thread::current().id();
    for former_inline_drop in [false, true] {
        let checkout = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        let local = Arc::new(
            LocalHarness::open(
                LocalHarnessConfig::native_host(checkout.path())
                    .state_directory(state.path().join("owner")),
            )
            .await?,
        );
        let mut owner = HostWorkspaceOwner::new(local)?;
        let (observed, retirement) = std::sync::mpsc::sync_channel(1);
        owner.retirement_observer = Some(observed);
        if former_inline_drop {
            // The actual former retire body from the retained original
            // control: replace the awaited blocking-pool drop with drop(self).
            // No view is admitted here, so the independent thread oracle can
            // reject that ordering without blocking native view callbacks.
            let local = Arc::clone(&owner.local);
            drop(owner);
            Arc::try_unwrap(local)
                .map_err(|_| Error::Conflict("control retained a harness handle".into()))?
                .disconnect()
                .await?;
        } else {
            owner.retire().await?;
        }
        let off_coordination_thread = retirement.recv().map_err(storage)? != coordination_thread;
        assert_eq!(
            off_coordination_thread, !former_inline_drop,
            "the retirement-thread oracle must reject the actual former inline drop"
        );
        let checkout_path = checkout.path().to_path_buf();
        let state_path = state.path().to_path_buf();
        drop(checkout);
        drop(state);
        assert!(!checkout_path.exists());
        assert!(!state_path.exists());
    }
    Ok(())
}

#[tokio::test]
async fn claude_native_owner_teardown_removes_storage_after_retiring_retained_views() -> Result<()>
{
    native_owner_teardown("claude-code").await
}
#[tokio::test]
async fn pi_native_owner_teardown_removes_storage_after_retiring_retained_views() -> Result<()> {
    native_owner_teardown("pi").await
}
async fn native_owner_teardown(host: &str) -> Result<()> {
    let mut fixture = Fixture::for_host(host).await?;
    let child = fixture
        .owner
        .dispatch(
            OperationId::new(),
            HostAction::Fork {
                parent: fixture.parent,
                reference: "retained-at-shutdown".into(),
            },
        )
        .await?;
    let view = child["cwd"]
        .as_str()
        .ok_or_else(|| Error::Invalid("fixture child cwd absent".into()))?;
    native_write(&Path::new(view).join("retained.txt"), b"native child")?;
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

// Negative control: this same lifecycle on the previous per-tool ledger leaves
// a pending write with no PostToolUse and refuses SessionEnd/capture/merge.
async fn failed_and_cancelled_tools_leave_mergeable_views(
    fixture: &mut Fixture,
    host: &str,
) -> Result<()> {
    // Continue the admitted native session across both outcomes. A failed or
    // cancelled write must not require allocating a replacement child/view.
    let observed = fixture.session("child", SessionAction::Observe).await?;
    let retained = serde_json::from_value(observed["task"].clone()).map_err(storage)?;
    for reference in ["failed-write", "cancelled-write"] {
        let location = fixture
            .owner
            .dispatch(OperationId::new(), HostAction::Reopen { task: retained })
            .await?;
        let child = serde_json::from_value(location["task"].clone()).map_err(storage)?;
        let cwd = PathBuf::from(
            location["cwd"]
                .as_str()
                .ok_or_else(|| Error::Invalid("view absent".into()))?,
        );
        assert!(
            cwd.starts_with(
                std::fs::canonicalize(fixture.checkout.path())
                    .map_err(storage)?
                    .join(host_workspace::HOST_VIEW_ROOT)
            )
        );
        let parent = fixture.owner.attached(fixture.parent)?;
        let attached = fixture.owner.attached(child)?;
        parent
            .workspace
            .name_child(
                &parent.context,
                &attached.context,
                &attached.workspace,
                "child",
            )
            .await?;
        let nested = fixture
            .owner
            .dispatch(
                OperationId::new(),
                HostAction::Fork {
                    parent: child,
                    reference: "nested".into(),
                },
            )
            .await?;
        let nested_task = serde_json::from_value(nested["task"].clone()).map_err(storage)?;
        let nested_path = PathBuf::from(
            nested["cwd"]
                .as_str()
                .ok_or_else(|| Error::Invalid("nested view absent".into()))?,
        );
        let parent = fixture.owner.attached(child)?;
        let attached = fixture.owner.attached(nested_task)?;
        parent
            .workspace
            .name_child(
                &parent.context,
                &attached.context,
                &attached.workspace,
                "nested",
            )
            .await?;
        let nested_file = format!("{reference}-nested.txt");
        native_write(&nested_path.join(&nested_file), b"nested native write")?;
        fixture
            .owner
            .dispatch(
                OperationId::new(),
                HostAction::Capture { task: nested_task },
            )
            .await?;
        fixture.owner.dispatch(OperationId::new(),HostAction::Hook {host:host.into(),event:"PreToolUse".into(),input:json!({"task":child,"session_id":"native-root","cwd":cwd,"tool_name":"bash","tool_use_id":reference,"tool_input":{"command":"write then fail or cancel"}})}).await?;
        native_write(
            &cwd.join(format!("{reference}.txt")),
            b"partial native write",
        )?;
        // The native host has stopped the failed/cancelled tool. No post hook.
        // A stale pre-tool record must not block this view as a merge target.
        fixture
            .owner
            .dispatch(
                OperationId::new(),
                HostAction::Fs {
                    task: child,
                    command: "merge agents/nested".into(),
                },
            )
            .await?;
        assert_eq!(
            std::fs::read(cwd.join(&nested_file)).map_err(storage)?,
            b"nested native write"
        );
        fixture
            .owner
            .dispatch(
                OperationId::new(),
                HostAction::Hook {
                    host: host.into(),
                    event: "SessionEnd".into(),
                    input: json!({"task":child,"session_id":"native-root","cwd":cwd}),
                },
            )
            .await?;
        fixture
            .owner
            .dispatch(
                OperationId::new(),
                HostAction::Fs {
                    task: fixture.parent,
                    command: "merge agents/child".into(),
                },
            )
            .await?;
        assert_eq!(
            std::fs::read(fixture.checkout.path().join(format!("{reference}.txt")))
                .map_err(storage)?,
            b"partial native write"
        );
        assert_eq!(
            std::fs::read(fixture.checkout.path().join(nested_file)).map_err(storage)?,
            b"nested native write"
        );
    }
    Ok(())
}

async fn shared_native_session_lifecycle(fixture: &mut Fixture, host: &str) -> Result<()> {
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
    assert!(spawned["control"].is_null());
    assert_eq!(spawned["pending"], 1);
    let resumed = fixture
        .session(
            "child",
            SessionAction::Control {
                control: crate::task_volume::Control::Resume,
            },
        )
        .await?;
    assert_eq!(resumed["control"], "resume");
    assert!(resumed["status"].is_null());
    let sent = fixture
        .session(
            "child",
            SessionAction::Send {
                prompt: "follow-up".into(),
            },
        )
        .await?;
    assert_eq!(sent["pending"], 2);
    let files = fixture.owner.files(fixture.parent).await?;
    let reference = Component::new("child").map_err(storage)?;
    let (canonical, canonical_reads) =
        crate::test_support::admission_work::measure(fixture.owner.local.observe_admitted_child(
            &files,
            &reference,
            fixture.parent,
            crate::task_volume::ChildResultWindow::Latest,
            1,
        ))
        .await;
    canonical?;
    let (observed, adapter_reads) = crate::test_support::admission_work::measure(
        fixture.session("child", SessionAction::Observe),
    )
    .await;
    assert_eq!(observed?["pending"], 2);
    assert!(canonical_reads > 0);
    assert_eq!(
        adapter_reads, canonical_reads,
        "native observation repeats the canonical admission lookup"
    );
    let first = fixture.session("child", SessionAction::Next).await?;
    assert_eq!(first["prompt"], "initial");
    assert_eq!(first["turn"], 1);
    assert!(fixture.session("child", SessionAction::Next).await.is_err());
    assert!(
        fixture
            .owner
            .dispatch(
                OperationId::new(),
                HostAction::Fs {
                    task: fixture.parent,
                    command: "merge agents/child".into()
                }
            )
            .await
            .is_err()
    );
    let child: TaskId = serde_json::from_value(first["task"].clone()).map_err(storage)?;
    let admission = fixture
        .owner
        .local
        .runtime
        .task_host()
        .observe_admission(child)
        .await?;
    assert_eq!(admission.parent, Some(fixture.parent));
    assert_eq!(admission.input.get("host"), Some(&json!(host)));
    let cwd = PathBuf::from(
        first["cwd"]
            .as_str()
            .ok_or_else(|| Error::Invalid("view absent".into()))?,
    );
    assert_eq!(
        std::fs::read(cwd.join("instructions.md")).map_err(storage)?,
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
    native_write(&cwd.join("child.txt"), b"native")?;
    fixture
        .session(
            "child",
            SessionAction::Complete {
                claim: serde_json::from_value(first["claim"].clone()).map_err(storage)?,
                outcome: crate::Outcome::Failed {
                    message: "native failed after write".into(),
                },
            },
        )
        .await?;
    let local = Arc::clone(&fixture.owner.local);
    let previous = std::mem::replace(&mut fixture.owner, HostWorkspaceOwner::new(local)?);
    drop(previous);
    fixture
        .owner
        .dispatch(OperationId::new(), HostAction::Reopen { task: child })
        .await?;
    let next = fixture.session("child", SessionAction::Next).await?;
    assert_eq!(next["turn"], 2);
    assert_eq!(next["prompt"], "follow-up");
    assert_eq!(
        next.pointer("/session/id"),
        Some(&json!("opaque-native-resume-id"))
    );
    let recreated = PathBuf::from(
        next["cwd"]
            .as_str()
            .ok_or_else(|| Error::Invalid("view absent".into()))?,
    );
    assert_eq!(
        std::fs::read(recreated.join("child.txt")).map_err(storage)?,
        b"native"
    );
    fixture
        .session(
            "child",
            SessionAction::Control {
                control: crate::task_volume::Control::Interrupt,
            },
        )
        .await?;
    let settled = fixture
        .session(
            "child",
            SessionAction::Complete {
                claim: serde_json::from_value(next["claim"].clone()).map_err(storage)?,
                outcome: crate::Outcome::Cancelled,
            },
        )
        .await?;
    assert_eq!(settled.pointer("/status/settled"), Some(&json!(true)));
    assert_eq!(
        settled.pointer("/status/state"),
        Some(&json!("Interrupted"))
    );
    fixture
        .session(
            "child",
            SessionAction::Control {
                control: crate::task_volume::Control::Resume,
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
        .owner
        .dispatch(
            OperationId::new(),
            HostAction::Fs {
                task: fixture.parent,
                command: "merge agents/child".into(),
            },
        )
        .await?;
    assert_eq!(
        std::fs::read(fixture.checkout.path().join("child.txt")).map_err(storage)?,
        b"native"
    );
    Ok(())
}

#[tokio::test]
async fn native_claim_recovery_does_not_replay_unacknowledged_input_or_replace_results()
-> Result<()> {
    let mut fixture = Fixture::open().await?;
    fixture
        .session(
            "recover",
            SessionAction::Spawn {
                prompt: "claimed".into(),
            },
        )
        .await?;
    let claimed = fixture.session("recover", SessionAction::Next).await?;
    assert_eq!(claimed["turn"], 1);
    let recovered = fixture.session("recover", SessionAction::Recover).await?;
    assert_eq!(recovered.pointer("/status/settled"), Some(&json!(true)));
    assert!(
        fixture
            .session("recover", SessionAction::Next)
            .await?
            .is_null()
    );
    fixture
        .session(
            "recover",
            SessionAction::Send {
                prompt: "second".into(),
            },
        )
        .await?;
    let next = fixture.session("recover", SessionAction::Next).await?;
    assert_eq!(next["turn"], 2);
    assert_eq!(next["prompt"], "second");
    fixture
        .session(
            "recover",
            SessionAction::Complete {
                claim: serde_json::from_value(next["claim"].clone()).map_err(storage)?,
                outcome: crate::Outcome::Cancelled,
            },
        )
        .await?;
    assert!(
        fixture
            .session("recover", SessionAction::Next)
            .await?
            .is_null()
    );
    Ok(())
}

async fn live_observation_keeps_latest_result_and_retains_shared_history(
    fixture: &mut Fixture,
) -> Result<()> {
    const TURNS: u64 = 3;
    // The physical-custody contract already completes the first real turn
    // with a retained large result. Continue that session, preserving all
    // three ordered results without creating another task or repeating turn 1.
    for turn in 2..=TURNS {
        fixture
            .session(
                "physical",
                SessionAction::Send {
                    prompt: "follow-up".into(),
                },
            )
            .await?;
        let claimed = fixture.session("physical", SessionAction::Next).await?;
        assert_eq!(claimed["turn"], turn);
        fixture
            .session(
                "physical",
                SessionAction::Complete {
                    claim: serde_json::from_value(claimed["claim"].clone()).map_err(storage)?,
                    outcome: crate::Outcome::Failed {
                        message: "x".repeat(24 * 1024),
                    },
                },
            )
            .await?;
    }
    let observed = fixture.session("physical", SessionAction::Observe).await?;
    assert_eq!(
        observed["results"].as_array().map(Vec::len),
        Some(1),
        "live observations must not accumulate historical answers"
    );
    assert_eq!(observed.pointer("/results/0/turn"), Some(&json!(TURNS)));
    assert!(serde_json::to_vec(&observed).map_err(storage)?.len() < 65_536);
    let reference = Component::new("physical").map_err(storage)?;
    let files = fixture.owner.files(fixture.parent).await?;
    let mut retained_bytes = 0;
    for turn in 1..=TURNS {
        let retained = files
            .snapshot(
                &Query::File(FileKey::Child {
                    reference: reference.clone(),
                    file: ChildFile::Result(turn),
                }),
                1,
            )
            .await
            .map_err(storage)?;
        assert_eq!(retained.files.len(), 1);
        retained_bytes += retained
            .files
            .iter()
            .map(|file| file.bytes.len())
            .sum::<usize>();
    }
    assert!(retained_bytes > 65_536);
    // A nested parent selects its own mailbox, even when the root has the
    // same child reference. Observation must never fall back to root files.
    let child = serde_json::from_value(observed["task"].clone()).map_err(storage)?;
    for action in [
        SessionAction::Spawn {
            prompt: "nested initial".into(),
        },
        SessionAction::Observe,
    ] {
        let nested = fixture
            .owner
            .dispatch(
                OperationId::new(),
                HostAction::Session {
                    parent: child,
                    reference: "physical".into(),
                    action,
                },
            )
            .await?;
        assert_ne!(nested["task"], observed["task"]);
        assert!(nested["status"].is_null());
        assert!(nested["control"].is_null());
        assert_eq!(nested["results"], json!([]));
        assert_eq!(nested["pending"], 1);
    }
    Ok(())
}

#[tokio::test]
async fn native_builtin_subagents_and_tool_events_do_not_create_harness_forks() -> Result<()> {
    let mut fixture = Fixture::open().await?;
    for event in [
        "PreToolUse",
        "PostToolUse",
        "PostToolUseFailure",
        "SubagentStart",
        "SubagentStop",
        "WorktreeCreate",
        "WorktreeRemove",
    ] {
        assert_eq!(
            fixture
                .owner
                .dispatch(
                    OperationId::new(),
                    HostAction::Hook {
                        host: "codex".into(),
                        event: event.into(),
                        input: json!({"agent_id":"native-only"})
                    }
                )
                .await?,
            json!({})
        );
    }
    assert_eq!(fixture.owner.views.len(), 1);
    assert_eq!(
        fixture
            .owner
            .dispatch(
                OperationId::new(),
                HostAction::Sessions {
                    parent: fixture.parent,
                    after: None,
                }
            )
            .await?,
        json!([])
    );
    Ok(())
}

#[tokio::test]
async fn native_cli_cancel_signals_the_process_owner_and_archive_settles_unstarted_inputs()
-> Result<()> {
    let mut fixture = Fixture::open().await?;
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
    fixture
        .session(
            "active",
            SessionAction::Complete {
                claim: serde_json::from_value(turn["claim"].clone()).map_err(storage)?,
                outcome: crate::Outcome::Cancelled,
            },
        )
        .await?;
    fixture
        .session(
            "active",
            SessionAction::SaveSession {
                id: "retained-session".into(),
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
                control: crate::task_volume::Control::Archive,
            },
        )
        .await?;
    assert_eq!(archived.pointer("/status/state"), Some(&json!("Archived")));
    assert_eq!(archived.pointer("/status/settled"), Some(&json!(true)));
    assert!(
        fixture
            .session(
                "unstarted",
                SessionAction::Control {
                    control: crate::task_volume::Control::Resume
                }
            )
            .await
            .is_err()
    );
    Ok(())
}
