use super::*;
use acyclic_harness::{
    AgentId, Capabilities,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner},
};
use std::path::Path;
#[cfg(all(unix, feature = "tui"))]
use std::time::Duration;

async fn configuration(root: &Path) -> Result<Config> {
    acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
    let provider = ProviderRef::new("restart".into(), "filesystem".into(), "1".into(), &mut None)?;
    let agent = AgentId::new();
    let private_volume = VolumeRef::new(
        provider.clone(),
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let authority = Authority {
        kind: acyclic_harness::core::AggregateKind::Conversation,
        id: "restart".into(),
    };
    let issuer = AuthorityIssuer::new("restart", [37; 32], authority.clone());
    #[derive(serde::Deserialize)]
    struct Scenario {
        grants: Vec<String>,
    }
    let mut grants = serde_json::from_str::<Scenario>(include_str!("../native_scenario/data.json"))
        .map_err(invalid)?
        .grants;
    grants.extend([
        private_volume.capability(VolumeOperation::Read, &mut None)?,
        private_volume.capability(VolumeOperation::Write, &mut None)?,
    ]);
    let scope = issuer.issue(
        "restart",
        &Capabilities::new(grants),
        Some(agent),
        None,
        &mut None,
    )?;
    let filesystem_root = root.join("filesystem");
    let files = FilesystemHost::new(
        acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&filesystem_root))
            .await
            .map_err(storage)?,
        &provider,
        None,
    )?;
    files.create_volume(&private_volume).await?;
    Ok(Config {
        filesystem_root,
        stream_root: root.join("stream"),
        filesystem_provider: provider,
        stream_provider: ProviderRef::new(
            "restart".into(),
            "stream".into(),
            "1".into(),
            &mut None,
        )?,
        private_volume,
        owner: AuthorityBinding {
            authority,
            issuer: "restart".into(),
            key: [37; 32],
            scope,
        },
        generation: "initial".into(),
        model_invocation_ledger: root.join("invocations.jsonl"),
        limits: Limits::default(),
        run_limits: Default::default(),
        session_limits: SessionLimits {
            active_tasks: 1,
            total_tasks: 1,
            depth: 1,
            model_steps: 1,
        },
        concurrency: 1,
        deterministic_delay_ms: 5000,
        fork_sources: Vec::new(),
        tools: crate::native_tools::NativeToolsConfig {
            content_budget: acyclic_fs::performance::BOUNDARY_BUDGET,
            ..Default::default()
        },
        context_manifest: None,
        native_publications: Vec::new(),
        project_source_root: None,
    })
}

async fn publish_manifest(
    session: &NativeSession,
    label: &str,
) -> Result<acyclic_harness::conversation::FileRef> {
    let publisher = FilesystemContentPublisher::new(
        session.files.clone(),
        &session.config.private_volume,
        &session.config.owner.verifier()?,
        &session.config.owner.scope,
        MAX_FRAME_BYTES as u64,
        None,
    )?;
    let manifest = ContextManifest {
        conventions: vec![label.into()],
        tools: acyclic_harness::tools::defaults()?.definitions(None)?,
        ..Default::default()
    };
    manifest
        .publish(
            publisher.as_ref(),
            OperationId::new(),
            &format!("context/{label}.json"),
            session.config.limits,
        )
        .await
}

// Exercise the real prompt publication, durable admission and resume paths.
// No dispatch has occurred in the first case; the second crashes after dispatch
// and must reconcile A without generating again, even though startup selects B.
#[tokio::test]
async fn queued_manifest_survives_restart_with_a_different_binding() -> Result<()> {
    use acyclic_harness::{
        model::{ModelContent, ModelEvent},
        test_support::{
            ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn,
        },
    };
    for dispatched in [false, true] {
        let directory = tempfile::tempdir().map_err(storage)?;
        let mut config = configuration(directory.path()).await?;
        config.deterministic_delay_ms = 0;
        let setup = NativeSession::open(config.clone()).await?;
        let a = publish_manifest(&setup, "ORIGINAL_A").await?;
        let b = publish_manifest(&setup, "REPLACEMENT_B").await?;
        drop(setup);
        config.context_manifest = Some(a.clone());
        let mut first = NativeSession::open(config.clone()).await?;
        let command = first
            .prompt_command("pinned prompt", OperationId::new())
            .await?;
        let reader = FilesystemContentVerifier::new(
            first.files.clone(),
            &config.owner.verifier()?,
            &config.owner.scope,
            MAX_FRAME_BYTES as u64,
            None,
        )?;
        use acyclic_harness::conversation::ContentResidencyVerifier as _;
        let bytes = reader
            .read(&command.payload, MAX_FRAME_BYTES as u64, None)?
            .await?;
        let retained: acyclic_harness::filesystem::ModelTaskCommand =
            serde_json::from_slice(&bytes).map_err(invalid)?;
        assert_eq!(retained.context_manifest, a);
        drop((bytes, reader, retained, a));
        let task = match first.queue(OperationId::new(), command, None).await? {
            Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => task_id,
            _ => panic!("expected an admitted durable task"),
        };
        let signal = Arc::new(tokio::sync::Notify::new());
        let observer = signal.clone();
        let original = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Events(vec![ScriptedEvent::Pending {}])])
                .with_dispatch_observer(move |_| observer.notify_one()),
        );
        if dispatched {
            first.model_provider = original.clone();
            let WorkPull::Claimed(lease) = first
                .claim(
                    task,
                    &Worker {
                        id: "original".into(),
                        available: Default::default(),
                        labels: Default::default(),
                    },
                )
                .await?
            else {
                panic!("original lease missing")
            };
            let running = first.resume(lease);
            tokio::pin!(running);
            tokio::select! {
                outcome = &mut running => panic!("completed before crash: {outcome:?}"),
                _ = signal.notified() => {},
            }
        }
        drop(first);
        config.context_manifest = Some(b);
        let mut reopened = NativeSession::open(config).await?;
        let recovered = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("done".into())]).with_reconciliations([
                ScriptedReconciliation {
                    orphan: true,
                    observed: Vec::new(),
                    continuation: Some(vec![ModelEvent::Completed {
                        metadata: serde_json::Value::Null,
                    }]),
                },
            ]),
        );
        reopened.model_provider = recovered.clone();
        let pull = if dispatched {
            reopened
                .runtime
                .task_host()
                .recover_work(task, &mut None)
                .await?
        } else {
            reopened
                .claim(
                    task,
                    &Worker {
                        id: "recovered".into(),
                        available: Default::default(),
                        labels: Default::default(),
                    },
                )
                .await?
        };
        let WorkPull::Claimed(lease) = pull else {
            panic!("retained task missing")
        };
        let outcome = reopened.resume(lease).await?;
        assert!(outcome.starts_with("worker completed"), "{outcome}");
        let requests = if dispatched {
            original.requests()
        } else {
            recovered.requests()
        };
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].messages[0].content,
            ModelContent::Text("ORIGINAL_A".into())
        );
        assert!(
            !serde_json::to_string(&requests)
                .map_err(invalid)?
                .contains("REPLACEMENT_B")
        );
        assert_eq!(recovered.requests().len(), usize::from(!dispatched));
        assert_eq!(recovered.reconciliations().len(), usize::from(dispatched));
    }
    Ok(())
}

#[cfg(all(unix, feature = "tui"))]
async fn plan_write_trigger(trigger: &Path, index: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while tokio::fs::read_to_string(trigger).await.unwrap_or_default() != index.to_string() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .map_err(storage)
}

#[cfg(all(unix, feature = "tui"))]
#[tokio::test]
async fn running_plan_panel_follows_authenticated_private_writes_over_pty()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use std::process::Command;
    let scenario = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/scenarios/plan-query/filenames-and-paragraphs.json");
    if let Some(config_path) = std::env::var_os("GC_PLAN_PTY_CONFIG") {
        #[derive(serde::Deserialize)]
        struct Revision {
            path: String,
            text: String,
        }
        #[derive(serde::Deserialize)]
        struct Scenario {
            writes: Vec<Revision>,
        }
        let config = Config::from_file(Path::new(&config_path))?;
        let native = Arc::new(NativeSession::open(config.clone()).await?);
        let publisher = FilesystemContentPublisher::new(
            native.files.clone(),
            &config.private_volume,
            &config.owner.verifier()?,
            &config.owner.scope,
            65_536,
            None,
        )?;
        let case: Scenario = serde_json::from_slice(&std::fs::read(scenario)?)?;
        let overflow = case.writes.len();
        let mut revisions = case.writes.into_iter();
        let first = revisions.next().ok_or("missing initial plan")?;
        publisher
            .stage_once(
                OperationId::new(),
                &first.path,
                first.text.as_bytes(),
                "text/markdown",
                "plan.md",
                None,
            )?
            .await?;
        let workspace = acyclic_harness::filesystem::workspace_ref(
            config.private_volume.provider().clone(),
            &config.private_volume.storage_name(&mut None)?,
        )?;
        native
            .files
            .apply(
                &workspace,
                None,
                &[
                    acyclic_harness::filesystem::WorkspaceMutation::CreateDirectory {
                        path: "/plans/archive.md".into(),
                    },
                    acyclic_harness::filesystem::WorkspaceMutation::PutFile {
                        path: "/plans/archive.md/ignored.txt".into(),
                        bytes: bytes::Bytes::from_static(b"ignored").into(),
                    },
                ],
                &acyclic_harness::IdempotencyKey::new("plan-pty-directory")?,
            )
            .await?;
        let trigger = config.stream_root.join("pty-write");
        let files = native.files.clone();
        let writer = tokio::spawn(async move {
            for (offset, revision) in revisions.enumerate() {
                let index = offset + 1;
                plan_write_trigger(&trigger, index).await?;
                files
                    .apply(
                        &workspace,
                        None,
                        &[
                            acyclic_harness::filesystem::WorkspaceMutation::CreateDirectory {
                                path: format!("/plans/live-{index}.md"),
                            },
                            acyclic_harness::filesystem::WorkspaceMutation::PutFile {
                                path: format!("/plans/live-{index}.md/ignored.txt"),
                                bytes: bytes::Bytes::from_static(b"ignored").into(),
                            },
                        ],
                        &acyclic_harness::IdempotencyKey::new(format!(
                            "plan-pty-live-directory-{index}"
                        ))?,
                    )
                    .await?;
                publisher
                    .stage_once(
                        OperationId::new(),
                        &revision.path,
                        revision.text.as_bytes(),
                        "text/markdown",
                        "plan.md",
                        None,
                    )?
                    .await?;
            }
            for (index, remove) in [(overflow, false), (overflow + 1, true)] {
                plan_write_trigger(&trigger, index).await?;
                let mutations: Vec<_> = (0..crate::terminal::MAX_PAGE_EVENTS)
                    .map(|file| {
                        let path = format!("/plans/limit-{file}.md");
                        if remove {
                            acyclic_harness::filesystem::WorkspaceMutation::Remove { path }
                        } else {
                            acyclic_harness::filesystem::WorkspaceMutation::PutFile {
                                path,
                                bytes: bytes::Bytes::from_static(b"- [ ] Limit").into(),
                            }
                        }
                    })
                    .collect();
                files
                    .apply(
                        &workspace,
                        None,
                        &mutations,
                        &acyclic_harness::IdempotencyKey::new(format!("plan-pty-limit-{index}"))?,
                    )
                    .await?;
                tokio::fs::write(&trigger, format!("{index}-done"))
                    .await
                    .map_err(storage)?;
            }
            Ok::<_, Error>(())
        });
        crate::run_backend(crate::Backend::Native(native), false).await?;
        writer.await??;
    } else {
        let directory = tempfile::tempdir()?;
        let config = configuration(directory.path()).await?;
        let path = directory.path().join("session.json");
        std::fs::write(&path, serde_json::to_vec(&config)?)?;
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/plan_pty.py");
        let status = Command::new("python3")
            .arg(script)
            .arg(std::env::current_exe()?)
            .arg(&path)
            .arg(scenario)
            .arg(config.stream_root.join("pty-write"))
            .status()?;
        assert!(
            status.success(),
            "production binary loop PTY scenario failed"
        );
    }
    Ok(())
}
