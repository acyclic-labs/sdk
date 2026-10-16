use super::*;
use acyclic_harness::{
    AgentId, Capabilities,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner},
};
use std::{path::Path, time::Duration};

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
        command_workspace: None,
        ui_session: None,
        command_roots: Vec::new(),
        skill_roots: Vec::new(),
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

#[tokio::test]
async fn command_requests_and_sdk_controls_share_the_native_runtime_without_a_checkout()
-> Result<()> {
    use acyclic_harness::client::{CommandRequest, CommandResolution, CommandTaskScope};
    let directory = tempfile::tempdir().map_err(storage)?;
    let mut config = configuration(directory.path()).await?;
    config.deterministic_delay_ms = 0;
    let native = NativeSession::open(config).await?;
    let queued = native
        .owner
        .dispatch_command(
            OperationId::new(),
            None,
            CommandResolution::Prompt {
                text: "shared task".into(),
                source: None,
                mentions: Vec::new(),
            },
        )
        .await?;
    let task = TaskId::parse(
        queued["task"]
            .as_str()
            .ok_or_else(|| invalid("missing task"))?,
    )?;
    let admission = native.runtime.task_host().observe_admission(task).await?;
    assert_eq!(admission.task.name, "acyclic.stock_turn");
    for (request, expected) in [
        (
            CommandRequest::Pause {
                scope: CommandTaskScope::Subtree,
            },
            true,
        ),
        (
            CommandRequest::Resume {
                scope: CommandTaskScope::Subtree,
            },
            false,
        ),
    ] {
        let result = native
            .owner
            .dispatch_command(
                OperationId::new(),
                Some(task),
                CommandResolution::Submit { request },
            )
            .await?;
        assert_eq!(result["paused"], expected);
    }
    let worker = Worker {
        id: "shared-owner".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let WorkPull::Claimed(lease) = native.claim(task, &worker).await? else {
        return Err(invalid("SDK command could not claim the request-file task"));
    };
    native.resume(lease).await?;
    assert!(matches!(
        native.runtime.task_host().outcome(task).await?,
        Some(acyclic_harness::Outcome::Succeeded(_))
    ));
    Ok(())
}

#[tokio::test]
async fn command_owner_reads_settings_written_by_the_native_client() -> Result<()> {
    use acyclic_harness::client::{CommandFileRoot, CommandSession, consume_local_command};
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    struct NoReads;
    impl acyclic_harness::conversation::ContentResidencyVerifier for NoReads {}
    let directory = tempfile::tempdir().map_err(storage)?;
    let native = NativeSession::open(configuration(directory.path()).await?).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(storage)?;
    let endpoint = format!("http://{}/find", listener.local_addr().map_err(storage)?);
    let server = tokio::spawn(async move {
        let (mut connection, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .map_err(storage)?
            .map_err(storage)?;
        let mut request = [0; 4096];
        let count = connection.read(&mut request).await.map_err(storage)?;
        assert!(
            std::str::from_utf8(&request[..count])
                .map_err(storage)?
                .starts_with("GET /find?q=needle ")
        );
        connection
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nfound")
            .await
            .map_err(storage)?;
        Ok::<_, Error>(())
    });
    // Reopen through the same provider/path as the production client, rather
    // than borrowing the owner's disposable in-memory index.
    let identity = serde_json::to_vec(&native.config.private_volume).map_err(storage)?;
    let volume = Arc::new(
        acyclic_harness::task_volume::TaskVolume::open(
            native.stream.clone(),
            acyclic_stream::StreamPath::new(format!(
                "graphcoder-files/{}",
                blake3::hash(&identity).to_hex()
            ))
            .map_err(storage)?,
        )
        .await?,
    );
    let mut client = CommandSession::open(volume, uuid::Uuid::new_v4(), 65_536, 64).await?;
    client
        .write_state(
            CommandFileRoot::Config,
            "settings.json",
            &serde_json::json!({"search_endpoint":endpoint}),
        )
        .await?;
    client.submit("/search needle", &NoReads).await?;
    let request = client.next_request().await?;
    let result = consume_local_command(&mut client, &native.owner, &request).await;
    server.await.map_err(storage)??;
    assert_eq!(
        result?.ok_or_else(|| invalid("unclaimed request"))?["stdout"],
        "found"
    );
    Ok(())
}

#[tokio::test]
async fn default_manifest_survives_restart_of_an_unfinished_native_turn() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let config = configuration(directory.path()).await?;
    let first = NativeSession::open(config.clone()).await?;
    let contracts = first.context.contracts(None)?;
    let command = first
        .prompt_command("keep the plan in plans/release.md", OperationId::new())
        .await?;
    let task = match first.queue(OperationId::new(), command, None).await? {
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => task_id,
        _ => panic!("expected an admitted durable task"),
    };
    let WorkPull::Claimed(lease) = first
        .claim(
            task,
            &Worker {
                id: "restart".into(),
                available: Default::default(),
                labels: Default::default(),
            },
        )
        .await?
    else {
        panic!("expected the original lease")
    };
    {
        let running = first.resume(lease);
        tokio::pin!(running);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    outcome = &mut running => panic!("turn completed before its delayed model: {outcome:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {},
                }
                if std::fs::read_to_string(&config.model_invocation_ledger).map_err(storage)?.contains("\"phase\":\"generate\"") { break; }
            }
            Ok::<_, Error>(())
        }).await.expect("model dispatch was never reached")?;
    }
    drop(first);
    let reopened = NativeSession::open(config.clone()).await?;
    assert_eq!(reopened.context.contracts(None)?, contracts);
    let WorkPull::Claimed(lease) = reopened
        .runtime
        .task_host()
        .recover_work(task, &mut None)
        .await?
    else {
        panic!("original lease was not retained")
    };
    let outcome = reopened.resume(lease).await?;
    assert!(outcome.starts_with("worker completed"), "{outcome}");
    let records: Vec<serde_json::Value> = std::fs::read_to_string(&config.model_invocation_ledger)
        .map_err(storage)?
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()
        .map_err(invalid)?;
    assert_eq!(
        records
            .iter()
            .map(|record| record["phase"].as_str().expect("phase"))
            .collect::<Vec<_>>(),
        ["generate", "reconcile"]
    );
    assert_eq!(records[0]["request_digest"], records[1]["request_digest"]);
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
        crate::command_line::run(crate::Backend::Native(native), false).await?;
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
