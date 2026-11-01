//! Real Machine execution over the same authenticated Actor socket and journal.
use acyclic_actors::{
    addressing::ActorReference,
    domain::ActorLimits,
    local::{Runtime, Sockets, Store, SystemClock},
};
use acyclic_auth::contract::{ResourceGrant, ResourcePattern};
use acyclic_harness_core::{
    Capabilities, OperationId,
    agent::{AgentId, AgentLifecycle, AgentPath, TargetState},
    approvals::ApprovalPolicy,
    devices::{
        DeviceAuthority, DeviceGrant, DeviceRunner, Run, RunCommand, TargetActor, TargetComposer,
        TargetPaths, TargetStatus, attach,
    },
    runtime::{InProcessTargets, Target},
};
use acyclic_stream::{
    AppendRecord, CommitMutation, CommitRequest, FollowRequest, FollowUpdate, IdempotencyKey,
    MemoryStream, StreamPath, StreamProvider, StreamProviderExt as _,
    runtime::PlatformExecutor,
    session::{Client, SocketProvider as _, testing},
};
use futures::StreamExt as _;
use std::sync::Arc;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn device_directory_preserves_home_path_reads_and_edits() -> Result {
    use acyclic_harness_core::{
        runtime::{Bindings, RuntimeScope, TargetResolver as _, ToolContext},
        tool::{ToolInvocation, files},
    };
    let storage = acyclic_harness_core::filesystem::MemoryHarnessStorage::new(
        acyclic_harness_core::AgentId::from_bytes([41; 16]),
        4096,
    )
    .await?;
    let (owner, scope, workspace) =
        super::workspace_fixture_for_volume(storage.volume().clone()).await?;
    let provider = Arc::new(MemoryStream::default());
    let sockets = Sockets::open(
        Store::new(provider.clone(), StreamPath::new("harness/actors")?),
        Arc::new(PlatformExecutor),
        Arc::new(SystemClock),
    )
    .await?;
    let directory = Arc::new(acyclic_harness_core::devices::TargetDirectory::new(
        Store::new(provider, StreamPath::new("harness/actors")?),
        Arc::new(sockets),
        TargetPaths::new(
            StreamPath::new("harness/devices/tree")?,
            ActorReference::new(b"account", b"agents", b"tree")?,
        ),
        Target {
            record: InProcessTargets::local_record("home", "ana"),
            executor: Arc::new(workspace),
        },
        DeviceGrant {
            scope,
            issuer: owner.issuer(&mut None)?,
            verifier: owner.verifier(&mut None)?,
        },
    )?);
    // No presence producer or remote socket is required for home paths. The
    // original Machine policy still refuses an unadmitted auxiliary mount.
    assert!(
        directory
            .read_path(".volumes/scratch/file.txt", None)
            .is_err()
    );
    let mut bindings = Bindings::local();
    bindings.targets = Some(directory);
    bindings.scope = RuntimeScope::new(
        Capabilities::default(),
        acyclic_harness_core::conversation::Limits {
            file_bytes: 4096,
            render_bytes: 4096,
            ..Default::default()
        },
    )?;
    let task = Arc::new(bindings.build()?.agent_context(
        storage.owner_scope().capabilities(),
        storage.content_bindings(),
    )?);
    storage
        .stage(
            OperationId::new(),
            "notes.txt",
            b"one",
            "text/plain",
            "notes.txt",
        )
        .await?;
    for (tool, arguments, expected) in [
        (
            files::read()?,
            serde_json::json!({"path":"notes.txt"}),
            Some("one"),
        ),
        (
            files::edit()?,
            serde_json::json!({"path":"notes.txt","old_text":"one","new_text":"two"}),
            None,
        ),
        (
            files::read()?,
            serde_json::json!({"path":"notes.txt"}),
            Some("two"),
        ),
    ] {
        let invocation = ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "home-path".into(),
            name: tool.definition.name.clone(),
            arguments,
        };
        let context = ToolContext::new(task.clone(), invocation.operation_id, &invocation.call_id)?;
        let result = tool.executor.execute(invocation, Some(context)).await?;
        if let Some(expected) = expected {
            assert_eq!(result.value["text"], expected);
        } else {
            let published: acyclic_harness_core::tool::files::FileResult =
                serde_json::from_value(result.value)?;
            assert_eq!(published.file.volume(), storage.volume());
            assert_eq!(
                task.read_file(&published.file, None).await?.as_ref(),
                b"two"
            );
        }
    }
    Ok(())
}

/// The attaching invocation owns this one wall/monotonic anchor and its
/// native timer registrations; command stages never construct another clock.
struct InvocationClock {
    unix: u64,
    monotonic: std::time::Instant,
}

impl acyclic_auth::operation::Clock for InvocationClock {
    fn now_unix_millis(&self) -> std::result::Result<u64, acyclic_auth::operation::Stopped> {
        self.unix
            .checked_add(
                u64::try_from(self.monotonic.elapsed().as_millis())
                    .map_err(|_| acyclic_auth::operation::Stopped::Cancelled)?,
            )
            .ok_or(acyclic_auth::operation::Stopped::Cancelled)
    }

    fn expires_at(
        &self,
        deadline: u64,
    ) -> std::result::Result<acyclic_auth::operation::StopFuture, acyclic_auth::operation::Stopped>
    {
        let at = self
            .monotonic
            .checked_add(std::time::Duration::from_millis(
                deadline.saturating_sub(self.unix),
            ))
            .ok_or(acyclic_auth::operation::Stopped::Cancelled)?;
        Ok(Box::pin(async move {
            tokio::time::sleep_until(at.into()).await;
            acyclic_auth::operation::Stopped::Deadline
        }))
    }
}

struct Task<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn status(
    events: &mut acyclic_stream::FollowStream,
    expected: TargetState,
) -> Result<TargetStatus> {
    while let Some(event) = events.next().await {
        if let FollowUpdate::Record(record) = event? {
            let status: TargetStatus = serde_json::from_slice(&record.value)?;
            if status.target.state == expected {
                return Ok(status);
            }
        }
    }
    Err("original presence producer ended".into())
}

async fn terminal(
    provider: &MemoryStream,
    path: StreamPath,
    after_output: impl AsyncFnOnce() -> Result,
) -> Result<serde_json::Value> {
    let mut request = FollowRequest::new(path, 0);
    request.when_absent = acyclic_stream::WhenAbsent::Wait;
    let mut events = provider.follow(request).await?;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut chunks = 0;
    let mut after_output = Some(after_output);
    while let Some(event) = events.next().await {
        if let FollowUpdate::Record(record) = event? {
            let value: serde_json::Value = serde_json::from_slice(&record.value)?;
            if value["event"].get("started").is_some() {
                continue;
            }
            if let Some(output) = value["event"].get("output") {
                let data: Vec<u8> = serde_json::from_value(output["data"].clone())?;
                assert!(!data.is_empty() && data.len() <= 16 * 1024);
                if output["stderr"] == true {
                    stderr.extend(data);
                } else {
                    stdout.extend(data);
                }
                chunks += 1;
                if chunks == 1 {
                    // The real process cannot exit until its output has crossed
                    // the socket and original operation publication boundary.
                    after_output
                        .take()
                        .ok_or("output gate was already consumed")?()
                    .await?;
                }
                continue;
            }
            let mut event = value["event"].clone();
            if let Some(result) = event
                .get_mut("exit")
                .and_then(|exit| exit.get_mut("result"))
            {
                assert!(
                    chunks > 0,
                    "no intermediate output reached the original operation stream"
                );
                result["stdout"] = String::from_utf8(stdout)?.into();
                result["stderr"] = String::from_utf8(stderr)?.into();
            }
            return Ok(event);
        }
    }
    Err("original operation producer ended".into())
}

#[tokio::test]
async fn committed_device_handover_runs_once_and_archive_fences_its_grant() -> Result {
    for refused_close in [false, true] {
        committed_handover(refused_close).await?;
    }
    Ok(())
}

async fn committed_handover(refused_close: bool) -> Result {
    let (owner, scope, workspace) = super::workspace_fixture().await?;
    let workspace = Arc::new(workspace);
    let provider = Arc::new(MemoryStream::default());
    // The original signed session credential certifies this account; a grant
    // for an Actor namespace does not authorize a different account's devices.
    let owner_actor = ActorReference::new(b"account", b"agents", b"tree")?;
    let paths = TargetPaths::new(StreamPath::new("harness/devices/tree")?, owner_actor);
    let target = InProcessTargets::local_record("desktop", "ana");
    let store = Store::new(provider.clone(), StreamPath::new("harness/actors")?);
    let actor = attach(&store, &paths, target.clone()).await?;
    let sockets = Sockets::open(
        Store::new(provider.clone(), StreamPath::new("harness/actors")?),
        Arc::new(PlatformExecutor),
        Arc::new(SystemClock),
    )
    .await?;
    let runtime = Runtime::new(
        store.with_composer(Arc::new(TargetComposer::new(Arc::new(sockets.clone())))),
        Arc::new(TargetActor::new(
            provider.clone(),
            ActorLimits::new(1000, 1024 * 1024, 1024 * 1024)?,
        )?),
        Arc::new(SystemClock),
    )
    .claiming(&actor);
    let (failures, mut failed) = tokio::sync::mpsc::unbounded_channel();
    let runtime_failures = failures.clone();
    let _runtime = Task(tokio::spawn(async move {
        let outcome = runtime
            .run(|_, error| {
                let _sent = runtime_failures.send(error.to_string());
            })
            .await;
        let _sent = runtime_failures.send(format!("original Actor runtime ended: {outcome:?}"));
    }));
    let directory = Arc::new(acyclic_harness_core::devices::TargetDirectory::new(
        Store::new(provider.clone(), StreamPath::new("harness/actors")?),
        Arc::new(sockets.clone()),
        paths.clone(),
        Target {
            record: InProcessTargets::local_record("home", "ana"),
            executor: workspace.clone(),
        },
        DeviceGrant {
            scope: scope.clone(),
            issuer: owner.issuer(&mut None)?,
            verifier: owner.verifier(&mut None)?,
        },
    )?);
    let lifecycle = directory.lifecycle(owner.clone(), None)?;
    let (_invocation, invocation_lost) = tokio::sync::oneshot::channel::<()>();
    let clock = InvocationClock {
        unix: acyclic_stream::UnixMillisClock::now_unix_millis(
            &acyclic_stream::SystemUnixMillisClock,
        ),
        monotonic: std::time::Instant::now(),
    };
    let runner = Arc::new(DeviceRunner::new(
        paths.clone(),
        target,
        workspace.clone(),
        DeviceAuthority {
            verifier: owner.verifier(&mut None)?,
            context: acyclic_auth::operation::Context::from_clock(
                u64::MAX,
                async move {
                    let _ended = invocation_lost.await;
                    acyclic_auth::operation::Stopped::Cancelled
                },
                Arc::new(clock),
            ),
        },
        ApprovalPolicy::accept_all("ana"),
    ));
    let attachment = runner.attach()?;
    let client = Client::in_process_with_sockets(
        provider.clone(),
        testing::authority(),
        None,
        Arc::new(sockets.clone()),
        Arc::new(PlatformExecutor),
        None,
    );
    client.register_credential(
        0,
        testing::credential_with_grants(vec![ResourceGrant {
            resource: ResourcePattern {
                name: sockets.resource(&attachment.open())?,
                prefix: false,
            },
            actions: vec!["actors/invoke".into()],
        }]),
    )?;
    let mut link = client.actor_socket(attachment.open(), 0).await?;
    let _device = Task(tokio::spawn(async move {
        let outcome = runner.serve(&attachment, &mut link).await;
        if !refused_close {
            let _sent = failures.send(format!("original device connection ended: {outcome:?}"));
        }
        outcome
    }));
    let exercise = async {
        let mut presence = provider
            .follow(FollowRequest::new(paths.presence()?, 0))
            .await?;
        let online = status(&mut presence, TargetState::Online).await?;
        let session = online
            .session
            .ok_or("online device has no original session")?;
        let agent = AgentPath::root(&AgentId::from_bytes([41; 16]));
        // Advance the original in-memory follow once through its published
        // prefix. No readiness loop or second presence producer is involved.
        let mut following = Box::pin(directory.follow());
        assert!(futures::poll!(following.as_mut()).is_pending());
        use acyclic_harness_core::runtime::TargetResolver as _;
        let remote = directory.resolve(Some("desktop"))?;
        let storage = acyclic_harness_core::filesystem::MemoryHarnessStorage::with_stream(
            acyclic_harness_core::AgentId::from_bytes(agent.id().to_bytes()),
            4096,
            provider.clone(),
        )
        .await?;
        let mut bindings = acyclic_harness_core::runtime::Bindings::local();
        bindings.targets = Some(directory.clone());
        bindings.scope = acyclic_harness_core::runtime::RuntimeScope::new(
            scope.capabilities().clone(),
            acyclic_harness_core::conversation::Limits {
                file_bytes: 2,
                render_bytes: 4,
                ..Default::default()
            },
        )?;
        let task = Arc::new(
            bindings
                .build()?
                .agent_context(
                    storage.owner_scope().capabilities(),
                    storage.content_bindings(),
                )?
                .for_agent(
                    agent.clone(),
                    vec![acyclic_harness_core::agent::VolumeBinding {
                        name: "project".into(),
                        volume: workspace.selected_volume().clone(),
                    }],
                ),
        );
        let oversized = acyclic_harness_core::tool::ToolInvocation {
            operation_id: OperationId::new(),
            call_id: "rejected-before-dispatch".into(),
            name: "shell".into(),
            arguments: serde_json::json!({"command": "exit 0", "target": "desktop"}),
        };
        let context = acyclic_harness_core::runtime::ToolContext::new(
            task.clone(),
            oversized.operation_id,
            &oversized.call_id,
        )?;
        // The live remote producer accepts this call absent shared operand
        // admission. Its dispatch is only a plan; it is never published here.
        assert!(
            remote
                .executor
                .dispatch(&oversized, Some(&context))?
                .is_some()
        );
        let registry = acyclic_harness_core::tools::defaults()?;
        let shell = &registry
            .get("shell", None, None)?
            .ok_or("shell is missing")?
            .executor;
        assert!(shell.dispatch(&oversized, Some(&context))?.is_none());
        let mut recovery = shell.reconcile(oversized.clone(), Some(context.clone()));
        let std::task::Poll::Ready(recovered) = futures::poll!(recovery.as_mut()) else {
            return Err("rejected command recovery waited for an unpublished Run".into());
        };
        // This standalone public context has no durable selected target. Only
        // Stock's retained claim may bind one during replay; direct recovery
        // must refuse without resolving the current roster or waiting on Run.
        assert!(matches!(
            recovered,
            Err(acyclic_harness_core::Error::Indeterminate(id)) if id == oversized.operation_id
        ));
        let refusal = shell.execute(oversized, Some(context)).await?;
        assert!(
            refusal.value["error"]
                .as_str()
                .is_some_and(|error| error.contains("command exceeds"))
        );
        assert!(matches!(
            provider.tail(paths.queue("desktop")?).await,
            Ok(0) | Err(acyclic_stream::StreamError::NotFound)
        ));
        for arguments in [
            serde_json::json!({"target": "desktop"}),
            serde_json::json!({"command": ""}),
        ] {
            let invalid = acyclic_harness_core::tool::ToolInvocation {
                operation_id: OperationId::new(),
                call_id: "malformed-before-dispatch".into(),
                name: "shell".into(),
                arguments,
            };
            let context = acyclic_harness_core::runtime::ToolContext::new(
                task.clone(),
                invalid.operation_id,
                &invalid.call_id,
            )?;
            assert!(
                remote
                    .executor
                    .dispatch(&invalid, Some(&context))?
                    .is_none()
            );
            let mut recovery = remote
                .executor
                .reconcile(invalid.clone(), Some(context.clone()));
            let std::task::Poll::Ready(recovered) = futures::poll!(recovery.as_mut()) else {
                return Err("malformed command recovery waited for an unpublished Run".into());
            };
            let recovered = recovered
                .err()
                .ok_or("malformed recovery accepted the call")?;
            let executed = remote
                .executor
                .execute(invalid, Some(context))
                .await
                .err()
                .ok_or("malformed execution accepted the call")?;
            assert_eq!(recovered.to_string(), executed.to_string());
        }
        let marker = tempfile::tempdir()?;
        let marker_path = marker.path().join("executions");
        let path = marker_path
            .to_str()
            .ok_or("marker path is not UTF-8")?
            .replace('\'', "''");
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let port = listener.local_addr()?.port();
        let command = if cfg!(windows) {
            format!(
                "$peer = [Net.Sockets.TcpClient]::new('127.0.0.1', {port}); [IO.File]::AppendAllText('{path}', 'x'); [Console]::Out.Write('out'); [Console]::Error.Write('err'); $null = $peer.GetStream().ReadByte(); $peer.Dispose(); exit 7"
            )
        } else {
            format!(
                "exec 3<>/dev/tcp/127.0.0.1/{port}; printf x >> '{path}'; printf out; printf err >&2; IFS= read -r permit <&3; exit 7"
            )
        };
        let op = OperationId::new();
        let run = RunCommand {
            op,
            call_id: "original-call".into(),
            agent: agent.clone(),
            volume: workspace.selected_volume().clone(),
            actor: *blake3::hash(&actor.encode()).as_bytes(),
            target: "desktop".into(),
            session: session.clone(),
            generation: online.generation,
            deadline_epoch_ms: None,
            arguments: serde_json::json!({"command": command, "target": "desktop"}),
        };
        let dispatch = |command: RunCommand, key: &[u8]| -> Result<CommitRequest> {
            let grant = owner
                .issuer(&mut None)?
                .issue(
                    &scope,
                    command.grant_id()?,
                    &Capabilities::new(["tool:call:shell"]),
                )?
                .seal(&owner.verifier(&mut None)?)?;
            let bytes = serde_json::to_vec(&Run {
                v: 1,
                command,
                grant,
            })?;
            Ok(CommitRequest {
                deadline_unix_millis: None,
                conditions: Vec::new(),
                mutations: vec![
                    CommitMutation::Append {
                        path: paths.queue("desktop")?,
                        records: vec![AppendRecord::from(bytes)],
                    },
                    Store::new(provider.clone(), StreamPath::new("harness/actors")?)
                        .activation(&actor)
                        .mutation,
                ],
                idempotency_key: IdempotencyKey::new(key.to_vec())?,
            })
        };
        let request = dispatch(run.clone(), b"original-device-dispatch")?;
        let contributed = acyclic_harness_core::tool::Dispatch {
            delivery: (
                paths.queue("desktop")?,
                match &request.mutations[0] {
                    CommitMutation::Append { records, .. } => records[0].value.clone().into(),
                    _ => return Err("missing original delivery".into()),
                },
            ),
            activation: Store::new(provider.clone(), StreamPath::new("harness/actors")?)
                .activation_record(&actor),
        };
        let journal = storage.journal();
        journal
            .append(
                op,
                "start",
                &acyclic_harness_core::executor::ExecutionEvent::Started {
                    request_digest: [1; 32],
                },
                None,
            )?
            .await?;
        let reference = journal
            .stage(op, "invocation", b"{}", "application/json", None)?
            .await?;
        let mut started = acyclic_harness_core::executor::ExecutionEvent::ToolStarted {
            step: 0,
            call_id: run.call_id.clone(),
            invocation: reference,
            publication: None,
        };
        assert!(
            journal
                .dispatch_if_tail(
                    op,
                    1,
                    "original-device-dispatch",
                    &mut started,
                    &contributed,
                    None
                )?
                .await
                .map_err(|error| format!("atomic start/run/wake claim for {op}: {error}"))?
        );
        let admitted = journal.replay(op, 0, 3, None)?.await?;
        assert_eq!(admitted.len(), 2);
        assert!(matches!(
            &admitted[1].event,
            acyclic_harness_core::executor::ExecutionEvent::ToolStarted {
                publication: Some(_),
                ..
            }
        ));
        let (mut release, _) = listener.accept().await?;
        let result = terminal(&provider, paths.run("desktop", op)?, async || {
            use tokio::io::AsyncWriteExt as _;
            release.write_all(b"continue\n").await?;
            Ok(())
        })
        .await?;
        assert_eq!(result["exit"]["result"]["stdout"], "out");
        assert_eq!(result["exit"]["result"]["stderr"], "err");
        assert_eq!(result["exit"]["result"]["exit_code"], 7);
        // Original lost-reply recovery reuses the exact dispatch, never another Run.
        assert!(
            journal
                .dispatch_if_tail(
                    op,
                    1,
                    "original-device-dispatch",
                    &mut started,
                    &contributed,
                    None
                )?
                .await?
        );
        let mut changed = contributed.clone();
        changed.delivery.1 = acyclic_stream::OwnedBytes::from_static(b"substituted run");
        assert!(
            journal
                .dispatch_if_tail(
                    op,
                    1,
                    "original-device-dispatch",
                    &mut started,
                    &changed,
                    None
                )?
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&marker_path)?, b"x");
        assert_eq!(provider.tail(paths.queue("desktop")?).await?, 1);

        // Even another authorized project cannot replace the original mounted
        // selection. The real command is harmless but would leave evidence.
        let mut foreign_run = run.clone();
        foreign_run.op = OperationId::new();
        foreign_run.volume = acyclic_harness_core::conversation::VolumeRef::new(
            run.volume.provider().clone(),
            "other-project".into(),
            acyclic_harness_core::conversation::VolumeClass::Project,
            acyclic_harness_core::conversation::VolumeOwner::Project("other-project".into()),
            &mut None,
        )?;
        foreign_run.arguments["command"] = if cfg!(windows) {
            format!("[IO.File]::AppendAllText('{path}', 'wrong-volume')")
        } else {
            format!("printf wrong-volume >> '{path}'")
        }
        .into();
        let foreign_op = foreign_run.op;
        provider
            .commit(dispatch(foreign_run, b"foreign-volume")?)
            .await?;
        let refusal = terminal(&provider, paths.run("desktop", foreign_op)?, async || {
            Err("foreign volume reached the process output boundary".into())
        })
        .await?;
        assert!(
            refusal.get("refused").is_some(),
            "foreign volume ran: {refusal}"
        );
        assert_eq!(std::fs::read(&marker_path)?, b"x");

        let foreign = AgentPath::root(&AgentId::from_bytes([42; 16]));
        assert!(lifecycle.close(&foreign).await.is_err());
        owner.verifier(&mut None)?.verify(&scope, &mut None)?;
        lifecycle.pause(&agent).await?;
        owner.verifier(&mut None)?.verify(&scope, &mut None)?;

        // Archive while the same real child is blocked. Its original trust
        // event interrupts the command, and native custody closes the child's
        // held descriptor; revocation is not an assertion that it already died.
        let mut active = run.clone();
        active.op = OperationId::new();
        let active_op = active.op;
        provider
            .commit(dispatch(active, b"archive-active-command")?)
            .await?;
        let (mut held, _) = listener.accept().await?;
        if refused_close {
            let mut original = provider
                .follow(FollowRequest::new(paths.run("desktop", active_op)?, 0))
                .await?;
            loop {
                let FollowUpdate::Record(record) = original
                    .next()
                    .await
                    .ok_or("original run producer ended")??
                else {
                    continue;
                };
                let value: serde_json::Value = serde_json::from_slice(&record.value)?;
                if value["event"].get("output").is_some() {
                    break;
                }
            }
            // Refuse the actual original Close with a real competing tail.
            // The run and session follows stay healthy and have no Lost marker.
            let connection = online.socket.as_ref().ok_or("missing original socket")?;
            let socket_path = StreamPath::new(format!(
                "harness/actors/_sockets/v1/s/{}",
                acyclic_auth::encode_component(connection.as_bytes())
            ))?;
            let tail = provider.head(socket_path.clone()).await?.tail;
            provider
                .append(acyclic_stream::AppendRequest {
                    path: socket_path,
                    records: vec![
                        acyclic_stream::OwnedBytes::from_static(b"competing close").into(),
                    ],
                    if_tail: Some(tail),
                    idempotency_key: IdempotencyKey::new(b"refuse-original-close".to_vec())?,
                })
                .await?;
            assert!(
                sockets.drain().await.is_err(),
                "the original close unexpectedly committed"
            );
            assert!(matches!(
                provider
                    .head(paths.session_end("desktop", online.generation)?)
                    .await,
                Err(acyclic_stream::StreamError::NotFound)
            ));
        } else {
            let lost = terminal(&provider, paths.run("desktop", active_op)?, async || {
                lifecycle.close(&agent).await?;
                Ok(())
            })
            .await?;
            assert!(
                lost.get("lost").is_some(),
                "interrupted command settled as {lost}"
            );
        }
        use tokio::io::AsyncReadExt as _;
        assert_eq!(
            held.read_u8().await.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        assert_eq!(std::fs::read(&marker_path)?, b"xx");
        assert_eq!(
            owner
                .verifier(&mut None)?
                .verify(&scope, &mut None)
                .is_err(),
            !refused_close
        );
        // Consume the original connection's committed end before recovery.
        // The failed command's output still belongs to its accepted prefix.
        if !refused_close {
            let mut ended_request =
                FollowRequest::new(paths.session_end("desktop", online.generation)?, 0);
            ended_request.when_absent = acyclic_stream::WhenAbsent::Wait;
            let mut ended = provider.follow(ended_request).await?;
            drop(_device);
            loop {
                let event = ended
                    .next()
                    .await
                    .ok_or("original session-end producer dropped")??;
                if matches!(event, FollowUpdate::Record(_)) {
                    break;
                }
            }
        }
        // A delayed presence follow cannot turn original socket loss into a
        // fatal validation error, even while its retained roster says online.
        assert_eq!(
            directory.roster().select(Some("desktop"))?.state,
            TargetState::Online
        );
        assert!(matches!(
            directory.resolve(Some("desktop")),
            Err(acyclic_harness_core::Error::Target(
                acyclic_harness_core::agent::TargetRefusal {
                    refused: acyclic_harness_core::agent::Refused::Offline,
                    ..
                }
            ))
        ));
        // Lose the roster producer as well as the original device. Recovery
        // opens the exact saved target without a live connection; fresh calls
        // still refuse it. Never reuse the executor retained while online.
        drop(following);
        assert!(matches!(
            directory.resolve(Some("desktop")),
            Err(acyclic_harness_core::Error::Target(
                acyclic_harness_core::agent::TargetRefusal {
                    refused: acyclic_harness_core::agent::Refused::Offline,
                    ..
                }
            ))
        ));
        assert!(directory.recover("user:ana").is_err());
        assert!(directory.recover("absent").is_err());
        let recovery_target = directory.recover("desktop")?;
        assert_eq!(recovery_target.record.state, TargetState::Offline);
        for (operation, expected_exit) in [(op, Some(7)), (active_op, None)] {
            let invocation = acyclic_harness_core::tool::ToolInvocation {
                operation_id: operation,
                call_id: "original-call".into(),
                name: "shell".into(),
                arguments: run.arguments.clone(),
            };
            let context = acyclic_harness_core::runtime::ToolContext::new(
                task.clone(),
                operation,
                &invocation.call_id,
            )?;
            // The original socket completion settles observation. Publishing
            // its accepted output files may still yield to their own producer.
            assert!(
                recovery_target
                    .executor
                    .dispatch(&invocation, Some(&context))
                    .is_err()
            );
            let recovered = recovery_target
                .executor
                .reconcile(invocation.clone(), Some(context))
                .await;
            if let Some(exit) = expected_exit {
                let result = recovered
                    .map_err(|error| format!("committed Exit recovery for {operation}: {error}"))?
                    .ok_or("committed Exit disappeared during recovery")?;
                assert_eq!(result.value["exit_code"], exit);
                assert_eq!(result.value["omitted_bytes"], 2);
                let bounded = acyclic_harness_core::tools::shell::bounded_result(result, 2048)?;
                assert_eq!(bounded.value["truncated"], true);
                assert_eq!(bounded.value["omitted_bytes"], 2);
                let shell_tool = registry
                    .get("shell", None, None)?
                    .ok_or("shell is missing")?;
                let (projected, _) = shell_tool.projection.project(&invocation, &bounded, None)?;
                let projected = serde_json::to_string(&projected)?;
                assert!(projected.len() <= 2048);
                assert!(projected.contains("2 bytes omitted"));
                assert!(projected.contains(&format!("tools/{operation}/device")));
                assert!(projected.contains(storage.volume().id()));
            } else {
                assert!(
                    matches!(recovered, Err(acyclic_harness_core::Error::Indeterminate(id)) if id == operation)
                );
            }
            let mut records = provider
                .follow(FollowRequest::new(paths.run("desktop", operation)?, 0))
                .await?;
            let mut chunks = 0;
            let mut split_chunks = 0;
            let tail = provider.head(paths.run("desktop", operation)?).await?.tail;
            loop {
                let event = records.next().await.ok_or("original run prefix ended")??;
                let FollowUpdate::Record(record) = event else {
                    continue;
                };
                let value: serde_json::Value = serde_json::from_slice(&record.value)?;
                if let Some(output) = value["event"].get("output") {
                    let channel = if output["stderr"] == true {
                        "stderr"
                    } else {
                        "stdout"
                    };
                    let original: Vec<u8> = serde_json::from_value(output["data"].clone())?;
                    split_chunks += usize::from(original.len() > 2);
                    let mut restored = Vec::new();
                    for (index, piece) in original.chunks(2).enumerate() {
                        let offset = index * 2;
                        let path = format!(
                            "tools/{operation}/device/{:020}-{offset:020}-{channel}.bin",
                            record.sequence
                        );
                        let (_, bytes, _) = storage
                            .content_reader()
                            .read_private_path(
                                storage.volume(),
                                "",
                                &path,
                                None,
                                2,
                                acyclic_harness_core::conversation::ContentAccess::default(),
                            )?
                            .await?;
                        assert_eq!(bytes.as_ref(), piece);
                        restored.extend_from_slice(&bytes);
                    }
                    assert_eq!(restored, original);
                    chunks += 1;
                } else if value["event"].get("started").is_none() {
                    break;
                }
                if record.sequence + 1 == tail {
                    break;
                }
            }
            assert!(chunks > 0, "real command produced no retained output");
            assert!(
                split_chunks > 0,
                "real output did not exceed the original file limit"
            );
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    tokio::select! {
        biased;
        error = failed.recv() => Err(error.unwrap_or_else(|| "original Actor runtime producer dropped".into()).into()),
        result = exercise => result,
    }
}
