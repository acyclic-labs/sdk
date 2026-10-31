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
    AppendRecord, CommitMutation, CommitOutcome, CommitRequest, FollowRequest, FollowUpdate,
    IdempotencyKey, MemoryStream, StreamPath, StreamProvider, StreamProviderExt as _,
    runtime::PlatformExecutor,
    session::{Client, SocketProvider as _, testing},
};
use futures::StreamExt as _;
use std::sync::Arc;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

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

async fn terminal(provider: &MemoryStream, path: StreamPath) -> Result<serde_json::Value> {
    let mut request = FollowRequest::new(path, 0);
    request.when_absent = acyclic_stream::WhenAbsent::Wait;
    let mut events = provider.follow(request).await?;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut chunks = 0;
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
    let (owner, scope, workspace) = super::workspace_fixture().await?;
    let workspace = Arc::new(workspace);
    let provider = Arc::new(MemoryStream::default());
    let owner_actor = ActorReference::new(b"device-owner", b"agents", b"tree")?;
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
    );
    let (failures, mut failed) = tokio::sync::mpsc::unbounded_channel();
    let _runtime = Task(tokio::spawn(async move {
        let outcome = runtime
            .run(|_, error| {
                let _sent = failures.send(error.to_string());
            })
            .await;
        let _sent = failures.send(format!("original Actor runtime ended: {outcome:?}"));
    }));
    let directory = acyclic_harness_core::devices::TargetDirectory::new(
        provider.clone(),
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
    )?;
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
        runner.serve(&attachment, &mut link).await
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
        let marker = tempfile::tempdir()?;
        let marker_path = marker.path().join("executions");
        let path = marker_path
            .to_str()
            .ok_or("marker path is not UTF-8")?
            .replace('\'', "''");
        let command = if cfg!(windows) {
            format!(
                "[IO.File]::AppendAllText('{path}', 'x'); [Console]::Out.Write('out'); [Console]::Error.Write('err'); exit 7"
            )
        } else {
            format!("printf x >> '{path}'; printf out; printf err >&2; exit 7")
        };
        let op = OperationId::new();
        let run = RunCommand {
            op,
            call_id: "original-call".into(),
            agent: agent.clone(),
            actor: *blake3::hash(&actor.encode()).as_bytes(),
            target: "desktop".into(),
            session: session.clone(),
            generation: online.generation,
            deadline_epoch_ms: None,
            arguments: serde_json::json!({"command": command, "target": "desktop"}),
        };
        let grant = owner
            .issuer(&mut None)?
            .issue(
                &scope,
                run.grant_id()?,
                &Capabilities::new(["tool:call:shell"]),
            )?
            .seal(&owner.verifier(&mut None)?)?;
        let bytes = serde_json::to_vec(&Run {
            v: 1,
            command: run,
            grant,
        })?;
        let request = CommitRequest {
            deadline_unix_millis: None,
            conditions: Vec::new(),
            mutations: vec![CommitMutation::Append {
                path: paths.queue("desktop")?,
                records: vec![AppendRecord::from(bytes)],
            }],
            idempotency_key: IdempotencyKey::new(b"original-device-dispatch".to_vec())?,
        };
        assert!(matches!(
            provider.commit(request.clone()).await?,
            CommitOutcome::Committed(_)
        ));
        let result = terminal(&provider, paths.run("desktop", op)?).await?;
        assert_eq!(result["exit"]["result"]["stdout"], "out");
        assert_eq!(result["exit"]["result"]["stderr"], "err");
        assert_eq!(result["exit"]["result"]["exit_code"], 7);
        // Original lost-reply recovery reuses the exact dispatch, never another Run.
        assert!(matches!(
            provider.commit(request).await?,
            CommitOutcome::Committed(_)
        ));
        assert_eq!(std::fs::read(&marker_path)?, b"x");
        assert_eq!(provider.tail(paths.queue("desktop")?).await?, 1);
        let foreign = AgentPath::root(&AgentId::from_bytes([42; 16]));
        assert!(lifecycle.close(&foreign).await.is_err());
        owner.verifier(&mut None)?.verify(&scope, &mut None)?;
        lifecycle.pause(&agent).await?;
        owner.verifier(&mut None)?.verify(&scope, &mut None)?;
        lifecycle.close(&agent).await?;
        assert!(
            owner
                .verifier(&mut None)?
                .verify(&scope, &mut None)
                .is_err()
        );
        Ok::<_, Box<dyn std::error::Error>>(())
    };
    tokio::select! {
        biased;
        error = failed.recv() => Err(error.unwrap_or_else(|| "original Actor runtime producer dropped".into()).into()),
        result = exercise => result,
    }
}
