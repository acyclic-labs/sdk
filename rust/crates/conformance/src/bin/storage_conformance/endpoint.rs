//! Real public server/client composition, separate from the unchanged host vectors.
use super::Domain;
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_agent_host_conformance::Result;
use acyclic_harness::{
    Capabilities,
    agent::{AgentId, AgentPath, IntentId, Reply, VolumeBinding},
    context::CompactionPolicy,
    contract::capability::INTERACTION_ROUTE,
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    filesystem::{Grants, PRIVATE_VOLUME, TurnLimits},
    hosting::{Bound, Placement, RootRequest, Serving, Turns, VerifiedCaller},
    local::{Connection, Event, agent_conversation::Conversation},
    model::{Model, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest},
    preset::Preset,
    resources::ProviderRef,
    server::{Hosted, ServerConfig},
    tool::ToolRegistry,
};
use acyclic_harness_client::{AgentCommand, ConnectionOptions};
use acyclic_objects::{
    Blobs, MemoryObjects, Store,
    s3::{Capabilities as S3Capabilities, S3Medium},
};
use acyclic_stream::BoxProviderStream;
use serde_json::Value;
use std::{sync::Arc, time::Duration};

/// A deterministic in-process model, not a captured external-provider fixture.
struct Answer;
impl ModelProvider for Answer {
    fn generate<'a>(
        &'a self,
        _: &'a PreparedModelRequest,
        _: ModelDispatch,
    ) -> BoxProviderStream<'a, acyclic_harness::Result<ModelEvent>> {
        Box::pin(futures::stream::iter([
            Ok(ModelEvent::Content {
                delta: "stored answer".into(),
            }),
            Ok(ModelEvent::Completed {
                metadata: Value::Null,
            }),
        ]))
    }
}

struct Owner<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for Owner<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct StopOnDrop(tokio::task::AbortHandle);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The fixture owns the disposable tree and admits its own endpoint credential.
struct User;
impl acyclic_harness::hosting::Platform for User {
    fn policy<'a>(
        &'a self,
        _: &'a acyclic_auth::contract::Principal,
        _: &'a AgentCommand,
    ) -> acyclic_stream::BoxProviderFuture<
        'a,
        std::result::Result<acyclic_auth::authority::PolicyDecision, acyclic_stream::StreamError>,
    > {
        Box::pin(async { Ok(acyclic_auth::authority::PolicyDecision::Allow) })
    }
}

fn checked<T>(
    phase: usize,
    step: &str,
    result: std::result::Result<T, impl std::fmt::Display>,
) -> Result<T> {
    result.map_err(|error| format!("step=endpoint-{phase}-{step}: {error}; recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/backends/storage-conformance.sh").into())
}

pub(super) async fn verify<P: acyclic_stream::NativeStreamProvider + 'static>(
    domain: &Domain,
    reopen: impl AsyncFn() -> Result<Arc<P>>,
) -> Result {
    match &domain.providers.objects {
        Some(store) => {
            let prefix = format!("agent-host/{}", domain.namespace);
            verify_pair(
                domain,
                reopen,
                async || {
                    Ok(Arc::new(
                        Store::new(S3Medium::new(
                            store.clone(),
                            &prefix,
                            S3Capabilities::REQUIRED,
                        )?)
                        .account(b"endpoint")?,
                    ))
                },
                acyclic_fs::EmbeddedCapabilities { durable: true },
            )
            .await
        }
        None => {
            let provider = Arc::new(MemoryObjects::memory());
            verify_pair(
                domain,
                reopen,
                async || Ok(provider.clone()),
                acyclic_fs::EmbeddedCapabilities::MEMORY,
            )
            .await
        }
    }
}

async fn verify_pair<P: acyclic_stream::NativeStreamProvider + 'static, Q: Blobs + 'static>(
    domain: &Domain,
    reopen: impl AsyncFn() -> Result<Arc<P>>,
    reopen_objects: impl AsyncFn() -> Result<Arc<Q>>,
    capabilities: acyclic_fs::EmbeddedCapabilities,
) -> Result {
    let mut saved = None;
    for phase in 0..2 {
        let objects = acyclic_fs::staged_objects::StagedObjects::open(
            acyclic_fs::CachedObjectStore::new(
                acyclic_fs::LogicalObjectStore::new(reopen_objects().await?),
                acyclic_fs::ObjectCacheOptions::default(),
            )?,
            domain.directory.path().join("staging"),
        );
        let stream = reopen().await?;
        let provider_ref = ProviderRef::new(
            "endpoint".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?;
        let id = AgentId::from_bytes([31; 16]);
        let private = VolumeRef::new(
            provider_ref.clone(),
            "endpoint-private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(acyclic_harness::AgentId::from_bytes(id.to_bytes())),
            &mut None,
        )?;
        let audience = Authority {
            kind: AggregateKind::Conversation,
            id: "endpoint".into(),
        };
        let issuer = AuthorityRoot::new("conformance", [7; 32], audience.clone());
        let tree =
            acyclic_fs::access::Capability::whole_volume(None, &acyclic_fs::access::Action::ALL)
                .to_scope_label()?;
        let scope = issuer.bootstrap_scope(
            "root",
            &Capabilities::new([INTERACTION_ROUTE, tree.as_str()]),
            None,
            &mut None,
        )?;
        let mut preset = Preset::graphcoder();
        preset.tools.clear();
        let limits = Limits::default();
        let turns = Turns {
            executors: Arc::new(preset.executors(
                (
                    Model::new(
                        "conformance".into(),
                        "answer".into(),
                        "1".into(),
                        Value::Null,
                    )?,
                    Arc::new(Answer),
                ),
                ToolRegistry::default(),
                (limits, CompactionPolicy::Disabled),
                Arc::new(Grants(Vec::new())),
            )),
            limits: TurnLimits {
                maximum_bytes: 64 * 1024,
                max_steps: 4,
                history_turns: 8,
            },
            actors: ActorLimits::new(1_000, 1 << 20, 1 << 16)?,
            clock: Arc::new(SystemClock),
            approval_policy: preset.approval_policy,
        };
        let bound = Bound::admitted(
            issuer,
            VerifiedCaller { audience, scope },
            Placement {
                filesystem: acyclic_fs::Fs::new(
                    acyclic_fs::StreamAuthorityStore::new(stream),
                    objects,
                    capabilities,
                ),
                provider: provider_ref,
                within: ActorReference::new(b"conformance", b"endpoint", b"tree")?,
            },
            turns,
        )?;
        let server = if phase == 0 {
            match acyclic_harness::hosting::serve(
                bound,
                Arc::new(User),
                &IntentId::try_from("root")?,
                RootRequest {
                    profile: None,
                    agent: id,
                    fresh: vec![VolumeBinding {
                        name: PRIVATE_VOLUME.into(),
                        volume: private,
                    }],
                    retained: Vec::new(),
                    brief: None,
                    policy: None,
                },
            )
            .await.map_err(|error| format!("step=endpoint-{phase}-create-root: {error}; recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/backends/storage-conformance.sh"))?
            {
                Serving::Served(server) => server,
                Serving::Unknown(_) => return Err("endpoint root unexpectedly uncertain".into()),
            }
        } else {
            checked(
                phase,
                "reopen-root",
                acyclic_harness::hosting::reopen(bound, Arc::new(User), AgentPath::root(&id)).await,
            )?
        };
        let runner = server.clone();
        let mut runtime = Owner(tokio::spawn(async move {
            runner
                .run(|actor, error| eprintln!("endpoint fixture actor {actor:?}: {error}"))
                .await
        }));
        let host = Hosted::new(
            server,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            StopOnDrop(runtime.0.abort_handle()),
            None,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:15402").await?;
        let (advertise, advertised) = tokio::sync::oneshot::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let mut serving = Owner(tokio::spawn(async move {
            acyclic_harness::server::serve(
                listener,
                ServerConfig::new(host),
                move |endpoint| {
                    assert_eq!(endpoint.server, acyclic_harness::server::IDENTITY);
                    let credential = endpoint
                        .observation
                        .as_ref()
                        .map(|minted| minted.credential.clone());
                    let _ = advertise.send((endpoint.endpoint.clone(), credential));
                },
                async move {
                    let _ = stopped.await;
                },
            )
            .await
        }));
        let (endpoint, credential) = match tokio::time::timeout(Duration::from_secs(10), advertised)
            .await?
        {
            Ok(endpoint) => endpoint,
            Err(_) => {
                let result = (&mut serving.0).await?;
                return Err(format!(
                        "step=endpoint-ready: server exited before readiness: {result:?}; recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/backends/storage-conformance.sh"
                    ).into());
            }
        };
        let credential = credential
            .ok_or("step=endpoint-ready: the key-file verifier published no credential")?;
        let options = ConnectionOptions::from_env(Some(&endpoint), Some(&credential))?;
        let observed = async {
            let connection = checked(phase, "connect", Connection::connect(&options).await)?;
            let conversation = checked(phase, "conversation", Conversation::open(connection))?;
            let mut observer = checked(phase, "observe", conversation.observe(0).await)?;
            let inbox = checked(phase, "send", conversation.send("once", "stored prompt").await)?;
            assert_eq!(checked(phase, "retry-send", conversation.send("once", "stored prompt").await)?, inbox);
            let entry = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Event::Turn(entry) = observer.next().await? {
                        break Ok::<_, acyclic_harness::Error>(entry);
                    }
                }
            })
            .await.map_err(|error| format!("step=endpoint-{phase}-turn-timeout: {error}; recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/backends/storage-conformance.sh"))?;
            let entry = checked(phase, "follow-turn", entry)?;
            assert_eq!(
                checked(phase, "read-turn", conversation.turn(&entry).await)?,
                (Some("stored prompt".into()), "stored answer".into())
            );
            let reply = checked(phase, "reply", conversation.reply("once", inbox).await)?;
            assert!(matches!(reply, Reply::Answered { sequence: 0, .. }));
            assert_eq!(checked(phase, "settled", conversation.settled().await)?, 1);
            assert!(conversation.send("once", "changed prompt").await.is_err());
            let reconnected = checked(phase, "reconnect", Conversation::open(checked(phase, "reconnect-transport", Connection::connect(&options).await)?))?;
            assert_eq!(checked(phase, "reconnect-send", reconnected.send("once", "stored prompt").await)?, inbox);
            assert_eq!(checked(phase, "reconnect-reply", reconnected.reply("once", inbox).await)?, reply);
            Ok::<_, acyclic_agent_host_conformance::Failure>((inbox, reply))
        }
        .await;
        let _ = stop.send(());
        tokio::time::timeout(Duration::from_secs(10), &mut serving.0).await???;
        runtime.0.abort();
        let _ = (&mut runtime.0).await;
        let observed = observed?;
        if let Some(previous) = &saved {
            assert_eq!(&observed, previous);
        }
        saved = Some(observed);
    }
    Ok(())
}
