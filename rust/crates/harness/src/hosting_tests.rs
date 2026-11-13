use super::*;
use crate::{
    OperationId,
    agent::{AgentId, AgentState, Delivered, WorkspaceEntry},
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    executor::Executor,
    filesystem::{PRIVATE_VOLUME, TurnAuthority, TurnExecutors, TurnLimits},
    model::ModelContent,
    resources::ProviderRef,
};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock, wire};
use acyclic_fs::{EmbeddedCapabilities, LogicalObjectStore};
use acyclic_harness_client::{AgentControlFailure, PreparedCommand, decode_agent_reply};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{MemoryStream, session::Client, testing::Counted};
use prost::Message as _;
use std::sync::atomic::{AtomicBool, Ordering};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

#[path = "hosting_tests/session_pages.rs"]
mod session_pages;

#[path = "hosting_tests/peer_delivery.rs"]
mod peer_delivery;

struct Never;

impl TurnExecutors for Never {
    fn executor<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        _: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, crate::Result<Arc<dyn Executor>>> {
        Box::pin(async { Err(crate::Error::Unsupported("no turns in this test".into())) })
    }

    fn templates(&self) -> bool {
        true
    }
}

/// Allows every command until denied; runs children unless childless.
#[derive(Default)]
struct Owner {
    denied: AtomicBool,
    revoke_read: AtomicBool,
    revoke_query: AtomicBool,
    childless: AtomicBool,
    sessions: AtomicBool,
    diff: std::sync::Mutex<Option<QueryPause>>,
    diff_issuer: std::sync::OnceLock<AuthorityRoot>,
    admission: std::sync::Mutex<Option<QueryPause>>,
}

struct QueryPause {
    entered: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}

impl Platform for Owner {
    fn sessions(&self) -> bool {
        self.sessions.load(Ordering::SeqCst)
    }

    fn policy<'a>(
        &'a self,
        _: &'a Principal,
        command: &'a AgentCommand,
    ) -> BoxProviderFuture<'a, std::result::Result<PolicyDecision, StreamError>> {
        let denied = self.denied.load(Ordering::SeqCst);
        if (matches!(command, AgentCommand::Read { .. })
            && self.revoke_read.swap(false, Ordering::SeqCst))
            || self.revoke_query.swap(false, Ordering::SeqCst)
        {
            self.denied.store(true, Ordering::SeqCst);
        }
        Box::pin(async move {
            Ok(if denied {
                PolicyDecision::Deny
            } else {
                PolicyDecision::Allow
            })
        })
    }

    fn runs_children(&self) -> bool {
        !self.childless.load(Ordering::SeqCst)
    }

    fn admitted(&self) -> BoxProviderFuture<'_, std::result::Result<(), StreamError>> {
        Box::pin(async move {
            let pause = self
                .admission
                .lock()
                .map_err(|_| StreamError::Unavailable)?
                .take();
            if let Some(pause) = pause {
                pause
                    .entered
                    .send(())
                    .map_err(|()| StreamError::Unavailable)?;
                pause.release.await.map_err(|_| StreamError::Unavailable)?;
            }
            Ok(())
        })
    }

    fn diff_source<'a>(
        &'a self,
        authorization: &'a CallAuthorization,
        agent: &'a AgentPath,
    ) -> BoxProviderFuture<'a, crate::Result<DiffSource>> {
        Box::pin(async move {
            authorization
                .principal()
                .map_err(|error| crate::Error::Storage(error.to_string()))?;
            let pause = self
                .diff
                .lock()
                .map_err(|_| crate::Error::Conflict("diff control poisoned".into()))?
                .take();
            if let Some(pause) = pause {
                pause
                    .entered
                    .send(())
                    .map_err(|()| crate::Error::Conflict("diff observer closed".into()))?;
                pause
                    .release
                    .await
                    .map_err(|_| crate::Error::Conflict("diff source owner closed".into()))?;
            }
            let provider = provider().map_err(|error| crate::Error::Storage(error.to_string()))?;
            let host =
                crate::filesystem::FilesystemHost::new(acyclic_fs::Fs::memory(), &provider, None)?;
            let owner = crate::AgentId::from_bytes([41; 16]);
            let volume = VolumeRef::new(
                provider.clone(),
                "diff-control".into(),
                VolumeClass::Project,
                VolumeOwner::Agent(owner),
                &mut None,
            )?;
            host.create_volume(&volume).await?;
            let issuer = self.diff_issuer.get_or_init(|| {
                AuthorityRoot::new(
                    "diff-control",
                    [41; 32],
                    Authority {
                        kind: AggregateKind::Conversation,
                        id: "diff-control".into(),
                    },
                )
            });
            let scope = issuer.bootstrap_scope(
                "reader",
                &crate::Capabilities::new([
                    volume.directory_read_capability("", &mut None)?,
                    volume.capability(crate::conversation::VolumeOperation::Read, &mut None)?,
                ]),
                Some(owner),
                &mut None,
            )?;
            let reader = crate::filesystem::FilesystemContentVerifier::new(
                host.clone(),
                &issuer.verifier(&mut None)?,
                &scope,
                64 * 1024,
                None,
            )?;
            let reference =
                crate::filesystem::workspace_ref(provider, &volume.storage_name(&mut None)?)?;
            let generation = host.resolve(&reference, None).await?.generation;
            Ok(DiffSource::new(
                reader,
                acyclic_harness_client::DiffSnapshot {
                    agent: agent.clone(),
                    volume,
                    base: generation.clone(),
                    current: generation,
                },
            ))
        })
    }
}

fn provider() -> Result<ProviderRef> {
    Ok(ProviderRef::new(
        "agents".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?)
}

/// A credential that may invoke exactly `resource`.
fn credential(resource: &str) -> Bytes {
    credentials(&[resource])
}

fn credentials(resources: &[&str]) -> Bytes {
    use acyclic_auth::contract::*;
    acyclic_auth::testing::root(|block| {
        let Some(key) = block
            .certificates
            .as_mut()
            .and_then(|certificates| certificates.keys.first_mut())
        else {
            return;
        };
        key.key.grants = GrantSet {
            grants: resources
                .iter()
                .map(|resource| Grant {
                    kind: GrantKind::Resource(ResourceGrant {
                        resource: ResourcePattern {
                            name: (*resource).into(),
                            prefix: false,
                        },
                        actions: vec!["actors/invoke".into()],
                    }),
                })
                .collect(),
        };
    })
    .into()
}

fn invocation(namespace: &str, command: &AgentCommand) -> Result<Bytes> {
    Ok(wire::InvokeActorRequest {
        deployment_envelope: Vec::new(),
        actor_id: "owner".into(),
        method: "POST".into(),
        url: "https://agents.invalid/command".into(),
        body: PreparedCommand::new(command)?.command().to_vec(),
        namespace: Some(namespace.into()),
        account_id: b"account".to_vec(),
        ..Default::default()
    }
    .encode_to_vec()
    .into())
}

async fn call(
    client: &Client,
    namespace: &str,
    command: &AgentCommand,
) -> Result<std::result::Result<AgentReply, AgentControlFailure>> {
    let reply = client
        .actor_call(&invocation(namespace, command)?, 0, 1 << 20)
        .await
        .inspect_err(|error| eprintln!("session control {command:?}: {error}"))?;
    let response = wire::InvokeActorResponse::decode(reply)?;
    Ok(decode_agent_reply(&response.body, command)?)
}

async fn diff_call(
    client: &Client,
    namespace: &str,
    command: &AgentCommand,
) -> Result<std::result::Result<AgentReply, AgentControlFailure>> {
    let mut query = client
        .actor_socket(
            acyclic_harness_client::DiffProjection::Patch.open(&invocation(namespace, command)?)?,
            0,
        )
        .await?;
    while let Some(frame) = query.next().await? {
        match frame.first() {
            Some(1) | Some(4) => {}
            Some(2) | Some(5) => return Ok(decode_agent_reply(&frame[1..], command)?),
            _ => return Err("unexpected diff source frame".into()),
        }
    }
    Ok(Ok(AgentReply::Diff { items: Vec::new() }))
}

#[test]
fn serving_reply_codec_preserves_invalid_and_capacity_refusals() -> Result {
    let command = AgentCommand::Root {};
    let call = acyclic_harness_client::AgentCall::decode(&invocation("agents", &command)?)?;
    assert!(matches!(
        super::reply(&call, &Ok(AgentReply::Staged {})),
        Err(StreamError::InvalidResponse)
    ));
    let oversized = AgentReply::Bound {
        binding: "x".repeat(acyclic_harness_client::MAX_AGENT_COMMAND_BYTES + 1),
        tools: String::new(),
        observation: None,
    };
    assert!(matches!(
        super::reply(&call, &Ok(oversized)),
        Err(StreamError::LimitExceeded)
    ));
    let root = AgentPath::root(&AgentId::from_bytes([13; 16]));
    let encoded = super::reply(
        &call,
        &Ok(AgentReply::Root {
            agent: root.clone(),
        }),
    )?;
    let response = wire::InvokeActorResponse::decode(encoded)?;
    assert!(matches!(
        decode_agent_reply(&response.body, &command)?,
        Ok(AgentReply::Root { agent }) if agent == root
    ));
    Ok(())
}

fn binding()
-> Result<Bound<Counted<MemoryStream>, LogicalObjectStore<acyclic_objects::MemoryObjects>>> {
    binding_with(Arc::new(Never), Default::default())
}

fn binding_with(
    executors: Arc<dyn TurnExecutors>,
    approval_policy: crate::approvals::ApprovalPolicy,
) -> Result<Bound<Counted<MemoryStream>, LogicalObjectStore<acyclic_objects::MemoryObjects>>> {
    let stream = Arc::new(Counted::new(MemoryStream::default()));
    let objects = acyclic_objects::MemoryObjects::memory();
    Ok(Bound::local_root(
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "local".into(),
            },
        ),
        Placement {
            filesystem: acyclic_fs::Fs::new(
                acyclic_fs::StreamAuthorityStore::new(stream.clone()),
                LogicalObjectStore::new(Arc::new(objects)),
                EmbeddedCapabilities::MEMORY,
            ),
            provider: provider()?,
            within: ActorReference::new(b"account", b"agents", b"owner")?,
        },
        Turns {
            executors,
            limits: TurnLimits {
                maximum_bytes: Some(64 * 1024),
                history_turns: 8,
            },
            actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            clock: Arc::new(SystemClock),
            approval_policy,
        },
    )?)
}

#[cfg(all(not(target_arch = "wasm32"), feature = "test-support"))]
#[path = "hosting_tests/machine_results.rs"]
mod machine_results;

#[tokio::test]
async fn session_reads_committed_tool_output_without_local_storage_features() -> Result {
    use crate::conversation::{ContentAccess, ContentPublisher};
    use crate::filesystem::{FilesystemContentPublisher, agent_authority};
    use acyclic_harness_client::{CommandError, MAX_READ_BYTES, ReadTarget};
    use acyclic_stream::wire as session_wire;
    use futures::StreamExt as _;

    let bound = binding()?;
    let agent = AgentId::from_bytes([2; 16]);
    let private = VolumeRef::new(
        provider()?,
        "output-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(crate::AgentId::from_bytes(agent.to_bytes())),
        &mut None,
    )?;
    let owner = Arc::new(Owner::default());
    let Serving::Served(server) = serve(
        bound,
        owner.clone(),
        &IntentId::try_from("output-root")?,
        RootRequest {
            peer: true,
            agent,
            fresh: vec![WorkspaceEntry::initial_volume(
                crate::agent::WorkspaceRole::Private,
                PRIVATE_VOLUME.into(),
                private,
            )],
            retained: Vec::new(),
            brief: None,
            policy: None,
        },
    )
    .await?
    else {
        return Err("root publication was uncertain".into());
    };
    let bound = server.bound();
    let root = server.root();
    let state = bound.agents().state(root).await?;
    let authority =
        agent_authority(bound.issuer(), bound.admission(), root, &state.volumes, &[]).await?;
    let publisher = FilesystemContentPublisher::new(
        bound.volumes().clone(),
        authority.private.volume()?,
        &authority.verifier,
        &authority.scope,
        64 * 1024,
        None,
    )?;
    let body: Vec<u8> = (0..MAX_READ_BYTES + 17).map(|index| index as u8).collect();
    let path = "tools/turn/stdout";
    let (file, _) = publisher
        .stage_once(
            OperationId::from_bytes([4; 16]),
            path,
            &body,
            "application/octet-stream",
            "stdout",
            ContentAccess::default(),
        )?
        .await?;
    // The effect precedes observation. Replay uses the original volume's
    // retained change stream, rather than a disposable notification.
    let resource = bound.tool_resource(root).await?;
    let resource_credential = acyclic_auth::testing::root(|block| {
        use acyclic_auth::contract::*;
        let Some(key) = block
            .certificates
            .as_mut()
            .and_then(|chain| chain.keys.first_mut())
        else {
            return;
        };
        key.key.grants = GrantSet {
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: resource.clone(),
                        prefix: true,
                    },
                    actions: vec!["fs/list".into()],
                }),
            }],
        };
    });
    let connect = |resources| -> Result<Client> {
        let client = Client::in_process_with_products(
            bound.agents().provider().clone(),
            acyclic_stream::session::testing::authority(),
            resources,
            server.clone(),
            Some(server.clone()),
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
            None,
        );
        client.register_credential(0, credential(server.resource()))?;
        client.register_credential(1, resource_credential.clone().into())?;
        Ok(client)
    };
    let subscription = session_wire::SessionSubscription {
        credential: 1,
        resume: Some(session_wire::SessionCursor {
            next: 0,
            opaque: Default::default(),
        }),
        kind: Some(session_wire::session_subscription::Kind::Follow(
            session_wire::SessionFollow {
                resource: resource.clone(),
                delivery: session_wire::SessionDelivery::All as i32,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    let missing = connect(None)?;
    assert!(matches!(
        missing.subscribe(subscription.clone()).await,
        Err(StreamError::Unsupported)
    ));
    let connected = connect(Some(Arc::new(bound.changes()?)))?;
    let mut follow = connected.subscribe(subscription).await?;
    // The already committed publication is replayed by this original follow;
    // loss of its producer ends the same channel with a typed failure.
    loop {
        let update = follow.next().await.ok_or(StreamError::Unavailable)??;
        if let Some(session_wire::session_frame::Body::Data(data)) = update.into_wire().body
            && let Some(session_wire::session_data::Item::Record(record)) = data.item
        {
            let changes = acyclic_stream::decode_changes(&record.value)?;
            if changes
                .entries
                .iter()
                .any(|entry| entry.key.as_ref() == path.as_bytes())
            {
                break;
            }
        }
    }
    for offset in [0, MAX_READ_BYTES, body.len() as u64] {
        // Reconnect between pages. Both exact FileRef and tool-path reads use
        // this same admitted reader, with no local filesystem provider.
        let client = connect(Some(Arc::new(bound.changes()?)))?;
        for target in [
            ReadTarget::ToolOutput {
                path: path.into(),
                at: None,
            },
            ReadTarget::File { file: file.clone() },
        ] {
            let reply = call(
                &client,
                "agents",
                &AgentCommand::Read {
                    agent: root.clone(),
                    target,
                    offset,
                },
            )
            .await??;
            let (bytes, length) = reply.content()?;
            assert_eq!(length, body.len() as u64);
            assert_eq!(
                bytes,
                body[offset as usize..]
                    .iter()
                    .take(MAX_READ_BYTES as usize)
                    .copied()
                    .collect::<Vec<_>>()
            );
        }
    }
    let client = connect(Some(Arc::new(bound.changes()?)))?;
    for target in [
        ReadTarget::File { file: file.clone() },
        ReadTarget::ToolOutput {
            path: path.into(),
            at: None,
        },
    ] {
        // Admission allowed this real immutable read, then the owner revoked
        // it. The shared post-I/O fence must refuse even a successful source.
        owner.revoke_read.store(true, Ordering::SeqCst);
        let refused = call(
            &client,
            "agents",
            &AgentCommand::Read {
                agent: root.clone(),
                target,
                offset: 0,
            },
        )
        .await
        .expect_err("content disclosed after owner revocation");
        assert!(matches!(
            refused.downcast_ref::<StreamError>(),
            Some(StreamError::AccessDenied)
        ));
        owner.denied.store(false, Ordering::SeqCst);
    }
    publisher
        .stage_once(
            OperationId::from_bytes([5; 16]),
            "notes/private",
            b"not tool output",
            "text/plain",
            "private",
            ContentAccess::default(),
        )?
        .await?;
    // Traversal is refused by the original command admission before transport.
    assert!(matches!(
        PreparedCommand::new(&AgentCommand::Read {
            agent: root.clone(),
            target: ReadTarget::ToolOutput {
                path: "../outside".into(),
                at: None,
            },
            offset: 0,
        }),
        Err(CommandError::Invalid)
    ));
    for (agent, path) in [
        (root.clone(), "tools/missing"),
        (root.clone(), "notes/private"),
        (AgentPath::root(&AgentId::from_bytes([9; 16])), path),
    ] {
        assert!(
            call(
                &client,
                "agents",
                &AgentCommand::Read {
                    agent,
                    target: ReadTarget::ToolOutput {
                        path: path.into(),
                        at: None
                    },
                    offset: 0,
                }
            )
            .await?
            .is_err()
        );
    }
    let foreign = VolumeRef::new(
        provider()?,
        "another-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(crate::AgentId::from_bytes([9; 16])),
        &mut None,
    )?;
    let foreign = crate::conversation::FileRef::new(
        foreign,
        file.path().into(),
        file.version().into(),
        file.descriptor().clone(),
        file.display_name().into(),
        &mut None,
    )?;
    assert!(
        call(
            &client,
            "agents",
            &AgentCommand::Read {
                agent: root.clone(),
                target: ReadTarget::File { file: foreign },
                offset: 0,
            }
        )
        .await?
        .is_err()
    );
    let denied = connect(Some(Arc::new(bound.changes()?)))?;
    denied.register_credential(0, credential("actor:/another-server"))?;
    assert!(
        call(
            &denied,
            "agents",
            &AgentCommand::Read {
                agent: root.clone(),
                target: ReadTarget::ToolOutput {
                    path: path.into(),
                    at: None
                },
                offset: 0,
            }
        )
        .await
        .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn a_served_tree_dispatches_session_commands_within_its_admission() -> Result {
    let bound = binding()?;
    let stream = bound.agents().provider().clone();
    let agent = AgentId::from_bytes([1; 16]);
    let private = WorkspaceEntry::initial_volume(
        crate::agent::WorkspaceRole::Private,
        PRIVATE_VOLUME.into(),
        VolumeRef::new(
            provider()?,
            "agent-private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes(agent.to_bytes())),
            &mut None,
        )?,
    );
    let owner = Arc::new(Owner::default());
    let server = serve(
        bound,
        owner.clone(),
        &IntentId::try_from("root")?,
        RootRequest {
            peer: true,
            agent,
            fresh: vec![private],
            retained: Vec::new(),
            brief: None,
            policy: None,
        },
    )
    .await?;
    let Serving::Served(server) = server else {
        return Err("serving the root was uncertain".into());
    };
    let client = Client::in_process_with_products(
        stream.clone(),
        acyclic_stream::session::testing::authority(),
        None,
        server.clone(),
        Some(server.clone()),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, credential(server.resource()))?;
    let root = server.root().clone();

    assert!(matches!(
        call(&client, "agents", &AgentCommand::Root {}).await?,
        Ok(AgentReply::Root { agent }) if agent == root
    ));
    let peer_client = Client::in_process_with_products(
        stream.clone(),
        acyclic_stream::session::testing::authority(),
        None,
        server.clone(),
        Some(server.clone()),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    peer_client.register_credential(0, credential("actors:/peer-directory/account"))?;
    for command in [
        AgentCommand::SetPeerMembership {
            agent: root.clone(),
            id: IntentId::try_from("peer-cannot-opt-out")?,
            enabled: false,
        },
        AgentCommand::Archive {
            agent: root.clone(),
            id: IntentId::try_from("peer-cannot-archive")?,
        },
        AgentCommand::Approvals {
            agent: root.clone(),
            page: AgentPageRequest::default(),
        },
        AgentCommand::Children {
            agent: root.clone(),
            page: AgentPageRequest::default(),
        },
        AgentCommand::PublishAgentFiles {
            agent: root.clone(),
            operation: OperationId::new(),
            changes: vec![crate::client_files::ClientFileChange {
                path: "ui/title".into(),
                expected: None,
                text: Some("forged title".into()),
            }],
        },
    ] {
        let before = stream.requests();
        let denied = call(&peer_client, "agents", &command).await;
        assert!(
            !matches!(&denied, Ok(Ok(_))),
            "peer authority reached a user command: {denied:?}"
        );
        let requests = stream.requests().since(before);
        assert_eq!(
            (requests.data(), requests.control()),
            (0, 0),
            "peer authority must be refused before observing or changing storage"
        );
    }
    for enabled in [false, true] {
        let command = AgentCommand::SetPeerMembership {
            agent: root.clone(),
            id: IntentId::try_from(if enabled { "peers-on" } else { "peers-off" })?,
            enabled,
        };
        assert!(matches!(call(&client, "agents", &command).await?,
            Ok(AgentReply::PeerMembership { enabled: committed }) if committed == enabled));
        // Replay the user's exact action without another membership record.
        let before = stream.tail(root.peer_membership()?).await?;
        assert!(matches!(call(&client, "agents", &command).await?,
            Ok(AgentReply::PeerMembership { enabled: committed }) if committed == enabled));
        assert_eq!(stream.tail(root.peer_membership()?).await?, before);
        let head = stream.head(root.peer_membership()?).await?;
        let record = head.last.ok_or("membership record missing")?;
        assert_eq!(
            crate::agent::PeerMembership::decode(&record.value)?.enabled,
            enabled
        );
    }
    let usage = AgentCommand::Usage {
        agent: root.clone(),
        input: 0,
        after: 0,
        wait: true,
        observed_children: Some(0),
    };
    let mut live_usage = std::pin::pin!(call(&client, "agents", &usage));
    assert!(
        futures::poll!(&mut live_usage).is_pending(),
        "adoption is committed but its original actor has not consumed the input"
    );
    let events = AgentCommand::Events {
        agent: root.clone(),
        input: 0,
        after: 0,
        wait: true,
    };
    let mut live_events = std::pin::pin!(call(&client, "agents", &events));
    assert!(futures::poll!(&mut live_events).is_pending());
    assert!(server.bound().agents().runtime().drain().await.is_empty());
    assert!(
        matches!(live_usage.await?, Ok(AgentReply::Usage { input: 0, page: Some(page), settled: true, .. })
        if page.end == 0 && page.items.is_empty()),
        "a consumed input with no model call must finish its original quiet query"
    );
    assert!(matches!(
        live_events.await?,
        Ok(AgentReply::Events { input: 0, page: Some(page), settled: true, .. })
            if page.operation.is_none() && page.end == 0 && page.items.is_empty()
    ));
    // Keep this query pending on the next original input. Driving other calls
    // must not complete it before revocation and its source publication.
    let events = AgentCommand::Events {
        agent: root.clone(),
        input: 1,
        after: 0,
        wait: true,
    };
    let (entered, admission) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *owner
        .admission
        .lock()
        .map_err(|_| "admission control poisoned")? = Some(QueryPause {
        entered,
        release: released,
    });
    let mut revoked_events = std::pin::pin!(call(&client, "agents", &events));
    tokio::select! {
        reply = &mut revoked_events => return Err(format!("query ended before admission: {reply:?}").into()),
        admitted = admission => admitted?,
    }
    owner.denied.store(true, Ordering::SeqCst);
    release
        .send(())
        .map_err(|()| "query admission closed before release")?;
    let by = Sender::Host {
        host: "query-source".into(),
    };
    assert!(matches!(
        server
            .bound()
            .agents()
            .send(
                &root,
                &by,
                &InboxRecord::Notice {
                    id: IntentId::try_from("release-revoked-query")?,
                    notice: crate::agent::NoticePayload::Quiet(crate::conversation::wire::Notice {
                        source: "test".into(),
                        kind: "test.control".into(),
                        body: serde_json::Value::Null,
                        wake: false,
                        coalesce_key: None,
                    }),
                },
            )
            .await?,
        Delivered::Committed { .. }
    ));
    assert!(server.bound().agents().runtime().drain().await.is_empty());
    let refused = revoked_events
        .await
        .expect_err("events disclosed after revocation");
    assert!(matches!(
        refused.downcast_ref::<StreamError>(),
        Some(StreamError::AccessDenied)
    ));
    owner.denied.store(false, Ordering::SeqCst);
    // A journal position is scoped to an original turn. Neither a consumed
    // control-only input nor the not-yet-created next input can own it.
    for input in [0, 1, 2] {
        assert!(matches!(
            call(&client, "agents", &AgentCommand::Events {
                agent: root.clone(),
                input,
                after: 1,
                wait: false,
            }).await?,
            Err(AgentControlFailure::Refused { code, reason })
                if code == "invalid_agent_record" && reason.contains("journal cursor")
        ));
    }
    let usage = AgentCommand::Usage {
        agent: root.clone(),
        input: 2,
        after: 0,
        wait: false,
        observed_children: Some(0),
    };
    assert!(matches!(
        call(&client, "agents", &usage).await?,
        Ok(AgentReply::Usage {
            input: 2,
            page: None,
            settled: false,
            ..
        })
    ));
    let diff = AgentCommand::Diff {
        agent: root.clone(),
        page: Default::default(),
    };
    let empty_diff = diff_call(&client, "agents", &diff).await?;
    assert!(
        matches!(&empty_diff, Ok(AgentReply::Diff { items }) if items.is_empty()),
        "the original empty source must finish normally: {empty_diff:?}"
    );
    // The real session admits this query before its source suspends. Revoking
    // policy during that wait must refuse disclosure, including an empty result.
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    *owner.diff.lock().map_err(|_| "diff control poisoned")? = Some(QueryPause {
        entered,
        release: resume,
    });
    let query = diff_call(&client, "agents", &diff);
    tokio::pin!(query);
    tokio::select! {
        _ = &mut query => return Err("query ended before its source suspended".into()),
        entered = waiting => entered?,
    }
    owner.denied.store(true, Ordering::SeqCst);
    release
        .send(())
        .map_err(|()| "diff source closed before release")?;
    let refused = query
        .await
        .expect_err("a suspended query disclosed after revocation");
    assert!(matches!(
        refused.downcast_ref::<StreamError>(),
        Some(StreamError::AccessDenied)
    ));
    owner.denied.store(false, Ordering::SeqCst);
    // A lost source owner ends the same waiter with a typed refusal, rather
    // than leaving its original admitted channel pending forever.
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    *owner.diff.lock().map_err(|_| "diff control poisoned")? = Some(QueryPause {
        entered,
        release: resume,
    });
    drop(release);
    let refused = diff_call(&client, "agents", &diff).await?;
    assert!(
        matches!(
            &refused,
            Err(AgentControlFailure::Refused { code, reason })
                if code == "agent_conflict" && reason.contains("diff source owner closed")
        ),
        "a lost diff source must preserve its definitive conflict: {refused:?}"
    );
    waiting.await?;
    // A source error can disclose state too. Revocation takes precedence over
    // the original typed source failure after the same admitted wait.
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    *owner.diff.lock().map_err(|_| "diff control poisoned")? = Some(QueryPause {
        entered,
        release: resume,
    });
    let query = diff_call(&client, "agents", &diff);
    tokio::pin!(query);
    tokio::select! {
        _ = &mut query => return Err("query ended before its source suspended".into()),
        entered = waiting => entered?,
    }
    owner.denied.store(true, Ordering::SeqCst);
    drop(release);
    let refused = query
        .await
        .expect_err("a revoked query disclosed its source error");
    assert!(matches!(
        refused.downcast_ref::<StreamError>(),
        Some(StreamError::AccessDenied)
    ));
    owner.denied.store(false, Ordering::SeqCst);
    let children = AgentCommand::Children {
        agent: root.clone(),
        page: Default::default(),
    };
    assert!(matches!(
        call(&client, "agents", &children).await?,
        Ok(AgentReply::Children { items, next: None }) if items.is_empty()
    ));
    for query in [
        children.clone(),
        AgentCommand::History {
            agent: root.clone(),
            page: Default::default(),
        },
        AgentCommand::Approvals {
            agent: root.clone(),
            page: Default::default(),
        },
    ] {
        call(&client, "agents", &query).await??;
        // Admission succeeds, then policy changes before the query's source
        // can return. Even an empty page cannot leave on the stale grant.
        owner.revoke_query.store(true, Ordering::SeqCst);
        let refused = call(&client, "agents", &query)
            .await
            .expect_err("session page disclosed after owner revocation");
        assert!(matches!(
            refused.downcast_ref::<StreamError>(),
            Some(StreamError::AccessDenied)
        ));
        owner.denied.store(false, Ordering::SeqCst);
    }
    for query in [
        AgentCommand::History {
            agent: root.clone(),
            page: AgentPageRequest::new(1, 1, None)?,
        },
        AgentCommand::Children {
            agent: root.clone(),
            page: AgentPageRequest::new(1, 1, None)?,
        },
    ] {
        let before = stream.requests();
        assert!(
            matches!(
                call(&client, "agents", &query).await?,
                Err(AgentControlFailure::Refused { code, .. })
                    if code == Error::Stream(StreamError::Capacity).code()
            ),
            "an empty page must reserve its complete canonical envelope"
        );
        assert_eq!(
            stream.requests().since(before),
            Default::default(),
            "insufficient empty-page credit must refuse before source requests"
        );
    }
    let spawn = AgentCommand::Spawn {
        parent: root.clone(),
        id: IntentId::try_from("s1")?,
        brief: ModelContent::Text("child".into()),
        role: "worker".into(),
    };
    // A host that runs no children refuses the spawn itself, typed, and
    // commits nothing: no child is created to wait forever.
    owner.childless.store(true, Ordering::SeqCst);
    assert!(matches!(
        call(&client, "agents", &spawn).await?,
        Err(AgentControlFailure::Refused { code, .. }) if code == "spawn_refused"
    ));
    assert!(matches!(
        call(&client, "agents", &children).await?,
        Ok(AgentReply::Children { items, next: None }) if items.is_empty()
    ));
    owner.childless.store(false, Ordering::SeqCst);
    // The parent's role decides: a role it does not admit is refused the
    // same way, before anything commits.
    let refused = AgentCommand::Spawn {
        parent: root.clone(),
        id: IntentId::try_from("s0")?,
        brief: ModelContent::Text("child".into()),
        role: "planner".into(),
    };
    assert!(matches!(
        call(&client, "agents", &refused).await?,
        Err(AgentControlFailure::Refused { code, .. }) if code == "spawn_refused"
    ));
    assert!(matches!(
        call(&client, "agents", &children).await?,
        Ok(AgentReply::Children { items, next: None }) if items.is_empty()
    ));
    // A spawn is the parent's own step: the reply names the child, and the
    // running host's next step for the parent creates it.
    let runner = {
        let server = server.clone();
        tokio::spawn(async move { server.run(|_, _| {}).await })
    };
    let Ok(AgentReply::Spawned { agent: child, .. }) = call(&client, "agents", &spawn).await?
    else {
        return Err("spawn was not delivered".into());
    };
    assert!(
        matches!(call(&client, "agents", &spawn).await?, Ok(AgentReply::Spawned { agent, .. }) if agent == child),
        "a retried spawn names the same child"
    );
    runner.abort();
    let _ = runner.await;
    // Spawn's committed result already names the child. No later publication
    // is needed to list it with its atomically published external JSON status.
    let before = stream.requests();
    assert!(matches!(
        call(&client, "agents", &children).await?,
        Ok(AgentReply::Children { items, next: None })
            if items.len() == 1 && items[0].agent == child && items[0].status.is_some()
    ));
    let requests = stream.requests().since(before);
    assert_eq!(requests.control(), 2, "one page and one status view");
    assert_eq!(
        requests.data(),
        0,
        "child status required separate record reads"
    );
    // Admission itself may suspend after the original policy decision. The
    // real child page and status still require current policy at disclosure.
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    *owner
        .admission
        .lock()
        .map_err(|_| "admission control poisoned")? = Some(QueryPause {
        entered,
        release: resume,
    });
    let query = call(&client, "agents", &children);
    tokio::pin!(query);
    tokio::select! {
        _ = &mut query => return Err("child query ended before admission suspended".into()),
        entered = waiting => entered?,
    }
    owner.denied.store(true, Ordering::SeqCst);
    release
        .send(())
        .map_err(|()| "child admission closed before release")?;
    let refused = query
        .await
        .expect_err("a suspended child query disclosed after revocation");
    assert!(matches!(
        refused.downcast_ref::<StreamError>(),
        Some(StreamError::AccessDenied)
    ));
    owner.denied.store(false, Ordering::SeqCst);

    // Dropping the admission producer must end the original query, too.
    let (entered, waiting) = tokio::sync::oneshot::channel();
    let (release, resume) = tokio::sync::oneshot::channel();
    *owner
        .admission
        .lock()
        .map_err(|_| "admission control poisoned")? = Some(QueryPause {
        entered,
        release: resume,
    });
    drop(release);
    let refused = call(&client, "agents", &children)
        .await
        .expect_err("a child query survived its admission producer");
    assert!(matches!(
        refused.downcast_ref::<StreamError>(),
        Some(StreamError::Unavailable)
    ));
    waiting.await?;
    // Nothing waits on an approval, so none is listed and none can be decided.
    assert!(matches!(
        call(
            &client,
            "agents",
            &AgentCommand::Approvals {
                agent: root.clone(),
                page: Default::default(),
            }
        )
        .await?,
        Ok(AgentReply::Approvals { items, next: None }) if items.is_empty()
    ));
    assert!(matches!(
        call(
            &client,
            "agents",
            &AgentCommand::Decide {
                agent: root.clone(),
                id: IntentId::try_from("approval-1")?,
                binding: crate::interaction::ApprovalBinding {
                    operation_id: OperationId::from_bytes([3; 16]),
                    action_digest: [4; 32],
                },
                verdict: true.into(),
                reason: None,
            }
        )
        .await?,
        Err(AgentControlFailure::Refused { code, .. }) if code == "invalid_agent_record"
    ));
    let prompt = AgentCommand::Prompt {
        peer: None,
        agent: root.clone(),
        id: IntentId::try_from("p1")?,
        body: ModelContent::Text("hello".into()),
    };
    assert!(matches!(
        call(&client, "agents", &prompt).await?,
        Ok(AgentReply::Delivered {
            delivery: Delivered::Committed { .. }
        })
    ));
    // A namespace fork alone is not an admitted agent publication: its
    // canonical external status is missing. Refuse the incomplete row.
    let orphan = root.child(&AgentId::from_bytes([8; 16]))?;
    stream
        .fork(acyclic_stream::ForkRequest {
            source: child.inbox()?,
            destination: orphan.inbox()?,
            at_tail: Some(0),
            exclude: Vec::new(),
            idempotency_key: acyclic_stream::IdempotencyKey::new(
                b"missing-child-status".as_slice(),
            )?,
        })
        .await?;
    assert!(matches!(
        call(&client, "agents", &children).await?,
        Err(AgentControlFailure::Refused { code, .. }) if code == "invalid_agent_record"
    ));
    // A foreign tree needs its own credential. Refuse before dispatch or any
    // source lookup, rather than disclosing whether the foreign agent exists.
    let stranger = AgentPath::root(&AgentId::from_bytes([9; 16]));
    let before = stream.requests();
    let refused = call(
        &client,
        "agents",
        &AgentCommand::Cancel {
            conversation: 0,
            input: 1,
            agent: stranger,
            id: IntentId::try_from("c1")?,
        },
    )
    .await
    .expect_err("a foreign tree needs its own original credential");
    assert!(matches!(
        refused.downcast_ref::<StreamError>(),
        Some(StreamError::AccessDenied)
    ));
    assert_eq!(stream.requests().since(before), Default::default());
    // Another namespace never reaches dispatch, and owner denial is final.
    assert!(
        call(&client, "elsewhere", &AgentCommand::Root {})
            .await
            .is_err()
    );
    server
        .bound()
        .agents()
        .archive(
            &root,
            &Sender::User {
                principal: "owner".into(),
            },
            &IntentId::try_from("close-usage-source")?,
        )
        .await?;
    assert!(server.bound().agents().runtime().drain().await.is_empty());
    let end = crate::agent::inbox_tail(&*stream, &root).await?;
    assert!(
        matches!(
            call(
                &client,
                "agents",
                &AgentCommand::Usage {
                    agent: root.clone(),
                    input: end,
                    after: 0,
                    wait: true,
                    observed_children: Some(0),
                }
            )
            .await?,
            Ok(AgentReply::Usage {
                page: None,
                settled: true,
                ..
            })
        ),
        "a closed input producer cannot leave the live query pending"
    );
    assert!(matches!(
        call(
            &client,
            "agents",
            &AgentCommand::Events {
                agent: root.clone(),
                input: end,
                after: 0,
                wait: true,
            }
        )
        .await?,
        Ok(AgentReply::Events {
            page: None,
            settled: true,
            ..
        })
    ));
    owner.denied.store(true, Ordering::SeqCst);
    assert!(
        call(&client, "agents", &AgentCommand::Root {})
            .await
            .is_err()
    );
    let before = stream.requests();
    assert!(call(&client, "agents", &children).await.is_err());
    let requests = stream.requests().since(before);
    assert_eq!(requests.control() + requests.data(), 0);
    Ok(())
}

/// A staged head is authorized before any part is read: a denied caller's
/// head makes the host read nothing, however many parts it names.
#[tokio::test]
async fn a_denied_staged_head_reads_no_part() -> Result {
    let stream = Arc::new(MemoryStream::default());
    let objects = acyclic_stream::testing::Counted::new(acyclic_objects::MemoryObjects::memory());
    let ledger = objects.ledger();
    let bound = Bound::local_root(
        AuthorityRoot::new(
            "agents",
            [7; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "local".into(),
            },
        ),
        Placement {
            filesystem: acyclic_fs::Fs::new(
                acyclic_fs::StreamAuthorityStore::new(stream.clone()),
                LogicalObjectStore::new(Arc::new(objects)),
                EmbeddedCapabilities::MEMORY,
            ),
            provider: provider()?,
            within: ActorReference::new(b"account", b"agents", b"owner")?,
        },
        Turns {
            executors: Arc::new(Never),
            limits: TurnLimits {
                maximum_bytes: Some(64 * 1024),
                history_turns: 8,
            },
            actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            clock: Arc::new(SystemClock),
            approval_policy: Default::default(),
        },
    )?;
    let agent = AgentId::from_bytes([1; 16]);
    let private = WorkspaceEntry::initial_volume(
        crate::agent::WorkspaceRole::Private,
        PRIVATE_VOLUME.into(),
        VolumeRef::new(
            provider()?,
            "agent-private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes(agent.to_bytes())),
            &mut None,
        )?,
    );
    let owner = Arc::new(Owner::default());
    let server = serve(
        bound,
        owner.clone(),
        &IntentId::try_from("root")?,
        RootRequest {
            peer: true,
            agent,
            fresh: vec![private],
            retained: Vec::new(),
            brief: None,
            policy: None,
        },
    )
    .await?;
    let Serving::Served(server) = server else {
        return Err("serving the root was uncertain".into());
    };
    let client = Client::in_process_with_products(
        stream.clone(),
        acyclic_stream::session::testing::authority(),
        None,
        server.clone(),
        Some(server.clone()),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, credential(server.resource()))?;
    let root = server.root().clone();
    let digest = blake3::hash(b"{}").to_hex().to_string();
    let stage = AgentCommand::Stage {
        agent: root.clone(),
        digest: digest.clone(),
        part: 0,
        text: "[]".into(),
    };
    assert!(matches!(
        call(&client, "agents", &stage).await?,
        Ok(AgentReply::Staged {})
    ));
    let head = AgentCommand::Staged {
        agent: root.clone(),
        command: "prompt".into(),
        digest,
        parts: 1,
        bytes: 2,
    };
    // Negative control: an admitted head reads its part, and refuses it.
    let before = ledger.requests();
    assert!(call(&client, "agents", &head).await?.is_err());
    assert!(
        ledger.requests() != before,
        "an admitted head reads its parts"
    );
    owner.denied.store(true, Ordering::SeqCst);
    let before = ledger.requests();
    assert!(call(&client, "agents", &head).await.is_err());
    assert!(ledger.requests() == before, "a denied head reads no part");
    Ok(())
}

#[test]
fn a_children_cursor_carries_its_hierarchy_version_and_last_path() -> Result {
    let version = CommitId::from_bytes([0xa5; 32]);
    let after = AgentPath::root(&crate::agent::AgentId::from_bytes([0x5a; 16]))
        .child(&crate::agent::AgentId::from_bytes([0x3c; 16]))?;
    let cursor = children_cursor(&version, after.as_str());
    assert_eq!(
        page_json_bytes(&cursor)? - 2,
        children_cursor_bytes(&version, after.as_str()) as u64
    );
    assert_eq!(
        parse_children_cursor(&cursor)?,
        (version, StreamPath::new(after.as_str())?)
    );
    assert!(parse_children_cursor("agents/root/children/a").is_err());
    Ok(())
}

#[path = "hosting_tests/editor_input.rs"]
mod editor_input;
