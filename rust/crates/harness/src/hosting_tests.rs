use super::*;
use crate::{
    OperationId,
    agent::{AgentId, AgentState, Delivered, VolumeBinding},
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
use acyclic_stream::{MemoryStream, session::Client};
use prost::Message as _;
use std::sync::atomic::{AtomicBool, Ordering};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

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
}

/// Allows every command until denied.
#[derive(Default)]
struct Owner(AtomicBool);

impl Platform for Owner {
    fn policy<'a>(
        &'a self,
        _: &'a Principal,
        _: &'a AgentCommand,
    ) -> BoxProviderFuture<'a, std::result::Result<PolicyDecision, StreamError>> {
        let denied = self.0.load(Ordering::SeqCst);
        Box::pin(async move {
            Ok(if denied {
                PolicyDecision::Deny
            } else {
                PolicyDecision::Allow
            })
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
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: resource.into(),
                        prefix: false,
                    },
                    actions: vec!["actors/invoke".into()],
                }),
            }],
        };
    })
    .into()
}

fn invocation(namespace: &str, command: &AgentCommand) -> Result<Bytes> {
    Ok(wire::InvokeActorRequest {
        actor_id: "root".into(),
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
        .await?;
    let response = wire::InvokeActorResponse::decode(reply)?;
    Ok(decode_agent_reply(&response.body, command)?)
}

fn binding() -> Result<Bound<MemoryStream, LogicalObjectStore<acyclic_objects::MemoryObjects>>> {
    let stream = Arc::new(MemoryStream::default());
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
            executors: Arc::new(Never),
            limits: TurnLimits {
                maximum_bytes: 64 * 1024,
                max_steps: 4,
                history_turns: 8,
            },
            actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            clock: Arc::new(SystemClock),
            approval_policy: Default::default(),
        },
    )?)
}

#[tokio::test]
async fn session_reads_committed_tool_output_without_local_storage_features() -> Result {
    use crate::conversation::{ContentAccess, ContentPublisher};
    use crate::filesystem::{FilesystemContentPublisher, agent_authority};
    use acyclic_harness_client::{MAX_READ_BYTES, ReadTarget};
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
    let Serving::Served(server) = serve(
        bound,
        Arc::new(Owner::default()),
        &IntentId::try_from("output-root")?,
        RootRequest {
            agent,
            fresh: vec![VolumeBinding {
                name: PRIVATE_VOLUME.into(),
                volume: private,
            }],
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
        &authority.private.volume,
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
    let observed = tokio::time::timeout(std::time::Duration::from_secs(8), async {
        while let Some(update) = follow.next().await {
            if let Some(session_wire::session_frame::Body::Data(data)) = update?.body
                && let Some(session_wire::session_data::Item::Record(record)) = data.item
            {
                let changes = acyclic_stream::decode_changes(&record.value)?;
                if changes
                    .entries
                    .iter()
                    .any(|entry| entry.key.as_ref() == path.as_bytes())
                {
                    return Ok::<_, acyclic_stream::StreamError>(());
                }
            }
        }
        Err(acyclic_stream::StreamError::Unavailable)
    })
    .await?;
    observed?;
    for offset in [0, MAX_READ_BYTES, body.len() as u64] {
        // Reconnect between pages. Both exact FileRef and tool-path reads use
        // this same admitted reader, with no local filesystem provider.
        let client = connect(Some(Arc::new(bound.changes()?)))?;
        for target in [
            ReadTarget::ToolOutput { path: path.into() },
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
    for (agent, path) in [
        (root.clone(), "../outside"),
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
                    target: ReadTarget::ToolOutput { path: path.into() },
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
                target: ReadTarget::ToolOutput { path: path.into() },
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
    let private = VolumeBinding {
        name: PRIVATE_VOLUME.into(),
        volume: VolumeRef::new(
            provider()?,
            "agent-private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes(agent.to_bytes())),
            &mut None,
        )?,
    };
    let owner = Arc::new(Owner::default());
    let server = serve(
        bound,
        owner.clone(),
        &IntentId::try_from("root")?,
        RootRequest {
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
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, credential(server.resource()))?;
    let root = server.root().clone();

    assert!(matches!(
        call(&client, "agents", &AgentCommand::Root {}).await?,
        Ok(AgentReply::Root { agent }) if agent == root
    ));
    let children = AgentCommand::Children {
        agent: root.clone(),
        page: Default::default(),
    };
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
    let spawn = AgentCommand::Spawn {
        parent: root.clone(),
        id: IntentId::try_from("s1")?,
        brief: ModelContent::Text("child".into()),
    };
    let Ok(AgentReply::Spawned { agent: child, .. }) = call(&client, "agents", &spawn).await?
    else {
        return Err("spawn was not delivered".into());
    };
    assert!(
        matches!(call(&client, "agents", &spawn).await?, Ok(AgentReply::Spawned { agent, .. }) if agent == child),
        "a retried spawn names the same child"
    );
    let mut listed = false;
    for _ in 0..500 {
        if let Ok(AgentReply::Children { items, .. }) = call(&client, "agents", &children).await?
            && items.iter().any(|item| item.agent == child)
        {
            listed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(listed, "the parent's step spawned the child");
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
                approved: true,
                reason: None,
            }
        )
        .await?,
        Err(AgentControlFailure::Refused { code, .. }) if code == "invalid_agent_record"
    ));
    let prompt = AgentCommand::Prompt {
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
    // Outside the admitted tree is a typed refusal.
    let stranger = AgentPath::root(&AgentId::from_bytes([9; 16]));
    assert!(matches!(
        call(
            &client,
            "agents",
            &AgentCommand::Cancel {
                agent: stranger,
                id: IntentId::try_from("c1")?,
            }
        )
        .await?,
        Err(AgentControlFailure::Refused { code, .. }) if code == "no_such_agent"
    ));
    // Another namespace never reaches dispatch, and owner denial is final.
    assert!(
        call(&client, "elsewhere", &AgentCommand::Root {})
            .await
            .is_err()
    );
    owner.0.store(true, Ordering::SeqCst);
    assert!(
        call(&client, "agents", &AgentCommand::Root {})
            .await
            .is_err()
    );
    runner.abort();
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
                maximum_bytes: 64 * 1024,
                max_steps: 4,
                history_turns: 8,
            },
            actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            clock: Arc::new(SystemClock),
            approval_policy: Default::default(),
        },
    )?;
    let agent = AgentId::from_bytes([1; 16]);
    let private = VolumeBinding {
        name: PRIVATE_VOLUME.into(),
        volume: VolumeRef::new(
            provider()?,
            "agent-private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes(agent.to_bytes())),
            &mut None,
        )?,
    };
    let owner = Arc::new(Owner::default());
    let server = serve(
        bound,
        owner.clone(),
        &IntentId::try_from("root")?,
        RootRequest {
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
    owner.0.store(true, Ordering::SeqCst);
    let before = ledger.requests();
    assert!(call(&client, "agents", &head).await.is_err());
    assert!(ledger.requests() == before, "a denied head reads no part");
    Ok(())
}

#[test]
fn a_children_cursor_carries_its_hierarchy_version_and_last_path() -> Result {
    let version = CommitId::from_bytes([0xa5; 32]);
    let after = StreamPath::new("agents/root/children/a")?;
    let cursor = children_cursor(&version, &after);
    assert_eq!(parse_children_cursor(&cursor)?, (version, after));
    assert!(parse_children_cursor("agents/root/children/a").is_err());
    Ok(())
}
