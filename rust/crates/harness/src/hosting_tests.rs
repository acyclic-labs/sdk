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

#[tokio::test]
async fn a_served_tree_dispatches_session_commands_within_its_admission() -> Result {
    let stream = Arc::new(MemoryStream::default());
    let objects = acyclic_objects::MemoryObjects::memory();
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
