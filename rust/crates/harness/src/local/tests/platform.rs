//! Agent behaviour vectors use both production compositions. Only storage,
//! admission and transport setup belong in this fixture.
use super::*;
use crate::{
    agent::{IntentId, WorkspaceEntry, WorkspaceRole},
    core::{AggregateKind, Authority, AuthorityRoot},
    filesystem::{PRIVATE_VOLUME, TurnLimits},
    hosting::{Placement, RootRequest, VerifiedCaller},
    local::Connection,
    server::{self, Actors, Execution, ServerConfig},
};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits};
use acyclic_auth::{authority::PolicyDecision, contract::Principal};
use acyclic_fs::{EmbeddedCapabilities, Fs, LogicalObjectStore, StreamAuthorityStore};
use acyclic_harness_client::AgentCommand;
use acyclic_stream::{MemoryStream, StreamError};
use tokio::sync::oneshot;

#[derive(Clone, Copy)]
pub(super) enum Mode {
    Local,
    Hosted,
}

pub(super) struct Platform {
    pub(super) connection: Connection,
    _directory: tempfile::TempDir,
    _local: Option<Arc<LocalHarness>>,
    serving: Option<tokio::task::JoinHandle<Result<()>>>,
    stop: Option<oneshot::Sender<()>>,
}

impl Drop for Platform {
    fn drop(&mut self) {
        if let Some(serving) = &self.serving {
            serving.abort();
        }
    }
}

struct Owner;
impl crate::hosting::Platform for Owner {
    fn policy<'a>(
        &'a self,
        _: &'a Principal,
        _: &'a AgentCommand,
    ) -> BoxProviderFuture<'a, std::result::Result<PolicyDecision, StreamError>> {
        Box::pin(async { Ok(PolicyDecision::Allow) })
    }
}

impl Mode {
    pub(super) async fn open(self, provider: Arc<ScriptedModelProvider>) -> Result<Platform> {
        let directory = tempfile::tempdir().map_err(super::super::storage)?;
        match self {
            Self::Local => {
                let project = volume(directory.path()).await?;
                let local = Arc::new(
                    open_scripted(
                        volume_config(directory.path(), project, provider)?
                            .without_default_tools()
                            .compaction(crate::context::CompactionPolicy::default()),
                    )
                    .await?,
                );
                Ok(Platform {
                    connection: local.connect().await?,
                    _directory: directory,
                    _local: Some(local),
                    serving: None,
                    stop: None,
                })
            }
            Self::Hosted => Self::hosted(directory, provider).await,
        }
    }

    async fn hosted(
        directory: tempfile::TempDir,
        provider: Arc<ScriptedModelProvider>,
    ) -> Result<Platform> {
        let root = crate::agent::AgentId::from_bytes([37; 16]);
        let audience = Authority {
            kind: AggregateKind::Task,
            id: "agent-conformance".into(),
        };
        let issuer = AuthorityRoot::new("agent-conformance", [37; 32], audience.clone());
        let filesystem = crate::resources::ProviderRef::new(
            "conformance".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?;
        let scope = issuer.bootstrap_scope(
            "owner",
            &Capabilities::new([
                acyclic_fs::access::Capability::whole_volume(
                    None,
                    &acyclic_fs::access::Action::ALL,
                )
                .to_scope_label()
                .map_err(super::super::storage)?,
                crate::contract::capability::INTERACTION_ROUTE.to_owned(),
            ]),
            None,
            &mut None,
        )?;
        let private = VolumeRef::new(
            filesystem.clone(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes(root.to_bytes())),
            &mut None,
        )?;
        let mut actors = Actors::graphcoder(
            issuer,
            VerifiedCaller { audience, scope },
            Placement {
                filesystem: Fs::new(
                    StreamAuthorityStore::new(Arc::new(MemoryStream::default())),
                    LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
                    EmbeddedCapabilities::MEMORY,
                ),
                provider: filesystem,
                within: ActorReference::new(b"test", b"agents", b"conformance")
                    .map_err(super::super::storage)?,
            },
            (
                IntentId::try_from("root").map_err(super::super::storage)?,
                RootRequest {
                    peer: true,
                    agent: root,
                    fresh: vec![WorkspaceEntry::initial_volume(
                        WorkspaceRole::Private,
                        PRIVATE_VOLUME.into(),
                        private,
                    )],
                    retained: Vec::new(),
                    brief: None,
                    policy: None,
                },
            ),
            Execution {
                models: None,
                model: (model()?, provider),
                limits: Default::default(),
                turns: TurnLimits {
                    maximum_bytes: None,
                    history_turns: 8,
                },
                actors: ActorLimits::default(),
                compaction: Default::default(),
                tools: Vec::new(),
                workspace: false,
                context: None,
                approvers: Default::default(),
                goals: None,
            },
            Arc::new(Owner),
        );
        actors.preset.approval_policy = crate::approvals::ApprovalPolicy::accept_all("conformance");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(super::super::storage)?;
        let (ready, announced) = oneshot::channel();
        let (stop, stopped) = oneshot::channel();
        let mut serving = tokio::spawn(async move {
            server::serve(
                listener,
                ServerConfig::new(actors),
                move |endpoint| {
                    let _ = ready.send((
                        endpoint.endpoint.clone(),
                        endpoint
                            .observation
                            .as_ref()
                            .map(|observation| observation.credential.clone()),
                    ));
                },
                async {
                    let _ = stopped.await;
                },
            )
            .await
            .map_err(super::super::storage)
        });
        let (endpoint, credential) = tokio::select! {
            endpoint = announced => endpoint.map_err(super::super::storage)?,
            result = &mut serving => {
                result.map_err(super::super::storage)??;
                return Err(Error::Storage("server ended before readiness".into()));
            }
        };
        let options = acyclic_harness_client::resolve_connection_options(
            Some(&endpoint),
            credential.as_deref(),
            None,
            None,
        )
        .map_err(super::super::storage)?;
        let platform = Platform {
            connection: Connection::connect(&options).await?,
            _directory: directory,
            _local: None,
            serving: Some(serving),
            stop: Some(stop),
        };
        // Ownership is installed before returning to the shared vector.
        platform.connection.require_execution(None, false).await?;
        Ok(platform)
    }
}

impl Platform {
    pub(super) async fn close(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(serving) = self.serving.take() {
            serving.await.map_err(super::super::storage)??;
        }
        Ok(())
    }
}

async fn summaries_remain_readable(mode: Mode) -> Result<()> {
    let provider =
        scripted(["FIRST", "SUMMARY", "AFTER"].map(|text| ScriptedTurn::Text(text.into())));
    let platform = mode.open(provider.clone()).await?;
    let conversation = Conversation::open(platform.connection.clone());
    let mut client = ObservedConversation::observe(conversation, None).await?;
    assert!(
        client
            .send("first", "remember the first fact")
            .await?
            .outcome
            .is_completed()
    );
    let compact = client
        .conversation
        .compact("compact", Some("retain the fact"))
        .await?;
    let (sequence, entry) = client.settled_at(compact).await?;
    assert!(entry.outcome.is_completed(), "{:?}", entry.outcome);
    assert_eq!(
        client.conversation.turn(sequence, &entry).await?.answer,
        "SUMMARY"
    );
    let after = client.send("after", "use the retained fact").await?;
    assert!(after.outcome.is_completed(), "{:?}", after.outcome);
    assert_eq!(provider.request_count(), 3);
    assert_eq!(provider.remaining(), 0);
    drop(client);
    platform.close().await
}

#[tokio::test]
async fn local_summaries_remain_readable() -> Result<()> {
    summaries_remain_readable(Mode::Local).await
}

#[tokio::test]
async fn hosted_summaries_remain_readable() -> Result<()> {
    summaries_remain_readable(Mode::Hosted).await
}
