//! Standard in-process endpoint fixture over each backend's real storage.
use super::*;
use crate::conformance::{Storage, observe::Observed};
use crate::hosting::{self, Bound, Placement, RootRequest, Server, Serving, Turns, VerifiedCaller};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock, wire};
use acyclic_auth::{authority::PolicyDecision, contract::Principal};
use acyclic_fs::{Fs, StreamAuthorityStore};
use acyclic_harness_core::{
    Capabilities,
    context::CompactionPolicy,
    conversation::Limits,
    core::{AggregateKind, Authority, AuthorityRoot},
    filesystem::{Grants, PRIVATE_VOLUME, TurnLimits},
    model::{Model, ModelDispatch, ModelProvider, PreparedModelRequest},
    preset::Preset,
    tool::{JsonToolProjection, Tool, ToolExecutor, ToolInvocation, ToolRegistry, ToolResult},
};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{BoxProviderStream, session::Client};
use futures::TryFutureExt as _;
use prost::Message as _;
use std::sync::atomic::{AtomicUsize, Ordering};

type Objects = crate::conformance::ObjectStore;
struct Owner;
impl hosting::Platform for Owner {
    fn policy<'a>(
        &'a self,
        _: &'a Principal,
        _: &'a AgentCommand,
    ) -> BoxProviderFuture<'a, std::result::Result<PolicyDecision, StreamError>> {
        Box::pin(async { Ok(PolicyDecision::Allow) })
    }
}

struct Scripted;
impl ModelProvider for Scripted {
    fn generate<'a>(
        &'a self,
        _: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> BoxProviderStream<'a, acyclic_harness_core::Result<ModelEvent>> {
        Box::pin(futures::stream::iter(
            model_events(dispatch.step).into_iter().map(Ok),
        ))
    }
}

struct Echo(Arc<AtomicUsize>);
impl ToolExecutor for Echo {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<acyclic_harness_core::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<ToolResult>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(ToolResult {
                value: invocation.arguments,
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<acyclic_harness_core::runtime::ToolContext>,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

struct Embedded {
    _storage: Box<dyn Storage>,
    bound: Bound<Observed, Objects>,
    server: Arc<Server<Observed, Objects>>,
    client: Client,
    binding: Vec<u8>,
    effects: Arc<AtomicUsize>,
}

/// The shared fixture composition for local and browser backend entries.
/// Hosted backend entries instead return their actual platform endpoint.
pub async fn open<B: Backend + ?Sized>(
    backend: &B,
    policy: ApprovalPolicy,
) -> Result<Box<dyn Endpoint>> {
    let storage = backend.fresh().await?;
    let stream = Arc::new(Observed::new(storage.open().await?));
    let objects = storage.objects().await?;
    let capabilities = objects.capabilities;
    let objects = objects.into_store(storage.as_ref())?;
    let audience = Authority {
        kind: AggregateKind::Task,
        id: "protocol".into(),
    };
    let issuer = AuthorityRoot::new("conformance", [7; 32], audience.clone());
    let grant =
        acyclic_fs::access::Capability::whole_volume(None, &acyclic_fs::access::Action::ALL)
            .to_scope_label()?;
    let scope = issuer.bootstrap_scope(
        "host",
        &Capabilities::new([
            grant,
            acyclic_harness_core::contract::capability::INTERACTION_ROUTE.to_owned(),
        ]),
        None,
        &mut None,
    )?;
    let preset = Preset {
        approval_policy: policy,
    };
    let effects = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolRegistry::default();
    tools.register(
        (
            Tool {
                skill_source: None,
                hook_names: &[],
                definition: tool_definition(),
                executor: Arc::new(Echo(effects.clone())),
                projection: Arc::new(JsonToolProjection),
            },
            None,
        ),
        &mut None,
        None,
    )?;
    let executors = preset.executors(
        (
            Model::new("conformance".into(), "scripted".into(), "1".into())?,
            Arc::new(Scripted),
        ),
        tools,
        (
            Limits {
                model_steps: 4,
                ..Limits::default()
            },
            CompactionPolicy::Disabled,
        ),
        Arc::new(Grants(vec![format!("tool:call:{TOOL}")])),
    );
    let bound = Bound::admitted(
        issuer,
        VerifiedCaller { audience, scope },
        Placement {
            filesystem: Fs::new(StreamAuthorityStore::new(stream), objects, capabilities),
            provider: crate::conformance::roots::volume(
                PRIVATE_VOLUME,
                crate::conformance::fixtures::adopted().agent,
            )?
            .volume
            .provider()
            .clone(),
            within: ActorReference::new(b"account", b"agents", b"host")?,
        },
        Turns {
            executors: Arc::new(executors),
            limits: TurnLimits {
                maximum_bytes: 1 << 20,
                max_steps: 4,
                history_turns: 8,
            },
            actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            clock: Arc::new(SystemClock),
            approval_policy: preset.approval_policy,
        },
    )?;
    let handler = backend.handler(bound.handler())?;
    let bound = bound.hosting(handler)?;
    let Serving::Served(server) = hosting::serve(
        bound.clone(),
        Arc::new(Owner),
        &IntentId::try_from("root")?,
        RootRequest {
            profile: None,
            agent: crate::conformance::fixtures::adopted().agent,
            fresh: vec![crate::conformance::roots::volume(
                PRIVATE_VOLUME,
                crate::conformance::fixtures::adopted().agent,
            )?],
            retained: Vec::new(),
            brief: None,
            policy: None,
        },
    )
    .await?
    else {
        return Err("fixture root was uncertain".into());
    };
    let client = Client::in_process_with_products(
        bound.agents().provider().clone(),
        acyclic_stream::session::testing::authority(),
        None,
        server.clone(),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, credential(server.resource()))?;
    let binding = wire::InvokeActorRequest {
        actor_id: "root".into(),
        method: "POST".into(),
        url: "https://agents.invalid/command".into(),
        namespace: Some("agents".into()),
        account_id: b"account".to_vec(),
        ..Default::default()
    }
    .encode_to_vec();
    Ok(Box::new(Embedded {
        _storage: storage,
        bound,
        server,
        client,
        binding,
        effects,
    }))
}

fn credential(resource: &str) -> Bytes {
    use acyclic_auth::contract::*;
    acyclic_auth::testing::root(|block| {
        if let Some(key) = block
            .certificates
            .as_mut()
            .and_then(|chain| chain.keys.first_mut())
        {
            key.key.principal.id = "human".into();
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
        }
    })
    .into()
}

impl Endpoint for Embedded {
    fn binding(&self) -> &[u8] {
        &self.binding
    }
    fn provider(&self) -> Arc<dyn StreamProvider> {
        self.bound.agents().provider().clone()
    }
    fn exchange(&self, invocation: Bytes) -> BoxProviderFuture<'_, Result<Bytes>> {
        Box::pin(async move {
            let call = acyclic_harness_client::AgentCall::decode(&invocation)?;
            let AgentCommand::Spawn { parent, .. } = call.command() else {
                return Ok(self
                    .client
                    .actor_call(
                        &invocation,
                        0,
                        acyclic_harness_client::MAX_AGENT_REPLY_BYTES,
                    )
                    .await?);
            };
            let path = parent.inbox()?;
            let provider = self.provider();
            let from = provider.tail(path.clone()).await?;
            let driving = async {
                let mut input = provider
                    .follow(acyclic_stream::FollowRequest::new(path, from))
                    .map_ok(acyclic_stream::records_only)
                    .await?;
                input.try_next().await?.ok_or("spawn inbox follow ended")?;
                self.server.drain(parent).await?;
                futures::future::pending::<Result<Bytes>>().await
            };
            let result = self.client.actor_call(
                &invocation,
                0,
                acyclic_harness_client::MAX_AGENT_REPLY_BYTES,
            );
            futures::pin_mut!(driving, result);
            match futures::future::select(result, driving).await {
                futures::future::Either::Left((result, _)) => Ok(result?),
                futures::future::Either::Right((result, _)) => result,
            }
        })
    }
    fn drain<'a>(&'a self, agent: &'a AgentPath) -> BoxProviderFuture<'a, Result> {
        Box::pin(async move {
            self.server.drain(agent).await?;
            Ok(())
        })
    }
    fn read_file<'a>(
        &'a self,
        agent: &'a AgentPath,
        file: &'a FileRef,
    ) -> BoxProviderFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            let (bytes, _) = self.bound.read_file(agent, file, 0, u64::MAX).await?;
            Ok(bytes)
        })
    }
    fn effects(&self) -> BoxProviderFuture<'_, Result<u64>> {
        Box::pin(async { Ok(self.effects.load(Ordering::SeqCst) as u64) })
    }
}
