//! The public endpoint vectors through GraphCoder's Actors composition.
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits};
use acyclic_auth::{authority::PolicyDecision, contract::Principal};
use acyclic_conformance::client::endpoint::{self, Scenario};
use acyclic_fs::{EmbeddedCapabilities, Fs, LogicalObjectStore, StreamAuthorityStore};
use acyclic_harness::{
    Capabilities, Error,
    agent::{AgentId, IntentId, WorkspaceEntry},
    context::CompactionPolicy,
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    filesystem::{PRIVATE_VOLUME, TurnLimits},
    hosting::{Placement, RootRequest, VerifiedCaller},
    model::{ModelDispatch, ModelProvider, PreparedModelRequest},
    resources::ProviderRef,
    server::{self, Actors, Execution, ServerConfig},
};
use acyclic_harness_client::AgentCommand;
use acyclic_stream::{BoxProviderFuture, MemoryStream, StreamCategory, StreamError};
use futures::{StreamExt as _, TryStreamExt as _};
use std::sync::{
    Arc, Mutex, PoisonError,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::oneshot;

type Outcome<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const ROOT: [u8; 16] = [91; 16];
const TASK_PROMPT: &str = "Answer the conformance task";

#[tokio::test]
async fn configured_volume_script_runs_a_hosted_swarm_and_fork() -> Outcome {
    use acyclic_harness::{
        IdempotencyKey,
        agent::{AgentPath, ConversationEntry, TurnOutcome, WorkspaceRole, decode_record},
        filesystem::{Grants, StockExecutors, WorkspaceMutation},
        hosting::{Bound, Serving, Turns, serve},
        model::ModelContent,
    };
    use acyclic_harness_client::{AgentBinding, AgentControls, AgentReply, PreparedCommand};
    use acyclic_harness_providers::{Credentials, ProviderConfig, Registry};
    use prost::Message as _;

    #[derive(serde::Deserialize)]
    struct OwnerConfig {
        model: ModelConfig,
    }
    #[derive(serde::Deserialize)]
    struct ModelConfig {
        provider: String,
        selection: String,
        spec: ProviderConfig,
        limits: Limits,
        compaction: CompactionPolicy,
    }
    let owner = std::fs::read(acyclic_test_support::crate_path!(
        "fixtures/owner.scripted.json"
    ))?;
    let ModelConfig {
        provider,
        selection,
        spec,
        limits,
        compaction,
    } = serde_json::from_slice::<OwnerConfig>(&owner)?.model;
    let ProviderConfig::Scripted(scripted) = &spec else {
        return Err("swarm owner must select a scripted provider".into());
    };
    let script_path = format!("/{}", scripted.script.path);
    let script = std::fs::read(acyclic_test_support::crate_path!(
        "fixtures/scripted-swarm.v1.json"
    ))?;
    let mut registry = Registry::new(Credentials::explicit(Default::default()))?;
    registry.register_config(provider, spec)?;
    assert!(!registry.needs_credentials(&selection)?);
    let names = acyclic_harness::tools::default_names();
    let tools = acyclic_harness::tools::registry(&names, |_| false, Vec::new())?;
    let grants = acyclic_harness::tools::call_grants(&tools, names.iter().map(String::as_str))?;
    let executors = StockExecutors::new(
        registry.resolve_binding(&selection)?,
        tools,
        (limits, compaction),
        Arc::new(Grants(grants)),
    )
    .with_agent_tools(&names)
    .with_templates()
    .with_plugins(Arc::new(acyclic_harness::plugins::Enabled::default()))
    .with_models(Arc::new(registry.into_model_resolver()));
    let stream = Arc::new(MemoryStream::default());
    let provider = ProviderRef::new(
        "scripted".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let agent = AgentId::from_bytes(ROOT);
    let bound = Bound::local_root(
        AuthorityRoot::new(
            "scripted-swarm",
            [91; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: agent.to_string(),
            },
        ),
        Placement {
            filesystem: Fs::new(
                StreamAuthorityStore::new(stream.clone()),
                LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
                EmbeddedCapabilities::MEMORY,
            ),
            provider: provider.clone(),
            within: ActorReference::new(b"test", b"scripted-swarm", b"root")?,
        },
        Turns {
            executors: Arc::new(executors),
            limits: TurnLimits {
                maximum_bytes: Some(1 << 20),
                history_turns: 8,
            },
            actors: ActorLimits::new(5_000, 1 << 20, 1 << 16)?,
            clock: Arc::new(acyclic_actors::local::SystemClock),
            approval_policy: acyclic_harness::approvals::ApprovalPolicy::accept_all("host"),
        },
    )?;
    let project = VolumeRef::new(
        provider.clone(),
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("swarm".into()),
        &mut None,
    )?;
    let workspace = bound.volumes().create_volume(&project).await?.workspace;
    bound
        .volumes()
        .apply(
            &workspace,
            None,
            &[WorkspaceMutation::PutFile {
                path: script_path,
                bytes: script.into(),
            }],
            &IdempotencyKey::new("script")?,
        )
        .await?;
    let private = acyclic_harness::conversation::private_volume(provider, &agent)?;
    let Serving::Served(server) = serve(
        bound,
        Arc::new(Owner(None)),
        &IntentId::try_from("scripted-root")?,
        RootRequest {
            peer: false,
            agent,
            fresh: vec![WorkspaceEntry::initial_volume(
                WorkspaceRole::Private,
                PRIVATE_VOLUME.into(),
                private,
            )],
            retained: vec![WorkspaceEntry::initial_volume(
                WorkspaceRole::User,
                "project".into(),
                project,
            )],
            brief: None,
            policy: None,
        },
    )
    .await?
    else {
        return Err("root submission was uncertain".into());
    };
    let bound = server.bound();
    let root = server.root();
    let observation = bound.client_credential(root, server.resource()).await?;
    let client = acyclic_stream::session::Client::in_process_with_products(
        stream,
        observation.authority,
        None,
        server.clone(),
        Some(server.clone()),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, observation.credential.into())?;
    let controls = AgentControls::new(
        client,
        0,
        AgentBinding::new(&server.binding().encode_to_vec())?,
    );
    let run = {
        let server = server.clone();
        tokio::spawn(async move { server.run(|_, _| {}).await })
    };
    let _abort = Abort(run.abort_handle());
    let prompt = async |text: &str| -> Outcome {
        let command = PreparedCommand::new(&AgentCommand::Prompt {
            peer: None,
            agent: root.clone(),
            id: IntentId::try_from(text)?,
            body: ModelContent::Text(text.into()),
        })?;
        controls.execute(&controls.prepare(command)?).await?;
        Ok(())
    };
    prompt("delegate").await?;
    let output =
        async |agent: &AgentPath, from: u64| -> Outcome<acyclic_harness::executor::TurnOutput> {
            let record = bound
                .agents()
                .follow_conversation(agent, from)
                .await?
                .try_next()
                .await?
                .ok_or("conversation did not settle")?;
            let entry: ConversationEntry = decode_record(&record.value)?;
            assert!(
                !matches!(entry.outcome, TurnOutcome::Failed { .. }),
                "{:?}",
                entry.outcome
            );
            let (bytes, _) = bound
                .read_conversation_file(
                    agent,
                    &entry.output,
                    record.sequence,
                    0,
                    entry.output.descriptor().byte_length(),
                )
                .await?;
            Ok(serde_json::from_slice(&bytes)?)
        };
    let parent_output = output(root, 0).await?;
    assert_eq!(parent_output.text, "root delegated");
    assert_eq!(parent_output.steps, 2);
    let children = PreparedCommand::new(&AgentCommand::Children {
        agent: root.clone(),
        page: Default::default(),
    })?;
    let AgentReply::Children { items, .. } = controls.execute(&controls.prepare(children)?).await?
    else {
        return Err("child page missing".into());
    };
    let [child] = items.as_slice() else {
        return Err("expected one spawned child".into());
    };
    let state = bound.agents().state(&child.agent).await?;
    assert_eq!(
        output(&child.agent, state.conversation_base).await?.text,
        "child settled"
    );
    assert_eq!(output(root, 1).await?.text, "root saw child settle");
    prompt("again").await?;
    assert_eq!(output(root, 2).await?.text, "root continued");
    let fork = PreparedCommand::new(&AgentCommand::Fork {
        parent: root.clone(),
        id: IntentId::try_from("scripted-fork")?,
        at: 0,
        brief: ModelContent::Text("fork".into()),
        role: "worker".into(),
        continuation: None,
    })?;
    let AgentReply::Spawned { agent: fork, .. } =
        controls.execute(&controls.prepare(fork)?).await?
    else {
        return Err("fork missing".into());
    };
    let state = bound.agents().state(&fork).await?;
    assert_eq!(state.conversation_base, 1);
    assert_eq!(
        output(&fork, state.conversation_base).await?.text,
        "fork settled"
    );
    run.abort();
    let _ = run.await;
    Ok(())
}

async fn listener() -> std::io::Result<tokio::net::TcpListener> {
    tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await
}

struct Abort(tokio::task::AbortHandle);
impl Drop for Abort {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Owner(Option<Mutex<Option<CommandGate>>>);
struct CommandGate {
    command: &'static str,
    skip: usize,
    entered: oneshot::Sender<()>,
    decision: oneshot::Receiver<()>,
}
impl acyclic_harness::hosting::Platform for Owner {
    fn policy<'a>(
        &'a self,
        _: &'a Principal,
        command: &'a AgentCommand,
    ) -> BoxProviderFuture<'a, Result<PolicyDecision, StreamError>> {
        let kind = match command {
            AgentCommand::Settlement { agent, .. } if agent.id_bytes() != ROOT => {
                "foreign_settlement"
            }
            _ => command.kind(),
        };
        let gate = self.0.as_ref().and_then(|gate| {
            let mut gate = gate.lock().unwrap_or_else(PoisonError::into_inner);
            let pending = gate.as_mut().filter(|gate| gate.command == kind)?;
            // Query policy runs again before disclosure. Only prompt retries
            // use earlier admissions to select an otherwise identical command.
            if pending.skip != 0 {
                pending.skip -= 1;
                None
            } else {
                gate.take()
            }
        });
        Box::pin(async move {
            if let Some(gate) = gate {
                let _ = gate.entered.send(());
                gate.decision.await.map_err(|_| StreamError::Unavailable)?;
            }
            Ok(PolicyDecision::Allow)
        })
    }
}

/// The shared host-suite script is gated by a real producer, which the negative
/// vector drops. No timeout invents model completion or an agent status.
#[derive(Default)]
struct ModelGate(Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>);

/// A real root credential is minted before its publication owner parks.
struct CredentialGate(Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>);

impl server::Verifier for CredentialGate {
    fn caller(
        &self,
        custody: &server::Custody<'_>,
        owner: &acyclic_harness::core::Scope,
    ) -> acyclic_harness::Result<VerifiedCaller> {
        server::Verifier::caller(&server::KeyFileVerifier, custody, owner)
    }

    fn observation<'a>(
        &'a self,
        root: server::RootCredential<'a>,
    ) -> BoxProviderFuture<'a, acyclic_harness::Result<Option<server::Observation>>> {
        let gate = self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
        Box::pin(async move {
            let (entered, published) =
                gate.ok_or_else(|| Error::Invalid("credential publication started twice".into()))?;
            let observation = server::Verifier::observation(&server::KeyFileVerifier, root).await?;
            let _ = entered.send(());
            published
                .await
                .map_err(|_| Error::Storage("credential publication producer closed".into()))?;
            Ok(observation)
        })
    }
}
impl ModelProvider for ModelGate {
    fn generate<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> acyclic_harness::model::ModelStream<'a> {
        use acyclic_harness::model::{ModelEvent, ModelStream};
        if dispatch.step == 1
            && acyclic_harness_core::test_support::is_retrospective(request.request())
        {
            return ModelStream::new(
                futures::stream::iter([
                    Ok(ModelEvent::Content {
                        delta: "none".into(),
                    }),
                    Ok(ModelEvent::Completed {
                        metadata: serde_json::Value::Null,
                    }),
                ])
                .boxed(),
            );
        }
        let gate = (dispatch.step == 0)
            .then(|| self.0.lock().unwrap_or_else(PoisonError::into_inner).take())
            .flatten();
        ModelStream::new(
            futures::stream::once(async move {
                let (entered, output) = gate.ok_or_else(|| {
                    Error::Invalid(format!(
                        "unexpected model dispatch at step {}",
                        dispatch.step
                    ))
                })?;
                let _ = entered.send(());
                output
                    .await
                    .map_err(|_| Error::Storage("model producer closed".into()))?;
                Ok::<_, Error>(futures::stream::iter(
                    acyclic_harness::conformance::protocol::model_events(1)
                        .into_iter()
                        .map(Ok),
                ))
            })
            .try_flatten()
            .boxed(),
        )
    }
}

fn composition(
    model: Arc<dyn ModelProvider>,
    owner: Owner,
) -> Outcome<Actors<MemoryStream, LogicalObjectStore<acyclic_objects::MemoryObjects>>> {
    composition_at(model, owner, b"root")
}

fn composition_at(
    model: Arc<dyn ModelProvider>,
    owner: Owner,
    member: &[u8],
) -> Outcome<Actors<MemoryStream, LogicalObjectStore<acyclic_objects::MemoryObjects>>> {
    let root = AgentId::from_bytes(ROOT);
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: root.to_string(),
    };
    let issuer = AuthorityRoot::new("hosted-actors", [91; 32], audience.clone());
    let capability =
        acyclic_fs::access::Capability::whole_volume(None, &acyclic_fs::access::Action::ALL)
            .to_scope_label()?;
    let scope = issuer.bootstrap_scope(
        "host",
        &Capabilities::new([
            capability,
            acyclic_harness::contract::capability::INTERACTION_ROUTE.to_owned(),
        ]),
        None,
        &mut None,
    )?;
    let provider = ProviderRef::new("actors".into(), "filesystem".into(), "1".into(), &mut None)?;
    let volume = VolumeRef::new(
        provider.clone(),
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(acyclic_harness::AgentId::from_bytes(root.to_bytes())),
        &mut None,
    )?;
    Ok(Actors::graphcoder(
        issuer,
        VerifiedCaller { audience, scope },
        Placement {
            filesystem: Fs::new(
                StreamAuthorityStore::new(Arc::new(MemoryStream::default())),
                LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
                EmbeddedCapabilities::MEMORY,
            ),
            provider,
            within: ActorReference::new(b"test", b"agents", member)?,
        },
        (
            IntentId::try_from("root")?,
            RootRequest {
                peer: true,
                agent: root,
                fresh: vec![WorkspaceEntry::initial_volume(
                    acyclic_harness_core::agent::WorkspaceRole::Private,
                    PRIVATE_VOLUME.into(),
                    volume,
                )],
                retained: Vec::new(),
                brief: None,
                policy: None,
            },
        ),
        Execution {
            clock: Arc::new(acyclic_actors::local::SystemClock),
            models: None,
            model: (
                acyclic_harness_core::model::Model::new(
                    "fixture".into(),
                    "test".into(),
                    "1".into(),
                )?,
                model,
            ),
            limits: Limits::default(),
            turns: TurnLimits {
                maximum_bytes: Some(1 << 20),
                history_turns: 8,
            },
            actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            compaction: CompactionPolicy::Disabled,
            tools: Vec::new(),
            default_tools: acyclic_harness::tools::default_names(),
            workspace: false,
            context: None,
            approvers: Default::default(),
            goals: Some(Default::default()),
        },
        Arc::new(owner),
    ))
}

#[tokio::test]
async fn selected_discovery_preserves_the_published_member() -> Outcome {
    use acyclic_harness_client::{
        AgentBinding, AgentControls, AgentExchangeError, PreparedCommand,
    };
    use prost::Message as _;

    let mut hosts = Vec::new();
    let mut stops = Vec::new();
    let mut clients = Vec::new();
    for member in [b"first".as_slice(), b"second".as_slice()] {
        let (ready, announced) = oneshot::channel();
        let (stop, stopped) = oneshot::channel();
        let host = tokio::spawn(server::serve(
            listener().await?,
            ServerConfig::new(composition_at(
                Arc::new(ModelGate::default()),
                Owner(None),
                member,
            )?),
            |endpoint| {
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
        ));
        let abort = Abort(host.abort_handle());
        let (url, credential) = announced.await?;
        let credential = credential.ok_or("missing hosted observation")?;
        let options = acyclic_harness_client::resolve_connection_options(
            Some(&url),
            Some(&credential),
            None,
            None,
        )?;
        clients.push(endpoint::connect(&options).await?);
        stops.push(stop);
        hosts.push((host, abort));
    }

    // Both credentials legitimately discover their own member. Routing must
    // still retain the caller's selected member rather than rediscover another.
    let (selected, _) = AgentControls::discover(clients[0].clone(), 0).await?;
    let binding = AgentBinding::new(&selected.binding_bytes())?;
    let root = acyclic_harness_client::AgentPath::root(&AgentId::from_bytes(ROOT));
    let foreign = AgentControls::new(clients[1].clone(), 0, binding);
    assert!(matches!(
        foreign.select(root.clone()).await,
        Err(AgentExchangeError::Session(error)) if error.category() == StreamCategory::AccessDenied
    ));
    let encoded = selected.binding_bytes();
    let published = acyclic_actors::wire::InvokeActorRequest::decode(encoded.as_slice())?;
    let binding = AgentBinding::new(&encoded)?;
    let command = PreparedCommand::new(&AgentCommand::Bind { agent: None })?;
    let routed = binding.routed("/agents/bind", command.command())?;
    // Fetch and session ingress attach the same command to the actual wire
    // publication, including every selected routing and deployment field.
    assert_eq!(binding.prepare(&command)?.invocations(), [routed.clone()]);
    clients[0].actor_call(&routed, 0, Some(1 << 20)).await?;
    assert!(matches!(
        clients[1].actor_call(&routed, 0, Some(1 << 20)).await,
        Err(error) if error.category() == StreamCategory::AccessDenied
    ));
    for field in ["account", "namespace"] {
        let mut other = published.clone();
        match field {
            "account" => other.account_id.push(b'x'),
            _ => other
                .namespace
                .as_mut()
                .ok_or("missing published namespace")?
                .push('x'),
        }
        assert!(matches!(
            AgentControls::discover_at(
                clients[0].clone(),
                0,
                AgentBinding::new(&other.encode_to_vec())?,
            )
            .await,
            Err(AgentExchangeError::Session(error)) if error.category() == StreamCategory::AccessDenied
        ));
    }
    let (bootstrapped, _) = AgentControls::discover_at(clients[0].clone(), 0, binding).await?;
    assert_eq!(bootstrapped.binding_bytes(), encoded);
    let (same, _) = bootstrapped.select(root).await?;
    assert_eq!(same.binding_bytes(), selected.binding_bytes());

    for stop in stops {
        let _ = stop.send(());
    }
    for (host, _abort) in hosts {
        host.await??;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum Producer {
    Complete,
    Model,
    Command {
        command: &'static str,
        skip: usize,
        case: &'static str,
    },
    Host,
    ReplyHost,
    AbortHost,
    ReplyAbortHost,
}

async fn startup_shutdown() -> Outcome {
    let (entered, pending) = oneshot::channel();
    let (publisher, published) = oneshot::channel();
    let mut config = ServerConfig::new(composition(Arc::new(ModelGate::default()), Owner(None))?);
    config.verifier = Arc::new(CredentialGate(Mutex::new(Some((entered, published)))));
    let listener = listener().await?;
    let (stop, stopping) = oneshot::channel::<()>();
    let announced = AtomicBool::new(false);
    let serving = server::serve(
        listener,
        config,
        |_| announced.store(true, Ordering::Relaxed),
        async {
            let _ = stopping.await;
        },
    );
    tokio::pin!(serving);
    tokio::select! {
        result = &mut serving => return Err(format!("startup completed before credential publication: {result:?}").into()),
        pending = pending => pending?,
    }
    drop(stop);
    assert!(matches!(
        serving.await,
        Err(server::ActorsError::Failed(Error::Cancelled))
    ));
    assert!(
        !announced.load(Ordering::Relaxed),
        "unfinished credential was announced"
    );
    drop(publisher);
    Ok(())
}

async fn startup_completion() -> Outcome {
    use server::Composition as _;
    let (entered, pending) = oneshot::channel();
    let (publisher, published) = oneshot::channel();
    let verifier: Arc<dyn server::Verifier> =
        Arc::new(CredentialGate(Mutex::new(Some((entered, published)))));
    let host = composition(Arc::new(ModelGate::default()), Owner(None))?
        .open(&verifier)
        .await?;
    let (owner, completion) = oneshot::channel::<()>();
    let mut config = ServerConfig::new(host.with_completion(async {
        completion
            .await
            .map_err(|_| Error::Storage("startup runtime producer closed".into()))
    }));
    config.verifier = verifier;
    let listener = listener().await?;
    let announced = AtomicBool::new(false);
    let serving = server::serve(
        listener,
        config,
        |_| announced.store(true, Ordering::Relaxed),
        std::future::pending(),
    );
    tokio::pin!(serving);
    tokio::select! {
        result = &mut serving => return Err(format!("startup completed before credential publication: {result:?}").into()),
        pending = pending => pending?,
    }
    drop(owner);
    assert!(
        matches!(serving.await, Err(Error::Storage(reason)) if reason == "startup runtime producer closed")
    );
    assert!(
        !announced.load(Ordering::Relaxed),
        "stopped runtime was announced"
    );
    drop(publisher);
    Ok(())
}

async fn run(producer: Producer) -> Outcome {
    let drop_model = matches!(producer, Producer::Model);
    let drop_reply_host = matches!(producer, Producer::ReplyHost | Producer::ReplyAbortHost);
    let drop_command = matches!(
        producer,
        Producer::Command { .. } | Producer::Host | Producer::AbortHost
    );
    let abort_host = matches!(producer, Producer::AbortHost | Producer::ReplyAbortHost);
    eprintln!(
        "STEP endpoint-vector start; closed-model={drop_model}; closed-command={drop_command}"
    );
    let (entered, entering) = oneshot::channel();
    let (output, generated) = oneshot::channel();
    let model = Arc::new(ModelGate(Mutex::new(Some((entered, generated)))));
    let (deciding, decision) = oneshot::channel();
    let (command, commanding) = oneshot::channel();
    let owner = Owner(drop_command.then(|| {
        Mutex::new(Some(CommandGate {
            command: match producer {
                Producer::Command { command, .. } => command,
                _ => "root",
            },
            skip: match producer {
                Producer::Command { skip, .. } => skip,
                _ => 0,
            },
            entered: command,
            decision,
        }))
    }));
    let listener = listener().await?;
    let (ready, endpoint) = oneshot::channel();
    let (stop, stopping) = oneshot::channel();
    let mut stop = Some(stop);
    let mut hosted = tokio::spawn(server::serve(
        listener,
        ServerConfig::new(composition(model, owner)?),
        |endpoint| {
            let _ = ready.send((
                endpoint.endpoint.clone(),
                endpoint
                    .observation
                    .as_ref()
                    .map(|observation| observation.credential.clone()),
            ));
        },
        async {
            let _ = stopping.await;
        },
    ));
    let _abort = Abort(hosted.abort_handle());
    // Preserve the server's cause when its endpoint producer closes.
    let Ok((url, credential)) = endpoint.await else {
        return Err(format!(
            "server stopped before its endpoint was ready: {:?}",
            hosted.await
        )
        .into());
    };
    let credential = credential.ok_or("missing hosted observation")?;
    let options = acyclic_harness_client::resolve_connection_options(
        Some(&url),
        Some(&credential),
        None,
        None,
    )?;
    let session = endpoint::connect(&options).await?;
    let scenario = Scenario {
        id: IntentId::try_from("live-client")?,
        prompt: TASK_PROMPT.into(),
        expected_text: acyclic_harness::conformance::protocol::REPLY.into(),
    };
    let (waiting, pending) = oneshot::channel();
    let waiting = Mutex::new(Some(waiting));
    let qualification = endpoint::verify_with_reply_wait(session, &scenario, || {
        if let Some(waiting) = waiting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            let _ = waiting.send(());
        }
    });
    // Command vectors keep the model producer alive and ready. A refusal of
    // a later command must not manufacture a second, unrelated model failure.
    let mut output = Some(output);
    if matches!(producer, Producer::Command { .. }) {
        let _ = output.take().ok_or("missing model producer")?.send(());
    }
    tokio::pin!(qualification);
    let result = tokio::select! {
        result = &mut qualification => result,
        pending = async {
            if drop_command { commanding.await?; } else { entering.await?; pending.await?; }
            Outcome::Ok(())
        } => {
            if let Err(error) = pending {
                let cause = tokio::select! {
                    result = &mut qualification => format!("endpoint: {result:?}"),
                    result = &mut hosted => format!("server: {result:?}"),
                };
                return Err(format!(
                    "producer gate closed before it was reached ({error}); {cause}"
                )
                .into());
            }
            eprintln!("STEP producer wait armed; closed-model={drop_model}; closed-command={drop_command}");
            if abort_host { hosted.abort(); }
            else if matches!(producer, Producer::Host | Producer::ReplyHost) { drop(stop.take()); }
            else if drop_command { drop(deciding); }
            if drop_model || drop_command { drop(output.take()); }
            else if !drop_reply_host && let Some(output) = output.take() { let _ = output.send(()); }
            qualification.await
        }
    };
    eprintln!(
        "STEP endpoint-vector completed; closed-model={drop_model}; closed-command={drop_command}"
    );
    if let Some(stop) = stop {
        let _ = stop.send(());
    }
    let serving = hosted.await;
    if drop_model {
        let failure = result
            .err()
            .ok_or("closed model producer completed the turn")?;
        assert_eq!(failure.case, "model_turn");
        assert!(
            matches!(
                &failure.cause,
                Some(acyclic_conformance::client::Cause::Turn(
                    acyclic_harness::agent::TurnOutcome::Failed { message },
                )) if message == concat!(
                    "model request interrupted before completing, and its provider cannot resume it: ",
                    "storage failure: model producer closed",
                )
            ),
            "closed model producer returned {failure:?}"
        );
    } else if drop_command || drop_reply_host {
        let failure = result
            .err()
            .ok_or("closed producer must fail the waiting client")?;
        assert!(
            matches!(
                &failure.cause,
                Some(acyclic_conformance::client::Cause::Stream(error))
                    if error.category() == StreamCategory::Unavailable
            ),
            "closed producer returned {failure:?}"
        );
        if drop_reply_host {
            assert_eq!(failure.case, "reply_liveness");
        } else if drop_command {
            assert_eq!(
                failure.case,
                match producer {
                    Producer::Command { case, .. } => case,
                    _ => "root",
                }
            );
        }
    } else {
        assert!(result?.passed.contains(&"model_turn"));
    }
    if abort_host {
        assert!(matches!(serving, Err(error) if error.is_cancelled()));
    } else {
        serving??;
    }
    Ok(())
}

#[tokio::test]
async fn graphcoder_on_actors_and_closed_producer_liveness() -> Outcome {
    startup_shutdown().await?;
    startup_completion().await?;
    run(Producer::Complete).await?;
    run(Producer::Model).await?;
    for (command, skip, case) in [
        ("bind", 0, "bind"),
        ("root", 0, "root"),
        ("children", 0, "discovery_pages"),
        ("approvals", 0, "discovery_pages"),
        ("prompt", 0, "prompt"),
        ("prompt", 1, "prompt_retry"),
        ("prompt", 2, "changed_prompt"),
        ("foreign_settlement", 0, "tree_admission"),
        ("settlement", 0, "reply_cut"),
        ("reply", 0, "reply"),
        ("read", 0, "output"),
    ] {
        run(Producer::Command {
            command,
            skip,
            case,
        })
        .await?;
    }
    run(Producer::Host).await?;
    run(Producer::ReplyHost).await?;
    run(Producer::AbortHost).await?;
    run(Producer::ReplyAbortHost).await?;
    // The connection producer is a real listening socket, not a scripted
    // HTTP response. Closing it must retain the original typed dial error.
    let listener = listener().await?;
    let url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let options = acyclic_harness_client::resolve_connection_options(
        Some(&url),
        Some("disposable-conformance"),
        None,
        None,
    )?;
    let failure = match endpoint::connect(&options).await {
        Ok(_) => return Err("closed listener admitted a connection".into()),
        Err(failure) => failure,
    };
    assert!(matches!(
        failure.cause,
        Some(acyclic_conformance::client::Cause::Connect(_))
    ));
    Ok(())
}
