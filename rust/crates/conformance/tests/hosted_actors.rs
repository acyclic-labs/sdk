//! The public endpoint vectors through GraphCoder's Actors composition.
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits};
use acyclic_auth::{authority::PolicyDecision, contract::Principal};
use acyclic_conformance::client::endpoint::{self, Scenario};
use acyclic_fs::{EmbeddedCapabilities, Fs, LogicalObjectStore, StreamAuthorityStore};
use acyclic_harness::{
    Capabilities, Error,
    agent::{AgentId, IntentId, VolumeBinding},
    context::CompactionPolicy,
    conversation::{Limits, VolumeClass, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    filesystem::{PRIVATE_VOLUME, TurnLimits},
    hosting::{Placement, RootRequest, VerifiedCaller},
    model::{Model, ModelDispatch, ModelProvider, PreparedModelRequest},
    resources::ProviderRef,
    server::{self, Actors, Execution, ServerConfig},
};
use acyclic_harness_client::AgentCommand;
use acyclic_stream::{BoxProviderFuture, MemoryStream, StreamError};
use futures::{StreamExt as _, TryStreamExt as _};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

type Outcome<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const ROOT: [u8; 16] = [91; 16];

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
            AgentCommand::Settlement { agent } if agent.id_bytes() != ROOT => "foreign_settlement",
            _ => command.kind(),
        };
        let gate = self.0.as_ref().and_then(|gate| {
            let mut gate = gate.lock().unwrap();
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
struct ModelGate {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    output: Mutex<Option<oneshot::Receiver<()>>>,
}

/// A real root credential is minted before its publication owner parks.
struct CredentialGate {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    published: Mutex<Option<oneshot::Receiver<()>>>,
}

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
        let entered = self.entered.lock().unwrap().take().unwrap();
        let published = self.published.lock().unwrap().take().unwrap();
        Box::pin(async move {
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
        _: &'a PreparedModelRequest,
        _: ModelDispatch,
    ) -> acyclic_harness::model::ModelStream<'a> {
        let entered = self.entered.lock().unwrap().take();
        let output = self.output.lock().unwrap().take();
        acyclic_harness::model::ModelStream::new(
            futures::stream::once(async move {
                if let Some(entered) = entered {
                    let _ = entered.send(());
                }
                output
                    .ok_or_else(|| Error::Invalid("model dispatched twice".into()))?
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
                agent: root,
                fresh: vec![VolumeBinding {
                    name: PRIVATE_VOLUME.into(),
                    volume,
                }],
                retained: Vec::new(),
                brief: None,
                policy: None,
            },
        ),
        Execution {
            model: (
                Model::new("conformance".into(), "scripted".into(), "1".into())?,
                model,
            ),
            limits: Limits::default(),
            turns: TurnLimits {
                maximum_bytes: 1 << 20,
                history_turns: 8,
            },
            actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            compaction: CompactionPolicy::Disabled,
            tools: Vec::new(),
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
                Arc::new(ModelGate {
                    entered: Mutex::new(None),
                    output: Mutex::new(None),
                }),
                Owner(None),
                member,
            )?),
            |endpoint| {
                let _ = ready.send((
                    endpoint.endpoint.clone(),
                    endpoint.observation.as_ref().unwrap().credential.clone(),
                ));
            },
            async {
                let _ = stopped.await;
            },
        ));
        let abort = Abort(host.abort_handle());
        let (url, credential) = announced.await?;
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
        Err(AgentExchangeError::Session(StreamError::AccessDenied))
    ));
    let encoded = selected.binding_bytes();
    let published = acyclic_actors::wire::InvokeActorRequest::decode(encoded.as_slice())?;
    let binding = AgentBinding::new(&encoded)?;
    let command = PreparedCommand::new(&AgentCommand::Bind { agent: None })?;
    let routed = binding.routed("/agents/bind", command.command())?;
    // Fetch and session ingress attach the same command to the actual wire
    // publication, including every selected routing and deployment field.
    assert_eq!(binding.prepare(&command)?.invocations(), [routed.clone()]);
    clients[0].actor_call(&routed, 0, 1 << 20).await?;
    assert!(matches!(
        clients[1].actor_call(&routed, 0, 1 << 20).await,
        Err(StreamError::AccessDenied)
    ));
    for field in ["account", "namespace"] {
        let mut other = published.clone();
        match field {
            "account" => other.account_id.push(b'x'),
            _ => other.namespace.as_mut().unwrap().push('x'),
        }
        assert!(matches!(
            AgentControls::discover_at(
                clients[0].clone(),
                0,
                AgentBinding::new(&other.encode_to_vec())?,
            )
            .await,
            Err(AgentExchangeError::Session(StreamError::AccessDenied))
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
    let mut config = ServerConfig::new(composition(
        Arc::new(ModelGate {
            entered: Mutex::new(None),
            output: Mutex::new(None),
        }),
        Owner(None),
    )?);
    config.verifier = Arc::new(CredentialGate {
        entered: Mutex::new(Some(entered)),
        published: Mutex::new(Some(published)),
    });
    let listener = listener().await?;
    let (stop, stopping) = oneshot::channel::<()>();
    let serving = server::serve(
        listener,
        config,
        |_| panic!("unfinished credential was announced"),
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
    drop(publisher);
    Ok(())
}

async fn startup_completion() -> Outcome {
    use server::Composition as _;
    let (entered, pending) = oneshot::channel();
    let (publisher, published) = oneshot::channel();
    let verifier: Arc<dyn server::Verifier> = Arc::new(CredentialGate {
        entered: Mutex::new(Some(entered)),
        published: Mutex::new(Some(published)),
    });
    let host = composition(
        Arc::new(ModelGate {
            entered: Mutex::new(None),
            output: Mutex::new(None),
        }),
        Owner(None),
    )?
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
    let serving = server::serve(
        listener,
        config,
        |_| panic!("stopped runtime was announced"),
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
    let model = Arc::new(ModelGate {
        entered: Mutex::new(Some(entered)),
        output: Mutex::new(Some(generated)),
    });
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
                endpoint.observation.as_ref().unwrap().credential.clone(),
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
    let options = acyclic_harness_client::resolve_connection_options(
        Some(&url),
        Some(&credential),
        None,
        None,
    )?;
    let session = endpoint::connect(&options).await?;
    let scenario = Scenario {
        id: IntentId::try_from("live-client")?,
        prompt: "Answer the conformance task".into(),
        expected_text: acyclic_harness::conformance::protocol::REPLY.into(),
    };
    let (waiting, pending) = oneshot::channel();
    let waiting = Mutex::new(Some(waiting));
    let qualification = endpoint::verify_with_reply_wait(session, &scenario, || {
        if let Some(waiting) = waiting.lock().unwrap().take() {
            let _ = waiting.send(());
        }
    });
    // Command vectors keep the model producer alive and ready. A refusal of
    // a later command must not manufacture a second, unrelated model failure.
    let mut output = Some(output);
    if matches!(producer, Producer::Command { .. }) {
        let _ = output.take().unwrap().send(());
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
    if drop_model || drop_command || drop_reply_host {
        let failure = result.expect_err("closed producer must fail the waiting client");
        assert!(
            matches!(
                failure.cause,
                Some(acyclic_conformance::client::Cause::Stream(
                    StreamError::Unavailable
                ))
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
    } else if drop_model {
        assert!(serving?.is_err());
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
