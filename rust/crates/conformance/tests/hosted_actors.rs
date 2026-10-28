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
    model::{Model, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest},
    resources::ProviderRef,
    server::{self, Actors, Execution, ServerConfig},
};
use acyclic_harness_client::AgentCommand;
use acyclic_stream::{BoxProviderFuture, BoxProviderStream, MemoryStream, StreamError};
use futures::{StreamExt as _, TryStreamExt as _};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

type Outcome<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Abort(tokio::task::AbortHandle);
impl Drop for Abort {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Owner(Option<Mutex<Option<CommandGate>>>);
struct CommandGate {
    entered: oneshot::Sender<()>,
    decision: oneshot::Receiver<()>,
}
impl acyclic_harness::hosting::Platform for Owner {
    fn policy<'a>(
        &'a self,
        _: &'a Principal,
        command: &'a AgentCommand,
    ) -> BoxProviderFuture<'a, Result<PolicyDecision, StreamError>> {
        let gate = if matches!(command, AgentCommand::Root {}) {
            self.0.as_ref().and_then(|gate| gate.lock().unwrap().take())
        } else {
            None
        };
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
impl ModelProvider for ModelGate {
    fn generate<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        _: ModelDispatch,
    ) -> BoxProviderStream<'a, acyclic_harness::Result<ModelEvent>> {
        let system = request.request().messages.iter().any(|message| {
            matches!(&message.content, acyclic_harness::model::ModelContent::Text(text)
                if text.contains("You are GraphCoder"))
        });
        let entered = self.entered.lock().unwrap().take();
        let output = self.output.lock().unwrap().take();
        futures::stream::once(async move {
            if !system {
                return Err(Error::Invalid("GraphCoder prompt missing".into()));
            }
            if let Some(entered) = entered {
                let _ = entered.send(());
            }
            output
                .ok_or_else(|| Error::Invalid("model dispatched twice".into()))?
                .await
                .map_err(|_| Error::Storage("model producer closed".into()))?;
            Ok(futures::stream::iter(
                acyclic_harness::conformance::protocol::model_events(1)
                    .into_iter()
                    .map(Ok),
            ))
        })
        .try_flatten()
        .boxed()
    }
}

fn composition(
    model: Arc<dyn ModelProvider>,
    owner: Owner,
) -> Outcome<Actors<MemoryStream, LogicalObjectStore<acyclic_objects::MemoryObjects>>> {
    let root = AgentId::from_bytes([91; 16]);
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
            within: ActorReference::new(b"test", b"agents", b"root")?,
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
                Model::new(
                    "conformance".into(),
                    "scripted".into(),
                    "1".into(),
                    serde_json::Value::Null,
                )?,
                model,
            ),
            limits: Limits::default(),
            turns: TurnLimits {
                maximum_bytes: 1 << 20,
                max_steps: 4,
                history_turns: 8,
            },
            actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            compaction: CompactionPolicy::Disabled,
            tools: Vec::new(),
            workspace: false,
            context: None,
        },
        Arc::new(owner),
    ))
}

#[derive(Clone, Copy, Debug)]
enum Producer {
    Complete,
    Model,
    Command,
    Host,
}

async fn run(producer: Producer) -> Outcome {
    let drop_model = matches!(producer, Producer::Model);
    let drop_command = matches!(producer, Producer::Command | Producer::Host);
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
            entered: command,
            decision,
        }))
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:15439").await?;
    let (ready, endpoint) = oneshot::channel();
    let (stop, stopping) = oneshot::channel();
    let mut stop = Some(stop);
    let hosted = tokio::spawn(server::serve(
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
    let (url, credential) = endpoint.await?;
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
    tokio::pin!(qualification);
    let result = tokio::select! {
        result = &mut qualification => result,
        pending = async {
            if drop_command { commanding.await?; } else { entering.await?; pending.await?; }
            Outcome::Ok(())
        } => {
            pending?;
            eprintln!("STEP producer wait armed; closed-model={drop_model}; closed-command={drop_command}");
            if matches!(producer, Producer::Host) { drop(stop.take()); }
            else if drop_command { drop(deciding); }
            if drop_model || drop_command { drop(output); } else { let _ = output.send(()); }
            qualification.await
        }
    };
    eprintln!(
        "STEP endpoint-vector completed; closed-model={drop_model}; closed-command={drop_command}"
    );
    if let Some(stop) = stop {
        let _ = stop.send(());
    }
    let serving = hosted.await?;
    if drop_model || drop_command {
        let failure = result.expect_err("closed producer must fail the waiting client");
        assert!(matches!(
            failure.cause,
            Some(acyclic_conformance::client::Cause::Stream(
                StreamError::Unavailable
            ))
        ));
        if drop_command {
            assert_eq!(failure.case, "root");
            serving?;
        } else {
            assert!(serving.is_err());
        }
    } else {
        assert!(result?.passed.contains(&"model_turn"));
        serving?;
    }
    Ok(())
}

#[tokio::test]
async fn graphcoder_on_actors_and_closed_producer_liveness() -> Outcome {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        run(Producer::Complete).await?;
        run(Producer::Model).await?;
        run(Producer::Command).await?;
        run(Producer::Host).await?;
        // The connection producer is a real listening socket, not a scripted
        // HTTP response. Closing it must retain the original typed dial error.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:15439").await?;
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
    })
    .await?
}
