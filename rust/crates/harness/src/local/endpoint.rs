//! One client connection to an agent tree's [`crate::hosting::Server`]: its
//! session, its controls bound to the tree and the root they drive. The local
//! harness serves its own tree to an in-process session; a remote endpoint
//! serves one over gRPC. Every frontend drives either through the same
//! [`super::agent_conversation::Conversation`].
use super::{AgentFailure, LocalHarness, storage};
use crate::{Error, Result, agent::AgentPath};
use acyclic_actors::addressing::ActorReference;
use acyclic_harness_client::{AgentCommand, AgentControls, AgentReply, PreparedCommand};
use acyclic_stream::session::Client;
use std::sync::Arc;
use tokio::sync::broadcast;

/// The connection's one credential slot: it drives and observes the root.
pub(super) const CREDENTIAL: u32 = 0;

/// Failed deliveries the in-process host reports for one actor, and the
/// harness hosting it: an in-process connection keeps its host running, so
/// its prompts' turns run for as long as any clone of it lives.
#[derive(Clone)]
pub(super) struct Failures {
    pub(super) sender: broadcast::Sender<AgentFailure>,
    pub(super) actor: ActorReference,
    pub(super) _host: Arc<LocalHarness>,
}

/// A session bound to one served agent tree.
#[derive(Clone)]
pub struct Connection {
    pub(super) controls: AgentControls,
    pub(super) root: AgentPath,
    /// The `fs:/` resource announcing the root's tool output.
    pub(super) tools: String,
    pub(super) failures: Option<Failures>,
}

fn exchange(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

impl Connection {
    /// Executes `command` once on this connection's tree.
    ///
    /// # Errors
    /// The session's failure or the host's refusal.
    pub async fn execute(&self, command: &AgentCommand) -> Result<AgentReply> {
        execute(&self.controls, command).await
    }

    /// The root this connection drives.
    #[must_use]
    pub const fn root(&self) -> &AgentPath {
        &self.root
    }

    /// Connects to the endpoint the one connection policy selects: an
    /// explicit `endpoint`, else `ACYCLIC_ENDPOINT`, else the managed
    /// default, with `ACYCLIC_API_KEY` as the credential. There is no config
    /// file and no local fallback.
    ///
    /// # Errors
    /// The policy's refusal by its stable code, such as
    /// `client_invalid_endpoint`, or as [`Self::connect`].
    #[cfg(feature = "grpc")]
    pub async fn connect_endpoint(endpoint: Option<&str>) -> Result<Self> {
        let options = acyclic_harness_client::ConnectionOptions::from_env(endpoint, None)
            .map_err(|error| Error::Invalid(error.code().into()))?;
        Self::connect(&options).await
    }

    /// Connects to the endpoint `options` names with its credential: a
    /// verified TLS endpoint, or a loopback one on this machine.
    ///
    /// # Errors
    /// When the endpoint cannot be reached, the credential is not one, or the
    /// endpoint binds it to no tree.
    #[cfg(feature = "grpc")]
    pub async fn connect(options: &acyclic_harness_client::ConnectionOptions) -> Result<Self> {
        use acyclic_harness_client::EndpointTransport;
        // Opaque to the client: the endpoint's verifier alone reads it.
        let credential = options.credential().as_bytes().to_vec();
        let client = match options.transport() {
            EndpointTransport::Tls => {
                acyclic_stream::grpc::Client::connect_eager(
                    options.endpoint(),
                    options.credential(),
                )
                .await
            }
            EndpointTransport::Loopback => {
                acyclic_stream::grpc::Client::connect_loopback(
                    options.endpoint(),
                    options.credential(),
                )
                .await
            }
        }
        .map_err(exchange)?;
        let session = client.session_client().map_err(exchange)?;
        Self::bind(session, credential, None).await
    }

    /// Binds `session`, holding `credential`, to the tree its endpoint
    /// serves that credential.
    async fn bind(
        session: Client,
        credential: Vec<u8>,
        failures: Option<Failures>,
    ) -> Result<Self> {
        session
            .register_credential(CREDENTIAL, credential.into())
            .map_err(exchange)?;
        let (controls, tools) = AgentControls::discover(session, CREDENTIAL)
            .await
            .map_err(exchange)?;
        let AgentReply::Root { agent: root } = execute(&controls, &AgentCommand::Root {}).await?
        else {
            return Err(Error::Conflict("the endpoint named no root".into()));
        };
        Ok(Self {
            controls,
            root,
            tools,
            failures,
        })
    }
}

async fn execute(controls: &AgentControls, command: &AgentCommand) -> Result<AgentReply> {
    let prepared = controls
        .prepare(PreparedCommand::new(command).map_err(exchange)?)
        .map_err(exchange)?;
    controls.execute(&prepared).await.map_err(exchange)
}

impl LocalHarness {
    /// A connection to this harness's own agent tree, served in process
    /// exactly as an endpoint serves it, under one credential minted for its
    /// root.
    ///
    /// # Errors
    /// Fails for an embedded harness or when the tree cannot be hosted.
    pub async fn connect(self: &Arc<Self>) -> Result<Connection> {
        self.agents().await?;
        let hosted = self
            .agents
            .get()
            .ok_or_else(|| Error::Conflict("agents are not hosted".into()))?;
        let server = Arc::clone(hosted.server());
        let stream = self
            .agent_stream
            .clone()
            .ok_or_else(|| Error::Unsupported("an embedded harness has no agent stream".into()))?;
        let bound = server.bound();
        let observation = bound
            .client_credential(server.root(), server.resource(), Arc::clone(&self.clock))
            .await
            .map_err(storage)?;
        let changes = bound.changes().map_err(storage)?;
        let session = Client::in_process_with_products(
            stream,
            observation.authority,
            Some(Arc::new(changes)),
            server.clone(),
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
            None,
        );
        let failures = Failures {
            sender: hosted.failures().clone(),
            actor: hosted.agents().actor(server.root()).map_err(storage)?,
            _host: Arc::clone(self),
        };
        Connection::bind(session, observation.credential, Some(failures)).await
    }
}
