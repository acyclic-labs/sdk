//! One client connection to an agent tree's [`crate::hosting::Server`]: its
//! session, its controls bound to the tree and the root they drive. The local
//! harness serves its own tree to an in-process session; a remote endpoint
//! serves one over gRPC. Every frontend drives either through the same
//! [`super::agent_conversation::Conversation`].
use super::{LocalHarness, storage};
use crate::{Error, Result, agent::AgentPath};
use acyclic_harness_client::{
    AgentCommand, AgentControls, AgentExchangeError, AgentReply, CommandError, PreparedCommand,
};
use acyclic_stream::session::Client;
use std::sync::Arc;

#[cfg(test)]
#[path = "endpoint_tests.rs"]
mod tests;

/// The connection's one credential slot: it drives and observes the root.
pub(super) const CREDENTIAL: u32 = 0;

/// A session bound to one served agent tree.
#[derive(Clone)]
pub struct Connection {
    pub(super) controls: AgentControls,
    pub(super) root: AgentPath,
    /// The `fs:/` resource announcing the root's tool output.
    pub(super) tools: String,
    /// The in-process harness serving the tree: a connection keeps it
    /// running, so its prompts' turns run for as long as any clone lives.
    _host: Option<Arc<LocalHarness>>,
}

fn exchange(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

impl Connection {
    /// A session `unavailable` names no cause: the in-process owner adds the
    /// one it retained, its stream's poison or else the Actors runtime failure
    /// that closed the session's lifetime.
    pub(super) fn stream_failure(&self, error: impl std::fmt::Display) -> Error {
        let cause = self._host.as_ref().and_then(|host| {
            host.agent_stream
                .as_ref()
                .and_then(|stream| stream.failure())
                .map(|cause| cause.to_string())
                .or_else(|| host.agents.get()?.runtime.failure())
        });
        match cause {
            Some(cause) => Error::Storage(format!("{error}; {cause}")),
            None => exchange(error),
        }
    }

    /// Executes `command` once on this connection's tree.
    ///
    /// # Errors
    /// The session's failure or the host's refusal.
    pub async fn execute(&self, command: &AgentCommand) -> Result<AgentReply> {
        let result = exchanged(&self.controls, command).await.map_err(exchange)?;
        match result {
            Err(
                error @ AgentExchangeError::Session(
                    acyclic_stream::StreamError::Unavailable
                    | acyclic_stream::StreamError::Refused(_),
                ),
            ) => Err(self.stream_failure(error)),
            result => result
                .map_err(|error| exchange(format_args!("{} command: {error}", command.kind()))),
        }
    }

    /// Executes `command` once, keeping its exchange failure typed.
    ///
    /// # Errors
    /// Only when `command` cannot be prepared, before anything is sent.
    pub(super) async fn exchange(&self, command: &AgentCommand) -> Exchange {
        exchanged(&self.controls, command).await
    }

    /// Select an existing descendant on the same authenticated session.
    /// Commands retain the original caller; a native host may enroll read-only
    /// access to the selected child's private output on that same session.
    pub async fn select(&self, agent: AgentPath) -> Result<Self> {
        if !self.root.contains(&agent) {
            return Err(Error::Unauthorized(
                "a selected agent must be the root or its descendant".into(),
            ));
        }
        let (controls, tools) = self
            .controls
            .select(agent.clone())
            .await
            .map_err(exchange)?;
        Ok(Self {
            controls,
            root: agent,
            tools,
            _host: self._host.clone(),
        })
    }

    /// The root this connection drives.
    #[must_use]
    pub const fn root(&self) -> &AgentPath {
        &self.root
    }

    /// A connection to the same agent that reads its committed records even
    /// after the in-process agents' runtime stopped: a delivery failure that
    /// closed this session never hides the calls, turns or history the tree
    /// already committed. A remote connection's session is its server's, so
    /// it is returned as it is.
    ///
    /// # Errors
    /// Fails when the agent's credential cannot be minted or bound.
    pub async fn observation(&self) -> Result<Self> {
        let Some(host) = &self._host else {
            return Ok(self.clone());
        };
        let root = host.agents.get().map(super::agents::Hosted::root);
        let agent = (root != Some(&self.root)).then(|| self.root.clone());
        host.attach(agent, false).await
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
        let credential = options
            .session_credential()
            .map_err(|_| Error::Unauthorized("credential is not a capability".into()))?;
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
        host: Option<Arc<LocalHarness>>,
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
            _host: host,
        })
    }
}

async fn execute(controls: &AgentControls, command: &AgentCommand) -> Result<AgentReply> {
    exchanged(controls, command)
        .await
        .map_err(exchange)?
        .map_err(exchange)
}

/// A command's exchange with its failure still typed. `Err` is a command
/// that never left: preparing it failed, so it committed nothing. `Ok` holds
/// what the sent request returned: a reply, a decoded refusal, an unknown
/// commit outcome or a session failure.
pub(super) type Exchange =
    std::result::Result<std::result::Result<AgentReply, AgentExchangeError>, CommandError>;

async fn exchanged(controls: &AgentControls, command: &AgentCommand) -> Exchange {
    let prepared = controls.prepare(PreparedCommand::new(command)?)?;
    Ok(controls.execute(&prepared).await)
}

impl LocalHarness {
    /// A connection to this harness's own agent tree, served in process
    /// exactly as an endpoint serves it, under one credential minted for its
    /// root.
    ///
    /// # Errors
    /// Fails for an embedded harness or when the tree cannot be hosted.
    pub async fn connect(self: &Arc<Self>) -> Result<Connection> {
        self.attach(None, true).await
    }

    /// One connection to `agent`'s tree. A `live` session closes with the
    /// agents' runtime, so no wait parks on a stopped producer. An observation
    /// session is not bound to it: what the tree already committed (usage,
    /// history, turns) stays readable after a delivery failure stops that
    /// runtime, and each read answers from those records.
    async fn attach(self: &Arc<Self>, agent: Option<AgentPath>, live: bool) -> Result<Connection> {
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
            live.then(|| hosted.runtime.lifetime()),
        );
        let connection =
            Connection::bind(session, observation.credential, Some(Arc::clone(self))).await?;
        match agent {
            Some(agent) => connection.select(agent).await,
            None => Ok(connection),
        }
    }
}
