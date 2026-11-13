//! One client connection to an agent tree's [`crate::hosting::Server`]: its
//! session, its controls bound to the tree and the root they drive. The local
//! harness serves its own tree to an in-process session; a remote endpoint
//! serves one over gRPC. Every frontend drives either through the same
//! [`super::agent_conversation::Conversation`].
use super::{LocalHarness, storage};
use crate::{Error, ExchangeStep, Result, agent::AgentPath};
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
    pub(super) session: Client,
    directory: bool,
    // One immutable authenticated selection, shared by commands and follows.
    pub(super) controls: Arc<AgentControls>,
    pub(super) root: AgentPath,
    /// The `fs:/` resource announcing the root's tool output.
    pub(super) tools: String,
    /// The in-process harness serving the tree: a connection keeps it
    /// running, so its prompts' turns run for as long as any clone lives.
    pub(super) _host: Option<Arc<LocalHarness>>,
    owner_exit: Option<super::service::exit::Observation>,
}

/// The client failure with its original in-process or recorded owner cause.
#[derive(Debug, thiserror::Error)]
enum OwnerFailure<E: std::error::Error + 'static> {
    #[error("{client}; {owner}")]
    Stream {
        client: E,
        #[source]
        owner: acyclic_stream::LocalStreamFailure,
    },
    #[error("{client}; {owner}")]
    Runtime {
        #[source]
        client: E,
        owner: String,
    },
    #[error("{client}; {owner}")]
    Recorded {
        client: E,
        #[source]
        owner: OwnerCause,
    },
}

#[derive(Debug, thiserror::Error)]
enum OwnerCause {
    #[error("local owner{0}")]
    Recorded(#[source] acyclic_auth::refusal::Refusal),
    #[error("cannot read the original owner's exit record: {0}")]
    Unreadable(#[source] Error),
}

impl OwnerCause {
    fn recorded(observation: &super::service::exit::Observation) -> Self {
        match observation.cause() {
            Ok(cause) => Self::Recorded(cause),
            Err(error) => Self::Unreadable(error),
        }
    }
}

impl Connection {
    /// Bind an OS-authenticated local channel using its in-memory capability.
    pub(super) async fn connect_local(directory: super::service::Directory) -> Result<Self> {
        let credential = Arc::new(std::sync::OnceLock::new());
        let bootstrap = Arc::clone(&credential);
        let channel = tonic::transport::Endpoint::from_static("http://local")
            .connect_with_connector(tower::service_fn(move |_| {
                let directory = directory.clone();
                let bootstrap = Arc::clone(&bootstrap);
                async move {
                    let (connection, credential, owner_exit) = directory
                        .connection()
                        .await
                        .map_err(|error| std::io::Error::other(error.to_string()))?;
                    // The session retains its original authority across
                    // authenticated transport reconnects. The server validates
                    // that capability on every reopened session.
                    bootstrap.get_or_init(|| (credential, owner_exit));
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(connection))
                }
            }))
            .await
            .map_err(|error| {
                Error::exchange(ExchangeStep::Step("connect to the local owner"), error)
            })?;
        let (credential, owner_exit) = credential
            .get()
            .ok_or_else(|| Error::Storage("local connection did not bootstrap".into()))?;
        let bytes = acyclic_auth::credential::capability_from_text(credential)
            .map_err(|_| Error::Unauthorized("credential is not a capability".into()))?;
        let client = acyclic_stream::grpc::Client::from_channel_with_credential(
            channel,
            acyclic_auth::credential::CredentialProvider::transport_identity(),
        );
        let mut connection = Self::bind(
            client.session_client().map_err(|error| {
                Error::exchange(ExchangeStep::Step("open the local owner session"), error)
            })?,
            bytes,
            None,
            false,
        )
        .await
        .map_err(|error| {
            let unavailable = match &error {
                Error::Exchange { source, .. } => {
                    source
                        .downcast_ref::<AgentExchangeError>()
                        .is_some_and(|cause| cause.code() == "unavailable")
                        || source
                            .downcast_ref::<acyclic_stream::StreamError>()
                            .is_some_and(|cause| {
                                cause.category() == acyclic_stream::StreamError::Unavailable
                            })
                }
                _ => false,
            };
            if !unavailable {
                return error;
            }
            Error::exchange(
                ExchangeStep::Session,
                OwnerFailure::Recorded {
                    client: error,
                    owner: OwnerCause::recorded(owner_exit),
                },
            )
        })?;
        connection.owner_exit = Some(owner_exit.clone());
        Ok(connection)
    }

    /// Refuse explicit launch requirements before creating a session or sending input.
    /// With no explicit requirement this performs no control request.
    pub async fn require_execution(&self, model: Option<&str>, no_compaction: bool) -> Result<()> {
        if model.is_none() && !no_compaction {
            return Ok(());
        }
        match self.execute(&AgentCommand::Settlement {
            agent: self.root.clone(),
            wait: false,
        }).await? {
            AgentReply::Settlement { model: actual, compaction_disabled, .. }
                if model.is_none_or(|expected| actual.as_deref() == Some(expected))
                    && (!no_compaction || compaction_disabled) => Ok(()),
            AgentReply::Settlement { .. } => Err(Error::Invalid(
                "selected owner or session does not meet the requested model or --no-compaction policy; start with a fresh --host-state-directory".into(),
            )),
            _ => Err(Error::Storage("execution policy query returned another reply".into())),
        }
    }

    /// Refuses an existing owner or selected session whose immutable policy
    /// differs from the run requested by this client, before any model work.
    pub async fn require_topology(&self, expected: &crate::topology::Topology) -> Result<()> {
        match self
            .execute(&AgentCommand::Settlement {
                agent: self.root.clone(),
                wait: false,
            })
            .await?
        {
            AgentReply::Settlement {
                topology: Some(actual),
                ..
            } if &actual == expected => Ok(()),
            AgentReply::Settlement { .. } => Err(Error::Invalid(
                "selected agent has another topology; start with a fresh --host-state-directory"
                    .into(),
            )),
            _ => Err(Error::Storage(
                "topology query returned another reply".into(),
            )),
        }
    }

    /// An `unavailable` reply names no cause: the in-process owner adds the
    /// one it retained, its stream's poison or else the Actors runtime failure
    /// that closed the session's lifetime.
    pub(super) fn stream_failure(
        &self,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> Error {
        if let Some(host) = &self._host {
            if let Some(owner) = host
                .agent_stream
                .as_ref()
                .and_then(|stream| stream.failure())
            {
                return Error::exchange(
                    ExchangeStep::Session,
                    OwnerFailure::Stream {
                        client: error,
                        owner,
                    },
                );
            }
            if let Some(owner) = host.agents.get().and_then(|host| host.runtime.failure()) {
                return Error::exchange(
                    ExchangeStep::Session,
                    OwnerFailure::Runtime {
                        client: error,
                        owner,
                    },
                );
            }
        }
        match &self.owner_exit {
            Some(observation) => Error::exchange(
                ExchangeStep::Session,
                OwnerFailure::Recorded {
                    client: error,
                    owner: OwnerCause::recorded(observation),
                },
            ),
            None => Error::exchange(ExchangeStep::Session, error),
        }
    }

    /// Executes `command` once on this connection's tree.
    ///
    /// # Errors
    /// The session's failure or the host's refusal.
    pub async fn execute(&self, command: &AgentCommand) -> Result<AgentReply> {
        exchanged(&self.controls, command)
            .await
            .map_err(|error| Error::exchange(ExchangeStep::Session, error))?
            .map_err(|error| self.command_failure(command, error))
    }

    fn command_failure(&self, command: &AgentCommand, error: AgentExchangeError) -> Error {
        if error.code() == "unavailable" {
            self.stream_failure(error)
        } else if matches!(
            error,
            AgentExchangeError::Control(
                acyclic_harness_client::AgentControlFailure::Refused { .. }
            )
        ) {
            Error::Refused(format!("{} command: {error}", command.kind()))
        } else {
            Error::exchange(ExchangeStep::Command(command.kind()), error)
        }
    }

    /// Executes `command` once, keeping its exchange failure typed.
    ///
    /// # Errors
    /// Only when `command` cannot be prepared, before anything is sent.
    pub(super) async fn exchange(&self, command: &AgentCommand) -> Exchange {
        exchanged(&self.controls, command).await
    }

    /// Select within this agent subtree, or within an explicitly admitted
    /// session directory, using the same authenticated caller.
    /// Commands retain the original caller; a native host may enroll read-only
    /// access to the selected child's private output on that same session.
    pub async fn select(&self, agent: AgentPath) -> Result<Self> {
        // Bind is authorized by the server against the original caller's tree
        // or directory capability. A selected read-only view is not a change
        // to that caller's authority, and transport constructors cannot infer
        // directory authority from their local attachment mode.
        let (controls, tools) = self.controls.select(agent.clone()).await.map_err(|error| {
            self.command_failure(
                &AgentCommand::Bind {
                    agent: Some(agent.clone()),
                },
                error,
            )
        })?;
        Ok(Self {
            session: controls.observation_session(),
            controls: Arc::new(controls),
            root: agent,
            tools,
            _host: self._host.clone(),
            directory: self.directory,
            owner_exit: self.owner_exit.clone(),
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
        host.attach_with_directory(agent, false, self.directory)
            .await
    }

    /// Connects to the endpoint the one connection policy selects: an
    /// explicit `endpoint`, else `ACYCLIC_ENDPOINT`, else the managed
    /// default, with `ACYCLIC_API_KEY` as the credential. There is no config
    /// file and no local fallback.
    ///
    /// # Errors
    /// The policy's refusal by its stable code, such as
    /// `client_invalid_endpoint`, or as [`Self::connect`].
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
        .map_err(|error| Error::exchange(ExchangeStep::Step("connect to the endpoint"), error))?;
        let session = client.session_client().map_err(|error| {
            Error::exchange(ExchangeStep::Step("open the endpoint session"), error)
        })?;
        Self::bind(session, credential, None, false).await
    }

    /// Binds `session`, holding `credential`, to the tree its endpoint
    /// serves that credential.
    async fn bind(
        session: Client,
        credential: Vec<u8>,
        host: Option<Arc<LocalHarness>>,
        directory: bool,
    ) -> Result<Self> {
        session
            .register_credential(CREDENTIAL, credential.into())
            .map_err(|error| {
                Error::exchange(ExchangeStep::Step("register the session credential"), error)
            })?;
        let (controls, tools) = AgentControls::discover(session.clone(), CREDENTIAL)
            .await
            .map_err(|error| {
                Error::exchange(ExchangeStep::Step("discover agent binding"), error)
            })?;
        let AgentReply::Root { agent: root } = execute(&controls, &AgentCommand::Root {}).await?
        else {
            return Err(Error::Conflict("the endpoint named no root".into()));
        };
        Ok(Self {
            session,
            controls: Arc::new(controls),
            root,
            tools,
            _host: host,
            directory,
            owner_exit: None,
        })
    }
}

async fn execute(controls: &AgentControls, command: &AgentCommand) -> Result<AgentReply> {
    exchanged(controls, command)
        .await
        .map_err(|error| Error::exchange(ExchangeStep::Session, error))?
        .map_err(|error| Error::exchange(ExchangeStep::Session, error))
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
        self.connect_agent(None).await
    }

    /// Explicit owner connection to this placement's independent root sessions.
    /// Ordinary connect retains its single-tree credential boundary.
    pub async fn connect_sessions(self: &Arc<Self>) -> Result<Connection> {
        self.attach_with_directory(None, true, true).await
    }

    /// [Self::connect] under a credential minted for the selected agent, the root when
    /// `None`: the one credential that drives and observes that agent.
    async fn connect_agent(self: &Arc<Self>, agent: Option<AgentPath>) -> Result<Connection> {
        self.attach(agent, true).await
    }

    /// One connection to `agent`'s tree. A `live` session closes with the
    /// agents' runtime, so no wait parks on a stopped producer. An observation
    /// session is not bound to it: what the tree already committed (usage,
    /// history, turns) stays readable after a delivery failure stops that
    /// runtime, and each read answers from those records.
    pub(super) async fn attach(
        self: &Arc<Self>,
        agent: Option<AgentPath>,
        live: bool,
    ) -> Result<Connection> {
        self.attach_with_directory(agent, live, false).await
    }

    async fn attach_with_directory(
        self: &Arc<Self>,
        agent: Option<AgentPath>,
        live: bool,
        directory: bool,
    ) -> Result<Connection> {
        self.agents().await?;
        let hosted = self
            .agents
            .get()
            .ok_or_else(|| Error::Conflict("agents are not hosted".into()))?;
        let admission = async {
            let server = Arc::clone(hosted.server());
            let stream = self.agent_stream.clone().ok_or_else(|| {
                Error::Unsupported("an embedded harness has no agent stream".into())
            })?;
            let bound = server.bound();
            let selected = agent.as_ref().unwrap_or(server.root());
            let observation = if directory {
                if !server.serves_sessions() {
                    return Err(Error::Unauthorized("this admission serves one tree".into()));
                }
                bound
                    .session_credential(
                        selected,
                        server.resource(),
                        &server.session_resource(),
                        Arc::clone(&self.clock),
                    )
                    .await
            } else {
                bound
                    .client_credential(server.root(), server.resource(), Arc::clone(&self.clock))
                    .await
            }
            .map_err(storage)?;
            let changes = bound.changes().map_err(storage)?;
            let session = Client::in_process_with_products(
                stream,
                observation.authority,
                Some(Arc::new(changes)),
                server.clone(),
                Some(server.clone()),
                Arc::new(acyclic_stream::runtime::PlatformExecutor),
                live.then(|| hosted.runtime.lifetime()),
            );
            let connection = Connection::bind(
                session,
                observation.credential,
                Some(Arc::clone(self)),
                directory,
            )
            .await?;
            let Some(agent) = agent else {
                return Ok(connection);
            };
            let (controls, tools) =
                connection
                    .controls
                    .select(agent.clone())
                    .await
                    .map_err(|error| {
                        connection.command_failure(
                            &AgentCommand::Bind {
                                agent: Some(agent.clone()),
                            },
                            error,
                        )
                    })?;
            Ok(Connection {
                session: controls.observation_session(),
                controls: Arc::new(controls),
                root: agent,
                tools,
                _host: connection._host,
                directory,
                owner_exit: connection.owner_exit,
            })
        };
        if live {
            hosted.runtime.admit(admission).await
        } else {
            admission.await
        }
    }
}
