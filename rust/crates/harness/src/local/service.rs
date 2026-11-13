//! Rendezvous for the one local `server::serve` owner. Owner-private IPC
//! checks kernel peer credentials and hands capabilities over the live channel.
//! Startup waits on an OS lock and the native readiness pipe, never probes.
use super::{Connection, LocalHarnessConfig, storage};
use crate::{Error, Result};
use acyclic_native_runtime::{FileLock, FileLockMode, ServiceStartOutcome, private_file};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

/// The checkout's private service rendezvous, outside the repository.
#[derive(Clone)]
pub struct Directory(PathBuf);

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    version: u32,
    credential: String,
    // Optional negotiation metadata: absence grants no configuration match.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    configuration: std::collections::BTreeMap<String, [u8; 32]>,
}

impl Directory {
    /// Selects a checkout's service without constructing a model configuration.
    /// Uses the owner's existing incarnation-keyed state selection.
    pub fn for_workspace(workspace: &Path, state: Option<&Path>) -> Result<Self> {
        let state = match state {
            Some(state) => state.to_owned(),
            None => {
                let checkout = super::owner::Checkout::capture(workspace)?;
                super::owner::state_directory_in(
                    &super::owner::user_state_root()?,
                    &super::LocalProject::Workspace(workspace.to_owned()),
                    Some(&checkout),
                )?
            }
        };
        Self::at(&state, Some(workspace))
    }

    /// Selects the same checkout incarnation as the harness's durable state.
    /// Does not open storage, signing custody or the agent runtime.
    pub fn for_config(config: &LocalHarnessConfig) -> Result<Self> {
        let state = config.resolved_state_directory()?;
        let checkout = match &config.project {
            super::LocalProject::Workspace(path) => Some(path.as_path()),
            super::LocalProject::Volume { .. } => None,
        };
        Self::at(&state, checkout)
    }

    fn at(state: &Path, checkout: Option<&Path>) -> Result<Self> {
        let directory = super::owner::separated_directory(state, checkout)?;
        let directory = directory.join("service");
        private_file::create_directory_all(&directory).map_err(storage)?;
        Ok(Self(directory))
    }

    fn file(&self, name: &str) -> Result<std::fs::File> {
        private_file::open_or_create(&self.0.join(name)).map_err(storage)
    }

    /// Claims the owner's lifetime before it starts serving. A competing
    /// owner is refused; client startup alone uses the waiting startup lock.
    pub fn own(&self) -> Result<Owner> {
        // Acquire publication custody first: seeing a live owner then proves
        // there is an event to wait for even if its starter has disappeared.
        let publication =
            FileLock::try_acquire(self.file("publication.lock")?, FileLockMode::Exclusive)
                .map_err(storage)?;
        let lock = FileLock::try_acquire(self.file("owner.lock")?, FileLockMode::Exclusive)
            .map_err(storage)?;
        Ok(Owner {
            directory: self.clone(),
            configuration: Default::default(),
            _lock: lock,
            publication: std::sync::Mutex::new(Some(publication)),
            bootstrap: Arc::new(OnceLock::new()),
        })
    }

    fn running(&self) -> Result<bool> {
        let occupied =
            || match FileLock::try_acquire(self.file("owner.lock")?, FileLockMode::Exclusive) {
                Ok(unowned) => {
                    drop(unowned);
                    Ok(false)
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(true),
                Err(error) => Err(storage(error)),
            };
        if !occupied()? {
            return Ok(false);
        }
        let published = FileLock::acquire(self.file("publication.lock")?, FileLockMode::Shared)
            .map_err(storage)?;
        // Publication or owner death released the original wait. Check lifetime
        // custody before admitting the descriptor, including an old one.
        let running = occupied();
        drop(published);
        running
    }

    /// Waits for an existing owner's publication, including after its starter
    /// exited. Does not open a model provider. Call from a blocking worker.
    pub fn existing(&self) -> Result<bool> {
        let _startup = FileLock::acquire(self.file("startup.lock")?, FileLockMode::Exclusive)
            .map_err(storage)?;
        self.running()
    }

    /// Attaches to the live owner or launches it once. `arguments` select the
    /// caller's executable's serving mode; the shared readiness protocol is
    /// appended by the native runtime. Call from a blocking worker.
    /// Returns true only when this call started the owner with `arguments`.
    pub fn start(&self, executable: &Path, arguments: &[OsString]) -> Result<bool> {
        let _startup = FileLock::acquire(self.file("startup.lock")?, FileLockMode::Exclusive)
            .map_err(storage)?;
        if self.running()? {
            return Ok(false);
        }
        match acyclic_native_runtime::spawn_service_process(executable, arguments)
            .and_then(acyclic_native_runtime::ServiceReadiness::wait)
            .map_err(storage)?
        {
            ServiceStartOutcome::Ready => Ok(true),
            ServiceStartOutcome::Failed(reason) => Err(Error::Storage(reason)),
            ServiceStartOutcome::Exited => Err(Error::Storage(
                "local server exited before readiness".into(),
            )),
        }
    }

    /// Opens the owner's published capability through the ordinary client.
    /// Endpoint errors are returned as errors, never reinterpreted as death.
    pub async fn connect(&self) -> Result<Connection> {
        Connection::connect_local(self.clone()).await
    }

    /// Non-secret configuration identities published by the live owner.
    /// Call only after `existing` or `start` has waited for publication.
    pub async fn configuration(&self) -> Result<std::collections::BTreeMap<String, [u8; 32]>> {
        Ok(self.descriptor().await?.1.configuration)
    }

    fn address(&self) -> Result<PathBuf> {
        acyclic_native_runtime::ipc::endpoint(&self.0).map_err(storage)
    }

    pub(super) async fn connection(&self) -> Result<(crate::server::LocalConnection, String)> {
        let (connection, descriptor) = self.descriptor().await?;
        Ok((connection, descriptor.credential))
    }
    async fn descriptor(&self) -> Result<(crate::server::LocalConnection, Descriptor)> {
        use acyclic_native_runtime::ipc::PeerCredentials;
        use tokio::io::AsyncReadExt as _;
        let owner = PeerCredentials::current().map_err(storage)?;
        #[cfg(unix)]
        let mut connection = owner
            .connect_unix(&self.address()?)
            .await
            .map_err(storage)?;
        #[cfg(windows)]
        let mut connection = owner
            .connect_pipe(self.address()?.as_os_str())
            .await
            .map_err(storage)?;
        let length =
            usize::try_from(connection.read_u64().await.map_err(storage)?).map_err(storage)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(storage)?;
        bytes.resize(length, 0);
        connection.read_exact(&mut bytes).await.map_err(storage)?;
        let descriptor: Descriptor = serde_json::from_slice(&bytes).map_err(storage)?;
        if descriptor.version != 1 {
            return Err(Error::Unsupported(
                "local service descriptor requires version 1".into(),
            ));
        }
        Ok((crate::server::LocalConnection::new(connection), descriptor))
    }
}

/// OS ownership retained for the entire serving lifetime, including shutdown.
pub struct Owner {
    directory: Directory,
    configuration: std::collections::BTreeMap<String, [u8; 32]>,
    publication: std::sync::Mutex<Option<FileLock>>,
    bootstrap: Arc<OnceLock<Arc<[u8]>>>,
    _lock: FileLock,
}

impl Owner {
    /// Binds configuration identities to this owner's readiness publication.
    pub fn with_configuration(
        mut self,
        configuration: std::collections::BTreeMap<String, [u8; 32]>,
    ) -> Self {
        self.configuration = configuration;
        self
    }

    /// Publishes only in process, after `server::serve` admitted the runtime
    /// and client capability. Readiness releases waiting starters afterward.
    pub fn publish(&self, endpoint: &crate::server::Endpoint) -> Result<()> {
        let observation = endpoint
            .observation
            .as_ref()
            .ok_or_else(|| Error::Invalid("local service requires a client credential".into()))?;
        let bytes = serde_json::to_vec(&Descriptor {
            version: 1,
            credential: observation.credential.clone(),
            configuration: self.configuration.clone(),
        })
        .map_err(storage)?;
        let mut frame = Vec::with_capacity(8 + bytes.len());
        frame.extend_from_slice(&u64::try_from(bytes.len()).map_err(storage)?.to_be_bytes());
        frame.extend_from_slice(&bytes);
        self.bootstrap
            .set(frame.into())
            .map_err(|_| Error::Conflict("local service was already published".into()))?;
        let publication = self
            .publication
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(publication);
        Ok(())
    }

    /// Bind the owner-private endpoint while retaining its exclusive lifetime.
    pub fn listener(&self) -> Result<crate::server::Listener> {
        use futures::StreamExt as _;
        let address = self.directory.address()?;
        #[cfg(unix)]
        let listener = {
            acyclic_native_runtime::ipc::PeerCredentials::current()
                .and_then(|owner| owner.retire_unix(&address))
                .map_err(storage)?;
            acyclic_native_runtime::ipc::UnixListener::bind(&address).map_err(storage)?
        };
        #[cfg(windows)]
        let listener = acyclic_native_runtime::ipc::PipeListener::bind(address.as_os_str())
            .map_err(storage)?;
        let bootstrap = Arc::clone(&self.bootstrap);
        let incoming =
            futures::stream::unfold((listener, bootstrap), |(listener, bootstrap)| async move {
                #[cfg(windows)]
                let mut listener = listener;
                let accepted = loop {
                    match listener.accept().await {
                        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                            continue;
                        }
                        accepted => break accepted,
                    }
                };
                let connection = accepted.and_then(|connection| {
                    let prefix = bootstrap
                        .get()
                        .ok_or_else(|| std::io::Error::other("local service is not published"))?;
                    Ok(crate::server::LocalConnection::new(connection)
                        .with_prefix(Arc::clone(prefix)))
                });
                Some((connection, (listener, bootstrap)))
            })
            .boxed();
        Ok(crate::server::Listener::Local(incoming))
    }
}

/// Runs the default local owner through the same server as every other host.
/// Readiness publication errors end the owner and reach its original starter.
pub async fn serve(
    config: crate::server::ServerConfig,
    owner: Owner,
    ready: acyclic_native_runtime::ServiceReadySignal,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let listener = match owner.listener() {
        Ok(listener) => listener,
        Err(error) => {
            let _ = ready.fail(&error.to_string());
            return Err(storage(error));
        }
    };
    let (failed, failure) = tokio::sync::oneshot::channel();
    let mut ready = Some(ready);
    let mut publication = Ok(());
    let result = crate::server::serve(
        listener,
        config,
        |endpoint| {
            let Some(ready) = ready.take() else {
                return;
            };
            match owner.publish(endpoint) {
                Ok(()) => {
                    if let Err(error) = ready.signal() {
                        publication = Err(storage(error));
                        let _ = failed.send(());
                    }
                }
                Err(error) => {
                    let _ = ready.fail(&error.to_string());
                    publication = Err(error);
                    let _ = failed.send(());
                }
            }
        },
        async move {
            tokio::select! {
                _ = shutdown => {},
                // A dropped sender means successful readiness, not shutdown.
                () = async {
                    if failure.await.is_err() { std::future::pending().await }
                } => {},
            }
        },
    )
    .await;
    if let Some(ready) = ready
        && let Err(error) = &result
    {
        let _ = ready.fail(&error.to_string());
    }
    drop(owner);
    result.and(publication)
}

#[cfg(test)]
#[path = "service_ipc_tests.rs"]
mod ipc_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        local::agent_conversation::{Conversation, Settlement},
        model::Model,
        test_support::{ScriptedModelProvider, ScriptedTurn, local_harness_config},
    };
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn session_directory_creates_independent_roots_and_pages_without_tree_escalation()
    -> Result<()> {
        use crate::client::{AgentCommand, AgentPageRequest, AgentReply};
        let workspace = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        let provider = Arc::new(
            ScriptedModelProvider::new(std::iter::empty::<ScriptedTurn>()).with_context_capacity(
                crate::context::ModelContextCapacity {
                    context_tokens: 32_768,
                    output_tokens: Some(4096),
                },
            ),
        );
        let config = local_harness_config(
            workspace.path(),
            (
                Model::new("scripted".into(), "test".into(), "1".into())?,
                provider.clone(),
            ),
        )?
        .state_directory(state.path())
        .workspace_mounting(crate::local::WorkspaceMounting::Native);
        let local = Arc::new(super::super::LocalHarness::open(config).await?);
        let project = crate::filesystem::workspace_ref(
            local.project.provider().clone(),
            &local.project.storage_name(&mut None)?,
        )?;
        local
            .files
            .apply(
                &project,
                None,
                &[crate::filesystem::WorkspaceMutation::PutFile {
                    path: "/shared.txt".into(),
                    bytes: acyclic_stream::OwnedBytes::from_static(b"checkout base"),
                }],
                &crate::IdempotencyKey::new("session-isolation-base")?,
            )
            .await?;
        let tree = local
            .connect()
            .await
            .map_err(|error| Error::Storage(format!("tree client connect: {error}")))?;
        assert!(
            tree.execute(&AgentCommand::Sessions {
                requester: None,
                scope: acyclic_harness_client::SessionsScope::Owned,
                page: AgentPageRequest::default()
            })
            .await
            .is_err()
        );
        assert!(
            tree.execute(&AgentCommand::NewSession {
                questions: None,
                peer: true,
                id: crate::agent::IntentId::try_from("denied").map_err(storage)?
            })
            .await
            .is_err()
        );
        tree.execute(&AgentCommand::Root {})
            .await
            .map_err(|error| {
                Error::Storage(format!("tree after denied directory call: {error}"))
            })?;
        let directory_local = local.connect_sessions().await.map_err(|error| {
            Error::Storage(format!("in-process directory client connect: {error}"))
        })?;
        assert!(matches!(
            directory_local
                .execute(&AgentCommand::LatestSession {})
                .await?,
            AgentReply::LatestSession { agent: None }
        ));
        let hosted = local
            .agents
            .get()
            .ok_or_else(|| Error::Invalid("missing owner".into()))?;
        let server = Arc::clone(hosted.server());
        let restricted = server
            .bound()
            .client_credential(server.root(), server.resource(), local.clock.clone())
            .await
            .map_err(storage)?;
        let restricted = acyclic_auth::credential::capability_text(&restricted.credential);
        let changes = server.bound().changes().map_err(storage)?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(storage)?;
        let (ready, readiness) = tokio::sync::oneshot::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let remote = tokio::spawn(crate::server::serve(
            listener,
            crate::server::ServerConfig::new(crate::server::Hosted::new(
                server,
                local.clock.clone(),
                local.clone(),
                Some(Arc::new(changes)),
            )),
            move |endpoint| {
                let observation = endpoint.observation.as_ref().unwrap();
                let options = acyclic_harness_client::ConnectionOptions::from_env(
                    Some(&endpoint.endpoint),
                    Some(&observation.credential),
                );
                let _ = ready.send((options, endpoint.endpoint.clone()));
            },
            async {
                let _ = stopped.await;
            },
        ));
        let result = async {
            let (options, endpoint) = readiness.await.map_err(storage)?;
            let root_options = acyclic_harness_client::ConnectionOptions::from_env(
                Some(&endpoint),
                Some(&restricted),
            )
            .map_err(storage)?;
            let restricted_remote = Connection::connect(&root_options)
                .await
                .map_err(|error| Error::Storage(format!("remote tree client connect: {error}")))?;
            restricted_remote.execute(&AgentCommand::Root {}).await?;
            let connection = Connection::connect(&options.map_err(storage)?)
                .await
                .map_err(|error| Error::Storage(format!("directory client connect: {error}")))?;
            assert!(matches!(
                connection.execute(&AgentCommand::LatestSession {}).await?,
                AgentReply::LatestSession { agent: None }
            ));
            let mut roots = std::collections::BTreeSet::new();
            for index in 0..4 {
                let create = AgentCommand::NewSession {
                    questions: None,
                    peer: index != 0,
                    id: crate::agent::IntentId::try_from(format!("session-{index}"))
                        .map_err(storage)?,
                };
                let AgentReply::Root { agent } = connection.execute(&create).await? else {
                    return Err(Error::Invalid("missing session".into()));
                };
                let AgentReply::Root { agent: replay } = connection.execute(&create).await? else {
                    return Err(Error::Invalid("missing replay".into()));
                };
                assert_eq!(agent, replay);
                let expected_peer = index != 0;
                assert_eq!(
                    crate::agent::initial_peer_membership(&**hosted.agents().provider(), &agent,)
                        .await
                        .map_err(storage)?
                        .enabled,
                    expected_peer
                );
                let toggle = AgentCommand::SetPeerMembership {
                    agent: agent.clone(),
                    id: crate::agent::IntentId::try_from(format!("toggle-{index}"))
                        .map_err(storage)?,
                    enabled: !expected_peer,
                };
                connection.execute(&toggle).await?;
                let AgentReply::Root {
                    agent: after_toggle,
                } = connection.execute(&create).await?
                else {
                    return Err(Error::Invalid(
                        "missing creation replay after membership change".into(),
                    ));
                };
                assert_eq!(
                    after_toggle, agent,
                    "creation replay must compare the original peer choice"
                );
                let mut changed = create.clone();
                if let AgentCommand::NewSession { peer, .. } = &mut changed {
                    *peer = !*peer;
                }
                assert!(
                    connection.execute(&changed).await.is_err(),
                    "same creation intent accepted a changed peer choice"
                );
                let AgentReply::LatestSession { agent: latest } =
                    connection.execute(&AgentCommand::LatestSession {}).await?
                else {
                    return Err(Error::Invalid("missing latest session".into()));
                };
                assert_eq!(latest.as_ref(), Some(&agent));
                assert!(
                    agent.parent().is_none(),
                    "sessions must be independent roots"
                );
                assert!(roots.insert(agent.clone()));
                assert!(
                    tree.select(agent.clone()).await.is_err(),
                    "the original tree credential cannot select a sibling"
                );
                let selected = connection.select(agent.clone()).await?;
                assert_eq!(selected.root(), &agent);
                let AgentReply::Settlement { .. } = selected
                    .execute(&AgentCommand::Settlement { agent, wait: false })
                    .await?
                else {
                    return Err(Error::Invalid("selected session unavailable".into()));
                };
            }
            let mut listed = std::collections::BTreeSet::new();
            let mut cursor = None;
            loop {
                let page = AgentPageRequest::new(2, 65536, cursor).map_err(storage)?;
                let AgentReply::Children { items, next } = connection
                    .execute(&AgentCommand::Sessions {
                        requester: None,
                        scope: acyclic_harness_client::SessionsScope::Owned,
                        page,
                    })
                    .await?
                else {
                    return Err(Error::Invalid("missing directory page".into()));
                };
                assert!(items.len() <= 2);
                for item in items {
                    assert!(listed.insert(item.agent));
                }
                cursor = next;
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(listed, roots);
            let mut projects = Vec::new();
            let mut shared = Vec::new();
            for root in &roots {
                let state = local.agents().await?.0.state(root).await.map_err(storage)?;
                let binding = state
                    .volumes
                    .iter()
                    .find(|binding| binding.name == "project")
                    .ok_or_else(|| Error::Invalid("missing project binding".into()))?;
                assert_ne!(binding.volume()?, &local.project);
                let workspace = local.workspace.as_ref().unwrap();
                let (signed, verifier) = local
                    .agents
                    .get()
                    .unwrap()
                    .server()
                    .bound()
                    .volume_scope(root)
                    .await
                    .map_err(storage)?;
                let authority = crate::filesystem::NativeMountAuthority::admitted(
                    verifier,
                    signed.copy(&mut None)?,
                );
                let view = workspace.spawned_project_view(binding.volume()?).await?;
                let bound = workspace
                    .bound_task_view(
                        view,
                        binding.volume()?,
                        &state.volumes,
                        &super::super::workspace::owner::AgentOwner,
                        &authority,
                    )
                    .await?;
                let mounts = &bound.mounts;
                super::super::checkout_tools::access::require_mount_access(
                    bound.view().workspace(),
                    mounts,
                    &signed,
                    signed.capabilities(),
                    "session shell",
                )
                .await?;
                assert_eq!(
                    mounts
                        .named(Some(super::super::SHARED_VOLUME))?
                        .1
                        .view
                        .volume,
                    state
                        .volumes
                        .iter()
                        .find(|binding| binding.name == super::super::SHARED_VOLUME)
                        .unwrap()
                        .volume()?
                        .clone()
                );

                projects.push(crate::filesystem::workspace_ref(
                    binding.volume()?.provider().clone(),
                    &binding.volume()?.storage_name(&mut None)?,
                )?);
                let binding = state
                    .volumes
                    .iter()
                    .find(|binding| binding.name == super::super::SHARED_VOLUME)
                    .ok_or_else(|| Error::Invalid("missing shared binding".into()))?;
                shared.push(crate::filesystem::workspace_ref(
                    binding.volume()?.provider().clone(),
                    &binding.volume()?.storage_name(&mut None)?,
                )?);
            }
            for (index, text) in [(0, "first board"), (1, "second board")] {
                local
                    .files
                    .apply(
                        &shared[index],
                        None,
                        &[crate::filesystem::WorkspaceMutation::PutFile {
                            path: "/board.txt".into(),
                            bytes: acyclic_stream::OwnedBytes::copy_from_slice(text.as_bytes()),
                        }],
                        &crate::IdempotencyKey::new(format!("shared-isolation-{index}"))?,
                    )
                    .await?;
            }
            for (workspace, expected) in [(&shared[0], "first board"), (&shared[1], "second board")]
            {
                assert_eq!(
                    local
                        .files
                        .read(workspace, None, "/board.txt", 1024, None)
                        .await?
                        .as_ref(),
                    expected.as_bytes(),
                    "independent sessions must not share writable boards"
                );
            }
            for (index, text) in [(0, "first session"), (1, "second session")] {
                local
                    .files
                    .apply(
                        &projects[index],
                        None,
                        &[crate::filesystem::WorkspaceMutation::PutFile {
                            path: "/shared.txt".into(),
                            bytes: acyclic_stream::OwnedBytes::copy_from_slice(text.as_bytes()),
                        }],
                        &crate::IdempotencyKey::new(format!("session-isolation-{index}"))?,
                    )
                    .await?;
            }
            for (workspace, expected) in [
                (&projects[0], "first session"),
                (&projects[1], "second session"),
                (&projects[2], "checkout base"),
                (&project, "checkout base"),
            ] {
                let bytes = local
                    .files
                    .read(workspace, None, "/shared.txt", 1024, None)
                    .await?;
                assert_eq!(
                    bytes.as_ref(),
                    expected.as_bytes(),
                    "sessions sharing one repo cannot overwrite each other"
                );
            }
            let selected = crate::local::agent_conversation::Conversation::open(
                connection.select(roots.first().unwrap().clone()).await?,
            );
            let mut diff = selected
                .diff_query(
                    AgentPageRequest::default(),
                    crate::client::DiffProjection::Patch,
                )
                .await
                .map_err(storage)?;
            let mut found = false;
            while let Some(event) = diff.next().await.map_err(storage)? {
                if let crate::client::DiffQueryEvent::Page(items) = event {
                    found |= items.iter().any(|item| item.path == "shared.txt");
                }
            }
            assert!(found, "missing session diff");
            let agent = roots
                .iter()
                .find(|agent| *agent != connection.root())
                .unwrap()
                .clone();
            let read = AgentCommand::AgentFiles {
                agent: agent.clone(),
                paths: vec!["ui/title".into()],
                after: None,
            };
            let AgentReply::ClientFiles { snapshot } = connection.execute(&read).await? else {
                return Err(Error::Invalid("missing title snapshot".into()));
            };
            assert!(snapshot.files["ui/title"].is_none());
            let create_title = AgentCommand::PublishAgentFiles {
                agent: agent.clone(),
                operation: crate::OperationId::new(),
                changes: vec![crate::client_files::ClientFileChange {
                    path: "ui/title".into(),
                    expected: None,
                    text: "Independent session".into(),
                }],
            };
            connection.execute(&create_title).await?;
            let selected = connection.select(agent.clone()).await?;
            let filtered = crate::local::sessions::page_filtered(
                &connection,
                AgentPageRequest::default(),
                &crate::local::sessions::Filter {
                    title: Some("INDEPENDENT".into()),
                    states: vec![crate::local::sessions::State::Idle],
                    created_after: Some(1),
                    created_before: Some(u64::MAX),
                    updated_after: Some(1),
                },
            )
            .await?;
            assert_eq!(filtered.items.len(), 1);
            assert_eq!(filtered.items[0].agent, agent);
            let request = AgentPageRequest::new(1, 64 * 1024, None).map_err(storage)?;
            let unfiltered = crate::local::sessions::page(&connection, request.clone()).await?;
            let empty = crate::local::sessions::page_filtered(
                &connection,
                request,
                &crate::local::sessions::Filter {
                    states: vec![crate::local::sessions::State::Archived],
                    ..Default::default()
                },
            )
            .await?;
            assert!(empty.items.is_empty());
            assert!(empty.next.is_some());
            assert_eq!(
                empty.next, unfiltered.next,
                "an empty filtered page must preserve the source continuation"
            );
            let name = AgentCommand::NameSession {
                agent: agent.clone(),
                name: "fix parser/branch".into(),
                id: crate::agent::IntentId::try_from("name-independent-session")
                    .map_err(storage)?,
            };
            connection.execute(&name).await?;
            connection.execute(&name).await?;
            assert_eq!(
                crate::local::sessions::select(&connection, "fix parser/branch")
                    .await?
                    .root(),
                &agent
            );
            assert!(
                restricted_remote.execute(&name).await.is_err(),
                "a tree credential cannot author directory names"
            );
            assert!(
                restricted_remote
                    .execute(&AgentCommand::ResolveSession {
                        name: "fix parser/branch".into()
                    })
                    .await
                    .is_err(),
                "name discovery requires directory admission"
            );
            let another = roots.iter().find(|other| *other != &agent).unwrap();
            assert!(
                connection
                    .execute(&AgentCommand::NameSession {
                        agent: another.clone(),
                        name: "fix parser/branch".into(),
                        id: crate::agent::IntentId::try_from("conflicting-name")
                            .map_err(storage)?,
                    })
                    .await
                    .is_err(),
                "an existing alias cannot silently move to another session"
            );
            assert!(
                connection
                    .execute(&AgentCommand::NameSession {
                        agent: tree.root().clone(),
                        name: "not-in-directory".into(),
                        id: crate::agent::IntentId::try_from("name-checkout-owner")
                            .map_err(storage)?,
                    })
                    .await
                    .is_err()
            );
            assert!(
                connection
                    .execute(&AgentCommand::ResolveSession {
                        name: "not-in-directory".into()
                    })
                    .await
                    .is_err(),
                "failed membership admission must publish no alias"
            );
            let selection = AgentCommand::SelectSession {
                agent: agent.clone(),
                id: crate::agent::IntentId::try_from("explicit-reattach").map_err(storage)?,
            };
            connection.execute(&selection).await?;
            connection.execute(&selection).await?;
            assert!(
                matches!(connection.execute(&AgentCommand::LatestSession {}).await?,
                AgentReply::LatestSession { agent: Some(latest) } if latest == agent)
            );
            let not_a_session = AgentCommand::SelectSession {
                agent: tree.root().clone(),
                id: crate::agent::IntentId::try_from("select-owner-is-not-session")
                    .map_err(storage)?,
            };
            assert!(connection.execute(&not_a_session).await.is_err());
            let AgentReply::ClientFiles { snapshot } = selected.execute(&read).await? else {
                return Err(Error::Invalid("missing shared title".into()));
            };
            assert_eq!(
                snapshot.files["ui/title"].as_ref().unwrap().text,
                "Independent session"
            );
            let waiting = AgentCommand::AgentFilePreviews {
                agent: agent.clone(),
                paths: vec!["ui/title".into()],
                after: Some(snapshot.generation),
                bytes: 18,
            };
            let rename = AgentCommand::PublishAgentFiles {
                agent: agent.clone(),
                operation: crate::OperationId::new(),
                changes: vec![crate::client_files::ClientFileChange {
                    path: "ui/title".into(),
                    expected: snapshot.files["ui/title"]
                        .as_ref()
                        .map(|file| file.file.clone()),
                    text: format!("Renamed session 🌳{}", "long title ".repeat(8192)),
                }],
            };
            let (changed, renamed) =
                futures::join!(selected.execute(&waiting), connection.execute(&rename));
            renamed?;
            let AgentReply::FilePreviews { snapshot } = changed? else {
                return Err(Error::Invalid("missing rename event".into()));
            };
            assert_eq!(
                snapshot.files["ui/title"].as_ref().unwrap(),
                "Renamed session "
            );
            assert_eq!(
                crate::local::sessions::select(&connection, "fix parser/branch")
                    .await?
                    .root(),
                &agent,
                "display title edits retain direct aliases"
            );
            let stale = AgentCommand::PublishAgentFiles {
                agent: agent.clone(),
                operation: crate::OperationId::new(),
                changes: vec![crate::client_files::ClientFileChange {
                    path: "ui/title".into(),
                    expected: None,
                    text: "stale create".into(),
                }],
            };
            assert!(
                connection.execute(&stale).await.is_err(),
                "rename cannot overwrite a concurrent edit"
            );
            let id = crate::agent::IntentId::try_from("archive-session").map_err(storage)?;
            connection
                .execute(&AgentCommand::Archive {
                    agent: agent.clone(),
                    id: id.clone(),
                })
                .await?;
            assert!(matches!(
                connection
                    .execute(&AgentCommand::Prompt {
                        peer: None,
                        agent,
                        id,
                        body: crate::model::ModelContent::Text("closed".into())
                    })
                    .await?,
                AgentReply::Delivered {
                    delivery: crate::agent::Delivered::Closed
                }
            ));
            assert_eq!(
                provider.request_count(),
                0,
                "directory operations cannot call the model"
            );
            Ok(())
        }
        .await;
        let _ = stop.send(());
        let served = remote.await.map_err(storage)?;
        result.and(served)
    }

    #[tokio::test]
    async fn detached_clients_share_one_owner_and_one_prompt_commit() -> Result<()> {
        for configuration in [
            std::collections::BTreeMap::new(),
            std::collections::BTreeMap::from([("catalog".into(), [42; 32])]),
        ] {
            detached_clients(configuration).await?;
        }
        Ok(())
    }

    async fn detached_clients(
        configuration: std::collections::BTreeMap<String, [u8; 32]>,
    ) -> Result<()> {
        let workspace = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        let (entered, entering) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let gate = Mutex::new(Some((entered, released)));
        let provider = Arc::new(
            ScriptedModelProvider::new([ScriptedTurn::Text("settled after detach".into())])
                .with_context_capacity(crate::context::ModelContextCapacity {
                    context_tokens: 32_768,
                    output_tokens: Some(4096),
                })
                .with_dispatch_observer(move |_| {
                    let gate = gate.lock().unwrap().take();
                    Box::pin(async move {
                        let (entered, released) =
                            gate.ok_or_else(|| Error::Invalid("duplicate model dispatch".into()))?;
                        let _ = entered.send(());
                        released.await.map_err(storage)
                    })
                }),
        );
        let config = local_harness_config(
            workspace.path(),
            (
                Model::new("scripted".into(), "test".into(), "1".into())?,
                provider.clone(),
            ),
        )?
        .state_directory(state.path())
        // This finite script measures prompt idempotency, not the optional
        // feedback settlement turn, which has its own lifecycle coverage.
        .feedback_plugin(false)
        // This lifecycle control executes no tools. Keep its small model's
        // request free of the unrelated coding-tool catalogue.
        .without_default_tools();
        let directory = Directory::for_config(&config)?;
        assert_eq!(
            directory.0,
            Directory::for_workspace(workspace.path(), Some(state.path()))?.0
        );
        let owner = directory.own()?.with_configuration(configuration.clone());
        assert!(
            directory.own().is_err(),
            "only one runtime may own the checkout"
        );
        // The original starter may already have exited. A second starter's
        // wait belongs to the owner's publication lock, not the first caller.
        let attaching = directory.clone();
        let attachment = tokio::task::spawn_blocking(move || {
            attaching.start(Path::new("nonexistent-service-executable"), &[])
        });
        let listener = owner.listener()?;
        let (ready, readiness) = tokio::sync::oneshot::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let result = crate::server::serve(
                listener,
                crate::server::ServerConfig::new(config),
                |endpoint| {
                    let result = owner.publish(endpoint);
                    let _ = ready.send(result);
                },
                async {
                    let _ = stopped.await;
                },
            )
            .await;
            drop(owner);
            result
        });
        let result = async {
            readiness.await.map_err(storage)??;
            assert!(!attachment.await.map_err(storage)??);
            let stray = directory.0.join("endpoint.json");
            assert!(!stray.exists(), "capabilities must never be published to a file");
            let untrusted = br#"{"version":1,"endpoint":"http://127.0.0.1:1","credential":"stray-write"}"#;
            std::fs::write(&stray, untrusted).map_err(storage)?;
            assert_eq!(directory.configuration().await?, configuration);
            // Reattachment observes the owner, never starts a replacement or
            // dispatches a model to decide whether that owner is ready.
            assert!(directory.existing()?);
            assert!(!directory.start(Path::new("nonexistent-service-executable"), &[])?);
            let first = Conversation::open(directory.connect().await?);
            let second = Conversation::open(directory.connect().await?);
            assert!(!first.phase().await?.in_turn(), "adoption alone is not a turn");
            let mut events = second.observe(None).await?;
            let inbox = first
                .send("shared-operation", "finish after I detach")
                .await?;
            let mut entering = std::pin::pin!(entering);
            loop {
                tokio::select! {
                    entered = &mut entering => { entered.map_err(storage)?; break; },
                    event = events.next() => match event? {
                        super::super::Event::Turn(entry) => return Err(Error::Storage(format!(
                            "turn ended before model dispatch: {:?}", entry.outcome,
                        ))),
                        super::super::Event::Status(status) if matches!(status.phase, crate::agent::Phase::Done { .. }) => {
                            return Err(Error::Storage(format!("owner ended before model dispatch: {:?}", status.phase)));
                        }
                        _ => {},
                    }
                }
            }
            drop(first);
            let attached = Conversation::open(directory.connect().await?);
            let (mut attached_events, phase) = attached.observe_with_phase(None).await?;
            assert!(phase.phase.in_turn(), "a fresh attachment must see the running turn");
            let status = attached.path().status().map_err(storage)?;
            let resource = acyclic_stream::wire_codec::session::stream_resource(status.as_str());
            loop {
                let (event, delivery) = attached_events.next_projected().await?;
                if delivery.resource == resource
                    && matches!(delivery.frame.body, Some(acyclic_stream::wire::session_frame::Body::Data(_)))
                {
                    assert!(event?.is_none(), "old idle status must not replace the opening phase");
                    break;
                }
                event?;
            }
            assert_eq!(
                second
                    .send("shared-operation", "finish after I detach")
                    .await?,
                inbox
            );
            release
                .send(())
                .map_err(|()| Error::Storage("model gate ended".into()))?;
            let Settlement::Reply(crate::agent::Reply::Answered { entry, .. }) = second
                .settle("shared-operation", inbox, &mut events)
                .await?
            else {
                return Err(Error::Invalid("shared prompt did not settle".into()));
            };
            assert_eq!(second.output_text(&entry).await?, "settled after detach");
            assert_eq!(provider.request_count(), 1);
            assert_eq!(std::fs::read(&stray).map_err(storage)?, untrusted);
            loop {
                if let super::super::Event::Status(status) = attached_events.next().await? {
                    assert_eq!(status.conversation, 1, "settlement publishes the new cut");
                    assert_eq!(status.phase, crate::agent::Phase::Idle);
                    break;
                }
            }
            assert!(!attached.phase().await?.in_turn());
            drop((attached, attached_events));
            drop((second, events));
            let reattached = Conversation::open(directory.connect().await?);
            assert_eq!(reattached.settled().await?, 1);
            Ok(())
        }
        .await;
        let _ = stop.send(());
        server.await.map_err(storage)??;
        result?;
        drop(directory.own()?);
        Ok(())
    }

    #[tokio::test]
    async fn session_merges_publish_checkout_files_and_preserve_conflicts() -> Result<()> {
        use crate::local::{WorkspaceMounting, sessions};
        use acyclic_fs::facade::FsWork as _;
        let checkout = tempfile::tempdir().map_err(storage)?;
        let storage_dir = tempfile::tempdir().map_err(storage)?;
        std::fs::write(checkout.path().join("shared.txt"), b"base\n").map_err(storage)?;
        std::fs::create_dir(checkout.path().join("nested")).map_err(storage)?;
        std::fs::write(
            checkout.path().join("nested/kept.txt"),
            b"native untouched\n",
        )
        .map_err(storage)?;
        let provider = Arc::new(
            ScriptedModelProvider::new([
                super::super::tests::checked_commands::call(
                    0,
                    crate::tools::shell::PLATFORM.name,
                    "echo parent > parent.txt",
                ),
                ScriptedTurn::Text("child ready".into()),
                ScriptedTurn::Text("grandchild ready".into()),
                super::super::tests::checked_commands::call(
                    1,
                    crate::tools::shell::PLATFORM.name,
                    "cp nested.txt observed.txt",
                ),
                ScriptedTurn::Text("parent read merged checkout".into()),
            ])
            .with_context_capacity(crate::context::ModelContextCapacity {
                context_tokens: 32_768,
                output_tokens: Some(4096),
            }),
        );
        let local = Arc::new(
            super::super::LocalHarness::open(
                local_harness_config(
                    checkout.path(),
                    (
                        Model::new("scripted".into(), "test".into(), "1".into())?,
                        provider.clone(),
                    ),
                )?
                .state_directory(storage_dir.path())
                .compaction(crate::context::CompactionPolicy::Disabled)
                .workspace_mounting(WorkspaceMounting::Physical),
            )
            .await?,
        );
        let directory = local.connect_sessions().await?;
        let first = sessions::create(&directory, "merge-first", None).await?;
        let second = sessions::create(&directory, "merge-second", None).await?;
        for (connection, name, text) in [
            (&first, "first.txt", b"first\n".as_slice()),
            (&second, "second.txt", b"second\n".as_slice()),
        ] {
            let state = local
                .agents()
                .await?
                .0
                .state(connection.root())
                .await
                .map_err(storage)?;
            let project = state
                .volumes
                .iter()
                .find(|binding| binding.name == "project")
                .ok_or_else(|| Error::Invalid("missing isolated project".into()))?;
            let view = local
                .workspace
                .as_ref()
                .unwrap()
                .spawned_project_view(project.volume()?)
                .await?;
            let cancellation = acyclic_fs::CancellationToken::new();
            let mut accounting = acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation);
            // Use the same lazy writer as tools: an overwrite preserves the
            // native source identity rather than adding an unrelated file to
            // the authored-only generation underneath that source.
            for path in [format!("/nested/{name}"), "/shared.txt".into()] {
                view.write(
                    &path,
                    acyclic_stream::OwnedBytes::copy_from_slice(text),
                    acyclic_fs::IdempotencyKey::from_bytes(crate::OperationId::new().into_bytes()),
                    acyclic_fs::PublicationPermit::Unrestricted,
                    &mut accounting,
                )
                .await
                .map_err(storage)?;
            }
        }
        assert_eq!(
            std::fs::read(checkout.path().join("shared.txt")).map_err(storage)?,
            b"base\n"
        );
        let first_commit = sessions::merge(&first, "publish-first")
            .await
            .map_err(|error| Error::Storage(format!("first integration: {error}")))?;
        assert!(
            first_commit.is_some(),
            "integration must create a merge commit"
        );
        assert_eq!(
            std::fs::read(checkout.path().join("nested/kept.txt")).map_err(storage)?,
            b"native untouched\n"
        );
        assert_eq!(
            std::fs::read(checkout.path().join("nested/first.txt")).map_err(storage)?,
            b"first\n"
        );
        assert_eq!(
            std::fs::read(checkout.path().join("shared.txt")).map_err(storage)?,
            b"first\n"
        );
        assert_eq!(
            sessions::merge(&first, "publish-first").await?,
            first_commit
        );
        // Spend the first attempt's capture identity before the merge reports
        // its conflict. Resolution is another capture under the same request.
        std::fs::write(checkout.path().join("local.txt"), b"local edit\n").map_err(storage)?;
        let conflict_error = sessions::merge(&second, "publish-second")
            .await
            .expect_err("divergent session edits must conflict");
        let conflicted =
            std::fs::read_to_string(checkout.path().join("shared.txt")).map_err(storage)?;
        assert!(
            conflicted.contains("<<<<<<<")
                && conflicted.contains("first")
                && conflicted.contains("second"),
            "merge conflicts must remain ordinary checkout files: {conflicted}; merge error: {conflict_error}"
        );
        assert_eq!(
            std::fs::read(checkout.path().join("nested/first.txt")).map_err(storage)?,
            b"first\n"
        );
        std::fs::write(checkout.path().join("shared.txt"), b"resolved\n").map_err(storage)?;
        let second_commit = sessions::merge(&second, "publish-second")
            .await
            .map_err(|error| Error::Storage(format!("resolved integration: {error}")))?;
        assert!(second_commit.is_some());
        assert_eq!(
            std::fs::read(checkout.path().join("shared.txt")).map_err(storage)?,
            b"resolved\n"
        );
        assert_eq!(
            std::fs::read(checkout.path().join("nested/second.txt")).map_err(storage)?,
            b"second\n"
        );
        assert_eq!(
            std::fs::read(checkout.path().join("local.txt")).map_err(storage)?,
            b"local edit\n"
        );
        assert_eq!(
            sessions::merge(&second, "publish-second").await?,
            second_commit
        );
        assert_eq!(
            provider.request_count(),
            0,
            "merging must not dispatch a model"
        );
        // A tree-only credential may act on its descendants, and a nested
        // merge must publish to its direct parent rather than the host checkout.
        let tree = local.connect().await?;
        let mut selected = tree.clone();
        let mut descendants = Vec::new();
        for name in ["child", "grandchild"] {
            let crate::client::AgentReply::Spawned { agent, .. } = selected
                .execute(&crate::client::AgentCommand::Spawn {
                    parent: selected.root().clone(),
                    id: crate::agent::IntentId::try_from(name).map_err(storage)?,
                    brief: crate::model::ModelContent::Text("wait for human integration".into()),
                    role: "worker".into(),
                })
                .await?
            else {
                return Err(Error::Invalid("missing descendant".into()));
            };
            selected = tree.select(agent).await?;
            let phase = Conversation::open(selected.clone())
                .wait_current_input()
                .await?
                .phase;
            assert!(
                matches!(phase, crate::agent::Phase::Idle),
                "{name} did not become idle: {phase:?}"
            );
            let state = local
                .agents()
                .await?
                .0
                .state(selected.root())
                .await
                .map_err(storage)?;
            let entries = Conversation::open(selected.clone())
                .entries(state.conversation_base, state.conversation_tail)
                .await?;
            assert!(
                matches!(entries.as_slice(), [entry] if entry.outcome.is_completed()),
                "{name} brief did not complete: {entries:?}"
            );
            descendants.push(selected.clone());
        }
        let parent_state = local
            .agents()
            .await?
            .0
            .state(descendants[0].root())
            .await
            .map_err(storage)?;
        let binding = |name: &str| {
            parent_state
                .volumes
                .iter()
                .find(|binding| binding.name == name)
                .unwrap()
                .volume()
                .unwrap()
                .clone()
        };
        let (signed, verifier) = local
            .agents
            .get()
            .unwrap()
            .server()
            .bound()
            .volume_scope(descendants[0].root())
            .await
            .map_err(storage)?;
        let parent_workspace = local
            .project_workspace(
                local.workspace.as_ref().unwrap(),
                &binding("project"),
                &parent_state.volumes,
                &signed,
                &verifier,
            )
            .await?;
        let parent_shell =
            super::super::tests::request_text(&local, &provider.requests()[1]).await?;
        assert!(
            std::fs::read_to_string(parent_workspace.path.join("parent.txt"))
                .map_err(|error| Error::Storage(format!("parent shell marker at {}: {error}; original model input after shell: {parent_shell}", parent_workspace.path.display())))?
                .contains("parent"),
            "the parent's shell must have bound its checkout before merging"
        );
        let grandchild_state = local
            .agents()
            .await?
            .0
            .state(descendants[1].root())
            .await
            .map_err(storage)?;
        let grandchild_shared = grandchild_state
            .volumes
            .iter()
            .find(|binding| binding.name == super::super::SHARED_VOLUME)
            .unwrap()
            .volume()?;
        let shared = binding(super::super::SHARED_VOLUME);
        let shared = crate::filesystem::workspace_ref(
            shared.provider().clone(),
            &shared.storage_name(&mut None)?,
        )?;
        local
            .files
            .apply(
                &shared,
                None,
                &[crate::filesystem::WorkspaceMutation::PutFile {
                    path: "/board.txt".into(),
                    bytes: acyclic_stream::OwnedBytes::from_static(b"tree board"),
                }],
                &crate::IdempotencyKey::new("descendant-board")?,
            )
            .await?;
        let shared = crate::filesystem::workspace_ref(
            grandchild_shared.provider().clone(),
            &grandchild_shared.storage_name(&mut None)?,
        )?;
        assert_eq!(
            local
                .files
                .read(&shared, None, "/board.txt", 1024, None)
                .await?
                .as_ref(),
            b"tree board"
        );
        let state = local
            .agents()
            .await?
            .0
            .state(selected.root())
            .await
            .map_err(storage)?;
        let project = state
            .volumes
            .iter()
            .find(|binding| binding.name == "project")
            .ok_or_else(|| Error::Invalid("missing descendant project".into()))?;
        let view = local
            .workspace
            .as_ref()
            .unwrap()
            .spawned_project_view(project.volume()?)
            .await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        view.write(
            "/nested.txt",
            acyclic_stream::OwnedBytes::from_static(b"nested\n"),
            acyclic_fs::IdempotencyKey::from_bytes(crate::OperationId::new().into_bytes()),
            acyclic_fs::PublicationPermit::Unrestricted,
            &mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation),
        )
        .await
        .map_err(storage)?;
        let commit = sessions::merge(&descendants[1], "merge-grandchild").await?;
        assert!(commit.is_some());
        assert_eq!(
            sessions::merge(&descendants[1], "merge-grandchild").await?,
            commit
        );
        assert!(
            !checkout.path().join("nested.txt").exists(),
            "a grandchild merge must not target the root checkout"
        );
        let parent = Conversation::open(descendants[0].clone());
        let mut diff = parent
            .diff_query(
                crate::client::AgentPageRequest::default(),
                crate::client::DiffProjection::Patch,
            )
            .await
            .map_err(storage)?;
        let mut merged = false;
        while let Some(event) = diff.next().await.map_err(storage)? {
            if let crate::client::DiffQueryEvent::Page(files) = event {
                merged |= files.iter().any(|file| file.path.ends_with("nested.txt"));
            }
        }
        assert!(merged, "the direct parent must own the merged edit");
        parent
            .send("read-merged-checkout", "read the merged file using shell")
            .await?;
        parent.wait_current_input().await?;
        assert_eq!(
            std::fs::read(parent_workspace.path.join("observed.txt")).map_err(|error| {
                Error::Storage(format!(
                    "parent shell after merge at {}: {error}",
                    parent_workspace.path.display()
                ))
            })?,
            b"nested\n",
            "a later parent shell must see the merged file"
        );
        sessions::merge(&descendants[0], "merge-child").await?;
        assert_eq!(
            std::fs::read(checkout.path().join("nested.txt")).map_err(storage)?,
            b"nested\n"
        );
        let discarded = Conversation::open(descendants[1].clone());
        sessions::archive(parent.connection(), "discard-child-subtree").await?;
        assert!(matches!(
            parent.wait_current_input().await?.phase,
            crate::agent::Phase::Done { .. }
        ));
        assert!(matches!(
            discarded.wait_current_input().await?.phase,
            crate::agent::Phase::Done { .. }
        ));
        assert!(matches!(
            discarded.deliver("after-discard", "must not run").await?,
            crate::local::agent_conversation::Sent::Refused(_)
        ));
        assert_eq!(
            provider.request_count(),
            5,
            "human filesystem actions must not dispatch a model"
        );
        Ok(())
    }
}
