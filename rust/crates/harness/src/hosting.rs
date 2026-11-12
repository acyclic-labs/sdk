//! The public serving entry point: one admitted agent tree as a running agent
//! server, dispatching the unified session protocol's agent commands.
//!
//! A platform (managed Swarm, a self-hosted Actors deployment, a local cell)
//! verifies its caller, binds it once with [`Bound::admitted`] over the one
//! admitted [`Placement`], and serves it:
//!
//! ```ignore
//! let bound = Bound::admitted(issuer, caller, placement, turns)?;
//! let hosting::Serving::Served(server) = hosting::serve(bound, platform, &intent, root).await?
//! else { /* uncertain: retain it and `resubmit` */ };
//! let service = acyclic_stream::session::Service::new(stream, authority, node, limits)
//!     .with_products(None, server.clone());
//! acyclic_native_runtime::spawn(async move { server.run(|actor, error| report(actor, error)).await });
//! ```
//!
//! Every command arrives as a canonical Actors invocation on the session's
//! authenticated credential. [`Server`] decodes it before any IO, rechecks the
//! credential and the platform's current policy at dispatch, then acts only
//! within the admitted tree. The session carrier, not this module, owns
//! admission, credit and fragmentation; no backend-specific route exists.

pub use acyclic_harness_core::filesystem::{
    attestation::{Attempt, Attestation, Attested, AttestedProvider, Attestor},
    hosting::{Bound, Placement, Prepared, RootRequest, RootSubmission, Turns, VerifiedCaller},
};

#[cfg(not(target_arch = "wasm32"))]
mod runtime;
#[cfg(not(target_arch = "wasm32"))]
pub use runtime::Runtime;
mod diff;
mod queries;
pub use diff::{DIFF_FILE_BYTES, DiffSource};

/// Original lazy input source; filtered work is distinct from an empty cut.
pub type InputPreviewSource =
    acyclic_stream::BoxProviderStream<'static, crate::Result<crate::agent::InputPreviewStep>>;

use crate::{
    agent::{
        AgentPath, AgentRecord, ContextAction, ContextRequest, Control, Delivered, Error,
        InboxRecord, IntentId, Message, Sender, SpawnOutcome, SpawnRefusal, SpawnRequest,
        StatusRecord, requested_child,
    },
    model::ModelContent,
};
use acyclic_auth::{authority::PolicyDecision, contract::Principal};
use acyclic_fs::{AsyncObjectStore, distributed::AuthorityProvider};
use acyclic_harness_client::{
    AgentBinding, AgentCall, AgentCommand, AgentPageRequest, AgentReply, ChildAgent,
    MAX_AGENT_COMMAND_BYTES, MAX_READ_BYTES, PendingApproval, ReadTarget,
};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    BoxProviderFuture, ChildrenPageRequest, CommitId, HeadProjection, HeadsRequest,
    ProviderPlatform, RevisionCut, StreamError, StreamPath,
    session::{CallAuthorization, CallProvider},
};
use bytes::Bytes;
use std::sync::Arc;

/// What a hosting platform decides at every dispatch: the owner's current
/// policy for the verified principal's command. Identity comes only from the
/// session credential; the platform never supplies a caller here.
pub trait Platform: ProviderPlatform + 'static {
    /// Explicitly serve the roots of this placement as independent sessions.
    /// Tree-only hosts retain their existing routing and credential boundary.
    fn sessions(&self) -> bool {
        false
    }

    /// Whether this host guarantees unchecked context without compaction.
    fn compaction_disabled(&self) -> bool {
        false
    }

    /// Optional source of the new session's independent volume forks. The
    /// host chooses this checkout binding; a client cannot supply volume grants.
    fn session_source(&self) -> Option<AgentPath> {
        None
    }

    /// Bind a new session to platform-owned resources. Requests cannot supply
    /// volume grants, signing custody or an actor placement.
    fn session_root(
        &self,
        agent: crate::agent::AgentId,
    ) -> BoxProviderFuture<'_, crate::Result<RootRequest>> {
        let _ = agent;
        Box::pin(async {
            Err(crate::Error::Unsupported(
                "session creation is unavailable".into(),
            ))
        })
    }

    /// Integrate a root through the platform's existing filesystem merge.
    /// The directory credential and current policy were checked first.
    fn merge_session<'a>(
        &'a self,
        agent: &'a AgentPath,
        operation: crate::OperationId,
    ) -> BoxProviderFuture<'a, crate::Result<Option<String>>> {
        let _ = (agent, operation);
        Box::pin(async {
            Err(crate::Error::Unsupported(
                "session merge is unavailable".into(),
            ))
        })
    }

    /// The current policy for `principal` issuing `command`.
    fn policy<'a>(
        &'a self,
        principal: &'a Principal,
        command: &'a AgentCommand,
    ) -> BoxProviderFuture<'a, Result<PolicyDecision, StreamError>>;

    /// Whether this platform runs the children a spawn creates. One that runs
    /// none refuses every spawn command before it stages or commits anything.
    fn runs_children(&self) -> bool {
        true
    }

    /// Durable wake admission, after both credential and current policy allow
    /// dispatch, and before any command effect. Native continuous runtimes need
    /// no additional wake; event platforms retain their recovery alarm here.
    fn admitted(&self) -> BoxProviderFuture<'_, Result<(), StreamError>> {
        Box::pin(async { Ok(()) })
    }

    /// The text a prompt's leading `/name` selects: a prompt command's or
    /// skill's body with its arguments substituted, which the message stages
    /// in place of `text` so the model reads exactly it. `None` keeps `text`.
    /// A platform with no project of its own to read expands none.
    fn prompt_command<'a>(
        &'a self,
        text: &'a str,
    ) -> BoxProviderFuture<'a, crate::Result<Option<String>>> {
        let _ = text;
        Box::pin(async { Ok(None) })
    }

    /// The attachment manifest of the files a prompt's `@` mentions in
    /// `text` name (see [`Message::attachments`]), staged once for the
    /// message `key` identifies, so a resend replays it exactly. With `None`,
    /// a replay's, only the manifest already staged for `key` answers: the
    /// project's current files are never read. A platform with no project of
    /// its own to read attaches none.
    fn attachments<'a>(
        &'a self,
        key: &'a [u8],
        text: Option<&'a str>,
    ) -> BoxProviderFuture<'a, crate::Result<Option<crate::conversation::FileRef>>> {
        let _ = (key, text);
        Box::pin(async { Ok(None) })
    }

    /// Select one immutable workspace diff under the original admission.
    /// The returned source owns its frontier for exactly the live client query.
    fn diff_source<'a>(
        &'a self,
        authorization: &'a CallAuthorization,
        agent: &'a AgentPath,
    ) -> BoxProviderFuture<'a, crate::Result<DiffSource>> {
        let _ = (authorization, agent);
        Box::pin(async {
            Err(crate::Error::Unsupported(
                "workspace diff source is unavailable".into(),
            ))
        })
    }

    /// Select accepted input under this platform's original admission.
    fn input_preview_source<'a>(
        &'a self,
        authorization: &'a CallAuthorization,
        agent: &'a AgentPath,
        maximum: u64,
        selection: crate::agent::InputSelection,
    ) -> BoxProviderFuture<'a, crate::Result<InputPreviewSource>> {
        let _ = (authorization, agent, maximum, selection);
        Box::pin(async {
            Err(crate::Error::Unsupported(
                "input preview source is unavailable".into(),
            ))
        })
    }

    /// The approver principal the verified `principal` decides approvals as:
    /// by default its own identity.
    fn approver(&self, principal: &Principal) -> String {
        principal.id.clone()
    }
}

/// The routing URL of every agent command; a host dispatches on the command.
const BINDING_URL: &str = "/agents/command";

/// The bodyless Actors invocation routed to the admitted host. It names only
/// routing; the session credential authorizes each command.
#[must_use]
pub fn binding(
    within: &acyclic_actors::addressing::ActorReference,
) -> acyclic_actors::wire::InvokeActorRequest {
    acyclic_actors::wire::InvokeActorRequest {
        deployment_envelope: Vec::new(),
        actor_id: within.id().to_owned(),
        method: "POST".into(),
        url: BINDING_URL.into(),
        namespace: Some(within.namespace().as_str().to_owned()),
        account_id: within.account().as_bytes().to_vec(),
        ..Default::default()
    }
}

/// One admitted agent tree served over the unified session protocol.
pub struct Server<P: AuthorityProvider + ?Sized + 'static, O: 'static> {
    this: std::sync::Weak<Self>,
    bound: Bound<P, O>,
    platform: Arc<dyn Platform>,
    root: AgentPath,
    resource: String,
    cursor_key: acyclic_auth::cursor::CursorKey,
    /// Only uncertain root publications retain process state; settled sessions
    /// are discovered directly from the stream directory.
    pending_roots:
        std::sync::Mutex<std::collections::BTreeMap<AgentPath, (blake3::Hash, Prepared)>>,
}

/// What serving a root came to.
pub enum Serving<P: AuthorityProvider + ?Sized + 'static, O: 'static> {
    /// The root is created and served.
    Served(Arc<Server<P, O>>),
    /// The root's creation is uncertain: [`Unsettled::resubmit`] submits the
    /// identical prepared commit, which still holds its fresh volumes'
    /// objects.
    Unknown(Unsettled<P, O>),
}

/// A root whose creation is uncertain, owning its prepared commit.
pub struct Unsettled<P: AuthorityProvider + ?Sized + 'static, O: 'static> {
    bound: Bound<P, O>,
    platform: Arc<dyn Platform>,
    prepared: Prepared,
}

impl<P, O> Unsettled<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    /// The prepared commit awaiting a definite outcome.
    #[must_use]
    pub const fn prepared(&self) -> &Prepared {
        &self.prepared
    }

    /// Resubmits the identical prepared commit.
    ///
    /// # Errors
    /// As [`serve`].
    pub async fn resubmit(self) -> Result<Serving<P, O>, Error> {
        settle(self.bound, self.platform, self.prepared).await
    }
}

/// Creates (or, on the same `intent`, exactly reopens) `root` under `bound`
/// and serves it. The root and its fresh volumes are one commit; an uncertain
/// creation is [`Serving::Unknown`], still owning that commit.
///
/// # Errors
/// When `bound`'s admission does not cover `root`, or the store fails.
pub async fn serve<P, O>(
    bound: Bound<P, O>,
    platform: Arc<dyn Platform>,
    intent: &IntentId,
    root: RootRequest,
) -> Result<Serving<P, O>, Error>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    let prepared = bound.prepare_root(intent, root).await?;
    settle(bound, platform, prepared).await
}

/// Submits `prepared` once under `bound` and serves the created root.
async fn settle<P, O>(
    bound: Bound<P, O>,
    platform: Arc<dyn Platform>,
    prepared: Prepared,
) -> Result<Serving<P, O>, Error>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    let root = match bound.submit_root(prepared).await? {
        RootSubmission::Created(root) => root,
        RootSubmission::Unknown(prepared) => {
            return Ok(Serving::Unknown(Unsettled {
                bound,
                platform,
                prepared,
            }));
        }
    };
    if platform.sessions() {
        bound.initialize_sessions().await?;
    }
    Ok(Serving::Served(server(bound, platform, root)?))
}

/// Restores a platform's durable placement of an already adopted tree.
/// Revalidates the original scope without storage IO; this grants no file
/// authority. File operations and turns use the agent's committed bindings.
/// The platform validates adoption before retaining placement; restoring it
/// neither runs the agent nor activates children.
pub async fn reopen<P, O>(
    bound: Bound<P, O>,
    platform: Arc<dyn Platform>,
    root: AgentPath,
) -> Result<Arc<Server<P, O>>, Error>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    bound.admit_path(&root)?;
    if platform.sessions() {
        bound.initialize_sessions().await?;
    }
    server(bound, platform, root)
}

fn server<P, O>(
    bound: Bound<P, O>,
    platform: Arc<dyn Platform>,
    root: AgentPath,
) -> Result<Arc<Server<P, O>>, Error>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    let resource = format!(
        "actors:/{}",
        acyclic_auth::encode_component(bound.within().namespace().as_str().as_bytes())
    );
    let cursor_key = acyclic_auth::cursor::CursorKey::new()
        .map_err(|_| Error::Record("cursor key generation failed"))?;
    Ok(Arc::new_cyclic(|this| Server {
        this: this.clone(),
        bound,
        platform,
        root,
        resource,
        cursor_key,
        pending_roots: Default::default(),
    }))
}

impl<P, O> Server<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    /// The served root.
    #[must_use]
    pub const fn root(&self) -> &AgentPath {
        &self.root
    }

    /// The canonical actor resource every command of this server is admitted
    /// against.
    #[must_use]
    pub fn resource(&self) -> &str {
        &self.resource
    }

    /// Delivers one explicitly placed agent to quiescence. Returns its
    /// committed undelivered alarm for an event host's native alarm primitive.
    /// An unsettled publication retains its runtime slot and cannot be released.
    pub async fn drain(&self, agent: &AgentPath) -> Result<Option<u64>, Error> {
        let agent = self.within_tree(agent)?;
        self.bound.agents().drain_event(agent).await
    }

    /// The binding a client sends this server's commands with: the canonical
    /// bodyless Actors invocation of the served tree. It names only routing,
    /// never authority; the session credential authorizes each command.
    #[must_use]
    pub fn binding(&self) -> acyclic_actors::wire::InvokeActorRequest {
        binding(self.bound.within())
    }

    /// The binding this server serves.
    #[must_use]
    pub fn bound(&self) -> &Bound<P, O> {
        &self.bound
    }

    /// Runs the tree's agents until the runtime stops; `failed` sees each
    /// failed delivery.
    ///
    /// # Errors
    /// The runtime's terminal failure.
    pub async fn run(
        &self,
        failed: impl Fn(&acyclic_actors::addressing::ActorReference, &acyclic_actors::local::Error),
    ) -> acyclic_actors::local::Error {
        match self.bound.agents().run(failed).await {
            Err(error) => error,
        }
    }

    /// Decodes `invocation` and checks it routes to this server, with no IO.
    fn admit(&self, invocation: &[u8]) -> Result<AgentCall, StreamError> {
        let call = AgentCall::decode(invocation).map_err(|_| StreamError::InvalidArgument)?;
        let within = self.bound.within();
        let routed = call.invocation();
        // Discovery names no tree: the endpoint answers it with its own.
        if AgentBinding::is_discovery(routed) {
            return if matches!(call.command(), AgentCommand::Bind { .. }) {
                Ok(call)
            } else {
                Err(StreamError::AccessDenied)
            };
        }
        if routed.actor_id != within.id()
            || routed.namespace.as_deref() != Some(within.namespace().as_str())
            || routed.account_id.as_slice() != within.account().as_bytes()
        {
            return Err(StreamError::AccessDenied);
        }
        Ok(call)
    }

    /// `agent` when it is the root or one of its descendants.
    fn within_tree<'a>(&self, agent: &'a AgentPath) -> Result<&'a AgentPath, Error> {
        (self.root.contains(agent) || self.platform.sessions())
            .then_some(agent)
            .ok_or(Error::NoSuchAgent)
    }

    /// Whether this platform admits an independent root-session directory.
    pub fn serves_sessions(&self) -> bool {
        self.platform.sessions()
    }

    /// Separate resource: a tree credential never acquires directory authority.
    pub fn session_resource(&self) -> String {
        format!(
            "actors:/session-directory/{}",
            acyclic_auth::encode_component(self.bound.within().namespace().as_str().as_bytes())
        )
    }

    fn command_resource(&self, command: &AgentCommand) -> String {
        if matches!(
            command,
            AgentCommand::Sessions {
                scope: acyclic_harness_client::SessionsScope::Peers,
                ..
            }
        ) {
            return self.peer_resource();
        }
        if matches!(
            command,
            AgentCommand::Sessions { .. }
                | AgentCommand::NewSession { .. }
                | AgentCommand::ForkSession { .. }
                | AgentCommand::NameSession { .. }
                | AgentCommand::ResolveSession { .. }
                | AgentCommand::SelectSession { .. }
                | AgentCommand::LatestSession {}
        ) || matches!(command, AgentCommand::MergeSession { agent, .. } if agent.parent().is_none())
            || matches!(command,
                AgentCommand::PreparePeer { sender, .. }
                | AgentCommand::Prompt { peer: Some((sender, _)), .. }
                if !self.root.contains(sender))
            || command
                .agent()
                .is_some_and(|agent| !self.root.contains(agent))
        {
            self.session_resource()
        } else {
            self.resource.clone()
        }
    }

    /// Read-only peer discovery is independent of session-directory control.
    pub fn peer_resource(&self) -> String {
        self.bound.peer_resource()
    }

    fn authorize_command(
        &self,
        authorization: &CallAuthorization,
        command: &AgentCommand,
        attributes: &acyclic_auth::contract::RecordAttributes,
        policy: PolicyDecision,
    ) -> Result<(), StreamError> {
        authorization.authorize(&self.resource, &[], attributes, policy)?;
        if let AgentCommand::Sessions {
            scope: acyclic_harness_client::SessionsScope::Peers,
            requester: Some(requester),
            ..
        } = command
            && requester != &self.root
        {
            let directory = self.session_resource();
            authorization
                .dependent_resource(
                    acyclic_auth::contract::ResourcePattern {
                        name: directory.clone(),
                        prefix: false,
                    },
                    "actors/invoke",
                )?
                .authorize(&directory, &[], attributes, policy)?;
        }
        let selected = self.command_resource(command);
        if selected != self.resource {
            authorization
                .dependent_resource(
                    acyclic_auth::contract::ResourcePattern {
                        name: selected.clone(),
                        prefix: false,
                    },
                    "actors/invoke",
                )?
                .authorize(&selected, &[], attributes, policy)?;
        }
        Ok(())
    }

    /// Delivers one context request to the agent; its checkpoint settles in
    /// the agent's conversation like a turn.
    async fn context(
        &self,
        agent: &AgentPath,
        by: &Sender,
        id: &IntentId,
        action: ContextAction,
    ) -> Result<AgentReply, Error> {
        let record = InboxRecord::Context(ContextRequest {
            by: by.clone(),
            id: id.clone(),
            action,
        });
        Ok(AgentReply::Delivered {
            delivery: self.bound.agents().send(agent, by, &record).await?,
        })
    }

    /// Runs `command` from `by`, who decides approvals as `approver`.
    #[inline(never)]
    fn dispatch<'a>(
        &'a self,
        command: &'a AgentCommand,
        by: &'a Sender,
        approver: &'a str,
    ) -> BoxProviderFuture<'a, Result<AgentReply, Error>> {
        // Construct this operation before entering its poll graph. The session
        // carrier keeps the admitted command; its caller retains one owner.
        Box::pin(self.dispatch_inner(command, by, approver))
    }

    async fn dispatch_inner(
        &self,
        command: &AgentCommand,
        by: &Sender,
        approver: &str,
    ) -> Result<AgentReply, Error> {
        let agents = self.bound.agents();
        match command {
            AgentCommand::Bind { agent } => {
                let agent = self.within_tree(agent.as_ref().unwrap_or(&self.root))?;
                #[cfg(not(target_arch = "wasm32"))]
                if agent != &self.root {
                    if let Some((tools, credential)) =
                        self.bound.selected_observation(&self.root, agent).await?
                    {
                        return Ok(AgentBinding::reply(
                            &self.binding(),
                            tools,
                            Some(&credential),
                        ));
                    }
                }
                Ok(AgentBinding::reply(
                    &self.binding(),
                    self.bound.tool_resource(agent).await?,
                    None,
                ))
            }
            AgentCommand::Root {} => Ok(AgentReply::Root {
                agent: self.root.clone(),
            }),
            AgentCommand::SetPeerMembership { agent, id, enabled } => {
                let agent = self.within_tree(agent)?;
                agents.set_peer_membership(agent, by, id, *enabled).await?;
                Ok(AgentReply::PeerMembership { enabled: *enabled })
            }
            AgentCommand::AgentFilePreviews {
                agent,
                paths,
                after,
                bytes,
            } => {
                let snapshot = self
                    .bound
                    .agent_file_previews(self.within_tree(agent)?, paths, after.as_ref(), *bytes)
                    .await?;
                Ok(AgentReply::FilePreviews { snapshot })
            }
            AgentCommand::AgentFiles {
                agent,
                paths,
                after,
            } => {
                let snapshot = self
                    .bound
                    .agent_files(self.within_tree(agent)?, paths, after.as_ref())
                    .await?;
                Ok(AgentReply::ClientFiles { snapshot })
            }
            AgentCommand::PublishAgentFiles {
                agent,
                operation,
                changes,
            } => {
                let generation = self
                    .bound
                    .publish_agent_files(self.within_tree(agent)?, *operation, changes)
                    .await?;
                Ok(AgentReply::ClientFilesPublished { generation })
            }
            AgentCommand::Archive { agent, id } => {
                let delivery = agents.archive(self.within_tree(agent)?, by, id).await?;
                Ok(AgentReply::Delivered { delivery })
            }
            AgentCommand::Pause { agent, paused } => {
                agents.pause(self.within_tree(agent)?, *paused).await?;
                Ok(AgentReply::Paused { paused: *paused })
            }
            AgentCommand::MergeSession { agent, id } => {
                if agent.parent().is_none() && !self.platform.sessions() {
                    return Err(Error::Record("root merge requires a session directory"));
                }
                let identity = crate::contract::canonical_json_bytes(&(by, id, agent))
                    .map_err(|_| Error::Record("merge identity cannot encode"))?;
                let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.session-merge.v1");
                hash.update(&identity);
                let mut operation = [0; 16];
                hash.finalize_xof().fill(&mut operation);
                let commit = self
                    .platform
                    .merge_session(
                        self.within_tree(agent)?,
                        crate::OperationId::from_bytes(operation),
                    )
                    .await
                    .map_err(|error| Error::Turn(Box::new(error)))?;
                Ok(AgentReply::Merged { commit })
            }
            AgentCommand::NameSession { agent, name, id } => {
                if !self.platform.sessions() {
                    return Err(Error::Record("this host serves one tree"));
                }
                self.bound.name_session(agent, name, id).await?;
                Ok(AgentReply::Root {
                    agent: agent.clone(),
                })
            }
            AgentCommand::ResolveSession { name } => {
                if !self.platform.sessions() {
                    return Err(Error::Record("this host serves one tree"));
                }
                Ok(AgentReply::Root {
                    agent: self.bound.resolve_session(name).await?,
                })
            }
            AgentCommand::SelectSession { agent, id } => {
                if !self.platform.sessions() {
                    return Err(Error::Record("this host serves one tree"));
                }
                self.bound.select_session(agent, id).await?;
                Ok(AgentReply::Root {
                    agent: agent.clone(),
                })
            }
            AgentCommand::LatestSession {} => {
                if !self.platform.sessions() {
                    return Err(Error::Record("this host serves one tree"));
                }
                Ok(AgentReply::LatestSession {
                    agent: self.bound.latest_session().await?,
                })
            }
            AgentCommand::NewSession { id, peer, .. }
            | AgentCommand::ForkSession { id, peer, .. } => {
                if !self.platform.sessions() {
                    return Err(Error::Record("this host serves one tree"));
                }
                let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.session.v1");
                hasher.update(self.root.as_str().as_bytes());
                // New and historical root intents are independent namespaces.
                if let AgentCommand::ForkSession { .. } = command {
                    hasher.update(b"history");
                }
                hasher.update(
                    &serde_json::to_vec(&(by, id))
                        .map_err(|_| Error::Record("session identity cannot encode"))?,
                );
                let mut bytes = [0; 16];
                hasher.finalize_xof().fill(&mut bytes);
                let identity = crate::agent::AgentId::from_bytes(bytes);
                let path = AgentPath::root(&identity);
                let parameters = blake3::hash(
                    &serde_json::to_vec(command)
                        .map_err(|_| Error::Record("session request cannot encode"))?,
                );
                let pending = self
                    .pending_roots
                    .lock()
                    .map_err(|_| Error::Record("session publication owner is poisoned"))?
                    .remove(&path);
                if pending.is_none() {
                    match agents.state(&path).await {
                        Ok(state) => {
                            let provenance = state
                                .profile
                                .as_ref()
                                .and_then(|profile| profile.continuation.as_ref());
                            let same = match command {
                                AgentCommand::ForkSession { agent, at, .. } => provenance
                                    .is_some_and(|origin| {
                                        origin.parent_run_id == *agent
                                            && origin.stream_offset == *at
                                    }),
                                _ => provenance.is_none(),
                            };
                            let original_peer =
                                crate::agent::initial_peer_membership(&**agents.provider(), &path)
                                    .await?;
                            if !same || original_peer.enabled != *peer {
                                return Err(Error::Record(
                                    "session intent already names another creation",
                                ));
                            }
                            return Ok(AgentReply::Root { agent: path });
                        }
                        Err(Error::NoSuchAgent) => {}
                        Err(error) => return Err(error),
                    }
                }
                let prepared = match pending {
                    Some((original, prepared)) => {
                        if original != parameters {
                            self.pending_roots
                                .lock()
                                .map_err(|_| {
                                    Error::Record("session publication owner is poisoned")
                                })?
                                .insert(path, (original, prepared));
                            return Err(Error::Record(
                                "uncertain session intent names another request",
                            ));
                        }
                        prepared
                    }
                    None => {
                        let mut root = self
                            .platform
                            .session_root(identity)
                            .await
                            .map_err(|error| Error::Turn(Box::new(error)))?;
                        root.peer = *peer;
                        match command {
                            AgentCommand::ForkSession { agent, at, .. } => {
                                self.bound
                                    .prepare_session_history(
                                        id,
                                        root,
                                        self.within_tree(agent)?,
                                        *at,
                                    )
                                    .await?
                            }
                            _ => match self.platform.session_source() {
                                Some(source) => {
                                    self.bound
                                        .prepare_isolated_session(
                                            id,
                                            root,
                                            &source,
                                            match command {
                                                AgentCommand::NewSession { questions, .. } => {
                                                    *questions
                                                }
                                                _ => None,
                                            },
                                        )
                                        .await?
                                }
                                None => {
                                    self.bound
                                        .prepare_session_root(
                                            id,
                                            root,
                                            match command {
                                                AgentCommand::NewSession { questions, .. } => {
                                                    *questions
                                                }
                                                _ => None,
                                            },
                                        )
                                        .await?
                                }
                            },
                        }
                    }
                };
                let agent = match self.bound.submit_root(prepared).await? {
                    RootSubmission::Created(agent) => agent,
                    RootSubmission::Unknown(prepared) => {
                        let request = prepared.request().clone();
                        self.pending_roots
                            .lock()
                            .map_err(|_| Error::Record("session publication owner is poisoned"))?
                            .insert(path, (parameters, prepared));
                        return Err(Error::Unknown(Box::new(request)));
                    }
                };
                Ok(AgentReply::Root { agent })
            }
            AgentCommand::PreparePeer { agent, sender } => {
                let agent = self.within_tree(agent)?;
                let sender = self.within_tree(sender)?;
                let state = agents.state(sender).await?;
                let Some(profile) = state.profile else {
                    return Ok(AgentReply::PeerAdmission { admission: None });
                };
                Ok(AgentReply::PeerAdmission {
                    admission: crate::agent::session_peer_admission(
                        &**agents.provider(),
                        &profile.peers,
                        sender,
                        agent,
                    )
                    .await?,
                })
            }
            AgentCommand::Settlement { agent, wait } => {
                let agent = self.within_tree(agent)?;
                if *wait {
                    let tail = crate::agent::inbox_tail(&**agents.provider(), agent).await?;
                    if let Some(input) = tail.checked_sub(1) {
                        agents.wait_for_input(agent, input).await?;
                    }
                }
                let (state, settlement) = agents.settlement_snapshot(agent).await?;
                let profile = state.profile.as_ref();
                Ok(AgentReply::Settlement {
                    settled: state.conversation_tail,
                    statuses: state.status_tail,
                    role: profile
                        .and_then(|profile| profile.role())
                        .map(str::to_owned),
                    model: profile.and_then(|profile| profile.model.clone()),
                    compaction_disabled: self.platform.compaction_disabled(),
                    topology: profile.and_then(|profile| profile.topology.clone()),
                    phase: settlement.phase,
                })
            }
            AgentCommand::Diff { .. } => Err(Error::Turn(Box::new(crate::Error::Unsupported(
                "workspace diff queries require an original actor socket".into(),
            )))),
            AgentCommand::Usage {
                agent,
                input,
                after,
                wait,
                observed_children,
            } => {
                let agent = self.within_tree(agent)?;
                let (input, page, settled, open_children) = self
                    .bound
                    .usage_page(agent, *input, *after, *wait, *observed_children)
                    .await?;
                Ok(AgentReply::Usage {
                    input,
                    page,
                    settled,
                    open_children,
                })
            }
            AgentCommand::Events {
                agent,
                input,
                after,
                wait,
            } => {
                let agent = self.within_tree(agent)?;
                let (input, inbox_tail, page, settled) =
                    self.bound.event_page(agent, *input, *after, *wait).await?;
                Ok(AgentReply::Events {
                    input,
                    inbox_tail,
                    page,
                    settled,
                })
            }
            AgentCommand::ClientFiles {
                agent,
                session,
                paths,
                after,
            } => {
                let agent = self.within_tree(agent)?;
                Ok(AgentReply::ClientFiles {
                    snapshot: self
                        .bound
                        .client_files(agent, *session, paths, after.as_ref())
                        .await?,
                })
            }
            AgentCommand::PublishClientFiles {
                agent,
                session,
                operation,
                changes,
            } => {
                let agent = self.within_tree(agent)?;
                Ok(AgentReply::ClientFilesPublished {
                    generation: self
                        .bound
                        .publish_client_files(agent, *session, *operation, changes)
                        .await?,
                })
            }
            AgentCommand::ChildWakes {
                agent,
                status,
                page,
            } => {
                let agent = self.within_tree(agent)?;
                let after = page
                    .cursor()
                    .map(|after| after.parse::<u64>())
                    .transpose()
                    .map_err(|_| Error::Record("invalid completion cursor"))?;
                let mut items = self
                    .bound
                    .child_wakes(agent, *status, after, page.items() as usize + 1)
                    .await?;
                let mut credit = PageCredit::new(page, |next| AgentReply::ChildWakes {
                    items: Vec::new(),
                    next,
                })?;
                for (index, item) in items.iter().take(page.items() as usize).enumerate() {
                    let continues = index + 1 < items.len();
                    let next = item
                        .status
                        .checked_add(1)
                        .ok_or(Error::Record("completion status overflow"))?;
                    if !credit.accept(item, continues.then_some(u64::from(next.ilog10()) + 1))? {
                        break;
                    }
                }
                let next = if credit.items < items.len() {
                    Some(
                        items[credit.items - 1]
                            .status
                            .checked_add(1)
                            .ok_or(Error::Record("completion status overflow"))?
                            .to_string(),
                    )
                } else {
                    None
                };
                items.truncate(credit.items);
                Ok(AgentReply::ChildWakes { items, next })
            }
            AgentCommand::Directory {
                agent,
                path,
                generation,
                page: request,
            } => {
                let agent = self.within_tree(agent)?;
                let mut page = self
                    .bound
                    .directory_page(
                        agent,
                        path,
                        generation.as_ref(),
                        request.cursor(),
                        request.items(),
                    )
                    .await?;
                let empty = |has_more| AgentReply::Directory {
                    page: crate::conversation::PrivateDirectoryPage {
                        generation: page.generation.clone(),
                        entries: Vec::new(),
                        has_more,
                    },
                };
                let mut credit = PageCredit::from_envelopes(request, &empty(false), &empty(true))?;
                for (index, entry) in page.entries.iter().enumerate() {
                    let continues = index + 1 < page.entries.len() || page.has_more;
                    if !credit.accept(entry, continues.then_some(0))? {
                        break;
                    }
                }
                page.has_more |= credit.items < page.entries.len();
                page.entries.truncate(credit.items);
                Ok(AgentReply::Directory { page })
            }
            AgentCommand::Read {
                agent,
                target,
                offset,
            } => {
                let agent = self.within_tree(agent)?;
                let (bytes, length) = match target {
                    ReadTarget::Private { path, generation } => {
                        let (bytes, length) = self
                            .bound
                            .read_private_file(agent, path, generation, *offset, MAX_READ_BYTES)
                            .await?;
                        return Ok(AgentReply::read(&bytes, length));
                    }
                    ReadTarget::Approval { status } => {
                        let (bytes, length) = self
                            .bound
                            .read_approval_request(agent, *status, *offset, MAX_READ_BYTES)
                            .await?;
                        return Ok(AgentReply::read(&bytes, length));
                    }
                    ReadTarget::Execution {
                        input,
                        sequence,
                        field,
                    } => {
                        let (bytes, length) = self
                            .bound
                            .read_event_payload(
                                agent,
                                *input,
                                *sequence,
                                *field,
                                *offset,
                                MAX_READ_BYTES,
                            )
                            .await?;
                        return Ok(AgentReply::read(&bytes, length));
                    }
                    ReadTarget::File { file } => {
                        self.bound
                            .read_file(agent, file, *offset, MAX_READ_BYTES)
                            .await?
                    }
                    ReadTarget::Conversation { sequence, file } => {
                        self.bound
                            .read_conversation_file(agent, file, *sequence, *offset, MAX_READ_BYTES)
                            .await?
                    }
                    ReadTarget::ToolOutput { path, at } => {
                        self.bound
                            .read_tool_output(agent, path, at.as_ref(), *offset, MAX_READ_BYTES)
                            .await?
                    }
                };
                Ok(AgentReply::read(&bytes, length))
            }
            AgentCommand::Prompt {
                agent,
                id,
                body,
                peer: Some((source, admission)),
            } => {
                let agent = self.within_tree(agent)?;
                let source = self.within_tree(source)?;
                let state = agents.state(source).await?;
                if !state
                    .profile
                    .as_ref()
                    .is_some_and(|profile| profile.peers.contains(crate::agent::SEND_PEER))
                {
                    return Err(Error::Record("the sender lacks peer message authority"));
                }
                let sender = Sender::Agent {
                    path: source.clone(),
                };
                // Peer text is content, never an invocation of user commands,
                // project skills, attachment expansion, or approval controls.
                let body = self.bound.stage_prompt(agent, &sender, id, body).await?;
                let message = Message {
                    from: sender,
                    id: id.clone(),
                    body: Box::new(body),
                    re: None,
                    attachments: None,
                };
                Ok(AgentReply::Delivered {
                    delivery: agents.send_peer(agent, source, &message, admission).await?,
                })
            }
            AgentCommand::Prompt {
                agent,
                id,
                body,
                peer: None,
            } => {
                let agent = self.within_tree(agent)?;
                let (body, attachments) = if let ModelContent::Text(source) = body {
                    // The message's own identity, as the inbox keys it: its
                    // recipient, sender and intent.
                    let key = crate::contract::canonical_json_bytes(&(agent, by, id))
                        .map_err(|_| Error::Record("message identity does not encode"))?;
                    let turn = |error| Error::Turn(Box::new(error));
                    // A replay is answered from what its first delivery pinned:
                    // its staged body, and the manifest staged before it. Only a
                    // new delivery reads the project's commands, skills and files.
                    let (body, attachments) = match self
                        .bound
                        .staged_text(agent, "message", by, id, source)
                        .await?
                    {
                        Some(body) => (
                            body,
                            self.platform.attachments(&key, None).await.map_err(turn)?,
                        ),
                        None => {
                            let expanded =
                                self.platform.prompt_command(source).await.map_err(turn)?;
                            let text = expanded.as_deref().unwrap_or(source);
                            let attachments = self
                                .platform
                                .attachments(&key, Some(text))
                                .await
                                .map_err(turn)?;
                            let body = self
                                .bound
                                .stage_text(agent, "message", by, id, (source, text))
                                .await?;
                            (body, attachments)
                        }
                    };
                    (body, attachments)
                } else {
                    (self.bound.stage_prompt(agent, by, id, body).await?, None)
                };
                let record = InboxRecord::Message(Message {
                    from: by.clone(),
                    id: id.clone(),
                    body: Box::new(body),
                    re: None,
                    attachments: attachments.map(Box::new),
                });
                Ok(AgentReply::Delivered {
                    delivery: agents.send(agent, by, &record).await?,
                })
            }
            AgentCommand::Reply { agent, id, inbox } => Ok(AgentReply::Reply {
                reply: agents.reply(self.within_tree(agent)?, id, *inbox).await?,
            }),
            AgentCommand::Cancel { agent, id } => {
                let agent = self.within_tree(agent)?;
                let record = InboxRecord::Cancel(Control {
                    by: by.clone(),
                    id: id.clone(),
                });
                Ok(AgentReply::Delivered {
                    delivery: agents.send(agent, by, &record).await?,
                })
            }
            AgentCommand::Compact {
                agent,
                id,
                instructions,
            } => {
                let agent = self.within_tree(agent)?;
                let instructions = match instructions.as_ref().map(text).transpose()? {
                    Some(guidance) if !guidance.trim().is_empty() => Some(Box::new(
                        self.bound
                            .stage_text(agent, "compact", by, id, (guidance, guidance))
                            .await?,
                    )),
                    _ => None,
                };
                self.context(agent, by, id, ContextAction::Compact { instructions })
                    .await
            }
            AgentCommand::Rewind { agent, id } => {
                self.context(self.within_tree(agent)?, by, id, ContextAction::Rewind)
                    .await
            }
            AgentCommand::Restore {
                agent,
                id,
                at,
                mode,
                expected,
            } => {
                self.context(
                    self.within_tree(agent)?,
                    by,
                    id,
                    ContextAction::Restore {
                        at: *at,
                        mode: *mode,
                        expected: expected.clone(),
                    },
                )
                .await
            }
            AgentCommand::ClearAll { agent, id } => {
                self.context(self.within_tree(agent)?, by, id, ContextAction::Clear)
                    .await
            }
            AgentCommand::ProjectHeads { agent } => Ok(AgentReply::ProjectHeads {
                items: self.bound.project_heads(self.within_tree(agent)?).await?,
            }),
            AgentCommand::History { agent, page } => {
                use base64::{Engine as _, engine::general_purpose::STANDARD};
                use futures::TryStreamExt as _;
                let agent = self.within_tree(agent)?;
                let mut credit = PageCredit::new(page, |next| AgentReply::History {
                    items: Vec::new(),
                    next,
                })?;
                let mut cursor = match page.cursor() {
                    Some(cursor) => {
                        let bytes = STANDARD
                            .decode(cursor)
                            .map_err(|_| Error::Record("invalid history cursor"))?;
                        let cursor: HistoryCursor = serde_json::from_slice(&bytes)
                            .map_err(|_| Error::Record("invalid history cursor"))?;
                        if cursor.version != 1
                            || cursor.agent != *agent
                            || cursor.before > cursor.tail
                        {
                            return Err(Error::Record("history cursor differs from selection"));
                        }
                        cursor
                    }
                    None => {
                        let tail = self.tail(agent.history()?).await?;
                        HistoryCursor {
                            version: 1,
                            agent: agent.clone(),
                            before: tail,
                            tail,
                        }
                    }
                };
                let count = u64::from(page.items()).min(cursor.before);
                let start = cursor.before - count;
                let mut items = Vec::new();
                if count != 0 {
                    let mut records = agents
                        .provider()
                        .read(acyclic_stream::ReadRequest {
                            key: None,
                            path: agent.history()?,
                            from: start,
                            limit: count as u32,
                            at_least: Default::default(),
                        })
                        .await
                        .map_err(Error::Stream)?;
                    while let Some(record) = records.try_next().await.map_err(Error::Stream)? {
                        let item = crate::agent::HistoryCheckpoint::summary(&record.value)?;
                        let sequence = start + items.len() as u64;
                        if record.sequence != sequence || item.conversation != sequence + 1 {
                            return Err(Error::Record("history page differs from indexed cut"));
                        }
                        items.push(item);
                    }
                    if items.len() as u64 != count {
                        return Err(Error::Record("history page ended before its retained cut"));
                    }
                }
                items.reverse();
                let mut before = cursor.before;
                for item in &items {
                    cursor.before = item.conversation - 1;
                    let next_bytes = if cursor.before == 0 {
                        None
                    } else {
                        Some(page_json_bytes(&cursor)?.div_ceil(3) * 4)
                    };
                    if !credit.accept(item, next_bytes)? {
                        break;
                    }
                    before = cursor.before;
                }
                items.truncate(credit.items);
                cursor.before = before;
                let next = history_cursor(&cursor)?;
                Ok(AgentReply::History { items, next })
            }
            AgentCommand::Children { page, .. } | AgentCommand::Sessions { page, .. } => {
                let sessions = matches!(command, AgentCommand::Sessions { .. });
                let peers = matches!(
                    command,
                    AgentCommand::Sessions {
                        scope: acyclic_harness_client::SessionsScope::Peers,
                        ..
                    }
                );
                let requester = match command {
                    AgentCommand::Sessions { requester, .. } => {
                        requester.as_ref().unwrap_or(&self.root)
                    }
                    _ => &self.root,
                };
                if peers && requester.parent().is_some() {
                    return Err(Error::Record("peer discovery requires a root requester"));
                }
                let parent = match command {
                    AgentCommand::Children { agent, .. } => self.within_tree(agent)?.children()?,
                    AgentCommand::Sessions { .. } if peers && self.platform.sessions() => {
                        self.bound.peer_roots()?
                    }
                    AgentCommand::Sessions { .. } if self.platform.sessions() => {
                        self.bound.session_roots()?
                    }
                    _ => return Err(Error::Record("this host serves one tree")),
                };
                use base64::{Engine as _, engine::general_purpose::STANDARD};
                let cursor_context =
                    peers.then(|| format!("peer-page-v1\0{}\0{}", requester, parent));
                let mut credit = PageCredit::new(page, |next| AgentReply::Children {
                    items: Vec::new(),
                    next,
                })?;
                let (hierarchy_version, after) = match page.cursor() {
                    Some(cursor) => {
                        let decrypted;
                        let cursor = if let Some(context) = &cursor_context {
                            let sealed = STANDARD
                                .decode(cursor)
                                .map_err(|_| Error::Record("invalid peer cursor"))?;
                            decrypted = self
                                .cursor_key
                                .open(context.as_bytes(), &sealed)
                                .map_err(|_| Error::Record("invalid peer cursor"))?;
                            std::str::from_utf8(&decrypted)
                                .map_err(|_| Error::Record("invalid peer cursor"))?
                        } else {
                            cursor
                        };
                        let (version, after) = parse_children_cursor(cursor)?;
                        (Some(version), Some(after))
                    }
                    None => (None, None),
                };
                let provider = agents.provider();
                let request = HeadsRequest {
                    at: None,
                    retained: Default::default(),
                    durable_through: None,
                    projection: HeadProjection::Watermarks,
                    paths: Default::default(),
                    children: Some(ChildrenPageRequest {
                        offset: 0,
                        tail_stream: None,
                        parent: Some(parent),
                        after,
                        hierarchy_version,
                        limit: page.items(),
                    }),
                    limit: page.items(),
                    at_least: Default::default(),
                };
                let view = match provider.heads(request.clone()).await {
                    Ok(view) => {
                        view.validate_for(&request).map_err(Error::Stream)?;
                        view
                    }
                    Err(error) if error.category() == StreamError::NotFound => {
                        return Ok(AgentReply::Children {
                            items: Vec::new(),
                            next: None,
                        });
                    }
                    Err(error) => return Err(Error::Stream(error)),
                };
                let listed = view
                    .children
                    .ok_or(Error::Stream(StreamError::InvalidResponse))?;
                let mut items = Vec::with_capacity(listed.children.len());
                let mut listed_paths = Vec::with_capacity(listed.children.len());
                let mut paths = acyclic_stream::HeadSet::with_capacity(listed.children.len());
                for child in listed.children {
                    let agent = if matches!(command, AgentCommand::Sessions { .. }) {
                        let id = child
                            .path
                            .as_str()
                            .rsplit('/')
                            .next()
                            .ok_or(Error::Record("session entry has no identity"))?;
                        AgentPath::root(&crate::agent::AgentId::try_from(id.to_owned())?)
                    } else {
                        AgentPath::try_from(child.path.as_str().to_owned())?
                    };
                    listed_paths.push(child.path.clone());
                    paths.push(agent.status()?);
                    if sessions {
                        paths.push(child.path);
                    }
                    if peers {
                        paths.push(agent.peer_membership()?);
                    }
                    items.push(ChildAgent {
                        agent,
                        status: None,
                        created_at_micros: None,
                        updated_at_micros: None,
                    });
                }
                let last_scanned = listed_paths.last().cloned();
                if !items.is_empty() || peers {
                    if peers {
                        paths.push(requester.peer_membership()?);
                    }
                    // Status is published outside each child's fork destination.
                    // Observe its actual canonical path, at or after the page's
                    // creation cut, in one bounded view for the returned rows.
                    let request = HeadsRequest {
                        at: None,
                        retained: Default::default(),
                        durable_through: None,
                        projection: HeadProjection::History,
                        paths,
                        children: None,
                        limit: page.items()
                            * if peers {
                                3
                            } else if sessions {
                                2
                            } else {
                                1
                            }
                            + u32::from(peers),
                        at_least: RevisionCut::of(view.snapshot),
                    };
                    let statuses = provider
                        .heads(request.clone())
                        .await
                        .map_err(Error::Stream)?;
                    statuses.validate_for(&request).map_err(Error::Stream)?;
                    let mut heads = statuses.heads.into_iter();
                    let mut eligible = Vec::with_capacity(items.len());
                    for item in &mut items {
                        let record = heads
                            .next()
                            .flatten()
                            .and_then(|head| head.last)
                            .ok_or(Error::Record("admitted child has no published status"))?;
                        item.updated_at_micros = Some(record.committed_at_micros);
                        item.status = Some(StatusRecord::decode(&record.value)?);
                        if sessions {
                            let member = heads
                                .next()
                                .flatten()
                                .and_then(|head| head.last)
                                .ok_or(Error::Record("session has no directory admission"))?;
                            if member.sequence != 0 {
                                return Err(Error::Record(
                                    "session directory admission is not immutable",
                                ));
                            }
                            item.created_at_micros = Some(member.committed_at_micros);
                        }
                        if peers {
                            let member = heads.next().flatten().and_then(|head| head.last);
                            eligible.push(
                                member
                                    .map(|record| {
                                        crate::agent::PeerMembership::decode(&record.value)
                                    })
                                    .transpose()?
                                    .is_some_and(|member| member.enabled)
                                    && &item.agent != requester,
                            );
                        }
                    }
                    if peers {
                        let member = heads.next().flatten().and_then(|head| head.last);
                        if !member
                            .map(|record| crate::agent::PeerMembership::decode(&record.value))
                            .transpose()?
                            .is_some_and(|member| member.enabled)
                        {
                            return Err(Error::Record("peer discovery requires an enabled root"));
                        }
                        let mut index = 0;
                        items.retain(|_| {
                            let keep = eligible[index];
                            index += 1;
                            keep
                        });
                        let mut index = 0;
                        listed_paths.retain(|_| {
                            let keep = eligible[index];
                            index += 1;
                            keep
                        });
                    }
                }
                for (index, item) in items.iter().enumerate() {
                    let next_bytes =
                        (index + 1 < items.len() || listed.next_after.is_some()).then(|| {
                            let bytes = children_cursor_bytes(
                                &listed.hierarchy_version,
                                listed_paths[index].as_str(),
                            );
                            (if peers {
                                acyclic_auth::cursor::CursorKey::sealed_len(bytes).div_ceil(3) * 4
                            } else {
                                bytes
                            }) as u64
                        });
                    if !credit.accept(item, next_bytes)? {
                        break;
                    }
                }
                let all_fit = credit.items == items.len();
                let continued = !all_fit || listed.next_after.is_some();
                items.truncate(credit.items);
                let next = if continued {
                    let last = if all_fit {
                        last_scanned.as_ref().ok_or(Error::Record(
                            "children page cannot continue without a source row",
                        ))?
                    } else {
                        listed_paths
                            .get(items.len().checked_sub(1).ok_or(Error::Record(
                                "children page cannot continue without a row",
                            ))?)
                            .ok_or(Error::Record("children page lost its source cursor"))?
                    };
                    let cursor = children_cursor(&listed.hierarchy_version, last.as_str());
                    Some(if let Some(context) = &cursor_context {
                        STANDARD.encode(
                            self.cursor_key
                                .seal(context.as_bytes(), cursor.as_bytes())
                                .map_err(|_| Error::Record("peer cursor encryption failed"))?,
                        )
                    } else {
                        cursor
                    })
                } else {
                    None
                };
                if items.is_empty()
                    && next.is_some()
                    && !acyclic_harness_client::encoded_agent_reply_size(page_json_bytes(
                        &AgentReply::Children {
                            items: Vec::new(),
                            next: next.clone(),
                        },
                    )?)
                    .is_ok_and(|bytes| bytes <= u64::from(page.bytes()))
                {
                    return Err(Error::Stream(StreamError::Capacity));
                }
                Ok(AgentReply::Children { items, next })
            }
            AgentCommand::Spawn {
                parent,
                id,
                brief,
                role,
            }
            | AgentCommand::Fork {
                parent,
                id,
                brief,
                role,
                ..
            } => {
                let parent = self.within_tree(parent)?;
                if !self.platform.runs_children() {
                    return Err(Error::SpawnRefused(SpawnRefusal::Unhosted));
                }
                // The parent's role admits the child's before anything commits.
                let continuation = match command {
                    AgentCommand::Fork { continuation, .. } => continuation.as_ref(),
                    _ => None,
                };
                let mut profile = if let Some(request) = continuation {
                    if request.version != 1 {
                        return Err(Error::Record("unsupported continuation version; use v1"));
                    }
                    let mut profile = agents
                        .state(parent)
                        .await?
                        .profile
                        .ok_or(Error::Record("continuation requires a hosted decider"))?;
                    if profile.context != crate::agent::SpawnContext::TopLevel
                        && profile.continuation.is_none()
                    {
                        return Err(Error::Record("continuation v1 requires a decider"));
                    }
                    profile.context = crate::agent::SpawnContext::Fork;
                    profile
                } else {
                    self.bound.child_profile(parent, role).await?
                };
                // A fork selects history, which a fresh-context role cannot
                // inherit; it is refused before the parent's inbox admits it.
                if matches!(command, AgentCommand::Fork { .. })
                    && profile.context == crate::agent::SpawnContext::Fresh
                {
                    return Err(Error::SpawnRefused(SpawnRefusal::Role));
                }
                let history = match command {
                    AgentCommand::Fork { at, .. } => {
                        let saved = crate::filesystem::history_checkpoint(
                            agents.provider().as_ref(),
                            parent,
                            *at,
                        )
                        .await
                        .map_err(|error| Error::Turn(Box::new(error)))?;
                        if let Some(request) = continuation {
                            let override_item = if request.override_item.is_some() {
                                let source = serde_json::to_string(request)
                                    .map_err(|_| Error::Record("continuation cannot encode"))?;
                                Some(
                                    self.bound
                                        .stage_text(
                                            parent,
                                            "continuation",
                                            by,
                                            id,
                                            (&source, &source),
                                        )
                                        .await?,
                                )
                            } else {
                                None
                            };
                            profile.continuation = Some(Box::new(crate::agent::Continuation::at(
                                parent,
                                *at,
                                &saved,
                                override_item,
                            )));
                        }
                        Some(crate::agent::HistorySelection {
                            conversation: saved.conversation,
                            checkpoint: saved.checkpoint,
                        })
                    }
                    _ => None,
                };
                let brief = if continuation.is_some() {
                    None
                } else {
                    let brief = text(brief)?;
                    Some(Box::new(
                        self.bound
                            .stage_text(parent, "brief", by, id, (brief, brief))
                            .await?,
                    ))
                };
                let record = InboxRecord::Spawn(SpawnRequest {
                    by: by.clone(),
                    id: id.clone(),
                    brief,
                    profile,
                    history,
                });
                let Delivered::Committed { inbox } = agents.send(parent, by, &record).await? else {
                    return Err(Error::Record("the parent is archived"));
                };
                // The parent's step applies the request against every spawn
                // before it and commits the outcome; the reply is that record.
                let (intent, _) = requested_child(parent, inbox)?;
                let child = match agents.spawned(parent, inbox, &intent).await? {
                    SpawnOutcome::Spawned { child } => child,
                    SpawnOutcome::Refused { reason } => return Err(Error::SpawnRefused(reason)),
                };
                Ok(AgentReply::Spawned {
                    agent: child,
                    delivery: Delivered::Committed { inbox },
                })
            }
            AgentCommand::Approvals { agent, page } => {
                let agent = self.within_tree(agent)?;
                // An agent waits on at most one approval: one page holds it.
                let pending = match page.cursor() {
                    Some(_) => None,
                    None => self.bound.pending_approval(agent).await?,
                };
                Ok(AgentReply::Approvals {
                    items: pending
                        .filter(|_| page.items() > 0)
                        .map(|(id, status, binding)| PendingApproval {
                            id,
                            status,
                            binding,
                        })
                        .into_iter()
                        .collect(),
                    next: None,
                })
            }
            AgentCommand::Stage {
                agent,
                digest,
                part,
                text,
            } => {
                let agent = self.within_tree(agent)?;
                // A part's source is its own identity: its digest and position.
                let intent = part_intent(digest, *part)?;
                self.bound
                    .stage_text(agent, STAGED_PART, by, &intent, (intent.as_str(), text))
                    .await?;
                Ok(AgentReply::Staged {})
            }
            // The call assembled its parts before dispatch.
            AgentCommand::Staged { .. } => Err(Error::Record("a staged command is not assembled")),
            AgentCommand::Decide {
                agent,
                id,
                binding,
                verdict,
                reason,
            } => {
                let agent = self.within_tree(agent)?;
                let Sender::User { .. } = by else {
                    return Err(Error::Record("a decision needs a human principal"));
                };
                Ok(AgentReply::Delivered {
                    delivery: self
                        .bound
                        .decide(agent, id, binding, approver, *verdict, reason.clone())
                        .await?,
                })
            }
        }
    }

    /// The whole encoding of a staged command: its `parts`, staged by `by`
    /// under `digest` in `agent`'s private volume, in order, checked against
    /// the digest and length its head names. It runs only for an authorized
    /// head, and holds at most what that same caller already staged: each
    /// part is hashed as it is read, and assembly stops at the first part
    /// that is missing or overruns the head's length.
    async fn assemble(
        &self,
        agent: &AgentPath,
        by: &Sender,
        digest: &str,
        parts: u32,
        bytes: u64,
    ) -> Result<Vec<u8>, Error> {
        let agent = self.within_tree(agent)?;
        let mut whole = Vec::new();
        let mut hasher = blake3::Hasher::new();
        for part in 0..parts {
            let intent = part_intent(digest, part)?;
            let file = self
                .bound
                .staged_text(agent, STAGED_PART, by, &intent, intent.as_str())
                .await?
                .ok_or(Error::Record("a staged command part is missing"))?;
            let (text, length) = self
                .bound
                .read_file(agent, &file, 0, MAX_AGENT_COMMAND_BYTES as u64)
                .await?;
            if text.len() as u64 != length || (whole.len() + text.len()) as u64 > bytes {
                return Err(Error::Record("staged command parts exceed their length"));
            }
            hasher.update(&text);
            whole.extend_from_slice(&text);
        }
        if whole.len() as u64 != bytes || hasher.finalize().to_hex().as_str() != digest {
            return Err(Error::Record(
                "staged command parts do not match their digest",
            ));
        }
        Ok(whole)
    }

    /// The tail of stream `path`; an absent stream holds nothing.
    async fn tail(&self, path: StreamPath) -> Result<u64, Error> {
        match self.bound.agents().provider().tail(path).await {
            Ok(tail) => Ok(tail),
            Err(error) if error.category() == StreamError::NotFound => Ok(0),
            Err(error) => Err(Error::Stream(error)),
        }
    }
}

/// The text of a prompt or brief; hosted agents take text input.
/// The staged-input kind each part of a staged command is held under.
const STAGED_PART: &str = "command-part";

/// The intent one part of the command staged under `digest` is held under.
fn part_intent(digest: &str, part: u32) -> Result<IntentId, Error> {
    IntentId::try_from(format!("{digest}.{part}"))
}

fn text(content: &ModelContent) -> Result<&str, Error> {
    match content {
        ModelContent::Text(text) => Ok(text.as_str()),
        _ => Err(Error::Record("agent input must be text")),
    }
}

/// AgentPath admits only fixed ASCII segments and hex identities, so this
/// cursor's content needs no JSON escaping. Share its size with allocation.
fn children_cursor_bytes(version: &CommitId, after: &str) -> usize {
    version.as_bytes().len() * 2 + 1 + after.len()
}

/// A children page's continuation: the listing's hierarchy version, which a
/// later page must name, and the last returned agent.
fn children_cursor(version: &CommitId, after: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut cursor = String::with_capacity(children_cursor_bytes(version, after));
    for byte in version.as_bytes() {
        cursor.push(char::from(HEX[usize::from(byte >> 4)]));
        cursor.push(char::from(HEX[usize::from(byte & 0xf)]));
    }
    cursor.push(':');
    cursor.push_str(after);
    cursor
}

fn page_json_bytes(value: &impl serde::Serialize) -> Result<u64, Error> {
    crate::executor::json::json_byte_length(value, None, &mut None)
        .map_err(|_| Error::Record("agent page encoding failed"))
}

/// Count each bounded row once, then reserve its actual canonical wire envelope.
/// Cursor content is counted separately from its quotes in the empty envelope.
struct PageCredit {
    end_envelope: u64,
    next_envelope: u64,
    bytes: u64,
    items: usize,
    maximum: u64,
}

impl PageCredit {
    fn new(
        page: &AgentPageRequest,
        empty: impl Fn(Option<String>) -> AgentReply,
    ) -> Result<Self, Error> {
        Self::from_envelopes(page, &empty(None), &empty(Some(String::new())))
    }

    fn from_envelopes(
        page: &AgentPageRequest,
        end: &AgentReply,
        next: &AgentReply,
    ) -> Result<Self, Error> {
        let end_envelope = page_json_bytes(end)?;
        let maximum = u64::from(page.bytes());
        if !acyclic_harness_client::encoded_agent_reply_size(end_envelope)
            .is_ok_and(|encoded| encoded <= maximum)
        {
            return Err(Error::Stream(StreamError::Capacity));
        }
        Ok(Self {
            end_envelope,
            next_envelope: page_json_bytes(next)?,
            bytes: 0,
            items: 0,
            maximum,
        })
    }

    fn accept(
        &mut self,
        item: &impl serde::Serialize,
        next_bytes: Option<u64>,
    ) -> Result<bool, Error> {
        let Some(bytes) = self.admits(item, next_bytes)? else {
            return if self.items == 0 {
                Err(Error::Stream(StreamError::Capacity))
            } else {
                Ok(false)
            };
        };
        self.bytes = bytes;
        self.items += 1;
        Ok(true)
    }

    /// The page's row bytes with `item` appended, or `None` when it would
    /// exceed the requested credit.
    fn admits(
        &self,
        item: &impl serde::Serialize,
        next_bytes: Option<u64>,
    ) -> Result<Option<u64>, Error> {
        let bytes = self.bytes + page_json_bytes(item)? + u64::from(self.items != 0);
        let envelope = next_bytes.map_or(self.end_envelope, |bytes| self.next_envelope + bytes);
        Ok(
            acyclic_harness_client::encoded_agent_reply_size(bytes + envelope)
                .is_ok_and(|encoded| encoded <= self.maximum)
                .then_some(bytes),
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryCursor {
    version: u32,
    agent: AgentPath,
    before: u64,
    tail: u64,
}

fn history_cursor(cursor: &HistoryCursor) -> Result<Option<String>, Error> {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    (cursor.before != 0)
        .then(|| {
            serde_json::to_vec(cursor)
                .map(|bytes| STANDARD.encode(bytes))
                .map_err(|_| Error::Record("history cursor encoding failed"))
        })
        .transpose()
}

/// The hierarchy version and last path [`children_cursor`] encoded.
fn parse_children_cursor(cursor: &str) -> Result<(CommitId, StreamPath), Error> {
    let invalid = || Error::Stream(StreamError::InvalidArgument);
    let (version, after) = cursor.split_once(':').ok_or_else(invalid)?;
    if version.len() != 64 {
        return Err(invalid());
    }
    let mut bytes = [0; 32];
    for (byte, pair) in bytes.iter_mut().zip(version.as_bytes().chunks_exact(2)) {
        let pair = std::str::from_utf8(pair).map_err(|_| invalid())?;
        *byte = u8::from_str_radix(pair, 16).map_err(|_| invalid())?;
    }
    Ok((
        CommitId::from_bytes(bytes),
        StreamPath::new(after).map_err(Error::Stream)?,
    ))
}

impl<P, O> CallProvider for Server<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    fn target(
        &self,
        invocation: &[u8],
    ) -> Result<acyclic_stream::session::CallTarget, StreamError> {
        self.admit(invocation)?;
        acyclic_stream::session::CallTarget::new(self.resource.clone(), "actors/invoke")
    }

    fn call(
        &self,
        invocation: Bytes,
        authorization: CallAuthorization,
    ) -> BoxProviderFuture<'static, Result<Bytes, StreamError>> {
        let admitted = self.admit(&invocation);
        let server = self.this.upgrade();
        Box::pin(async move {
            let call = admitted?;
            let server = server.ok_or(StreamError::Unavailable)?;
            let principal = authorization.principal()?;
            let by = Sender::User {
                principal: principal.id.clone(),
            };
            let attributes = acyclic_auth::contract::RecordAttributes::try_new(Vec::new())
                .map_err(|_| StreamError::InvalidArgument)?;
            let authorize = async |command: &AgentCommand| {
                let policy = server.platform.policy(&principal, command).await?;
                server.authorize_command(&authorization, command, &attributes, policy)
            };
            // Every command, a staged head included, is authorized as it
            // arrives: a head names its agent and kind, so a denied caller
            // makes the host read no part.
            authorize(call.command()).await?;
            // A staged command is then run and answered as the whole command
            // its parts assemble to, itself authorized like one frame.
            let call = match call.command() {
                AgentCommand::Staged {
                    agent,
                    digest,
                    parts,
                    bytes,
                    ..
                } => {
                    let whole = server.assemble(agent, &by, digest, *parts, *bytes).await;
                    match whole.and_then(|whole| {
                        call.assembled(&whole)
                            .map_err(|_| Error::Record("a staged command does not decode"))
                    }) {
                        Ok(whole) => {
                            authorize(whole.command()).await?;
                            whole
                        }
                        Err(error) => {
                            return reply(&call, &Err(error));
                        }
                    }
                }
                _ => call,
            };
            server.platform.admitted().await?;
            let approver = server.platform.approver(&principal);
            let dispatch = server.dispatch(call.command(), &by, &approver);
            #[cfg(test)]
            {
                let bytes = std::mem::size_of_val(&dispatch);
                // Five original references, an operation owner and alignment.
                let maximum = 8 * std::mem::size_of::<usize>();
                assert!(
                    bytes <= maximum,
                    "server dispatch caller retains {bytes} bytes, above {maximum} bytes"
                );
                eprintln!("server dispatch caller: {bytes} bytes");
            }
            let result = dispatch.await;
            let query = match call.command() {
                AgentCommand::Bind { .. }
                | AgentCommand::Root {}
                | AgentCommand::ResolveSession { .. }
                | AgentCommand::LatestSession {}
                | AgentCommand::Sessions { .. }
                | AgentCommand::Children { .. }
                | AgentCommand::ProjectHeads { .. }
                | AgentCommand::History { .. }
                | AgentCommand::Directory { .. }
                | AgentCommand::ChildWakes { .. }
                | AgentCommand::Approvals { .. }
                | AgentCommand::Reply { .. }
                | AgentCommand::Settlement { .. }
                | AgentCommand::PreparePeer { .. }
                | AgentCommand::Diff { .. }
                | AgentCommand::Usage { .. }
                | AgentCommand::Events { .. }
                | AgentCommand::AgentFilePreviews { .. }
                | AgentCommand::AgentFiles { .. }
                | AgentCommand::ClientFiles { .. }
                | AgentCommand::Read { .. } => true,
                AgentCommand::NewSession { .. }
                | AgentCommand::NameSession { .. }
                | AgentCommand::SelectSession { .. }
                | AgentCommand::ForkSession { .. }
                | AgentCommand::Pause { .. }
                | AgentCommand::MergeSession { .. }
                | AgentCommand::Prompt { .. }
                | AgentCommand::SetPeerMembership { .. }
                | AgentCommand::Cancel { .. }
                | AgentCommand::Compact { .. }
                | AgentCommand::Rewind { .. }
                | AgentCommand::Restore { .. }
                | AgentCommand::ClearAll { .. }
                | AgentCommand::Spawn { .. }
                | AgentCommand::Fork { .. }
                | AgentCommand::Decide { .. }
                | AgentCommand::PublishAgentFiles { .. }
                | AgentCommand::Archive { .. }
                | AgentCommand::PublishClientFiles { .. }
                | AgentCommand::Stage { .. }
                | AgentCommand::Staged { .. } => false,
            };
            if query {
                // Reads may suspend during admission or source IO. Revalidate
                // before disclosing either their data or source errors. Effect
                // receipts retain their original commit outcome semantics.
                let principal = authorization.principal()?;
                let policy = server.platform.policy(&principal, call.command()).await?;
                server.authorize_command(&authorization, call.command(), &attributes, policy)?;
            }
            reply(&call, &result)
        })
    }
}

// The serving boundary must not turn an invalid reply into a capacity refusal.
// Record only the command kind and stable codec failure, never command bodies.
fn reply(
    call: &acyclic_harness_client::AgentCall,
    result: &Result<AgentReply, Error>,
) -> Result<Bytes, StreamError> {
    call.reply(result).map_err(|error| {
        tracing::error!(
            command = ?call.command().kind(),
            code = error.code(),
            "agent reply encoding refused"
        );
        match error {
            acyclic_harness_client::CommandError::Invalid => StreamError::InvalidResponse,
            acyclic_harness_client::CommandError::Capacity => StreamError::LimitExceeded,
        }
    })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "hosting_tests.rs"]
mod tests;
