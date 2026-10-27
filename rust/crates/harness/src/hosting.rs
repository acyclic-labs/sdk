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
//! tokio::spawn(async move { server.run(|actor, error| report(actor, error)).await });
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

use crate::{
    agent::{
        AgentPath, AgentRecord, Control, Delivered, Error, InboxRecord, IntentId, Message, Sender,
        SpawnOutcome, SpawnRequest, StatusRecord, requested_child,
    },
    model::ModelContent,
};
use acyclic_auth::{authority::PolicyDecision, contract::Principal};
use acyclic_fs::{AsyncObjectStore, distributed::AuthorityProvider};
use acyclic_harness_client::{
    AgentBinding, AgentCall, AgentCommand, AgentReply, ChildAgent, MAX_READ_BYTES, PendingApproval,
    ReadTarget,
};
use acyclic_stream::{
    BoxProviderFuture, ChildrenPageRequest, CommitId, ProviderPlatform, ReadRequest, StreamError,
    StreamPath,
    session::{CallAuthorization, CallProvider},
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use std::sync::Arc;

/// What a hosting platform decides at every dispatch: the owner's current
/// policy for the verified principal's command. Identity comes only from the
/// session credential; the platform never supplies a caller here.
pub trait Platform: ProviderPlatform + 'static {
    /// The current policy for `principal` issuing `command`.
    fn policy<'a>(
        &'a self,
        principal: &'a Principal,
        command: &'a AgentCommand,
    ) -> BoxProviderFuture<'a, Result<PolicyDecision, StreamError>>;

    /// Durable wake admission, after both credential and current policy allow
    /// dispatch, and before any command effect. Native continuous runtimes need
    /// no additional wake; event platforms retain their recovery alarm here.
    fn admitted(&self) -> BoxProviderFuture<'_, Result<(), StreamError>> {
        Box::pin(async { Ok(()) })
    }
}

/// The routing URL of every agent command; a host dispatches on the command.
const BINDING_URL: &str = "/agents/command";

/// One admitted agent tree served over the unified session protocol.
pub struct Server<P: AuthorityProvider + ?Sized + 'static, O: 'static> {
    this: std::sync::Weak<Self>,
    bound: Bound<P, O>,
    platform: Arc<dyn Platform>,
    root: AgentPath,
    resource: String,
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
    Ok(Serving::Served(server(bound, platform, root)))
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
    Ok(server(bound, platform, root))
}

fn server<P, O>(
    bound: Bound<P, O>,
    platform: Arc<dyn Platform>,
    root: AgentPath,
) -> Arc<Server<P, O>>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    let resource = format!(
        "actors:/{}",
        acyclic_auth::encode_component(bound.within().namespace().as_str().as_bytes())
    );
    Arc::new_cyclic(|this| Server {
        this: this.clone(),
        bound,
        platform,
        root,
        resource,
    })
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
        let within = self.bound.within();
        acyclic_actors::wire::InvokeActorRequest {
            actor_id: within.id().to_owned(),
            method: "POST".into(),
            url: BINDING_URL.into(),
            namespace: Some(within.namespace().as_str().to_owned()),
            account_id: within.account().as_bytes().to_vec(),
            ..Default::default()
        }
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
            return if matches!(call.command(), AgentCommand::Bind) {
                Ok(call)
            } else {
                Err(StreamError::AccessDenied)
            };
        }
        if matches!(call.command(), AgentCommand::Bind)
            || routed.namespace.as_deref() != Some(within.namespace().as_str())
            || routed.account_id.as_slice() != within.account().as_bytes()
        {
            return Err(StreamError::AccessDenied);
        }
        Ok(call)
    }

    /// `agent` when it is the root or one of its descendants.
    fn within_tree<'a>(&self, agent: &'a AgentPath) -> Result<&'a AgentPath, Error> {
        let mut cursor = Some(agent.clone());
        while let Some(path) = cursor {
            if path == self.root {
                return Ok(agent);
            }
            cursor = path.parent();
        }
        Err(Error::NoSuchAgent)
    }

    /// The `fs:/` resource announcing the root's tool output, on a host whose
    /// volumes announce it.
    async fn tool_resource(&self) -> Result<String, Error> {
        #[cfg(feature = "filesystem-local")]
        return self.bound.tool_resource(&self.root).await;
        #[cfg(not(feature = "filesystem-local"))]
        Err(Error::Record("this host announces no tool output"))
    }

    async fn dispatch(&self, command: &AgentCommand, by: &Sender) -> Result<AgentReply, Error> {
        let agents = self.bound.agents();
        match command {
            AgentCommand::Bind => Ok(AgentBinding::reply(
                &self.binding(),
                self.tool_resource().await?,
            )),
            AgentCommand::Root => Ok(AgentReply::Root {
                agent: self.root.clone(),
            }),
            AgentCommand::Settlement { agent } => {
                let agent = self.within_tree(agent)?;
                Ok(AgentReply::Settlement {
                    settled: self.tail(agent.conversation()?).await?,
                    statuses: self.tail(agent.status()?).await?,
                    phase: agents.state(agent).await?.phase,
                })
            }
            AgentCommand::Read {
                agent,
                target,
                offset,
            } => {
                let agent = self.within_tree(agent)?;
                let (bytes, length) = match target {
                    ReadTarget::File { file } => {
                        self.bound
                            .read_file(agent, file, *offset, MAX_READ_BYTES)
                            .await?
                    }
                    #[cfg(feature = "filesystem-local")]
                    ReadTarget::ToolOutput { path } => {
                        self.bound
                            .read_tool_output(agent, path, *offset, MAX_READ_BYTES)
                            .await?
                    }
                    #[cfg(not(feature = "filesystem-local"))]
                    ReadTarget::ToolOutput { .. } => {
                        return Err(Error::Record("this host announces no tool output"));
                    }
                };
                Ok(AgentReply::read(&bytes, length))
            }
            AgentCommand::Prompt { agent, id, body } => {
                let agent = self.within_tree(agent)?;
                let body = self
                    .bound
                    .stage_text(agent, "message", by, id, text(body)?)
                    .await?;
                let record = InboxRecord::Message(Message {
                    from: by.clone(),
                    id: id.clone(),
                    body: Box::new(body),
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
            AgentCommand::Children { agent, page } => {
                let agent = self.within_tree(agent)?;
                let (hierarchy_version, after) = match page.cursor() {
                    Some(cursor) => {
                        let (version, after) = parse_children_cursor(cursor)?;
                        (Some(version), Some(after))
                    }
                    None => (None, None),
                };
                let listed = match agents
                    .provider()
                    .children_page(ChildrenPageRequest {
                        tail_stream: None,
                        parent: Some(agent.children()?),
                        after,
                        hierarchy_version,
                        limit: page.items(),
                    })
                    .await
                {
                    Ok(listed) => listed,
                    Err(StreamError::NotFound) => {
                        return Ok(AgentReply::Children {
                            items: Vec::new(),
                            next: None,
                        });
                    }
                    Err(error) => return Err(Error::Stream(error)),
                };
                let mut items = Vec::with_capacity(listed.children.len());
                for child in listed.children {
                    let agent = AgentPath::try_from(child.path.as_str().to_owned())?;
                    let status = self.status(&agent).await?;
                    items.push(ChildAgent { agent, status });
                }
                Ok(AgentReply::Children {
                    items,
                    next: listed
                        .next_after
                        .map(|after| children_cursor(&listed.hierarchy_version, &after)),
                })
            }
            AgentCommand::Spawn { parent, id, brief } => {
                let parent = self.within_tree(parent)?;
                let record = InboxRecord::Spawn(SpawnRequest {
                    by: by.clone(),
                    id: id.clone(),
                    brief: text(brief)?.to_owned(),
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
                        .map(|(id, request)| PendingApproval { id, request })
                        .into_iter()
                        .collect(),
                    next: None,
                })
            }
            AgentCommand::Decide {
                agent,
                id,
                binding,
                approved,
                reason,
            } => {
                let agent = self.within_tree(agent)?;
                let Sender::User { principal } = by else {
                    return Err(Error::Record("a decision needs a human principal"));
                };
                Ok(AgentReply::Delivered {
                    delivery: self
                        .bound
                        .decide(agent, id, binding, principal, *approved, reason.clone())
                        .await?,
                })
            }
        }
    }

    /// The tail of stream `path`; an absent stream holds nothing.
    async fn tail(&self, path: StreamPath) -> Result<u64, Error> {
        match self.bound.agents().provider().tail(path).await {
            Ok(tail) => Ok(tail),
            Err(StreamError::NotFound) => Ok(0),
            Err(error) => Err(Error::Stream(error)),
        }
    }

    /// `agent`'s last published status, if any.
    async fn status(&self, agent: &AgentPath) -> Result<Option<StatusRecord>, Error> {
        let provider = self.bound.agents().provider();
        let path = agent.status()?;
        let tail = match provider.tail(path.clone()).await {
            Ok(tail) => tail,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(Error::Stream(error)),
        };
        let Some(last) = tail.checked_sub(1) else {
            return Ok(None);
        };
        let mut records = provider
            .read(ReadRequest {
                path,
                from: last,
                limit: 1,
            })
            .await
            .map_err(Error::Stream)?;
        match records.try_next().await.map_err(Error::Stream)? {
            Some(record) => Ok(Some(StatusRecord::decode(&record.value)?)),
            None => Ok(None),
        }
    }
}

/// The text of a prompt or brief; hosted agents take text input.
fn text(content: &ModelContent) -> Result<&str, Error> {
    match content {
        ModelContent::Text(text) => Ok(text.as_str()),
        _ => Err(Error::Record("agent input must be text")),
    }
}

/// A children page's continuation: the listing's hierarchy version, which a
/// later page must name, and the last listed path.
fn children_cursor(version: &CommitId, after: &StreamPath) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut cursor = String::with_capacity(65 + after.as_str().len());
    for byte in version.as_bytes() {
        cursor.push(char::from(HEX[usize::from(byte >> 4)]));
        cursor.push(char::from(HEX[usize::from(byte & 0xf)]));
    }
    cursor.push(':');
    cursor.push_str(after.as_str());
    cursor
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
    fn resource(&self, invocation: &[u8]) -> Result<String, StreamError> {
        self.admit(invocation)?;
        Ok(self.resource.clone())
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
            let policy = server.platform.policy(&principal, call.command()).await?;
            let attributes = acyclic_auth::contract::RecordAttributes::try_new(Vec::new())
                .map_err(|_| StreamError::InvalidArgument)?;
            authorization.authorize(&server.resource, &[], &attributes, policy)?;
            server.platform.admitted().await?;
            let by = Sender::User {
                principal: principal.id.clone(),
            };
            let result = server.dispatch(call.command(), &by).await;
            call.reply(&result).map_err(|_| StreamError::LimitExceeded)
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "hosting_tests.rs"]
mod tests;
