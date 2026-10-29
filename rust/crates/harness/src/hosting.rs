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

#[cfg(not(target_arch = "wasm32"))]
mod runtime;
#[cfg(not(target_arch = "wasm32"))]
pub use runtime::Runtime;

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

    /// A page of the selected workspace's pinned, authorized diff query.
    /// The server has checked the session credential, current policy and agent
    /// tree before delegation. The source must also check file read/list authority
    /// before disclosing any path or content, including on continuation pages.
    /// `authorization` is the original session admission, including its current
    /// credential; a request field or cached principal cannot replace it.
    fn diff<'a>(
        &'a self,
        authorization: &'a CallAuthorization,
        agent: &'a AgentPath,
        page: &'a AgentPageRequest,
    ) -> BoxProviderFuture<'a, crate::Result<Vec<crate::context::DiffFilePage>>> {
        let _ = (authorization, agent, page);
        Box::pin(async {
            Err(crate::Error::Unsupported(
                "workspace diff query is unavailable".into(),
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
            return if matches!(call.command(), AgentCommand::Bind {}) {
                Ok(call)
            } else {
                Err(StreamError::AccessDenied)
            };
        }
        if matches!(call.command(), AgentCommand::Bind {})
            || routed.namespace.as_deref() != Some(within.namespace().as_str())
            || routed.account_id.as_slice() != within.account().as_bytes()
        {
            return Err(StreamError::AccessDenied);
        }
        Ok(call)
    }

    /// `agent` when it is the root or one of its descendants.
    fn within_tree<'a>(&self, agent: &'a AgentPath) -> Result<&'a AgentPath, Error> {
        self.root
            .contains(agent)
            .then_some(agent)
            .ok_or(Error::NoSuchAgent)
    }

    /// The `fs:/` resource announcing the root's tool output, on a host whose
    /// volumes announce it.
    async fn tool_resource(&self) -> Result<String, Error> {
        self.bound.tool_resource(&self.root).await
    }

    /// Delivers one context request to `agent`; its checkpoint settles in
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
    async fn dispatch(
        &self,
        command: &AgentCommand,
        by: &Sender,
        approver: &str,
        authorization: &CallAuthorization,
    ) -> Result<AgentReply, Error> {
        let agents = self.bound.agents();
        match command {
            AgentCommand::Bind {} => Ok(AgentBinding::reply(
                &self.binding(),
                self.tool_resource().await?,
            )),
            AgentCommand::Root {} => Ok(AgentReply::Root {
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
            AgentCommand::Diff { agent, page } => {
                let agent = self.within_tree(agent)?;
                Ok(AgentReply::Diff {
                    items: self
                        .platform
                        .diff(authorization, agent, page)
                        .await
                        .map_err(|error| Error::Turn(Box::new(error)))?,
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
                    ReadTarget::ToolOutput { path } => {
                        self.bound
                            .read_tool_output(agent, path, *offset, MAX_READ_BYTES)
                            .await?
                    }
                };
                Ok(AgentReply::read(&bytes, length))
            }
            AgentCommand::Prompt { agent, id, body } => {
                let agent = self.within_tree(agent)?;
                let source = text(body)?;
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
                        let expanded = self.platform.prompt_command(source).await.map_err(turn)?;
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
                let record = InboxRecord::Message(Message {
                    from: by.clone(),
                    id: id.clone(),
                    body: Box::new(body),
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
            AgentCommand::ClearAll { agent, id } => {
                self.context(self.within_tree(agent)?, by, id, ContextAction::Clear)
                    .await
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
                let provider = agents.provider();
                let request = HeadsRequest {
                    retained: Default::default(),
                    durable_through: None,
                    projection: HeadProjection::Watermarks,
                    paths: Default::default(),
                    children: Some(ChildrenPageRequest {
                        tail_stream: None,
                        parent: Some(agent.children()?),
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
                    Err(StreamError::NotFound) => {
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
                let mut paths = acyclic_stream::HeadSet::with_capacity(listed.children.len());
                for child in listed.children {
                    let agent = AgentPath::try_from(child.path.as_str().to_owned())?;
                    paths.push(agent.status()?);
                    items.push(ChildAgent {
                        agent,
                        status: None,
                    });
                }
                if !items.is_empty() {
                    // Status is published outside each child's fork destination.
                    // Observe its actual canonical path, at or after the page's
                    // creation cut, in one bounded view for the returned rows.
                    let request = HeadsRequest {
                        retained: Default::default(),
                        durable_through: None,
                        projection: HeadProjection::History,
                        paths,
                        children: None,
                        limit: page.items(),
                        at_least: RevisionCut::of(view.snapshot),
                    };
                    let statuses = provider
                        .heads(request.clone())
                        .await
                        .map_err(Error::Stream)?;
                    statuses.validate_for(&request).map_err(Error::Stream)?;
                    for (item, head) in items.iter_mut().zip(statuses.heads) {
                        let record = head
                            .and_then(|head| head.last)
                            .ok_or(Error::Record("admitted child has no published status"))?;
                        item.status = Some(StatusRecord::decode(&record.value)?);
                    }
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
                if !self.platform.runs_children() {
                    return Err(Error::SpawnRefused(SpawnRefusal::Unhosted));
                }
                let brief = text(brief)?;
                let brief = self
                    .bound
                    .stage_text(parent, "brief", by, id, (brief, brief))
                    .await?;
                let record = InboxRecord::Spawn(SpawnRequest {
                    by: by.clone(),
                    id: id.clone(),
                    brief: Box::new(brief),
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
                approved,
                reason,
            } => {
                let agent = self.within_tree(agent)?;
                let Sender::User { .. } = by else {
                    return Err(Error::Record("a decision needs a human principal"));
                };
                Ok(AgentReply::Delivered {
                    delivery: self
                        .bound
                        .decide(agent, id, binding, approver, *approved, reason.clone())
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
            Err(StreamError::NotFound) => Ok(0),
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
            let by = Sender::User {
                principal: principal.id.clone(),
            };
            let attributes = acyclic_auth::contract::RecordAttributes::try_new(Vec::new())
                .map_err(|_| StreamError::InvalidArgument)?;
            let authorize = async |command: &AgentCommand| {
                let policy = server.platform.policy(&principal, command).await?;
                authorization.authorize(&server.resource, &[], &attributes, policy)
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
                            return call
                                .reply(&Err(error))
                                .map_err(|_| StreamError::LimitExceeded);
                        }
                    }
                }
                _ => call,
            };
            server.platform.admitted().await?;
            let approver = server.platform.approver(&principal);
            let result = server
                .dispatch(call.command(), &by, &approver, &authorization)
                .await;
            if matches!(&result, Ok(AgentReply::Diff { .. })) {
                // The source may have awaited IO. No path or patch leaves after
                // its original call credential or current host policy is revoked.
                let principal = authorization.principal()?;
                let policy = server.platform.policy(&principal, call.command()).await?;
                authorization.authorize(&server.resource, &[], &attributes, policy)?;
            }
            call.reply(&result).map_err(|_| StreamError::LimitExceeded)
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "hosting_tests.rs"]
mod tests;
