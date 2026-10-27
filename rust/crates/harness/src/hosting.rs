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
        AgentPath, AgentRecord, Control, Delivered, Error, InboxRecord, IntentId,
        MAX_OPEN_CHILDREN, Message, Sender, SpawnRequest, StatusRecord, requested_child,
    },
    approvals::ApprovalResponder,
    interaction::Interaction,
    model::ModelContent,
};
use acyclic_auth::{authority::PolicyDecision, contract::Principal};
use acyclic_fs::AsyncObjectStore;
use acyclic_harness_client::{AgentCall, AgentCommand, AgentReply, ChildAgent, PendingApproval};
use acyclic_stream::{
    BoxProviderFuture, ChildrenPageRequest, CommitId, ProviderPlatform, ReadRequest, StreamError,
    StreamPath, StreamProvider as _,
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
}

/// One admitted agent tree served over the unified session protocol.
pub struct Server<P: AttestedProvider + 'static, O: 'static> {
    this: std::sync::Weak<Self>,
    bound: Bound<P, O>,
    platform: Arc<dyn Platform>,
    root: AgentPath,
    resource: String,
}

/// What serving a root came to.
pub enum Serving<P: AttestedProvider + 'static, O: 'static> {
    /// The root is created and served.
    Served(Arc<Server<P, O>>),
    /// The root's creation is uncertain: [`Unsettled::resubmit`] submits the
    /// identical prepared commit, which still holds its fresh volumes'
    /// objects.
    Unknown(Unsettled<P, O>),
}

/// A root whose creation is uncertain, owning its prepared commit.
pub struct Unsettled<P: AttestedProvider + 'static, O: 'static> {
    bound: Bound<P, O>,
    platform: Arc<dyn Platform>,
    prepared: Prepared,
}

impl<P, O> Unsettled<P, O>
where
    P: AttestedProvider + 'static,
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
    P: AttestedProvider + 'static,
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
    P: AttestedProvider + 'static,
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
    let resource = format!(
        "actors:/{}",
        acyclic_auth::encode_component(bound.within().namespace().as_str().as_bytes())
    );
    Ok(Serving::Served(Arc::new_cyclic(|this| Server {
        this: this.clone(),
        bound,
        platform,
        root,
        resource,
    })))
}

impl<P, O> Server<P, O>
where
    P: AttestedProvider + 'static,
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
        if routed.namespace.as_deref() != Some(within.namespace().as_str())
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

    async fn dispatch(&self, command: &AgentCommand, by: &Sender) -> Result<AgentReply, Error> {
        let agents = self.bound.agents();
        match command {
            AgentCommand::Root => Ok(AgentReply::Root {
                agent: self.root.clone(),
            }),
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
                let child = requested_child(parent, inbox)?.1;
                // The parent's step consumes a request it cannot honour (it is
                // archived or has its open-children limit) without creating
                // the child, so a recorded refusal is reported, not a path
                // that never appears. A child already created, as on a retry,
                // is spawned whatever the parent's state now.
                let state = agents.state(parent).await?;
                if !state.open_children.contains(&child)
                    && (state.closed || state.open_children.len() >= MAX_OPEN_CHILDREN)
                    && !matches!(agents.state(&child).await, Ok(_))
                {
                    return Err(Error::Record("the parent refused the spawn"));
                }
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
                    None => self.pending(agent).await?,
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
                // The decision binds the exact action the caller was shown:
                // the pending approval, or, for an identical retry after the
                // agent resumed, the request this principal already decided.
                let binds = |shown: Option<Interaction>| {
                    matches!(
                        shown,
                        Some(Interaction::Approval { operation_id, action_digest, .. })
                            if operation_id == binding.operation_id
                                && action_digest == binding.action_digest
                    )
                };
                let pending = self
                    .pending(agent)
                    .await?
                    .and_then(|(pending, shown)| (&pending == id).then_some(shown));
                let shown = if pending.is_some() {
                    binds(pending)
                } else {
                    let state = self.bound.agents().state(agent).await?;
                    binds(
                        self.bound
                            .turns()
                            .decided_approval(&state, id, principal)
                            .await
                            .map_err(|error| Error::Turn(Box::new(error)))?,
                    )
                };
                if !shown {
                    return Err(Error::Record("approval is not pending with this binding"));
                }
                Ok(AgentReply::Delivered {
                    delivery: self
                        .bound
                        .turns()
                        .respond(
                            agents,
                            agent,
                            id,
                            ApprovalResponder::Human(principal),
                            *approved,
                            reason.clone(),
                        )
                        .await?,
                })
            }
        }
    }

    /// The approval `agent` waits on.
    async fn pending(&self, agent: &AgentPath) -> Result<Option<(IntentId, Interaction)>, Error> {
        let state = self.bound.agents().state(agent).await?;
        self.bound
            .turns()
            .pending_approval(&state)
            .await
            .map_err(|error| Error::Turn(Box::new(error)))
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
    P: AttestedProvider + 'static,
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
            let by = Sender::User {
                principal: principal.id.clone(),
            };
            let result = server.dispatch(call.command(), &by).await;
            call.reply(&result).map_err(|_| StreamError::LimitExceeded)
        })
    }
}

#[cfg(test)]
#[path = "hosting_tests.rs"]
mod tests;
