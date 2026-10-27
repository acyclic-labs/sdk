//! The public serving entry point: one admitted agent tree as a running agent
//! server, dispatching the unified session protocol's agent commands.
//!
//! A platform (managed Swarm, a self-hosted Actors deployment, a local cell)
//! verifies its caller, binds it once with [`Bound::admitted`] over the one
//! admitted [`Placement`], and serves it:
//!
//! ```ignore
//! let bound = Bound::admitted(issuer, caller, placement, turns)?;
//! let server = hosting::serve(bound, platform, &intent, root).await?;
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

pub use acyclic_harness_core::filesystem::hosting::{
    Bound, Placement, Prepared, RootRequest, Turns, VerifiedCaller,
};

use crate::agent::{
    AgentPath, AgentRecord, Control, Error, InboxRecord, IntentId, Message, Sender, StatusRecord,
};
use acyclic_auth::{authority::PolicyDecision, contract::Principal};
use acyclic_fs::{AsyncObjectStore, distributed::AuthorityProvider};
use acyclic_harness_client::{AgentCall, AgentCommand, AgentReply, ChildAgent};
use acyclic_stream::{
    BoxProviderFuture, ChildrenPageRequest, ProviderPlatform, ReadRequest, StreamError, StreamPath,
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
pub struct Server<P: AuthorityProvider + 'static, O: 'static> {
    this: std::sync::Weak<Self>,
    bound: Bound<P, O>,
    platform: Arc<dyn Platform>,
    root: AgentPath,
    resource: String,
}

/// Creates (or, on the same `intent`, exactly reopens) `root` under `bound`
/// and returns its server. The root and its fresh volumes are one commit.
///
/// # Errors
/// When `bound`'s admission does not cover `root`, the store fails, or the
/// creation is uncertain ([`Error::Unknown`]; serving again with the same
/// `intent` resubmits it exactly).
pub async fn serve<P, O>(
    bound: Bound<P, O>,
    platform: Arc<dyn Platform>,
    intent: &IntentId,
    root: RootRequest,
) -> Result<Arc<Server<P, O>>, Error>
where
    P: AuthorityProvider + 'static,
    O: AsyncObjectStore + 'static,
{
    let root = bound.create_root(intent, root).await?;
    let resource = format!(
        "actors:/{}",
        acyclic_auth::encode_component(bound.within().namespace().as_str().as_bytes())
    );
    Ok(Arc::new_cyclic(|this| Server {
        this: this.clone(),
        bound,
        platform,
        root,
        resource,
    }))
}

impl<P, O> Server<P, O>
where
    P: AuthorityProvider + 'static,
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
                let text = match body {
                    crate::model::ModelContent::Text(text) => text,
                    _ => return Err(Error::Unsupported("prompt content other than text")),
                };
                let body = self.bound.stage_text(agent, "message", id, text).await?;
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
                let after = page
                    .cursor()
                    .map(StreamPath::new)
                    .transpose()
                    .map_err(Error::Stream)?;
                let listed = match agents
                    .provider()
                    .children_page(ChildrenPageRequest {
                        tail_stream: None,
                        parent: Some(agent.children()?),
                        after,
                        hierarchy_version: None,
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
                    next: listed.next_after.map(|path| path.as_str().to_owned()),
                })
            }
            AgentCommand::Spawn { .. } => Err(Error::Unsupported("client spawn")),
            AgentCommand::Approvals { .. } | AgentCommand::Decide { .. } => {
                Err(Error::Unsupported("served approvals"))
            }
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

impl<P, O> CallProvider for Server<P, O>
where
    P: AuthorityProvider + 'static,
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
