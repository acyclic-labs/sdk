//! Session files use the conversation's existing authenticated connection.
use super::*;
use crate::{
    OperationId,
    client_files::{ClientFileChange, ClientFiles},
    resources::GenerationRef,
};
use futures::future::LocalBoxFuture;
use std::{
    collections::{BTreeMap, BTreeSet},
    task::{Context, Poll},
};

type Snapshot = LocalBoxFuture<'static, Result<(Arc<Conversation>, ClientFiles)>>;

struct VisibleFileQuery {
    snapshot: Option<Result<ClientFiles>>,
    pending: Option<Snapshot>,
}

/// Publication-driven file queries for exactly the agents a view displays.
/// Dropping a row cancels its pending request and releases its snapshot. There
/// are no per-agent tasks, timers, tree walks or retained offscreen agents.
pub struct VisibleFiles {
    root: Arc<Conversation>,
    paths: Vec<String>,
    rows: BTreeMap<AgentPath, VisibleFileQuery>,
}

impl VisibleFiles {
    /// Select explicit private paths. Membership comes from the caller's one
    /// shared Explorer; this query never discovers children independently.
    #[must_use]
    pub fn new(root: Arc<Conversation>, paths: Vec<String>) -> Self {
        Self {
            root,
            paths,
            rows: BTreeMap::new(),
        }
    }

    /// Reconcile the visible set. An unchanged row keeps its original request,
    /// generation and source failure across redraws.
    pub fn retain(&mut self, agents: impl IntoIterator<Item = AgentPath>) {
        let visible: BTreeSet<_> = agents.into_iter().collect();
        self.rows.retain(|agent, _| visible.contains(agent));
        for agent in visible {
            self.rows.entry(agent.clone()).or_insert_with(|| {
                let root = Arc::clone(&self.root);
                let paths = self.paths.clone();
                VisibleFileQuery {
                    snapshot: None,
                    pending: Some(Box::pin(async move {
                        let conversation = root.select(agent).await?;
                        let snapshot = conversation.private_files(paths, None).await?;
                        Ok((conversation, snapshot))
                    })),
                }
            });
        }
    }

    /// Cached immutable cut, absent while the first request is outstanding.
    /// Rendering borrows this value and never performs IO.
    #[must_use]
    pub fn snapshot(
        &self,
        agent: &AgentPath,
    ) -> Option<std::result::Result<&ClientFiles, &crate::Error>> {
        self.rows
            .get(agent)?
            .snapshot
            .as_ref()
            .map(std::result::Result::as_ref)
    }

    /// Register the source wakes and report completed publications. All ready
    /// rows advance once, so one busy agent cannot starve other visible rows.
    pub fn poll_changed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let mut changed = false;
        for query in self.rows.values_mut() {
            let Some(pending) = query.pending.as_mut() else {
                continue;
            };
            let Poll::Ready(result) = pending.as_mut().poll(cx) else {
                continue;
            };
            query.pending = None;
            query.snapshot = Some(match result {
                Ok((conversation, snapshot)) => {
                    let paths = self.paths.clone();
                    let after = snapshot.generation.clone();
                    query.pending = Some(Box::pin(async move {
                        let snapshot = conversation.private_files(paths, Some(after)).await?;
                        Ok((conversation, snapshot))
                    }));
                    Ok(snapshot)
                }
                Err(error) => Err(error),
            });
            changed = true;
        }
        if changed {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Conversation {
    /// The default client session of this selected conversation. Callers that
    /// need independent UI sessions supply their own stable session identity.
    #[must_use]
    pub fn client_session(&self) -> OperationId {
        OperationId::from_bytes(self.agent().id_bytes())
    }

    /// Snapshot explicit session paths, optionally waiting for the original
    /// filesystem publication event after a previously observed generation.
    pub async fn client_files(
        &self,
        session: OperationId,
        paths: Vec<String>,
        after: Option<GenerationRef>,
    ) -> Result<ClientFiles> {
        match self
            .connection
            .execute(&AgentCommand::ClientFiles {
                agent: self.agent().clone(),
                session,
                paths,
                after,
            })
            .await?
        {
            AgentReply::ClientFiles { snapshot } => Ok(snapshot),
            _ => Err(unexpected()),
        }
    }

    /// Snapshot explicit private paths, optionally waiting on this volume's
    /// original publication stream. The shared query owns cancellation/credit.
    pub async fn private_files(
        &self,
        paths: Vec<String>,
        after: Option<GenerationRef>,
    ) -> Result<ClientFiles> {
        match self
            .connection
            .execute(&AgentCommand::PrivateFiles {
                agent: self.agent().clone(),
                paths,
                after,
            })
            .await?
        {
            AgentReply::ClientFiles { snapshot } => Ok(snapshot),
            _ => Err(unexpected()),
        }
    }

    /// Publish an atomic conditional file batch. An uncertain outcome retains
    /// the caller's operation identity and exact original source descriptors.
    pub async fn publish_client_files(
        &self,
        session: OperationId,
        operation: OperationId,
        changes: Vec<ClientFileChange>,
    ) -> Result<GenerationRef> {
        match self
            .connection
            .execute(&AgentCommand::PublishClientFiles {
                agent: self.agent().clone(),
                session,
                operation,
                changes,
            })
            .await?
        {
            AgentReply::ClientFilesPublished { generation } => Ok(generation),
            _ => Err(unexpected()),
        }
    }
}
