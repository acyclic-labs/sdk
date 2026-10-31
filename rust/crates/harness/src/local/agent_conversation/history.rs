//! Original history queries and fork commands on the bound agent connection.
use super::{Conversation, intent, unexpected};
use crate::{
    Error, Result,
    agent::{AgentPath, Delivered, HistoryCheckpoint},
    model::ModelContent,
};
use acyclic_harness_client::{AgentCommand, AgentPageRequest, AgentReply};

/// One immutable interval of settled history, in original conversation order.
pub struct HistoryPage {
    /// Original descriptors validated by the shared client reply boundary.
    pub items: Vec<HistoryCheckpoint>,
    /// Opaque continuation returned unchanged to the original query.
    pub next: Option<String>,
}

impl Conversation {
    /// Read one bounded page through the original authenticated history query.
    /// The host owns its fixed source cut and opaque continuation. No local
    /// filesystem or frontend-specific reader bypasses that source authority.
    ///
    /// # Errors
    /// Original session refusal, invalid page or malformed shared reply.
    pub async fn history_page(&self, page: AgentPageRequest) -> Result<HistoryPage> {
        match self
            .connection
            .execute(&AgentCommand::History {
                agent: self.agent().clone(),
                page,
            })
            .await?
        {
            AgentReply::History { items, next } => Ok(HistoryPage { items, next }),
            _ => Err(unexpected()),
        }
    }

    /// Fork an exact zero-based settled turn with a new task brief. The host's
    /// original fork publication retains the saved files and context together.
    /// Retrying uses the same `id`, turn and brief, including after a lost reply.
    ///
    /// # Errors
    /// Original session failure, decoded refusal or malformed reply.
    pub async fn fork_at(&self, id: &str, at: u64, brief: &str) -> Result<AgentPath> {
        match self
            .connection
            .execute(&AgentCommand::Fork {
                parent: self.agent().clone(),
                id: intent(id)?,
                at,
                brief: ModelContent::Text(brief.to_owned()),
            })
            .await?
        {
            AgentReply::Spawned {
                agent,
                delivery: Delivered::Committed { .. },
            } => Ok(agent),
            AgentReply::Spawned {
                delivery: Delivered::Closed,
                ..
            } => Err(Error::Conflict("conversation is archived".into())),
            _ => Err(unexpected()),
        }
    }
}
