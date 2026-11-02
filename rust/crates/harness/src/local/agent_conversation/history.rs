//! Bounded history projection on the conversation's authenticated connection.
use super::{Conversation, unexpected};
use crate::{Result, agent::HistorySummary};
use acyclic_harness_client::{AgentCommand, AgentPageRequest, AgentReply};

/// One immutable interval of settled history, newest conversation cut first.
pub struct HistoryPage {
    /// Summaries validated by the shared client reply boundary.
    pub items: Vec<HistorySummary>,
    /// Return unchanged to read older summaries under the same opening cut.
    pub next: Option<String>,
}

impl Conversation {
    /// Read one page through the bound agent's original history query.
    /// An initial page seeks the current tail; a continuation freezes that cut.
    /// No full history or project descriptor is loaded by this client.
    ///
    /// # Errors
    /// Original session refusal or malformed shared reply.
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
}
