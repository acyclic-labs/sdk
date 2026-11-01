//! Original history queries and prepared actions on the bound agent connection.
use super::{Conversation, failed, intent, unexpected};
use crate::{
    Result,
    agent::{HistorySummary, HistoryVolume, RestoreMode},
    model::ModelContent,
};
use acyclic_harness_client::{
    AgentCommand, AgentExchangeError, AgentPageRequest, AgentReply, PreparedAgentInvocation,
    PreparedCommand,
};
use std::sync::Arc;

/// An exact prepared action and the original authenticated connection that
/// prepared it. No transport starts until [`Self::execute`]; every attempt
/// borrows the same canonical command frames and the same credential slot.
pub struct PreparedHistoryAction {
    conversation: Arc<Conversation>,
    invocation: PreparedAgentInvocation<'static>,
    failure: Option<AgentExchangeError>,
}

impl PreparedHistoryAction {
    /// Execute once, retaining the request on every outcome. A decoded refusal
    /// is definitive; a session failure is not evidence of mutation settlement.
    /// The last failure remains typed here until another attempt completes;
    /// Unknown retains the host's original recovery request, not just its text.
    ///
    /// # Errors
    /// A borrowed original session, framing or host failure. No retry is started.
    pub async fn execute(&mut self) -> std::result::Result<AgentReply, &AgentExchangeError> {
        match self
            .conversation
            .connection
            .controls
            .execute(&self.invocation)
            .await
        {
            Ok(reply) => {
                self.failure = None;
                Ok(reply)
            }
            Err(error) => Err(self.failure.insert(error)),
        }
    }
}

/// One immutable interval of settled history, newest conversation cut first.
pub struct HistoryPage {
    /// Bounded source summaries validated by the shared client reply boundary.
    pub items: Vec<HistorySummary>,
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

    /// Prepare restoration of an exact zero-based settled turn. `expected`
    /// contains current project heads, captured before confirmation, rather
    /// than the saved target heads. Preparation sends nothing.
    ///
    /// # Errors
    /// Original command validation or preparation failure, before transport.
    pub fn prepare_restore(
        self: &Arc<Self>,
        id: &str,
        at: u64,
        mode: RestoreMode,
        expected: Vec<HistoryVolume>,
    ) -> Result<PreparedHistoryAction> {
        self.prepare_history(&AgentCommand::Restore {
            agent: self.agent().clone(),
            id: intent(id)?,
            at,
            mode,
            expected,
        })
    }

    /// Prepare the original fork publication with the complete owned task
    /// brief. Retries borrow the prepared bytes without copying the brief.
    ///
    /// # Errors
    /// Original command validation or preparation failure, before transport.
    pub fn prepare_fork(
        self: &Arc<Self>,
        id: &str,
        at: u64,
        brief: String,
    ) -> Result<PreparedHistoryAction> {
        self.prepare_history(&AgentCommand::Fork {
            parent: self.agent().clone(),
            id: intent(id)?,
            at,
            brief: ModelContent::Text(brief),
        })
    }

    fn prepare_history(self: &Arc<Self>, command: &AgentCommand) -> Result<PreparedHistoryAction> {
        let invocation = self
            .connection
            .controls
            .prepare(PreparedCommand::new(command).map_err(failed)?)
            .map_err(failed)?;
        Ok(PreparedHistoryAction {
            conversation: Arc::clone(self),
            invocation,
            failure: None,
        })
    }
}
