//! Original execution observations through the conversation's existing session.
use super::*;

impl Conversation {
    /// Reads a bounded chunk of the immutable approval request named by this
    /// exact original status. This never substitutes today's pending request.
    pub async fn approval_request(&self, status: u64, offset: u64) -> Result<(Vec<u8>, u64)> {
        Self::read_at(
            &self.connection,
            self.agent(),
            ReadTarget::Approval { status },
            offset,
        )
        .await
    }

    /// Reads one bounded chunk of a payload selected by its original journal
    /// observation. The shared command retains original input ownership; an
    /// immutable file reference supplied by a caller is never sufficient.
    pub async fn execution_payload(
        &self,
        input: u64,
        sequence: u64,
        field: crate::executor::ExecutionPayloadField,
        offset: u64,
    ) -> Result<(Vec<u8>, u64)> {
        Self::read_at(
            &self.connection,
            self.agent(),
            ReadTarget::Execution {
                input,
                sequence,
                field,
            },
            offset,
        )
        .await
    }

    /// Query a bounded original execution page. A quiet live request parks on
    /// the original journal and actor events; the source's closure is an error.
    ///
    /// # Errors
    /// Original source, credential, policy or cursor refusal.
    pub async fn execution_page(
        &self,
        input: u64,
        after: u64,
        wait: bool,
    ) -> Result<ExecutionQuery> {
        self.connection
            .execute(&AgentCommand::Events {
                agent: self.agent().clone(),
                input,
                after,
                wait,
            })
            .await?
            .try_into()
            .map_err(|_| unexpected())
    }

    /// Selected identity, shared by follow and execution-query projections.
    #[must_use]
    pub fn path(&self) -> &AgentPath {
        self.agent()
    }
}
