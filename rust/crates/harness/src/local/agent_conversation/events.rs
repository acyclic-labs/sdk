//! Original execution observations through the conversation's existing session.
use super::*;

/// A bounded page plus the original input cut and actor settlement fence.
pub struct ExecutionQuery {
    /// Exact input, at or after this agent's adoption cut.
    pub input: u64,
    /// Admitted inbox cut, for a finite export that later inputs cannot extend.
    pub inbox_tail: u64,
    /// Original journal page, or the current inbox tail.
    pub page: Option<crate::executor::ExecutionEventPage>,
    /// Original actor has consumed this input and retains no earlier work.
    pub settled: bool,
}

impl ExecutionQuery {
    /// Exclusive position after every observation in this page was flushed.
    /// Only an original actor settlement retires a journal into the next input.
    pub fn continuation(&self) -> Result<acyclic_harness_client::ExecutionPosition> {
        use acyclic_harness_client::ExecutionPosition;
        match &self.page {
            Some(page) if self.settled && page.next.is_none() => Ok(ExecutionPosition {
                input: self.input.checked_add(1).ok_or_else(|| {
                    Error::Invalid("execution input continuation overflows".into())
                })?,
                after: 0,
            }),
            Some(page) => Ok(ExecutionPosition {
                input: self.input,
                after: page.end,
            }),
            None => Ok(ExecutionPosition {
                input: self.input,
                after: 0,
            }),
        }
    }
}

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
        match self
            .connection
            .execute(&AgentCommand::Events {
                agent: self.agent().clone(),
                input,
                after,
                wait,
            })
            .await?
        {
            AgentReply::Events {
                input,
                inbox_tail,
                page,
                settled,
            } => Ok(ExecutionQuery {
                input,
                inbox_tail,
                page,
                settled,
            }),
            _ => Err(unexpected()),
        }
    }

    /// Selected identity, shared by follow and execution-query projections.
    #[must_use]
    pub fn path(&self) -> &AgentPath {
        self.agent()
    }
}
