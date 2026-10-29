use super::*;

/// Capability-scoped context for one agent turn's tools and model content.
#[derive(Clone)]
pub struct TaskContext {
    pub(crate) harness: Arc<AgentHarness>,
    pub(super) scope: RuntimeScope,
    pub(super) output_content: ContentBindings,
    pub(super) journal: Option<Arc<dyn crate::executor::ExecutionJournal>>,
}

impl TaskContext {
    /// Retains the same finalized journal supplied to this turn's executor.
    #[must_use]
    pub fn with_execution_journal(
        mut self,
        journal: Arc<dyn crate::executor::ExecutionJournal>,
    ) -> Self {
        self.journal = Some(journal);
        self
    }

    /// Authenticates an original model call's committed terminal observation.
    /// Missing bindings, unreadable records and foreign identities refuse;
    /// an absent terminal record remains unresolved.
    pub fn tool_settlement<'a>(
        &'a self,
        query: crate::executor::ToolSettlementQuery<'a>,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<'a, Option<crate::executor::ToolSettlement>>,
    > {
        self.journal
            .as_ref()
            .ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "task has no original execution journal",
                )
            })?
            .tool_settlement(query, io)
    }

    /// Returns the exact effective authority.
    #[must_use]
    pub const fn scope(&self) -> &RuntimeScope {
        &self.scope
    }

    /// Narrows this context's grants and limits for one bound execution.
    pub(crate) fn scoped(&self, grants: Capabilities, limits: Limits) -> Result<Self> {
        Ok(Self {
            scope: self.scope.narrow(grants, limits)?,
            ..self.clone()
        })
    }
}

#[doc(hidden)]
pub fn read_granted(grants: &Capabilities, file: &FileRef) -> Result<bool> {
    read_granted_with_work(grants, file, &mut None)
}

pub(crate) fn read_granted_with_work(
    grants: &Capabilities,
    file: &FileRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
    }
    file.validate(work)?;
    Ok(crate::core::file_read_grant(grants, file, work)?.is_some())
}
