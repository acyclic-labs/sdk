use super::*;

/// Capability-scoped context for one agent turn's tools and model content.
#[derive(Clone)]
pub struct TaskContext {
    pub(crate) harness: Arc<AgentHarness>,
    pub(super) scope: RuntimeScope,
    pub(super) output_content: ContentBindings,
    pub(super) journal_output: Option<ContentBindings>,
    /// Host custody for retained effects; never selected by guest publication.
    pub(super) journal_publisher: Option<Arc<dyn ContentPublisher>>,
    pub(super) journal: Option<(Arc<dyn crate::executor::ExecutionJournal>, OperationId)>,
    /// The agent whose turn this is, with the volumes it binds.
    pub(super) agent: Option<(crate::agent::AgentPath, Vec<crate::agent::WorkspaceEntry>)>,
    pub(super) children: Option<Arc<dyn crate::agent::TurnChildren>>,
}

impl TaskContext {
    /// Owns one child effect through the same journal and claim registry as
    /// its enclosing execution. A missing original journal grants no custody.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn claim_effect(
        &self,
        effect: OperationId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<crate::executor::ExecutionClaim<'_>> {
        let (journal, parent) = self.journal.as_ref().ok_or_else(|| {
            crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "task has no original execution journal",
            )
        })?;
        journal.acquire_effect(*parent, effect, io)?.into_owner(io)
    }

    /// Binds the host's shared name resolver for filesystem tools.
    #[must_use]
    pub fn with_agent_children(
        mut self,
        children: Option<Arc<dyn crate::agent::TurnChildren>>,
    ) -> Self {
        self.children = children;
        self
    }

    /// The original host observer and name resolver for this turn.
    pub fn agent_children(&self) -> Option<&dyn crate::agent::TurnChildren> {
        self.children.as_deref()
    }

    /// Retains the finalized journal and its read-only public tool-output route.
    /// Ordinary content keeps its publisher; only `tools/` is delegated here.
    /// Retained lifecycle effects keep the journal publisher in host custody.
    pub fn with_execution_journal(
        mut self,
        journal: Arc<dyn crate::executor::ExecutionJournal>,
        operation: OperationId,
    ) -> Result<Self> {
        let previous = self
            .journal_output
            .as_ref()
            .map(|content| content.volume.directory_read_capability("tools", &mut None))
            .transpose()?;
        let mut output = journal.output_content(operation)?;
        let mut publisher = None;
        let grant = output
            .as_mut()
            .map(|content| {
                publisher = content.writer.take();
                content.volume.directory_read_capability("tools", &mut None)
            })
            .transpose()?;
        self.scope.grants = Capabilities::new(
            self.scope
                .grants
                .iter()
                .filter(|grant| previous.as_deref() != Some(*grant))
                .chain(grant.as_deref()),
        );
        self.journal_output = output;
        self.journal_publisher = publisher;
        self.journal = Some((journal, operation));
        Ok(self)
    }

    /// Protected host publication, independent of workspace and tool grants.
    /// A missing journal never selects the task's private publisher instead.
    pub(crate) fn retained_content(&self) -> Result<(&ContentBindings, &dyn ContentPublisher)> {
        let content = self
            .journal_output
            .as_ref()
            .ok_or_else(|| Error::Unsupported("retained effects require a bound journal".into()))?;
        let publisher = self.journal_publisher.as_deref().ok_or_else(|| {
            Error::Unsupported("retained effects require a journal publisher".into())
        })?;
        if !content.volume.is_agent_journal() || publisher.volume() != &content.volume {
            return Err(Error::Invalid(
                "retained effects require the agent's journal".into(),
            ));
        }
        Ok((content, publisher))
    }

    /// Follows this turn's original journal stop signal.
    /// Only reversible preparation may be abandoned when this resolves;
    /// admitted effects retain their original publication owner until settlement.
    pub fn model_cancellation(
        &self,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<acyclic_stream::BoxProviderFuture<'_, Result<()>>> {
        Ok(self
            .journal
            .as_ref()
            .ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "task has no original execution journal",
                )
            })?
            .0
            .model_cancellation())
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
            .0
            .tool_settlement(query, io)
    }

    /// Returns the exact effective authority.
    #[must_use]
    pub const fn scope(&self) -> &RuntimeScope {
        &self.scope
    }

    /// This context as agent `path`'s turn over `volumes`, so a host's tools
    /// can name the agent's own children.
    #[must_use]
    pub fn for_agent(
        mut self,
        path: crate::agent::AgentPath,
        volumes: Vec<crate::agent::WorkspaceEntry>,
    ) -> Self {
        self.agent = Some((path, volumes));
        self
    }

    /// The agent whose turn this is and the volumes it binds, if any.
    #[must_use]
    pub fn agent(&self) -> Option<(&crate::agent::AgentPath, &[crate::agent::WorkspaceEntry])> {
        self.agent
            .as_ref()
            .map(|(path, volumes)| (path, volumes.as_slice()))
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
