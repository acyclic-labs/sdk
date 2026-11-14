//! Source events use the original agent journal, grants and approval owner.
use super::*;

pub(in crate::filesystem) struct HostInvocation {
    pub(in crate::filesystem) executor: Arc<dyn Executor>,
    // Keep signing custody with the journal and original task it issued.
    pub(in crate::filesystem) authority: TurnAuthority,
    // The event actor parks on this original owner, just like an ordinary turn.
    pub(in crate::filesystem) approvals: Arc<AgentApprovals>,
}

impl<P, A, O> FilesystemTurnHost<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Admit a host-selected call without creating a conversation or prompt.
    /// The event actor supplies the stable original operation identity; the
    /// ordinary journal owns its selections, approvals, effects and receipts.
    pub(in crate::filesystem) async fn invocation(
        &self,
        agent: &AgentState,
        operation: OperationId,
        stop: acyclic_actors::guest::CancellationToken,
    ) -> Result<HostInvocation> {
        let (journal, bindings, files) = self
            .journal(
                self.admit(agent)?,
                agent.policy.as_slice(),
                &[],
                None,
                Some(stop.clone()),
            )
            .await?;
        let (policy, resolved) = self
            .approvals(agent, bindings.journal.reader.as_ref())
            .await?;
        let run_admission = self.run_admission(agent, &bindings).await?;
        let approvals = Arc::new(AgentApprovals::new(files, Arc::new(policy)));
        let journal: Arc<dyn crate::executor::ExecutionJournal> = Arc::new(
            journal
                .with_turn(operation, operation)
                .with_run_admission(run_admission)
                .with_actor_admissions(self.actor_admissions(agent)?)
                .with_agent_lineage(agent.path.clone())
                .with_stop(stop)
                .with_agent_approvals(approvals.clone()),
        );
        let mut authority = TurnAuthority {
            templates: self.templates(),
            issuer: bindings.issuer,
            scope: bindings.scope,
            grants: bindings.grants,
            content: bindings.content,
            approval: Arc::new(resolved.bind()?),
            effects: None,
            journal,
            children: None,
            workspace_tools: None,
        };
        if let Some(factory) = &self.workspace_tools {
            authority.workspace_tools = Some(factory(agent, &authority)?);
        }
        let executor = self
            .executors
            .invocation_executor(agent, operation, &authority)
            .await?;
        Ok(HostInvocation {
            executor,
            authority,
            approvals,
        })
    }
}
