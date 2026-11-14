//! Source events use the original agent journal, grants and approval owner.
use super::*;

pub(in crate::filesystem) struct HostInvocation {
    pub(in crate::filesystem) executor: Arc<dyn Executor>,
    // Keep signing custody with the journal and original task it issued.
    pub(in crate::filesystem) authority: TurnAuthority,
    // The event actor parks on this original owner, just like an ordinary turn.
    pub(in crate::filesystem) approvals: Arc<AgentApprovals>,
}

pub(in crate::filesystem) struct ProgramCatalogue {
    pub(in crate::filesystem) programs: Arc<crate::plugins::ProgramFiles>,
    pub(in crate::filesystem) authority: TurnAuthority,
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
        let (mut authority, approvals) = self.invocation_authority(agent, operation, stop).await?;
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

    /// Discover original names and selectors without opening a native target.
    /// The same issued journal retains the selected source generations; only
    /// an executing event enters `invocation` and binds its native workspace.
    pub(in crate::filesystem) async fn program_catalogue(
        &self,
        agent: &AgentState,
        operation: OperationId,
        stop: acyclic_actors::guest::CancellationToken,
    ) -> Result<Option<ProgramCatalogue>> {
        let (authority, _) = self.invocation_authority(agent, operation, stop).await?;
        let Some(programs) = self.executors.program_files(agent, &authority).await? else {
            return Ok(None);
        };
        Ok(Some(ProgramCatalogue {
            programs,
            authority,
        }))
    }

    /// Source hooks use the existing authenticated observation session. A
    /// subscription selector cannot enroll another agent's streams, invoke an
    /// actor, or obtain host storage custody. The session rechecks its original
    /// credential and current trust before returning source records.
    pub(in crate::filesystem) async fn source_session(
        &self,
        agent: &AgentState,
    ) -> Result<acyclic_stream::session::Client> {
        let (journal, authority) = self.journal_authority(agent).await?;
        let observation = self
            .host
            .agent_observation(
                &authority,
                &agent.path,
                &journal,
                Arc::new(super::super::hosting::ObservationClock(self.clock.clone())),
                None,
            )
            .await?;
        let client = acyclic_stream::session::Client::in_process(
            self.stream.clone(),
            observation.authority,
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
        );
        client.register_credential(1, observation.credential.into())?;
        Ok(client.select_credential(1))
    }

    async fn invocation_authority(
        &self,
        agent: &AgentState,
        operation: OperationId,
        stop: acyclic_actors::guest::CancellationToken,
    ) -> Result<(TurnAuthority, Arc<AgentApprovals>)> {
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
        let authority = TurnAuthority {
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
        Ok((authority, approvals))
    }
}
