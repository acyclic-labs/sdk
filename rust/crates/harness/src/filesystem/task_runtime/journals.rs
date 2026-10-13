use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Opens the original owner-fenced journal and returns its exact task/turn namespace.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn execution_journal_with_native_work(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        turn: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Arc<FilesystemExecutionJournal<P, A, O>>, OperationId)> {
        let io = super::super::PreparationIo::from_native_work(work)?;
        let admission = self.admission_observed(task, Some(&io)).await?;
        let operation = execution_operation(task, turn, Some(&io))?;
        let owner = self
            .host
            .journal_owner(
                task,
                fence,
                Some(&crate::executor::PreparationIo::from_native_work(work)?),
            )
            .await?;
        let journal = FilesystemExecutionJournal::for_task(
            owner,
            operation,
            turn,
            Arc::clone(&self.filesystem),
            &self.volume,
            &self.verifier,
            &self.signed,
            self.maximum_payload_bytes.min(admission.limits.file_bytes),
            Some(&io),
        )?;
        let journal = if admission
            .grants
            .contains(crate::contract::capability::INTERACTION_ROUTE)
        {
            if let Some(owner) = &self.interaction_owner {
                journal.with_interaction_owner(
                    &owner.verifier,
                    &owner.schemas,
                    &owner.scope,
                    &owner.volume,
                    Some(&io),
                )?
            } else {
                journal
            }
        } else {
            journal
        };
        Ok((journal.into_native_shared(work)?, operation))
    }

    /// Opens the protected original readonly journal for native witnesses.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn readonly_execution_journal_with_native_work(
        &self,
        task: TaskId,
        turn: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Arc<FilesystemExecutionJournal<P, A, O>>, OperationId)> {
        let io = super::super::PreparationIo::from_native_work(work)?;
        let admission = self.admission_observed(task, Some(&io)).await?;
        let operation = execution_operation(task, turn, Some(&io))?;
        let owner = self.host.readonly_journal_owner(task, work).await?;
        let journal = FilesystemExecutionJournal::for_task(
            owner,
            operation,
            turn,
            Arc::clone(&self.filesystem),
            &self.volume,
            &self.verifier,
            &self.signed,
            self.maximum_payload_bytes.min(admission.limits.file_bytes),
            Some(&io),
        )?;
        Ok((journal.into_native_shared(work)?, operation))
    }

    pub(super) async fn open_execution_journal(
        &self,
        task: TaskId,
        fence: LeaseFence,
        turn: OperationId,
        admission: &TaskAdmissionRecord,
    ) -> Result<FilesystemExecutionJournal<P, A, O>> {
        let journal = FilesystemExecutionJournal::for_task(
            self.host.journal_owner(task, &(fence), None).await?,
            execution_operation(task, turn, None)?,
            turn,
            self.filesystem.clone(),
            &self.volume,
            &self.verifier,
            &self.signed,
            self.maximum_payload_bytes.min(admission.limits.file_bytes),
            None,
        )?;
        if admission
            .grants
            .contains(crate::contract::capability::INTERACTION_ROUTE)
            && let Some(owner) = &self.interaction_owner
        {
            return journal.with_interaction_owner(
                &owner.verifier,
                &owner.schemas,
                &owner.scope,
                &owner.volume,
                None,
            );
        }
        Ok(journal)
    }

    /// Opens the canonical tool-effect workflow under the original caller reservation.
    pub async fn workflow_journal_for_tool(
        &self,
        context: &crate::runtime::ToolContext,
        tool: &crate::durable_tool::ResumableTool,
        invocation: &crate::tool::ToolInvocation,
        cancellation: &acyclic_fs::CancellationToken,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<Arc<FilesystemWorkflowJournal<P, A, O>>> {
        let task = context
            .task()
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("tool has no admitted caller".into()))?;
        #[cfg(not(target_arch = "wasm32"))]
        let owner = match io.and_then(super::super::PreparationIo::original_native_work) {
            Some(work) => {
                let (_, fence) = context.task().owned_task_binding(Some(
                    &crate::executor::PreparationIo::from_native_work(work)?,
                ))?;
                self.host
                    .journal_owner(
                        task,
                        fence,
                        Some(&crate::executor::PreparationIo::from_native_work(work)?),
                    )
                    .await?
            }
            None => {
                self.host
                    .journal_owner(task, context.task().owned_task_binding(None)?.1, None)
                    .await?
            }
        };
        #[cfg(target_arch = "wasm32")]
        let owner = self
            .host
            .journal_owner(task, context.task().owned_task_binding(None)?.1, None)
            .await?;
        let owner = owner
            .for_tool_workflow(context, tool, invocation, cancellation, io)
            .await?;
        let maximum_payload_bytes = self
            .maximum_payload_bytes
            .min(owner.input_limits().file_bytes);
        FilesystemWorkflowJournal::for_task(
            owner,
            self.filesystem.clone(),
            &self.volume,
            &self.verifier,
            &self.signed,
            maximum_payload_bytes,
            io,
        )
    }

    /// Actual caller/source route for portable recursive tool adapters.
    pub async fn recursive_tool_owner(
        self: &Arc<Self>,
        context: &crate::runtime::ToolContext,
        tool: &crate::durable_tool::ResumableTool,
        invocation: &crate::tool::ToolInvocation,
        cancellation: &acyclic_fs::CancellationToken,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<(Arc<Self>, Arc<FilesystemWorkflowJournal<P, A, O>>)> {
        let workflow = self
            .workflow_journal_for_tool(context, tool, invocation, cancellation, io)
            .await?;
        workflow.owner.as_ref().ok_or_else(|| {
            Error::Unauthorized("recursive tool lacks its original task owner".into())
        })?;
        Ok((self.clone(), workflow))
    }

    /// Opens a workflow journal under the exact retained lease and admission.
    pub async fn workflow_journal(
        &self,
        task: TaskId,
        fence: LeaseFence,
    ) -> Result<Arc<FilesystemWorkflowJournal<P, A, O>>> {
        let admission = self.host.observe_admission(task).await?;
        FilesystemWorkflowJournal::for_task(
            self.host.journal_owner(task, &(fence), None).await?,
            self.filesystem.clone(),
            &self.volume,
            &self.verifier,
            &self.signed,
            self.maximum_payload_bytes.min(admission.limits.file_bytes),
            None,
        )
    }

    /// Constructs the same owner-fenced journal with intrinsic native metadata custody.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn workflow_journal_with_native_work(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<FilesystemWorkflowJournal<P, A, O>>> {
        let io = super::super::PreparationIo::from_native_work(work)?;
        let admission = self.admission_observed(task, Some(&io)).await?;
        let owner = self
            .host
            .journal_owner(
                task,
                fence,
                Some(&crate::executor::PreparationIo::from_native_work(work)?),
            )
            .await?;
        FilesystemWorkflowJournal::for_task(
            owner,
            Arc::clone(&self.filesystem),
            &self.volume,
            &self.verifier,
            &self.signed,
            self.maximum_payload_bytes.min(admission.limits.file_bytes),
            Some(&io),
        )
    }

    /// Observes the original workflow under its protected readonly owner, including terminal records, without claiming a new lease.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn readonly_workflow_journal_with_native_work(
        &self,
        task: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<FilesystemWorkflowJournal<P, A, O>>> {
        let io = super::super::PreparationIo::from_native_work(work)?;
        let admission = self.admission_observed(task, Some(&io)).await?;
        self.readonly_workflow_journal_observed(task, &admission, work)
            .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn readonly_workflow_journal_observed(
        &self,
        task: TaskId,
        admission: &TaskAdmissionRecord,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<FilesystemWorkflowJournal<P, A, O>>> {
        let owner = self.host.readonly_journal_owner(task, work).await?;
        FilesystemWorkflowJournal::for_task(
            owner,
            Arc::clone(&self.filesystem),
            &self.volume,
            &self.verifier,
            &self.signed,
            self.maximum_payload_bytes.min(admission.limits.file_bytes),
            Some(&super::super::PreparationIo::from_native_work(work)?),
        )
    }
}
