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
        self.execution_journal_with_native_context(task, fence, turn, work, None)
            .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) async fn execution_journal_with_native_context(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        turn: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
        context: Option<Arc<crate::runtime::TaskContext>>,
    ) -> Result<(Arc<FilesystemExecutionJournal<P, A, O>>, OperationId)> {
        let io = super::super::PreparationIo::from_native_work(work)?;
        let operation = execution_operation(task, turn, Some(&io))?;
        let owner = self
            .host
            .journal_owner(
                task,
                fence,
                Some(&crate::executor::PreparationIo::from_native_work(work)?),
            )
            .await?;
        let admission = owner.admission();
        let maximum_payload_bytes = self.maximum_payload_bytes.min(admission.limits.file_bytes);
        let interaction = admission
            .grants
            .contains(crate::contract::capability::INTERACTION_ROUTE);
        let journal = FilesystemExecutionJournal::for_task(
            owner,
            operation,
            turn,
            Arc::clone(&self.filesystem),
            crate::filesystem::FilesystemJournalBinding {
                volume: &self.volume,
                verifier: &self.verifier,
                scope: &self.signed,
                maximum_bytes: maximum_payload_bytes,
                io: Some(&io),
            },
        )?;
        let journal = match context {
            Some(context) => {
                if !context.original_task_binding_matches(task, fence)
                    || !context.native_metadata_matches(work)
                {
                    return Err(observed::boundary(
                        Some(&io),
                        crate::wire::ErrorCode::Unauthorized,
                        "journal reader differs from its original task context",
                    ));
                }
                journal.with_input_verifier(context)
            }
            None => journal,
        };
        let journal = journal.with_task_files(self.task_files.clone());
        let journal = if interaction {
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
        let operation = execution_operation(task, turn, Some(&io))?;
        let owner = self.host.readonly_journal_owner(task, work).await?;
        let maximum_payload_bytes = self
            .maximum_payload_bytes
            .min(owner.input_limits().file_bytes);
        let journal = FilesystemExecutionJournal::for_task(
            owner,
            operation,
            turn,
            Arc::clone(&self.filesystem),
            crate::filesystem::FilesystemJournalBinding {
                volume: &self.volume,
                verifier: &self.verifier,
                scope: &self.signed,
                maximum_bytes: maximum_payload_bytes,
                io: Some(&io),
            },
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
            crate::filesystem::FilesystemJournalBinding {
                volume: &self.volume,
                verifier: &self.verifier,
                scope: &self.signed,
                maximum_bytes: self.maximum_payload_bytes.min(admission.limits.file_bytes),
                io: None,
            },
        )?;
        let journal = journal.with_task_files(self.task_files.clone());
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
        #[cfg(not(target_arch = "wasm32"))]
        let owner = match io.and_then(super::super::PreparationIo::original_native_work) {
            Some(work) => owner.into_native_shared(work)?,
            None => Arc::new(owner),
        };
        #[cfg(target_arch = "wasm32")]
        let owner = Arc::new(owner);
        FilesystemWorkflowJournal::for_task(
            owner,
            self.filesystem.clone(),
            crate::filesystem::FilesystemJournalBinding {
                volume: &self.volume,
                verifier: &self.verifier,
                scope: &self.signed,
                maximum_bytes: maximum_payload_bytes,
                io,
            },
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
        let owner = self.host.journal_owner(task, &fence, None).await?;
        let maximum = self
            .maximum_payload_bytes
            .min(owner.input_limits().file_bytes);
        FilesystemWorkflowJournal::for_task(
            Arc::new(owner),
            self.filesystem.clone(),
            crate::filesystem::FilesystemJournalBinding {
                volume: &self.volume,
                verifier: &self.verifier,
                scope: &self.signed,
                maximum_bytes: maximum,
                io: None,
            },
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
        let owner = self
            .host
            .journal_owner(
                task,
                fence,
                Some(&crate::executor::PreparationIo::from_native_work(work)?),
            )
            .await?;
        let maximum = self
            .maximum_payload_bytes
            .min(owner.input_limits().file_bytes);
        FilesystemWorkflowJournal::for_task(
            owner.into_native_shared(work)?,
            Arc::clone(&self.filesystem),
            crate::filesystem::FilesystemJournalBinding {
                volume: &self.volume,
                verifier: &self.verifier,
                scope: &self.signed,
                maximum_bytes: maximum,
                io: Some(&io),
            },
        )
    }

    /// Observes the original workflow under its protected readonly owner, including terminal records, without claiming a new lease.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn readonly_workflow_journal_with_native_work(
        &self,
        task: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<FilesystemWorkflowJournal<P, A, O>>> {
        let owner = self.host.readonly_journal_owner(task, work).await?;
        let maximum = self
            .maximum_payload_bytes
            .min(owner.input_limits().file_bytes);
        FilesystemWorkflowJournal::for_task(
            owner.into_native_shared(work)?,
            Arc::clone(&self.filesystem),
            crate::filesystem::FilesystemJournalBinding {
                volume: &self.volume,
                verifier: &self.verifier,
                scope: &self.signed,
                maximum_bytes: maximum,
                io: Some(&super::super::PreparationIo::from_native_work(work)?),
            },
        )
    }
}
