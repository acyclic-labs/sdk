//! Journals for the filesystem task runtime.

use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) fn bind_context_interactions(
        &self,
        context: TaskContext,
        task: TaskId,
        journal: Arc<FilesystemExecutionJournal<P, A, O>>,
    ) -> Result<TaskContext> {
        if self.interaction_owner.is_some() {
            context.with_owned_interactions(task, journal)
        } else {
            Ok(context)
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn bind_native_tool_interactions(
        &self,
        context: crate::runtime::ToolContext,
        task: TaskId,
        fence: &LeaseFence,
        turn: OperationId,
        journal: Arc<FilesystemExecutionJournal<P, A, O>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::runtime::ToolContext> {
        if !context.native_metadata_matches(work)
            || !context.task().original_task_binding_matches(task, fence)
            || !journal.original_native_execution_work_matches(work)
            || !journal.original_task_execution_binding_matches(
                task,
                fence,
                turn,
                context.operation_id(),
                self.host.as_ref(),
            )
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "journal interaction route differs from original task",
            });
        }
        if self.interaction_owner.is_some() {
            journal.supports_native_interactions(work)?;
            context.with_native_interactions(journal, work)
        } else {
            Ok(context)
        }
    }

    /// The existing typed admission, recovery and cancellation API.
    #[must_use]
    pub const fn harness(&self) -> &Arc<AgentHarness> {
        &self.harness
    }

    /// The existing authoritative task host, also used by the bound journals.
    #[must_use]
    pub const fn task_host(&self) -> &Arc<CoordinatorTaskHost<P>> {
        &self.host
    }

    /// Stock model/tool command adapters using this runtime's existing bindings.
    #[must_use]
    pub const fn commands(&self) -> crate::filesystem::FilesystemTaskCommands<'_, P, A, O> {
        crate::filesystem::FilesystemTaskCommands::new(self)
    }

    pub(super) fn tool_definition(
        &self,
        name: &str,
        revision: &str,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<&ToolDefinition> {
        self.tools
            .get_version(name, revision, io)?
            .map(|tool| &tool.definition)
            .ok_or_else(|| {
                crate::filesystem::interaction_host::interaction_boundary(
                    io,
                    crate::wire::ErrorCode::NotFound,
                    "pinned tool revision is absent",
                )
            })
    }

    pub(super) fn tool_registry(&self) -> &ToolRegistry {
        &self.tools
    }

    /// Opens owner-fenced observations on the optional native preparation context.
    pub async fn execution_journal(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        turn: OperationId,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<(Arc<FilesystemExecutionJournal<P, A, O>>, OperationId)> {
        #[cfg(not(target_arch = "wasm32"))]
        let native_io = io
            .and_then(crate::filesystem::PreparationIo::original_native_work)
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let observer = native_io.as_ref();
        #[cfg(target_arch = "wasm32")]
        let observer = {
            let _ = io;
            None
        };
        let admission = self.admission_observed(task, observer).await?;
        let operation = execution_operation(task, turn, observer)?;
        let journal = self
            .open_execution_journal(task, fence, turn, operation, &admission, observer)
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) =
            observer.and_then(crate::filesystem::PreparationIo::original_native_work)
        {
            return Ok((journal.into_native_shared(work)?, operation));
        }
        Ok((Arc::new(journal), operation))
    }

    /// Constructs the ordinary retained scope/parent context with the exact current mailbox owner token.
    pub async fn task_context(&self, task: TaskId, fence: LeaseFence) -> Result<TaskContext> {
        let owner = self.host.journal_owner(task, fence).await?;
        let context = self
            .harness
            .durable_context(task, OperationId::from_bytes(task.into_bytes()))
            .await?;
        context.with_owned_mailbox(
            task,
            Arc::new(TaskJournalMailbox {
                host: Arc::clone(&self.host),
                owner: Arc::new(owner),
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: None,
            }),
        )
    }

    /// Issues the stock runtime's genuine current-owner native task context.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn task_context_with_native_work(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<crate::runtime::TaskContext>> {
        self.task_context_for_original_owner_with_native_work(task, fence, false, work)
            .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn readonly_fork_preparation_with_native_work(
        &self,
        task: TaskId,
        query: &Arc<crate::runtime::TaskContext>,
        admitted: &crate::resource_preparation::ResourcePreparationRequest,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<Arc<crate::resource_preparation::TaskResourcePreparation>>> {
        let witness = self
            .host
            .task_preparation_readonly_witness_with_native_work(query, work)?
            .await?;
        if witness.task() != task {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "readonly fork witness belongs to another task",
            });
        }
        let original = witness
            .original_preparation_request_with_native_work(query, work)?
            .await?;
        if &original.0 != admitted {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "readonly fork request differs from its original admission",
            });
        }
        let claim = self
            .filesystem
            .fork_allocation(&witness, query, &original.0, &self.volume, work)?
            .await?;
        let Some(claim) = claim else {
            return Ok(None);
        };
        let (owner, request) = self
            .host
            .readonly_fork_journal_owner_with_native_work(&witness, query, claim, work)?
            .await?;
        let mut preparation = owner
            .resource_preparation_with_native_work(request, work)?
            .await?;
        let mut header = work.claim(
            std::mem::size_of::<crate::resource_preparation::TaskResourcePreparation>() as u64,
            u64::from(
                (std::mem::size_of::<crate::resource_preparation::TaskResourcePreparation>()
                    as u64)
                    != 0,
            ),
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        preparation
            .native_custody
            .as_mut()
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native preparation has no original metadata custody",
            })?
            .absorb(&mut header)
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: Some(header),
            })?;
        Ok(Some(Arc::new(preparation)))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn task_context_for_original_owner_with_native_work(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        allow_settlement: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<crate::runtime::TaskContext>> {
        let owner = self
            .host
            .journal_owner_with_native_work(task, fence, work)
            .await?;
        owner
            .verify(
                allow_settlement,
                &mut Some(&mut crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(work)?,
                )),
            )
            .await?;
        let owner = owner.into_native_shared(work)?;
        let mailbox: Arc<dyn OwnedTaskMailbox> = {
            if !owner.original_host_binding_matches(&self.host) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native mailbox owner belongs to another original host",
                });
            }
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let bytes = std::mem::size_of::<TaskJournalMailbox<P>>() as u64;
            io.retain_owned(bytes)?;
            io.allocation_attempt(bytes)?;
            let custody = io.take_native_custody(io.owned_bytes()?)?;
            Arc::new(TaskJournalMailbox {
                host: Arc::clone(&self.host),
                owner,
                native_custody: Some(custody),
            })
        };
        let operation = OperationId::from_bytes(task.into_bytes());
        let purpose = if allow_settlement {
            crate::runtime::NativeContextPurpose::Reconciliation { fence, mailbox }
        } else {
            crate::runtime::NativeContextPurpose::Dispatch { fence, mailbox }
        };
        let (context, custody) = self
            .harness
            .open_durable_context(
                task,
                operation,
                crate::runtime::ContextSource::Native { work, purpose },
            )
            .await?;
        context.into_native_shared(
            custody.expect("native owner context retains its original custody"),
            work,
        )
    }

    /// Opens the protected original readonly journal for native witnesses.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn readonly_execution_journal_with_native_work(
        &self,
        task: TaskId,
        turn: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Arc<FilesystemExecutionJournal<P, A, O>>, OperationId)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
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
        fence: &LeaseFence,
        turn: OperationId,
        operation: OperationId,
        admission: &TaskAdmissionRecord,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<FilesystemExecutionJournal<P, A, O>> {
        #[cfg(not(target_arch = "wasm32"))]
        let owner = match io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            Some(work) => {
                self.host
                    .journal_owner_with_native_work(task, fence, work)
                    .await?
            }
            None => self.host.journal_owner(task, fence.clone()).await?,
        };
        #[cfg(target_arch = "wasm32")]
        let owner = self.host.journal_owner(task, fence.clone()).await?;
        let journal = FilesystemExecutionJournal::for_task(
            owner,
            operation,
            turn,
            self.filesystem.clone(),
            &self.volume,
            &self.verifier,
            &self.signed,
            self.maximum_payload_bytes.min(admission.limits.file_bytes),
            io,
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
                io,
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
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Arc<FilesystemWorkflowJournal<P, A, O>>> {
        let task = context
            .task()
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("tool has no admitted caller".into()))?;
        #[cfg(not(target_arch = "wasm32"))]
        let owner = match io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            Some(work) => {
                let (_, fence) = context.task().owned_task_binding_with_native_work(work)?;
                self.host
                    .journal_owner_with_native_work(task, fence, work)
                    .await?
            }
            None => {
                self.host
                    .journal_owner(task, context.task().owned_task_fence()?)
                    .await?
            }
        };
        #[cfg(target_arch = "wasm32")]
        let owner = self
            .host
            .journal_owner(task, context.task().owned_task_fence()?)
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

    /// Opens the owner-fenced workflow with optional original native preparation IO.
    pub async fn workflow_journal(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Arc<FilesystemWorkflowJournal<P, A, O>>> {
        #[cfg(not(target_arch = "wasm32"))]
        let native_io = io
            .and_then(crate::filesystem::PreparationIo::original_native_work)
            .map(crate::filesystem::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let observer = native_io.as_ref();
        #[cfg(target_arch = "wasm32")]
        let observer = {
            let _ = io;
            None
        };
        let admission = self.admission_observed(task, observer).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let owner = match observer {
            Some(io) => {
                self.host
                    .journal_owner_with_native_work(
                        task,
                        fence,
                        io.original_native_work().expect("native workflow observer"),
                    )
                    .await?
            }
            None => self.host.journal_owner(task, fence.clone()).await?,
        };
        #[cfg(target_arch = "wasm32")]
        let owner = self.host.journal_owner(task, fence.clone()).await?;
        FilesystemWorkflowJournal::for_task(
            owner,
            Arc::clone(&self.filesystem),
            &self.volume,
            &self.verifier,
            &self.signed,
            self.maximum_payload_bytes.min(admission.limits.file_bytes),
            observer,
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn readonly_workflow_journal(
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
            Some(&crate::filesystem::PreparationIo::from_native_work(work)?),
        )
    }

    /// Reopens the admitted registered task and its bound workflow without requiring a typed definition at the worker boundary.
    pub async fn open_task(&self, task: TaskId, fence: LeaseFence) -> Result<ResumableTaskSession> {
        let journal = self.workflow_journal(task, &fence, None).await?;
        self.harness
            .open_registered_task(
                task,
                &fence,
                journal,
                #[cfg(not(target_arch = "wasm32"))]
                None,
            )
            .await
    }

    /// Opens the original registered task using its one issued shared context.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn open_task_with_native_work(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        context: Arc<crate::runtime::TaskContext>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<ResumableTaskSession> {
        if !context.as_ref().original_task_binding_matches(task, fence) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native task context differs from original task lease",
            });
        }
        let journal = self
            .workflow_journal(
                task,
                fence,
                Some(&crate::filesystem::PreparationIo::from_native_work(work)?),
            )
            .await?;
        self.harness
            .open_registered_task(task, fence, journal, Some(context))
            .await
    }
}
