//! Wake for the filesystem task runtime.

use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Inspects one page of a finite coordinator snapshot and polls its owned passive waits.
    pub async fn poll_task_wake_page(
        &self,
        cursor: Option<TaskWakeCursor>,
        maximum_events: u32,
    ) -> Result<TaskWakePage> {
        let (next_revision, through_revision, events_read, tasks) = self
            .host
            .workflow_wait_page(
                cursor.map_or(0, |cursor| cursor.after_revision),
                cursor.map(|cursor| cursor.through_revision),
                maximum_events,
            )
            .await?;
        let tasks_polled = u32::try_from(tasks.len())
            .map_err(|_| Error::Storage("wake candidate count exceeds its allowance".into()))?;
        let mut woken = Vec::new();
        for task in tasks {
            if self.poll_task_wake(task, None).await? {
                woken.push(task);
            }
        }
        Ok(TaskWakePage {
            cursor: (next_revision < through_revision).then_some(TaskWakeCursor {
                after_revision: next_revision,
                through_revision,
            }),
            through_revision,
            events_read,
            tasks_polled,
            woken,
        })
    }

    /// Polls one durable passive wait and fills its existing authenticated wake slot when ready.
    pub async fn poll_task_wake(
        &self,
        task: TaskId,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<bool> {
        use crate::workflow::WorkflowJournal as _;
        #[cfg(not(target_arch = "wasm32"))]
        let slot = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| {
                observed::boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "native wake requires original observer",
                )
            })?;
            let mut operation_io = crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            );
            crate::store::HistoryIo::retain_owned(
                &mut operation_io,
                std::mem::size_of::<Option<crate::scheduler::WorkflowSuspension>>() as u64,
            )?;
            let slot = self
                .host
                .workflow_suspension(task, &mut Some(&mut operation_io))
                .await?;
            io.absorb_native_custody(operation_io.into_custody()?)?;
            slot
        } else {
            self.host.workflow_suspension(task, &mut None).await?
        };
        #[cfg(target_arch = "wasm32")]
        let slot = self.host.workflow_suspension(task, &mut None).await?;
        let Some(slot) = slot else {
            return Ok(false);
        };
        let Some(waiting) = slot.waiting_command else {
            return Ok(false);
        };
        let admission = self.admission_observed(task, io).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let journal = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| {
                observed::boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "native wake journal requires original observer",
                )
            })?;
            self.readonly_workflow_journal(task, &admission, work)
                .await?
        } else {
            FilesystemWorkflowJournal::new(
                self.host.workflow_stream(),
                Arc::clone(&self.filesystem),
                &admission.operation_id.to_string(),
                &self.volume,
                &self.verifier,
                &self.signed,
                self.maximum_payload_bytes.min(admission.limits.file_bytes),
                None,
            )?
        };
        #[cfg(target_arch = "wasm32")]
        let journal = FilesystemWorkflowJournal::new(
            self.host.workflow_stream(),
            Arc::clone(&self.filesystem),
            &admission.operation_id.to_string(),
            &self.volume,
            &self.verifier,
            &self.signed,
            self.maximum_payload_bytes.min(admission.limits.file_bytes),
            None,
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let retained = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| {
                observed::boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "native wake admission requires original observer",
                )
            })?;
            let (retained, custody) = journal.admission_with_native_work(work)?.await?;
            io.absorb_native_custody(custody)?;
            retained
        } else {
            journal.admission().await?
        };
        #[cfg(target_arch = "wasm32")]
        let retained = journal.admission().await?;
        let retained = retained.ok_or_else(|| {
            observed::boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "suspended workflow admission is absent",
            )
        })?;
        if retained.operation_id != admission.operation_id
            || retained.request_digest
                != crate::executor::with_history_io(io, |work| {
                    crate::contract::canonical_json_digest(
                        &admission,
                        if io.is_some() {
                            acyclic_stream::MAX_RECORD_BYTES as u64
                        } else {
                            u64::MAX
                        },
                        work,
                    )
                })?
            || retained.initial.machine != admission.machine
        {
            return Err(observed::boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "wake workflow differs from admitted task",
            ));
        }
        let record = journal.observe_transition(slot.revision, io).await?;
        if record.next.revision != slot.revision
            || record.next.machine != admission.machine
            || !matches!(record.transition.status, MachineStatus::Suspended)
        {
            return Err(observed::boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "wake checkpoint differs from suspension",
            ));
        }
        let command = record
            .transition
            .commands
            .iter()
            .find(|command| command.operation_id == waiting)
            .ok_or_else(|| {
                observed::boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "suspended command is absent from outbox",
                )
            })?;
        #[cfg(not(target_arch = "wasm32"))]
        let context = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| {
                observed::boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "native wait context requires original observer",
                )
            })?;
            let (context, custody) = self
                .harness
                .durable_context_with_native_work(task, admission.operation_id, work)
                .await?;
            WorkerContext::Native(context.into_native_shared(custody, work)?)
        } else {
            WorkerContext::Ordinary(
                self.harness
                    .durable_context(task, admission.operation_id)
                    .await?,
            )
        };
        #[cfg(target_arch = "wasm32")]
        let context = WorkerContext::Ordinary(
            self.harness
                .durable_context(task, admission.operation_id)
                .await?,
        );
        #[cfg(not(target_arch = "wasm32"))]
        let payload = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| {
                observed::boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "native wait payload requires original observer",
                )
            })?;
            let body = context
                .context()
                .read_file_with_native_work(&command.payload, work)
                .await?;
            crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData::<Value>,
                &body,
                crate::wire::ErrorCode::Storage,
            )?)
        } else {
            crate::contract::json_from_slice(&context.context().read_file(&command.payload).await?)
                .map_err(|error| Error::Invalid(format!("invalid wait command JSON: {error}")))?
        };
        #[cfg(target_arch = "wasm32")]
        let payload =
            crate::contract::json_from_slice(&context.context().read_file(&command.payload).await?)
                .map_err(|error| Error::Invalid(format!("invalid wait command JSON: {error}")))?;
        if !self
            .commands()
            .wait_ready_observed(context.context(), command, payload, io)
            .await?
        {
            return Ok(false);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| {
                observed::boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "native wake publication requires original observer",
                )
            })?;
            self.host
                .wake_command(
                    task,
                    slot.revision,
                    waiting,
                    &mut Some(&mut crate::store::HistoryWork::new(
                        crate::filesystem::PreparationIo::from_native_work(work)?,
                    )),
                )
                .await?;
        } else {
            self.host
                .wake_command(task, slot.revision, waiting, &mut None)
                .await?;
        }
        #[cfg(target_arch = "wasm32")]
        self.host
            .wake_command(task, slot.revision, waiting, &mut None)
            .await?;
        Ok(true)
    }

    pub(super) async fn suspend_command(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        revision: u64,
        command: &WorkflowCommand,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        let guard_execution = matches!(
            command.kind.as_str(),
            crate::filesystem::TOOL_TASK_COMMAND_KIND | crate::filesystem::MODEL_TASK_COMMAND_KIND
        );
        let command = command.operation_id;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            io.original_native_work().ok_or_else(|| {
                observed::boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "native suspension requires original observer",
                )
            })?;
        }
        if guard_execution {
            self.execution_journal(task, fence, command, io)
                .await?
                .0
                .suspend_workflow_command_if_quiescent(revision, command, io)
                .await
        } else {
            self.host
                .suspend_workflow(
                    task,
                    fence,
                    revision,
                    Some(command),
                    None,
                    None,
                    &mut observed::history(io)?
                        .as_mut()
                        .map(|work| work as &mut dyn crate::store::HistoryIo),
                )
                .await
        }
    }
}
