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
        io: Option<&super::super::PreparationIo>,
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
            let mut operation_io = crate::filesystem::PreparationIo::from_native_work(work)
                .map(crate::store::HistoryWork::new)?;
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
            self.readonly_workflow_journal_with_native_work(task, work)
                .await?
        } else {
            FilesystemWorkflowJournal::new(
                self.host.workflow_stream(),
                Arc::clone(&self.filesystem),
                &admission.operation_id.to_string(),
                crate::filesystem::FilesystemJournalBinding {
                    volume: &self.volume,
                    verifier: &self.verifier,
                    scope: &self.signed,
                    maximum_bytes: self.maximum_payload_bytes.min(admission.limits.file_bytes),
                    io: None,
                },
            )?
        };
        #[cfg(target_arch = "wasm32")]
        let journal = FilesystemWorkflowJournal::new(
            self.host.workflow_stream(),
            Arc::clone(&self.filesystem),
            &admission.operation_id.to_string(),
            crate::filesystem::FilesystemJournalBinding {
                volume: &self.volume,
                verifier: &self.verifier,
                scope: &self.signed,
                maximum_bytes: self.maximum_payload_bytes.min(admission.limits.file_bytes),
                io: None,
            },
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let retained = if let Some(io) = io {
            let _work = io.original_native_work().ok_or_else(|| {
                observed::boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "native wake admission requires original observer",
                )
            })?;
            let (retained, custody) = journal.admission(Some(io))?.await?;
            let Some(custody) = custody else {
                drop(retained);
                return Err(Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                    custody: None,
                });
            };
            io.absorb_native_custody(custody)?;
            retained
        } else {
            journal.admission(None)?.await?.0
        };
        #[cfg(target_arch = "wasm32")]
        let retained = journal.admission(None)?.await?.0;
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
                    crate::executor::json::canonical_json_digest(
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
        let checkpoint = record.checkpoint()?;
        if checkpoint.1 != slot.revision
            || checkpoint.0 != &admission.machine
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
            let context = self
                .harness
                .durable_context(
                    task,
                    admission.operation_id,
                    Some(&crate::filesystem::PreparationIo::from_native_work(work)?),
                )
                .await?;
            WorkerContext::Native(context)
        } else {
            WorkerContext::Ordinary(
                self.harness
                    .durable_context(task, admission.operation_id, None)
                    .await?,
            )
        };
        #[cfg(target_arch = "wasm32")]
        let context = WorkerContext::Ordinary(
            self.harness
                .durable_context(task, admission.operation_id, None)
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
                .read_file(
                    &command.payload,
                    Some(&crate::executor::PreparationIo::from_native_work(work)?),
                )
                .await?;
            crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData::<Value>,
                &body,
                crate::wire::ErrorCode::Storage,
            )?)?
        } else {
            crate::contract::json_from_slice(
                &context.context().read_file(&command.payload, None).await?,
            )
            .map_err(|error| Error::Invalid(format!("invalid wait command JSON: {error}")))?
        };
        #[cfg(target_arch = "wasm32")]
        let payload = crate::contract::json_from_slice(
            &context.context().read_file(&command.payload, None).await?,
        )
        .map_err(|error| Error::Invalid(format!("invalid wait command JSON: {error}")))?;
        let mail_after = if command.kind == super::super::MAIL_RECEIVE_TASK_COMMAND_KIND
            && super::super::child_agent::is_child(&admission)
        {
            Some(
                super::super::child_agent::mail_cursor(&record.transition, command).map_err(
                    |reason| observed::boundary(io, crate::wire::ErrorCode::Unauthorized, reason),
                )?,
            )
        } else {
            None
        };
        if !self
            .commands()
            .wait_ready_observed(context.context(), command, payload, mail_after, io)
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
                    &mut Some(
                        &mut crate::filesystem::PreparationIo::from_native_work(work)
                            .map(crate::store::HistoryWork::new)?,
                    ),
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
}
