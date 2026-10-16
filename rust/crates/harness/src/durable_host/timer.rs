//! Durable timer publication and bounded replay on the existing IO context.
use super::*;

#[cfg(feature = "filesystem")]
impl crate::filesystem::UploadRecord for TimerEvent {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(0)
    }
}

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Guest scheduling uses the same fenced publication as native timer commands.
    #[cfg(feature = "filesystem")]
    pub async fn schedule_timer(
        self: &Arc<Self>,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        timer: &acyclic_actors::timers::TimerRecord,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<bool> {
        acyclic_actors::timers::schedule(
            async |target, operation, deadline| {
                if target.into_bytes() != task.into_bytes() {
                    return Err(Error::Unauthorized(
                        "timer target differs from scoped task".into(),
                    ));
                }
                self.poll_timer(
                    task,
                    fence,
                    OperationId::from_bytes(operation.into_bytes()),
                    deadline,
                    io,
                )
                .await
            },
            timer,
        )
        .await
    }

    /// Retains a scoped absolute wait and reports readiness on the injected clock.
    /// The runtime owns waiting for timer delivery; this entry point never polls time.
    #[cfg(feature = "filesystem")]
    pub async fn wait_until(
        self: &Arc<Self>,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        operation: OperationId,
        deadline_unix_ms: u64,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<bool> {
        self.poll_timer(task, fence, operation, deadline_unix_ms, io)
            .await
    }

    /// Retains the exact timer identity under the current task fence.
    #[cfg(feature = "filesystem")]
    pub async fn poll_timer(
        self: &Arc<Self>,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        operation: OperationId,
        deadline_unix_ms: u64,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<bool> {
        if deadline_unix_ms == 0 {
            return Err(Error::Invalid("timer deadline is invalid".into()));
        }
        let owner = self.journal_owner(task, fence, io).await?;
        if !owner.admission.grants.contains(capability::TIMER_WAIT) {
            return Err(Error::Unauthorized("task scope lacks timer:wait".into()));
        }
        let mut history = io.map(|io| io.history_io());
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        let text = crate::executor::preparation_format(
            format_args!("harness/v2/timers/{task}"),
            &mut work,
        )?;
        let mut quota =
            crate::wire_codec::CodecQuota::from_retained(&mut work, text.capacity() as u64);
        let path = acyclic_stream::StreamPath::new(text)?;
        let (key, retained) = Self::event_key("timers", task, operation, quota.work())?;
        quota.adopt(retained)?;
        let event = TimerEvent {
            task_id: task,
            operation_id: operation,
            deadline_unix_ms,
        };
        let bytes = canonical_payload(&event, acyclic_stream::MAX_RECORD_BYTES as u64, io)?;
        loop {
            self.verify_owner(task, fence, false, quota.work()).await?;
            let (tail, found) = self
                .timer_state(task, operation, deadline_unix_ms, quota.work())
                .await?;
            if found
                || owner
                    .append(
                        path.clone(),
                        tail,
                        Some(&key),
                        bytes.clone(),
                        crate::distributed::JournalWrite::Fresh,
                        quota.work(),
                    )
                    .await?
            {
                self.verify_owner(task, fence, false, quota.work()).await?;
                return Ok(self.now_unix_millis() >= deadline_unix_ms);
            }
        }
    }

    /// Requires the exact retained timer before reporting readiness.
    #[cfg(feature = "filesystem")]
    pub(crate) async fn timer_ready(
        &self,
        task: TaskId,
        operation: OperationId,
        deadline: u64,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<bool> {
        let (_, retained) = self.timer_state(task, operation, deadline, work).await?;
        Ok(retained && self.now_unix_millis() >= deadline)
    }

    pub(super) async fn timer_state(
        &self,
        task: TaskId,
        operation: OperationId,
        deadline: u64,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<(u64, bool)> {
        let io = operation_io(work)?;
        let text =
            crate::executor::preparation_format(format_args!("harness/v2/timers/{task}"), work)?;
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, text.capacity() as u64);
        let path = acyclic_stream::StreamPath::new(text)?;
        let tail =
            match crate::store::operations::bounds_with_work(&self.stream, &path, quota.work())
                .await?
            {
                Ok(bounds) => bounds.tail,
                Err(StreamError::NotFound) => 0,
                Err(error) => {
                    return Err(crate::store::operations::stream_error_with_work(
                        error,
                        quota.work(),
                    ));
                }
            };
        let mut after = 0;
        let mut found = false;
        while after < tail {
            let count = u32::try_from((tail - after).min(64))
                .map_err(|_| Error::Storage("timer replay page extent overflows".into()))?;
            let end = after + u64::from(count);
            let mut page = crate::store::operations::read_records(
                &self.stream,
                &path,
                after,
                count,
                quota.work(),
            )
            .await?
            .map_err(|error| {
                crate::store::operations::stream_error_with_work(error, quota.work())
            })?;
            while let Some(record) = page.next(quota.work()).await? {
                if record.sequence != after || after >= end {
                    return Err(Error::Storage("timer replay sequence differs".into()));
                }
                let event: TimerEvent = match &io {
                    #[cfg(feature = "filesystem")]
                    Some(io) => io.decode_json(&record.value)?,
                    _ => crate::contract::json_from_slice(&record.value)
                        .map_err(|error| Error::Storage(error.to_string()))?,
                };
                if event.task_id != task || event.deadline_unix_ms == 0 {
                    return Err(Error::Conflict(
                        "timer history belongs to another task".into(),
                    ));
                }
                if event.operation_id == operation {
                    if found || event.deadline_unix_ms != deadline {
                        return Err(Error::Conflict("timer identity reused".into()));
                    }
                    found = true;
                }
                after += 1;
            }
            if after != end {
                return Err(Error::Storage("timer replay page is incomplete".into()));
            }
        }
        Ok((tail, found))
    }
}
