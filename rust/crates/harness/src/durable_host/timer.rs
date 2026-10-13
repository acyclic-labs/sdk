//! Main durable task operations.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Durably retains one timer identity without holding a waiting future.
    /// The existing owner-authenticated workflow wake supplies later admission.
    #[cfg(feature = "filesystem")]
    pub async fn poll_timer(
        self: &Arc<Self>,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        operation: OperationId,
        deadline_unix_ms: u64,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<bool> {
        let mut borrowed = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = borrowed.as_mut().map(|work| work as &mut dyn crate::store::HistoryIo);
        if deadline_unix_ms == 0 {
            return Err(Error::Invalid("timer deadline is invalid".into()));
        }
        let owner = self.journal_owner(task, fence.clone_with_work(&mut work)?, io).await?;
        if !owner.admission.grants.contains("timer:wait") {
            return Err(Error::Unauthorized("task scope lacks timer:wait".into()));
        }
        let path = acyclic_stream::StreamPath::new(format!("harness/v2/timers/{task}"))?;
        let key = Self::event_key("timers", task, operation)?;
        let bytes = OwnedBytes::from(crate::store::history::canonical_bytes_with_work(&TimerEvent {
            task_id: task,
            operation_id: operation,
            deadline_unix_ms,
        }, acyclic_stream::MAX_RECORD_BYTES as u64, &mut work)?);
        // Only the selected timer is retained. Paging bounds resident records;
        // no session-wide history ceiling is imposed.
        loop {
            self.verify_owner(task, &fence, false, &mut work).await?;
            let (tail, found) = self.timer_state(task, operation, deadline_unix_ms, &mut work).await?;
            if found {
                self.verify_owner(task, &fence, false, &mut work).await?;
                return Ok(self.clock.now_unix_millis() >= deadline_unix_ms);
            }
            if owner
                .append(
                    path.clone(),
                    tail,
                    &key,
                    bytes.clone(),
                    crate::distributed::JournalWrite::Fresh,
                    &mut work,
                )
                .await?
            {
                self.verify_owner(task, &fence, false, &mut work).await?;
                return Ok(self.clock.now_unix_millis() >= deadline_unix_ms);
            }
        }
    }

    /// Readiness requires the exact timer already retained by owned dispatch.
    #[cfg(feature = "filesystem")]
    pub(crate) async fn timer_ready(
        &self,
        task: TaskId,
        operation: OperationId,
        deadline: u64,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<bool> {
        let (_, retained) = self.timer_state(task, operation, deadline, work).await?;
        Ok(retained && self.clock.now_unix_millis() >= deadline)
    }

    pub(super) fn timer_stream(&self, task_id: TaskId) -> Result<acyclic_stream::Stream<P>> {
        acyclic_stream::Stream::new(self.stream.clone(), format!("harness/v2/timers/{task_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    pub(super) async fn timer_state(
        &self, task: TaskId, operation: OperationId, deadline_unix_ms: u64,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<(u64, bool)> {
        let path = crate::executor::preparation_format(format_args!("harness/v2/timers/{task}"), work)?;
        let (stream, _path) = crate::store::operations::stream(&self.stream, &path, work)?;
        let tail = match crate::store::operations::bounds_with_work(&self.stream, stream.path(), work).await? {
            Ok(bounds) => bounds.tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(crate::store::operations::stream_error_with_work(error, work)),
        };
        let mut after = 0;
        let mut found = false;
        while after < tail {
            let expected = after + (tail - after).min(64);
            let mut page = crate::store::operations::read_records(&self.stream, stream.path(), after,
                (expected - after) as u32, work).await?
                .map_err(|error| crate::store::operations::stream_error_with_work(error, work))?;
            while let Some(record) = page.next(work).await? {
                if record.sequence != after || after >= expected {
                    return Err(Error::Storage("timer replay sequence differs".into()));
                }
                let native_io;
                #[cfg(not(target_arch = "wasm32"))]
                { native_io = work.as_deref().and_then(|work| work.original_native_work())
                    .map(crate::filesystem::PreparationIo::from_native_work).transpose()?; }
                #[cfg(target_arch = "wasm32")]
                { native_io = None; }
                let event: TimerEvent = match native_io.as_ref() {
                    Some(io) => crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData,
                        &record.value, crate::wire::ErrorCode::Storage)?),
                    None => crate::contract::json_from_slice(&record.value)
                        .map_err(|error| Error::Storage(error.to_string()))?,
                };
                if event.task_id != task || event.deadline_unix_ms == 0 {
                    return Err(Error::Conflict("timer history belongs to another task".into()));
                }
                if event.operation_id == operation {
                    if found || event.deadline_unix_ms != deadline_unix_ms {
                        return Err(Error::Conflict("timer identity reused".into()));
                    }
                    found = true;
                }
                after += 1;
            }
            if after != expected { return Err(Error::Storage("invalid timer replay page".into())); }
        }
        Ok((tail, found))
    }
}
