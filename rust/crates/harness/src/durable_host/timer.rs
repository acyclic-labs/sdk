//! Main durable task operations.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Durably retains one timer identity without holding a waiting future.
    /// The existing owner-authenticated workflow wake supplies later admission.
    #[cfg(feature = "filesystem")]
    pub async fn poll_timer(
        self: &Arc<Self>,
        task: TaskId,
        fence: crate::scheduler::LeaseFence,
        operation: OperationId,
        deadline_unix_ms: u64,
    ) -> Result<bool> {
        if deadline_unix_ms == 0 {
            return Err(Error::Invalid("timer deadline is invalid".into()));
        }
        let owner = self.journal_owner(task, &(fence.clone()), None).await?;
        if !owner.admission.grants.contains("timer:wait") {
            return Err(Error::Unauthorized("task scope lacks timer:wait".into()));
        }
        let path = acyclic_stream::StreamPath::new(format!("harness/v2/timers/{task}"))?;
        let key = Self::event_key("timers", task, operation)?;
        let bytes = OwnedBytes::from(crate::contract::canonical_json_bytes(&TimerEvent {
            task_id: task,
            operation_id: operation,
            deadline_unix_ms,
        })?);
        // Only the selected timer is retained. Paging bounds resident records;
        // no session-wide history ceiling is imposed.
        loop {
            self.verify_owner(task, &fence, false).await?;
            let (tail, found) = self.timer_state(task, operation, deadline_unix_ms).await?;
            if found {
                self.verify_owner(task, &fence, false).await?;
                return Ok(self.clock.now_unix_millis() >= deadline_unix_ms);
            }
            if owner
                .append(
                    path.clone(),
                    tail,
                    &key,
                    bytes.clone(),
                    crate::distributed::JournalWrite::Fresh,
                    &mut None,
                )
                .await?
            {
                self.verify_owner(task, &fence, false).await?;
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
    ) -> Result<bool> {
        let (_, retained) = self.timer_state(task, operation, deadline).await?;
        Ok(retained && self.clock.now_unix_millis() >= deadline)
    }

    pub(super) fn timer_stream(&self, task_id: TaskId) -> Result<acyclic_stream::Stream<P>> {
        acyclic_stream::Stream::new(self.stream.clone(), format!("harness/v2/timers/{task_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    pub(super) async fn timer_state(
        &self,
        task: TaskId,
        operation: OperationId,
        deadline_unix_ms: u64,
    ) -> Result<(u64, bool)> {
        let stream = self.timer_stream(task)?;
        let tail = match stream.bounds().await {
            Ok(bounds) => bounds.tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(error.into()),
        };
        let mut after = 0;
        let mut found = false;
        while after < tail {
            let page = stream
                .read(after, (tail - after).min(64) as u32)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            if page.is_empty() || page.len() > 64 {
                return Err(Error::Storage("invalid timer replay page".into()));
            }
            for record in page {
                if record.sequence != after || after >= tail {
                    return Err(Error::Storage("timer replay sequence differs".into()));
                }
                let event: TimerEvent = crate::contract::json_from_slice(&record.value)
                    .map_err(|error| Error::Storage(error.to_string()))?;
                if event.task_id != task || event.deadline_unix_ms == 0 {
                    return Err(Error::Conflict(
                        "timer history belongs to another task".into(),
                    ));
                }
                if event.operation_id == operation {
                    if found || event.deadline_unix_ms != deadline_unix_ms {
                        return Err(Error::Conflict("timer identity reused".into()));
                    }
                    found = true;
                }
                after += 1;
            }
        }
        Ok((tail, found))
    }
}
