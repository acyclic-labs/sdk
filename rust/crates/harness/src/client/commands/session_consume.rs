use super::*;

#[cfg(test)]
thread_local! {
    static SELECTION_AFTER_OUTCOME: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl<P: StreamProvider + ?Sized> CommandSession<P> {
    /// Consume one immutable command, never repeating an unacknowledged effect.
    pub async fn consume<F, Fut>(
        &mut self,
        request: &files::FileValue,
        execute: F,
    ) -> Result<Option<Value>>
    where
        F: FnOnce(OperationId, Option<TaskId>, CommandResolution) -> Fut,
        Fut: std::future::Future<Output = Result<Value>>,
    {
        Box::pin(self.consume_with_recovery::<false, _, _, _, _>(
            request,
            execute,
            |operation, _, _| async move { Err::<Value, _>(Error::Indeterminate(operation)) },
        ))
        .await
    }

    pub(crate) async fn consume_with_recovery<const RECOVER: bool, F, Fut, R, Recovery>(
        &mut self,
        request: &files::FileValue,
        execute: F,
        recover: R,
    ) -> Result<Option<Value>>
    where
        F: FnOnce(OperationId, Option<TaskId>, CommandResolution) -> Fut,
        Fut: std::future::Future,
        Fut::Output: Into<crate::client::CommandExecution>,
        R: FnOnce(OperationId, Option<TaskId>, CommandResolution) -> Recovery,
        Recovery: std::future::Future,
        Recovery::Output: Into<crate::client::CommandExecution>,
    {
        if self.read_file(&request.key).await?.as_ref() != Some(request) {
            return Err(Error::Unauthorized(
                "command request differs from its observed immutable file".into(),
            ));
        }
        let (task_id, id) = match &request.key {
            files::FileKey::Task {
                task,
                file: files::TaskFile::Request(id),
            } => (Some(TaskId::parse(task.as_str())?), id),
            files::FileKey::Session {
                session,
                file: files::TaskFile::Request(id),
            } if session.as_str() == self.session.to_string() => (None, id),
            _ => {
                return Err(Error::Invalid(
                    "expected this session's command request file".into(),
                ));
            }
        };
        let (owner, operation) = files::command_query::identity(id)
            .ok_or_else(|| Error::Invalid("not a command request identity".into()))?;
        if owner.is_some_and(|owner| owner != self.session) {
            return Err(Error::Invalid(
                "command belongs to another UI session".into(),
            ));
        }
        let key = |file| match &request.key {
            files::FileKey::Task { task, .. } => files::FileKey::Task {
                task: task.clone(),
                file,
            },
            files::FileKey::Session { session, .. } => files::FileKey::Session {
                session: session.clone(),
                file,
            },
            _ => unreachable!(),
        };
        let claim = key(files::TaskFile::ExecutionClaim(id.clone()));
        let claimed = self.read_file(&claim).await?.is_some();
        if claimed
            && self
                .read_file(&key(files::TaskFile::ExecutionResult(id.clone())))
                .await?
                .is_some()
        {
            return Ok(None);
        }
        if claimed && !RECOVER {
            return Err(Error::Indeterminate(operation));
        }
        let resolution = CommandResolution::from_request(&request.bytes)?;
        let bytes = request.bytes.to_vec();
        self.make_room(3)?;
        if !claimed && let Err(error) = self.publish(claim, bytes, true).await {
            if matches!(error, Error::Conflict(_)) {
                self.pending.push_front(request.clone());
            }
            return Err(error);
        }
        let execution: crate::client::CommandExecution = if claimed {
            recover(operation, task_id, resolution).await.into()
        } else {
            execute(operation, task_id, resolution).await.into()
        };
        let (retained_task, result) = execution.into_parts();
        if matches!(
            &result,
            Err(Error::Indeterminate(_) | Error::CommandIndeterminate { .. })
        ) {
            return result.map(Some);
        }
        let result_key = key(files::TaskFile::ExecutionResult(id.clone()));
        let result_budget = self.publication_byte_limit(result_key.clone())? as u64;
        // Completion releases the prompt. Its captured selection must already
        // be visible when any observer sees that completion file.
        #[cfg(not(test))]
        let selection_first = true;
        #[cfg(test)]
        let selection_first = !SELECTION_AFTER_OUTCOME.with(std::cell::Cell::get);
        // Admission owns the transcript even when cancellation or an unknown
        // outcome cannot carry its identity in an ordinary error value.
        let retained_selection = retained_task.map(|task| json!({"selected_task": task}));
        let selection = result.as_ref().ok().or(retained_selection.as_ref());
        if selection_first && let Some(value) = selection {
            self.apply_outcome(value).await?;
        }
        self.publish_execution_result(result_key, &result, result_budget)
            .await?;
        self.persist_activity().await?;
        if !selection_first && let Some(value) = selection {
            self.apply_outcome(value).await?;
        }
        result.map(Some)
    }
    async fn publish_execution_result(
        &mut self,
        result_key: files::FileKey,
        result: &Result<Value>,
        result_budget: u64,
    ) -> Result<()> {
        loop {
            let previous = self.generation;
            let sequence = self.generation.position;
            let bytes = archive::output_bytes(sequence, result, result_budget)?;
            match self.publish(result_key.clone(), bytes, true).await {
                Ok(()) => break,
                // The effect has already finished. Only retry its outcome file,
                // after following the competing publication; never run it again.
                Err(Error::Conflict(_))
                    if self.generation.position > previous.position
                        && self.next_change.is_none() =>
                {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
#[path = "session_consume/tests.rs"]
mod tests;
