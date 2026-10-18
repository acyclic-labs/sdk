use super::*;

impl CommandResolution {
    pub(crate) fn from_request(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .or_else(|_| serde_json::from_slice(bytes).map(|request| Self::Submit { request }))
            .map_err(codec)
    }
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
            |operation, _, _| async move { Err(Error::Indeterminate(operation)) },
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
        Fut: std::future::Future<Output = Result<Value>>,
        R: FnOnce(OperationId, Option<TaskId>, CommandResolution) -> Recovery,
        Recovery: std::future::Future<Output = Result<Value>>,
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
        let result = if claimed {
            recover(operation, task_id, resolution).await
        } else {
            execute(operation, task_id, resolution).await
        };
        if claimed && matches!(&result, Err(Error::Indeterminate(_))) {
            return result.map(Some);
        }
        let result_key = key(files::TaskFile::ExecutionResult(id.clone()));
        let result_budget = self.publication_byte_limit(result_key.clone())? as u64;
        loop {
            let previous = self.generation;
            let sequence = self.generation.position;
            let bytes = archive::output_bytes(sequence, &result, result_budget)?;
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
        self.persist_activity().await?;
        if let Ok(value) = &result {
            self.apply_outcome(value).await?;
        }
        result.map(Some)
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
#[path = "session_consume/tests.rs"]
mod tests;
