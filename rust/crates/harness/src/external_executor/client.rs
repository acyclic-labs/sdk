use super::{ExternalRequest, volume};
use crate::task_volume::{
    Component, Error, FileKey, FileValue, Query, Result, TaskFile, TaskVolume,
};
use acyclic_stream::StreamProvider;
use futures::StreamExt as _;
use serde_json::Value;

impl ExternalRequest {
    /// Publishes the original request and follows its immutable answer.
    /// Authentication and transport belong to the supplied platform provider.
    /// Concurrent writes to other paths retain the original immutable request.
    pub fn send<'a, P: StreamProvider + ?Sized, R: StreamProvider + ?Sized>(
        &'a self,
        requests: &'a TaskVolume<P>,
        results: &'a TaskVolume<R>,
        task: Component,
    ) -> impl Future<Output = Result<Value>> + 'a {
        Box::pin(async move {
            let write = self.to_file(task.clone())?;
            super::append::publish(requests, &write, None, None).await?;
            self.follow_result(results, task).await
        })
    }

    pub(crate) fn follow_result<'a, P: StreamProvider + ?Sized>(
        &'a self,
        results: &'a TaskVolume<P>,
        task: Component,
    ) -> impl Future<Output = Result<Value>> + 'a {
        Box::pin(async move {
            let key = self.result_key(task)?;
            let file = follow_file(results, key).await?.await?;
            volume::decode(&file.bytes)
        })
    }

    /// Standard result-file identity; only the fenced harness owner may write it.
    pub fn result_key(&self, task: Component) -> Result<FileKey> {
        let mut name = [0; 36];
        let operation = uuid::Uuid::from_bytes(self.operation_id.into_bytes());
        Ok(FileKey::Task {
            task,
            file: TaskFile::RequestResult(Component::new(
                operation.hyphenated().encode_lower(&mut name),
            )?),
        })
    }

    pub(crate) fn original_tool_result_key(
        &self,
        task: Component,
        call_id: &str,
    ) -> Result<FileKey> {
        let operation = super::normalize::operation(
            self.host,
            &self.session_id,
            &self.caller,
            &super::ExternalAction::RunTool {
                call_id: call_id.to_owned(),
                name: String::new(),
                arguments: Value::Null,
            },
        );
        Ok(FileKey::Task {
            task,
            file: TaskFile::RequestResult(Component::new(operation.to_string())?),
        })
    }

    /// Encodes one native hook answer for publication with the actor checkpoint.
    pub fn result_file(&self, task: Component, answer: &Value) -> Result<FileValue> {
        if !self.valid() {
            return Err(Error::InvalidSegment);
        }
        Ok(FileValue::new(
            self.result_key(task)?,
            volume::encode(answer)?,
        ))
    }
}

/// Establishes the atomic snapshot/follow cut before returning a file waiter.
pub(super) async fn follow_file<P: StreamProvider + ?Sized>(
    files: &TaskVolume<P>,
    key: FileKey,
) -> Result<acyclic_stream::BoxProviderFuture<'static, Result<FileValue>>> {
    let mut observation = files.observe(Query::File(key.clone()), None, 1).await?;
    Ok(Box::pin(async move {
        let file = if let Some(file) = observation
            .snapshot
            .and_then(|snapshot| snapshot.files.into_iter().next())
        {
            file
        } else {
            loop {
                let change = observation
                    .changes
                    .next()
                    .await
                    .ok_or(Error::ResetRequired)??;
                if let Some(file) = change.file {
                    break file;
                }
            }
        };
        if file.key != key || file.channel.is_some() {
            return Err(Error::InvalidSegment);
        }
        Ok(file)
    }))
}
