//! Disposable chunk files published only after their canonical journal append.
use super::*;
use crate::task_volume::{Channel, Component, FileKey, FileValue, TaskFile};

struct ModelPayload {
    channel: Option<Channel>,
    bytes: acyclic_stream::OwnedBytes,
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn append_with_chunks(
        &self,
        operation: OperationId,
        expected_tail: Option<u64>,
        key: &str,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        let accepted = self
            .append_validated(operation, expected_tail, key, event, io)
            .await?;
        if accepted {
            crate::conversation::BoundedContentFuture::new(io, || {
                self.publish_chunk(key, event, io)
            })?
            .await?;
        }
        Ok(accepted)
    }

    pub(super) async fn publish_chunk(
        &self,
        key: &str,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let (Some(files), Some((owner, operation))) = (&self.task_files, &self.owner) else {
            return Ok(());
        };
        let ExecutionEvent::Model {
            step,
            purpose: crate::executor::ModelPurpose::Response,
            ..
        } = event
        else {
            return Ok(());
        };
        let Some(model) = self.verify_event_refs_with_io(event, io).await? else {
            return Ok(());
        };
        let Some(ModelPayload { channel, bytes }) =
            model_payload(model, io, self.maximum_payload_bytes)?
        else {
            return Ok(());
        };
        let _bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?;
        let sequence = key
            .rsplit(':')
            .next()
            .and_then(|part| part.parse().ok())
            .ok_or_else(|| Error::Invalid("model event has no canonical ordinal".into()))?;
        #[cfg(not(target_arch = "wasm32"))]
        let work = io.and_then(PreparationIo::original_native_work);
        #[cfg(target_arch = "wasm32")]
        let work = None;
        let task = Component::new(owner.task_id().to_string())?;
        let turn = files
            .inner
            .model_turn(
                task.clone(),
                Component::new(operation.to_string())?,
                *step,
                work,
            )
            .await?;
        let value = FileValue {
            requester: None,
            publication: None,
            key: FileKey::Task {
                task,
                file: if channel.is_none() {
                    TaskFile::Completion(turn)
                } else {
                    TaskFile::Chunk { turn, sequence }
                },
            },
            channel,
            bytes,
        };
        loop {
            let head = files.inner.head().await?;
            match files
                .inner
                .append_if(head, &value, Vec::new(), None, work)
                .await
            {
                Ok(_) => return Ok(()),
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}

fn model_payload(
    model: ModelEvent,
    io: Option<&PreparationIo>,
    maximum_bytes: u64,
) -> Result<Option<ModelPayload>> {
    let (channel, bytes) = match model {
        ModelEvent::Content { delta } => (Some(Channel::Text), delta.into_bytes().into()),
        ModelEvent::Reasoning { delta } => (Some(Channel::Reasoning), delta.into_bytes().into()),
        ModelEvent::Completed { metadata } => (
            None,
            match io {
                Some(io) => io.encode(&metadata, maximum_bytes, true)?,
                None => serde_json::to_vec(&metadata)
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .into(),
            },
        ),
        _ => return Ok(None),
    };
    Ok(Some(ModelPayload { channel, bytes }))
}
