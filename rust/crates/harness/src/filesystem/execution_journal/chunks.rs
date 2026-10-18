//! Chunk projection prepared for the canonical observation's transaction.
use super::*;
use crate::task_volume::{Channel, Component};

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn prepare_chunk(
        &self,
        execution: OperationId,
        event: &ExecutionEvent,
        model: Option<&ModelEvent>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<crate::task_volume::JournalFile>> {
        let (Some(files), Some((owner, _))) = (&self.task_files, &self.owner) else {
            return Ok(None);
        };
        let ExecutionEvent::Model {
            step,
            purpose: crate::executor::ModelPurpose::Response,
            ..
        } = event
        else {
            return Ok(None);
        };
        let (channel, delta) = match model {
            Some(ModelEvent::Content { delta }) => (Channel::Text, delta),
            Some(ModelEvent::Reasoning { delta }) => (Channel::Reasoning, delta),
            _ => return Ok(None),
        };
        if delta.len() as u64 > files.maximum_bytes() {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "assistant chunk exceeds task file bound",
            ));
        }
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let copied = if crate::test_support::accounted::chunk_copy_enabled() {
            Some(crate::executor::with_history_io(io, |work| {
                crate::wire_codec::CodecQuota::produce(work, |quota| {
                    crate::core::copy_copy_vector(delta.as_bytes(), quota)
                })
                .and_then(|(bytes, _)| {
                    crate::store::operations::owned_bytes_from_retained_vec(bytes, work)
                        .map(|(bytes, _)| bytes)
                })
            })?)
        } else {
            None
        };
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let payload = copied.as_deref().unwrap_or(delta.as_bytes());
        #[cfg(not(all(test, not(target_arch = "wasm32"))))]
        let payload = delta.as_bytes();
        let task = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{}", owner.task_id()), work)
        })?;
        let key = files
            .inner
            .next_chunk_key(Component::new(task)?, execution, u64::from(*step), io)
            .await?;
        files
            .inner
            .prepare_named_chunk(key, channel, payload, io)
            .await
    }
}
