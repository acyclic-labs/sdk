//! Point selections of original model call proposals, including rejected tools.
//! Keys index the original observation; there is no second invocation record.
use super::*;
use acyclic_stream::OwnedBytes;

fn key(step: u32, call: &str, io: Option<&PreparationIo>) -> Result<OwnedBytes> {
    let digest = crate::model::hash_bytes(call.as_bytes(), io)?;
    let identity = ("model-call", 1_u32, step, digest);
    match io {
        Some(io) => io.encode(&identity, acyclic_stream::MAX_RECORD_BYTES as u64, true),
        None => crate::contract::canonical_json_bytes(&identity).map(Into::into),
    }
}

pub(super) fn observation_key(
    event: &ExecutionEvent,
    model: Option<&ModelEvent>,
    io: Option<&PreparationIo>,
) -> Result<Option<OwnedBytes>> {
    match (event, model) {
        (
            ExecutionEvent::Model {
                step,
                purpose: crate::executor::ModelPurpose::Response,
                ..
            }
            | ExecutionEvent::ModelInline {
                step,
                purpose: crate::executor::ModelPurpose::Response,
                ..
            },
            Some(ModelEvent::ToolCall(call)),
        ) => key(*step, &call.call_id, io).map(Some),
        _ => Ok(None),
    }
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Read one source key and authenticate that exact original observation.
    /// No step, history, private directory or model-response scan is needed.
    pub(super) async fn original_model_call(
        &self,
        operation: OperationId,
        step: u32,
        call_id: &str,
        io: Option<&PreparationIo>,
    ) -> Result<Option<crate::tool::ToolInvocation>> {
        self.require_operation(operation, io)?;
        let key = key(step, call_id, io)?;
        let _key_hold = io.map(|io| io.guard_bytes(&key)).transpose()?;
        let sequence = {
            let mut history = io.map(PreparationIo::history_io);
            let mut work = history
                .as_mut()
                .map(|io| io as &mut dyn crate::store::HistoryIo);
            let path = crate::distributed::execution_path(operation, &mut work)?;
            let _path_hold = io
                .filter(|io| io.original_native_work().is_none())
                .map(|io| io.guard_owned(path.as_str().len() as u64))
                .transpose()?;
            crate::store::operations::one_selected_record(
                &self.stream,
                acyclic_stream::ReadRequest {
                    key: Some(acyclic_stream::ReadKeySeek::exact(key.clone(), u64::MAX)),
                    path,
                    from: 0,
                    limit: 1,
                    at_least: Default::default(),
                },
                &mut work,
            )
            .await?
            .map(|record| record.sequence)
        };
        let Some(sequence) = sequence else {
            return Ok(None);
        };
        let selected = self.replay_page(operation, sequence, 1, true, io).await?;
        let Some((record, Some(ModelEvent::ToolCall(call)))) = selected.into_iter().next() else {
            return Err(Error::Storage(
                "indexed model call has no original invocation".into(),
            ));
        };
        if call.call_id != call_id
            || !matches!(record.event,
            ExecutionEvent::Model { step: original, purpose: crate::executor::ModelPurpose::Response, .. }
            | ExecutionEvent::ModelInline { step: original, purpose: crate::executor::ModelPurpose::Response, .. }
            if original == step)
        {
            return Err(Error::Storage(
                "indexed model call differs from its original binding".into(),
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let work = io.and_then(PreparationIo::original_native_work);
        #[cfg(target_arch = "wasm32")]
        let work = None;
        crate::tool::ToolInvocation::for_model_call(
            operation,
            step,
            call.call_id,
            call.name,
            call.arguments,
            work,
        )
        .map(Some)
    }
}
