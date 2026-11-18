use super::*;
use crate::executor::{ToolSettlement, ToolSettlementQuery, ToolTerminalOutcome};

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn observe_tool_settlement(
        &self,
        query: ToolSettlementQuery<'_>,
        io: Option<&PreparationIo>,
    ) -> Result<Option<ToolSettlement>> {
        let scope = crate::filesystem::PreparationScope::new(io)?;
        let io = scope.io();
        query.validate(io)?;
        let Some((state, _)) = self.load_index_checkpoint(query.turn, io).await? else {
            return Ok(None);
        };
        let path = self.index_path(query.turn, "pages", io)?;
        let reader = persistent_index::Reader {
            provider: &self.stream,
            path: &path,
            base: state.nodes_tail,
            staged: &[],
            io,
            cache: None,
        };
        let call = crate::model::hash_bytes(query.call_id.as_bytes(), io)?;
        let Some(entry) =
            step_index::tool_entry(&reader, state.roots[4], state.tail, query.step, call).await?
        else {
            return Ok(None);
        };
        if !entry.settled {
            return Ok(None);
        }
        let outcome = self
            .verify_terminal_record(query, entry.sequence, entry.digest, entry.retry, io)
            .await?;
        // The call's one step-index chain admits one terminal outcome. The
        // signed checkpoint authenticates that outcome on every replay.
        Ok(Some(ToolSettlement {
            cut: state.tail,
            sequence: entry.sequence,
            outcome,
        }))
    }

    async fn verify_terminal_record(
        &self,
        query: ToolSettlementQuery<'_>,
        sequence: u64,
        digest: [u8; 32],
        retry: [u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<ToolTerminalOutcome> {
        let path_hold;
        let path = crate::executor::with_history_io(io, |work| {
            crate::distributed::execution_path(query.turn, work)
        })?;
        path_hold = io
            .filter(|io| io.original_native_work().is_none())
            .map(|io| io.guard_owned(path.as_str().len() as u64))
            .transpose()?;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        let record_hold;
        let record = crate::store::operations::one_record(
            self.stream.as_ref(),
            &path,
            sequence - 1,
            &mut work,
        )
        .await?
        .ok_or_else(|| invalid(io))?;
        record_hold = io
            .map(|io| {
                io.guard_owned(crate::store::operations::sdk_record_retained_bytes(
                    &record, &work,
                ))
            })
            .transpose()?;
        let observation = observations::decode_observation(&record.value, io)?;
        let outcome = match &observation.event {
            ExecutionEvent::ToolCompleted { step, call_id, .. }
                if *step == query.step && call_id == query.call_id =>
            {
                ToolTerminalOutcome::Completed
            }
            ExecutionEvent::ToolFailed { step, call_id, .. }
                if *step == query.step && call_id == query.call_id =>
            {
                ToolTerminalOutcome::Failed
            }
            _ => return Err(invalid(io)),
        };
        let actual_digest = crate::executor::with_history_io(io, |work| {
            crate::executor::json::canonical_json_digest(
                &observation.event,
                Some(acyclic_stream::MAX_RECORD_BYTES as u64),
                work,
            )
        })?;
        let observed_retry = crate::model::hash_bytes(observation.retry_digest.as_bytes(), io)?;
        if record.sequence != sequence - 1
            || observation.operation_id != query.turn
            || observation.turn_operation != self.turn_operation(query.turn)
            || observed_retry != retry
            || digest != actual_digest
        {
            return Err(invalid(io));
        }
        drop(observation);
        drop(record);
        drop(record_hold);
        drop(path);
        drop(path_hold);
        Ok(outcome)
    }
}

fn invalid(io: Option<&PreparationIo>) -> Error {
    canonical_boundary(
        io,
        crate::wire::ErrorCode::Storage,
        "tool terminal record differs from its authenticated journal index",
    )
}

#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
mod tests;
