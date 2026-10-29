use super::*;
use crate::executor::{
    ToolSettlement, ToolSettlementQuery, ToolTerminalOutcome, tool_settlement_key,
};

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider + ?Sized,
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
        let mut terminal = None;
        for outcome in [ToolTerminalOutcome::Completed, ToolTerminalOutcome::Failed] {
            let key_hold;
            let key = tool_settlement_key(query.step, query.call_id, outcome, io)?;
            key_hold = io
                .map(|io| io.guard_owned(key.capacity() as u64))
                .transpose()?;
            let digest = content::operation_digest(query.turn, &key, io)?;
            if let Some(io) = io {
                io.charge(acyclic_fs::WorkCounters {
                    bytes_copied: 64,
                    ..Default::default()
                })?;
            }
            let retry = digest.to_hex();
            let retry_key = crate::model::hash_bytes(retry.as_bytes(), io)?;
            let entry = checkpoint_maps::retry_entry(&reader, state, retry_key).await?;
            if let Some((sequence, digest)) = entry {
                if terminal.is_some() {
                    return Err(invalid(io));
                }
                self.verify_terminal_record(query, outcome, sequence, digest, retry.as_str(), io)
                    .await?;
                terminal = Some(ToolSettlement {
                    cut: state.tail,
                    sequence,
                    outcome,
                });
            }
            drop(key);
            drop(key_hold);
        }
        Ok(terminal)
    }

    async fn verify_terminal_record(
        &self,
        query: ToolSettlementQuery<'_>,
        outcome: ToolTerminalOutcome,
        sequence: u64,
        digest: [u8; 32],
        retry: &str,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let path_hold;
        let path = crate::executor::with_history_io(io, |work| {
            crate::distributed::execution_path(query.turn, work)
        })?;
        path_hold = io
            .map(|io| io.guard_owned(path.as_str().len() as u64))
            .transpose()?;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        let record_hold;
        let record =
            crate::store::operations::one_record(&self.stream, &path, sequence - 1, &mut work)
                .await?
                .ok_or_else(|| invalid(io))?;
        record_hold = io
            .map(|io| {
                io.guard_owned(crate::store::operations::sdk_record_retained_bytes(
                    &record, &work,
                ))
            })
            .transpose()?;
        let observation: Observation = crate::filesystem::decode_record(&record.value, io)?;
        let valid_event = match (&observation.event, outcome) {
            (
                ExecutionEvent::ToolCompleted { step, call_id, .. },
                ToolTerminalOutcome::Completed,
            )
            | (ExecutionEvent::ToolFailed { step, call_id, .. }, ToolTerminalOutcome::Failed) => {
                *step == query.step && call_id == query.call_id
            }
            _ => false,
        };
        let actual_digest = crate::executor::with_history_io(io, |work| {
            crate::executor::json::canonical_json_digest(
                &observation.event,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )
        })?;
        if record.sequence != sequence - 1
            || observation.version != OBSERVATION_VERSION
            || observation.operation_id != query.turn
            || observation.turn_operation != self.turn_operation(query.turn)
            || observation.retry_digest != retry
            || digest != actual_digest
            || !valid_event
        {
            return Err(invalid(io));
        }
        drop(observation);
        drop(record);
        drop(record_hold);
        drop(path);
        drop(path_hold);
        Ok(())
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
