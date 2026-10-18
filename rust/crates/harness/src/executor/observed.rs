use super::*;

/// The latest version of each path that a completed tool result showed the
/// model, folded once over the turn's journal. Each record is selected, each
/// invocation loaded and each completion paired exactly once; later calls
/// continue after `after`. The turn owns this state and lends it exclusively
/// across each call, so neither locking nor concurrent fold replacement exists.
#[derive(Default)]
pub(in crate::executor) struct ObservedFold {
    operation: Option<OperationId>,
    after: u64,
    /// Started calls awaiting their completion or failure.
    pending: Vec<(u32, String, FileRef)>,
    files: Vec<FileRef>,
}

/// Only the reference a file tool result names: the result body is skipped
/// by the decoder, never materialized.
#[derive(Deserialize, Serialize)]
struct ObservedResult {
    value: ObservedValue,
}

#[derive(Deserialize, Serialize)]
struct ObservedValue {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    file: Option<FileRef>,
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ObservedResult {}

impl StockExecution<'_> {
    /// Derives, from this turn's canonical journal alone, the latest version of
    /// each path that a completed tool result showed the model. Execution, a
    /// resumed turn and reconciliation all derive the same set from the same
    /// records, and failed calls never contribute because only
    /// `ToolCompleted` results are read. The fold lives only as long as this
    /// session; a new session folds the journal once from its start.
    pub(super) async fn observed_files(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        fold: &mut ObservedFold,
    ) -> Result<Vec<FileRef>> {
        if fold.operation != Some(operation_id) {
            *fold = ObservedFold {
                operation: Some(operation_id),
                ..ObservedFold::default()
            };
        }
        // Every error ends the turn, dropping its partially advanced fold.
        self.fold_observed(fold, journal, operation_id).await?;
        let mut files = Vec::new();
        for file in &fold.files {
            stock_vec_push(&mut files, file.clone(), self.io)?;
        }
        Ok(files)
    }

    async fn fold_observed(
        &self,
        fold: &mut ObservedFold,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
    ) -> Result<()> {
        let maximum = self
            .stock
            .limits
            .model_steps
            .saturating_mul(self.stock.limits.tool_calls_per_step)
            .saturating_mul(3);
        let (tail, records, _hold) = replay_execution_after(
            journal,
            operation_id,
            fold.after,
            maximum,
            |event| {
                matches!(
                    event,
                    ExecutionEvent::ToolStarted { .. }
                        | ExecutionEvent::ToolCompleted { .. }
                        | ExecutionEvent::ToolFailed { .. }
                )
            },
            self.io,
        )
        .await?;
        if tail < fold.after {
            return Err(stock_boundary(
                self.io,
                crate::wire::ErrorCode::Conflict,
                "execution journal is shorter than its observed fold",
            ));
        }
        for record in records {
            match record.event {
                ExecutionEvent::ToolStarted {
                    step,
                    call_id,
                    invocation,
                } => stock_vec_push(&mut fold.pending, (step, call_id, invocation), self.io)?,
                ExecutionEvent::ToolFailed { step, call_id, .. } => {
                    take_pending(&mut fold.pending, step, &call_id);
                }
                ExecutionEvent::ToolCompleted {
                    step,
                    call_id,
                    result,
                    ..
                } => {
                    let invocation =
                        take_pending(&mut fold.pending, step, &call_id).ok_or_else(|| {
                            stock_boundary(
                                self.io,
                                crate::wire::ErrorCode::Invalid,
                                "completed tool has no admitted invocation",
                            )
                        })?;
                    if let Some(file) = self.observed_file(journal, &invocation, &result).await? {
                        fold.files.retain(|seen| {
                            seen.volume() != file.volume() || seen.path() != file.path()
                        });
                        stock_vec_push(&mut fold.files, file, self.io)?;
                    }
                }
                _ => {}
            }
        }
        fold.after = tail;
        Ok(())
    }

    /// The version a completed call's result names, loading its result only
    /// for tools that can name one (bash, search, ... are skipped unloaded).
    async fn observed_file(
        &self,
        journal: &dyn ExecutionJournal,
        invocation: &FileRef,
        result: &FileRef,
    ) -> Result<Option<FileRef>> {
        let (invocation, _) =
            load_json::<crate::tool::RecordedInvocation>(journal, invocation, self.io).await?;
        let Ok(tool) = stock_tool_lookup(&self.stock.tools, &invocation.invocation.name, self.io)
        else {
            return Ok(None);
        };
        if !tool.executor.reports_observed_files() {
            return Ok(None);
        }
        let bytes = super::payload::load_json_bytes(journal, result, self.io).await?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = self.io {
            let _bytes_hold = io.guard_bytes(&bytes)?;
            let (observed, _hold) = io.decode(
                std::marker::PhantomData::<ObservedResult>,
                &bytes,
                crate::wire::ErrorCode::Storage,
            )?;
            return Ok(observed.value.file);
        }
        let observed: ObservedResult = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Storage(format!("tool result is invalid: {error}")))?;
        Ok(observed.value.file)
    }
}

/// Removes and returns the started invocation of one call. Pending calls are
/// at most those of the steps still in flight, so this never scans history.
fn take_pending(
    pending: &mut Vec<(u32, String, FileRef)>,
    step: u32,
    call_id: &str,
) -> Option<FileRef> {
    let index = pending
        .iter()
        .position(|(started, id, _)| *started == step && id == call_id)?;
    Some(pending.swap_remove(index).2)
}
