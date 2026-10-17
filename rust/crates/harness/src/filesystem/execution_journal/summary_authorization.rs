use super::*;

impl PreparationRecord for crate::context::ContextSummary {}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn authenticate_summary_with_io(
        &self,
        file: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let result = async {
            if file.volume() != &self.volume || file.descriptor().media_type() != "text/plain" {
                return Ok(false);
            }
            if let Some(io) = io {
                io.charge(acyclic_fs::WorkCounters {
                    items_examined: file.path().len() as u64,
                    ..Default::default()
                })?;
            }
            let Some(operation) = crate::executor::summary_identity::summary_operation(file.path())
            else {
                return Ok(false);
            };
            if self.execution.is_some_and(|bound| bound.0 != operation) {
                return Ok(false);
            }
            if let Some((owner, bound)) = &self.owner {
                let Some((execution, turn)) = self.execution else {
                    return Ok(false);
                };
                if *bound != execution
                    || execution
                        != super::super::task_runtime::execution_operation(
                            owner.task_id(),
                            turn,
                            io,
                        )?
                {
                    return Ok(false);
                }
            }
            crate::executor::with_history_io(io, |work| {
                ContentGrant::authorize_volume(
                    &self.verifier,
                    &self.scope,
                    &self.volume,
                    VolumeOperation::Read,
                    self.maximum_payload_bytes,
                    work,
                )
            })?;
            let key = crate::executor::with_history_io(io, |work| file.read_capability(work))?;
            let key_hold = io
                .map(|io| io.guard_owned(key.capacity() as u64))
                .transpose()?;
            let (retry, path) = self.stage_identity_with_io(operation, &key, io)?;
            let path_hold = io
                .map(|io| io.guard_owned((retry.as_str().len() + path.capacity()) as u64))
                .transpose()?;
            if let Some((owner, _)) = &self.owner {
                owner
                    .verify_read_access(
                        &mut io
                            .map(PreparationIo::history_io)
                            .as_mut()
                            .map(|work| work as &mut dyn crate::store::HistoryIo),
                    )
                    .await?;
            }
            // Only the original harness upload receipt authenticates this
            // provenance. A file at the same mutable Head path is insufficient.
            let Some((provenance, bytes)) = self
                .host
                .content_for_operation_record_io(
                    &self.volume,
                    &retry,
                    Some(&path),
                    self.maximum_payload_bytes,
                    Some(InternalContentClass::Execution),
                    io,
                )
                .await?
            else {
                return Ok(false);
            };
            if provenance.descriptor().media_type() != "application/json" {
                return Ok(false);
            }
            let allowed = match io {
                Some(io) => {
                    let (summary, hold) = io.decode(
                        std::marker::PhantomData::<crate::context::ContextSummary>,
                        &bytes,
                        crate::wire::ErrorCode::Invalid,
                    )?;
                    let allowed = payload_matches::<true>(&summary, operation, file);
                    drop(summary);
                    drop(hold);
                    allowed
                }
                None => {
                    let summary: crate::context::ContextSummary =
                        crate::contract::json_from_slice(&bytes)
                            .map_err(|error| Error::Storage(error.to_string()))?;
                    payload_matches::<true>(&summary, operation, file)
                }
            };
            drop(bytes);
            drop(provenance);
            if let Some((owner, _)) = &self.owner {
                owner
                    .verify_read_access(
                        &mut io
                            .map(PreparationIo::history_io)
                            .as_mut()
                            .map(|work| work as &mut dyn crate::store::HistoryIo),
                    )
                    .await?;
            }
            drop(retry);
            drop(path);
            drop(path_hold);
            drop(key);
            drop(key_hold);
            Ok(allowed)
        }
        .await;
        PreparationIo::release_since(io, Some(before))?;
        result
    }
}

// The test-only false instantiation is an assertion-deletion control over the
// same predicate; live authorization always checks the complete output ref.
pub(super) fn payload_matches<const CHECK_OUTPUT: bool>(
    summary: &crate::context::ContextSummary,
    operation: OperationId,
    file: &FileRef,
) -> bool {
    summary.operation_id == operation
        && summary.source_messages > 0
        && (!CHECK_OUTPUT || summary.output == *file)
}
