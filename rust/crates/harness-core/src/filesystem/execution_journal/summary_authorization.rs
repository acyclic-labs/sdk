use super::*;

impl PreparationRecord for crate::context::ContextSummary {}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    async fn verify_summary_owner(&self, io: Option<&PreparationIo>) -> Result<()> {
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
        Ok(())
    }

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
            self.verify_summary_owner(io).await?;
            // Only the original harness upload receipt authenticates this
            // provenance. A file at the same mutable Head path is insufficient.
            let Some((provenance, Some(bytes))) = self
                .host
                .content_for_operation_record_io(
                    super::super::PublicationLookup {
                        volume: &self.volume,
                        key: &retry,
                        expected_path: Some(&path),
                        maximum_bytes: self.maximum_payload_bytes,
                        internal_class: Some(InternalContentClass::Execution),
                        purpose: super::super::PublicationPurpose::Content,
                        scope: &self.scope,
                        ceiling: None,
                    },
                    io,
                )
                .await?
            else {
                return Ok(false);
            };
            if provenance.descriptor().media_type() != "application/json" {
                return Ok(false);
            }
            let allowed = decode_summary(&bytes, operation, file, io)?;
            drop(bytes);
            drop(provenance);
            self.verify_summary_owner(io).await?;
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

fn decode_summary(
    bytes: &[u8],
    operation: OperationId,
    file: &FileRef,
    io: Option<&PreparationIo>,
) -> Result<bool> {
    let allowed = match io {
        Some(io) => {
            let (summary, hold) = io.decode(
                std::marker::PhantomData::<crate::context::ContextSummary>,
                bytes,
                crate::wire::ErrorCode::Invalid,
            )?;
            let allowed = payload_matches::<true>(&summary, operation, file);
            drop(summary);
            drop(hold);
            allowed
        }
        None => {
            let summary: crate::context::ContextSummary =
                crate::contract::json_from_slice(bytes)
                    .map_err(|error| Error::Storage(error.to_string()))?;
            payload_matches::<true>(&summary, operation, file)
        }
    };
    Ok(allowed)
}
