use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {


    /// Terminally deletes one named workspace without requiring a live handle.
    pub async fn delete_workspace(
        &self, name: impl AsRef<str>, idempotency_key: crate::IdempotencyKey,
    ) -> Result<crate::workspace::WorkspaceDelete, WorkspaceError> {
        let span = crate::obs::span!(
            INFO, "acyclic.fs.delete_workspace", outcome = crate::obs::Empty, error.kind = crate::obs::Empty
        );
        let result = crate::obs::in_span(&span, async move {
            let (_, id) = self.workspace_name_identity(name, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|failure| failure.error)?.value;
            let volume = match self.open_volume(
                    id.volume_id(), WorkBudget::UNBOUNDED, &CancellationToken::new(),
                ).await
            {
                Ok(volume) => volume.value, Err(failure) if matches!(failure.error, FsError::WorkspaceDeleted) => {
                    return Ok(crate::workspace::WorkspaceDelete::AlreadyDeleted);
                }
                Err(failure) => return Err(WorkspaceError::engine(failure)),
            };
            self.delete_workspace_volume(&volume, idempotency_key.operation_id()).await
        }).await;
        crate::obs::outcome_on(&span, result)
    }


    pub(crate) async fn delete_workspace_volume(
        &self,
        volume: &Volume<A, O>,
        operation_id: OperationId,
    ) -> Result<crate::workspace::WorkspaceDelete, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let authority_id = volume_authority_id(volume.id);
        let head = match {
            let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let result = self.inner.authority.head(authority_id, &mut authority_work).await;
            authority_work.receipt(result)
        }
        {
            Ok(head) => head.value,
            Err(failure) if matches!(failure.error, AuthorityStoreError::Retired) => {
                return Ok(crate::workspace::WorkspaceDelete::AlreadyDeleted);
            }
            Err(failure) => return Err(WorkspaceError::engine(failure)),
        };
        if head.sequence == Sequence::GENESIS { return Err(WorkspaceError::engine(FsError::EmptyAuthority)); }
        let latest = {
            let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let result = self.inner.authority.replay(authority_id, Sequence::new(head.sequence.get().saturating_sub(1)), ReplayLimit {
                            records: 1,
                            payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES,
                        }, &mut authority_work).await.map(|(value, _allocation)| value);
            authority_work.receipt(result)
        }
            .map_err(WorkspaceError::engine)?;
        let latest = latest.value.first().ok_or_else(|| {
            WorkspaceError::engine(FsError::InvalidAuthorityHistory)
        })?;
        if let Ok(deleted) = decode_workspace_deleted(&latest.payload, MAXIMUM_VOLUME_EVENT_BYTES) {
            if deleted != volume.id {
                return Err(WorkspaceError::engine(
                    FsError::VolumeMismatch,
                ));
            }
            // A retry after the tombstone finishes what the delete began.
            self.retire_deleted_authority(authority_id, &cancellation)
                .await?;
            return Ok(crate::workspace::WorkspaceDelete::AlreadyDeleted);
        }
        generation_from_record(latest, volume.id, &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation))
            .map_err(WorkspaceError::engine)?;
        let encoded = crate::kernel::encode::<FsError>(WorkBudget::UNBOUNDED, Some(&cancellation), 0, |observe| crate::kernel::encode_workspace_deleted(volume.id, observe, FsError::EncodedAllocationFailed))
            .map_err(|failure| WorkspaceError::engine(failure.error))?;
        let encoded = creation_commit(operation_id, encoded, WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|failure| WorkspaceError::engine(failure.error))?;
        let (commit, _) = encoded.value;
        let appended = {
            let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            let result = self.inner.authority.compare_and_append(crate::GuardedAppend { authority_id: authority_id, epoch: head.epoch, expected: head, commit: commit, permit: crate::PublicationPermit::Unrestricted }, &mut authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value);
            authority_work.receipt(result)
        }
            .map_err(WorkspaceError::engine)?;
        Ok(match appended.value {
            AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_) => {
                self.retire_deleted_authority(authority_id, &cancellation)
                    .await?;
                crate::workspace::WorkspaceDelete::Deleted
            }
            AppendOutcome::Conflict { .. } | AppendOutcome::Fenced { .. } => {
                crate::workspace::WorkspaceDelete::Conflict
            }
            AppendOutcome::IdempotencyConflict { .. } => {
                crate::workspace::WorkspaceDelete::IdempotencyConflict
            }
        })
    }


    /// Releases the authority of a workspace whose tombstone is durable:
    pub(super) async fn retire_deleted_authority(
        &self,
        authority_id: crate::AuthorityId,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        {
            let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation);
            let result = self.inner.authority.retire_authority(authority_id, &mut authority_work).await;
            authority_work.receipt(result)
        }
            .map_err(WorkspaceError::engine)?;
        Ok(())
    }
}

