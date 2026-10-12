use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    /// Terminally deletes one named workspace without requiring a live handle.
    pub async fn delete_workspace(
        &self,
        name: impl AsRef<str>,
        idempotency_key: crate::IdempotencyKey,
    ) -> Result<crate::workspace::WorkspaceDelete, WorkspaceError> {
        let result = (async move {
            let (_, id) = self
                .workspace_name_identity(name, WorkBudget::UNBOUNDED, &CancellationToken::new())
                .map_err(|failure| failure.error)?
                .value;
            let volume = match self
                .open_volume(
                    id.volume_id(),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
            {
                Ok(volume) => volume.value,
                Err(failure) if matches!(failure.error, FsError::WorkspaceDeleted) => {
                    return Ok(crate::workspace::WorkspaceDelete::AlreadyDeleted);
                }
                Err(failure) => return Err(WorkspaceError::engine(failure)),
            };
            self.delete_workspace_volume(&volume, idempotency_key.operation_id())
                .await
        })
        .await;
        result
    }

    pub(crate) async fn delete_workspace_volume(
        &self,
        volume: &Volume<A, O>,
        operation_id: OperationId,
    ) -> Result<crate::workspace::WorkspaceDelete, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let authority_id = volume_authority_id(volume.id);
        let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let head = match self.inner.authority.head(authority_id, &mut work).await {
            Ok(head) => head,
            Err(AuthorityStoreError::Retired) => {
                return Ok(crate::workspace::WorkspaceDelete::AlreadyDeleted);
            }
            Err(failure) => return Err(WorkspaceError::engine(failure)),
        };
        if head.sequence == Sequence::GENESIS {
            return Err(WorkspaceError::engine(FsError::EmptyAuthority));
        }
        let latest = self
            .inner
            .authority
            .replay(
                authority_id,
                Sequence::new(head.sequence.get().saturating_sub(1)),
                ReplayLimit {
                    records: 1,
                    payload_bytes: MAXIMUM_VOLUME_EVENT_BYTES,
                },
                &mut work,
            )
            .await
            .map_err(WorkspaceError::engine)?
            .0;
        let latest = latest
            .first()
            .ok_or_else(|| WorkspaceError::engine(FsError::InvalidAuthorityHistory))?;
        if let Ok(deleted) = decode_workspace_deleted(&latest.payload, MAXIMUM_VOLUME_EVENT_BYTES) {
            if deleted != volume.id {
                return Err(WorkspaceError::engine(FsError::VolumeMismatch));
            }
            // A retry after the tombstone finishes what the delete began.
            self.retire_deleted_authority(authority_id, &cancellation)
                .await?;
            return Ok(crate::workspace::WorkspaceDelete::AlreadyDeleted);
        }
        generation_from_record(
            latest,
            volume.id,
            &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation),
        )
        .map_err(WorkspaceError::engine)?;
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let encoded =
            crate::kernel::codec::encode_owned(&mut accounting, |observe, allocation_error| {
                crate::kernel::encode_workspace_deleted(volume.id, observe, allocation_error)
            })
            .map_err(WorkspaceError::engine)?;
        let commit = creation_commit(operation_id, encoded, &mut accounting)
            .map_err(WorkspaceError::engine)?;
        let appended = self
            .inner
            .authority
            .compare_and_append(
                crate::GuardedAppend {
                    authority_id,
                    epoch: head.epoch,
                    expected: head,
                    commit,
                    permit: crate::PublicationPermit::Unrestricted,
                },
                &mut work,
                None::<&mut ()>,
            )
            .await
            .map_err(|error| WorkspaceError::engine(error.into_error(std::convert::identity)))?;
        Ok(match appended {
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

    /// Releases an authority after its durable workspace tombstone.
    pub(super) async fn retire_deleted_authority(
        &self,
        authority_id: crate::AuthorityId,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation);
        self.inner
            .authority
            .retire_authority(authority_id, &mut work)
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(())
    }
}
