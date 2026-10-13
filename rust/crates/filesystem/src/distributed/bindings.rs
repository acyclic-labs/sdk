use super::*;

impl<P: acyclic_stream::StreamProvider> AsyncAuthorityStore for StreamAuthorityStore<P> {
    fn supports_generation_lineage_prefix(&self) -> bool {
        true
    }

    async fn create_authority<G: AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
        let mut child = work.operation()?;
        let result = self.create(authority, epoch, &mut child, guard).await;
        work.record(child.spent())?;
        result
    }

    async fn create_first_record<G: AuthorityGuard>(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<bool, AuthorityError<G::Error>> {
        let mut child = work.operation()?;
        let result = self.first(authority, commit, &mut child, guard).await;
        work.record(child.spent())?;
        result
    }

    async fn fork_workspace<G: AuthorityGuard>(
        &self,
        fork: WorkspaceForkCommit,
        permit: PublicationPermit,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<WorkspaceForkOutcome, AuthorityError<G::Error>> {
        let mut child = work.operation()?;
        let result = self.fork_records(fork, permit, &mut child, guard).await;
        work.record(child.spent())?;
        result
    }

    async fn fork_authority<G: AuthorityGuard>(
        &self,
        source: crate::GenerationForkSource,
        destination: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
        if source.lineage == crate::GenerationFork::Independent {
            return self
                .create_authority(destination, Epoch::GENESIS, work, guard)
                .await;
        }
        let mut child = work.operation()?;
        let result = self
            .fork_generation(source, destination, operation, &mut child, guard)
            .await;
        work.record(child.spent())?;
        result
    }

    async fn compare_and_append<G: AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<FsAppendOutcome, AuthorityError<G::Error>> {
        let mut child = work.operation()?;
        let result = self
            .append(request, &mut child, guard)
            .await
            .and_then(|value| child.finish(value).map_err(Into::into));
        work.record(child.spent())?;
        result
    }

    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        let mut child = work.operation()?;
        let result = self
            .fork_snapshot(authority, &mut child)
            .await
            .and_then(|snapshot| child.finish(snapshot.head));
        work.record(child.spent())?;
        result
    }

    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, AuthorityAllocation), AuthorityStoreError> {
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(AuthorityStoreError::InvalidReplayLimit);
        }
        let mut child = work.operation()?;
        let result = self
            .authority_replay(authority, after, limit, &mut child, ReadKind::Replay)
            .await
            .and_then(|value| child.finish(value));
        work.record(child.spent())?;
        result
    }

    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut AuthorityWork<'_>,
    ) -> Result<FenceOutcome, AuthorityStoreError> {
        let mut child = work.operation()?;
        let result = self
            .authority_fence(authority, expected, &mut child)
            .await
            .and_then(|value| child.finish(value));
        work.record(child.spent())?;
        result
    }

    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        let mut child = work.operation()?;
        let result = self
            .authority_find_operation(authority, operation, &mut child)
            .await
            .and_then(|value| child.finish(value));
        work.record(child.spent())?;
        result
    }

    async fn publication_reservation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<PublicationReservation>, AuthorityStoreError> {
        let mut child = work.operation()?;
        let result = self.reservation(authority, operation, &mut child).await;
        work.record(child.spent())?;
        result
    }

    async fn reserve_publication(
        &self,
        authority: AuthorityId,
        expected: Head,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ReservationOutcome, AuthorityStoreError> {
        let mut child = work.operation()?;
        let result = self
            .reserve(authority, expected, operation, &mut child)
            .await;
        work.record(child.spent())?;
        result
    }

    async fn release_publication(
        &self,
        reservation: PublicationReservation,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let mut child = work.operation()?;
        let result = self.release(reservation, &mut child).await;
        work.record(child.spent())?;
        result
    }

    async fn retire_authority(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.original.is_some() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported);
        }
        work.cancellation
            .check()
            .map_err(|_| AuthorityStoreError::Cancelled)?;
        let root = stream_path(authority, "")?;
        let path = stream_path(authority, "/retirement")?;
        work.record(authority_read_work(1))?;
        work.remaining()?;
        match self.provider.tail(root).await {
            Err(acyclic_stream::StreamError::NotFound) => return Ok(()),
            Ok(_) => {}
            Err(error) => return Err(map_stream_error(error)),
        }
        work.record(WorkCounters {
            authority_records_appended: 1,
            authority_bytes_written: AUTHORITY_RETIRED.len() as u64,
            backend_write_operations: 1,
            durability_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        work.remaining()?;
        let request = acyclic_stream::AppendRequest {
            path,
            records: vec![acyclic_stream::OwnedBytes::from_static(AUTHORITY_RETIRED)],
            if_tail: Some(0),
            idempotency_key: Some(stream_key(b"retire-authority", &authority.into_bytes())?),
        };
        match self.provider.append(request).await {
            Ok(acyclic_stream::AppendOutcome::Committed(_)) => Ok(()),
            Ok(acyclic_stream::AppendOutcome::TailConflict { .. }) => {
                work.record(authority_read_work(2))?;
                work.remaining()?;
                if self.is_retired(authority).await? {
                    Ok(())
                } else {
                    Err(AuthorityStoreError::CorruptStatic(
                        "filesystem retirement marker conflicted without retirement",
                    ))
                }
            }
            Err(error) => Err(map_stream_error(error)),
        }
    }
}
