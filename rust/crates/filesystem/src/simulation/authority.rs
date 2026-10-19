use super::*;

fn select_authority_fault(
    state: &Mutex<State>,
    operation: SimulationOperation,
    work: &mut crate::AuthorityWork<'_>,
) -> Result<Option<SimulationFault>, AuthorityStoreError> {
    work.check()?;
    let mut state = state
        .lock()
        .map_err(|_| AuthorityStoreError::CorruptStatic("simulation state is poisoned"))?;
    if work.is_native() {
        state
            .prepare_trace(&mut work.accounting())
            .map_err(crate::async_storage::authority_error)?;
    }
    let fault = state
        .intercept(operation)
        .map_err(AuthorityStoreError::RejectedStatic)?;
    match fault {
        Some(SimulationFault::RejectBefore) => {
            Err(AuthorityStoreError::RejectedStatic("scheduled rejection"))
        }
        Some(SimulationFault::PartitionBefore) => {
            Err(AuthorityStoreError::RejectedStatic("scheduled partition"))
        }
        _ => Ok(fault),
    }
}

fn lost_authority_acknowledgement(operation: &'static str) -> AuthorityStoreError {
    AuthorityStoreError::Indeterminate {
        operation,
        source: std::io::ErrorKind::ConnectionReset.into(),
    }
}

impl<A: AsyncAuthorityStore> AsyncAuthorityStore for SimulatedStore<A> {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.inner.operation_claims()
    }

    async fn create_first_record<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        work: &mut crate::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<bool, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_native() && guard.is_some() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        let fault =
            select_authority_fault(&self.state, SimulationOperation::AuthorityCreate, work)?;
        let value = self
            .inner
            .create_first_record(authority, commit, work, guard)
            .await?;
        if value && matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend)) {
            return Err(
                lost_authority_acknowledgement("create-authority-with-first-record").into(),
            );
        }
        Ok(value)
    }

    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut crate::AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<AppendOutcome, crate::AuthorityError<G::Error>> {
        let fault =
            select_authority_fault(&self.state, SimulationOperation::AuthorityAppend, work)?;
        if matches!(fault, Some(SimulationFault::FenceBeforeAuthorityAppend)) {
            self.inner
                .fence(request.authority_id, request.expected, work)
                .await?;
        }
        let value = self.inner.compare_and_append(request, work, guard).await?;
        if matches!(
            &value,
            AppendOutcome::Committed(_) | AppendOutcome::AlreadyCommitted(_)
        ) && matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend))
        {
            return Err(lost_authority_acknowledgement("compare-and-append").into());
        }
        Ok(value)
    }

    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_native() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        select_authority_fault(&self.state, SimulationOperation::AuthorityCreate, work)?;
        let value = self
            .inner
            .create_authority(authority, epoch, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })?;
        if matches!(value, CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard, work).await?;
        }
        Ok(value)
    }

    async fn fork_authority<G: crate::AuthorityGuard>(
        &self,
        source: GenerationForkSource,
        destination: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_native() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported.into());
        }
        crate::async_storage::seal(guard.as_deref_mut(), work).await?;
        select_authority_fault(&self.state, SimulationOperation::AuthorityCreate, work)?;
        let value = self
            .inner
            .fork_authority(source, destination, operation, work, None::<&mut ()>)
            .await
            .map_err(|error| {
                crate::AuthorityError::Authority(error.into_error(std::convert::identity))
            })?;
        if matches!(value, CreateAuthorityOutcome::Existing(_)) {
            crate::async_storage::seal(guard, work).await?;
        }
        Ok(value)
    }

    async fn publication_reservation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<PublicationReservation>, AuthorityStoreError> {
        work.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_native() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported);
        }
        select_authority_fault(&self.state, SimulationOperation::AuthorityReservation, work)?;
        self.inner
            .publication_reservation(authority, operation, work)
            .await
    }

    async fn reserve_publication(
        &self,
        authority: AuthorityId,
        expected: Head,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ReservationOutcome, AuthorityStoreError> {
        work.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_native() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported);
        }
        let fault =
            select_authority_fault(&self.state, SimulationOperation::AuthorityReserve, work)?;
        let value = self
            .inner
            .reserve_publication(authority, expected, operation, work)
            .await?;
        if matches!(
            value,
            ReservationOutcome::Reserved(_) | ReservationOutcome::AlreadyReserved(_)
        ) && matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend))
        {
            return Err(lost_authority_acknowledgement("reserve-publication"));
        }
        Ok(value)
    }

    async fn release_publication(
        &self,
        reservation: PublicationReservation,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        work.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if work.is_native() {
            return Err(AuthorityStoreError::NativeOriginalUnsupported);
        }
        let fault =
            select_authority_fault(&self.state, SimulationOperation::AuthorityRelease, work)?;
        self.inner.release_publication(reservation, work).await?;
        if matches!(fault, Some(SimulationFault::AmbiguousAuthorityAppend)) {
            return Err(lost_authority_acknowledgement("release-publication"));
        }
        Ok(())
    }

    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        let fault = select_authority_fault(&self.state, SimulationOperation::AuthorityHead, work)?;
        if let Some(SimulationFault::StaleAuthorityHead(head)) = fault {
            if !work.is_native() {
                let delta = WorkCounters {
                    authority_records_read: 1,
                    ..WorkCounters::UNCHARGED
                };
                work.admit(delta)?;
                work.charge(delta)?;
            }
            return Ok(head);
        }
        self.inner.head(authority, work).await
    }

    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::FenceOutcome, AuthorityStoreError> {
        select_authority_fault(&self.state, SimulationOperation::AuthorityFence, work)?;
        self.inner.fence(authority, expected, work).await
    }

    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        select_authority_fault(
            &self.state,
            SimulationOperation::AuthorityFindOperation,
            work,
        )?;
        self.inner.find_operation(authority, operation, work).await
    }

    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {
        let fault =
            select_authority_fault(&self.state, SimulationOperation::AuthorityReplay, work)?;
        let (mut records, allocation) = self.inner.replay(authority, after, limit, work).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut allocation = allocation;
        if matches!(fault, Some(SimulationFault::DuplicateAuthorityReplay)) && !records.is_empty() {
            let growth = records.len() == records.capacity();
            let count = records
                .len()
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?;
            let copied = (if growth { count } else { 1 })
                .checked_mul(std::mem::size_of::<DurableCommit>())
                .ok_or(crate::WorkError::Overflow)? as u64;
            let delta = WorkCounters {
                bytes_copied: copied,
                items_returned: 1,
                ..WorkCounters::UNCHARGED
            };
            work.admit(delta)?;
            if growth {
                let _old_bytes = records
                    .capacity()
                    .checked_mul(std::mem::size_of::<DurableCommit>())
                    .ok_or(crate::WorkError::Overflow)? as u64;
                let (mut replacement, mut _replacement_allocation) = work.vec_owned(count)?;
                replacement.append(&mut records);
                drop(records);
                if let Err(error) = work.charge(WorkCounters {
                    bytes_copied: copied - std::mem::size_of::<DurableCommit>() as u64,
                    ..WorkCounters::UNCHARGED
                }) {
                    drop(replacement);
                    return Err(error);
                }
                #[cfg(not(target_arch = "wasm32"))]
                if let (Some(held), Some(new)) = (&mut allocation, &mut _replacement_allocation)
                    && let Err(error) = held.release(_old_bytes).and_then(|()| held.absorb(new))
                {
                    drop(replacement);
                    return Err(error.into());
                }
                records = replacement;
            }
            let last = records
                .last()
                .ok_or(AuthorityStoreError::CorruptStatic("empty replay page"))?
                .clone();
            records.push(last);
            work.charge(WorkCounters {
                bytes_copied: std::mem::size_of::<DurableCommit>() as u64,
                items_returned: 1,
                ..WorkCounters::UNCHARGED
            })?;
        }
        Ok((records, allocation))
    }
}
