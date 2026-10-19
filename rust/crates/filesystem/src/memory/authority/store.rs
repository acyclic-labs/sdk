use super::*;
use crate::async_storage::{AsyncAuthorityStore, authority_error, seal};

impl AsyncAuthorityStore for MemoryAuthorityStore {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        &self.operation_claims
    }

    async fn fork_workspace<G: crate::AuthorityGuard>(
        &self,
        fork: crate::WorkspaceForkCommit,
        permit: crate::PublicationPermit,
        work: &mut AuthorityWork<'_>,
        guard: Option<&mut G>,
    ) -> Result<crate::WorkspaceForkOutcome, crate::AuthorityError<G::Error>> {
        crate::async_storage::workspace_fork(self, fork, permit, work, guard).await
    }

    async fn publication_reservation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<crate::PublicationReservation>, AuthorityStoreError> {
        self.lookup_reservation(authority, operation, &mut work.accounting())
            .map_err(authority_error)
    }
    async fn reserve_publication(
        &self,
        authority: AuthorityId,
        expected: Head,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<crate::ReservationOutcome, AuthorityStoreError> {
        self.acquire_reservation(authority, expected, operation, &mut work.accounting())
            .map_err(authority_error)
    }
    async fn release_publication(
        &self,
        proof: crate::PublicationReservation,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        self.drop_reservation(proof, &mut work.accounting())
            .map_err(authority_error)
    }

    async fn create_authority<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        epoch: Epoch,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<CreateAuthorityOutcome, crate::AuthorityError<G::Error>> {
        let native = work.is_native();
        if !native {
            seal(guard.as_deref_mut(), work).await?;
        }
        loop {
            let plan = self
                .plan_create(authority, &mut work.accounting())
                .map_err(authority_error)?;
            if native {
                seal(guard.as_deref_mut(), work).await?;
            }
            if let Some(value) = self
                .publish_create(authority, epoch, plan, &mut work.accounting())
                .map_err(authority_error)?
            {
                if !native && matches!(value, CreateAuthorityOutcome::Existing(_)) {
                    seal(guard.as_deref_mut(), work).await?;
                }
                return Ok(value);
            }
        }
    }

    async fn create_first_record<G: crate::AuthorityGuard>(
        &self,
        authority: AuthorityId,
        commit: ProposedCommit,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<bool, crate::AuthorityError<G::Error>> {
        loop {
            let plan = self
                .plan_first_record(authority, &commit, &mut work.accounting())
                .map_err(authority_error)?;
            if work.is_native() {
                seal(guard.as_deref_mut(), work).await?;
            } else if guard.is_some() && !matches!(plan, FirstRecordPlan::Resolved(false)) {
                // Owner allocations overlap the prepared first record.
                let live = work.allocations.live_bytes();
                let mut child = work.operation()?;
                let result = seal(guard.as_deref_mut(), &mut child).await;
                let mut spent = child.spent();
                spent.peak_allocation_bytes = if result.is_err() {
                    live.saturating_add(spent.peak_allocation_bytes)
                } else {
                    live.checked_add(spent.peak_allocation_bytes)
                        .ok_or(AuthorityStoreError::from(WorkError::Overflow))?
                };
                work.record(spent)?;
                result?;
                work.check()?;
            }
            if let Some(value) = self
                .publish_first_record(authority, &commit, plan, &mut work.accounting())
                .map_err(authority_error)?
            {
                return Ok(value);
            }
        }
    }

    async fn compare_and_append<G: crate::AuthorityGuard>(
        &self,
        request: crate::GuardedAppend,
        work: &mut AuthorityWork<'_>,
        mut guard: Option<&mut G>,
    ) -> Result<AppendOutcome, crate::AuthorityError<G::Error>> {
        let native = work.is_native();
        if !native {
            seal(guard.as_deref_mut(), work).await?;
        }
        let plan = self
            .plan_append(&request, &mut work.accounting())
            .map_err(authority_error)?;
        if matches!(
            plan,
            AppendPlan::Resolved(AppendOutcome::Conflict { .. } | AppendOutcome::Fenced { .. })
        ) {
            work.check()?;
        } else if native {
            seal(guard.as_deref_mut(), work).await?;
        }
        self.publish_append(&request, plan, &mut work.accounting())
            .map_err(authority_error)
            .map_err(Into::into)
    }

    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        self.read_head(authority, &mut work.accounting())
            .map_err(authority_error)
    }
    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, crate::AuthorityAllocation), AuthorityStoreError> {
        let (page, custody) = self
            .read_page(authority, after, limit, &mut work.accounting())
            .map_err(authority_error)?;
        Ok((page, allocation(custody)))
    }
    async fn fence(
        &self,
        authority: AuthorityId,
        expected: Head,
        work: &mut AuthorityWork<'_>,
    ) -> Result<crate::FenceOutcome, AuthorityStoreError> {
        self.advance_fence(authority, expected, &mut work.accounting())
            .map_err(authority_error)
    }
    async fn find_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        self.lookup_operation(authority, operation, &mut work.accounting())
            .map_err(authority_error)
    }
}
