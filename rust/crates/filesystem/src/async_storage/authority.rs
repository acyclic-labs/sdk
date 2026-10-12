use super::*;

/// Nonblocking authority-store contract.
pub trait AsyncAuthorityStore: ProviderPlatform {
    /// Whether creation can retain a published-generation prefix.
    fn supports_generation_lineage_prefix(&self) -> bool { false }

    /// Creates an authority, sealing publication and recovery.
    fn create_authority<G: AuthorityGuard>(
        &self, authority: AuthorityId, epoch: Epoch, work: &mut AuthorityWork<'_>, guard: Option<&mut G>,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, AuthorityError<G::Error>>> + ProviderTask;

    /// Creates an authority rooted at one source generation.
    fn fork_authority<G: AuthorityGuard>(
        &self, source: GenerationForkSource, destination: AuthorityId, operation: OperationId,
        work: &mut AuthorityWork<'_>, mut guard: Option<&mut G>,
    ) -> impl Future<Output = Result<CreateAuthorityOutcome, AuthorityError<G::Error>>> + ProviderTask {
        async move {
            if source.lineage == GenerationFork::PublishedPrefix {
                #[cfg(not(target_arch = "wasm32"))]
                if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
                seal(guard.as_deref_mut(), work).await?;
                return Err(crate::AuthorityStoreError::RejectedStatic("authority store does not retain generation lineage prefixes").into());
            }
            let _ = operation;self.create_authority(destination, Epoch::GENESIS, work, guard.as_deref_mut()).await
        }
    }

    /// Creates or authenticates an authority's exact first record.
    fn create_first_record<G: AuthorityGuard>(
        &self, authority: AuthorityId, commit: ProposedCommit, work: &mut AuthorityWork<'_>, mut guard: Option<&mut G>,
    ) -> impl Future<Output = Result<bool, AuthorityError<G::Error>>> + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
            first_record(self, authority, commit, work, guard.as_deref_mut()).await
        }
    }

    /// Publishes retention before a forked workspace's creation.
    fn fork_workspace<G: AuthorityGuard>(
        &self, fork: WorkspaceForkCommit, permit: PublicationPermit, work: &mut AuthorityWork<'_>, mut guard: Option<&mut G>,
    ) -> impl Future<Output = Result<WorkspaceForkOutcome, AuthorityError<G::Error>>> + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() && guard.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported.into()); }
            workspace_fork(self, fork, permit, work, guard.as_deref_mut()).await
        }
    }

    /// Retires authority metadata after workspace deletion is durable.
    fn retire_authority(&self, authority: AuthorityId, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<(), crate::AuthorityStoreError>> + ProviderTask {
        let _ = (authority, work);
        std::future::ready(Ok(()))
    }

    /// Reads the linearizable authority head.
    fn head(&self, authority: AuthorityId, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<Head, crate::AuthorityStoreError>> + ProviderTask;

    /// Compares and appends under the caller's publication permit and guard.
    fn compare_and_append<G: AuthorityGuard>(
        &self, request: GuardedAppend, work: &mut AuthorityWork<'_>, guard: Option<&mut G>,
    ) -> impl Future<Output = Result<(AppendOutcome, AuthorityAllocation), AuthorityError<G::Error>>> + ProviderTask;

    /// Reads an existing reservation for the exact operation identity.
    fn publication_reservation(&self, authority: AuthorityId, operation: OperationId, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<Option<PublicationReservation>, crate::AuthorityStoreError>> + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = work.original { original.admit(WorkCounters::UNCHARGED)?; }
            work.cancellation.check().map_err(|_| crate::AuthorityStoreError::Cancelled)?;
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() { return Err(crate::AuthorityStoreError::NativeOriginalUnsupported); }
            let _ = (authority, operation);
            Err(crate::AuthorityStoreError::RejectedStatic("authority backend cannot query original publication reservation evidence"))
        }
    }

    /// Acquires the exclusive publication gate at an exact head.
    fn reserve_publication(&self, authority: AuthorityId, expected: Head, operation: OperationId, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<ReservationOutcome, crate::AuthorityStoreError>> + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() { work.check()?; return Err(crate::AuthorityStoreError::NativeOriginalUnsupported); }
            let _ = (authority, expected, operation, work);
            Err(crate::AuthorityStoreError::RejectedStatic("authority backend does not support durable publication reservations"))
        }
    }

    /// Releases the exact publication reservation.
    fn release_publication(&self, reservation: PublicationReservation, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<(), crate::AuthorityStoreError>> + ProviderTask {
        async move {
            #[cfg(not(target_arch = "wasm32"))]
            if work.original.is_some() { work.check()?; return Err(crate::AuthorityStoreError::NativeOriginalUnsupported); }
            let _ = (reservation, work);
            Err(crate::AuthorityStoreError::RejectedStatic("authority backend does not support durable publication reservations"))
        }
    }

    /// Replays one bounded page while retaining its native storage.
    fn replay(&self, authority: AuthorityId, after: Sequence, limit: ReplayLimit, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<(Vec<crate::DurableCommit>, AuthorityAllocation), crate::AuthorityStoreError>> + ProviderTask;

    /// Advances a writer fence.
    fn fence(&self, authority: AuthorityId, expected: Head, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<FenceOutcome, crate::AuthorityStoreError>> + ProviderTask;

    /// Resolves an idempotent operation while retaining its native storage.
    fn find_operation(&self, authority: AuthorityId, operation: OperationId, work: &mut AuthorityWork<'_>)
        -> impl Future<Output = Result<(Option<crate::DurableCommit>, AuthorityAllocation), crate::AuthorityStoreError>> + ProviderTask;
}

impl<T: AsyncAuthorityStore + ?Sized> AsyncAuthorityStore for Arc<T> {
    fn supports_generation_lineage_prefix(&self) -> bool {
        self.as_ref().supports_generation_lineage_prefix()
    }

    async fn create_authority<G: AuthorityGuard>(&self, authority: AuthorityId, epoch: Epoch, work: &mut AuthorityWork<'_>, guard: Option<&mut G>) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {AsyncAuthorityStore::create_authority(self.as_ref(), authority, epoch, work, guard).await
    }
    async fn fork_authority<G: AuthorityGuard>(&self, source: GenerationForkSource, destination: AuthorityId, operation: OperationId, work: &mut AuthorityWork<'_>, guard: Option<&mut G>) -> Result<CreateAuthorityOutcome, AuthorityError<G::Error>> {
        AsyncAuthorityStore::fork_authority(self.as_ref(), source, destination, operation, work, guard).await
    }
    async fn create_first_record<G: AuthorityGuard>(&self, authority: AuthorityId, commit: ProposedCommit, work: &mut AuthorityWork<'_>, guard: Option<&mut G>) -> Result<bool, AuthorityError<G::Error>> {
        AsyncAuthorityStore::create_first_record(self.as_ref(), authority, commit, work, guard).await
    }
    async fn fork_workspace<G: AuthorityGuard>(&self, fork: WorkspaceForkCommit, permit: PublicationPermit, work: &mut AuthorityWork<'_>, guard: Option<&mut G>) -> Result<WorkspaceForkOutcome, AuthorityError<G::Error>> {
        AsyncAuthorityStore::fork_workspace(self.as_ref(), fork, permit, work, guard).await
    }
    async fn compare_and_append<G: AuthorityGuard>(&self, request: GuardedAppend, work: &mut AuthorityWork<'_>, guard: Option<&mut G>) -> Result<(AppendOutcome, AuthorityAllocation), AuthorityError<G::Error>> {
        AsyncAuthorityStore::compare_and_append(self.as_ref(), request, work, guard).await
    }
    async fn head(&self, authority: AuthorityId, work: &mut AuthorityWork<'_>) -> Result<Head, crate::AuthorityStoreError> {
        AsyncAuthorityStore::head(self.as_ref(), authority, work).await
    }
    async fn retire_authority(&self, authority: AuthorityId, work: &mut AuthorityWork<'_>) -> Result<(), crate::AuthorityStoreError> {
        AsyncAuthorityStore::retire_authority(self.as_ref(), authority, work).await
    }
    async fn replay(&self, authority: AuthorityId, after: Sequence, limit: ReplayLimit, work: &mut AuthorityWork<'_>) -> Result<(Vec<crate::DurableCommit>, AuthorityAllocation), crate::AuthorityStoreError> {
        AsyncAuthorityStore::replay(self.as_ref(), authority, after, limit, work).await
    }
    async fn fence(&self, authority: AuthorityId, expected: Head, work: &mut AuthorityWork<'_>) -> Result<FenceOutcome, crate::AuthorityStoreError> {
        AsyncAuthorityStore::fence(self.as_ref(), authority, expected, work).await
    }
    async fn find_operation(&self, authority: AuthorityId, operation: OperationId, work: &mut AuthorityWork<'_>) -> Result<(Option<crate::DurableCommit>, AuthorityAllocation), crate::AuthorityStoreError> {
        AsyncAuthorityStore::find_operation(self.as_ref(), authority, operation, work).await
    }
    async fn publication_reservation(&self, authority: AuthorityId, operation: OperationId, work: &mut AuthorityWork<'_>) -> Result<Option<PublicationReservation>, crate::AuthorityStoreError> {
        AsyncAuthorityStore::publication_reservation(self.as_ref(), authority, operation, work).await
    }
    async fn reserve_publication(&self, authority: AuthorityId, expected: Head, operation: OperationId, work: &mut AuthorityWork<'_>) -> Result<ReservationOutcome, crate::AuthorityStoreError> {
        AsyncAuthorityStore::reserve_publication(self.as_ref(), authority, expected, operation, work).await
    }
    async fn release_publication(&self, reservation: PublicationReservation, work: &mut AuthorityWork<'_>) -> Result<(), crate::AuthorityStoreError> {
        AsyncAuthorityStore::release_publication(self.as_ref(), reservation, work).await
    }
}

