use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Fences this workspace's mutable head for an exact publication operation.
    pub async fn reserve_publication<W: crate::facade::FsWork>(&self, operation_id: OperationId, work: W, cancellation: &CancellationToken) -> W::Output<crate::PublicationReservation, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            accounting.check()?;
            let authority = self.volume.fs.authority();
            let authority_id = self.authority_id();
            let reservation = async {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = accounting.native_work() {
                    let mut authority_work = crate::AuthorityWork::native(original, cancellation);
                    let head = authority.head(authority_id, &mut authority_work).await.map_err(|error| WorkspaceError::from(FsError::Authority(error)))?;
                    return authority.reserve_publication(authority_id, head, operation_id, &mut authority_work).await.and_then(|value| match value {
                        crate::ReservationOutcome::Reserved(proof) | crate::ReservationOutcome::AlreadyReserved(proof) => Ok(proof),
                        crate::ReservationOutcome::Conflict { .. } => Err(crate::AuthorityStoreError::RejectedStatic("publication reservation head or gate changed")),
                    }).map_err(|error| WorkspaceError::from(FsError::Authority(error)));
                }
                let mut authority_work = crate::AuthorityWork::new(accounting.remaining()?, cancellation);
                let head = authority.head(authority_id, &mut authority_work).await;
                let head = authority_work.receipt(head)
                    .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::UNCHARGED, |error| WorkspaceError::from(FsError::Authority(error))));
                let (_, spent, _, _) = accounting.standalone_parts().ok_or(crate::WorkError::Overflow)?;
                let head = merge_workspace_backend(head, spent, 0)?.value;
                let mut authority_work = crate::AuthorityWork::new(accounting.remaining()?, cancellation);
                let reserved = authority.reserve_publication(authority_id, head, operation_id, &mut authority_work).await;
                let reserved = authority_work.receipt(reserved)
                    .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::UNCHARGED, |error| WorkspaceError::from(FsError::Authority(error))));
                let (_, spent, _, _) = accounting.standalone_parts().ok_or(crate::WorkError::Overflow)?;
                match merge_workspace_backend(reserved, spent, 0)?.value {
                    crate::ReservationOutcome::Reserved(reservation) | crate::ReservationOutcome::AlreadyReserved(reservation) => Ok(reservation),
                    crate::ReservationOutcome::Conflict { .. } => Err(WorkspaceError::StaleGeneration),
                }
            }.await?;
            if reservation.authority_id != authority_id || reservation.operation_id != operation_id { return Err(WorkspaceError::IncompatibleWorkspace); }
            Ok(reservation)
        }.await;
        work.finish(result, &accounting)
    }

    /// Releases only this workspace's authenticated reservation.
    pub async fn release_publication<W: crate::facade::FsWork>(&self, reservation: crate::PublicationReservation, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if reservation.authority_id != self.authority_id() { return Err(WorkspaceError::IncompatibleWorkspace); }
            accounting.check()?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                let mut authority_work = crate::AuthorityWork::native(original, cancellation);
                return self.volume.fs.authority().release_publication(reservation, &mut authority_work).await.map_err(|error| WorkspaceError::from(FsError::Authority(error)));
            }
            let mut authority_work = crate::AuthorityWork::new(accounting.remaining()?, cancellation);
            let released = self.volume.fs.authority().release_publication(reservation, &mut authority_work).await;
            let released = authority_work.receipt(released)
                .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::UNCHARGED, |error| WorkspaceError::from(FsError::Authority(error))));
            let (_, spent, _, _) = accounting.standalone_parts().ok_or(crate::WorkError::Overflow)?;
            merge_workspace_backend(released, spent, 0).map(|receipt| receipt.value)
        }.await;
        work.finish(result, &accounting)
    }

    /// Queries the durable reservation for this exact operation.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn publication_reservation_with_native_work(&self, operation_id: OperationId, original: &crate::NativeMaterializationWork, cancellation: &CancellationToken) -> Result<Option<crate::PublicationReservation>, (WorkspaceError, Option<crate::NativeAllocation>)> {
        let result = async {
            original.admit(crate::WorkCounters::UNCHARGED).map_err(WorkspaceError::from)?;
            cancellation.check()?;
            let mut authority_work = crate::AuthorityWork::native(original, cancellation);
            let reservation = self.volume.fs.authority().publication_reservation(self.authority_id(), operation_id, &mut authority_work).await.map_err(|error| WorkspaceError::from(FsError::Authority(error)))?;
            if let Some(proof) = reservation && (proof.authority_id != self.authority_id() || proof.operation_id != operation_id) { return Err(WorkspaceError::IncompatibleWorkspace); }
            Ok(reservation)
        }.await;
        result.map_err(|error| (error, None))
    }

    /// Resolves an immutable operation without adopting the mutable head.
    pub async fn operation_generation<W: crate::facade::FsWork>(&self, key: IdempotencyKey, work: W, cancellation: &CancellationToken) -> W::Output<Option<Generation<A, O>>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.volume.fs.workspace_operation_generation(&self.volume, key.operation_id(), &mut accounting, cancellation).await.map(|mut observed| {
            let id = observed.0.take().map(|(id, record)| { drop(record); id });
            drop(observed);
            id.map(|id| Generation { workspace: self.clone(), id, publication_closure: None })
        });
        work.finish(result, &accounting)
    }

    pub(crate) async fn operation_generation_record<W: crate::facade::FsWork>(&self, key: IdempotencyKey, work: W, cancellation: &CancellationToken) -> W::Output<(Option<(Generation<A, O>, crate::DurableCommit)>, KernelOwnedAllocation), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.volume.fs.workspace_operation_generation(&self.volume, key.operation_id(), &mut accounting, cancellation).await
            .map(|(value, allocation)| (value.map(|(id, record)| (Generation { workspace: self.clone(), id, publication_closure: None }, record)), allocation));
        work.finish(result, &accounting)
    }

    /// Verifies the exact immutable result of this workspace's join operation.
    pub async fn verify_join_commit(&self, witness: &crate::JoinCommitWitness) -> Result<bool, WorkspaceError> {
        self.volume.fs.verify_workspace_join_commit(&self.volume, witness, WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }
}
