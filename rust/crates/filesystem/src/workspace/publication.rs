use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Fences this workspace's mutable head for an exact publication operation.
    pub async fn reserve_publication<W: crate::facade::FsWork>(
        &self,
        operation_id: OperationId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::PublicationReservation, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            accounting.check()?;
            let authority = self.volume.fs.authority();
            let authority_id = self.authority_id();
            let mut authority_work =
                crate::AuthorityWork::new(accounting.remaining()?, cancellation);
            #[cfg(not(target_arch = "wasm32"))]
            {
                authority_work.original = accounting.native_work();
            }
            let reserved = async {
                let head = authority.head(authority_id, &mut authority_work).await?;
                authority_work.remaining()?;
                authority
                    .reserve_publication(authority_id, head, operation_id, &mut authority_work)
                    .await
            }
            .await
            .map_err(|error| WorkspaceError::from(FsError::Authority(error)));
            let reserved = match accounting.standalone_parts() {
                Some((_, spent, _, _)) => {
                    merge_workspace_backend(authority_work.receipt(reserved), spent, 0)?.value
                }
                None => reserved?,
            };
            let reservation = match reserved {
                crate::ReservationOutcome::Reserved(proof)
                | crate::ReservationOutcome::AlreadyReserved(proof) => proof,
                crate::ReservationOutcome::Conflict { .. } => {
                    #[cfg(not(target_arch = "wasm32"))]
                    if accounting.native_work().is_some() {
                        return Err(FsError::Authority(
                            crate::AuthorityStoreError::RejectedStatic(
                                "publication reservation head or gate changed",
                            ),
                        )
                        .into());
                    }
                    return Err(WorkspaceError::StaleGeneration);
                }
            };
            if reservation.authority_id != authority_id || reservation.operation_id != operation_id
            {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            Ok(reservation)
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Releases only this workspace's authenticated reservation.
    pub async fn release_publication<W: crate::facade::FsWork>(
        &self,
        reservation: crate::PublicationReservation,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if reservation.authority_id != self.authority_id() {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            accounting.check()?;
            let mut authority_work =
                crate::AuthorityWork::new(accounting.remaining()?, cancellation);
            #[cfg(not(target_arch = "wasm32"))]
            {
                authority_work.original = accounting.native_work();
            }
            let released = self
                .volume
                .fs
                .authority()
                .release_publication(reservation, &mut authority_work)
                .await
                .map_err(|error| WorkspaceError::from(FsError::Authority(error)));
            match accounting.standalone_parts() {
                Some((_, spent, _, _)) => {
                    merge_workspace_backend(authority_work.receipt(released), spent, 0)
                        .map(|receipt| receipt.value)
                }
                None => released,
            }
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Queries the durable reservation for this exact operation.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn publication_reservation(
        &self,
        operation_id: OperationId,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Option<crate::PublicationReservation>, WorkspaceError> {
        original
            .admit(crate::WorkCounters::UNCHARGED)
            .map_err(WorkspaceError::from)?;
        cancellation.check()?;
        let mut authority_work = crate::AuthorityWork::native(original, cancellation);
        let reservation = self
            .volume
            .fs
            .authority()
            .publication_reservation(self.authority_id(), operation_id, &mut authority_work)
            .await
            .map_err(|error| WorkspaceError::from(FsError::Authority(error)))?;
        if let Some(proof) = reservation
            && (proof.authority_id != self.authority_id() || proof.operation_id != operation_id)
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        Ok(reservation)
    }

    /// Resolves an immutable operation without adopting the mutable head.
    pub async fn operation_generation<W: crate::facade::FsWork>(
        &self,
        key: IdempotencyKey,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<Generation<A, O>>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .volume
            .fs
            .workspace_operation_generation(
                &self.volume,
                key.operation_id(),
                &mut accounting,
                cancellation,
            )
            .await
            .map(|mut observed| {
                let id = observed.0.take().map(|(id, record)| {
                    drop(record);
                    id
                });
                drop(observed);
                id.map(|id| Generation {
                    workspace: self.clone(),
                    id,
                    publication_closure: None,
                })
            });
        work.finish(result, &accounting)
    }

    /// Verifies the exact immutable result of this workspace's join operation.
    pub async fn verify_join_commit<W: crate::facade::FsWork>(
        &self,
        witness: &crate::JoinCommitWitness,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<bool, WorkspaceError> {
        self.volume
            .fs
            .verify_workspace_join_commit(&self.volume, witness, work, cancellation)
            .await
    }
}
