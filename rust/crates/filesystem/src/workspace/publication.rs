use super::*;

#[cfg(test)]
mod tests;

/// An original publication and its caller-supplied evidence, recovered together.
/// The evidence aliases the durable record and retains its original allocation.
/// It is not a workspace file and never appears in a directory or checkout.
pub struct OperationPublication<A: 'static, O: 'static> {
    generation: Generation<A, O>,
    predecessor: Option<(crate::Sequence, crate::Digest)>,
    evidence: Option<Bytes>,
    // Backing drops before this credit. Funded provider bytes retain their
    // own original owner instead of acquiring a second allocation lease.
    _allocation: Option<crate::kernel::RetainedAllocation>,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> OperationPublication<A, O> {
    /// Resolves the actual serial predecessor of this publication, fenced by
    /// its original authority position and digest. An unchanged publication can
    /// retain its predecessor's generation; that generation's DAG parent is
    /// not the predecessor of this operation. Workspace creation has none.
    pub async fn predecessor<W: crate::facade::FsWork>(
        &self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<Generation<A, O>>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let workspace = &self.generation.workspace;
        let result = async {
            accounting.check()?;
            let Some((position, digest)) = self.predecessor else {
                return Ok(None);
            };
            let id = workspace
                .volume
                .fs
                .workspace_generation_at_position(
                    &workspace.volume,
                    position,
                    digest,
                    &mut accounting,
                )
                .await?;
            Ok(Some(Generation {
                workspace: workspace.clone(),
                id,
                publication_closure: None,
            }))
        }
        .await;
        let result = read_core::authorize(workspace, result, &mut accounting, cancellation).await;
        work.finish(result, &accounting)
    }
}

impl<A, O> OperationPublication<A, O> {
    /// The immutable result of this operation, independent of the current head.
    pub fn generation(&self) -> &Generation<A, O> {
        &self.generation
    }

    /// The exact evidence supplied when this publication was prepared.
    pub fn evidence(&self) -> Option<&[u8]> {
        self.evidence.as_deref()
    }

    /// Releases the record backing while retaining the immutable result.
    pub fn into_generation(self) -> Generation<A, O> {
        self.generation
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Workspace<A, O> {
    /// Recovers a publication's immutable generation and original evidence using
    /// its existing authority operation index, without reading workspace paths.
    /// Bound handles recheck current access before returning evidence; unbound
    /// owner handles need only the indexed lookup. Publications without caller
    /// evidence, including workspace creation, return a generation with no evidence. Missing
    /// operations return `None`. `maximum_evidence_bytes` bounds the original
    /// evidence before decoding its contents or hashing it; `None` leaves it unbounded.
    pub async fn operation_publication<W: crate::facade::FsWork>(
        &self,
        key: IdempotencyKey,
        maximum_evidence_bytes: Option<u64>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<OperationPublication<A, O>>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let (observed, mut allocation) = self
                .volume
                .fs
                .workspace_operation_generation(
                    &self.volume,
                    key.operation_id(),
                    None,
                    maximum_evidence_bytes,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let Some((id, record, evidence)) = observed else {
                crate::kernel::retire_allocation(allocation, 0, &mut accounting)?;
                return Ok(None);
            };
            let bytes =
                if !self.volume.fs.authority().retains_operation_payload() && evidence.is_some() {
                    record.payload.len() as u64
                } else {
                    0
                };
            if bytes != 0 && matches!(allocation, KernelOwnedAllocation::Standalone) {
                let (funding, admitted) = accounting.record_actual_owned(bytes)?;
                allocation = funding;
                if let Err(error) = admitted {
                    drop(evidence);
                    drop(record);
                    crate::kernel::retire_allocation(allocation, bytes, &mut accounting)?;
                    return Err(error.into());
                }
            }
            let predecessor = record
                .sequence
                .get()
                .checked_sub(1)
                .filter(|position| *position != 0)
                .map(|position| (crate::Sequence::new(position), record.previous_digest));
            drop(record);
            let allocation = if bytes != 0 {
                Some(crate::kernel::RetainedAllocation::transfer(
                    &mut allocation,
                    bytes,
                    &mut accounting,
                )?)
            } else {
                crate::kernel::retire_allocation(allocation, bytes, &mut accounting)?;
                None
            };
            Ok(Some(OperationPublication {
                generation: Generation {
                    workspace: self.clone(),
                    id,
                    publication_closure: None,
                },
                predecessor,
                evidence,
                _allocation: allocation,
            }))
        }
        .await;
        let result = read_core::authorize(self, result, &mut accounting, cancellation).await;
        work.finish(result, &accounting)
    }

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
            let mut authority_work = crate::async_storage::AuthorityFrame::borrow(&mut accounting)?;
            let reserved = async {
                let head = authority.head(authority_id, &mut authority_work).await?;
                authority_work.remaining()?;
                authority
                    .reserve_publication(authority_id, head, operation_id, &mut authority_work)
                    .await
            }
            .await
            .map_err(|error| WorkspaceError::from(FsError::Authority(error)));
            crate::facade::finish_authority_work(authority_work)?;
            let reserved = reserved?;
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
            let mut authority_work = crate::async_storage::AuthorityFrame::borrow(&mut accounting)?;
            let released = self
                .volume
                .fs
                .authority()
                .release_publication(reservation, &mut authority_work)
                .await
                .map_err(|error| WorkspaceError::from(FsError::Authority(error)));
            crate::facade::finish_authority_work(authority_work)?;
            released
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
                None,
                None,
                &mut accounting,
                cancellation,
            )
            .await
            .map(|mut observed| {
                let id = observed.0.take().map(|(id, record, _evidence)| {
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
