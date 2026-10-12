use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    /// Durably retains one generation under a retention label.
    pub(crate) async fn retain_workspace_generation<W: FsWork>(
        &self,
        volume: &Volume<A, O>,
        generation_id: GenerationId,
        kind: RetentionKind,
        mut label: Owned<String>,
        publication: Option<VerifiedPublication<A, O>>,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                if label.bytes != label.value.capacity() as u64
                    || !matches!(&label.allocation,
                    KernelOwnedAllocation::NativeOriginal(allocation)
                        if allocation.bytes() == label.bytes && allocation.work().shares_ledger(original))
                {
                    return Err(FsError::from(NativeTreeMaterializationError::InvalidLayout).into());
                }
            } else {
                label.allocation = accounting.claim_owned(label.bytes, 0)?;
            }
            #[cfg(target_arch = "wasm32")]
            {
                label.allocation = accounting.claim_owned(label.bytes, 0)?;
            }
            let accounting = &mut accounting;

            accounting.check()?;
            let generation_root = ObjectId { kind: ObjectKind::GenerationRoot, digest: generation_id.digest() };
            let hashed = WorkCounters { bytes_hashed: (b"acyclic-fs-retention-authority-v1\0".len() + 16 + 1 + 8 + label.value.len()) as u64, bytes_copied: 16, ..WorkCounters::UNCHARGED };
            accounting.admit(hashed)?;
            let authority_id = retention_authority_id(volume.id, kind, &label.value);
            accounting.record_completed(hashed)?;
            let Owned { value: label, allocation, bytes } = label;
            let fact = Owned::new(RetentionCreated { volume_id: volume.id, kind, label, generation_root, config: volume.config }, allocation, bytes);
            let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_retention_created(&fact.value, observe, allocation_error))?;
            let commit = creation_commit_with_accounting(OperationId::from_bytes(authority_id.into_bytes()), encoded, accounting)?;
            fact.retire(accounting)?;
            let cached_bytes = publication.as_ref().map_or(0, VerifiedPublication::retained_bytes);
            let cached = if let Some(publication) = publication {
                let bytes = if accounting.standalone_parts().is_some() { cached_bytes } else { 0 };
                let allocation = accounting.claim_owned(bytes, 0)?;
                Some(Owned::new(publication, allocation, bytes))
            } else {
                None
            };
            let head = self.retention_head_with_accounting(authority_id, accounting, cancellation).await;
            match head {
                Ok(_) => {
                    let found = self.retention_operation_with_accounting(authority_id, commit.operation_id, accounting, cancellation).await?;
                    if found.is_some_and(|(sequence, fingerprint)| sequence == Sequence::new(1) && fingerprint == commit.fingerprint) {
                        if let Some(cached) = cached {
                            cached.retire(accounting)?;
                        }
                        return Ok(());
                    }
                }
                Err(FsError::Authority(AuthorityStoreError::Missing)) => {}
                Err(error) => return Err(error.into()),
            }
            let limits = closure_limits(volume.config);
            let cached = match cached {
                Some(proof)
                    if self.same_deployment(&proof.value.filesystem)
                        && proof.value.generation_root == generation_root
                        && proof.value.volume_id == volume.id
                        && proof.value.limits == limits
                        && proof.value.proven_at == self.inner.objects.collection_sweeps() =>
                {
                    Some(proof)
                }
                Some(proof) => {
                    proof.retire(accounting)?;
                    None
                }
                None => None,
            };
            let publication = if let Some(proof) = cached { proof } else { self.retention_publication_with_accounting(volume, generation_root, accounting, cancellation).await? };
            let result = self.publish_retention_with_accounting(authority_id, commit, &publication.value, accounting, cancellation).await;
            publication.retire(accounting)?;
            if result? { Ok(()) } else { Err(WorkspaceError::RetentionConflict) }
        }
        .await;
        budget.finish(result, &accounting)
    }

    pub(super) async fn retention_head_with_accounting(
        &self,
        authority_id: AuthorityId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Head, FsError> {
        let mut work = authority_work(accounting, cancellation)?;
        let result = self.inner.authority.head(authority_id, &mut work).await;
        record_authority_work(accounting, &work)?;
        result.map_err(Into::into)
    }

    pub(super) async fn retention_operation_with_accounting(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(Sequence, Digest)>, FsError> {
        let mut work = authority_work(accounting, cancellation)?;
        let result = self
            .inner
            .authority
            .find_operation(authority_id, operation_id, &mut work)
            .await;
        record_authority_work(accounting, &work)?;
        let (record, _allocation) = result?;
        Ok(record.map(|record| (record.sequence, record.fingerprint)))
    }
    pub(super) async fn retention_publication_with_accounting(
        &self,
        volume: &Volume<A, O>,
        root: ObjectId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Owned<VerifiedPublication<A, O>>, WorkspaceError> {
        self.publication_proof(
            root,
            self.inner.objects.collection_sweeps(),
            volume.id,
            volume.config,
            accounting,
            cancellation,
        )
        .await
        .map_err(|error| match error {
            FsError::VolumeMismatch => WorkspaceError::ForeignGeneration,
            error => error.into(),
        })
    }
    pub(super) async fn publish_retention_with_accounting(
        &self,
        authority_id: AuthorityId,
        commit: ProposedCommit,
        publication: &VerifiedPublication<A, O>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<bool, WorkspaceError> {
        let _hold = self
            .flush_publication(
                crate::PublicationScope::Closure {
                    objects: &publication.objects,
                    proven_at: publication.proven_at,
                },
                accounting,
                cancellation,
            )
            .await?;
        let mut authority = authority_work(accounting, cancellation)?;
        let result = self
            .inner
            .authority
            .create_first_record(authority_id, commit, &mut authority, None::<&mut ()>)
            .await;
        record_authority_work(accounting, &authority)?;
        result.map_err(|error| {
            WorkspaceError::from(FsError::from(error.into_error(std::convert::identity)))
        })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    /// Writes the immutable generation-root checkpoint for the current sparse
    pub async fn checkpoint(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationId> {
        let checkpoint = self.checkpoint_root(budget, cancellation).await?;
        Ok(OperationReceipt {
            value: GenerationId::new(checkpoint.value.digest),
            work: checkpoint.work,
        })
    }

    pub(super) async fn checkpoint_root<W: FsWork>(
        &self,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<ObjectId, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = if self.prepared_merge_parent.is_some() || !self.has_pending_mutations() {
            Ok(self.generation_root)
        } else {
            crate::kernel::build_checkpoint_with_accounting(
                &self.volume.fs.inner.objects,
                CheckpointRequest {
                    base: self.root.base(),
                    file_table: self.root.file_table,
                    merge_parent: None,
                },
                DecodeLimits::for_volume(self.volume.config),
                &mut accounting,
                cancellation,
            )
            .await
        };
        budget.finish(result, &accounting)
    }
}
