use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {

    /// Durably retains one generation under a retention label.
    pub(crate) async fn retain_workspace_generation<W: FsWork>(
        &self, volume: &Volume<A, O>, generation_id: GenerationId, kind: RetentionKind, mut label: Owned<String>,
        publication: Option<VerifiedPublication<A, O>>, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                if label.bytes != label.value.capacity() as u64 || !matches!(&label.allocation,
                    KernelOwnedAllocation::NativeOriginal(allocation)
                        if allocation.bytes() == label.bytes && allocation.work().shares_ledger(original))
                { return Err(FsError::from(NativeTreeMaterializationError::InvalidLayout).into()); }
            } else {
                label.allocation = accounting.claim_owned(label.bytes, 0)?;
            }
            #[cfg(target_arch = "wasm32")]
            { label.allocation = accounting.claim_owned(label.bytes, 0)?; }
            self.retain_workspace_generation_with_accounting(
                volume, generation_id, kind, label, publication, &mut accounting, cancellation,
            ).await
        }.await;
        budget.finish(result, &accounting)
    }

    pub(super) async fn retain_workspace_generation_with_accounting(
        &self, volume: &Volume<A, O>, generation_id: GenerationId, kind: RetentionKind, label: Owned<String>,
        publication: Option<VerifiedPublication<A, O>>, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        accounting.check()?;
        let generation_root = ObjectId { kind: ObjectKind::GenerationRoot, digest: generation_id.digest() };
        let hashed = WorkCounters {
            bytes_hashed: (b"acyclic-fs-retention-authority-v1\0".len() + 16 + 1 + 8 + label.value.len()) as u64,
            bytes_copied: 16,..WorkCounters::UNCHARGED
        };
        accounting.admit(hashed)?;
        let authority_id = retention_authority_id(volume.id, kind, &label.value);
        accounting.record_completed(hashed)?;
        let Owned { value: label, allocation, bytes } = label;
        let fact = Owned::new(
            RetentionCreated { volume_id: volume.id, kind, label, generation_root, config: volume.config }, allocation, bytes,
        );
        let encoded = crate::kernel::codec::encode_owned(accounting, |observe, allocation_error| crate::kernel::encode_retention_created(&fact.value, observe, allocation_error))?;
        let commit = creation_commit_with_accounting(
            OperationId::from_bytes(authority_id.into_bytes()), encoded, accounting,
        )?;
        fact.retire(accounting)?;
        let cached_bytes = publication.as_ref().map_or(0, VerifiedPublication::retained_bytes);
        let cached = if let Some(publication) = publication {
            let bytes = if accounting.standalone_parts().is_some() { cached_bytes } else { 0 };
            let allocation = accounting.claim_owned(bytes, 0)?;
            Some(Owned::new(publication, allocation, bytes))
        } else { None };
        let head = self.retention_head_with_accounting(authority_id, accounting, cancellation).await;
        match head {
            Ok(_) => {
                let found = self.retention_operation_with_accounting(
                    authority_id, commit.operation_id, accounting, cancellation,
                ).await?;
                if found.is_some_and(|(sequence, fingerprint)| sequence == Sequence::new(1)
                    && fingerprint == commit.fingerprint)
                { if let Some(cached) = cached { cached.retire(accounting)?; } return Ok(()); }
            }
            Err(FsError::Authority(AuthorityStoreError::Missing)) => {}
            Err(error) => return Err(error.into()),
        }
        let limits = closure_limits(volume.config);
        let cached = match cached {
            Some(proof) if self.same_deployment(&proof.value.filesystem)
                && proof.value.generation_root == generation_root && proof.value.volume_id == volume.id
                && proof.value.limits == limits
                && proof.value.proven_at == self.inner.objects.collection_sweeps() => Some(proof),
            Some(proof) => { proof.retire(accounting)?; None }
            None => None,
        };
        let publication = if let Some(proof) = cached { proof } else {
            self.retention_publication_with_accounting(volume, generation_root, accounting, cancellation).await?
        };
        let result = self.publish_retention_with_accounting(
            authority_id, commit, &publication.value, accounting, cancellation,
        ).await;
        publication.retire(accounting)?;
        if result? { Ok(()) } else { Err(WorkspaceError::RetentionConflict) }
    }

    pub(super) async fn retention_head_with_accounting(
        &self, authority_id: AuthorityId, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<Head, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            return ({ let __authority_args = (authority_id, original, cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = self.inner.authority.head(__authority_args.0, &mut __authority_work).await; __authority_result }).map_err(Into::into);
        }
        let result = ({ let __authority_args = (authority_id, accounting.remaining()?, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = self.inner.authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) });
        let spent = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_creation_backend_work(accounting, spent)?;
        result.map(|receipt| receipt.value).map_err(|failure| failure.error.into())
    }

    pub(super) async fn retention_operation_with_accounting(
        &self, authority_id: AuthorityId, operation_id: OperationId, accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(Sequence, Digest)>, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let found = ({ let __authority_args = (authority_id, operation_id, original, cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = self.inner.authority.find_operation(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_result })?;
            return Ok(found.as_ref().map(|record| (record.sequence, record.fingerprint)));
        }
        let result = ({ let __authority_args = (authority_id, operation_id, accounting.remaining()?, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = self.inner.authority.find_operation(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) });
        let spent = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_creation_backend_work(accounting, spent)?;
        result.map(|receipt| receipt.value.map(|record| (record.sequence, record.fingerprint))).map_err(|failure| failure.error.into())
    }

    pub(super) async fn retention_publication_with_accounting(
        &self, volume: &Volume<A, O>, generation_root: ObjectId, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<Owned<VerifiedPublication<A, O>>, WorkspaceError> {
        let proven_at = self.inner.objects.collection_sweeps();
        let limits = closure_limits(volume.config);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let proof = prove_generation_closure_with_native_work(
                &self.inner.objects, generation_root, limits, original, cancellation,
            ).await?;
            if proof.0.root.volume_id != volume.id { return Err(WorkspaceError::ForeignGeneration); }
            let root_bytes = root_parent_bytes(&proof.0.root)?;
            let bytes = (proof.0.objects.capacity() as u64).checked_mul(size_of::<ObjectId>() as u64).ok_or(WorkError::Overflow)?;
            let (proof, allocation) = proof;
            drop(proof.root);
            let mut objects = (proof.objects, allocation);
            objects.1.release(root_bytes)?;
            let publication = VerifiedPublication {
                filesystem: self.clone(), generation_root, volume_id: volume.id, limits,
                objects: objects.0, proven_at, _allocation: KernelOwnedAllocation::Standalone,
            };
            return Ok(Owned::new(publication, KernelOwnedAllocation::NativeOriginal(objects.1), bytes));
        }
        let result = prove_generation_closure(
            &self.inner.objects, generation_root, limits, accounting.remaining()?, cancellation,
        ).await;
        let spent = match &result { Ok(proof) => proof.work, Err(failure) => *failure.work };
        merge_creation_backend_work(accounting, spent)?;
        let proof = result.map_err(|failure| FsError::from(failure.error))?;
        if proof.root.volume_id != volume.id { return Err(WorkspaceError::ForeignGeneration); }
        drop(proof.root);
        let bytes = (proof.objects.capacity() as u64).checked_mul(size_of::<ObjectId>() as u64).ok_or(WorkError::Overflow)?;
        let allocation = accounting.claim_owned(bytes, 0)?;
        Ok(Owned::new(VerifiedPublication {
            filesystem: self.clone(), generation_root, volume_id: volume.id, limits,
            objects: proof.objects, proven_at, _allocation: KernelOwnedAllocation::Standalone,
        }, allocation, bytes))
    }

    pub(super) async fn publish_retention_with_accounting(
        &self, authority_id: AuthorityId, commit: ProposedCommit, publication: &VerifiedPublication<A, O>,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<bool, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let _hold = ({ let __object_args = (crate::PublicationScope::Closure { objects: &publication.objects, proven_at: publication.proven_at }, original, cancellation,); let mut __object_work = crate::AuthorityWork::native(__object_args.1, __object_args.2); let __object_result = self.inner.objects.flush_before_publish(__object_args.0, &mut __object_work).await; __object_result })?;
            accounting.check()?;
            let created = ({ let __authority_args = (authority_id, commit, original, cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = self.inner.authority.create_first_record(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_result })?;
            return Ok(created);
        }
        let drained = ({ let __object_args = (crate::PublicationScope::Closure { objects: &publication.objects, proven_at: publication.proven_at }, accounting.remaining()?, cancellation,); let mut __object_work = crate::AuthorityWork::new(__object_args.1, __object_args.2); let __object_result = self.inner.objects.flush_before_publish(__object_args.0, &mut __object_work).await; __object_work.receipt(__object_result) });
        let spent = match &drained { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_creation_backend_work(accounting, spent)?;
        let _hold = drained.map_err(|failure| FsError::Object(failure.error))?.value;
        accounting.check()?;
        let created = ({ let __authority_args = (authority_id, commit, accounting.remaining()?, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = self.inner.authority.create_first_record(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) });
        let spent = match &created { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
        merge_creation_backend_work(accounting, spent)?;
        Ok(created.map_err(|failure| FsError::Authority(failure.error))?.value)
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
        Ok(FsReceipt {
            value: GenerationId::new(checkpoint.value.digest),
            work: checkpoint.work,
        })
    }

    pub(super) async fn checkpoint_root<W: FsWork>(
        &self, budget: W, cancellation: &CancellationToken,
    ) -> W::Output<ObjectId, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = if self.prepared_merge_parent.is_some() || !self.has_pending_mutations() {
            Ok(self.generation_root)
        } else {
            crate::kernel::build_checkpoint_with_accounting(
                &self.volume.fs.inner.objects,
                CheckpointRequest {
                    base: self.root.base(), file_table: self.root.file_table, merge_parent: None,
                },
                DecodeLimits::for_volume(self.volume.config), &mut accounting, cancellation,
            ).await
        };
        budget.finish(result, &accounting)
    }
}

