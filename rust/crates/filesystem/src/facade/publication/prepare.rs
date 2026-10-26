use super::*;

pub(super) fn root_storage_bytes(
    prepared: &super::super::candidates::PreparedCheckoutRoots,
) -> Result<u64, FsError> {
    let base = root_parent_bytes(&prepared.base.0)?;
    match &prepared.held {
        Some((root, _)) => base
            .checked_add(root_parent_bytes(root)?)
            .ok_or(FsError::Work(WorkError::Overflow)),
        None => Ok(base),
    }
}

impl<A, O> VerifiedPublication<A, O> {
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn native_slot(
        original: &crate::NativeMaterializationWork,
    ) -> Result<(PublicationClosure<A, O>, crate::NativeAllocation), FsError> {
        let bytes = usize_to_u64(size_of::<Mutex<Option<Self>>>() + 2 * size_of::<usize>());
        let allocation = original.claim(bytes, 1, WorkCounters::UNCHARGED)?;
        Ok((Arc::new(Mutex::new(None)), allocation))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    pub(super) fn publication_dependencies(
        &self,
    ) -> Result<
        (
            super::super::proof::PublicationFreeze,
            Option<crate::kernel::PublicationPreconditions>,
        ),
        FsError,
    > {
        self.dependencies.publication_snapshot(
            self.root.base,
            self.volume.config,
            self.base_authority_head(),
        )
    }

    pub(super) fn publication_request(
        &self,
        operation_id: OperationId,
        expected: Head,
        generation_root: ObjectId,
    ) -> PublishGenerationRequest {
        PublishGenerationRequest {
            authority_id: volume_authority_id(self.volume.id),
            volume_id: self.volume.id,
            epoch: expected.epoch,
            expected,
            operation_id,
            generation_root,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn prepare_published_native_roots(
        &self,
        root: &GenerationRoot,
        guard: &mut crate::NativeAllocation,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        producer: Option<&Arc<ProofDataCustody>>,
    ) -> Result<super::super::candidates::PreparedCheckoutRoots, FsError> {
        let mut prepared = self.prepare_root_replacement(
            root,
            root,
            &mut KernelAccounting::native(original, cancellation),
            cancellation,
            producer,
        )?;
        self.prepare_root_working_control_with_native_work(
            root,
            guard,
            &mut prepared,
            original,
            cancellation,
        )?;
        Ok(prepared)
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn native_frame(
    original: &crate::NativeMaterializationWork,
    bytes: u64,
) -> Result<crate::NativeAllocation, FsError> {
    Ok(original.claim(bytes, u64::from(bytes != 0), WorkCounters::UNCHARGED)?)
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
    pub(super) fn prepare_published_roots(
        &self,
        root: &GenerationRoot,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        producer: Option<&Arc<ProofDataCustody>>,
    ) -> crate::MeasuredResult<
        OperationReceipt<super::super::candidates::PreparedCheckoutRoots>,
        FsError,
    > {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        measured(
            self.prepare_root_replacement(root, root, &mut accounting, cancellation, producer),
            &accounting,
        )
    }
}
