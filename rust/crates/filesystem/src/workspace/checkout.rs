use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Opens an authored private or immutable checkout.
    pub async fn checkout<W: crate::facade::FsWork>(&self, selector: GenerationSelector, mode: CheckoutMode, work: W, cancellation: &CancellationToken) -> W::Output<Checkout<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.checkout_accounted(selector, mode, &mut accounting, cancellation).await;
        work.finish(result, &accounting)
    }

    pub(super) async fn checkout_accounted(&self, selector: GenerationSelector, mode: CheckoutMode, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken) -> Result<Checkout<A, O>, WorkspaceError> {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            return self.volume.checkout(selector, mode, original, cancellation).await.map_err(WorkspaceError::from);
        }
        let opened = self.volume.checkout(selector, mode, accounting.remaining()?, cancellation).await;
        let (allocations, work, budget, _) = accounting.standalone_parts().ok_or(crate::WorkError::Overflow)?;
        let checkout = merge_workspace_backend(opened, work, allocations.live_bytes())?.value;
        work.verify(budget)?;
        Ok(checkout)
    }

    pub(super) async fn open_checkout(&self, selector: GenerationSelector, mode: CheckoutMode, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken) -> Result<crate::kernel::Owned<Checkout<A, O>>, WorkspaceError> {
        let checkout = self.checkout_accounted(selector, mode, accounting, cancellation).await?;
        #[cfg(not(target_arch = "wasm32"))]
        if accounting.native_work().is_some() { return Ok(crate::kernel::Owned::new(checkout, accounting.claim_owned(0, 0)?, 0)); }
        let bytes = checkout.logical_owned_bytes();
        Ok(crate::kernel::Owned::new(checkout, accounting.claim_owned(bytes, 0)?, bytes))
    }

    pub(super) async fn open_read_path(&self, selector: GenerationSelector, path: &str, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken) -> Result<(crate::kernel::Owned<Checkout<A, O>>, crate::kernel::Owned<NamespacePath>), WorkspaceError> {
        let checkout = self.open_checkout(selector, CheckoutMode::read_only_pinned(), accounting, cancellation).await?;
        let (path, allocation) = customer_path_with_accounting(path, checkout.value.volume_config(), accounting, cancellation)?;
        let bytes = path.retained_bytes();
        Ok((checkout, crate::kernel::Owned::new(path, allocation, bytes)))
    }

    /// Pins the authenticated head under supplied work limits.
    pub async fn head<W: crate::facade::FsWork>(&self, work: W, cancellation: &CancellationToken) -> W::Output<Generation<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.checkout_accounted(GenerationSelector::Head, CheckoutMode::read_only_pinned(), &mut accounting, cancellation).await
            .map(|checkout| Generation { workspace: self.clone(), id: checkout.generation_id(), publication_closure: None });
        work.finish(result, &accounting)
    }

    /// Authenticates an exact immutable generation.
    pub async fn generation<W: crate::facade::FsWork>(&self, id: GenerationId, work: W, cancellation: &CancellationToken) -> W::Output<Generation<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.checkout_accounted(GenerationSelector::Exact(id), CheckoutMode::read_only_pinned(), &mut accounting, cancellation).await
            .map(|checkout| Generation { workspace: self.clone(), id: checkout.generation_id(), publication_closure: None });
        work.finish(result, &accounting)
    }

    /// Opens a private transaction against the current head.
    pub async fn begin_transaction<W: crate::facade::FsWork>(&self, key: IdempotencyKey, work: W, cancellation: &CancellationToken) -> W::Output<Transaction<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.checkout_accounted(GenerationSelector::Head, CheckoutMode::tracking_transaction(), &mut accounting, cancellation).await
            .map(|checkout| Transaction::from_checkout(self, checkout, key, false));
        work.finish(result, &accounting)
    }

    /// Opens an exact transaction and observes whether its head has advanced.
    pub async fn begin_transaction_at<W: crate::facade::FsWork>(&self, generation: &Generation<A, O>, key: IdempotencyKey, work: W, cancellation: &CancellationToken) -> W::Output<Transaction<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            accounting.check()?;
            if generation.workspace.id != self.id { return Err(WorkspaceError::ForeignGeneration); }
            let head = self.checkout_accounted(GenerationSelector::Head, CheckoutMode::read_only_pinned(), &mut accounting, cancellation).await?.generation_id();
            let checkout = self.checkout_accounted(GenerationSelector::Exact(generation.id), CheckoutMode::tracking_transaction(), &mut accounting, cancellation).await?;
            Ok(Transaction::from_checkout(self, checkout, key, head != generation.id))
        }.await;
        work.finish(result, &accounting)
    }

    /// Opens an exact transaction without consulting the mutable head.
    pub async fn begin_pinned_transaction<W: crate::facade::FsWork>(&self, generation: &Generation<A, O>, key: IdempotencyKey, work: W, cancellation: &CancellationToken) -> W::Output<Transaction<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            accounting.check()?;
            if generation.workspace.id != self.id { return Err(WorkspaceError::ForeignGeneration); }
            let checkout = self.checkout_accounted(GenerationSelector::Exact(generation.id), CheckoutMode::tracking_transaction(), &mut accounting, cancellation).await?;
            Ok(Transaction::from_checkout(self, checkout, key, false))
        }.await;
        work.finish(result, &accounting)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn reader(&self, generation: GenerationId, original: &crate::NativeMaterializationWork, cancellation: &CancellationToken) -> Result<crate::PinnedReader<A, O>, WorkspaceError> {
        self.volume.checkout(GenerationSelector::Exact(generation), CheckoutMode::read_only_pinned(), original, cancellation).await?
            .into_pinned_reader_with_native_work(original, cancellation).map_err(WorkspaceError::from)
    }

    pub(crate) async fn head_with_file_record_measured(&self, file_id: FileId, budget: WorkBudget, cancellation: &CancellationToken) -> crate::MeasuredResult<crate::OperationReceipt<(Generation<A, O>, bool)>, WorkspaceError> {
        let checkout = self.checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned(), budget, cancellation).await?;
        let reader = checkout.value.pinned_reader().map_err(|error| crate::OperationFailure::new(WorkspaceError::engine(error), checkout.work))?;
        let prior = checkout.work;
        drop(checkout.value);
        let remaining = prior.remaining(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
        let records = reader.file_records_by_id(&[file_id], remaining, cancellation).await.map_err(|failure| failure.map_with_prior_work(prior, WorkspaceError::from))?;
        let work = prior.checked_add(records.work).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
        Ok(crate::OperationReceipt { value: (Generation { workspace: self.clone(), id: reader.generation_id(), publication_closure: None }, records.value.first().is_some_and(Option::is_some)), work })
    }
}
