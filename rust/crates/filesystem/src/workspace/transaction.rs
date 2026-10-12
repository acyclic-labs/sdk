use super::*;

/// One unpublished sparse atomic workspace transaction.
pub struct Transaction<A, O> {
    pub(super) workspace: Workspace<A, O>,
    pub(crate) checkout: Checkout<A, O>,
    pub(super) idempotency_key: IdempotencyKey,
    pub(super) requires_rebase: bool,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    pub(crate) fn from_checkout(
        workspace: &Workspace<A, O>,
        mut checkout: Checkout<A, O>,
        idempotency_key: IdempotencyKey,
        requires_rebase: bool,
    ) -> Self {
        checkout.bind_authored_operation(idempotency_key.operation_id());
        Self {
            workspace: workspace.clone(),
            checkout,
            idempotency_key,
            requires_rebase,
        }
    }

    pub(crate) fn for_checkout_candidate<W: crate::facade::FsWork>(
        workspace: &Workspace<A, O>,
        checkout: Checkout<A, O>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        let accounting = work.accounting(cancellation);
        let result = (|| -> Result<Self, WorkspaceError> {
            accounting.check()?;
            let idempotency_key = match checkout.bound_authored_operation_id() {
                #[cfg(not(target_arch = "wasm32"))]
                Some(id) if accounting.native_work().is_some() => {
                    IdempotencyKey::from_bytes(id.into_bytes())
                }
                #[cfg(not(target_arch = "wasm32"))]
                None if accounting.native_work().is_some() => {
                    return Err(FsError::MutationNotAllowed.into());
                }
                _ => IdempotencyKey::new(),
            };
            Ok(Self {
                workspace: workspace.clone(),
                checkout,
                idempotency_key,
                requires_rebase: false,
            })
        })();
        work.finish(result, &accounting)
    }

    pub(crate) fn mutation_path(
        &self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<NamespacePath>, WorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        let (path, allocation) = customer_path_with_accounting(
            path,
            self.checkout.volume_config(),
            accounting,
            cancellation,
        )?;
        let bytes = path.retained_bytes();
        Ok(crate::kernel::Owned::new(path, allocation, bytes))
    }

    pub(crate) fn push_mutation<T>(
        operations: &mut crate::kernel::Owned<Vec<T>>,
        mut operation: crate::kernel::Owned<T>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), WorkspaceError> {
        let copied = copy_work(std::mem::size_of::<T>() as u64);
        accounting.admit(copied)?;
        operations.allocation.absorb(&mut operation.allocation)?;
        operations.bytes = operations
            .bytes
            .checked_add(operation.bytes)
            .ok_or(crate::WorkError::Overflow)?;
        operations.value.push(operation.value);
        accounting.record_completed(copied)?;
        Ok(())
    }

    pub(crate) async fn apply_authored(
        &mut self,
        operations: crate::kernel::Owned<Vec<AuthoredMutation>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        self.checkout
            .apply_authored_transaction_with_accounting(operations, accounting, cancellation)
            .await?
            .retire(accounting)
            .map_err(WorkspaceError::from)
    }

    pub(super) async fn mutate<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        build: impl FnOnce(NamespacePath) -> Result<AuthoredMutation, WorkspaceError>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let path = self.mutation_path(path, &mut accounting, cancellation)?;
            let operation = match build(path.value) {
                Ok(value) => crate::kernel::Owned::new(value, path.allocation, path.bytes),
                Err(error) => {
                    let mut allocation = path.allocation;
                    accounting.release_owned(&mut allocation, path.bytes)?;
                    return Err(error);
                }
            };
            let mut operations = crate::kernel::Owned::allocate(1, &mut accounting)?;
            Self::push_mutation(&mut operations, operation, &mut accounting)?;
            self.apply_authored(operations, &mut accounting, cancellation)
                .await
        }
        .await;
        work.finish(result, &accounting)
    }
}
