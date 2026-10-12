use super::*;

pub(super) fn authored_input_owned_bytes(authored: &[AuthoredMutation], capacity: usize) -> Result<u64, WorkError> {
    let vector_bytes = capacity.checked_mul(size_of::<AuthoredMutation>()).map(usize_to_u64).ok_or(WorkError::Overflow)?;
    authored.iter().try_fold(vector_bytes, |owned, mutation| {
        owned.checked_add(mutation.logical_owned_bytes()).ok_or(WorkError::Overflow)
    })
}

pub(super) enum AuthoredTransactionInput<'a> {
    Owned { mutations: std::vec::IntoIter<AuthoredMutation>, allocation: KernelOwnedAllocation, capacity: usize, },
    Borrowed(&'a [AuthoredMutation]),
}

impl AuthoredTransactionInput<'_> {

    pub(super) fn mutations(&self) -> &[AuthoredMutation] {
        match self { Self::Owned { mutations, .. } => mutations.as_slice(), Self::Borrowed(mutations) => mutations, }
    }

    pub(super) fn capacity(&self) -> usize {
        match self { Self::Owned { capacity, .. } => *capacity, Self::Borrowed(mutations) => mutations.len(), }
    }

    pub(super) fn retire(self, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
        match self {
            Self::Owned { mutations, allocation, capacity } => {
                drop(mutations);
                let backing = match &allocation {
                    KernelOwnedAllocation::Standalone => { usize_to_u64(capacity * size_of::<AuthoredMutation>()) }
                    #[cfg(not(target_arch = "wasm32"))]
                    KernelOwnedAllocation::NativeOriginal(guard) => guard.bytes(),
                };
                retire_allocation(allocation, backing, accounting)
            }
            Self::Borrowed(_) => Ok(()),
        }
    }
}

pub(super) fn mutation_path_owned_bytes(mutation: &Mutation) -> u64 {
    mutation.paths().map_or(0, |[first, second]| {
        let first_bytes = first.retained_bytes();
        if std::ptr::eq(first, second) { first_bytes } else { first_bytes + second.retained_bytes() }
    })
}

pub(super) fn authored_mutation_operation_count(mutation: &AuthoredMutation) -> usize {
    match mutation {
        AuthoredMutation::CreateFile { bytes, .. } => {
            usize::from(bytes.len() > crate::kernel::MAXIMUM_INLINE_FILE_BYTES) + 1
        }
        AuthoredMutation::CreateFileFromContent { content, .. } => {
            usize::from(content.logical_bytes != 0) + 1
        }
        AuthoredMutation::Reidentify { .. }
        | AuthoredMutation::Write { .. }
        | AuthoredMutation::WriteFromContent { .. }
        | AuthoredMutation::CreateDirectory { .. }
        | AuthoredMutation::CreateSymbolicLink { .. }
        | AuthoredMutation::CreateEmptySpecial { .. }
        | AuthoredMutation::CreateDevice { .. }
        | AuthoredMutation::CreateReparsePoint { .. }
        | AuthoredMutation::Remove { .. }
        | AuthoredMutation::Rename { .. }
        | AuthoredMutation::HardLink { .. }
        | AuthoredMutation::SetMetadata { .. }
        | AuthoredMutation::Resize { .. }
        | AuthoredMutation::ZeroRange { .. }
        | AuthoredMutation::Preallocate { .. }
        | AuthoredMutation::CloneRange(_) => 1,
    }
}

/// Stable result positions for an authored transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoredTransactionResult {
    /// One entry per authored operation; create operations contain their new
    pub created_file_ids: Vec<Option<FileId>>,
}

/// Stable result positions plus the terminal outcome of one authored live transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthoredLiveMutationResult {
    /// One entry per authored operation; create operations contain their new
    pub created_file_ids: Vec<Option<FileId>>,
    /// Terminal direct-live publication outcome.
    pub outcome: LiveMutationOutcome,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {

    /// Compiles and atomically applies one complete caller-authored transaction.
    pub async fn apply_authored_transaction(
        &mut self,
        authored: Vec<AuthoredMutation>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<AuthoredTransactionResult> {
        crate::heap_future::in_heap_measured(move |budget| async move {
                let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
                let result = async {
                    let input = Self::admit_authored_input(authored, &mut accounting)?;
                    let output = self.apply_authored_transaction_with_accounting(input, &mut accounting, cancellation).await?;
                    let crate::kernel::Owned { value, allocation, bytes } = output;
                    crate::kernel::retire_allocation(allocation, bytes, &mut accounting)?;
                    Ok(value)
                }.await;
                measured(result, &accounting)
            }, budget, cancellation, FsError::Cancelled(CancellationError), None).await
    }

    /// Applies a large ordered capture without exposing an intermediate candidate.
    pub(crate) async fn apply_authored_bulk_transaction(
        &mut self,
        authored: Vec<AuthoredMutation>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let input = Self::admit_authored_input(authored, &mut accounting)?;
            self.apply_authored_bulk_transaction_with_accounting(input, &mut accounting, cancellation).await
        }.await;
        measured(result, &accounting)
    }

    pub(super) fn admit_authored_input(
        authored: Vec<AuthoredMutation>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<crate::kernel::Owned<Vec<AuthoredMutation>>, FsError> {
        let bytes = authored_input_owned_bytes(&authored, authored.capacity())?;
        let allocation = accounting.claim_owned(bytes, 0)?;
        Ok(crate::kernel::Owned::new(authored, allocation, bytes))
    }

    /// Borrows producer-owned authored paths and opaque payloads on the original
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn apply_borrowed_authored_transaction_with_native_work(
        &mut self,
        authored: &[AuthoredMutation],
        slots_allocation: &crate::NativeAllocation,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(AuthoredTransactionResult, crate::NativeAllocation), FsError> {
        let input = (authored, slots_allocation);
        self.validate_native_work(original)?;
        let slots = input.0.len().checked_mul(size_of::<AuthoredMutation>())
            .map(crate::foundation::usize_to_u64).ok_or(WorkError::Overflow)?;
        if input.1.bytes() < slots || !input.1.work().shares_ledger(original) {
            return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
        }
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        let output = self.apply_authored_transaction_input_with_accounting(
            AuthoredTransactionInput::Borrowed(input.0), &mut accounting, cancellation,
        ).await?;
        let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = output.allocation else {
            unreachable!("native authored result retains original custody")
        };
        Ok((output.value, allocation))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn validate_authored_input(
        &self, input: &crate::kernel::Owned<Vec<AuthoredMutation>>, accounting: &KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        if let Some(original) = accounting.native_work() {
            self.validate_native_work(original)?;
            let bytes = authored_input_owned_bytes(&input.value, input.value.capacity())?;
            let KernelOwnedAllocation::NativeOriginal(guard) = &input.allocation else {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            };
            if guard.bytes() < bytes || !guard.work().shares_ledger(original) {
                return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
            }
        }
        Ok(())
    }

    pub(crate) async fn apply_authored_transaction_with_accounting(
        &mut self,
        input: crate::kernel::Owned<Vec<AuthoredMutation>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<AuthoredTransactionResult>, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        self.validate_authored_input(&input, accounting)?;
        let capacity = input.value.capacity();
        self.apply_authored_transaction_input_with_accounting(
            AuthoredTransactionInput::Owned {
                mutations: input.value.into_iter(), allocation: input.allocation, capacity,
            },
            accounting, cancellation,
        ).await
    }

    pub(super) async fn apply_authored_transaction_input_with_accounting(
        &mut self,
        mut input: AuthoredTransactionInput<'_>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<AuthoredTransactionResult>, FsError> {
        use crate::kernel::{Owned, split_allocation};
        accounting.check()?;
        let maximum = usize::try_from(self.volume.config.limits.maximum_mutations_per_batch).unwrap_or(usize::MAX);
        if input.mutations().len() > maximum || input.capacity() > maximum {
            return Err(FsError::TooManyPendingMutations);
        }
        let count = input.mutations().iter().try_fold(0_usize, |count, mutation| {
            count.checked_add(authored_mutation_operation_count(mutation))
        }).ok_or(FsError::TooManyPendingMutations)?;
        if count > maximum { return Err(FsError::TooManyPendingMutations); }
        let mut operations = Owned::<Vec<Mutation>>::allocate(count, accounting)?;
        let length = input.mutations().len();
        let mut created = Owned::<Vec<Option<FileId>>>::allocate(length, accounting)?;
        let mut metadata = AuthoredMetadataCache::new();
        for index in 0..length {
            let authored = match &mut input {
                AuthoredTransactionInput::Owned { mutations, allocation, .. } => {
                    let mutation = mutations.next().expect("preflight retained every authored mutation");
                    let bytes = mutation.logical_owned_bytes();
                    let allocation = split_allocation(allocation, bytes)?;
                    Owned::new(mutation, allocation, bytes)
                }
                AuthoredTransactionInput::Borrowed(mutations) => {
                    accounting.check()?;
                    clone_borrowed_authored_descriptor(&mutations[index], accounting)?
                }
            };
            let file_id = self.compile_authored_mutation_with_accounting(
                authored, &mut operations, &mut metadata, accounting, cancellation,
            ).await?;
            accounting.charge(WorkCounters {
                bytes_copied: crate::foundation::usize_to_u64(size_of::<Option<FileId>>()),
                ..WorkCounters::UNCHARGED
            })?;
            created.value.push(file_id);
        }
        input.retire(accounting)?;
        if operations.value.is_empty() {
            operations.retire(accounting)?;
        } else {
            if self.mode.access != AccessMode::ReadWrite
                || !matches!(self.mode.mutations, MutationMode::PrivateOverlay | MutationMode::DirectLive)
            { return Err(FsError::MutationNotAllowed); }
            self.mutate_owned_with_accounting(operations, accounting, cancellation).await?;
        }
        Ok(Owned::new(AuthoredTransactionResult { created_file_ids: created.value }, created.allocation, created.bytes))
    }

    pub(crate) async fn apply_authored_bulk_transaction_with_accounting(
        &mut self,
        input: crate::kernel::Owned<Vec<AuthoredMutation>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), FsError> {
        use crate::kernel::{Owned, retire_allocation, split_allocation};
        use crate::kernel::allocation::KernelOwnedAllocation;
        #[cfg(not(target_arch = "wasm32"))]
        self.validate_authored_input(&input, accounting)?;
        accounting.check()?;
        let maximum = usize::try_from(self.volume.config.limits.maximum_mutations_per_batch).unwrap_or(usize::MAX);
        if maximum == 0 { return Err(FsError::TooManyPendingMutations); }
        let mut candidate = self.private_candidate_with_accounting(accounting, cancellation)?;
        let backing = crate::foundation::usize_to_u64(input.value.capacity() * size_of::<AuthoredMutation>());
        let mut input = (input.value.into_iter(), input.allocation);
        let mut batch = Owned::<Vec<AuthoredMutation>>::allocate(0, accounting)?;
        let mut count = 0_usize;
        while let Some(mutation) = input.0.next() {
            let next = authored_mutation_operation_count(&mutation);
            if next > maximum { return Err(FsError::TooManyPendingMutations); }
            if !batch.value.is_empty() && (batch.value.len() == maximum || next > maximum - count) {
                let output = candidate.apply_authored_transaction_with_accounting(batch, accounting, cancellation).await?;
                output.retire(accounting)?;
                batch = Owned::<Vec<AuthoredMutation>>::allocate(0, accounting)?;
                count = 0;
            }
            batch.ensure_for_push(maximum, accounting)?;
            let bytes = mutation.logical_owned_bytes();
            let allocation = split_allocation(&mut input.1, bytes)?;
            let mut mutation = Owned::new(mutation, allocation, bytes);
            accounting.charge(WorkCounters {
                bytes_copied: crate::foundation::usize_to_u64(size_of::<AuthoredMutation>()),
                ..WorkCounters::UNCHARGED
            })?;
            batch.value.push(mutation.value);
            match (&mut batch.allocation, &mut mutation.allocation) {
                (KernelOwnedAllocation::Standalone, KernelOwnedAllocation::Standalone) => {}
                #[cfg(not(target_arch = "wasm32"))]
                (KernelOwnedAllocation::NativeOriginal(target), KernelOwnedAllocation::NativeOriginal(source)) => target.absorb(source)?,
                #[cfg(not(target_arch = "wasm32"))]
                _ => unreachable!("authored batches preserve one ledger"),
            }
            batch.bytes = batch.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
            count += next;
        }
        drop(input.0);
        if batch.value.is_empty() {
            batch.retire(accounting)?;
        } else {
            candidate.apply_authored_transaction_with_accounting(batch, accounting, cancellation).await?.retire(accounting)?;
        }
        let backing = match &input.1 {
            KernelOwnedAllocation::Standalone => backing,
            #[cfg(not(target_arch = "wasm32"))]
            KernelOwnedAllocation::NativeOriginal(guard) => guard.bytes(),
        };
        retire_allocation(input.1, backing, accounting)?;
        *self = candidate;
        Ok(())
    }

    pub(crate) fn bind_authored_operation(&mut self, operation_id: OperationId) {
        self.authored_operation_id = Some(operation_id);
    }

    /// The compiler's already-bound retry identity, not a publication grant.
    pub(crate) const fn bound_authored_operation_id(&self) -> Option<OperationId> {
        self.authored_operation_id
    }
}

