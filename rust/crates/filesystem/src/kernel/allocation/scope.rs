use super::KernelAccounting;

/// Retires an operation's discarded scratch after its physical temporaries drop.
/// Earlier scratch, independently retained backing and completed work survive.
/// Scratch handed to an owned continuation remains charged to its original ledger.
pub(crate) struct AllocationScope<'w, 'a> {
    accounting: &'w mut KernelAccounting<'a>,
    floor: Option<u64>,
}

impl<'a> KernelAccounting<'a> {
    /// A handed-off caller cannot retire scratch owned by its continuation.
    pub(crate) fn allocation_scope(&mut self) -> Result<AllocationScope<'_, 'a>, crate::FsError> {
        self.ensure_active()?;
        let floor = self
            .standalone_parts()
            .map(|(allocations, ..)| allocations.scratch_bytes());
        Ok(AllocationScope {
            accounting: self,
            floor,
        })
    }
}

impl AllocationScope<'_, '_> {
    /// Keeps only the already funded output; discarded temporaries retire here.
    pub(crate) fn keep_bytes(&mut self, bytes: u64) -> Result<(), crate::FsError> {
        if let Some(floor) = self.floor {
            let floor = floor.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
            let (allocations, ..) = self
                .accounting
                .standalone_parts()
                .unwrap_or_else(|| unreachable!("ordinary output keeps its original ledger"));
            if floor > allocations.scratch_bytes() {
                return Err(super::AllocationError::ReleaseInvariant
                    .into_work_or(crate::FsError::PendingMutationAllocationFailed));
            }
            self.floor = Some(floor);
        }
        Ok(())
    }

    /// Successful output keeps its existing caller-owned allocation charges.
    pub(crate) fn keep(mut self) {
        self.floor = None;
    }
}

impl<'a> std::ops::Deref for AllocationScope<'_, 'a> {
    type Target = KernelAccounting<'a>;

    fn deref(&self) -> &Self::Target {
        self.accounting
    }
}

impl std::ops::DerefMut for AllocationScope<'_, '_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.accounting
    }
}

impl Drop for AllocationScope<'_, '_> {
    fn drop(&mut self) {
        if let Some(floor) = self.floor {
            let (allocations, ..) = self
                .accounting
                .standalone_parts()
                .unwrap_or_else(|| unreachable!("ordinary scratch keeps its original ledger"));
            if matches!(allocations.state, super::LedgerState::Suspended(..)) {
                return;
            }
            allocations
                .release_scratch_to(floor)
                .unwrap_or_else(|_| unreachable!("an operation retires only its own scratch"));
        }
    }
}
