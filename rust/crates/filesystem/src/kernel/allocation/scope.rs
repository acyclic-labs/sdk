use super::KernelAccounting;

/// Retires an operation's discarded scratch after its physical temporaries drop.
/// Earlier scratch, independently retained backing and completed work survive.
pub(crate) struct AllocationScope<'w, 'a> {
    accounting: &'w mut KernelAccounting<'a>,
    floor: Option<u64>,
}

impl<'a> KernelAccounting<'a> {
    pub(crate) fn allocation_scope(&mut self) -> AllocationScope<'_, 'a> {
        let floor = self
            .standalone_parts()
            .map(|(allocations, ..)| allocations.scratch_bytes());
        AllocationScope {
            accounting: self,
            floor,
        }
    }
}

impl AllocationScope<'_, '_> {
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
            allocations
                .release_scratch_to(floor)
                .unwrap_or_else(|_| unreachable!("an operation retires only its own scratch"));
        }
    }
}
