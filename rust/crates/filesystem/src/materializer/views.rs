use super::*;
use std::sync::{Arc, Weak};

pub(crate) struct NativeInvocationView {
    pub(super) cancellation: crate::CancellationToken,
    pub(super) self_ref: Weak<Self>,
    pub(super) metadata: NativeAllocation,
    pub(super) read_credit: NativeResourceCredit,
}
impl NativeInvocationView {
    pub(crate) fn check(&self) -> Result<(), crate::DemandError> {
        self.cancellation
            .check()
            .map_err(|_| crate::DemandError::Cancelled)
    }
    pub(crate) fn owns_resource(&self, resource: Option<&Arc<NativeResourceAllowance>>) -> bool {
        match (&self.metadata.work().resource, resource) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        }
    }
    pub(super) fn with_original_leaf<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self.metadata.work().state(|state| {
            state.scope(
                |work| &mut work.native_invocation,
                Some(self.self_ref.clone()),
                operation,
            )
        })
    }
}
#[cfg(feature = "native-mount")]
pub(crate) struct NativeSourceExecutionView {
    pub(super) lease: crate::native_mount::NativeSourceIoLease,
    pub(super) metadata: NativeAllocation,
}
#[cfg(feature = "native-mount")]
impl NativeSourceExecutionView {
    pub(super) fn with_original_leaf<T>(
        &self,
        operation: impl FnOnce(&mut crate::demand::DemandWork) -> T,
    ) -> T {
        self.metadata.work().state(operation)
    }
}
impl NativeMaterializationWork {
    /// Adds invocation cancellation on the existing ledger.
    pub fn invocation_view(
        &self,
    ) -> Result<(Self, crate::CancellationToken), NativeTreeMaterializationError> {
        let cancellation =
            crate::CancellationToken::linked_child_with_native_work(self.cancellation(), self)?;
        let mut work = self.allocation_work();
        work.bind_invocation(cancellation.clone(), NativeResourceCredit::ReadBytes)?;
        Ok((self.retain_source(work)?, cancellation))
    }
    pub(crate) fn bind_invocation(
        &mut self,
        cancellation: crate::CancellationToken,
        read_credit: NativeResourceCredit,
    ) -> Result<(), NativeTreeMaterializationError> {
        let bytes = const {
            match crate::arc_layout::allocation_bytes(
                std::alloc::Layout::new::<NativeInvocationView>(),
            ) {
                Ok(bytes) => bytes as u64,
                Err(_) => panic!("invocation view Arc layout overflow"),
            }
        };
        let metadata = self.claim(bytes, 1, crate::WorkCounters::UNCHARGED)?;
        self.invocation = Some(Arc::new_cyclic(|self_ref| NativeInvocationView {
            cancellation,
            metadata,
            self_ref: self_ref.clone(),
            read_credit,
        }));
        Ok(())
    }
    #[cfg(any(test, feature = "distributed"))]
    pub(crate) fn authority_read_allocation(
        &self,
    ) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        let mut work = self.allocation_work();
        work.bind_invocation(
            self.cancellation().clone(),
            NativeResourceCredit::AuthorityReadBytes,
        )?;
        Ok(NativeAllocation {
            meter: work,
            bytes: 0,
        })
    }
    #[cfg(any(test, feature = "distributed"))]
    pub(crate) fn authority_read_meter_for(
        &self,
        original: &Self,
    ) -> Option<Arc<dyn acyclic_native_runtime::NativeReadMeter>> {
        let bound = self.authority_read_original()?;
        let same_invocation = match (&bound.invocation, &original.invocation) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        let same_capture = match (&bound.capture_origin, &original.capture_origin) {
            (Some(a), Some(b)) => Weak::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        (bound.shares_allocation_context(original)
            && bound.cancellation().same_state(original.cancellation())
            && same_invocation
            && same_capture)
            .then(|| self.runtime_meter())
    }
    #[cfg(any(test, feature = "distributed"))]
    pub(crate) fn authority_read_original(&self) -> Option<&Self> {
        self.invocation.as_ref().and_then(|view| {
            matches!(view.read_credit, NativeResourceCredit::AuthorityReadBytes)
                .then(|| view.metadata.work())
        })
    }
    /// Adds a finite resource ceiling without changing the original ledger.
    pub fn native_volume_work(
        &self,
        budget: crate::WorkBudget,
    ) -> Result<Self, NativeTreeMaterializationError> {
        let bytes = const {
            match crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<
                NativeResourceAllowance,
            >()) {
                Ok(bytes) => bytes as u64,
                Err(_) => panic!("resource allowance Arc layout overflow"),
            }
        };
        let metadata = self.claim_root_metadata(bytes)?;
        let resource = Arc::new_cyclic(|self_ref| NativeResourceAllowance {
            budget,
            metadata,
            self_ref: self_ref.clone(),
            parent: self.resource.clone(),
            state: crate::sync::Mutex::new(NativeResourceState::default()),
        });
        let mut work = self.allocation_work();
        work.resource = Some(resource);
        work.capture_origin = None;
        self.retain_source(work)
    }
    #[cfg(any(test, feature = "native-watch"))]
    pub(crate) fn with_capture_custody(
        &self,
        capture: &Arc<crate::demand::CaptureTransientCustody>,
    ) -> Result<Self, NativeTreeMaterializationError> {
        if !capture.owns_native_work(self) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let mut work = self.allocation_work();
        work.capture_origin = Some(Arc::downgrade(capture));
        self.retain_source(work)
    }
    fn retain_source(&self, work: Self) -> Result<Self, NativeTreeMaterializationError> {
        #[cfg(feature = "native-mount")]
        if let Some(source) = &self.execution {
            return work.with_source_io_lease(source.lease.clone());
        }
        Ok(work)
    }
    /// Keeps accepted source I/O alive through its native completion.
    #[cfg(feature = "native-mount")]
    pub fn with_source_io_lease(
        &self,
        lease: crate::native_mount::NativeSourceIoLease,
    ) -> Result<Self, NativeTreeMaterializationError> {
        let mut work = self.allocation_work();
        if let Some(source) = lease.original_work() {
            if !work.shares_ledger(source) {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
            work.resource = match (&work.resource, &source.resource) {
                (None, resource) | (resource, None) => resource.clone(),
                (Some(a), Some(b)) if a.contains_scope(b) => Some(a.clone()),
                (Some(a), Some(b)) if b.contains_scope(a) => Some(b.clone()),
                _ => return Err(NativeTreeMaterializationError::InvalidLayout),
            };
        }
        if let Some(view) = &work.invocation
            && !view.owns_resource(work.resource.as_ref())
        {
            work.bind_invocation(view.cancellation.clone(), view.read_credit)?;
        }
        let metadata = work.claim(
            std::mem::size_of::<NativeSourceExecutionView>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        work.execution = Some(Arc::new(NativeSourceExecutionView { lease, metadata }));
        Ok(work)
    }
}

#[cfg(test)]
#[cfg(not(acyclic_fs_loom))]
pub(super) mod tests;
