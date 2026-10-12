use super::*;
use crate::NativeMaterializationWork;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Default)]
pub(crate) enum SourcePayloadOwnership {
    #[default]
    Borrowed,
    Allocation(crate::NativeAllocation),
    Registered(Arc<NativeMountAllocationCustody>, AtomicU64),
}
impl SourcePayloadOwnership {
    pub(crate) fn from_native_allocation(allocation: crate::NativeAllocation) -> Self {
        Self::Allocation(allocation)
    }
    pub(crate) fn new<W: SourceDemandReadWork>(work: &mut W) -> Result<Self, DemandError> {
        work.check()?;
        if work.native_source_work().is_some() {
            return Ok(Self::Registered(
                NativeMountAllocationCustody::new(work)?,
                AtomicU64::new(0),
            ));
        }
        Ok(Self::Borrowed)
    }
    pub(crate) fn retain<W: SourceDemandReadWork>(
        &self,
        bytes: u64,
        work: &mut W,
    ) -> Result<(), DemandError> {
        match self {
            Self::Borrowed => Ok(()),
            Self::Allocation(_) => Err(DemandError::InvalidRequest),
            Self::Registered(custody, retained) => {
                retained
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        current.checked_add(bytes)
                    })
                    .map_err(|_| crate::WorkError::Overflow)?;
                if let Err(error) = work.with_original_leaf(|work| custody.retain(bytes, work)) {
                    retained.fetch_sub(bytes, Ordering::AcqRel);
                    return Err(error.into());
                }
                Ok(())
            }
        }
    }
    pub(crate) fn retire(&self, bytes: u64) -> Result<(), DemandError> {
        match self {
            Self::Borrowed => Ok(()),
            Self::Allocation(_) => Err(DemandError::InvalidRequest),
            Self::Registered(custody, retained) => {
                retained
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        current.checked_sub(bytes)
                    })
                    .map_err(|_| crate::WorkError::Overflow)?;
                Ok(custody.retire(bytes)?)
            }
        }
    }
    pub(crate) fn has_native_custody(&self) -> bool {
        !matches!(self, Self::Borrowed)
    }
    pub(crate) fn split_owned_payload(
        &mut self,
        bytes: u64,
    ) -> Result<Self, crate::NativeTreeMaterializationError> {
        match self {
            Self::Borrowed => Ok(Self::Borrowed),
            Self::Allocation(allocation) => allocation
                .split_off(bytes)
                .map(Self::from_native_allocation),
            Self::Registered(custody, retained) => {
                retained
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        current.checked_sub(bytes)
                    })
                    .map_err(|_| crate::WorkError::Overflow)?;
                Ok(Self::Registered(custody.clone(), AtomicU64::new(bytes)))
            }
        }
    }
}
impl Drop for SourcePayloadOwnership {
    fn drop(&mut self) {
        if let Self::Registered(custody, bytes) = self {
            if let Err(error) = custody.retire(*bytes.get_mut()) {
                if let Some(original) = &custody.original {
                    original.with_original_leaf(|work| {
                        work.record_native_io_failure(error.into());
                    });
                }
            }
        }
    }
}

/// Physical bytes retained by one native allocation owner.
#[derive(Debug)]
pub struct NativeMountAllocationCustody {
    bytes: AtomicU64,
    original: Option<NativeMaterializationWork>,
}
impl NativeMountAllocationCustody {
    pub(super) fn retained_bytes(&self) -> u64 {
        self.bytes.load(Ordering::Acquire)
    }
    pub(crate) fn new<W: SourceDemandReadWork>(work: &mut W) -> Result<Arc<Self>, DemandError> {
        work.check()?;
        let original = work.native_source_work().map(|work| work.allocation_work());
        work.with_original_leaf(|work| {
            work.reconcile_cancellation_registry()?;
            let index = super::admissions::insert(
                &mut work.roots.native_mount_admissions,
                (std::sync::Weak::new(), 0),
                &mut work.allocations,
                &mut work.work,
                work.budget,
            )?;
            let metadata = std::mem::size_of::<Self>() as u64;
            work.claim_allocation(metadata, 1, WorkCounters::UNCHARGED)?;
            let custody = Arc::new(Self {
                bytes: AtomicU64::new(metadata),
                original,
            });
            work.roots.native_mount_admissions.0[index] = (Arc::downgrade(&custody), metadata);
            Ok(custody)
        })
    }
    pub(crate) fn is_admitted(&self, work: &mut DemandWork) -> Result<bool, DemandError> {
        work.reconcile_cancellation_registry()?;
        for entry in work.roots.native_mount_admissions.0.iter() {
            work.work.charge(
                &WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                },
                &work.budget,
            )?;
            if std::ptr::eq(entry.0.as_ptr(), self) {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub(crate) fn claim(&self, bytes: u64, work: &mut DemandWork) -> Result<(), DemandError> {
        work.check()?;
        self.retained_bytes()
            .checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        if !self.is_admitted(work)? {
            return Err(DemandError::InvalidRequest);
        }
        work.claim_allocation(bytes, u64::from(bytes != 0), WorkCounters::UNCHARGED)?;
        self.retain(bytes, work).map_err(DemandError::from)
    }
    pub(crate) fn retain(&self, bytes: u64, work: &mut DemandWork) -> Result<(), crate::WorkError> {
        let entry = work
            .roots
            .native_mount_admissions
            .0
            .iter_mut()
            .find(|entry| std::ptr::eq(entry.0.as_ptr(), self))
            .ok_or(crate::WorkError::Overflow)?;
        let adopted = entry
            .1
            .checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        self.retained_bytes()
            .checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        if let Some(capture) = self
            .original
            .as_ref()
            .and_then(|work| work.capture_origin.as_ref())
            .and_then(std::sync::Weak::upgrade)
        {
            capture.transfer_retained_bytes(bytes)?;
        }
        self.bytes.fetch_add(bytes, Ordering::AcqRel);
        entry.1 = adopted;
        Ok(())
    }
    pub(crate) fn retire(&self, bytes: u64) -> Result<(), crate::WorkError> {
        self.bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_sub(bytes)
                    .filter(|current| *current >= std::mem::size_of::<Self>() as u64)
            })
            .map_err(|_| crate::WorkError::Overflow)?;
        if let Some(resource) = self
            .original
            .as_ref()
            .and_then(|work| work.resource.as_ref())
        {
            if let Err(error) = resource.retire(bytes) {
                self.original
                    .as_ref()
                    .expect("resource owner")
                    .with_original_leaf(|work| {
                        work.record_native_io_failure(error);
                    });
                return Err(crate::WorkError::Overflow);
            }
        }
        Ok(())
    }
}
impl Drop for NativeMountAllocationCustody {
    fn drop(&mut self) {
        if let Some(original) = &self.original {
            if let Some(resource) = &original.resource {
                if let Err(error) = resource.retire(self.retained_bytes()) {
                    original.with_original_leaf(|work| {
                        work.record_native_io_failure(error);
                    });
                }
            }
        }
    }
}
