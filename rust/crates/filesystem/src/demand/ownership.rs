#[cfg(feature = "native-watch")]
use super::*;
use crate::NativeMaterializationWork;
use crate::sync::atomic::{self as custody_atomics, AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
#[path = "ownership_accounting.rs"]
pub(super) mod accounting;

#[cfg(test)]
#[cfg(acyclic_fs_loom)]
#[path = "ownership_loom_tests.rs"]
mod loom_tests;

#[cfg(feature = "native-watch")]
#[derive(Debug, Default)]
pub(crate) enum SourcePayloadOwnership {
    #[default]
    Borrowed,
    Allocation {
        _allocation: crate::NativeAllocation,
    },
    Registered(Arc<NativeMountAllocationCustody>, AtomicU64),
}
#[cfg(feature = "native-watch")]
impl SourcePayloadOwnership {
    pub(crate) fn from_native_allocation(allocation: crate::NativeAllocation) -> Self {
        Self::Allocation {
            _allocation: allocation,
        }
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
    #[cfg(any(target_os = "linux", windows))]
    pub(crate) fn original_work(&self) -> Option<&NativeMaterializationWork> {
        match self {
            Self::Borrowed => None,
            Self::Allocation { _allocation } => Some(_allocation.work()),
            Self::Registered(custody, _) => custody.original.as_ref(),
        }
    }
    pub(crate) fn retain<W: SourceDemandReadWork>(
        &self,
        bytes: u64,
        work: &mut W,
    ) -> Result<(), DemandError> {
        match self {
            Self::Borrowed => Ok(()),
            Self::Allocation { .. } => Err(DemandError::InvalidRequest),
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
    /// Fund new watch storage on its owner, while charging copying to this call.
    /// A reused source can deliver a watch to a different invocation ledger.
    #[cfg(target_os = "linux")]
    pub(crate) fn claim_retained<W: SourceDemandReadWork>(
        &self,
        bytes: u64,
        delta: WorkCounters,
        work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        match self {
            Self::Borrowed => work.claim_allocation(bytes, u64::from(bytes != 0), delta),
            Self::Allocation { .. } => Err(DemandError::InvalidRequest),
            Self::Registered(custody, retained) => {
                work.admit(delta)?;
                let original = custody
                    .original
                    .as_ref()
                    .ok_or(DemandError::InvalidRequest)?;
                retained
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        current.checked_add(bytes)
                    })
                    .map_err(|_| crate::WorkError::Overflow)?;
                // These bytes are born directly in retained watch storage, not
                // in the transient capture that originally created the watch.
                let claimed = original.state(|owner| {
                    owner.claim_allocation(
                        bytes,
                        u64::from(bytes != 0),
                        WorkCounters::UNCHARGED,
                    )?;
                    if let Err(error) = custody.retain_allocation(bytes, owner, None) {
                        owner.release(bytes)?;
                        return Err(error.into());
                    }
                    Ok::<(), DemandError>(())
                });
                if let Err(error) = claimed {
                    retained.fetch_sub(bytes, Ordering::AcqRel);
                    return Err(error);
                }
                if let Err(error) = work.record(delta) {
                    self.retire(bytes)?;
                    return Err(error);
                }
                Ok(())
            }
        }
    }
    #[cfg(target_os = "linux")]
    pub(crate) fn discard_retained<W: SourceDemandReadWork>(
        &self,
        bytes: u64,
        work: &mut W,
    ) -> Result<(), DemandError> {
        if self.has_native_custody() {
            self.retire(bytes)
        } else {
            work.release(bytes)
        }
    }
    pub(crate) fn retire(&self, bytes: u64) -> Result<(), DemandError> {
        match self {
            Self::Borrowed => Ok(()),
            Self::Allocation { .. } => Err(DemandError::InvalidRequest),
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
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(crate) fn has_native_custody(&self) -> bool {
        !matches!(self, Self::Borrowed)
    }
    #[cfg(any(windows, target_os = "macos"))]
    pub(crate) fn split_owned_payload(
        &mut self,
        bytes: u64,
    ) -> Result<Self, crate::NativeTreeMaterializationError> {
        match self {
            Self::Borrowed => Ok(Self::Borrowed),
            Self::Allocation {
                _allocation: allocation,
            } => allocation
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
#[cfg(feature = "native-watch")]
impl Drop for SourcePayloadOwnership {
    fn drop(&mut self) {
        if let Self::Registered(custody, bytes) = self {
            #[cfg(acyclic_fs_loom)]
            let bytes = bytes.with_mut(|bytes| *bytes);
            #[cfg(not(acyclic_fs_loom))]
            let bytes = *bytes.get_mut();
            custody.retire_deferred(bytes);
        }
    }
}

// Arc retains two reference counters alongside the aligned gauge payload.
pub(super) const GAUGE_ALLOCATION_BYTES: u64 =
    std::mem::size_of::<([usize; 2], AtomicU64)>() as u64;

/// Physical bytes retained by one native allocation owner.
#[derive(Debug)]
pub struct NativeMountAllocationCustody {
    bytes: Option<Arc<AtomicU64>>,
    accounting_dirty: Arc<AtomicBool>,
    original: Option<NativeMaterializationWork>,
}
impl NativeMountAllocationCustody {
    pub(super) fn retained_bytes(&self) -> u64 {
        self.bytes
            .as_ref()
            .map_or(0, |bytes| bytes.load(Ordering::Acquire))
    }
    #[cfg(any(test, feature = "native-watch"))]
    pub(super) fn accounts_for(&self, gauge: &Arc<AtomicU64>) -> bool {
        self.bytes
            .as_ref()
            .is_some_and(|bytes| Arc::ptr_eq(gauge, bytes))
    }
    #[cfg(feature = "native-watch")]
    pub(crate) fn new<W: SourceDemandReadWork>(work: &mut W) -> Result<Arc<Self>, DemandError> {
        work.check()?;
        let original = work.native_source_work().map(|work| work.allocation_work());
        work.with_original_leaf(|work| {
            // Reject an unavailable owner footprint before allocating its gauge.
            work.admit_allocation(std::mem::size_of::<Self>() as u64, 1)?;
            // Like registry capacity and its dirty flag, this gauge is original
            // ledger metadata. It holds no invocation, resource or callbacks.
            let counter_bytes = GAUGE_ALLOCATION_BYTES;
            work.allocations
                .claim_bytes(counter_bytes, 1, &mut work.work, work.budget)
                .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
            let bytes = Arc::new(AtomicU64::new(0));
            let inserted = super::admissions::insert(
                &mut work.roots.native_mount_admissions,
                (bytes.clone(), 0),
                &mut work.allocations,
                &mut work.work,
                work.budget,
            );
            let index = match inserted {
                Ok(index) => index,
                Err(error) => {
                    drop(bytes);
                    work.allocations
                        .release(counter_bytes)
                        .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                    return Err(error);
                }
            };
            // A failed admission leaves this placeholder to be reclaimed.
            if let Some(dirty) = &work.roots.native_mount_dirty {
                accounting::publish(dirty);
            }
            work.scope(
                |work| &mut work.roots.native_mount_admission_pending,
                true,
                |work| {
                    let metadata = std::mem::size_of::<Self>() as u64;
                    let accounting_dirty = if let Some(dirty) = &work.roots.native_mount_dirty {
                        dirty.clone()
                    } else {
                        work.allocations
                            .claim_bytes(
                                std::mem::size_of::<AtomicBool>() as u64,
                                1,
                                &mut work.work,
                                work.budget,
                            )
                            .map_err(|error| error.into_work_or(DemandError::InvalidRequest))?;
                        let dirty = Arc::new(AtomicBool::new(true));
                        work.roots.native_mount_dirty = Some(dirty.clone());
                        dirty
                    };
                    work.claim_allocation(metadata, 1, WorkCounters::UNCHARGED)?;
                    bytes.store(metadata, Ordering::Release);
                    let custody = Arc::new(Self {
                        bytes: Some(bytes),
                        accounting_dirty,
                        original,
                    });
                    let slot = work
                        .roots
                        .native_mount_admissions
                        .0
                        .get_mut(index)
                        .ok_or(DemandError::InvalidRequest)?;
                    slot.1 = metadata;
                    Ok(custody)
                },
            )
        })
    }
    #[cfg(feature = "native-watch")]
    pub(crate) fn is_admitted(&self, work: &mut DemandWork) -> Result<bool, DemandError> {
        work.reconcile_cancellation_registry()?;
        for entry in work.roots.native_mount_admissions.0.iter() {
            work.work.charge(&super::admissions::SCAN, &work.budget)?;
            if self.accounts_for(&entry.0) {
                return Ok(true);
            }
        }
        Ok(false)
    }
    #[cfg(feature = "native-watch")]
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
    #[cfg(feature = "native-watch")]
    pub(crate) fn retain(&self, bytes: u64, work: &mut DemandWork) -> Result<(), crate::WorkError> {
        let capture = self
            .original
            .as_ref()
            .and_then(|work| work.capture_origin.as_ref())
            .and_then(std::sync::Weak::upgrade);
        self.retain_allocation(bytes, work, capture.as_deref())
    }
    #[cfg(feature = "native-watch")]
    fn retain_allocation(
        &self,
        bytes: u64,
        work: &mut DemandWork,
        capture: Option<&super::CaptureTransientCustody>,
    ) -> Result<(), crate::WorkError> {
        let entry = work
            .roots
            .native_mount_admissions
            .0
            .iter_mut()
            .find(|entry| self.accounts_for(&entry.0))
            .ok_or(crate::WorkError::Overflow)?;
        let adopted = entry
            .1
            .checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        self.retained_bytes()
            .checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        if let Some(capture) = capture {
            capture.transfer_retained_bytes(bytes)?;
        }
        self.bytes
            .as_ref()
            .ok_or(crate::WorkError::Overflow)?
            .fetch_add(bytes, Ordering::AcqRel);
        entry.1 = adopted;
        Ok(())
    }
    #[cfg(feature = "native-watch")]
    pub(crate) fn retire(&self, bytes: u64) -> Result<(), crate::WorkError> {
        accounting::retire_bytes::<true>(
            &self.accounting_dirty,
            self.bytes.as_ref().ok_or(crate::WorkError::Overflow)?,
            bytes,
            std::mem::size_of::<Self>() as u64,
        )?;
        self.retire_resource(bytes)
    }
    #[cfg(feature = "native-watch")]
    pub(crate) fn retire_deferred(&self, bytes: u64) {
        if let Err(error) = self.retire(bytes)
            && let Some(original) = &self.original
        {
            original.with_original_leaf(|work| {
                work.record_native_io_failure(error.into());
            });
        }
    }
    fn retire_resource(&self, bytes: u64) -> Result<(), crate::WorkError> {
        if let Some(original) = &self.original
            && let Some(resource) = &original.resource
            && let Err(error) = resource.retire(bytes)
        {
            original.with_original_leaf(|work| {
                work.record_native_io_failure(error);
            });
            return Err(crate::WorkError::Overflow);
        }
        Ok(())
    }
}
impl Drop for NativeMountAllocationCustody {
    fn drop(&mut self) {
        let _ = self.retire_resource(self.retained_bytes());
        // Release the custody's gauge reference before publishing cleanup. A
        // reconciler can now observe uniqueness without owning this destructor.
        accounting::retire(&self.accounting_dirty, || drop(self.bytes.take()));
    }
}

#[cfg(test)]
#[cfg(all(feature = "native-watch", any(target_os = "linux", windows)))]
mod properties {
    use super::*;
    use proptest::prelude::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn persistent_watch_growth_does_not_reenter_its_initial_capture() {
        let original = NativeMaterializationWork::new(
            crate::performance::BOUNDARY_BUDGET,
            &crate::CancellationToken::new(),
        )
        .unwrap();
        let capture = super::super::CaptureTransientCustody::new_native(&original).unwrap();
        let mut birth = original.with_capture_custody(&capture).unwrap();
        let owner = SourcePayloadOwnership::new(&mut birth).unwrap();
        let mut caller = NativeMaterializationWork::new(
            crate::performance::BOUNDARY_BUDGET,
            &crate::CancellationToken::new(),
        )
        .unwrap();
        // Settlement has already taken the initial capture's temporary bytes.
        let _ = capture.take_retained_bytes();
        owner
            .claim_retained(7, WorkCounters::UNCHARGED, &mut caller)
            .unwrap();
        assert_eq!(capture.take_retained_bytes(), 0);
        owner.discard_retained(7, &mut caller).unwrap();
        drop(capture);
        owner
            .claim_retained(11, WorkCounters::UNCHARGED, &mut caller)
            .unwrap();
        owner.discard_retained(11, &mut caller).unwrap();
    }

    proptest! {
        // Property: both native ownership representations retain their original
        // ledger and cancellation, independent of the payload size or caller.
        #[test]
        fn source_ownership_never_changes_the_original_context(bytes in any::<u16>()) {
            let stop = crate::CancellationToken::new();
            let original = NativeMaterializationWork::new(
                crate::performance::BOUNDARY_BUDGET, &stop,
            ).unwrap();
            let incoming = NativeMaterializationWork::new(
                crate::performance::BOUNDARY_BUDGET, &crate::CancellationToken::new(),
            ).unwrap();
            let mut registration_work = original.clone();
            let registered = SourcePayloadOwnership::new(&mut registration_work).unwrap();
            let allocated = SourcePayloadOwnership::from_native_allocation(
                original.claim(u64::from(bytes), 1, crate::WorkCounters::UNCHARGED).unwrap(),
            );
            prop_assert!(SourcePayloadOwnership::Borrowed.original_work().is_none());
            for owner in [&registered, &allocated] {
                let retained = owner.original_work().unwrap();
                prop_assert!(retained.shares_allocation_context(&original));
                prop_assert!(!retained.shares_allocation_context(&incoming));
            }
            stop.cancel();
            prop_assert!(registered.original_work().unwrap().remaining().is_err());
            prop_assert!(allocated.original_work().unwrap().remaining().is_err());
            prop_assert!(incoming.remaining().is_ok());
        }
    }
}
