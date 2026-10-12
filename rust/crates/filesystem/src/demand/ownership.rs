use super::*;

#[derive(Default)]
pub(crate) struct SourcePayloadOwnership {
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::demand) custody: Option<NativeMountAllocationCustody>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::demand) owned_bytes: std::sync::atomic::AtomicU64,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::demand) allocation: Option<crate::NativeAllocation>,
}

impl SourcePayloadOwnership {
    pub(in crate::demand) fn from_kernel_allocation(allocation: crate::kernel::allocation::KernelOwnedAllocation) -> Self {
        match allocation {
            crate::kernel::allocation::KernelOwnedAllocation::Standalone => Self::default(),
            #[cfg(not(target_arch = "wasm32"))]
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => Self::from_native_allocation(allocation),
        }
    }
}

impl Clone for SourcePayloadOwnership {
    fn clone(&self) -> Self { Self::default() }
}

impl PartialEq for SourcePayloadOwnership {
    fn eq(&self, _other: &Self) -> bool { true }
}

impl Eq for SourcePayloadOwnership {}

impl std::fmt::Debug for SourcePayloadOwnership {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SourcePayloadOwnership")
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl SourcePayloadOwnership {
    pub(crate) fn from_native_allocation(allocation: crate::NativeAllocation) -> Self {
            Self { custody: None, owned_bytes: Default::default(), allocation: Some(allocation) }
        }

    pub(crate) fn into_native_allocation(mut self) -> Option<crate::NativeAllocation> {
            self.allocation.take()
        }

    pub(crate) fn new<W: SourceDemandReadWork>(work: &mut W) -> Result<Self, DemandError> {
            work.check()?;
            if work.is_native_original() {
                let source = work.native_source_work().ok_or(DemandError::NativeWorkUnavailable)?;
                let original = source.owned().allocation_work();
                let custody = NativeMountAllocationCustody::new(&mut NativeSourceWork::from_native_materialization_work(&original))?;
                return Ok(Self { custody: Some(custody), owned_bytes: Default::default(), allocation: None });
            }
            Ok(Self::default())
        }

    pub(crate) fn retain<W: SourceDemandReadWork>(&self, bytes: u64, work: &mut W) -> Result<(), DemandError> {
            if self.allocation.is_some() { return Err(DemandError::InvalidRequest); }
            if let Some(custody) = &self.custody {
                self.owned_bytes.fetch_update(
                    std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
                    |retained| retained.checked_add(bytes),
                ).map_err(|_| crate::WorkError::Overflow)?;
                if let Err(error) = work.with_original_leaf(|work| custody.retain_completed_admitted(bytes, work)) {
                    self.owned_bytes.fetch_sub(bytes, std::sync::atomic::Ordering::AcqRel);
                    return Err(error.into());
                }
                return Ok(());
            }
            Ok(())
        }

    pub(crate) fn retire(&self, bytes: u64) -> Result<(), DemandError> {
            if self.allocation.is_some() { return Err(DemandError::InvalidRequest); }
            if let Some(custody) = &self.custody {
                self.owned_bytes.fetch_update(
                    std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
                    |retained| retained.checked_sub(bytes),
                ).map_err(|_| crate::WorkError::Overflow)?;
                custody.retire(bytes)?;
            }
            Ok(())
        }

    pub(crate) fn has_native_custody(&self) -> bool {
            self.custody.is_some() || self.allocation.is_some()
        }

    pub(crate) fn split_owned_payload(
            &mut self, bytes: u64,
        ) -> Result<Self, crate::NativeTreeMaterializationError> {
            if let Some(allocation) = &mut self.allocation {
                return allocation.split_off(bytes).map(Self::from_native_allocation);
            }
            if let Some(custody) = &self.custody {
                self.owned_bytes.fetch_update(
                    std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
                    |retained| retained.checked_sub(bytes),
                ).map_err(|_| crate::WorkError::Overflow)?;
                return Ok(Self {
                    custody: Some(custody.clone()),
                    owned_bytes: std::sync::atomic::AtomicU64::new(bytes),
                    allocation: None,
                });
            }
            if self.owned_bytes.load(std::sync::atomic::Ordering::Acquire) != 0 {
                return Err(crate::WorkError::Overflow.into());
            }
            Ok(Self::default())
        }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for SourcePayloadOwnership {
    fn drop(&mut self) {
        let bytes = *self.owned_bytes.get_mut();
        if bytes != 0 && let Some(custody) = &self.custody
            && let Err(error) = custody.retire(bytes)
            && let Some(original) = &custody.live.original {
            let _recorded = original.source_leaf(|work|
                work.record_native_io_failure(DemandError::Work(error)));
        }
    }
}

/// Physical ownership of known native SDK payloads admitted by one original
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct NativeMountAllocationCustody {
    pub(in crate::demand) live: Arc<NativeMountAllocationCustodyData>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::demand) struct NativeMountAllocationCustodyData {
    pub(in crate::demand) bytes: std::sync::atomic::AtomicU64,
    pub(in crate::demand) original: Option<crate::NativeMaterializationWork>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeMountAllocationCustodyData {
    fn drop(&mut self) {
        if let Some(original) = &self.original
            && let Some(resource) = original.resource.as_ref()
            && let Err(error) = resource.retire(self.bytes.load(std::sync::atomic::Ordering::Acquire)) {
            let _recorded = original.source_leaf(|work| work.record_native_io_failure(error));
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::demand) struct NativeMountAllocationAdmission {
    pub(in crate::demand) live: std::sync::Weak<NativeMountAllocationCustodyData>,
    pub(in crate::demand) adopted: u64,
    pub(in crate::demand) known_control: Option<NativeMountControlAllocation>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type NativeMountControlAllocation = std::sync::Weak<dyn std::any::Any + Send + Sync>;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Default)]
pub(crate) struct NativeMountAllocationAdmissions {
    pub(in crate::demand) first: Option<NativeMountAllocationAdmission>,
    pub(in crate::demand) more: Vec<NativeMountAllocationAdmission>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeMountAllocationAdmissions {
    pub(crate) fn freed_bytes(&self) -> u64 {
            self.first.iter().chain(self.more.iter()).try_fold(0_u64, |freed, entry| {
                let actual = entry.live.upgrade().map_or(0, |live|
                    live.bytes.load(std::sync::atomic::Ordering::Acquire));
                freed.checked_add(entry.adopted.checked_sub(actual)?)
            }).expect("native physical retirement cannot exceed original admitted ownership")
        }

    pub(crate) fn reconcile(
            &mut self, allocations: &mut crate::kernel::allocation::AllocationLedger,
        ) -> Result<(), crate::WorkError> {
            for entry in self.first.iter_mut().chain(self.more.iter_mut()) {
                let actual = match entry.live.upgrade() {
                    Some(live) => live.bytes.load(std::sync::atomic::Ordering::Acquire),
                    None => {
                        drop(std::mem::take(&mut entry.live));
                        0
                    }
                };
                let retired = entry.adopted.checked_sub(actual).ok_or(crate::WorkError::Overflow)?;
                allocations.release(retired).map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
                entry.adopted = actual;
            }
            Ok(())
        }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeMountAllocationCustody {
    pub(crate) fn new<W: SourceDemandReadWork>(work: &mut W) -> Result<Self, DemandError> {
            work.check()?;
            let original = work.native_source_work().map(|work| work.owned());
            let keeper = original.as_ref().map(crate::NativeMaterializationWork::allocation_work);
            work.with_original_leaf(|work| {
                work.reconcile_cancellation_registry()?;
                let admissions = &mut work.cancellation_wait.native_mount_admissions;
                if admissions.first.is_some() && admissions.more.len() == admissions.more.capacity() {
                    let capacity = admissions.more.capacity().max(1).checked_mul(2).ok_or(crate::WorkError::Overflow)?;
                    grow_registry(&mut admissions.more, capacity, &mut work.allocations, &mut work.work, work.budget)?;
                }
                let copied = WorkCounters {
                    bytes_copied: if work.cancellation_wait.native_mount_admissions.first.is_some() {
                        std::mem::size_of::<NativeMountAllocationAdmission>() as u64
                    } else { 0 },
                    ..WorkCounters::UNCHARGED
                };
                let metadata = std::mem::size_of::<NativeMountAllocationCustodyData>() as u64;
                let resource = original.as_ref().and_then(|_| work.native_resource.clone());
                work.with_resource_scope(resource, |work| work.claim_native_owned(metadata, 1, copied, &None))?;
                let live = Arc::new(NativeMountAllocationCustodyData {
                    bytes: std::sync::atomic::AtomicU64::new(metadata),
                    original: keeper,
                });
                let admissions = &mut work.cancellation_wait.native_mount_admissions;
                let admission = NativeMountAllocationAdmission {
                    live: Arc::downgrade(&live), adopted: metadata, known_control: None,
                };
                if admissions.first.is_none() { admissions.first = Some(admission); }
                else { admissions.more.push(admission); }
                Ok(Self { live })
            })
        }

    pub(crate) fn attach_known_control(
            &self, owner: NativeMountControlAllocation, work: &mut crate::demand::DemandWork,
        ) -> Result<(), DemandError> {
            if owner.strong_count() == 0 { return Err(crate::DemandError::InvalidRequest.into()); }
            let (admissions, _, counters, budget) = work.native_mount_admission_parts()?;
            for entry in admissions.first.iter_mut().chain(admissions.more.iter_mut()) {
                let step = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
                counters.charge(&step, &budget)?;
                if entry.live.as_ptr() == Arc::as_ptr(&self.live) {
                    if entry.known_control.is_some() { return Err(crate::DemandError::InvalidRequest.into()); }
                    entry.known_control = Some(owner);
                    return Ok(());
                }
            }
            Err(crate::DemandError::InvalidRequest.into())
        }

    pub(crate) fn from_admitted(
            bytes: u64, work: &mut crate::demand::DemandWork,
        ) -> Result<Self, DemandError> {
            let custody = Self::new(work)?;
            custody.retain_admitted(bytes, work)?;
            Ok(custody)
        }

    pub(crate) fn is_admitted(
            &self, work: &mut crate::demand::DemandWork,
        ) -> Result<bool, DemandError> {
            let (admissions, _, counters, budget) = work.native_mount_admission_parts()?;
            for entry in admissions.first.iter().chain(admissions.more.iter()) {
                let step = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
                counters.charge(&step, &budget)?;
                if entry.live.as_ptr() == Arc::as_ptr(&self.live) { return Ok(true); }
            }
            Ok(false)
        }

    pub(crate) fn claim(
            &self, bytes: u64, work: &mut crate::demand::DemandWork,
        ) -> Result<(), DemandError> {
            work.check()?;
            let adopted = {
                let (admissions, _, _, _) = work.native_mount_admission_parts()?;
                let entry = admissions.first.iter().chain(admissions.more.iter())
                    .find(|entry| entry.live.as_ptr() == Arc::as_ptr(&self.live))
                    .ok_or(DemandError::InvalidRequest)?;
                entry.adopted.checked_add(bytes).ok_or(crate::WorkError::Overflow)?
            };
            self.live.bytes.load(std::sync::atomic::Ordering::Acquire).checked_add(bytes)
                .ok_or(crate::WorkError::Overflow)?;
            work.claim_native_owned(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED, &None)?;
            let (admissions, _, _, _) = work.native_mount_completed_admission_parts();
            let entry = admissions.first.iter_mut().chain(admissions.more.iter_mut())
                .find(|entry| entry.live.as_ptr() == Arc::as_ptr(&self.live))
                .ok_or(DemandError::InvalidRequest)?;
            self.live.bytes.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
            entry.adopted = adopted;
            Ok(())
        }

    pub(crate) fn retain_admitted(
            &self, bytes: u64, work: &mut crate::demand::DemandWork,
        ) -> Result<(), DemandError> {
            work.check()?;
            self.retain_completed_admitted(bytes, work).map_err(DemandError::from)
        }

    pub(crate) fn retain_completed_admitted(
            &self, bytes: u64, work: &mut crate::demand::DemandWork,
        ) -> Result<(), crate::WorkError> {
            let (admissions, _, _, _) = work.native_mount_completed_admission_parts();
            let entry = admissions.first.iter_mut().chain(admissions.more.iter_mut())
                .find(|entry| entry.live.as_ptr() == Arc::as_ptr(&self.live))
                .ok_or(crate::WorkError::Overflow)?;
            let adopted = entry.adopted.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
            self.live.bytes.load(std::sync::atomic::Ordering::Acquire).checked_add(bytes)
                .ok_or(crate::WorkError::Overflow)?;
            if let Some(capture) = self.live.original.as_ref().and_then(|original| original.capture_origin.as_ref()).and_then(std::sync::Weak::upgrade) {
                capture.transfer_retained_bytes(bytes)?;
            }
            self.live.bytes.fetch_add(bytes, std::sync::atomic::Ordering::AcqRel);
            entry.adopted = adopted;
            Ok(())
        }

    pub(crate) fn retire(&self, bytes: u64) -> Result<(), crate::WorkError> {
            let metadata = std::mem::size_of::<NativeMountAllocationCustodyData>() as u64;
            self.live.bytes.fetch_update(
                std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
                |live| live.checked_sub(bytes).filter(|live| *live >= metadata),
            ).map_err(|_| crate::WorkError::Overflow)?;
            if let Some(original) = &self.live.original
                && let Some(resource) = original.resource.as_ref()
                && let Err(error) = resource.retire(bytes) {
                let _recorded = original.source_leaf(|work| work.record_native_io_failure(error));
                return Err(crate::WorkError::Overflow);
            }
            Ok(())
        }
}
