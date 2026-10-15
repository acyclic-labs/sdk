use super::*;
use parking_lot::Mutex;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::AtomicBool;
use std::sync::{
    Weak,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Default)]
pub(crate) struct ProofDataCustody {
    retained: AtomicU64,
    physical_retained: AtomicU64,
    #[cfg(not(target_arch = "wasm32"))]
    native_accounting_dirty: AtomicBool,
    completed_data_work: Mutex<ProofDataWorkReport>,
}

#[derive(Debug, Default)]
struct ProofDataWorkReport {
    pending: WorkCounters,
    reported_retained_bytes: u64,
    reported_physical_bytes: u64,
    delivery_generation: u64,
    live_future_bytes: u64,
    live_cache_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    native_original: Option<Weak<crate::demand::NativeDemandReadMeter>>,
    #[cfg(not(target_arch = "wasm32"))]
    native_control_born: u128,
    #[cfg(not(target_arch = "wasm32"))]
    native_control_retired: u128,
}

impl ProofDataCustody {
    fn payload_changed(&self, completed: &ProofDataWorkReport) {
        #[cfg(not(target_arch = "wasm32"))]
        if completed.native_original.is_some() {
            self.retry_native_accounting();
        }
        #[cfg(target_arch = "wasm32")]
        let _ = completed;
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn retry_native_accounting(&self) {
        self.native_accounting_dirty.store(true, Ordering::Release);
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native_accounting_pending(&self) -> bool {
        self.native_accounting_dirty.load(Ordering::Acquire)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn take_native_accounting_change(&self) -> Result<Option<(u128, u64)>, WorkError> {
        if !self.native_accounting_pending()
            || !self.native_accounting_dirty.swap(false, Ordering::AcqRel)
        {
            return Ok(None);
        }
        let snapshot = (|| {
            let completed = self.completed_data_work.lock();
            let born = completed.native_control_born;
            let retired = completed.native_control_retired;
            let native_live = u64::try_from(born.checked_sub(retired).ok_or(WorkError::Overflow)?)
                .map_err(|_| WorkError::Overflow)?;
            let non_native = self
                .retained_payload_bytes()
                .checked_sub(native_live)
                .ok_or(WorkError::Overflow)?;
            Ok((retired, non_native))
        })();
        if snapshot.is_err() {
            self.native_accounting_dirty.store(true, Ordering::Release);
        }
        snapshot.map(Some)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn bind_native_original(
        &self,
        original: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> bool {
        let mut completed = self.completed_data_work.lock();
        let bound = if let Some(bound) = completed.native_original.as_ref() {
            bound.as_ptr() == Arc::as_ptr(original)
        } else {
            completed.native_original = Some(Arc::downgrade(original));
            true
        };
        if bound {
            self.retry_native_accounting();
        }
        bound
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn is_native_original(
        &self,
        original: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> bool {
        self.completed_data_work
            .lock()
            .native_original
            .as_ref()
            .is_some_and(|bound| bound.as_ptr() == Arc::as_ptr(original))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native_control_events(&self) -> (u128, u128) {
        let completed = self.completed_data_work.lock();
        (
            completed.native_control_born,
            completed.native_control_retired,
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[allow(
        clippy::expect_used,
        reason = "Claims are admitted before attachment and retire exactly once with their owned data."
    )]
    pub(crate) fn non_native_retained_payload_bytes(&self) -> u64 {
        let completed = self.completed_data_work.lock();
        let native_live =
            u64::try_from(completed.native_control_born - completed.native_control_retired)
                .expect("live native primitives fit their original allocation ledger");
        self.retained_payload_bytes()
            .checked_sub(native_live)
            .expect("native live primitives are included in actual payload custody")
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        self.retained.load(Ordering::Acquire)
    }

    pub(crate) fn physical_retained_bytes(&self) -> u64 {
        self.physical_retained.load(Ordering::Acquire)
    }

    #[allow(
        clippy::expect_used,
        reason = "Claims are admitted before attachment and retire exactly once with their owned data."
    )]
    pub(crate) fn retained_payload_bytes(&self) -> u64 {
        self.retained_bytes()
            .checked_add(self.physical_retained_bytes())
            .expect("admitted proof payloads fit their original allocation ledger")
    }

    #[allow(
        clippy::expect_used,
        reason = "Claims are admitted before attachment and retire exactly once with their owned data."
    )]
    pub(crate) fn live_payload_counts(&self) -> (u64, u64) {
        let completed = self.completed_data_work.lock();
        let standing = completed
            .reported_retained_bytes
            .checked_add(completed.reported_physical_bytes)
            .expect("reported payloads fit their original allocation ledger");
        (self.retained_payload_bytes(), standing)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) const fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }

    pub(crate) fn record_data_work(&self, mut work: WorkCounters) -> Result<(), WorkError> {
        let mut completed = self.completed_data_work.lock();
        work.peak_allocation_bytes = work
            .peak_allocation_bytes
            .checked_add(completed.live_future_bytes)
            .ok_or(WorkError::Overflow)?;
        let pending = completed.pending.checked_add(work)?;
        completed.pending = pending;
        Ok(())
    }

    pub(crate) fn record_completed_leaf_work(
        &self,
        mut work: WorkCounters,
    ) -> Result<(), WorkError> {
        let mut completed = self.completed_data_work.lock();
        let data = self
            .retained_payload_bytes()
            .checked_sub(completed.reported_retained_bytes)
            .and_then(|bytes| bytes.checked_sub(completed.reported_physical_bytes))
            .ok_or(WorkError::Overflow)?;
        work.peak_allocation_bytes = work
            .peak_allocation_bytes
            .checked_add(completed.live_future_bytes)
            .and_then(|bytes| bytes.checked_add(completed.live_cache_bytes))
            .and_then(|bytes| bytes.checked_add(data))
            .ok_or(WorkError::Overflow)?;
        let pending = completed.pending.checked_add(work)?;
        completed.pending = pending;
        Ok(())
    }

    pub(crate) fn record_operation_clone_work(
        &self,
        bytes: u64,
        uncached_live_bytes: u64,
    ) -> Result<(), WorkError> {
        if bytes == 0 {
            return Ok(());
        }
        let mut completed = self.completed_data_work.lock();
        let data = self
            .retained_payload_bytes()
            .checked_sub(completed.reported_retained_bytes)
            .and_then(|bytes| bytes.checked_sub(completed.reported_physical_bytes))
            .ok_or(WorkError::Overflow)?;
        let peak = completed
            .live_future_bytes
            .checked_add(completed.live_cache_bytes)
            .and_then(|live| live.checked_add(data))
            .and_then(|live| live.checked_add(uncached_live_bytes))
            .and_then(|live| live.checked_add(bytes))
            .ok_or(WorkError::Overflow)?;
        let work = WorkCounters {
            allocation_operations: 1,
            bytes_copied: bytes,
            peak_allocation_bytes: peak,
            ..WorkCounters::default()
        };
        let pending = completed.pending.checked_add(work)?;
        completed.pending = pending;
        Ok(())
    }

    pub(crate) fn operation_cache_bytes(&self) -> u64 {
        self.completed_data_work.lock().live_cache_bytes
    }

    pub(crate) fn add_operation_cache_bytes(&self, bytes: u64) -> Result<(), WorkError> {
        let mut completed = self.completed_data_work.lock();
        completed.live_cache_bytes = completed
            .live_cache_bytes
            .checked_add(bytes)
            .ok_or(WorkError::Overflow)?;
        Ok(())
    }

    #[allow(
        clippy::expect_used,
        reason = "Claims are admitted before attachment and retire exactly once with their owned data."
    )]
    pub(crate) fn release_operation_cache_bytes(&self, bytes: u64) {
        let mut completed = self.completed_data_work.lock();
        completed.live_cache_bytes = completed
            .live_cache_bytes
            .checked_sub(bytes)
            .expect("operation cache retirement follows actual buffer destruction");
    }

    pub(crate) fn record_heap_future_event(
        &self,
        event: crate::heap_future::HeapFutureEvent,
    ) -> Result<u64, WorkError> {
        let mut completed = self.completed_data_work.lock();
        match event {
            crate::heap_future::HeapFutureEvent::BeforeFrame { bytes, budget } => {
                let peak = completed
                    .live_cache_bytes
                    .checked_add(bytes)
                    .ok_or(WorkError::Overflow)?;
                if peak > budget.peak_allocation_bytes {
                    return Err(WorkError::BudgetExceeded {
                        counter: "peak_allocation_bytes",
                        observed: peak,
                        maximum: budget.peak_allocation_bytes,
                    });
                }
                return Ok(completed.live_cache_bytes);
            }
            crate::heap_future::HeapFutureEvent::Admitted(mut work) => {
                let bytes = work.peak_allocation_bytes;
                let live = completed
                    .live_future_bytes
                    .checked_add(bytes)
                    .ok_or(WorkError::Overflow)?;
                let data = self
                    .retained_payload_bytes()
                    .checked_sub(completed.reported_retained_bytes)
                    .and_then(|bytes| bytes.checked_sub(completed.reported_physical_bytes))
                    .ok_or(WorkError::Overflow)?;
                work.peak_allocation_bytes = live
                    .checked_add(data)
                    .and_then(|bytes| bytes.checked_add(completed.live_cache_bytes))
                    .ok_or(WorkError::Overflow)?;
                let pending = completed.pending.checked_add(work)?;
                completed.live_future_bytes = live;
                completed.pending = pending;
            }
            crate::heap_future::HeapFutureEvent::Released(bytes) => {
                completed.live_future_bytes = completed
                    .live_future_bytes
                    .checked_sub(bytes)
                    .ok_or(WorkError::Overflow)?;
            }
        }
        Ok(0)
    }

    #[cfg(test)]
    #[allow(
        clippy::expect_used,
        reason = "Exhausting the generation counter must fail rather than reuse an ownership identity."
    )]
    pub(crate) fn take_completed_data_work(&self) -> WorkCounters {
        let mut completed = self.completed_data_work.lock();
        let generation = completed
            .delivery_generation
            .checked_add(1)
            .expect("proof DATA delivery generation exhausted");
        completed.reported_retained_bytes = self.retained_bytes();
        completed.reported_physical_bytes = self.physical_retained_bytes();
        completed.delivery_generation = generation;
        std::mem::take(&mut completed.pending)
    }
}

#[derive(Default)]
pub(crate) struct ProofDataClaim {
    owner: Weak<ProofDataCustody>,
    pub(super) bytes: u64,
    delivery_generation: u64,
    pub(super) physical: bool,
    physical_reported: bool,
    #[cfg(not(target_arch = "wasm32"))]
    native_resource: Option<crate::materializer::NativeResourceRetirement>,
    // Last: retain the original ledger until physical retirement is published.
    #[cfg(not(target_arch = "wasm32"))]
    native_original: Option<Arc<crate::demand::NativeDemandReadMeter>>,
}

impl std::fmt::Debug for ProofDataClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProofDataClaim")
            .field("owner", &self.owner)
            .field("bytes", &self.bytes)
            .field("delivery_generation", &self.delivery_generation)
            .field("physical", &self.physical)
            .field("physical_reported", &self.physical_reported)
            .finish_non_exhaustive()
    }
}

impl ProofDataClaim {
    pub(crate) fn new(owner: Option<&Arc<ProofDataCustody>>, bytes: u64) -> Self {
        let Some(owner) = owner.filter(|_| bytes != 0) else {
            return Self::default();
        };
        let completed = owner.completed_data_work.lock();
        owner.payload_changed(&completed);
        owner.retained.fetch_add(bytes, Ordering::AcqRel);
        Self {
            owner: Arc::downgrade(owner),
            bytes,
            delivery_generation: completed.delivery_generation,
            physical: false,
            physical_reported: false,
            #[cfg(not(target_arch = "wasm32"))]
            native_resource: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_original: None,
        }
    }

    pub(crate) fn control(owner: &Arc<ProofDataCustody>, bytes: u64) -> Self {
        Self::control_claim(owner, bytes, true)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native_control(
        owner: &Arc<ProofDataCustody>,
        retirement: crate::materializer::NativeResourceRetirement,
    ) -> Self {
        let bytes = retirement.bytes();
        if bytes == 0 {
            return Self::default();
        }
        let original = Arc::clone(retirement.original_meter());
        let mut completed = owner.completed_data_work.lock();
        debug_assert!(
            completed
                .native_original
                .as_ref()
                .is_some_and(|registered| registered.ptr_eq(&Arc::downgrade(&original)))
        );
        // Each newly owned primitive has one genuine positive allocation.
        // At most u64 allocation operations of at most u64 bytes fit in u128.
        completed.native_control_born += u128::from(bytes);
        let mut claim = Self::control_claim_in_report(owner, bytes, true, &mut completed);
        claim.native_resource = Some(retirement);
        claim.native_original = Some(original);
        claim
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn is_native_original(
        &self,
        original: &Arc<crate::demand::NativeDemandReadMeter>,
        known_bytes: u64,
    ) -> bool {
        if known_bytes == 0 {
            return self.bytes == 0 && self.native_original.is_none();
        }
        self.physical
            && self.bytes == known_bytes
            && self
                .native_original
                .as_ref()
                .is_some_and(|paid| Arc::ptr_eq(paid, original))
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[allow(
        clippy::expect_used,
        reason = "Claims are admitted before attachment and retire exactly once with their owned data."
    )]
    pub(crate) fn native_data(
        owner: &Arc<ProofDataCustody>,
        retirement: crate::materializer::NativeResourceRetirement,
    ) -> Self {
        let bytes = retirement.bytes();
        if bytes == 0 {
            return Self::default();
        }
        let original = Arc::clone(retirement.original_meter());
        let mut completed = owner.completed_data_work.lock();
        owner.payload_changed(&completed);
        debug_assert!(
            completed
                .native_original
                .as_ref()
                .is_some_and(|registered| registered.ptr_eq(&Arc::downgrade(&original)))
        );
        completed.reported_retained_bytes = completed
            .reported_retained_bytes
            .checked_add(bytes)
            .expect("native DATA fits its admitted original allocation ledger");
        // Each actual buffer has one birth; u64 bytes * u64 allocation ops
        // cannot overflow the fixed u128 cumulative original-event counters.
        completed.native_control_born += u128::from(bytes);
        owner.retained.fetch_add(bytes, Ordering::AcqRel);
        Self {
            owner: Arc::downgrade(owner),
            bytes,
            delivery_generation: completed.delivery_generation,
            physical: false,
            physical_reported: false,
            native_resource: Some(retirement),
            native_original: Some(original),
        }
    }

    pub(crate) fn pending_control(owner: &Arc<ProofDataCustody>, bytes: u64) -> Self {
        Self::control_claim(owner, bytes, false)
    }

    fn control_claim(owner: &Arc<ProofDataCustody>, bytes: u64, reported: bool) -> Self {
        if bytes == 0 {
            return Self::default();
        }
        let mut completed = owner.completed_data_work.lock();
        Self::control_claim_in_report(owner, bytes, reported, &mut completed)
    }

    #[allow(
        clippy::expect_used,
        reason = "Claims are admitted before attachment and retire exactly once with their owned data."
    )]
    fn control_claim_in_report(
        owner: &Arc<ProofDataCustody>,
        bytes: u64,
        reported: bool,
        completed: &mut ProofDataWorkReport,
    ) -> Self {
        owner.payload_changed(completed);
        if reported {
            completed.reported_physical_bytes = completed
                .reported_physical_bytes
                .checked_add(bytes)
                .expect("constructor control fits its original allocation ledger");
        }
        owner.physical_retained.fetch_add(bytes, Ordering::AcqRel);
        Self {
            owner: Arc::downgrade(owner),
            bytes,
            delivery_generation: completed.delivery_generation,
            physical: true,
            physical_reported: reported,
            #[cfg(not(target_arch = "wasm32"))]
            native_resource: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_original: None,
        }
    }
}

impl Clone for ProofDataClaim {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl Drop for ProofDataClaim {
    #[allow(
        clippy::expect_used,
        reason = "Claims are admitted before attachment and retire exactly once with their owned data."
    )]
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            let mut completed = owner.completed_data_work.lock();
            owner.payload_changed(&completed);
            #[cfg(not(target_arch = "wasm32"))]
            let native = self.native_original.is_some();
            #[cfg(target_arch = "wasm32")]
            let native = false;
            #[cfg(not(target_arch = "wasm32"))]
            if native {
                completed.native_control_retired += u128::from(self.bytes);
            }
            if self.physical {
                if self.physical_reported
                    || self.delivery_generation < completed.delivery_generation
                {
                    completed.reported_physical_bytes = completed
                        .reported_physical_bytes
                        .checked_sub(self.bytes)
                        .expect("reported control retires with its actual payload");
                }
                owner
                    .physical_retained
                    .fetch_sub(self.bytes, Ordering::AcqRel);
                return;
            }
            if native || self.delivery_generation < completed.delivery_generation {
                completed.reported_retained_bytes = completed
                    .reported_retained_bytes
                    .checked_sub(self.bytes)
                    .expect("reported proof DATA retires with its actual key");
            }
            owner.retained.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}
