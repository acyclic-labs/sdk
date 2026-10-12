//! Exact observation and mutation dependencies for no-scan safe rebasing.

use super::{ExtentSeekTarget, LogicalName};
use crate::cancellation::CancellationToken;
use crate::foundation::{Digest, FileId, GenerationId};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Weak};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

/// One exact semantic region whose state can affect a checkout.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DependencyRegion {
    /// Complete path-independent file record, including kind and payload roots.
    FileRecord(FileId),
    /// Complete canonical metadata object selected by one file record.
    Metadata(FileId),
    /// Exact logical length of one regular file.
    FileLength(FileId),
    /// A non-empty bounded logical content range.
    ContentRange {
        /// Stable file identity.
        file_id: FileId,
        /// Inclusive logical offset.
        offset: u64,
        /// Positive byte length.
        length: u64,
    },
    /// One exact sparse seek observation.
    SparseSeek {
        /// Stable file identity.
        file_id: FileId,
        /// Inclusive logical starting offset.
        offset: u64,
        /// Sparse class requested by the caller.
        target: ExtentSeekTarget,
    },
    /// One exact name lookup, including a negative lookup when state is absent.
    DirectoryName {
        /// Stable directory identity.
        directory_id: FileId,
        /// Canonical queried name.
        name: LogicalName,
    },
    /// A bounded directory cursor interval used to prevent phantom-safe rebases.
    DirectoryRange {
        /// Stable directory identity.
        directory_id: FileId,
        /// Exclusive lower cursor; `None` starts before the first name.
        after: Option<LogicalName>,
        /// Positive maximum entries requested by the observed page.
        maximum_entries: u32,
    },
}

impl DependencyRegion {
    /// Validates range arithmetic and directory cursor ordering.
    pub fn validate(&self) -> Result<(), DependencyError> {
        match self {
            Self::ContentRange { offset, length, .. } => {
                if *length == 0 {
                    return Err(DependencyError::EmptyContentRange);
                }
                offset
                    .checked_add(*length)
                    .ok_or(DependencyError::RangeOverflow)?;
            }
            Self::DirectoryRange {
                maximum_entries: 0, ..
            } => return Err(DependencyError::ZeroDirectoryPageLimit),
            Self::FileRecord(_)
            | Self::Metadata(_)
            | Self::FileLength(_)
            | Self::SparseSeek { .. }
            | Self::DirectoryName { .. }
            | Self::DirectoryRange { .. } => {}
        }
        Ok(())
    }
}

/// Canonical semantic state of one exact dependency region.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyState {
    /// The exact queried name/record/region does not exist.
    Absent,
    /// Domain-separated digest of the complete bounded semantic result.
    Present(Digest),
}

/// One dependency captured while evaluating or mutating a checkout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dependency {
    /// Exact semantic region.
    pub region: DependencyRegion,
    /// State authenticated against the checkout's base generation.
    pub expected: DependencyState,
}

/// Whether a conflicting dependency came from observation, mutation, or both.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyUse {
    /// A read, stat, listing, or negative lookup observed this region.
    Observation,
    /// A local mutation uses this region as its compare-before-write base.
    Mutation,
    /// The same region was both observed and mutated.
    ObservationAndMutation,
}

impl DependencyUse {
    fn with(self, other: Self) -> Self {
        if self == other {
            self
        } else {
            Self::ObservationAndMutation
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CapturedState {
    expected: DependencyState,
    usage: DependencyUse,
}

#[derive(Debug, Default)]
pub(crate) struct ProofDataCustody {
    retained: AtomicU64,
    physical_retained: AtomicU64,
    completed_data_work: Mutex<ProofDataWorkReport>,
}

#[derive(Debug, Default)]
struct ProofDataWorkReport {
    total: WorkCounters,
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
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn bind_native_original(
        &self, original: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> bool {
        let mut completed = self.completed_data_work.lock();
        match completed.native_original.as_ref() {
            Some(bound) => bound.as_ptr() == Arc::as_ptr(original),
            None => {
                completed.native_original = Some(Arc::downgrade(original));
                true
            },
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn is_native_original(
        &self, original: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> bool {
        self.completed_data_work.lock().native_original.as_ref()
            .is_some_and(|bound| bound.as_ptr() == Arc::as_ptr(original))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native_control_events(&self) -> (u128, u128) {
        let completed = self.completed_data_work.lock();
        (completed.native_control_born, completed.native_control_retired)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn non_native_retained_payload_bytes(&self) -> u64 {
        let completed = self.completed_data_work.lock();
        let native_live = u64::try_from(completed.native_control_born - completed.native_control_retired)
            .expect("live native primitives fit their original allocation ledger");
        self.retained_payload_bytes().checked_sub(native_live)
            .expect("native live primitives are included in actual payload custody")
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        self.retained.load(Ordering::Acquire)
    }

    pub(crate) fn physical_retained_bytes(&self) -> u64 {
        self.physical_retained.load(Ordering::Acquire)
    }

    pub(crate) fn retained_payload_bytes(&self) -> u64 {
        self.retained_bytes().checked_add(self.physical_retained_bytes())
            .expect("admitted proof payloads fit their original allocation ledger")
    }

    pub(crate) fn live_payload_counts(&self) -> (u64, u64) {
        let completed = self.completed_data_work.lock();
        let standing = completed.reported_retained_bytes.checked_add(completed.reported_physical_bytes)
            .expect("reported payloads fit their original allocation ledger");
        (self.retained_payload_bytes(), standing)
    }

    pub(crate) const fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }

    pub(crate) fn completed_data_work(&self) -> WorkCounters {
        self.completed_data_work.lock().total
    }

    pub(crate) fn pending_data_work(&self) -> WorkCounters {
        self.completed_data_work.lock().pending
    }

    pub(crate) fn record_data_work(&self, mut work: WorkCounters) -> Result<(), WorkError> {
        let mut completed = self.completed_data_work.lock();
        work.peak_allocation_bytes = work.peak_allocation_bytes
            .checked_add(completed.live_future_bytes).ok_or(WorkError::Overflow)?;
        let total = completed.total.checked_add(work)?;
        let pending = completed.pending.checked_add(work)?;
        completed.total = total;
        completed.pending = pending;
        Ok(())
    }

    pub(crate) fn record_completed_leaf_work(&self, mut work: WorkCounters) -> Result<(), WorkError> {
        let mut completed = self.completed_data_work.lock();
        let data = self.retained_payload_bytes()
            .checked_sub(completed.reported_retained_bytes)
            .and_then(|bytes| bytes.checked_sub(completed.reported_physical_bytes))
            .ok_or(WorkError::Overflow)?;
        work.peak_allocation_bytes = work.peak_allocation_bytes
            .checked_add(completed.live_future_bytes)
            .and_then(|bytes| bytes.checked_add(completed.live_cache_bytes))
            .and_then(|bytes| bytes.checked_add(data)).ok_or(WorkError::Overflow)?;
        let total = completed.total.checked_add(work)?;
        let pending = completed.pending.checked_add(work)?;
        completed.total = total;
        completed.pending = pending;
        Ok(())
    }

    pub(crate) fn record_operation_clone_work(&self, bytes: u64, uncached_live_bytes: u64) -> Result<(), WorkError> {
        if bytes == 0 { return Ok(()); }
        let mut completed = self.completed_data_work.lock();
        let data = self.retained_payload_bytes()
            .checked_sub(completed.reported_retained_bytes)
            .and_then(|bytes| bytes.checked_sub(completed.reported_physical_bytes))
            .ok_or(WorkError::Overflow)?;
        let peak = completed.live_future_bytes.checked_add(completed.live_cache_bytes)
            .and_then(|live| live.checked_add(data))
            .and_then(|live| live.checked_add(uncached_live_bytes))
            .and_then(|live| live.checked_add(bytes)).ok_or(WorkError::Overflow)?;
        let work = WorkCounters {
            allocation_operations: 1, bytes_copied: bytes,
            peak_allocation_bytes: peak, ..WorkCounters::default()
        };
        let total = completed.total.checked_add(work)?;
        let pending = completed.pending.checked_add(work)?;
        completed.total = total;
        completed.pending = pending;
        Ok(())
    }

    pub(crate) fn operation_cache_bytes(&self) -> u64 {
        self.completed_data_work.lock().live_cache_bytes
    }

    pub(crate) fn add_operation_cache_bytes(&self, bytes: u64) -> Result<(), WorkError> {
        let mut completed = self.completed_data_work.lock();
        completed.live_cache_bytes = completed.live_cache_bytes.checked_add(bytes)
            .ok_or(WorkError::Overflow)?;
        Ok(())
    }

    pub(crate) fn release_operation_cache_bytes(&self, bytes: u64) {
        let mut completed = self.completed_data_work.lock();
        completed.live_cache_bytes = completed.live_cache_bytes.checked_sub(bytes)
            .expect("operation cache retirement follows actual buffer destruction");
    }

    pub(crate) fn record_heap_future_event(
        &self,
        event: crate::heap_future::HeapFutureEvent,
    ) -> Result<u64, WorkError> {
        let mut completed = self.completed_data_work.lock();
        match event {
            crate::heap_future::HeapFutureEvent::BeforeFrame { bytes, budget } => {
                let peak = completed.live_cache_bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
                if peak > budget.peak_allocation_bytes {
                    return Err(WorkError::BudgetExceeded {
                        counter: "peak_allocation_bytes", observed: peak,
                        maximum: budget.peak_allocation_bytes,
                    });
                }
                return Ok(completed.live_cache_bytes);
            }
            crate::heap_future::HeapFutureEvent::Admitted(mut work) => {
                let bytes = work.peak_allocation_bytes;
                let live = completed.live_future_bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
                let data = self.retained_payload_bytes()
                    .checked_sub(completed.reported_retained_bytes)
                    .and_then(|bytes| bytes.checked_sub(completed.reported_physical_bytes))
                    .ok_or(WorkError::Overflow)?;
                work.peak_allocation_bytes = live.checked_add(data)
                    .and_then(|bytes| bytes.checked_add(completed.live_cache_bytes))
                    .ok_or(WorkError::Overflow)?;
                let total = completed.total.checked_add(work)?;
                let pending = completed.pending.checked_add(work)?;
                completed.live_future_bytes = live;
                completed.total = total;
                completed.pending = pending;
            }
            crate::heap_future::HeapFutureEvent::Released(bytes) => {
                completed.live_future_bytes = completed.live_future_bytes.checked_sub(bytes)
                    .ok_or(WorkError::Overflow)?;
            }
        }
        Ok(0)
    }

    pub(crate) fn take_completed_data_work(&self) -> WorkCounters {
        let mut completed = self.completed_data_work.lock();
        let generation = completed.delivery_generation.checked_add(1)
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
    bytes: u64,
    delivery_generation: u64,
    physical: bool,
    physical_reported: bool,
    #[cfg(not(target_arch = "wasm32"))]
    native_resource: Option<crate::materializer::NativeResourceRetirement>,
    // Last: retain the original ledger until physical retirement is published.
    #[cfg(not(target_arch = "wasm32"))]
    native_original: Option<Arc<crate::demand::NativeDemandReadMeter>>,
}

impl std::fmt::Debug for ProofDataClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ProofDataClaim")
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
        let Some(owner) = owner.filter(|_| bytes != 0) else { return Self::default(); };
        let completed = owner.completed_data_work.lock();
        owner.retained.fetch_add(bytes, Ordering::AcqRel);
        Self {
            owner: Arc::downgrade(owner), bytes,
            delivery_generation: completed.delivery_generation,
            physical: false, physical_reported: false,
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
        if bytes == 0 { return Self::default(); }
        let original = Arc::clone(retirement.original_meter());
        let mut completed = owner.completed_data_work.lock();
        debug_assert!(completed.native_original.as_ref().is_some_and(|registered|
            registered.ptr_eq(&Arc::downgrade(&original))));
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
        &self, original: &Arc<crate::demand::NativeDemandReadMeter>, known_bytes: u64,
    ) -> bool {
        if known_bytes == 0 {
            return self.bytes == 0 && self.native_original.is_none();
        }
        self.physical && self.bytes == known_bytes
            && self.native_original.as_ref().is_some_and(|paid| Arc::ptr_eq(paid, original))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native_data(
        owner: &Arc<ProofDataCustody>,
        retirement: crate::materializer::NativeResourceRetirement,
    ) -> Self {
        let bytes = retirement.bytes();
        if bytes == 0 { return Self::default(); }
        let original = Arc::clone(retirement.original_meter());
        let mut completed = owner.completed_data_work.lock();
        debug_assert!(completed.native_original.as_ref().is_some_and(|registered|
            registered.ptr_eq(&Arc::downgrade(&original))));
        completed.reported_retained_bytes = completed.reported_retained_bytes.checked_add(bytes)
            .expect("native DATA fits its admitted original allocation ledger");
        // Each actual buffer has one birth; u64 bytes * u64 allocation ops
        // cannot overflow the fixed u128 cumulative original-event counters.
        completed.native_control_born += u128::from(bytes);
        owner.retained.fetch_add(bytes, Ordering::AcqRel);
        Self {
            owner: Arc::downgrade(owner), bytes,
            delivery_generation: completed.delivery_generation,
            physical: false, physical_reported: false,
            native_resource: Some(retirement),
            native_original: Some(original),
        }
    }

    pub(crate) fn pending_control(owner: &Arc<ProofDataCustody>, bytes: u64) -> Self {
        Self::control_claim(owner, bytes, false)
    }

    fn control_claim(owner: &Arc<ProofDataCustody>, bytes: u64, reported: bool) -> Self {
        if bytes == 0 { return Self::default(); }
        let mut completed = owner.completed_data_work.lock();
        Self::control_claim_in_report(owner, bytes, reported, &mut completed)
    }

    fn control_claim_in_report(
        owner: &Arc<ProofDataCustody>,
        bytes: u64,
        reported: bool,
        completed: &mut ProofDataWorkReport,
    ) -> Self {
        if reported {
            completed.reported_physical_bytes = completed.reported_physical_bytes.checked_add(bytes)
                .expect("constructor control fits its original allocation ledger");
        }
        owner.physical_retained.fetch_add(bytes, Ordering::AcqRel);
        Self {
            owner: Arc::downgrade(owner), bytes,
            delivery_generation: completed.delivery_generation,
            physical: true, physical_reported: reported,
            #[cfg(not(target_arch = "wasm32"))]
            native_resource: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_original: None,
        }
    }
}

impl Clone for ProofDataClaim {
    fn clone(&self) -> Self { Self::default() }
}

impl Drop for ProofDataClaim {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            let mut completed = owner.completed_data_work.lock();
            #[cfg(not(target_arch = "wasm32"))]
            let native = self.native_original.is_some();
            #[cfg(target_arch = "wasm32")]
            let native = false;
            #[cfg(not(target_arch = "wasm32"))]
            if native {
                completed.native_control_retired += u128::from(self.bytes);
            }
            if self.physical {
                if self.physical_reported || self.delivery_generation < completed.delivery_generation {
                    completed.reported_physical_bytes = completed.reported_physical_bytes.checked_sub(self.bytes)
                        .expect("reported control retires with its actual payload");
                }
                owner.physical_retained.fetch_sub(self.bytes, Ordering::AcqRel);
                return;
            }
            if native || self.delivery_generation < completed.delivery_generation {
                completed.reported_retained_bytes = completed.reported_retained_bytes.checked_sub(self.bytes)
                    .expect("reported proof DATA retires with its actual key");
            }
            owner.retained.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

#[derive(Clone, Debug)]
struct CapturedValue {
    state: CapturedState,
    data: ProofDataClaim,
}

impl PartialEq for CapturedValue {
    fn eq(&self, other: &Self) -> bool { self.state == other.state }
}

impl Eq for CapturedValue {}

impl std::ops::Deref for CapturedValue {
    type Target = CapturedState;
    fn deref(&self) -> &Self::Target { &self.state }
}

impl std::ops::DerefMut for CapturedValue {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.state }
}

#[derive(Clone, Debug, Default)]
struct CapturedMap {
    entries: BTreeMap<DependencyRegion, CapturedValue>,
    _control: ProofDataClaim,
}

impl PartialEq for CapturedMap {
    fn eq(&self, other: &Self) -> bool { self.entries == other.entries }
}

impl Eq for CapturedMap {}

impl std::ops::Deref for CapturedMap {
    type Target = BTreeMap<DependencyRegion, CapturedValue>;
    fn deref(&self) -> &Self::Target { &self.entries }
}

impl std::ops::DerefMut for CapturedMap {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.entries }
}

/// Validated, sorted, and deduplicated checkout dependency proof.
#[derive(Clone, Debug)]
pub struct CheckoutDependencies {
    captured: Arc<CapturedMap>,
    name_data_bytes: u64,
    cloned_name_data_bytes: u64,
    data_generation: u64,
}

impl PartialEq for CheckoutDependencies {
    fn eq(&self, other: &Self) -> bool { self.captured == other.captured }
}

impl Eq for CheckoutDependencies {}

fn dependency_name_bytes(region: &DependencyRegion, cloned: bool) -> u64 {
    match region {
        DependencyRegion::DirectoryName { name, .. }
        | DependencyRegion::DirectoryRange { after: Some(name), .. } => {
            if cloned { name.as_bytes().len() as u64 } else { name.retained_bytes() as u64 }
        }
        _ => 0,
    }
}

impl CheckoutDependencies {
    pub(crate) const fn empty_logical_owned_bytes() -> usize {
        std::mem::size_of::<CapturedMap>()
    }

    pub(crate) fn attach_fresh_control_custody(&mut self, owner: &Arc<ProofDataCustody>) -> bool {
        self.attach_fresh_control_with(owner, ProofDataClaim::control)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn attach_fresh_paid_control(&mut self, control: &mut ProofDataClaim) -> bool {
        let Some(captured) = Arc::get_mut(&mut self.captured) else { return false; };
        if !captured.entries.is_empty() || captured._control.bytes != 0 || self.data_generation != 0
            || !control.physical || control.bytes != Self::empty_logical_owned_bytes() as u64
        {
            return false;
        }
        captured._control = std::mem::take(control);
        true
    }

    fn attach_fresh_control_with(
        &mut self, owner: &Arc<ProofDataCustody>,
        claim: fn(&Arc<ProofDataCustody>, u64) -> ProofDataClaim,
    ) -> bool {
        let Some(captured) = Arc::get_mut(&mut self.captured) else { return false; };
        if !captured.entries.is_empty() || captured._control.bytes != 0 || self.data_generation != 0 {
            return false;
        }
        captured._control = claim(owner, Self::empty_logical_owned_bytes() as u64);
        true
    }

    pub(crate) fn logical_data_with_step<E: From<WorkError>>(
        &self,
        mut step: impl FnMut() -> Result<(), E>,
    ) -> Result<(usize, u64, u64), E> {
        step()?;
        Ok(self.logical_data())
    }

    pub(crate) fn logical_data(&self) -> (usize, u64, u64) {
        (Arc::as_ptr(&self.captured) as usize, self.data_generation, self.name_data_bytes)
    }

    /// Validates and normalizes exact observations and mutation preconditions.
    pub fn new(
        observations: impl IntoIterator<Item = Dependency>,
        mutations: impl IntoIterator<Item = Dependency>,
        maximum_dependencies: u32,
    ) -> Result<Self, DependencyError> {
        if maximum_dependencies == 0 {
            return Err(DependencyError::ZeroLimit);
        }
        let mut captured: BTreeMap<DependencyRegion, CapturedValue> = BTreeMap::new();
        for (dependency, usage) in observations
            .into_iter()
            .map(|value| (value, DependencyUse::Observation))
            .chain(
                mutations
                    .into_iter()
                    .map(|value| (value, DependencyUse::Mutation)),
            )
        {
            dependency.region.validate()?;
            if let Some(existing) = captured.get_mut(&dependency.region) {
                if existing.expected != dependency.expected {
                    return Err(DependencyError::ContradictoryState);
                }
                if existing.usage != usage {
                    existing.usage = DependencyUse::ObservationAndMutation;
                }
                continue;
            }
            if u32::try_from(captured.len()).unwrap_or(u32::MAX) >= maximum_dependencies {
                return Err(DependencyError::TooManyDependencies {
                    maximum: maximum_dependencies,
                });
            }
            captured.insert(
                dependency.region,
                CapturedValue {
                    state: CapturedState {
                        expected: dependency.expected,
                        usage,
                    },
                    data: ProofDataClaim::default(),
                },
            );
        }
        Ok(Self {
            name_data_bytes: captured.keys().map(|region| dependency_name_bytes(region, false)).sum(),
            cloned_name_data_bytes: captured.keys().map(|region| dependency_name_bytes(region, true)).sum(),
            data_generation: 0,
            captured: Arc::new(CapturedMap { entries: captured, _control: ProofDataClaim::default() }),
        })
    }

    /// Number of distinct exact regions in the proof.
    #[must_use]
    pub fn len(&self) -> usize {
        self.captured.len()
    }

    /// Whether no operation has observed or mutated a region.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.captured.is_empty()
    }

    pub(crate) fn clear(&mut self) {
        self.captured = Arc::default();
        self.name_data_bytes = 0;
        self.cloned_name_data_bytes = 0;
        self.data_generation = self.data_generation.checked_add(1)
            .expect("proof allocation generation exhausted");
    }

    pub(crate) fn observes(&self, region: &DependencyRegion) -> bool {
        self.captured.get(region).is_some_and(|state| {
            matches!(
                state.usage,
                DependencyUse::Observation | DependencyUse::ObservationAndMutation
            )
        })
    }

    pub(crate) fn clear_mutations(&mut self) {
        let mut retained_bytes = 0_u64;
        let mut cloned_bytes = 0_u64;
        if Arc::strong_count(&self.captured) > 1 || Arc::weak_count(&self.captured) != 0 {
            self.data_generation = self.data_generation.checked_add(1)
                .expect("proof allocation generation exhausted");
        }
        Arc::make_mut(&mut self.captured).retain(|region, state| {
            let keep = match state.usage {
                DependencyUse::Observation => true,
                DependencyUse::Mutation => false,
                DependencyUse::ObservationAndMutation => {
                    state.usage = DependencyUse::Observation;
                    true
                }
            };
            if keep {
                retained_bytes += dependency_name_bytes(region, false);
                cloned_bytes += dependency_name_bytes(region, true);
            }
            keep
        });
        self.name_data_bytes = retained_bytes;
        self.cloned_name_data_bytes = cloned_bytes;
    }

    pub(crate) fn extend_observations(
        &mut self,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
    ) -> Result<(), DependencyError> {
        self.extend(
            dependencies,
            maximum_dependencies,
            DependencyUse::Observation,
        )
    }

    pub(crate) fn prepare_observations(
        &self,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
    ) -> Result<DependencyExtension, DependencyError> {
        self.prepare(dependencies, maximum_dependencies, DependencyUse::Observation)
    }

    pub(crate) fn prepare_mutations(
        &self,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
    ) -> Result<DependencyExtension, DependencyError> {
        self.prepare(dependencies, maximum_dependencies, DependencyUse::Mutation)
    }

    pub(crate) fn commit(&mut self, extension: DependencyExtension) {
        self.commit_with_data_admission::<WorkError>(
            extension, |_, _, _, _| Ok(()),
        ).expect("live dependency data fits the addressable memory space");
    }

    pub(crate) fn commit_with_data_admission<E: From<WorkError>>(
        &mut self,
        extension: DependencyExtension,
        admit: impl FnMut(u64, u64, u64, u64) -> Result<(), E>,
    ) -> Result<(), E> {
        self.commit_with_data_custody(extension, None, admit)
    }

    pub(crate) fn commit_with_data_custody<E: From<WorkError>>(
        &mut self,
        extension: DependencyExtension,
        owner: Option<&Arc<ProofDataCustody>>,
        admit: impl FnMut(u64, u64, u64, u64) -> Result<(), E>,
    ) -> Result<(), E> {
        self.commit_with_data_claims(
            extension, admit,
            |bytes| Ok(owner.map(|owner| ProofDataClaim::pending_control(owner, bytes)).unwrap_or_default()),
            |bytes, _| Ok(ProofDataClaim::new(owner, bytes)),
        )
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn commit_with_original_data_custody(
        &mut self,
        extension: DependencyExtension,
        owner: &Arc<ProofDataCustody>,
        staged: &mut crate::NativeAllocation,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), crate::FsError> {
        if !staged.work().shares_ledger(original) {
            return Err(crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout));
        }
        cancellation.check()?;
        let witness = original.checkout_control_sink(owner)?;
        // The guard owns only this invocation's new COW buffers. Physical
        // map cloning and replacement happen outside every original-root loan.
        let copied_allocation = std::cell::RefCell::new(None::<crate::NativeAllocation>);
        self.commit_with_data_claims(
            extension,
            |bytes, operations, copied, items| {
                cancellation.check()?;
                let delta = WorkCounters { bytes_copied: copied, items_examined: items,
                    ..WorkCounters::UNCHARGED };
                if operations != 0 || copied != 0 {
                    let allocation = original.claim(bytes, operations, delta)?;
                    let previous = copied_allocation.replace(Some(allocation));
                    debug_assert!(previous.as_ref().is_none_or(|guard| guard.bytes() == 0));
                    drop(previous);
                } else {
                    original.charge(delta)?;
                }
                Ok(())
            },
            |bytes| {
                let mut allocation = copied_allocation.borrow_mut();
                let allocation = allocation.as_mut()
                    .ok_or(crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout))?;
                let mut control = allocation.split_off(bytes)?;
                control.transfer_checkout_control(owner, &witness).map_err(Into::into)
            },
            |bytes, copied| {
                if copied {
                    let mut allocation = copied_allocation.borrow_mut();
                    let allocation = allocation.as_mut()
                        .ok_or(crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout))?;
                    allocation.transfer_checkout_data_bytes(bytes, owner, &witness).map_err(Into::into)
                } else {
                    staged.transfer_checkout_data_bytes(bytes, owner, &witness).map_err(Into::into)
                }
            },
        )
    }

    fn commit_with_data_claims<E: From<WorkError>>(
        &mut self,
        extension: DependencyExtension,
        mut admit: impl FnMut(u64, u64, u64, u64) -> Result<(), E>,
        mut control: impl FnMut(u64) -> Result<ProofDataClaim, E>,
        mut data: impl FnMut(u64, bool) -> Result<ProofDataClaim, E>,
    ) -> Result<(), E> {
        let mut changes = false;
        for region in extension.staged.keys() {
            admit(0, 0, 0, 1)?;
            if self.captured.get(region).is_none_or(|state| state.usage.with(extension.usage) != state.usage) {
                changes = true;
            }
        }
        if !changes {
            return Ok(());
        }
        let copied = Arc::strong_count(&self.captured) > 1;
        let moved = !copied && Arc::weak_count(&self.captured) != 0;
        let generation = if copied || moved {
            self.data_generation.checked_add(1).ok_or(WorkError::Overflow)?
        } else { self.data_generation };
        if copied {
            let mut operations = 0_u64;
            for region in self.captured.keys() {
                admit(0, 0, 0, 1)?;
                operations = operations.checked_add(u64::from(dependency_name_bytes(region, true) != 0))
                    .ok_or(WorkError::Overflow)?;
            }
            // Admit the actual new Arc payload with its actual name buffers
            // atomically: denial must not report a copy that never occurred.
            // Opaque BTree nodes and allocator/Arc headers remain excluded.
            let control_bytes = Self::empty_logical_owned_bytes() as u64;
            let bytes = self.cloned_name_data_bytes.checked_add(control_bytes)
                .ok_or(WorkError::Overflow)?;
            let operations = operations.checked_add(1).ok_or(WorkError::Overflow)?;
            admit(bytes, operations, self.cloned_name_data_bytes, 0)?;
            let captured = Arc::make_mut(&mut self.captured);
            captured._control = control(control_bytes)?;
            for (region, value) in captured.iter_mut() {
                value.data = data(dependency_name_bytes(region, false), true)?;
            }
            self.name_data_bytes = self.cloned_name_data_bytes;
        } else if moved {
            let control_bytes = Self::empty_logical_owned_bytes() as u64;
            admit(control_bytes, 1, 0, 0)?;
            let claim = control(control_bytes)?;
            // Arc moves the keys without cloning them, but constructs a new
            // known payload. Only its control claim is replaced.
            Arc::make_mut(&mut self.captured)._control = claim;
        }
        self.data_generation = generation;
        let DependencyExtension { staged, usage } = extension;
        let captured = Arc::make_mut(&mut self.captured);
        for (region, expected) in staged {
            admit(0, 0, 0, 1)?;
            match captured.get_mut(&region) {
                Some(existing) => existing.usage = existing.usage.with(usage),
                None => {
                    // The staged key owns its name buffer already; insertion
                    // moves that buffer rather than allocating or copying it.
                    let retained = dependency_name_bytes(&region, false);
                    let name_data_bytes = self.name_data_bytes.checked_add(retained)
                        .ok_or(WorkError::Overflow)?;
                    let cloned_name_data_bytes = self.cloned_name_data_bytes
                        .checked_add(dependency_name_bytes(&region, true)).ok_or(WorkError::Overflow)?;
                    admit(retained, 0, 0, 0)?;
                    let data = data(retained, false)?;
                    captured.insert(region, CapturedValue { state: CapturedState { expected, usage }, data });
                    self.name_data_bytes = name_data_bytes;
                    self.cloned_name_data_bytes = cloned_name_data_bytes;
                }
            }
        }
        Ok(())
    }

    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn merged(&self, other: &Self) -> Option<Self> {
        let mut work = WorkCounters::default();
        let mut allocations = super::allocation::AllocationLedger::default();
        self.merged_with_data_admission::<WorkError>(
            other, None, |bytes, operations, copied, items| {
                let mut next = work.checked_add(WorkCounters {
                    bytes_copied: copied, items_examined: items, ..WorkCounters::default()
                })?;
                next.verify(WorkBudget::UNBOUNDED)?;
                allocations.claim_bytes(bytes, operations, &mut next, WorkBudget::UNBOUNDED)
                    .map_err(|error| error.into_work_or(WorkError::Overflow))?;
                work = next;
                Ok(())
            },
        ).ok().flatten()
    }

    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn merged_with_data_admission<E: From<WorkError>>(
        &self,
        other: &Self,
        owner: Option<&Arc<ProofDataCustody>>,
        admit: impl FnMut(u64, u64, u64, u64) -> Result<(), E>,
    ) -> Result<Option<Self>, E> {
        self.merged_with_data_claims(
            other, admit, |_| Ok(()),
            |bytes| Ok(owner.map(|owner| ProofDataClaim::pending_control(owner, bytes)).unwrap_or_default()),
            |bytes| Ok(ProofDataClaim::new(owner, bytes)),
        )
    }

    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn merged_with_accounting(
        &self,
        other: &Self,
        owner: Option<&Arc<ProofDataCustody>>,
        accounting: &mut super::allocation::KernelAccounting<'_>,
    ) -> Result<Option<Self>, crate::FsError> {
        use super::allocation::KernelOwnedAllocation;
        #[cfg(not(target_arch = "wasm32"))]
        let witness = accounting.native_work().map(|original| {
            original.checkout_control_sink(owner.ok_or(crate::FsError::NativeKernel(
                crate::NativeTreeMaterializationError::InvalidLayout,
            ))?).map_err(crate::FsError::from)
        }).transpose()?;
        let accounting = std::cell::RefCell::new(accounting);
        // Declared outside the physical merge so a rejected constructor drops
        // its actual payload before this untransferred ticket is released.
        let pending = std::cell::RefCell::new(None::<KernelOwnedAllocation>);
        self.merged_with_data_claims(
            other,
            |bytes, operations, copied, items| {
                let mut accounting = accounting.borrow_mut();
                accounting.charge(WorkCounters { items_examined: items, ..WorkCounters::UNCHARGED })?;
                if bytes != 0 || operations != 0 {
                    accounting.admit(WorkCounters { bytes_copied: copied, ..WorkCounters::UNCHARGED })?;
                    let allocation = accounting.claim_owned(bytes, operations)?;
                    let previous = pending.replace(Some(allocation));
                    #[cfg(not(target_arch = "wasm32"))]
                    debug_assert!(previous.as_ref().is_none_or(|guard| match guard {
                        KernelOwnedAllocation::Standalone => true,
                        KernelOwnedAllocation::NativeOriginal(guard) => guard.bytes() == 0,
                    }));
                    drop(previous);
                }
                Ok(())
            },
            |copied| accounting.borrow_mut().record_completed(WorkCounters {
                bytes_copied: copied, ..WorkCounters::UNCHARGED
            }),
            |bytes| {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(witness) = witness.as_ref() {
                    let mut pending = pending.borrow_mut();
                    let Some(KernelOwnedAllocation::NativeOriginal(allocation)) = pending.as_mut() else {
                        return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
                    };
                    return allocation.transfer_checkout_control(
                        owner.ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?, witness,
                    ).map_err(Into::into);
                }
                Ok(owner.map(|owner| ProofDataClaim::pending_control(owner, bytes)).unwrap_or_default())
            },
            |bytes| {
                if bytes == 0 { return Ok(ProofDataClaim::default()); }
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(witness) = witness.as_ref() {
                    let mut pending = pending.borrow_mut();
                    let Some(KernelOwnedAllocation::NativeOriginal(allocation)) = pending.as_mut() else {
                        return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
                    };
                    return allocation.transfer_checkout_data_bytes(
                        bytes, owner.ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?, witness,
                    ).map_err(Into::into);
                }
                Ok(ProofDataClaim::new(owner, bytes))
            },
        )
    }

    #[cfg(any(feature = "native-mount", test))]
    fn merged_with_data_claims<E: From<WorkError>>(
        &self,
        other: &Self,
        mut admit: impl FnMut(u64, u64, u64, u64) -> Result<(), E>,
        mut completed: impl FnMut(u64) -> Result<(), E>,
        mut control: impl FnMut(u64) -> Result<ProofDataClaim, E>,
        mut data: impl FnMut(u64) -> Result<ProofDataClaim, E>,
    ) -> Result<Option<Self>, E> {
        if Arc::ptr_eq(&self.captured, &other.captured) {
            return Ok(Some(self.clone()));
        }
        let generation = self.data_generation.checked_add(1).ok_or(WorkError::Overflow)?;
        let mut captured: BTreeMap<DependencyRegion, CapturedValue> = BTreeMap::new();
        let mut name_data_bytes = 0_u64;
        for (region, state) in self.captured.iter() {
            admit(0, 0, 0, 1)?;
            let bytes = dependency_name_bytes(region, true);
            let retained = name_data_bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
            admit(bytes, u64::from(bytes != 0), bytes, 0)?;
            let region = region.clone();
            completed(bytes)?;
            let data = data(dependency_name_bytes(&region, false))?;
            captured.insert(region, CapturedValue { state: state.state, data });
            name_data_bytes = retained;
        }
        for (region, state) in other.captured.iter() {
            admit(0, 0, 0, 1)?;
            match captured.get_mut(region) {
                Some(existing) if existing.expected != state.expected => return Ok(None),
                Some(existing) => existing.usage = existing.usage.with(state.usage),
                None => {
                    let bytes = dependency_name_bytes(region, true);
                    let retained = name_data_bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
                    admit(bytes, u64::from(bytes != 0), bytes, 0)?;
                    let region = region.clone();
                    completed(bytes)?;
                    let data = data(dependency_name_bytes(&region, false))?;
                    captured.insert(region, CapturedValue { state: state.state, data });
                    name_data_bytes = retained;
                }
            }
        }
        // The new Arc owns this exact map control payload, not a guessed
        // per-entry/node allocation. Stack-resident merge control is excluded.
        admit(Self::empty_logical_owned_bytes() as u64, 1, 0, 0)?;
        let mut captured = Arc::new(CapturedMap { entries: captured, _control: ProofDataClaim::default() });
        completed(0)?;
        Arc::get_mut(&mut captured).expect("the new merged map has no aliases")._control =
            control(Self::empty_logical_owned_bytes() as u64)?;
        Ok(Some(Self {
            captured,
            name_data_bytes,
            cloned_name_data_bytes: name_data_bytes,
            data_generation: generation,
        }))
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn same_capture(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.captured, &other.captured)
            && self.data_generation == other.data_generation
    }

    fn extend(
        &mut self,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
        usage: DependencyUse,
    ) -> Result<(), DependencyError> {
        let extension = self.prepare(dependencies, maximum_dependencies, usage)?;
        self.commit(extension);
        Ok(())
    }

    fn prepare(
        &self,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
        usage: DependencyUse,
    ) -> Result<DependencyExtension, DependencyError> {
        if maximum_dependencies == 0 {
            return Err(DependencyError::ZeroLimit);
        }
        let mut staged = BTreeMap::new();
        for dependency in dependencies {
            dependency.region.validate()?;
            if let Some(existing) = self.captured.get(&dependency.region)
                && existing.expected != dependency.expected
            {
                return Err(DependencyError::ContradictoryState);
            }
            match staged.get(&dependency.region) {
                Some(expected) if *expected != dependency.expected => {
                    return Err(DependencyError::ContradictoryState);
                }
                Some(_) => {}
                None => {
                    staged.insert(dependency.region, dependency.expected);
                }
            }
        }
        let distinct_new = staged
            .keys()
            .filter(|region| !self.captured.contains_key(*region))
            .count();
        if self.captured.len().saturating_add(distinct_new)
            > usize::try_from(maximum_dependencies).unwrap_or(usize::MAX)
        {
            return Err(DependencyError::TooManyDependencies {
                maximum: maximum_dependencies,
            });
        }
        Ok(DependencyExtension { staged, usage })
    }
}

#[must_use]
pub(crate) struct DependencyExtension {
    staged: BTreeMap<DependencyRegion, DependencyState>,
    usage: DependencyUse,
}

impl DependencyExtension {
    pub(crate) fn retained_name_bytes_with_step<E: From<WorkError>>(
        &self,
        mut step: impl FnMut() -> Result<(), E>,
    ) -> Result<u64, E> {
        let mut bytes = 0_u64;
        for region in self.staged.keys() {
            step()?;
            let retained = match region {
                DependencyRegion::DirectoryName { name, .. }
                | DependencyRegion::DirectoryRange { after: Some(name), .. } => {
                    name.retained_bytes() as u64
                }
                _ => 0,
            };
            bytes = bytes.checked_add(retained).ok_or(WorkError::Overflow)?;
        }
        Ok(bytes)
    }
}

/// Backend-independent exact-region resolver.
pub trait RebaseProbe {
    /// Stable backend-specific failure.
    type Error: std::error::Error;

    fn probe(
        &self,
        generation: GenerationId,
        region: &DependencyRegion,
        budget: WorkBudget,
    ) -> Result<ProbeReceipt, OperationFailure<Self::Error>>;
}

/// Runtime-neutral nonblocking exact-region resolver.
pub trait AsyncRebaseProbe {
    /// Stable backend-specific failure.
    type Error: std::error::Error;

    fn probe_async(
        &self,
        generation: GenerationId,
        region: &DependencyRegion,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<ProbeReceipt, OperationFailure<Self::Error>>>;
}

/// One exact-region result and the work required to authenticate it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeReceipt {
    /// Canonical semantic state in the selected generation.
    pub value: DependencyState,
    /// Exact backend and semantic work.
    pub work: WorkCounters,
}

/// One region-specific reason automatic advancement is unsafe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebaseConflict {
    /// Exact changed region.
    pub region: DependencyRegion,
    /// How the checkout depended on it.
    pub usage: DependencyUse,
    /// State captured from the old base.
    pub expected: DependencyState,
    /// State resolved from the candidate generation.
    pub actual: DependencyState,
}

/// Bounded safe-rebase result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RebaseDecision {
    /// Candidate generation can become the new base without replay ambiguity.
    Safe {
        /// New immutable base.
        generation: GenerationId,
    },
    /// Candidate changed one or more exact dependencies.
    Conflicted {
        /// Region-specific conflicts, bounded by the caller.
        conflicts: Vec<RebaseConflict>,
        /// More conflicts existed than the caller admitted returning.
        truncated: bool,
    },
}

/// Successful decision with exact probe work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebaseReceipt {
    /// Safe or region-conflicted result.
    pub decision: RebaseDecision,
    /// Exact semantic and backend work.
    pub work: WorkCounters,
}

/// Safe-rebase classification failures.
#[derive(Debug, Error)]
pub enum RebaseError<E: std::error::Error> {
    /// Cooperative cancellation occurred before the next region probe.
    #[error("safe rebase classification was cancelled")]
    Cancelled,
    /// Conflict output must have a positive hard bound.
    #[error("maximum rebase conflicts must be non-zero")]
    ZeroConflictLimit,
    /// Exact region resolution failed.
    #[error("rebase region probe failed: {0}")]
    Probe(E),
    /// Work exceeded or overflowed the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// A bounded conflict result could not be allocated.
    #[error("safe rebase conflict allocation failed")]
    AllocationFailed,
}

/// Compares only captured regions against a candidate generation.
pub fn classify_rebase<P: RebaseProbe>(
    probe: &P,
    base: GenerationId,
    candidate: GenerationId,
    dependencies: &CheckoutDependencies,
    maximum_conflicts: u32,
    budget: WorkBudget,
) -> Result<RebaseReceipt, OperationFailure<RebaseError<P::Error>>> {
    if base == candidate {
        return Ok(RebaseReceipt {
            decision: RebaseDecision::Safe {
                generation: candidate,
            },
            work: WorkCounters::default(),
        });
    }
    if maximum_conflicts == 0 {
        return Err(OperationFailure::before_work(
            RebaseError::ZeroConflictLimit,
        ));
    }
    let mut work = WorkCounters::default();
    let mut conflicts = Vec::new();
    let mut truncated = false;
    for (region, captured) in dependencies.captured.iter() {
        let semantic = WorkCounters {
            items_examined: 1,
            ..WorkCounters::default()
        };
        work = work
            .checked_add(semantic)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let remaining = work
            .remaining(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let actual = probe
            .probe(candidate, region, remaining)
            .map_err(|failure| failure.map_with_prior_work(work, RebaseError::Probe))?;
        work = work
            .checked_add(actual.work)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        work.verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        if actual.value != captured.expected {
            if u32::try_from(conflicts.len()).unwrap_or(u32::MAX) < maximum_conflicts {
                conflicts.push(RebaseConflict {
                    region: region.clone(),
                    usage: captured.usage,
                    expected: captured.expected,
                    actual: actual.value,
                });
            } else {
                truncated = true;
            }
        }
    }
    let decision = if conflicts.is_empty() {
        RebaseDecision::Safe {
            generation: candidate,
        }
    } else {
        RebaseDecision::Conflicted {
            conflicts,
            truncated,
        }
    };
    Ok(RebaseReceipt { decision, work })
}

/// Nonblocking safe-rebase classification over the same exact dependencies as [`classify_rebase`].
pub async fn classify_rebase_async<P: AsyncRebaseProbe>(
    probe: &P,
    base: GenerationId,
    candidate: GenerationId,
    dependencies: &CheckoutDependencies,
    maximum_conflicts: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<RebaseReceipt, OperationFailure<RebaseError<P::Error>>> {
    let mut accounting = super::allocation::KernelAccounting::standalone(budget, cancellation);
    let adapter = StandaloneClassificationProbe(probe);
    let classified = classify_rebase_with_accounting(
        &adapter, base, candidate, dependencies, maximum_conflicts, &mut accounting, cancellation,
    ).await;
    let work = accounting.spent();
    match classified {
        Ok((decision, allocation)) => {
            drop(allocation);
            Ok(RebaseReceipt { decision, work })
        }
        Err(error) => Err(OperationFailure::new(match error {
            ClassifyRebaseFailure::Probe(error) => RebaseError::Probe(error),
            ClassifyRebaseFailure::ZeroConflictLimit => RebaseError::ZeroConflictLimit,
            ClassifyRebaseFailure::Accounting(crate::FsError::Work(error)) => RebaseError::Work(error),
            ClassifyRebaseFailure::Accounting(crate::FsError::Cancelled(_)) => RebaseError::Cancelled,
            ClassifyRebaseFailure::Accounting(_) => RebaseError::AllocationFailed,
        }, work)),
    }
}

pub(crate) trait AccountingRebaseProbe {
    type Error;
    fn probe_with_accounting(
        &self,
        generation: GenerationId,
        region: &DependencyRegion,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> impl Future<Output = Result<DependencyState, ClassifyRebaseFailure<Self::Error>>>;
}

pub(crate) enum ClassifyRebaseFailure<E> {
    Probe(E),
    Accounting(crate::FsError),
    ZeroConflictLimit,
}

struct StandaloneClassificationProbe<'a, P>(&'a P);

impl<P: AsyncRebaseProbe> AccountingRebaseProbe for StandaloneClassificationProbe<'_, P> {
    type Error = P::Error;
    async fn probe_with_accounting(
        &self,
        generation: GenerationId,
        region: &DependencyRegion,
        accounting: &mut super::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<DependencyState, ClassifyRebaseFailure<Self::Error>> {
        // This adapter is constructed only by the standalone public API.
        let budget = accounting.remaining().map_err(ClassifyRebaseFailure::Accounting)?;
        match self.0.probe_async(generation, region, budget, cancellation).await {
            Ok(receipt) => {
                retain_standalone_probe_work(accounting, receipt.work)
                    .map_err(ClassifyRebaseFailure::Accounting)?;
                Ok(receipt.value)
            }
            Err(failure) => {
                // Probe failures retain the work that actually happened,
                // including an over-budget measured backend receipt.
                retain_standalone_probe_work(accounting, *failure.work)
                    .map_err(ClassifyRebaseFailure::Accounting)?;
                Err(ClassifyRebaseFailure::Probe(failure.error))
            }
        }
    }
}

fn retain_standalone_probe_work(
    accounting: &mut super::allocation::KernelAccounting<'_>,
    delta: WorkCounters,
) -> Result<(), crate::FsError> {
    match accounting {
        super::allocation::KernelAccounting::Standalone { work, budget, .. } => {
            *work = work.checked_add(delta)?;
            work.verify(*budget)?;
        }
        super::allocation::KernelAccounting::BorrowedStandalone { work, budget, .. } => {
            **work = work.checked_add(delta)?;
            work.verify(*budget)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        super::allocation::KernelAccounting::NativeOriginal { .. } =>
            unreachable!("the standalone adapter cannot wrap an original-native probe"),
    }
    Ok(())
}

pub(crate) async fn classify_rebase_with_accounting<P: AccountingRebaseProbe>(
    probe: &P,
    base: GenerationId,
    candidate: GenerationId,
    dependencies: &CheckoutDependencies,
    maximum_conflicts: u32,
    accounting: &mut super::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(RebaseDecision, super::allocation::KernelOwnedAllocation), ClassifyRebaseFailure<P::Error>> {
    use super::persistent_point::{Owned, absorb};
    accounting.check().map_err(ClassifyRebaseFailure::Accounting)?;
    if base == candidate {
        let allocation = accounting.claim_owned(0, 0).map_err(ClassifyRebaseFailure::Accounting)?;
        return Ok((RebaseDecision::Safe { generation: candidate }, allocation));
    }
    if maximum_conflicts == 0 { return Err(ClassifyRebaseFailure::ZeroConflictLimit); }
    let mut conflicts = Owned::<Vec<RebaseConflict>>::allocate(0, accounting)
        .map_err(ClassifyRebaseFailure::Accounting)?;
    let result = async {
        let mut truncated = false;
        for (region, captured) in dependencies.captured.iter() {
            cancellation.check().map_err(|error| ClassifyRebaseFailure::Accounting(error.into()))?;
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
                .map_err(ClassifyRebaseFailure::Accounting)?;
            let actual = probe.probe_with_accounting(candidate, region, accounting, cancellation)
                .await?;
            if actual == captured.expected { continue; }
            if conflicts.value.len() >= maximum_conflicts as usize {
                truncated = true;
                continue;
            }
            conflicts.ensure_for_push(maximum_conflicts as usize, accounting)
                .map_err(ClassifyRebaseFailure::Accounting)?;
            let (region, allocation, bytes) = clone_conflict_region(region, accounting)
                .map_err(ClassifyRebaseFailure::Accounting)?;
            if let Err(error) = absorb(&mut conflicts.allocation, allocation) {
                drop(region);
                return Err(ClassifyRebaseFailure::Accounting(error));
            }
            conflicts.bytes = conflicts.bytes.checked_add(bytes)
                .ok_or_else(|| ClassifyRebaseFailure::Accounting(WorkError::Overflow.into()))?;
            conflicts.value.push(RebaseConflict {
                region, usage: captured.usage, expected: captured.expected, actual,
            });
        }
        Ok(truncated)
    }.await;
    match result {
        Ok(truncated) => {
            let decision = if conflicts.value.is_empty() {
                RebaseDecision::Safe { generation: candidate }
            } else {
                RebaseDecision::Conflicted { conflicts: conflicts.value, truncated }
            };
            Ok((decision, conflicts.allocation))
        }
        Err(error) => {
            conflicts.retire(accounting).map_err(ClassifyRebaseFailure::Accounting)?;
            Err(error)
        }
    }
}

fn clone_conflict_region(
    region: &DependencyRegion,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<(DependencyRegion, super::allocation::KernelOwnedAllocation, u64), crate::FsError> {
    use super::persistent_btree::Format;
    use super::tree_mutation::TreeFormat;
    match region {
        DependencyRegion::DirectoryName { directory_id, name } => {
            let (name, allocation) = super::persistent_io::clone_key_with_accounting::<TreeFormat>(
                name, u32::MAX, accounting,
            )?;
            let bytes = TreeFormat::key_owned_bytes(&name);
            Ok((DependencyRegion::DirectoryName { directory_id: *directory_id, name }, allocation, bytes))
        }
        DependencyRegion::DirectoryRange { directory_id, after: Some(after), maximum_entries } => {
            let (after, allocation) = super::persistent_io::clone_key_with_accounting::<TreeFormat>(
                after, u32::MAX, accounting,
            )?;
            let bytes = TreeFormat::key_owned_bytes(&after);
            Ok((DependencyRegion::DirectoryRange {
                directory_id: *directory_id, after: Some(after), maximum_entries: *maximum_entries,
            }, allocation, bytes))
        }
        _ => {
            let allocation = accounting.claim_owned(0, 0)?;
            Ok((region.clone(), allocation, 0))
        }
    }
}

/// Dependency-proof construction errors.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DependencyError {
    /// Dependency admission bound must be non-zero.
    #[error("maximum dependencies must be non-zero")]
    ZeroLimit,
    /// Content ranges must contain at least one byte.
    #[error("content dependency range is empty")]
    EmptyContentRange,
    /// Content range arithmetic overflowed.
    #[error("content dependency range overflowed")]
    RangeOverflow,
    /// Directory listing observations require a positive page bound.
    #[error("directory dependency page limit must be non-zero")]
    ZeroDirectoryPageLimit,
    /// The same exact region was captured with two different base states.
    #[error("dependency region has contradictory captured states")]
    ContradictoryState,
    /// Distinct dependency count exceeded its hard bound.
    #[error("dependency count exceeds maximum {maximum}")]
    TooManyDependencies {
        /// Configured maximum.
        maximum: u32,
    },
}

#[cfg(test)]
#[path = "tests/rebase.rs"]
mod tests;
