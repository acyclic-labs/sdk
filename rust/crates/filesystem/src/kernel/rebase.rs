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
    ///
    /// # Errors
    ///
    /// Rejects empty/overflowing content ranges and inverted directory windows.
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
    /// The use of a region used both ways.
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

/// Observer-local logical DATA custody. This control token never holds a map.
#[derive(Debug, Default)]
pub(crate) struct ProofDataCustody {
    retained: AtomicU64,
    completed_data_work: Mutex<ProofDataWorkReport>,
}

#[derive(Debug, Default)]
struct ProofDataWorkReport {
    total: WorkCounters,
    pending: WorkCounters,
    reported_retained_bytes: u64,
    delivery_generation: u64,
}

impl ProofDataCustody {
    pub(crate) fn retained_bytes(&self) -> u64 {
        self.retained.load(Ordering::Acquire)
    }

    pub(crate) fn live_data_counts(&self) -> (u64, u64) {
        let completed = self.completed_data_work.lock();
        (self.retained_bytes(), completed.reported_retained_bytes)
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

    pub(crate) fn record_data_work(&self, work: WorkCounters) -> Result<(), WorkError> {
        let mut completed = self.completed_data_work.lock();
        let total = completed.total.checked_add(work)?;
        let pending = completed.pending.checked_add(work)?;
        completed.total = total;
        completed.pending = pending;
        Ok(())
    }

    /// A receipt-report cache only; operative work and allocation custody stay intact.
    pub(crate) fn take_completed_data_work(&self) -> WorkCounters {
        let mut completed = self.completed_data_work.lock();
        let generation = completed.delivery_generation.checked_add(1)
            .expect("proof DATA delivery generation exhausted");
        completed.reported_retained_bytes = self.retained_bytes();
        completed.delivery_generation = generation;
        std::mem::take(&mut completed.pending)
    }
}

/// Lives beside the actual owned key; a weak-only map move moves this claim,
/// while a deep clone deliberately starts without the original owner's claim.
#[derive(Debug, Default)]
pub(crate) struct ProofDataClaim {
    owner: Weak<ProofDataCustody>,
    bytes: u64,
    delivery_generation: u64,
}

impl ProofDataClaim {
    pub(crate) fn new(owner: Option<&Arc<ProofDataCustody>>, bytes: u64) -> Self {
        let Some(owner) = owner.filter(|_| bytes != 0) else { return Self::default(); };
        let completed = owner.completed_data_work.lock();
        owner.retained.fetch_add(bytes, Ordering::AcqRel);
        Self {
            owner: Arc::downgrade(owner), bytes,
            delivery_generation: completed.delivery_generation,
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
            if self.delivery_generation < completed.delivery_generation {
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

/// Validated, sorted, and deduplicated checkout dependency proof.
///
/// Construction pays normalization cost once. Equal-generation refresh can
/// therefore return in constant work regardless of dependency count. Copies
/// share one proof until either side changes it, so snapshotting a proof for
/// classification is constant work too.
#[derive(Clone, Debug)]
pub struct CheckoutDependencies {
    captured: Arc<BTreeMap<DependencyRegion, CapturedValue>>,
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
    /// Arc payload allocated by construction of an empty dependency proof,
    /// excluding allocator and Arc headers.
    pub(crate) const fn empty_logical_owned_bytes() -> usize {
        std::mem::size_of::<BTreeMap<DependencyRegion, CapturedValue>>()
    }

    /// Visits retained name buffers, not opaque BTree node/entry storage.
    pub(crate) fn logical_data_with_step<E: From<WorkError>>(
        &self,
        mut step: impl FnMut() -> Result<(), E>,
    ) -> Result<(usize, u64, u64), E> {
        step()?;
        Ok(self.logical_data())
    }

    /// Cancellation-independent observation of already completed owned data.
    pub(crate) fn logical_data(&self) -> (usize, u64, u64) {
        (Arc::as_ptr(&self.captured) as usize, self.data_generation, self.name_data_bytes)
    }

    /// Validates and normalizes exact observations and mutation preconditions.
    ///
    /// # Errors
    ///
    /// Rejects excessive, malformed, or contradictory dependencies.
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
            captured: Arc::new(captured),
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

    /// Whether an observation of `region` is already part of the proof.
    /// Observing it again against the same or a safely rebased base adds
    /// exactly the dependency the proof already holds.
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

    /// Validates mutation preconditions without applying them, so a caller
    /// commits them only once its mutation has succeeded.
    pub(crate) fn prepare_mutations(
        &self,
        dependencies: Vec<Dependency>,
        maximum_dependencies: u32,
    ) -> Result<DependencyExtension, DependencyError> {
        self.prepare(dependencies, maximum_dependencies, DependencyUse::Mutation)
    }

    /// Applies an extension prepared against this unchanged proof.
    pub(crate) fn commit(&mut self, extension: DependencyExtension) {
        self.commit_with_data_admission::<WorkError>(
            extension, |_, _, _, _| Ok(()),
        ).expect("live dependency data fits the addressable memory space");
    }

    /// Admits actual COW buffers and moved key/value data in the existing
    /// commit engine. Opaque BTree node layout is not a logical payload.
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
        mut admit: impl FnMut(u64, u64, u64, u64) -> Result<(), E>,
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
            // Only actual name buffers are DATA. Tree layout and inline
            // key/value control metadata are not fabricated logical buffers.
            admit(self.cloned_name_data_bytes, operations, self.cloned_name_data_bytes, 0)?;
            let captured = Arc::make_mut(&mut self.captured);
            for (region, value) in captured.iter_mut() {
                value.data = ProofDataClaim::new(owner, dependency_name_bytes(region, false));
            }
            self.name_data_bytes = self.cloned_name_data_bytes;
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
                    let data = ProofDataClaim::new(owner, retained);
                    captured.insert(region, CapturedValue { state: CapturedState { expected, usage }, data });
                    self.name_data_bytes = name_data_bytes;
                    self.cloned_name_data_bytes = cloned_name_data_bytes;
                }
            }
        }
        Ok(())
    }

    /// The proof of both this proof's and `other`'s operations over the same
    /// base generation, or `None` when they captured one region in
    /// different states.
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn merged(&self, other: &Self) -> Option<Self> {
        if Arc::ptr_eq(&self.captured, &other.captured) {
            return Some(self.clone());
        }
        let mut merged = self.clone();
        let captured = Arc::make_mut(&mut merged.captured);
        merged.name_data_bytes = merged.cloned_name_data_bytes;
        merged.data_generation = merged.data_generation.checked_add(1)?;
        for (region, state) in other.captured.iter() {
            match captured.get_mut(region) {
                Some(existing) if existing.expected != state.expected => return None,
                Some(existing) => existing.usage = existing.usage.with(state.usage),
                None => {
                    let region = region.clone();
                    merged.name_data_bytes += dependency_name_bytes(&region, false);
                    merged.cloned_name_data_bytes += dependency_name_bytes(&region, true);
                    captured.insert(region, state.clone());
                }
            }
        }
        Some(merged)
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

/// Dependencies validated against a proof but not yet applied to it.
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
///
/// Implementations must derive `DependencyState` from canonical authenticated
/// generation objects, not from mutable projections or cache presence.
pub trait RebaseProbe {
    /// Stable backend-specific failure.
    type Error: std::error::Error;

    /// Resolves one exact semantic region in one immutable generation.
    ///
    /// # Errors
    ///
    /// Returns a backend-specific failure with exact work performed before the
    /// region could be authenticated.
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

    /// Resolves one exact semantic region without blocking the calling runtime.
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
}

/// Compares only captured regions against a candidate generation.
///
/// Equal roots return with zero work and no probes. Otherwise each distinct
/// dependency is resolved once; untouched volume regions are never enumerated.
///
/// # Errors
///
/// Fails with retained work when conflict bounds, probe execution, or work
/// admission fail.
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

/// Nonblocking safe-rebase classification over the same exact dependencies as
/// [`classify_rebase`].
///
/// # Errors
///
/// Fails with retained exact work on cancellation, probe failure, invalid
/// conflict bounds, or work-budget exhaustion.
pub async fn classify_rebase_async<P: AsyncRebaseProbe>(
    probe: &P,
    base: GenerationId,
    candidate: GenerationId,
    dependencies: &CheckoutDependencies,
    maximum_conflicts: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
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
        cancellation
            .check()
            .map_err(|_| OperationFailure::new(RebaseError::Cancelled, work))?;
        work = work
            .checked_add(WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            })
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let remaining = work
            .remaining(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let actual = probe
            .probe_async(candidate, region, remaining, cancellation)
            .await
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
