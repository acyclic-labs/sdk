//! Bounded hypotheses over immutable authoritative references.
//!
//! This module performs no admission, IO, domain reduction or effect cancellation.
//! A trusted [`Domain`] adapter supplies validated facts and correspondence.
//! Exported hypotheses remain hypotheses; they are never accepted as evidence.

#![doc = include_str!("../docs/client/README.md")]

use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Stable identity within an embedding application's durable namespace.
/// The namespace must be unique; sequence allocation is monotonic within a client.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct BranchId {
    /// Embedding application's session/replica identity.
    pub namespace: u128,
    /// Monotonic local identity, never recycled by disposal.
    pub sequence: u64,
}

/// Required provenance of one explicitly declared dependency.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DependencyRequirement {
    /// A still-valid local prediction or its confirmed canonical counterpart.
    Prediction,
    /// Correspondence has already been established by trusted evidence.
    Confirmed,
}

/// A typed dependency edge. It never grants effect admission or authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Dependency {
    /// Stable identity of an older hypothesis.
    pub branch: BranchId,
    /// Provenance required at creation; subsequent invalidation is transitive.
    pub requirement: DependencyRequirement,
}

impl Dependency {
    /// A pure dependent prediction may use a provisional parent projection.
    pub fn prediction(branch: BranchId) -> Self {
        Self {
            branch,
            requirement: DependencyRequirement::Prediction,
        }
    }
}

/// Hard bounds on active demand and each transition.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Limits {
    /// Resident authoritative records.
    pub records: usize,
    /// Resident hypotheses including terminal outcomes.
    pub branches: usize,
    /// Total resident hypothesis dependency edges.
    pub edges: usize,
    /// Total conservatively accounted resident bytes: adapter-declared deep
    /// allocations plus kernel/index metadata. Caller-held snapshots are external.
    pub bytes: usize,
    /// Maximum adapter steps plus branch/edge visits per mutation (map lookups
    /// additionally cost logarithmic time in bounded active demand).
    pub work: usize,
    /// Maximum logical retention ticks of a hypothesis.
    pub retention: u64,
    /// Maximum explicitly selected overlays in one view request.
    pub visible: usize,
}

/// Explicit failure; mutations are atomic on failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// Finite capacity or per-transition work exhausted.
    Budget,
    /// Exact basis no longer available.
    StaleBasis,
    /// Incompatible adapter, evidence or assumptions.
    Conflict,
    /// Adapter cannot establish correspondence.
    Unsupported,
    /// Identity, demanded record or dependency absent.
    Missing,
    /// A competing visible hypothesis targets the same record.
    Ambiguous,
    /// Logical time regressed or expiry invalid.
    Time,
}

/// Evidence establishes correspondence, rather than UI equality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Correspondence {
    /// Prediction matches canonical outcome under the pinned adapter.
    Match,
    /// Canonical result replaces the prediction.
    Different,
}

/// Provider operation outcome, independent of prediction status.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OperationOutcome {
    /// No evidence of admission or completion.
    Unknown,
    /// Admission is not confirmation.
    Admitted,
    /// Effect outcome cannot yet be established.
    Indeterminate,
    /// Authoritative terminal success, with canonical projection.
    Completed,
    /// Authoritative rejection (not an uncertain effect).
    Rejected,
    /// Authoritative cancellation; adapter must establish actual termination.
    Cancelled,
}

impl OperationOutcome {
    fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Rejected | Self::Cancelled)
    }
}

/// Status of a local prediction, not an authority or operation receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PredictionOutcome {
    /// Eligible for explicit overlay selection.
    Pending,
    /// Canonical correspondence established.
    Confirmed,
    /// Canonical result differs.
    Replaced,
    /// A pinned basis or dependency ceased to hold.
    Invalidated,
    /// Definitive rejection or cancellation.
    Removed,
}

/// Immutable authoritative reference; `B` pins authority, generation, revision
/// and content identity in the domain's existing typed representation.
#[derive(Clone, Debug)]
pub struct Fact<K, B, V> {
    /// Stable normalized domain identity.
    pub key: K,
    /// Exact domain basis.
    pub basis: Arc<B>,
    /// Shared domain-owned immutable projection (large bodies may be references).
    pub value: Arc<V>,
    /// Adapter-declared deep allocation bytes including cloned key/basis/value.
    /// The kernel additionally accounts record/index metadata.
    pub bytes: usize,
}

/// Adapter-validated evidence. No raw client hypothesis can construct a fact
/// inside the kernel; only the adapter's `observe` result is consumed.
pub struct Observation<K, B, O, V> {
    /// Updated canonical record, or none for admission/uncertainty/rejection.
    pub fact: Option<Fact<K, B, V>>,
    /// Original operation identity and outcome, if evidence correlates it.
    pub operation: Option<(O, OperationOutcome)>,
    /// Actual adapter work, within the supplied allowance.
    pub work: usize,
}

/// Minimal typed domain boundary, not a collection of arbitrary callbacks.
///
/// Implementations must be deterministic, pure, bounded by `work`, verify trusted
/// evidence against `current`, reject reordered/conflicting revisions, preserve
/// original operation identity and use the existing domain reducer/validation.
/// Missing evidence never implies rejection. `Basis` includes adapter-relevant
/// authority/generation/revision/content pins. Values must be immutable and
/// byte accounting conservative, including up to three key/operation clones in
/// kernel indexes. These are adapter obligations, not cryptographic
/// guarantees supplied by this generic kernel.
pub trait Domain {
    /// Normalized record identity.
    type Key: Clone + Ord;
    /// Exact existing authoritative basis.
    type Basis: Eq;
    /// Existing admitted operation identity; never generated by this module.
    type Operation: Clone + Ord;
    /// Typed domain assumptions.
    type Assumption;
    /// Immutable reference-oriented domain projection.
    type Value;
    /// Trusted domain journal/reducer evidence.
    type Evidence;

    /// Pinned correspondence implementation identity/version.
    fn identity(&self) -> u128;
    /// Validate a hypothesis and return accounted bytes and actual work.
    /// No side effects, reduction or admission may occur here.
    fn validate(
        &self,
        fact: &Fact<Self::Key, Self::Basis, Self::Value>,
        value: &Self::Value,
        assumption: &Self::Assumption,
        work: usize,
    ) -> Result<(usize, usize), Error>;
    /// Consume verified evidence using the existing domain semantics.
    fn observe(
        &self,
        evidence: &Self::Evidence,
        current: Option<&Fact<Self::Key, Self::Basis, Self::Value>>,
        work: usize,
    ) -> Result<DomainObservation<Self>, Error>;
    /// Compare exact canonical result and prediction under this adapter.
    fn corresponds(
        &self,
        predicted: &Self::Value,
        canonical: &Self::Value,
        work: usize,
    ) -> Result<(Correspondence, usize), Error>;
}

/// Data-only persistence/share representation. It carries no authoritative fact.
/// Restoring requires fresh authoritative demand and ordinary `begin` validation;
/// this module supplies no journal, transport or automatic import/promotion.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hypothesis<K, B, O, A, V> {
    /// Stable hypothesis identity.
    pub id: BranchId,
    /// Correspondence adapter pin.
    pub adapter: u128,
    /// Target normalized record identity.
    pub key: K,
    /// Original admitted operation identity, if any.
    pub operation: Option<O>,
    /// Exact authoritative basis pinned at prediction.
    pub basis: Arc<B>,
    /// Domain-typed assumptions.
    pub assumption: A,
    /// Dependencies must be live and older, so cycles cannot be introduced.
    pub dependencies: Vec<Dependency>,
    /// Deterministic, immutable predicted projection.
    pub predicted: Arc<V>,
    /// Logical expiry supplied by the embedding host.
    pub expires: u64,
    /// Prediction status remains separate from the operation.
    pub prediction: PredictionOutcome,
    /// Last trusted correlated operation status.
    pub outcome: OperationOutcome,
    /// Conservatively accounted retained bytes including kernel/index metadata.
    pub bytes: usize,
}

/// Arguments for a pure `begin` or dependent `fork`.
pub struct Begin<K, B, O, A, V> {
    /// Target demanded key.
    pub key: K,
    /// Exact pinned basis.
    pub basis: Arc<B>,
    /// Optional original operation identity.
    pub operation: Option<O>,
    /// Domain assumptions.
    pub assumption: A,
    /// Deterministic prediction.
    pub predicted: Arc<V>,
    /// Explicit dependencies; no speculative fanout is started.
    pub dependencies: Vec<Dependency>,
    /// Logical expiry.
    pub expires: u64,
}

/// Provenance of a selected immutable value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Provenance<B> {
    /// Exact authoritative basis.
    Authoritative(Arc<B>),
    /// A local hypothesis under an exact authoritative basis and adapter.
    Hypothesis {
        /// Stable local identity.
        id: BranchId,
        /// Exact authority/generation/revision/content basis.
        basis: Arc<B>,
        /// Pinned correspondence implementation and revision.
        adapter: u128,
    },
}

/// Stable snapshot of one record. Caller-held snapshots survive reconciliation.
pub struct View<B, V> {
    /// Shared immutable value.
    pub value: Arc<V>,
    /// Explicit authority/hypothesis provenance.
    pub provenance: Provenance<B>,
}

type Record<D> = Fact<<D as Domain>::Key, <D as Domain>::Basis, <D as Domain>::Value>;
type Branch<D> = Hypothesis<
    <D as Domain>::Key,
    <D as Domain>::Basis,
    <D as Domain>::Operation,
    <D as Domain>::Assumption,
    <D as Domain>::Value,
>;
type Request<D> = Begin<
    <D as Domain>::Key,
    <D as Domain>::Basis,
    <D as Domain>::Operation,
    <D as Domain>::Assumption,
    <D as Domain>::Value,
>;

/// Evidence result specialized to a typed domain adapter.
pub type DomainObservation<D> = Observation<
    <D as Domain>::Key,
    <D as Domain>::Basis,
    <D as Domain>::Operation,
    <D as Domain>::Value,
>;

/// Narrow observation notification and accountable work receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Changes {
    /// The explicitly supplied demanded key acquired a new authoritative basis.
    pub authoritative: bool,
    /// Only changed hypothesis identities; no history/state serialization.
    pub hypotheses: Vec<BranchId>,
    /// Adapter steps and kernel branch/edge visits in this transition.
    pub work: usize,
}

type Resolution = (BranchId, OperationOutcome, PredictionOutcome);

// BTreeMap/BTreeSet nodes reserve multiple inline slots even when almost empty.
// Charge thirty-two times inline size plus node linkage per indexed item, covering
// sparse nodes and the fixed indexes here. Deep allocations remain adapter-owned.
// This is conservative accounting, not a claim about allocator RSS/fragmentation.
fn metadata_bytes<T>() -> Result<usize, Error> {
    std::mem::size_of::<T>()
        .checked_add(64)
        .and_then(|n| n.checked_mul(32))
        .ok_or(Error::Budget)
}

fn dependency_bytes(length: usize, capacity: usize) -> Result<usize, Error> {
    let backing = capacity
        .checked_mul(std::mem::size_of::<Dependency>())
        .ok_or(Error::Budget)?;
    let indexes = length
        .checked_mul(metadata_bytes::<Dependency>()?)
        .ok_or(Error::Budget)?;
    backing.checked_add(indexes).ok_or(Error::Budget)
}

/// Demand-scoped client kernel. Construction grants no authority and starts no work.
pub struct Client<D: Domain> {
    domain: D,
    adapter: u128,
    limits: Limits,
    namespace: u128,
    sequence: u64,
    now: u64,
    facts: BTreeMap<D::Key, Record<D>>,
    branches: BTreeMap<BranchId, Branch<D>>,
    by_key: BTreeMap<D::Key, BTreeSet<BranchId>>,
    by_operation: BTreeMap<D::Operation, BranchId>,
    dependents: BTreeMap<BranchId, BTreeSet<BranchId>>,
    bytes: usize,
    edges: usize,
}

impl<D: Domain> Client<D> {
    /// Construct an empty client with explicit finite positive bounds.
    /// Namespace uniqueness and recovered sequence allocation belong to the host.
    pub fn new(domain: D, namespace: u128, sequence: u64, limits: Limits) -> Result<Self, Error> {
        if [
            limits.records,
            limits.branches,
            limits.edges,
            limits.bytes,
            limits.work,
            limits.visible,
        ]
        .contains(&0)
            || limits.retention == 0
            || limits.visible > limits.work
        {
            return Err(Error::Budget);
        }
        let adapter = domain.identity();
        Ok(Self {
            domain,
            adapter,
            limits,
            namespace,
            sequence,
            now: 0,
            facts: BTreeMap::new(),
            branches: BTreeMap::new(),
            by_key: BTreeMap::new(),
            by_operation: BTreeMap::new(),
            dependents: BTreeMap::new(),
            bytes: 0,
            edges: 0,
        })
    }

    /// Last allocated sequence for data-only host checkpoints.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Accounted resident bytes, records, branches and edges. Caller-held views
    /// and adapter-owned state are outside this client's residency.
    pub fn residency(&self) -> (usize, usize, usize, usize) {
        (
            self.bytes,
            self.facts.len(),
            self.branches.len(),
            self.edges,
        )
    }

    /// Pure creation of a hypothesis. A dependency establishes fork provenance;
    /// no reducer, worker, provider operation or automatic continuation runs.
    pub fn begin(&mut self, request: Request<D>) -> Result<BranchId, Error> {
        if self.domain.identity() != self.adapter {
            return Err(Error::Conflict);
        }
        let fact = self.facts.get(&request.key).ok_or(Error::Missing)?;
        if fact.basis != request.basis {
            return Err(Error::StaleBasis);
        }
        if request.expires <= self.now || request.expires - self.now > self.limits.retention {
            return Err(Error::Time);
        }
        if self.branches.len() >= self.limits.branches
            || request.dependencies.len() > self.limits.work
            || request.dependencies.len() > self.limits.edges.saturating_sub(self.edges)
        {
            return Err(Error::Budget);
        }
        if request
            .operation
            .as_ref()
            .is_some_and(|o| self.by_operation.contains_key(o))
        {
            return Err(Error::Conflict);
        }
        let mut unique = BTreeSet::new();
        for edge in &request.dependencies {
            let dependency = self.branches.get(&edge.branch).ok_or(Error::Missing)?;
            let eligible = match edge.requirement {
                DependencyRequirement::Prediction => matches!(
                    dependency.prediction,
                    PredictionOutcome::Pending | PredictionOutcome::Confirmed
                ),
                DependencyRequirement::Confirmed => {
                    dependency.prediction == PredictionOutcome::Confirmed
                }
            };
            if !unique.insert(edge.branch) || !eligible {
                return Err(Error::Conflict);
            }
        }
        let (bytes, work) = self.domain.validate(
            fact,
            &request.predicted,
            &request.assumption,
            self.limits.work - unique.len(),
        )?;
        if work > self.limits.work - unique.len() {
            return Err(Error::Budget);
        }
        let dependencies_bytes = dependency_bytes(unique.len(), request.dependencies.capacity())?;
        let bytes = bytes
            .checked_add(metadata_bytes::<Branch<D>>()?)
            .and_then(|n| n.checked_add(dependencies_bytes))
            .ok_or(Error::Budget)?;
        if bytes > self.limits.bytes.saturating_sub(self.bytes) {
            return Err(Error::Budget);
        }
        let sequence = self.sequence.checked_add(1).ok_or(Error::Budget)?;
        let id = BranchId {
            namespace: self.namespace,
            sequence,
        };
        for dependency in &unique {
            self.dependents.entry(*dependency).or_default().insert(id);
        }
        self.by_key
            .entry(request.key.clone())
            .or_default()
            .insert(id);
        if let Some(operation) = &request.operation {
            self.by_operation.insert(operation.clone(), id);
        }
        self.edges += unique.len();
        self.bytes += bytes;
        self.sequence = sequence;
        self.branches.insert(
            id,
            Hypothesis {
                id,
                adapter: self.adapter,
                key: request.key,
                operation: request.operation,
                basis: request.basis,
                assumption: request.assumption,
                dependencies: request.dependencies,
                predicted: request.predicted,
                expires: request.expires,
                prediction: PredictionOutcome::Pending,
                outcome: OperationOutcome::Unknown,
                bytes,
            },
        );
        Ok(id)
    }

    /// Select one demanded record and explicit overlays. Multiple competing
    /// predictions are an ambiguity, never implicit last-writer-wins.
    pub fn view(
        &self,
        key: &D::Key,
        overlays: &[BranchId],
    ) -> Result<View<D::Basis, D::Value>, Error> {
        if overlays.len() > self.limits.visible {
            return Err(Error::Budget);
        }
        let fact = self.facts.get(key).ok_or(Error::Missing)?;
        let mut selected = None;
        for id in overlays {
            let branch = self.branches.get(id).ok_or(Error::Missing)?;
            if &branch.key == key && branch.prediction == PredictionOutcome::Pending {
                if selected.is_some() {
                    return Err(Error::Ambiguous);
                }
                selected = Some(branch);
            }
        }
        Ok(if let Some(branch) = selected {
            View {
                value: Arc::clone(&branch.predicted),
                provenance: Provenance::Hypothesis {
                    id: branch.id,
                    basis: branch.basis.clone(),
                    adapter: branch.adapter,
                },
            }
        } else {
            View {
                value: Arc::clone(&fact.value),
                provenance: Provenance::Authoritative(fact.basis.clone()),
            }
        })
    }

    /// Inspect/export a hypothesis by reference. Serialization is data only.
    pub fn hypothesis(&self, id: BranchId) -> Option<&Branch<D>> {
        self.branches.get(&id)
    }

    fn affected(
        &self,
        roots: BTreeSet<BranchId>,
        work: &mut usize,
    ) -> Result<BTreeSet<BranchId>, Error> {
        let mut pending = roots;
        let mut result = BTreeSet::new();
        while let Some(id) = pending.pop_first() {
            *work = work.checked_add(1).ok_or(Error::Budget)?;
            if *work > self.limits.work {
                return Err(Error::Budget);
            }
            if result.insert(id)
                && let Some(children) = self.dependents.get(&id)
            {
                for child in children {
                    *work = work.checked_add(1).ok_or(Error::Budget)?;
                    if *work > self.limits.work {
                        return Err(Error::Budget);
                    }
                    pending.insert(*child);
                }
            }
        }
        Ok(result)
    }

    fn resolve_operation(
        &self,
        key: &D::Key,
        observation: &DomainObservation<D>,
        current: Option<&Record<D>>,
        work: &mut usize,
    ) -> Result<Option<Resolution>, Error> {
        let Some((operation, incoming)) = &observation.operation else {
            return Ok(None);
        };
        let Some(id) = self.by_operation.get(operation) else {
            return Ok(None);
        };
        let branch = self.branches.get(id).ok_or(Error::Missing)?;
        if &branch.key != key || (branch.outcome.terminal() && branch.outcome != *incoming) {
            return Err(Error::Conflict);
        }
        // Reordered admission/unknown receipts cannot erase known uncertainty.
        let outcome = match (branch.outcome, incoming) {
            (
                OperationOutcome::Indeterminate,
                OperationOutcome::Unknown | OperationOutcome::Admitted,
            )
            | (OperationOutcome::Admitted, OperationOutcome::Unknown) => branch.outcome,
            _ => *incoming,
        };
        let prediction =
            if branch.prediction == PredictionOutcome::Invalidated || branch.outcome.terminal() {
                // Late completion cannot revive invalid dependencies. Exact duplicate
                // terminal receipts never compare predictions to later canonical state.
                branch.prediction
            } else {
                match outcome {
                    OperationOutcome::Completed => {
                        let canonical = observation
                            .fact
                            .as_ref()
                            .or(current)
                            .ok_or(Error::Missing)?;
                        let (correspondence, steps) = self.domain.corresponds(
                            &branch.predicted,
                            &canonical.value,
                            self.limits.work.saturating_sub(*work),
                        )?;
                        *work = work.checked_add(steps).ok_or(Error::Budget)?;
                        match correspondence {
                            Correspondence::Match => PredictionOutcome::Confirmed,
                            Correspondence::Different => PredictionOutcome::Replaced,
                        }
                    }
                    OperationOutcome::Rejected | OperationOutcome::Cancelled => {
                        PredictionOutcome::Removed
                    }
                    _ => branch.prediction,
                }
            };
        Ok(Some((*id, outcome, prediction)))
    }

    fn invalidation_roots(
        &self,
        key: &D::Key,
        changed_basis: bool,
        correlated: Option<Resolution>,
        work: &mut usize,
    ) -> Result<BTreeSet<BranchId>, Error> {
        let mut roots = BTreeSet::new();
        if changed_basis && let Some(ids) = self.by_key.get(key) {
            for id in ids {
                *work = work.checked_add(1).ok_or(Error::Budget)?;
                if *work > self.limits.work {
                    return Err(Error::Budget);
                }
                let branch = self.branches.get(id).ok_or(Error::Missing)?;
                let newly_confirmed = correlated.is_some_and(|(matching, _, prediction)| {
                    matching == *id
                        && prediction == PredictionOutcome::Confirmed
                        && !branch.outcome.terminal()
                });
                if !newly_confirmed
                    && matches!(
                        branch.prediction,
                        PredictionOutcome::Pending | PredictionOutcome::Confirmed
                    )
                {
                    roots.insert(*id);
                }
            }
        }
        if let Some((id, _, prediction)) = correlated
            && matches!(
                prediction,
                PredictionOutcome::Replaced | PredictionOutcome::Removed
            )
            && self
                .branches
                .get(&id)
                .is_some_and(|b| b.prediction != prediction)
        {
            roots.insert(id);
        }
        Ok(roots)
    }

    /// Observe trusted evidence for one explicitly demanded key. The adapter
    /// rejects stale/reordered/conflicting evidence before atomic publication.
    /// Returned changes support narrow notifications and expose bounded work.
    pub fn observe(&mut self, key: D::Key, evidence: &D::Evidence) -> Result<Changes, Error> {
        if self.domain.identity() != self.adapter {
            return Err(Error::Conflict);
        }
        let current = self.facts.get(&key);
        let observation = self.domain.observe(evidence, current, self.limits.work)?;
        let mut work = observation.work;
        if work > self.limits.work {
            return Err(Error::Budget);
        }
        let mut next_bytes = self.bytes;
        if let Some(fact) = &observation.fact {
            let record_metadata = metadata_bytes::<Record<D>>()?;
            let fact_bytes = fact
                .bytes
                .checked_add(record_metadata)
                .ok_or(Error::Budget)?;
            if fact.key != key {
                return Err(Error::Conflict);
            }
            if current.is_none() && self.facts.len() >= self.limits.records {
                return Err(Error::Budget);
            }
            if current.is_none_or(|old| old.basis != fact.basis) {
                let old_bytes = current
                    .map(|old| old.bytes.checked_add(record_metadata).ok_or(Error::Budget))
                    .transpose()?
                    .unwrap_or(0);
                next_bytes = next_bytes
                    .checked_sub(old_bytes)
                    .and_then(|bytes| bytes.checked_add(fact_bytes))
                    .ok_or(Error::Budget)?;
            }
            if next_bytes > self.limits.bytes {
                return Err(Error::Budget);
            }
        }
        let mut correlated = self.resolve_operation(&key, &observation, current, &mut work)?;
        let changed_basis = observation
            .fact
            .as_ref()
            .is_some_and(|fact| current.is_some_and(|old| old.basis != fact.basis));
        let roots = self.invalidation_roots(&key, changed_basis, correlated, &mut work)?;
        let affected = self.affected(roots, &mut work)?;
        if work > self.limits.work {
            return Err(Error::Budget);
        }
        if let Some((id, _, prediction)) = &mut correlated
            && affected.contains(id)
            && matches!(
                *prediction,
                PredictionOutcome::Pending | PredictionOutcome::Confirmed
            )
        {
            *prediction = PredictionOutcome::Invalidated;
        }
        // All fallible planning precedes mutation, including dependency closure.
        let mut changed = Vec::new();
        for id in affected {
            if let Some(branch) = self.branches.get_mut(&id)
                && branch.prediction != PredictionOutcome::Invalidated
            {
                branch.prediction = PredictionOutcome::Invalidated;
                changed.push(id);
            }
        }
        if let Some((id, outcome, prediction)) = correlated
            && let Some(branch) = self.branches.get_mut(&id)
            && (branch.outcome != outcome || branch.prediction != prediction)
        {
            branch.outcome = outcome;
            branch.prediction = prediction;
            if !changed.contains(&id) {
                changed.push(id);
            }
        }
        let authoritative = observation
            .fact
            .as_ref()
            .is_some_and(|fact| current.is_none_or(|old| old.basis != fact.basis));
        if let Some(fact) = observation.fact {
            // Same exact content basis preserves the existing reference identity.
            if current.is_none_or(|old| old.basis != fact.basis) {
                self.facts.insert(key, fact);
            }
        }
        self.bytes = next_bytes;
        Ok(Changes {
            authoritative,
            hypotheses: changed,
            work,
        })
    }

    /// Discard one hypothesis and invalidate descendants. Does not cancel,
    /// reject, undo or forget the original operation in the authoritative journal.
    pub fn discard(&mut self, id: BranchId) -> Result<Vec<BranchId>, Error> {
        if !self.branches.contains_key(&id) {
            return Err(Error::Missing);
        }
        let mut work = 0;
        let affected = self.affected(BTreeSet::from([id]), &mut work)?;
        self.charge_removal(id, &mut work)?;
        for child in &affected {
            if *child != id
                && let Some(branch) = self.branches.get_mut(child)
            {
                branch.prediction = PredictionOutcome::Invalidated;
            }
        }
        self.remove(id);
        Ok(affected.into_iter().collect())
    }

    fn charge_removal(&self, id: BranchId, work: &mut usize) -> Result<(), Error> {
        let branch = self.branches.get(&id).ok_or(Error::Missing)?;
        // Removal visits incoming edges in addition to the outgoing closure.
        *work = work
            .checked_add(branch.dependencies.len())
            .ok_or(Error::Budget)?;
        if *work > self.limits.work {
            return Err(Error::Budget);
        }
        Ok(())
    }

    fn remove(&mut self, id: BranchId) {
        if let Some(branch) = self.branches.remove(&id) {
            self.bytes -= branch.bytes;
            self.edges -= branch.dependencies.len();
            if let Some(ids) = self.by_key.get_mut(&branch.key) {
                ids.remove(&id);
                if ids.is_empty() {
                    self.by_key.remove(&branch.key);
                }
            }
            if let Some(operation) = &branch.operation {
                self.by_operation.remove(operation);
            }
            for dependency in branch.dependencies {
                if let Some(ids) = self.dependents.get_mut(&dependency.branch) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        self.dependents.remove(&dependency.branch);
                    }
                }
            }
            self.dependents.remove(&id);
        }
    }

    /// Explicit logical retention maintenance, bounded by active demand. Host
    /// ticks are monotonic; there is no clock, timer or background worker.
    pub fn advance(&mut self, now: u64) -> Result<Vec<BranchId>, Error> {
        if now < self.now {
            return Err(Error::Time);
        }
        let mut work = self.branches.len();
        if work > self.limits.work {
            return Err(Error::Budget);
        }
        let expired: BTreeSet<_> = self
            .branches
            .values()
            .filter(|b| b.expires <= now)
            .map(|b| b.id)
            .collect();
        let affected = self.affected(expired.clone(), &mut work)?;
        for id in &expired {
            self.charge_removal(*id, &mut work)?;
        }
        for id in &affected {
            if let Some(branch) = self.branches.get_mut(id) {
                branch.prediction = PredictionOutcome::Invalidated;
            }
        }
        for id in expired {
            self.remove(id);
        }
        self.now = now;
        Ok(affected.into_iter().collect())
    }

    /// Release demanded canonical data when no hypothesis references it.
    /// Caller snapshots remain valid; the adapter's journal is unaffected.
    pub fn release(&mut self, key: &D::Key) -> Result<(), Error> {
        if self.by_key.contains_key(key) {
            return Err(Error::Conflict);
        }
        let fact = self.facts.get(key).ok_or(Error::Missing)?;
        let bytes = fact
            .bytes
            .checked_add(metadata_bytes::<Record<D>>()?)
            .ok_or(Error::Budget)?;
        self.facts.remove(key);
        self.bytes -= bytes;
        Ok(())
    }
}
