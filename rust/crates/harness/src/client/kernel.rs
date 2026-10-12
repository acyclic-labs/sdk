use super::*;

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
    pub(super) domain: D,
    adapter: u128,
    limits: Limits,
    namespace: u128,
    sequence: u64,
    now: u64,
    facts: BTreeMap<D::Key, Record<D>>,
    pub(super) branches: BTreeMap<BranchId, ClientHypothesis<D>>,
    by_key: BTreeMap<D::Key, BTreeSet<BranchId>>,
    by_operation: BTreeMap<D::Operation, BranchId>,
    dependents: BTreeMap<BranchId, BTreeSet<BranchId>>,
    bytes: usize,
    edges: usize,
}

impl<D: Domain> Client<D> {
    /// Construct an empty client with explicit finite positive bounds.
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

    /// Accounted resident bytes, records, branches and edges.
    pub fn residency(&self) -> (usize, usize, usize, usize) {
        (
            self.bytes,
            self.facts.len(),
            self.branches.len(),
            self.edges,
        )
    }

    /// Pure creation of a hypothesis.
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
            .checked_add(metadata_bytes::<ClientHypothesis<D>>()?)
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

    /// Select one demanded record and explicit overlays.
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
    pub fn hypothesis(&self, id: BranchId) -> Option<&ClientHypothesis<D>> {
        self.branches.get(&id)
    }

    /// Import one data-only hypothesis after restoring its authoritative record.
    pub fn restore(&mut self, mut branch: ClientHypothesis<D>) -> Result<(), Error> {
        if self.domain.identity() != self.adapter
            || branch.adapter != self.adapter
            || branch.id.namespace != self.namespace
            || branch.id.sequence == 0
            || branch.id.sequence > self.sequence
            || self.branches.contains_key(&branch.id)
        {
            return Err(Error::Conflict);
        }
        if branch.expires <= self.now || branch.expires - self.now > self.limits.retention {
            return Err(Error::Time);
        }
        if self.branches.len() >= self.limits.branches
            || branch.dependencies.len() > self.limits.work
            || branch.dependencies.len() > self.limits.edges.saturating_sub(self.edges)
        {
            return Err(Error::Budget);
        }
        if branch
            .operation
            .as_ref()
            .is_some_and(|o| self.by_operation.contains_key(o))
        {
            return Err(Error::Conflict);
        }
        let mut unique = BTreeSet::new();
        for edge in &branch.dependencies {
            let parent = self.branches.get(&edge.branch).ok_or(Error::Missing)?;
            if parent.id.namespace != branch.id.namespace
                || parent.id.sequence >= branch.id.sequence
                || !unique.insert(edge.branch)
            {
                return Err(Error::Conflict);
            }
            if matches!(
                branch.prediction,
                PredictionOutcome::Pending | PredictionOutcome::Confirmed
            ) {
                let eligible = match edge.requirement {
                    DependencyRequirement::Prediction => matches!(
                        parent.prediction,
                        PredictionOutcome::Pending | PredictionOutcome::Confirmed
                    ),
                    DependencyRequirement::Confirmed => {
                        parent.prediction == PredictionOutcome::Confirmed
                    }
                };
                if !eligible {
                    return Err(Error::Conflict);
                }
            }
        }
        let fact = self.facts.get(&branch.key).ok_or(Error::Missing)?;
        if branch.prediction == PredictionOutcome::Pending && fact.basis != branch.basis {
            return Err(Error::StaleBasis);
        }
        let allowance = self.limits.work - unique.len();
        let (bytes, work) = self.domain.restore(fact, &branch, allowance)?;
        if work > allowance {
            return Err(Error::Budget);
        }
        let bytes = bytes
            .checked_add(metadata_bytes::<ClientHypothesis<D>>()?)
            .and_then(|n| {
                n.checked_add(dependency_bytes(unique.len(), branch.dependencies.capacity()).ok()?)
            })
            .ok_or(Error::Budget)?;
        if bytes > self.limits.bytes.saturating_sub(self.bytes) {
            return Err(Error::Budget);
        }
        branch.bytes = bytes;
        for parent in &unique {
            self.dependents
                .entry(*parent)
                .or_default()
                .insert(branch.id);
        }
        self.by_key
            .entry(branch.key.clone())
            .or_default()
            .insert(branch.id);
        if let Some(operation) = &branch.operation {
            self.by_operation.insert(operation.clone(), branch.id);
        }
        self.edges += unique.len();
        self.bytes += bytes;
        self.branches.insert(branch.id, branch);
        Ok(())
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
                // Late completion cannot revive invalid dependencies.
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

    /// Observe trusted evidence for one explicitly demanded key.
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

    /// Discard one hypothesis and invalidate descendants.
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

    /// Explicit logical retention maintenance, bounded by active demand.
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
