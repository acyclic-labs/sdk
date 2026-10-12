use super::*;

impl<D: Domain> Client<D> {
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
