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

    /// Observe trusted evidence for one demanded key.
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
}
