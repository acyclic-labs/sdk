use super::*;

impl<D: Domain> Client<D> {
    /// Create a hypothesis from an authoritative basis.
    pub fn begin(&mut self, request: Request<D>) -> Result<BranchId, Error> {
        let id = BranchId {
            namespace: self.namespace,
            sequence: self.sequence.checked_add(1).ok_or(Error::Budget)?,
        };
        let branch = Hypothesis {
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
            bytes: 0,
        };
        let fact = self.preflight(&branch)?;
        let measured = self.domain.validate(
            fact,
            &branch.predicted,
            &branch.assumption,
            self.limits.work - branch.dependencies.len(),
        )?;
        self.publish(branch, measured)?;
        self.sequence = id.sequence;
        Ok(id)
    }

    /// Import a hypothesis against its restored authoritative record.
    pub fn restore(&mut self, branch: ClientHypothesis<D>) -> Result<(), Error> {
        if branch.adapter != self.adapter
            || branch.id.namespace != self.namespace
            || branch.id.sequence == 0
            || branch.id.sequence > self.sequence
            || self.branches.contains_key(&branch.id)
        {
            return Err(Error::Conflict);
        }
        let fact = self.preflight(&branch)?;
        let measured =
            self.domain
                .restore(fact, &branch, self.limits.work - branch.dependencies.len())?;
        self.publish(branch, measured)
    }

    fn preflight(&self, branch: &ClientHypothesis<D>) -> Result<&Record<D>, Error> {
        if self.domain.identity() != self.adapter {
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
        Ok(fact)
    }

    fn publish(
        &mut self,
        mut branch: ClientHypothesis<D>,
        (bytes, work): (usize, usize),
    ) -> Result<(), Error> {
        if work > self.limits.work - branch.dependencies.len() {
            return Err(Error::Budget);
        }
        let dependencies =
            dependency_bytes(branch.dependencies.len(), branch.dependencies.capacity())?;
        branch.bytes = bytes
            .checked_add(metadata_bytes::<ClientHypothesis<D>>()?)
            .and_then(|bytes| bytes.checked_add(dependencies))
            .ok_or(Error::Budget)?;
        if branch.bytes > self.limits.bytes.saturating_sub(self.bytes) {
            return Err(Error::Budget);
        }
        for edge in &branch.dependencies {
            self.dependents
                .entry(edge.branch)
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
        self.edges += branch.dependencies.len();
        self.bytes += branch.bytes;
        self.branches.insert(branch.id, branch);
        Ok(())
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
}
