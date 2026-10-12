use super::*;

impl<D: Domain> Client<D> {
    pub(super) fn affected(
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
