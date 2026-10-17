use super::*;

/// Coordinates one journal adapter and one physical backend.
pub struct JournaledMaterializer<S, B> {
    pub(super) store: S,
    pub(super) backend: B,
}
impl<S, B> JournaledMaterializer<S, B> {
    /// Creates a materializer.
    pub const fn new(store: S, backend: B) -> Self {
        Self { store, backend }
    }
    /// Journal adapter.
    pub const fn store(&self) -> &S {
        &self.store
    }
    /// Physical backend.
    pub const fn backend(&self) -> &B {
        &self.backend
    }
}
impl<S: MaterializationJournalStore, B: MaterializationBackend> JournaledMaterializer<S, B> {
    /// Applies a plan with authorization at every physical boundary.
    pub async fn apply<E, F, Fut>(
        &self,
        plan: MaterializationPlan,
        guard: &mut F,
    ) -> Result<MaterializationJournal, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let journal = match self.load(plan.operation_id).await? {
            Some(journal) if journal.plan == plan => journal,
            Some(_) => return Err(MaterializationError::PlanConflict.into()),
            None => {
                self.prepare(
                    plan,
                    #[cfg(not(target_arch = "wasm32"))]
                    None,
                )
                .await?
            }
        };
        self.replay(journal, MaterializationRecovery::Complete, guard)
            .await
    }
    /// Completes or restores an interrupted operation.
    pub async fn recover<E, F, Fut>(
        &self,
        operation: OperationId,
        recovery: MaterializationRecovery,
        guard: &mut F,
    ) -> Result<Option<MaterializationJournal>, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        match self.load(operation).await? {
            Some(journal) => self.replay(journal, recovery, guard).await.map(Some),
            None => Ok(None),
        }
    }
    /// Persists immutable intent before staging or preimage capture.
    pub async fn prepare(
        &self,
        plan: MaterializationPlan,
        #[cfg(not(target_arch = "wasm32"))] custody: Option<NativeAllocation>,
    ) -> Result<MaterializationJournal, MaterializationError<S::Error, B::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        let custody = match custody {
            Some(custody) => Some(custody),
            None => self
                .backend
                .native_work()
                .map(|work| work.claim(plan_owned_bytes(&plan)?, 0, crate::WorkCounters::UNCHARGED))
                .transpose()?,
        };
        self.check_plan(&plan)?;
        let journal = MaterializationJournal {
            version: JOURNAL_VERSION,
            revision: 1,
            plan,
            root_identity: self.backend.allocation_identity(),
            preparation_identity: self.backend.preparation_identity(),
            preparation_progress: None,
            work: self.observed_work()?.unwrap_or_default(),
            preimages: Vec::new(),
            phase: MaterializationPhase::Preparing,
            applied: 0,
            restored: 0,
            #[cfg(not(target_arch = "wasm32"))]
            custody,
        };
        self.check_custody(&journal)?;
        let (stored, journal) = self
            .store
            .compare_and_swap(journal.plan.operation_id, 0, journal)
            .await
            .map_err(MaterializationError::Store)?;
        let journal = if stored {
            journal
        } else {
            let current = self
                .load(journal.plan.operation_id)
                .await?
                .ok_or(MaterializationError::Contended)?;
            if current.plan != journal.plan {
                return Err(MaterializationError::PlanConflict);
            }
            current
        };
        self.verify_allocation(&journal).await?;
        Ok(journal)
    }
    /// Loads validated progress on its original accounting context.
    pub async fn load(
        &self,
        operation: OperationId,
    ) -> Result<Option<MaterializationJournal>, MaterializationError<S::Error, B::Error>> {
        let journal = self
            .store
            .load(operation)
            .await
            .map_err(MaterializationError::Store)?;
        if let Some(journal) = &journal {
            if !valid_journal(journal, operation) {
                return Err(MaterializationError::IncompatibleJournal);
            }
            self.check_plan(&journal.plan)?;
            self.check_custody(journal)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.backend.native_work() {
            work.set_original_work(
                journal
                    .as_ref()
                    .map_or(crate::WorkCounters::UNCHARGED, |journal| journal.work),
            );
        }
        Ok(journal)
    }
    pub(super) async fn capture_preimages<E, F, Fut>(
        &self,
        mut journal: MaterializationJournal,
        guard: &mut F,
    ) -> Result<MaterializationJournal, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        if journal.preparation_progress.is_some() {
            return Err(MaterializationError::IncompatibleJournal.into());
        }
        guard().await?;
        #[cfg(not(target_arch = "wasm32"))]
        if self.backend.native_work().is_some()
            && journal.preimages.capacity() < journal.plan.edits.len()
        {
            let custody = journal
                .custody
                .as_mut()
                .ok_or(NativeTreeMaterializationError::MissingCustody)
                .map_err(MaterializationError::Native)?;
            reserve_buffer(&mut journal.preimages, journal.plan.edits.len(), custody)
                .map_err(MaterializationError::Native)?;
        }
        while journal.preimages.len() < journal.plan.edits.len() {
            self.verify_effect_context(&journal).await?;
            let preimage = self
                .backend
                .capture(
                    journal
                        .plan
                        .edits
                        .get(journal.preimages.len())
                        .ok_or(MaterializationError::IncompatibleJournal)?,
                )
                .await
                .map_err(MaterializationError::Backend)?;
            #[cfg(not(target_arch = "wasm32"))]
            let mut preimage = preimage;
            #[cfg(not(target_arch = "wasm32"))]
            if self.backend.native_work().is_some() {
                let mut allocation = preimage
                    .custody
                    .take()
                    .ok_or(NativeTreeMaterializationError::MissingCustody)
                    .map_err(MaterializationError::Native)?;
                let custody = journal
                    .custody
                    .as_mut()
                    .ok_or(NativeTreeMaterializationError::MissingCustody)
                    .map_err(MaterializationError::Native)?;
                custody
                    .absorb(&mut allocation)
                    .map_err(MaterializationError::Native)?;
            }
            journal.preimages.push(preimage);
            journal = self.persist(journal).await?;
            guard().await?;
        }
        journal.phase = if journal.plan.edits.is_empty() {
            MaterializationPhase::Applied
        } else {
            MaterializationPhase::Prepared
        };
        self.persist(journal).await.map_err(Into::into)
    }
    pub(super) async fn replay<E, F, Fut>(
        &self,
        mut journal: MaterializationJournal,
        recovery: MaterializationRecovery,
        guard: &mut F,
    ) -> Result<MaterializationJournal, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        use MaterializationPhase::*;
        use MaterializationRecovery::*;
        self.verify_allocation(&journal).await?;
        match (recovery, journal.phase) {
            (Complete, RolledBack) => return Err(MaterializationError::PlanConflict.into()),
            (Complete, RollingBack) => return Err(MaterializationError::IncompatibleJournal.into()),
            (Complete, _) if journal.restored != 0 => {
                return Err(MaterializationError::IncompatibleJournal.into());
            }
            (Complete, Preparing) => journal = self.capture_preimages(journal, guard).await?,
            (RollBack, _) if journal.preparation_progress.is_some() => {
                return Err(MaterializationError::IncompatibleJournal.into());
            }
            (RollBack, RolledBack) => return Ok(journal),
            (RollBack, Preparing) => {
                journal.phase = RolledBack;
                return self.persist(journal).await.map_err(Into::into);
            }
            (RollBack, Applying) if (journal.applied as usize) < journal.plan.edits.len() => {
                journal.applied += 1;
            }
            _ => {}
        }
        let forward = recovery == Complete;
        let repair = forward && journal.phase == Applied;
        let target = if forward {
            MaterializationObservation::Postimage
        } else {
            MaterializationObservation::Preimage
        };
        if !repair {
            journal.phase = if forward { Applying } else { RollingBack };
            journal = self.persist(journal).await?;
        }
        for offset in 0..journal.plan.edits.len() {
            if !repair
                && (forward && journal.applied as usize >= journal.plan.edits.len()
                    || !forward && journal.restored >= journal.applied)
            {
                break;
            }
            let index = if repair {
                offset
            } else if forward {
                journal.applied as usize
            } else {
                (journal.applied - journal.restored - 1) as usize
            };
            self.verify_allocation(&journal).await?;
            let edit = journal
                .plan
                .edits
                .get(index)
                .ok_or(MaterializationError::IncompatibleJournal)?;
            let preimage = journal
                .preimages
                .get(index)
                .ok_or(MaterializationError::IncompatibleJournal)?;
            let effect = match self
                .backend
                .observe(edit, preimage)
                .await
                .map_err(MaterializationError::Backend)?
            {
                MaterializationObservation::Diverged => {
                    return Err(MaterializationError::ExternalMutation.into());
                }
                MaterializationObservation::Unverified => !repair,
                observed => observed != target,
            };
            if effect || !repair {
                guard().await?;
                self.verify_effect_context(&journal).await?;
                if !effect {
                    self.backend.settle_observed(edit).await
                } else if forward {
                    self.backend.apply(edit, preimage).await
                } else {
                    self.backend.restore(edit, preimage).await
                }
                .map_err(MaterializationError::Backend)?;
            }
            if !repair {
                if forward {
                    journal.applied += 1;
                } else {
                    journal.restored += 1;
                }
                journal = self.persist(journal).await?;
            }
        }
        if repair {
            return Ok(journal);
        }
        journal.phase = if forward { Applied } else { RolledBack };
        self.persist(journal).await.map_err(Into::into)
    }
    pub(super) async fn verify_allocation(
        &self,
        journal: &MaterializationJournal,
    ) -> Result<(), MaterializationError<S::Error, B::Error>> {
        if self.backend.allocation_identity() != journal.root_identity
            || self
                .backend
                .observe_allocation_identity()
                .await
                .map_err(MaterializationError::Backend)?
                != journal.root_identity
            || self.backend.preparation_identity() != journal.preparation_identity
            || self
                .backend
                .observe_preparation_identity()
                .await
                .map_err(MaterializationError::Backend)?
                != journal.preparation_identity
        {
            return Err(MaterializationError::ExternalMutation);
        }
        Ok(())
    }
    pub(super) async fn verify_effect_context(
        &self,
        journal: &MaterializationJournal,
    ) -> Result<(), MaterializationError<S::Error, B::Error>> {
        if self.load(journal.plan.operation_id).await?.as_ref() != Some(journal) {
            return Err(MaterializationError::PlanConflict);
        }
        self.verify_allocation(journal).await
    }
    /// Publishes progress with its current revision.
    pub async fn persist(
        &self,
        mut journal: MaterializationJournal,
    ) -> Result<MaterializationJournal, MaterializationError<S::Error, B::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            if let Some(work) = self.observed_work()? {
                journal.work = work;
            }
            let expected = next_revision(&mut journal.revision);
            let (stored, replacement) = self
                .store
                .compare_and_swap(journal.plan.operation_id, expected, journal)
                .await
                .map_err(MaterializationError::Store)?;
            journal = replacement;
            if stored {
                return Ok(journal);
            }
            let current = self
                .load(journal.plan.operation_id)
                .await?
                .ok_or(MaterializationError::IncompatibleJournal)?;
            if current.plan != journal.plan {
                return Err(MaterializationError::PlanConflict);
            }
            journal = current;
        }
        Err(MaterializationError::Contended)
    }
    fn observed_work(
        &self,
    ) -> Result<Option<crate::WorkCounters>, MaterializationError<S::Error, B::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        return self
            .backend
            .native_work()
            .map(NativeMaterializationWork::journal_work)
            .transpose()
            .map_err(Into::into);
        #[cfg(target_arch = "wasm32")]
        Ok(None)
    }
    pub(super) fn check_custody(
        &self,
        _journal: &MaterializationJournal,
    ) -> Result<(), MaterializationError<S::Error, B::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.backend.native_work() {
            let allocation = _journal
                .custody
                .as_ref()
                .ok_or(NativeTreeMaterializationError::MissingCustody)?;
            if !work.shares_allocation_context(allocation.work())
                || allocation.bytes() < journal_owned_bytes(_journal)?
            {
                return Err(NativeTreeMaterializationError::MissingCustody.into());
            }
        }
        Ok(())
    }
    pub(super) fn check_plan(
        &self,
        plan: &MaterializationPlan,
    ) -> Result<(), MaterializationError<S::Error, B::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.backend.native_work() {
            check_native_plan(plan, work)?;
            return Ok(());
        }
        let mut paths = Vec::new();
        paths
            .try_reserve_exact(
                plan.edits
                    .len()
                    .checked_mul(2)
                    .ok_or(MaterializationError::IncompatibleJournal)?,
            )
            .map_err(|_| MaterializationError::IncompatibleJournal)?;
        if validate_plan(plan, paths) {
            Ok(())
        } else {
            Err(MaterializationError::IncompatibleJournal)
        }
    }
}
pub(super) fn valid_journal(journal: &MaterializationJournal, operation: OperationId) -> bool {
    let count = journal.plan.edits.len();
    let applied = journal.applied as usize;
    let restored = journal.restored as usize;
    let allocations = journal.preparation_progress.is_none_or(|_| {
        journal.phase == MaterializationPhase::Preparing
            && journal.preimages.is_empty()
            && journal.root_identity.is_some()
            && journal.preparation_identity.is_none()
    });
    journal.version == JOURNAL_VERSION
        && journal.revision != 0
        && journal.plan.operation_id == operation
        && allocations
        && applied <= count
        && restored <= applied
        && match journal.phase {
            MaterializationPhase::Preparing => {
                journal.preimages.len() <= count && applied == 0 && restored == 0
            }
            MaterializationPhase::Prepared => {
                journal.preimages.len() == count && applied == 0 && restored == 0
            }
            MaterializationPhase::Applying => journal.preimages.len() == count && restored == 0,
            MaterializationPhase::Applied => {
                journal.preimages.len() == count && applied == count && restored == 0
            }
            MaterializationPhase::RollingBack => journal.preimages.len() == count,
            MaterializationPhase::RolledBack => {
                (applied == 0 || journal.preimages.len() == count)
                    && journal.preimages.len() <= count
                    && restored == applied
            }
        }
}
pub(super) fn validate_plan<'a>(plan: &'a MaterializationPlan, mut paths: Vec<&'a str>) -> bool {
    if plan.edits.len() > u32::MAX as usize {
        return false;
    }
    for edit in &plan.edits {
        match edit {
            MaterializationEdit::SetMetadata { path, .. } => {
                if !crate::path::is_canonical_relative(path) {
                    return false;
                }
            }
            MaterializationEdit::Install { path, .. } | MaterializationEdit::Remove { path } => {
                paths.push(path);
            }
            MaterializationEdit::Rename { from, to } => {
                paths.extend([from.as_str(), to.as_str()]);
            }
        }
    }
    for path in &paths {
        if !crate::path::is_canonical_relative(path) {
            return false;
        }
    }
    paths.sort_unstable();
    for [left, right] in paths.array_windows::<2>() {
        if left == right
            || right
                .strip_prefix(*left)
                .is_some_and(|suffix| suffix.starts_with('/'))
        {
            return false;
        }
    }
    true
}
