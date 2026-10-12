use super::*;

/// Coordinates one journal store and one host backend.
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
    /// Host backend.
    pub const fn backend(&self) -> &B {
        &self.backend
    }
}
impl<S: MaterializationJournalStore, B: MaterializationBackend> JournaledMaterializer<S, B> {
    /// Applies an immutable plan, checking authorization before each effect.
    pub async fn apply<E, F, Fut>(
        &self,
        plan: MaterializationPlan,
        before_effect: &mut F,
    ) -> Result<MaterializationJournal, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let journal = match self.load(plan.operation_id).await? {
            Some(journal) => {
                if journal.plan != plan {
                    return Err(MaterializationError::PlanConflict.into());
                }
                journal
            }
            None => {
                self.prepare(
                    plan,
                    #[cfg(not(target_arch = "wasm32"))]
                    None,
                )
                .await?
            }
        };
        self.replay(journal, MaterializationRecovery::Complete, before_effect)
            .await
    }
    /// Completes or restores an interrupted operation.
    pub async fn recover<E, F, Fut>(
        &self,
        operation: OperationId,
        recovery: MaterializationRecovery,
        before_effect: &mut F,
    ) -> Result<Option<MaterializationJournal>, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        match self.load(operation).await? {
            Some(journal) => self
                .replay(journal, recovery, before_effect)
                .await
                .map(Some),
            None => Ok(None),
        }
    }
    /// Persists intent before staging or preimage capture.
    pub async fn prepare(
        &self,
        plan: MaterializationPlan,
        #[cfg(not(target_arch = "wasm32"))] custody: Option<NativeAllocation>,
    ) -> Result<MaterializationJournal, MaterializationError<S::Error, B::Error>> {
        self.check_plan(&plan)?;
        if let MaterializationIntent::GenerationTransition { from, .. } = plan.intent {
            if !self
                .backend
                .verify_source(from)
                .await
                .map_err(MaterializationError::Backend)?
            {
                return Err(MaterializationError::PlanConflict);
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        let custody = match custody {
            Some(custody) => Some(custody),
            None => self
                .backend
                .native_work(|work| {
                    work.claim(plan_owned_bytes(&plan)?, 0, crate::WorkCounters::UNCHARGED)
                })
                .map_err(MaterializationError::Backend)?,
        };
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
    /// Loads compatible progress on its original accounting context.
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
        self.backend
            .native_work(|work| {
                work.set_original_work(
                    journal
                        .as_ref()
                        .map_or(crate::WorkCounters::UNCHARGED, |journal| journal.work),
                );
                Ok(())
            })
            .map_err(MaterializationError::Backend)?;
        Ok(journal)
    }
    pub(super) async fn capture_preimages<E, F, Fut>(
        &self,
        mut journal: MaterializationJournal,
        before_effect: &mut F,
    ) -> Result<MaterializationJournal, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        if journal.preparation_progress.is_some() {
            return Err(MaterializationError::IncompatibleJournal.into());
        }
        before_effect().await?;
        while journal.preimages.len() < journal.plan.edits.len() {
            self.verify_effect_context(&journal).await?;
            let preimage = self
                .backend
                .capture(&journal.plan.edits[journal.preimages.len()])
                .await
                .map_err(MaterializationError::Backend)?;
            let mut preimage = Some(preimage);
            #[cfg(not(target_arch = "wasm32"))]
            self.backend
                .native_work(|work| {
                    let mut preimage = preimage.take().expect("captured preimage");
                    let mut image = preimage
                        .custody
                        .take()
                        .ok_or(NativeTreeMaterializationError::MissingCustody)?;
                    let custody = journal
                        .custody
                        .as_mut()
                        .ok_or(NativeTreeMaterializationError::MissingCustody)?;
                    if !work.shares_allocation_context(custody.work()) {
                        return Err(NativeTreeMaterializationError::InvalidLayout);
                    }
                    custody.absorb(&mut image)?;
                    native_vec_push(&mut journal.preimages, preimage, custody)
                })
                .map_err(MaterializationError::Backend)?;
            if let Some(preimage) = preimage {
                journal.preimages.push(preimage);
            }
            journal = self.persist(journal).await?;
            before_effect().await?;
        }
        journal.phase = MaterializationPhase::Prepared;
        self.persist(journal).await.map_err(Into::into)
    }
    pub(super) async fn replay<E, F, Fut>(
        &self,
        mut journal: MaterializationJournal,
        recovery: MaterializationRecovery,
        before_effect: &mut F,
    ) -> Result<MaterializationJournal, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        self.verify_allocation(&journal).await?;
        match recovery {
            MaterializationRecovery::Complete => {
                if journal.phase == MaterializationPhase::RollingBack || journal.restored != 0 {
                    return Err(MaterializationError::IncompatibleJournal.into());
                }
                if journal.phase == MaterializationPhase::RolledBack {
                    return Err(MaterializationError::PlanConflict.into());
                }
                if journal.phase == MaterializationPhase::Preparing {
                    journal = self.capture_preimages(journal, before_effect).await?;
                }
            }
            MaterializationRecovery::RollBack => {
                if journal.preparation_progress.is_some() {
                    return Err(MaterializationError::IncompatibleJournal.into());
                }
                if journal.phase == MaterializationPhase::RolledBack {
                    return Ok(journal);
                }
                if journal.phase == MaterializationPhase::Preparing {
                    journal.phase = MaterializationPhase::RolledBack;
                    return self.persist(journal).await.map_err(Into::into);
                }
                // The next edit may have completed before its progress record.
                if journal.phase == MaterializationPhase::Applying
                    && (journal.applied as usize) < journal.plan.edits.len()
                {
                    journal.applied += 1;
                }
            }
        }
        let repair = recovery == MaterializationRecovery::Complete
            && journal.phase == MaterializationPhase::Applied;
        let mut forward = if repair { 0 } else { journal.applied as usize };
        if !repair {
            journal.phase = match recovery {
                MaterializationRecovery::Complete => MaterializationPhase::Applying,
                MaterializationRecovery::RollBack => MaterializationPhase::RollingBack,
            };
            journal = self.persist(journal).await?;
        }
        while match recovery {
            MaterializationRecovery::Complete => forward < journal.plan.edits.len(),
            MaterializationRecovery::RollBack => journal.restored < journal.applied,
        } {
            let index = match recovery {
                MaterializationRecovery::Complete => forward,
                MaterializationRecovery::RollBack => {
                    (journal.applied - journal.restored - 1) as usize
                }
            };
            self.verify_allocation(&journal).await?;
            let edit = &journal.plan.edits[index];
            let preimage = &journal.preimages[index];
            let observation = self
                .backend
                .observe(edit, preimage)
                .await
                .map_err(MaterializationError::Backend)?;
            if observation == MaterializationObservation::Diverged {
                return Err(MaterializationError::ExternalMutation.into());
            }
            let effect = !matches!(
                (recovery, observation),
                (
                    MaterializationRecovery::Complete,
                    MaterializationObservation::Postimage
                ) | (
                    MaterializationRecovery::RollBack,
                    MaterializationObservation::Preimage
                )
            ) && !(repair && observation == MaterializationObservation::Unverified);
            if effect || !repair {
                before_effect().await?;
                self.verify_effect_context(&journal).await?;
                if !effect {
                    self.backend
                        .settle_observed(edit)
                        .await
                        .map_err(MaterializationError::Backend)?;
                } else {
                    match recovery {
                        MaterializationRecovery::Complete => {
                            self.backend.apply(edit, preimage).await
                        }
                        MaterializationRecovery::RollBack => {
                            self.backend.restore(edit, preimage).await
                        }
                    }
                    .map_err(MaterializationError::Backend)?;
                }
            }
            forward += 1;
            if !repair {
                match recovery {
                    MaterializationRecovery::Complete => journal.applied += 1,
                    MaterializationRecovery::RollBack => journal.restored += 1,
                }
                journal = self.persist(journal).await?;
            }
        }
        if repair {
            return Ok(journal);
        }
        journal.phase = match recovery {
            MaterializationRecovery::Complete => MaterializationPhase::Applied,
            MaterializationRecovery::RollBack => MaterializationPhase::RolledBack,
        };
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
    async fn verify_effect_context(
        &self,
        journal: &MaterializationJournal,
    ) -> Result<(), MaterializationError<S::Error, B::Error>> {
        if self.load(journal.plan.operation_id).await?.as_ref() != Some(journal) {
            return Err(MaterializationError::PlanConflict);
        }
        self.verify_allocation(journal).await
    }
    /// Advances the journal using its current revision.
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
            .native_work(NativeMaterializationWork::journal_work)
            .map_err(MaterializationError::Backend);
        #[cfg(target_arch = "wasm32")]
        Ok(None)
    }
    fn check_custody(
        &self,
        _journal: &MaterializationJournal,
    ) -> Result<(), MaterializationError<S::Error, B::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        self.backend
            .native_work(|work| {
                let custody = _journal
                    .custody
                    .as_ref()
                    .ok_or(NativeTreeMaterializationError::MissingCustody)?;
                if !work.shares_allocation_context(custody.work())
                    || custody.bytes() < journal_owned_bytes(_journal)?
                {
                    return Err(NativeTreeMaterializationError::MissingCustody);
                }
                Ok(())
            })
            .map_err(MaterializationError::Backend)?;
        Ok(())
    }
    fn check_plan(
        &self,
        plan: &MaterializationPlan,
    ) -> Result<(), MaterializationError<S::Error, B::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        if self
            .backend
            .native_work(|work| check_native_plan(plan, work))
            .map_err(MaterializationError::Backend)?
            .is_some()
        {
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
        if validate_plan(plan, paths, || Ok::<_, ()>(())).unwrap_or(false) {
            Ok(())
        } else {
            Err(MaterializationError::IncompatibleJournal)
        }
    }
}

pub(super) async fn compare_next_journal<
    S: MaterializationJournalStore,
    B: std::error::Error + 'static,
>(
    store: &S,
    mut journal: MaterializationJournal,
) -> Result<(bool, MaterializationJournal), MaterializationError<S::Error, B>> {
    let expected = next_revision(&mut journal.revision);
    store
        .compare_and_swap(journal.plan.operation_id, expected, journal)
        .await
        .map_err(MaterializationError::Store)
}

pub(super) fn valid_journal(journal: &MaterializationJournal, operation: OperationId) -> bool {
    let count = journal.plan.edits.len();
    let applied = journal.applied as usize;
    let restored = journal.restored as usize;
    let allocations = journal.preparation_progress.is_none_or(|progress| {
        journal.phase == MaterializationPhase::Preparing
            && journal.preimages.is_empty()
            && journal.root_identity.is_some()
            && journal.preparation_identity.is_none()
            && (progress.target.is_none() || progress.operation.is_some())
            && (progress.backup.is_none() || progress.target.is_some())
            && match progress.pending {
                None => true,
                Some(MaterializationPreparationAllocation::Operation) => {
                    progress.operation.is_none()
                        && progress.target.is_none()
                        && progress.backup.is_none()
                }
                Some(MaterializationPreparationAllocation::Target) => {
                    progress.operation.is_some()
                        && progress.target.is_none()
                        && progress.backup.is_none()
                }
                Some(MaterializationPreparationAllocation::Backup) => {
                    progress.operation.is_some()
                        && progress.target.is_some()
                        && progress.backup.is_none()
                }
            }
    });
    journal.version == JOURNAL_VERSION
        && journal.plan.operation_id == operation
        && allocations
        && applied <= count
        && restored <= applied
        && journal.preimages.len() <= count
        && (journal.preimages.len() == count
            || journal.phase == MaterializationPhase::Preparing
            || journal.phase == MaterializationPhase::RolledBack && applied == 0)
        && match journal.phase {
            MaterializationPhase::Preparing | MaterializationPhase::Prepared => {
                applied == 0 && restored == 0
            }
            MaterializationPhase::Applying => restored == 0,
            MaterializationPhase::Applied => applied == count && restored == 0,
            MaterializationPhase::RollingBack => true,
            MaterializationPhase::RolledBack => restored == applied,
        }
}
pub(super) fn validate_plan<'a, E>(
    plan: &'a MaterializationPlan,
    mut paths: Vec<&'a str>,
    mut step: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    if matches!(&plan.intent, MaterializationIntent::NativePreparation { original, .. } if original.is_empty())
    {
        return Ok(false);
    }
    for edit in &plan.edits {
        step()?;
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(plan.intent, MaterializationIntent::NativePreparation { .. })
            && !matches!(edit, MaterializationEdit::Install {image, ..} if image.starts_with(NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX))
        {
            return Ok(false);
        }
        let (path, second) = match edit {
            MaterializationEdit::Install { path, .. } | MaterializationEdit::Remove { path } => {
                (path, None)
            }
            MaterializationEdit::SetMetadata { path, .. } => {
                if !crate::path::is_canonical_relative_with_step(path, &mut step)? {
                    return Ok(false);
                }
                continue;
            }
            MaterializationEdit::Rename { from, to } => {
                if from == to {
                    return Ok(false);
                }
                (from, Some(to))
            }
        };
        if !crate::path::is_canonical_relative_with_step(path, &mut step)? {
            return Ok(false);
        }
        paths.push(path);
        if let Some(second) = second {
            if !crate::path::is_canonical_relative_with_step(second, &mut step)? {
                return Ok(false);
            }
            paths.push(second);
        }
    }
    let mut failure = None;
    paths.sort_unstable_by(|a, b| {
        if failure.is_some() {
            return std::cmp::Ordering::Equal;
        }
        for (a, b) in a.bytes().zip(b.bytes()) {
            if let Err(error) = step() {
                failure = Some(error);
                return std::cmp::Ordering::Equal;
            }
            match a.cmp(&b) {
                std::cmp::Ordering::Equal => {}
                result => return result,
            }
        }
        a.len().cmp(&b.len())
    });
    if let Some(error) = failure {
        return Err(error);
    }
    for pair in paths.windows(2) {
        let [left, right] = pair else { continue };
        step()?;
        if left == right
            || right
                .strip_prefix(*left)
                .is_some_and(|suffix| suffix.starts_with('/'))
        {
            return Ok(false);
        }
    }
    Ok(true)
}
pub(super) fn validate_materialization_path(path: &str) -> Result<(), ()> {
    crate::path::is_canonical_relative(path)
        .then_some(())
        .ok_or(())
}
