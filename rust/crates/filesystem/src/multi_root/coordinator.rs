use super::*;

impl<S: MultiRootPublicationStore, P: MultiRootPublisher, A: MultiRootPublicationAuthorizer>
    MultiRootPublicationCoordinator<S, P, A>
{
    /// Resumes one durable publication, returning `None` when no such operation exists.
    pub async fn resume(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<Publication>, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        self.resume_with_finalization(operation_id, true).await
    }

    /// Resumes publication while retaining an Applied journal until an external, idempotent compatibility projection has been recorded.
    pub async fn resume_retained(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<Publication>, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        self.resume_with_finalization(operation_id, false).await
    }

    async fn resume_with_finalization(
        &self,
        operation_id: OperationId,
        finalize: bool,
    ) -> Result<Option<Publication>, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        let Some(journal) = self
            .store
            .load(operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
        else {
            return Ok(None);
        };
        validate_journal(&journal).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
        if journal.phase == MultiRootPublicationPhase::Applied {
            if finalize {
                self.finalize(&journal).await?;
            }
            return Ok(Some(Publication::Applied(journal)));
        }
        if journal.phase == MultiRootPublicationPhase::Conflicted {
            return self
                .project_conflicted(journal)
                .await
                .map(|journal| Some(Publication::Conflicted(journal)));
        }
        if journal.phase == MultiRootPublicationPhase::Aborting {
            self.abort_journal(journal).await?;
            return Ok(None);
        }
        self.publish_with_finalization(journal.candidate, finalize)
            .await
            .map(Some)
    }

    /// Lists durable publication operations for restart recovery.
    pub async fn pending_operations(
        &self,
    ) -> Result<Vec<OperationId>, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        self.store
            .list_operations()
            .await
            .map_err(MultiRootPublicationError::Store)
    }

    /// Validates, commits, and rolls forward one exact candidate.
    pub fn publish(
        &self,
        candidate: MultiRootMergeCandidate,
    ) -> impl Future<Output = MultiRootResult<Publication, S::Error, P::Error, A::Error>>
    + ProviderTask
    + '_ {
        Box::pin(async move { self.publish_with_finalization(candidate, true).await })
    }

    /// Publishes while retaining the terminal journal for a caller-owned, idempotent compatibility projection before acknowledgement.
    pub fn publish_retained(
        &self,
        candidate: MultiRootMergeCandidate,
    ) -> impl Future<Output = MultiRootResult<Publication, S::Error, P::Error, A::Error>>
    + ProviderTask
    + '_ {
        Box::pin(async move { self.publish_with_finalization(candidate, false).await })
    }

    async fn publish_with_finalization(
        &self,
        candidate: MultiRootMergeCandidate,
        finalize: bool,
    ) -> MultiRootResult<Publication, S::Error, P::Error, A::Error> {
        validate_candidate(&candidate).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
        if !self
            .authorizer
            .authorize(&candidate.plan)
            .await
            .map_err(MultiRootPublicationError::Authorizer)?
        {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        let mut journal = self.load_or_create(candidate).await?;
        validate_journal(&journal).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
        if journal.phase == MultiRootPublicationPhase::Applied {
            if finalize {
                self.finalize(&journal).await?;
            }
            return Ok(Publication::Applied(journal));
        }
        if journal.phase == MultiRootPublicationPhase::Conflicted {
            return self
                .project_conflicted(journal)
                .await
                .map(Publication::Conflicted);
        }
        if journal.phase == MultiRootPublicationPhase::Validating {
            journal = match self.validate_and_commit(journal).await? {
                MultiRootValidation::Committed(journal) => journal,
                MultiRootValidation::Stale(root_id, journal) => {
                    self.discard_uncommitted(&journal).await?;
                    return Ok(Publication::StaleBeforeCommit(root_id));
                }
                MultiRootValidation::Conflicted(journal) => {
                    return self
                        .project_conflicted(journal)
                        .await
                        .map(Publication::Conflicted);
                }
            };
        }
        self.roll_forward(journal, finalize).await
    }

    /// Accepts the ordinary edits in a fully projected conflicted working copy.
    pub fn continue_conflicted(
        &self,
        operation_id: OperationId,
        caller_context_id: WorkspaceContextId,
    ) -> impl Future<Output = MultiRootResult<Publication, S::Error, P::Error, A::Error>>
    + ProviderTask
    + '_ {
        Box::pin(async move {
            self.continue_conflicted_with_finalization(operation_id, caller_context_id, true)
                .await
        })
    }

    /// Continues a conflict while retaining the terminal journal until the caller acknowledges its compatibility projection.
    pub fn continue_conflicted_retained(
        &self,
        operation_id: OperationId,
        caller_context_id: WorkspaceContextId,
    ) -> impl Future<Output = MultiRootResult<Publication, S::Error, P::Error, A::Error>>
    + ProviderTask
    + '_ {
        Box::pin(async move {
            self.continue_conflicted_with_finalization(operation_id, caller_context_id, false)
                .await
        })
    }

    /// Declares the current projected contents of conflict paths as the caller-selected resolutions.
    pub async fn declare_conflicted_paths(
        &self,
        operation_id: OperationId,
        caller_context_id: WorkspaceContextId,
        root_id: WorkspaceRootId,
        paths: &[String],
    ) -> MultiRootResult<(), S::Error, P::Error, A::Error> {
        let mut journal = self
            .store
            .load(operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
            .ok_or(MultiRootPublicationError::InvalidCandidate)?;
        validate_journal(&journal).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
        if journal.phase != MultiRootPublicationPhase::Conflicted
            || !self
                .authorizer
                .authorize_transition(&journal.candidate.plan, caller_context_id)
                .await
                .map_err(MultiRootPublicationError::Authorizer)?
        {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        let root = journal
            .candidate
            .plan
            .roots
            .get(&root_id)
            .ok_or(MultiRootPublicationError::InvalidCandidate)?;
        let conflict = journal
            .conflicts
            .get(&root_id)
            .ok_or(MultiRootPublicationError::InvalidCandidate)?;
        let declared = self
            .publisher
            .declare_conflicts(operation_id, root_id, root, conflict, paths)
            .await
            .map_err(MultiRootPublicationError::Publisher)?;
        if declared.is_empty() {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        journal
            .declared_conflicts
            .entry(root_id)
            .or_default()
            .extend(declared);
        self.persist(journal).await?;
        Ok(())
    }

    async fn continue_conflicted_with_finalization(
        &self,
        operation_id: OperationId,
        caller_context_id: WorkspaceContextId,
        finalize: bool,
    ) -> MultiRootResult<Publication, S::Error, P::Error, A::Error> {
        let journal = self
            .store
            .load(operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
            .ok_or(MultiRootPublicationError::InvalidCandidate)?;
        validate_journal(&journal).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
        if !self
            .authorizer
            .authorize_transition(&journal.candidate.plan, caller_context_id)
            .await
            .map_err(MultiRootPublicationError::Authorizer)?
        {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        if journal.phase != MultiRootPublicationPhase::Conflicted
            || journal
                .projected_roots
                .keys()
                .ne(journal.candidate.plan.roots.keys())
        {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        for (root_id, conflict) in &journal.conflicts {
            let expected = conflict
                .conflicts
                .iter()
                .map(|view| view.key.clone())
                .collect::<BTreeSet<_>>();
            if !journal
                .declared_conflicts
                .get(root_id)
                .is_some_and(|declared| expected.is_subset(declared))
            {
                return Err(MultiRootPublicationError::InvalidCandidate);
            }
        }
        let roots = journal.candidate.plan.roots.clone();
        let mut applied = journal;
        for (root_id, root) in &roots {
            if applied.fences.contains_key(root_id) {
                continue;
            }
            let projected = *applied
                .projected_roots
                .get(root_id)
                .ok_or(MultiRootPublicationError::InvalidCandidate)?;
            let finished = self
                .publisher
                .finish_conflict(operation_id, *root_id, root, projected)
                .await
                .map_err(MultiRootPublicationError::Publisher)?;
            applied
                .projected_roots
                .insert(*root_id, finished.generation);
            applied
                .published_generations
                .insert(*root_id, finished.generation);
            applied.fences.insert(*root_id, finished.fence);
            applied = self.persist(applied).await?;
        }
        applied.phase = MultiRootPublicationPhase::Applied;
        applied.published_roots = applied.candidate.plan.roots.keys().copied().collect();
        applied.conflicts.clear();
        applied.projected_roots.clear();
        applied = self.persist(applied).await?;
        if finalize {
            self.finalize(&applied).await?;
        }
        Ok(Publication::Applied(applied))
    }

    /// Acknowledges a retained Applied journal after external compatibility state has been durably and idempotently projected.
    pub async fn acknowledge_applied(
        &self,
        journal: &MultiRootPublication,
    ) -> MultiRootResult<(), S::Error, P::Error, A::Error> {
        validate_journal(journal).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
        if journal.phase != MultiRootPublicationPhase::Applied {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        self.finalize(journal).await
    }

    /// Restores every conflicted root to its exact pre-merge generation.
    pub fn abort_conflicted(
        &self,
        operation_id: OperationId,
        caller_context_id: WorkspaceContextId,
    ) -> impl Future<Output = MultiRootResult<(), S::Error, P::Error, A::Error>> + ProviderTask + '_
    {
        Box::pin(async move {
            let mut journal = self
                .store
                .load(operation_id)
                .await
                .map_err(MultiRootPublicationError::Store)?
                .ok_or(MultiRootPublicationError::InvalidCandidate)?;
            validate_journal(&journal).map_err(|()| MultiRootPublicationError::InvalidCandidate)?;
            if !self
                .authorizer
                .authorize_transition(&journal.candidate.plan, caller_context_id)
                .await
                .map_err(MultiRootPublicationError::Authorizer)?
            {
                return Err(MultiRootPublicationError::InvalidCandidate);
            }
            if journal.phase == MultiRootPublicationPhase::Conflicted {
                for (root_id, fence) in &journal.fences {
                    self.publisher
                        .release(
                            conflict_finish_operation_id(operation_id, *root_id),
                            *root_id,
                            fence,
                        )
                        .await
                        .map_err(MultiRootPublicationError::Publisher)?;
                }
                journal.fences.clear();
                journal.declared_conflicts.clear();
                journal.phase = MultiRootPublicationPhase::Aborting;
                journal.published_roots.clear();
                journal = self.persist(journal).await?;
            }
            if journal.phase != MultiRootPublicationPhase::Aborting {
                return Err(MultiRootPublicationError::InvalidCandidate);
            }
            self.abort_journal(journal).await
        })
    }

    async fn project_conflicted(
        &self,
        mut journal: MultiRootPublication,
    ) -> Result<MultiRootPublication, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        let operation_id = journal.candidate.plan.operation_id;
        let roots = journal.candidate.plan.roots.clone();
        for (root_id, root) in roots {
            if journal.projected_roots.contains_key(&root_id) {
                continue;
            }
            let generation = self
                .publisher
                .project_conflict(
                    operation_id,
                    root_id,
                    &root,
                    journal.conflicts.get(&root_id),
                )
                .await
                .map_err(MultiRootPublicationError::Publisher)?;
            journal.projected_roots.insert(root_id, generation);
            journal = self.persist(journal).await?;
        }
        Ok(journal)
    }

    async fn abort_journal(
        &self,
        mut journal: MultiRootPublication,
    ) -> MultiRootResult<(), S::Error, P::Error, A::Error> {
        let operation_id = journal.candidate.plan.operation_id;
        let roots = journal.candidate.plan.roots.clone();
        for (root_id, root) in roots {
            if journal.published_roots.contains(&root_id) {
                continue;
            }
            if let Some(projected_generation) = journal.projected_roots.get(&root_id).copied() {
                self.publisher
                    .abort_conflict(operation_id, root_id, &root, projected_generation)
                    .await
                    .map_err(MultiRootPublicationError::Publisher)?;
            }
            journal.published_roots.insert(root_id);
            journal = self.persist(journal).await?;
        }
        if !self
            .store
            .release_parent(journal.candidate.plan.parent_context_id, operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            return Err(MultiRootPublicationError::Contended);
        }
        if !self
            .store
            .compare_and_delete(operation_id, journal.revision)
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            return Err(MultiRootPublicationError::Contended);
        }
        Ok(())
    }

    async fn discard_uncommitted(
        &self,
        journal: &MultiRootPublication,
    ) -> MultiRootResult<(), S::Error, P::Error, A::Error> {
        let operation_id = journal.candidate.plan.operation_id;
        if !self
            .store
            .release_parent(journal.candidate.plan.parent_context_id, operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
            || !self
                .store
                .compare_and_delete(operation_id, journal.revision)
                .await
                .map_err(MultiRootPublicationError::Store)?
        {
            return Err(MultiRootPublicationError::Contended);
        }
        Ok(())
    }

    async fn load_or_create(
        &self,
        candidate: MultiRootMergeCandidate,
    ) -> Result<MultiRootPublication, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        let operation_id = candidate.plan.operation_id;
        if let Some(existing) = self
            .store
            .load(operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            if existing.candidate != candidate {
                return Err(MultiRootPublicationError::InvalidCandidate);
            }
            if !self
                .store
                .claim_parent(candidate.plan.parent_context_id, operation_id)
                .await
                .map_err(MultiRootPublicationError::Store)?
            {
                if is_pristine_unclaimed(&existing)
                    && !self
                        .store
                        .compare_and_delete(operation_id, existing.revision)
                        .await
                        .map_err(MultiRootPublicationError::Store)?
                {
                    return Err(MultiRootPublicationError::Contended);
                }
                return Err(MultiRootPublicationError::ParentBusy);
            }
            return Ok(existing);
        }
        let journal = MultiRootPublication {
            version: MULTI_ROOT_VERSION,
            revision: 1,
            candidate,
            phase: MultiRootPublicationPhase::Validating,
            published_roots: BTreeSet::new(),
            published_generations: BTreeMap::new(),
            fences: BTreeMap::new(),
            conflicts: BTreeMap::new(),
            projected_roots: BTreeMap::new(),
            declared_conflicts: BTreeMap::new(),
            paused_root: None,
        };
        let created = match self
            .store
            .compare_and_swap(operation_id, 0, journal.clone())
            .await
        {
            Ok(created) => created,
            Err(error) => return Err(MultiRootPublicationError::Store(error)),
        };
        if created {
            let claimed = self
                .store
                .claim_parent(journal.candidate.plan.parent_context_id, operation_id)
                .await
                .map_err(MultiRootPublicationError::Store)?;
            if !claimed {
                if !self
                    .store
                    .compare_and_delete(operation_id, journal.revision)
                    .await
                    .map_err(MultiRootPublicationError::Store)?
                {
                    return Err(MultiRootPublicationError::Contended);
                }
                return Err(MultiRootPublicationError::ParentBusy);
            }
            return Ok(journal);
        }
        let existing = self
            .store
            .load(operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
            .ok_or(MultiRootPublicationError::Contended)?;
        if existing.candidate != journal.candidate {
            return Err(MultiRootPublicationError::InvalidCandidate);
        }
        if !self
            .store
            .claim_parent(existing.candidate.plan.parent_context_id, operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            if is_pristine_unclaimed(&existing)
                && !self
                    .store
                    .compare_and_delete(operation_id, existing.revision)
                    .await
                    .map_err(MultiRootPublicationError::Store)?
            {
                return Err(MultiRootPublicationError::Contended);
            }
            return Err(MultiRootPublicationError::ParentBusy);
        }
        Ok(existing)
    }

    async fn validate_and_commit(
        &self,
        mut journal: MultiRootPublication,
    ) -> Result<MultiRootValidation, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        let operation_id = journal.candidate.plan.operation_id;
        let roots = journal.candidate.plan.roots.clone();
        for (root_id, root) in roots {
            if journal.fences.contains_key(&root_id) {
                continue;
            }
            let resolutions = journal
                .candidate
                .resolutions
                .get(&root_id)
                .cloned()
                .ok_or(MultiRootPublicationError::InvalidCandidate)?;
            let prepared = self
                .publisher
                .prepare(operation_id, root_id, &root, &resolutions)
                .await;
            let fence = match prepared {
                Ok(MultiRootPrepare::Ready(fence)) => fence,
                Ok(MultiRootPrepare::Stale) => {
                    for (prepared_root, prepared_fence) in &journal.fences {
                        self.publisher
                            .release(operation_id, *prepared_root, prepared_fence)
                            .await
                            .map_err(MultiRootPublicationError::Publisher)?;
                    }
                    journal.fences.clear();
                    journal.conflicts.clear();
                    journal = self.persist(journal).await?;
                    return Ok(MultiRootValidation::Stale(root_id, journal));
                }
                Ok(MultiRootPrepare::Conflicted(plan)) => {
                    journal.conflicts.insert(root_id, *plan);
                    continue;
                }
                Err(error) => {
                    for (prepared_root, prepared_fence) in &journal.fences {
                        self.publisher
                            .release(operation_id, *prepared_root, prepared_fence)
                            .await
                            .map_err(MultiRootPublicationError::Publisher)?;
                    }
                    journal.fences.clear();
                    journal.conflicts.clear();
                    let _ = self.persist(journal).await?;
                    return Err(MultiRootPublicationError::Publisher(error));
                }
            };
            journal.fences.insert(root_id, fence);
            journal = self.persist(journal).await?;
        }
        if !journal.conflicts.is_empty() {
            for (prepared_root, prepared_fence) in &journal.fences {
                self.publisher
                    .release(operation_id, *prepared_root, prepared_fence)
                    .await
                    .map_err(MultiRootPublicationError::Publisher)?;
            }
            journal.fences.clear();
            journal.phase = MultiRootPublicationPhase::Conflicted;
            return self
                .persist(journal)
                .await
                .map(MultiRootValidation::Conflicted);
        }
        journal.phase = MultiRootPublicationPhase::Committed;
        journal.paused_root = None;
        self.persist(journal)
            .await
            .map(MultiRootValidation::Committed)
    }

    async fn roll_forward(
        &self,
        mut journal: MultiRootPublication,
        finalize: bool,
    ) -> MultiRootResult<Publication, S::Error, P::Error, A::Error> {
        let operation_id = journal.candidate.plan.operation_id;
        journal.phase = MultiRootPublicationPhase::Publishing;
        journal.paused_root = None;
        journal = self.persist(journal).await?;
        let roots = journal.candidate.plan.roots.clone();
        for (root_id, root) in roots {
            if journal.published_roots.contains(&root_id) {
                continue;
            }
            let resolutions = journal
                .candidate
                .resolutions
                .get(&root_id)
                .ok_or(MultiRootPublicationError::InvalidCandidate)?;
            let fence = journal
                .fences
                .get(&root_id)
                .ok_or(MultiRootPublicationError::InvalidCandidate)?;
            match self
                .publisher
                .publish(operation_id, root_id, &root, resolutions, fence)
                .await
                .map_err(MultiRootPublicationError::Publisher)?
            {
                MultiRootPublishRoot::Published(generation)
                | MultiRootPublishRoot::AlreadyPublished(generation) => {
                    journal.published_roots.insert(root_id);
                    journal.published_generations.insert(root_id, generation);
                    journal = self.persist(journal).await?;
                }
                MultiRootPublishRoot::UnexpectedTarget => {
                    journal.phase = MultiRootPublicationPhase::Paused;
                    journal.paused_root = Some(root_id);
                    journal = self.persist(journal).await?;
                    return Ok(Publication::Paused(journal));
                }
            }
        }
        journal.phase = MultiRootPublicationPhase::Applied;
        journal.paused_root = None;
        journal = self.persist(journal).await?;
        if finalize {
            self.finalize(&journal).await?;
        }
        Ok(Publication::Applied(journal))
    }

    async fn finalize(
        &self,
        journal: &MultiRootPublication,
    ) -> MultiRootResult<(), S::Error, P::Error, A::Error> {
        let operation_id = journal.candidate.plan.operation_id;
        if !journal.declared_conflicts.is_empty() {
            for (root_id, fence) in &journal.fences {
                self.publisher
                    .release(
                        conflict_finish_operation_id(operation_id, *root_id),
                        *root_id,
                        fence,
                    )
                    .await
                    .map_err(MultiRootPublicationError::Publisher)?;
            }
        }
        for (root_id, root) in &journal.candidate.plan.roots {
            self.publisher
                .finalize(operation_id, *root_id, root)
                .await
                .map_err(MultiRootPublicationError::Publisher)?;
        }
        if !self
            .store
            .release_parent(journal.candidate.plan.parent_context_id, operation_id)
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            return Err(MultiRootPublicationError::Contended);
        }
        if !self
            .store
            .compare_and_delete(operation_id, journal.revision)
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            return Err(MultiRootPublicationError::Contended);
        }
        Ok(())
    }

    async fn persist(
        &self,
        mut journal: MultiRootPublication,
    ) -> Result<MultiRootPublication, MultiRootPublicationError<S::Error, P::Error, A::Error>> {
        let expected = next_revision(&mut journal.revision);
        if self
            .store
            .compare_and_swap(
                journal.candidate.plan.operation_id,
                expected,
                journal.clone(),
            )
            .await
            .map_err(MultiRootPublicationError::Store)?
        {
            Ok(journal)
        } else {
            Err(MultiRootPublicationError::Contended)
        }
    }
}
