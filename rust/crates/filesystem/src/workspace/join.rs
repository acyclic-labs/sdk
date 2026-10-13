use super::*;

/// How a successful join records immutable ancestry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinHistory {
    /// Preserve source and target as two parents.
    Merge,
    /// Replay source state onto the target as one target-parent generation.
    Rebase,
    /// Collapse the complete source delta into one target-parent generation.
    Squash,
    /// Apply the selected source delta as one target-parent generation.
    CherryPick,
}

/// Side-effect-free join builder.
pub struct JoinBuilder<A, O> {
    pub(super) source: Workspace<A, O>,
    pub(super) target: Workspace<A, O>,
    pub(super) history: JoinHistory,
    pub(super) maximum_generations: u32,
    pub(super) maximum_changes: u32,
    pub(super) maximum_conflicts: u32,
}

impl<A, O> JoinBuilder<A, O> {
    pub(super) fn validate_workspaces(&self) -> Result<(), WorkspaceError> {
        if self.maximum_generations == 0 || self.maximum_changes == 0 || self.maximum_conflicts == 0
        {
            return Err(WorkspaceError::JoinLimit);
        }
        if !self
            .source
            .volume
            .fs
            .same_deployment(&self.target.volume.fs)
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        if self.source.volume.config != self.target.volume.config {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        self.source
            .volume
            .config
            .validate()
            .map_err(FsError::from)?;
        Ok(())
    }

    pub(super) fn validate_endpoints(
        &self,
        source: &Generation<A, O>,
        target: &Generation<A, O>,
    ) -> Result<(), WorkspaceError> {
        self.validate_workspaces()?;
        if source.workspace.id != self.source.id
            || target.workspace.id != self.target.id
            || !self
                .source
                .volume
                .fs
                .same_deployment(&source.workspace.volume.fs)
            || !self
                .target
                .volume
                .fs
                .same_deployment(&target.workspace.volume.fs)
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        if source.workspace.volume.config != self.source.volume.config
            || target.workspace.volume.config != self.target.volume.config
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        Ok(())
    }

    /// Selects the immutable ancestry shape of a successful join.
    pub const fn history(mut self, history: JoinHistory) -> Self {
        self.history = history;
        self
    }

    /// Sets exact bounded lineage, diff, and conflict frontiers.
    pub const fn bounds(
        mut self,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
    ) -> Self {
        self.maximum_generations = maximum_generations;
        self.maximum_changes = maximum_changes;
        self.maximum_conflicts = maximum_conflicts;
        self
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> JoinBuilder<A, O> {
    /// Authenticates current endpoints and discovers a bounded common ancestor.
    pub async fn plan<W: crate::facade::FsWork>(
        self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<JoinPlan<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            self.validate_workspaces()?;
            let source_id = self
                .source
                .checkout_accounted(
                    GenerationSelector::Head,
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?
                .generation_id();
            let (target_id, authority_head) = self
                .target
                .volume
                .fs
                .workspace_head_state(&self.target.volume, &mut accounting, cancellation)
                .await?;
            self.target
                .checkout_accounted(
                    GenerationSelector::Exact(target_id),
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let source = Generation {
                workspace: self.source.clone(),
                id: source_id,
                publication_closure: None,
            };
            let target = Generation {
                workspace: self.target.clone(),
                id: target_id,
                publication_closure: None,
            };
            self.plan_generations(
                source,
                target,
                authority_head,
                &mut accounting,
                cancellation,
            )
            .await
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Plans exact immutable endpoints under supplied work limits.
    pub async fn plan_at<W: crate::facade::FsWork>(
        self,
        source: &Generation<A, O>,
        target: &Generation<A, O>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<JoinPlan<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            self.validate_endpoints(source, target)?;
            let (target_head_id, target_authority_head) = self
                .target
                .volume
                .fs
                .workspace_head_state(&self.target.volume, &mut accounting, cancellation)
                .await?;
            if target.id != target_head_id {
                return Err(WorkspaceError::StaleTarget);
            }
            let copied = copy_work((2 * std::mem::size_of::<Generation<A, O>>()) as u64);
            accounting.admit(copied)?;
            let source = source.clone();
            let target = target.clone();
            accounting.record_completed(copied)?;
            let plan = self
                .plan_generations(
                    source,
                    target,
                    target_authority_head,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            Ok::<_, WorkspaceError>(plan)
        }
        .await;
        work.finish(result, &accounting)
    }

    pub(crate) async fn plan_generations(
        self,
        mut source_head: Generation<A, O>,
        mut target_head: Generation<A, O>,
        target_authority_head: crate::Head,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<JoinPlan<A, O>, WorkspaceError> {
        self.validate_endpoints(&source_head, &target_head)?;
        let base = self
            .source
            .volume
            .fs
            .workspace_common_ancestor_with_accounting(
                &source_head,
                &target_head,
                self.maximum_generations,
                accounting,
                cancellation,
            )
            .await?;
        let copied = copy_work(std::mem::size_of::<JoinPlan<A, O>>() as u64);
        accounting.admit(copied)?;
        source_head.workspace = self.source;
        target_head.workspace = self.target;
        let plan = JoinPlan {
            source_head,
            target_head,
            target_authority_head,
            base,
            history: self.history,
            maximum_generations: self.maximum_generations,
            maximum_changes: self.maximum_changes,
            maximum_conflicts: self.maximum_conflicts,
        };
        accounting.record_completed(copied)?;
        Ok(plan)
    }
}

/// Immutable side-effect-free join plan.
pub struct JoinPlan<A, O> {
    pub(super) source_head: Generation<A, O>,
    pub(super) target_head: Generation<A, O>,
    pub(super) target_authority_head: crate::Head,
    pub(crate) base: crate::facade::WorkspaceCommonAncestor,
    pub(super) history: JoinHistory,
    pub(super) maximum_generations: u32,
    pub(super) maximum_changes: u32,
    pub(super) maximum_conflicts: u32,
}

impl<A, O> JoinPlan<A, O> {
    /// Source generation captured while planning.
    pub const fn source_head(&self) -> GenerationId {
        self.source_head.id
    }

    /// Target generation observed while planning.
    pub const fn target_head(&self) -> GenerationId {
        self.target_head.id
    }

    /// Exact discovered common ancestor.
    pub const fn common_ancestor(&self) -> GenerationId {
        self.base.id
    }

    /// Source workspace retained by the plan.
    pub const fn source(&self) -> &Workspace<A, O> {
        &self.source_head.workspace
    }

    /// Captures exact join inputs under supplied work limits.
    pub fn commit_witness<W: crate::facade::FsWork>(
        &self,
        application: &JoinApplication<A, O>,
        idempotency_key: IdempotencyKey,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::JoinCommitWitness, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| -> Result<_, WorkspaceError> {
            let examined = item_work(1);
            accounting.admit(examined)?;
            let incompatible = application.generation.workspace.id()
                != self.target_head.workspace.id()
                || application.generation.id() == self.target_head.id();
            accounting.record_completed(examined)?;
            if incompatible {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            let copied = copy_work(std::mem::size_of::<crate::JoinCommitWitness>() as u64);
            accounting.admit(copied)?;
            let witness = crate::JoinCommitWitness {
                source_workspace: self.source_head.workspace.id().volume_id(),
                source_generation: self.source_head.id,
                target_workspace: self.target_head.workspace.id().volume_id(),
                expected_target: self.target_head.id,
                base_workspace: self.base.volume_id,
                base_generation: self.base.id,
                history: self.history,
                maximum_generations: self.maximum_generations,
                maximum_changes: self.maximum_changes,
                maximum_conflicts: self.maximum_conflicts,
                resolutions_digest: application.resolutions_digest,
                expected_head: self.target_authority_head,
                operation_id: idempotency_key.operation_id(),
                result_generation: application.generation.id,
            };
            accounting.record_completed(copied)?;
            Ok(witness)
        })();
        work.finish(result, &accounting)
    }
}
