use super::*;

/// Failure while resolving or validating a declarative real-workspace join.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum DrivenJoinError {
    /// Workspace planning, storage, or publication failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Driver selection, execution, or cache reuse failed.
    #[error(transparent)]
    Resolution(#[from] MergePlanResolutionError),
    /// Candidate generations or conflict identities do not match this join plan.
    #[error("merge candidate does not match the immutable workspace join plan")]
    StaleCandidate,
    /// The current kernel cannot safely materialize this synthesized resolution.
    #[error("merge resolution for {0:?} is not representable by this join")]
    UnsupportedResolution(ConflictKey),
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> JoinPlan<A, O> {
    /// Runs merge drivers and publishes only while the supplied writer permit remains valid.
    pub async fn apply_with_drivers<C: MergeResolutionCache>(
        &self,
        options: ApplyOptions,
        registry: &MergeDriverRegistry,
        cache: &mut C,
        replanning: bool,
        permit: crate::PublicationPermit,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        let initial = self.apply(options, BTreeMap::new(), permit).await?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = initial
        else {
            return Ok(initial);
        };
        let plan = self.describe_conflicts(conflicts, truncated).await?;
        let candidate = resolve_merge_plan(plan, registry, cache, replanning)?;
        self.apply_candidate(options, &candidate, permit).await
    }

    /// Applies caller-selected immutable sides after validating that they
    pub async fn apply_sides(
        &self,
        options: ApplyOptions,
        selections: BTreeMap<MergeConflict, ConflictSide>,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        let initial = self
            .apply(
                options,
                BTreeMap::new(),
                crate::PublicationPermit::Unrestricted,
            )
            .await?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = initial
        else {
            return Ok(initial);
        };
        if truncated
            || conflicts.len() != selections.len()
            || conflicts
                .iter()
                .any(|conflict| !selections.contains_key(conflict))
        {
            return Err(DrivenJoinError::StaleCandidate);
        }
        let resolutions = selections
            .into_iter()
            .map(|(conflict, side)| (conflict, side_resolution(side)))
            .collect();
        self.apply(options, resolutions, crate::PublicationPermit::Unrestricted)
            .await
            .map_err(Into::into)
    }

    /// Validates and publishes a declarative candidate under one writer permit.
    pub async fn apply_candidate(
        &self,
        options: ApplyOptions,
        candidate: &UnpublishedMergeCandidate,
        permit: crate::PublicationPermit,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        let conflict_keys = candidate
            .plan
            .conflicts
            .iter()
            .map(|conflict| &conflict.key)
            .collect::<BTreeSet<_>>();
        if candidate.plan.base != self.base.id
            || candidate.plan.ours != self.target_head.id
            || candidate.plan.theirs != self.source_head.id
            || candidate.plan.truncated
            || candidate.plan.conflicts.len() != candidate.resolutions.len()
            || conflict_keys.len() != candidate.plan.conflicts.len()
            || !candidate.resolutions.keys().eq(conflict_keys)
        {
            return Err(DrivenJoinError::StaleCandidate);
        }
        let mut resolutions = BTreeMap::new();
        for conflict in &candidate.plan.conflicts {
            let resolution = candidate
                .resolutions
                .get(&conflict.key)
                .ok_or(DrivenJoinError::StaleCandidate)?;
            if !resolution_matches_conflict(&conflict.key, resolution) {
                return Err(DrivenJoinError::UnsupportedResolution(conflict.key.clone()));
            }
            let resolution = match resolution {
                MergeResolution::Select(side) => side_resolution(*side),
                MergeResolution::Text(text) => crate::kernel::MergeConflictResolution::File(Some(
                    self.stage_regular_resolution(
                        &conflict.key,
                        Bytes::copy_from_slice(text.as_bytes()),
                    )
                    .await?,
                )),
                MergeResolution::Binary(bytes) => {
                    crate::kernel::MergeConflictResolution::File(Some(
                        self.stage_regular_resolution(&conflict.key, Bytes::copy_from_slice(bytes))
                            .await?,
                    ))
                }
                MergeResolution::Metadata(bytes) => {
                    let file_id = conflict_file_id(&conflict.key).ok_or_else(|| {
                        DrivenJoinError::UnsupportedResolution(conflict.key.clone())
                    })?;
                    let mut record = self
                        .target_head
                        .workspace
                        .volume
                        .fs
                        .workspace_conflict_record(
                            &self.target_head.workspace.volume,
                            self.target_head.id,
                            file_id,
                        )
                        .await?
                        .ok_or(WorkspaceError::InvalidMergeResolution)?;
                    record.metadata = self
                        .target_head
                        .workspace
                        .volume
                        .fs
                        .stage_merge_metadata(&self.target_head.workspace.volume, bytes)
                        .await?;
                    crate::kernel::MergeConflictResolution::File(Some(record))
                }
                MergeResolution::Binding(file_id) => {
                    crate::kernel::MergeConflictResolution::Binding(*file_id)
                }
                MergeResolution::Unresolved => {
                    return Err(DrivenJoinError::UnsupportedResolution(conflict.key.clone()));
                }
            };
            resolutions.insert(
                kernel_conflict(
                    &conflict.key,
                    self.target_head
                        .workspace
                        .volume
                        .config
                        .limits
                        .maximum_component_bytes,
                )?,
                resolution,
            );
        }
        self.apply(options, resolutions, permit)
            .await
            .map_err(Into::into)
    }
}

impl From<FsError> for DrivenJoinError {
    fn from(error: FsError) -> Self {
        Self::Workspace(WorkspaceError::from(error))
    }
}
