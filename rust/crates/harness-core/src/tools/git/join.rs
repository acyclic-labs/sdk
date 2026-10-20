//! Project text markers and retain other conflicting entries for manual edits.
use super::*;

impl<F, Fut> WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    pub(super) async fn join(
        &self,
        operation: OperationId,
        source: acyclic_fs::WorkspaceId,
        rebase: bool,
        tracked: &BTreeSet<String>,
    ) -> crate::Result<GitFilesystemResult> {
        let source = self.workspace(source).await?;
        let plan = self.plan_join(&source, rebase).await?;
        // A mounted child projection authors its host-owned volume aliases.
        // Join from a fork that holds the target's own entries there, so a
        // merge never publishes them into the target's tree.
        let reserved = self.reserved_in(&plan).await?;
        let plan = if reserved.is_empty() {
            plan
        } else {
            let filtered = acyclic_fs::git_generation_with_paths_from(
                &self
                    .join_generation(plan.source(), plan.source_head())
                    .await?,
                &self
                    .join_generation(&self.current, plan.target_head())
                    .await?,
                &reserved,
                reserved_source_operation(operation),
            )
            .await
            .map_err(Error::from)?;
            let filtered = self.plan_join(&filtered.workspace(), rebase).await?;
            if filtered.target_head() != plan.target_head() {
                return Err(Self::error("Git join raced with another workspace writer"));
            }
            filtered
        };
        let options = ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::from_bytes(operation.into_bytes()),
        };
        let initial = plan.apply(options, BTreeMap::new(), self.permit).await?;
        let mut unresolved = false;
        let outcome =
            if let JoinOutcome::Conflicted {
                conflicts,
                truncated,
            } = initial
            {
                let typed = plan.describe_conflicts(conflicts, truncated).await?;
                if typed.truncated {
                    return Err(Self::error("Git join conflict frontier is incomplete"));
                }
                let paths = typed
                    .conflicts
                    .iter()
                    .map(|conflict| {
                        let path = conflict.path.clone().ok_or_else(|| {
                            Self::error("Git join conflict has no resolvable path")
                        })?;
                        use acyclic_fs::ConflictKey;
                        let identity = match &conflict.key {
                            ConflictKey::File(id)
                            | ConflictKey::Metadata(id)
                            | ConflictKey::HardLinks(id)
                            | ConflictKey::Directory(id)
                            | ConflictKey::SpecialPayload(id)
                            | ConflictKey::ContentRange { file_id: id, .. }
                            | ConflictKey::Rename { file_id: id, .. } => Some(*id),
                            ConflictKey::Binding { .. } => None,
                        };
                        Ok((path, identity))
                    })
                    .collect::<crate::Result<BTreeMap<_, _>>>()?;
                let candidate = super::materialize::prepared(
                async {
                    let mut cache = self.merge_cache.lock().await;
                    let mut resolutions = BTreeMap::new();
                    for conflict in &typed.conflicts {
                        let resolution = match acyclic_fs::resolve_merge_conflict(
                            &typed,
                            conflict,
                            &self.merge_drivers,
                            &mut *cache,
                            false,
                        ) {
                            Ok(resolution)
                                if !matches!(
                                    resolution,
                                    acyclic_fs::MergeResolution::Unresolved
                                ) =>
                            {
                                resolution
                            }
                            Ok(_)
                            | Err(
                                acyclic_fs::MergePlanResolutionError::MissingDriver(_)
                                | acyclic_fs::MergePlanResolutionError::Driver(
                                    acyclic_fs::DriverError::Unsupported,
                                ),
                            ) => {
                                unresolved = true;
                                acyclic_fs::MergeResolution::Select(
                                    if matches!(conflict.ours, acyclic_fs::ConflictValue::Absent) {
                                        acyclic_fs::ConflictSide::Theirs
                                    } else { acyclic_fs::ConflictSide::Ours })
                            }
                            Err(error) => {
                                return Err(Error::from(acyclic_fs::DrivenJoinError::Resolution(
                                    error,
                                )));
                            }
                        };
                        unresolved |= matches!(&resolution, acyclic_fs::MergeResolution::Text(text)
                        if acyclic_fs::text_merge::has_conflict_markers(text));
                        resolutions.insert(conflict.key.clone(), resolution);
                    }
                    Ok(acyclic_fs::UnpublishedMergeCandidate {
                        plan: typed,
                        resolutions,
                    })
                },
                &|| self.validate(),
            )
            .await?;
                let resolutions = super::materialize::prepared(
                    async {
                        plan.prepare_candidate(&candidate)
                            .await
                            .map_err(Error::from)
                    },
                    &|| self.validate(),
                )
                .await?;
                let outcome = plan.apply(options, resolutions, self.permit).await?;
                if matches!(
                    &outcome,
                    JoinOutcome::Applied(_)
                        | JoinOutcome::AlreadyApplied(_)
                        | JoinOutcome::NoChanges(_)
                ) {
                    // Only an applied projection can authorize continuation.
                    // An interruption before this durable write leaves the
                    // existing transition uninspected and continuation refuses.
                    GitCompatRepository::new(self.repository_id, self.store.clone())
                        .record_merge_conflicts(operation, &paths)
                        .await
                        .map_err(super::errors::compat_error)?;
                }
                outcome
            } else {
                initial
            };
        let generation = match outcome {
            JoinOutcome::Applied(application) | JoinOutcome::AlreadyApplied(application) => {
                application.generation
            }
            JoinOutcome::NoChanges(generation) => generation,
            JoinOutcome::StaleTarget(_) => {
                return Err(Self::error("Git join raced with another workspace writer"));
            }
            JoinOutcome::Conflicted { .. } => {
                return Err(Self::error("Git join has unresolved entries"));
            }
            JoinOutcome::Fenced => return Err(Self::error("Git join was fenced")),
            JoinOutcome::IdempotencyConflict => {
                return Err(Self::error("Git join retry identity was reused"));
            }
        };
        if unresolved {
            return Err(Self::error(
                "Git join has unresolved conflicts. Edit the conflicted paths, then run `fs merge --continue`, or `fs merge --abort`",
            ));
        }
        let before = self
            .current
            .generation(
                plan.target_head(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| Error::from(failure.error))?;
        // The joined source's authored files enter history like any capture:
        // newly published eligible paths become tracked, not only the union of
        // the two branches' previously committed paths.
        let tracked_paths = acyclic_fs::git_tracked_paths_after(
            &before,
            &generation,
            &generation,
            &self.repository,
            tracked,
            u32::MAX,
            &CancellationToken::new(),
        )
        .await
        .map_err(Error::from)?;
        Ok(GitFilesystemResult::Applied {
            tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
            tracked_paths: Some(tracked_paths),
        })
    }

    async fn plan_join(
        &self,
        source: &LocalWorkspace,
        rebase: bool,
    ) -> crate::Result<acyclic_fs::JoinPlan<LocalAuthorityBackend, LocalObjectBackend>> {
        let mut builder = source.join_into(&self.current).git_metadata();
        if rebase {
            builder = builder.history(acyclic_fs::JoinHistory::Rebase);
        }
        super::materialize::prepared(
            async {
                builder
                    .plan(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| Error::from(failure.error))
            },
            &|| self.validate(),
        )
        .await
    }

    async fn join_generation(
        &self,
        workspace: &LocalWorkspace,
        generation: acyclic_fs::GenerationId,
    ) -> crate::Result<LocalGeneration> {
        workspace
            .generation(generation, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| Error::from(failure.error))
    }

    /// Reserved host paths present in either side of a planned join.
    async fn reserved_in(
        &self,
        plan: &acyclic_fs::JoinPlan<LocalAuthorityBackend, LocalObjectBackend>,
    ) -> crate::Result<Vec<String>> {
        let sides = [
            self.join_generation(plan.source(), plan.source_head())
                .await?,
            self.join_generation(&self.current, plan.target_head())
                .await?,
        ];
        let mut present = Vec::new();
        for path in self.repository.reserved_paths() {
            for side in &sides {
                match side
                    .stat(
                        &format!("/{path}"),
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                {
                    Ok(_) => {
                        present.push(path.to_owned());
                        break;
                    }
                    Err(failure)
                        if matches!(failure.error, acyclic_fs::WorkspaceError::NotFound) => {}
                    Err(failure) => return Err(Error::from(failure.error)),
                }
            }
        }
        Ok(present)
    }
}

/// The fork that drops reserved source entries has its own retry identity.
fn reserved_source_operation(operation: OperationId) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"acyclic.git.join-reserved-source/v1\0");
    hash.update(&operation.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}
