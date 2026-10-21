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
        target_tree: GitTreeRef,
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
        let (plan, initial) = match initial {
            JoinOutcome::Conflicted {
                conflicts,
                truncated,
            } => match self
                .exact_base(&plan, &source, conflicts.clone(), truncated)
                .await?
            {
                Some(base) => {
                    let plan = plan.refine_base(&base).await.map_err(Error::from)?;
                    let initial = plan.apply(options, BTreeMap::new(), self.permit).await?;
                    (plan, initial)
                }
                None => (
                    plan,
                    JoinOutcome::Conflicted {
                        conflicts,
                        truncated,
                    },
                ),
            },
            initial => (plan, initial),
        };
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
        // Tracking is a function of the recorded transition, not of this
        // attempt: the target as the transition found it and the generation
        // this operation published under its key. A resumed transition whose
        // join already published re-plans against the advanced target and
        // may see no changes, but it still tracks what the join published.
        let before = self.generation(target_tree).await?;
        let joined = self
            .current
            .operation_generation(
                IdempotencyKey::from_bytes(operation.into_bytes()),
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
            joined.as_ref().unwrap_or(&generation),
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

    /// The fork cut materialized at the paths whose identity both sides
    /// added, when the plan's common ancestor is a lazy fork's cut.
    ///
    /// A lazy view's authored generation leaves unresolved source files out,
    /// and each view that materializes one binds the same source-derived
    /// identity. When both sides of a fork materialize a file the cut never
    /// bound, the cut's authored generation reads them as two additions of one
    /// identity, so an edit on one side conflicts with the unchanged copy on
    /// the other. Their true base is the content the forked side observed,
    /// readable only while the source still holds that version; once the
    /// host changed, nothing pins it and the conflict stands. Only those
    /// identities are materialized: a file one side never materialized is
    /// unresolved there, not deleted, and must keep its absent base. The cut
    /// of either
    /// side's fork counts, so a parent merging its child and a child merging
    /// its parent agree.
    async fn exact_base(
        &self,
        plan: &acyclic_fs::JoinPlan<LocalAuthorityBackend, LocalObjectBackend>,
        source: &LocalWorkspace,
        conflicts: Vec<acyclic_fs::MergeConflict>,
        truncated: bool,
    ) -> crate::Result<Option<LocalGeneration>> {
        let View::Checkout(current) = self.lazy_current else {
            return Ok(None);
        };
        let described = plan.describe_conflicts(conflicts, truncated).await?;
        let mut paths = described
            .conflicts
            .iter()
            .filter(|conflict| {
                matches!(conflict.key, acyclic_fs::ConflictKey::File(_))
                    && matches!(conflict.base, acyclic_fs::ConflictValue::Absent)
                    && !matches!(conflict.ours, acyclic_fs::ConflictValue::Absent)
                    && !matches!(conflict.theirs, acyclic_fs::ConflictValue::Absent)
            })
            .filter_map(|conflict| conflict.path.clone())
            .collect::<Vec<_>>();
        if paths.is_empty() {
            return Ok(None);
        }
        paths.sort_unstable();
        let cancellation = CancellationToken::new();
        for workspace in [source, &self.current] {
            let mut accounting = WorkBudget::UNBOUNDED.accounting(&cancellation);
            let lazy = match current
                .open_related(workspace.clone(), &mut accounting)
                .await
            {
                Ok(lazy) => lazy,
                // A workspace without a lazy binding has no unresolved source.
                Err(acyclic_fs::LazyWorkspaceError::NotFound) => continue,
                Err(error) => return Err(Error::from(error)),
            };
            let Some(origin) = lazy
                .state(&mut accounting)
                .await
                .map_err(Error::from)?
                .fork_origin
            else {
                continue;
            };
            let cut = origin.snapshot;
            if cut.authored_generation != plan.common_ancestor() {
                continue;
            }
            let owner = current
                .open_related(self.workspace(cut.workspace_id).await?, &mut accounting)
                .await
                .map_err(Error::from)?;
            // The forked side's own observations pin the base; without them
            // the live source would stand in for content it no longer has.
            return owner
                .exactify_cut(cut, &paths, &lazy, &mut accounting)
                .await
                .map_err(Error::from);
        }
        Ok(None)
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
