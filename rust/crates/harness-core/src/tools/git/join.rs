//! Project text markers and retain other conflicting entries for manual edits.
use super::*;

type LocalJoinPlan = acyclic_fs::JoinPlan<LocalAuthorityBackend, LocalObjectBackend>;
type LocalJoinOutcome = JoinOutcome<LocalAuthorityBackend, LocalObjectBackend>;

impl<F, Fut> WorkspaceGitExecutor<'_, F>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), Error>> + Send,
{
    /// Each phase is built in a heap frame of its own, so the join's frame
    /// holds only pointers and its depth is that of its deepest phase.
    pub(super) async fn join(
        &self,
        operation: OperationId,
        target_tree: GitTreeRef,
        source: acyclic_fs::WorkspaceId,
        rebase: bool,
        tracked: &BTreeSet<String>,
    ) -> crate::Result<GitFilesystemResult> {
        let source = acyclic_fs::in_heap(|| self.workspace(source)).await?;
        let plan = acyclic_fs::in_heap(|| self.plan_join(&source, rebase)).await?;
        let plan = acyclic_fs::in_heap(|| self.without_reserved(plan, operation, rebase)).await?;
        let options = ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: IdempotencyKey::from_bytes(operation.into_bytes()),
        };
        let (plan, initial) =
            acyclic_fs::in_heap(|| self.apply_on_exact_base(plan, &source, options)).await?;
        let (outcome, unresolved) = match initial {
            JoinOutcome::Conflicted {
                conflicts,
                truncated,
            } => {
                // Erased: the conflict phases' Send proof starts here, not in
                // every caller's.
                let conflicted: acyclic_stream::BoxProviderFuture<'_, _> =
                    acyclic_fs::in_heap(|| {
                        self.join_conflicts(operation, &plan, options, conflicts, truncated)
                    });
                conflicted.await?
            }
            initial => (initial, false),
        };
        acyclic_fs::in_heap(|| self.joined(operation, target_tree, outcome, unresolved, tracked))
            .await
    }

    /// Resolves a conflicted join's entries and applies the resolved
    /// candidate; the flag reports entries left for manual edits.
    async fn join_conflicts(
        &self,
        operation: OperationId,
        plan: &LocalJoinPlan,
        options: ApplyOptions,
        conflicts: Vec<acyclic_fs::MergeConflict>,
        truncated: bool,
    ) -> crate::Result<(LocalJoinOutcome, bool)> {
        let typed = acyclic_fs::in_heap(|| plan.describe_conflicts(conflicts, truncated)).await?;
        if typed.truncated {
            return Err(Self::error("Git join conflict frontier is incomplete"));
        }
        let paths = conflict_paths(&typed)?;
        let (candidate, unresolved) = acyclic_fs::in_heap(|| self.merge_candidate(typed)).await?;
        let validate = || self.validate();
        let resolutions = acyclic_fs::in_heap(|| {
            super::materialize::prepared(
                async {
                    plan.prepare_candidate(&candidate)
                        .await
                        .map_err(Error::from)
                },
                &validate,
            )
        })
        .await?;
        let outcome = acyclic_fs::in_heap(|| plan.apply(options, resolutions, self.permit)).await?;
        if matches!(
            &outcome,
            JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_) | JoinOutcome::NoChanges(_)
        ) {
            // Only an applied projection can authorize continuation.
            // An interruption before this durable write leaves the
            // existing transition uninspected and continuation refuses.
            let repository = GitCompatRepository::new(self.repository_id, self.store.clone());
            acyclic_fs::in_heap(|| repository.record_merge_conflicts(operation, &paths))
                .await
                .map_err(super::errors::compat_error)?;
        }
        Ok((outcome, unresolved))
    }

    /// The command result of a finished join.
    async fn joined(
        &self,
        operation: OperationId,
        target_tree: GitTreeRef,
        outcome: LocalJoinOutcome,
        unresolved: bool,
        tracked: &BTreeSet<String>,
    ) -> crate::Result<GitFilesystemResult> {
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
        let tracked_paths = acyclic_fs::in_heap(|| {
            self.joined_tracked_paths(operation, target_tree, &generation, tracked)
        })
        .await?;
        Ok(GitFilesystemResult::Applied {
            tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
            tracked_paths: Some(tracked_paths),
        })
    }

    /// Resolves every described conflict with the configured drivers, the
    /// way Git would leave each one in the worktree. Returns the candidate and
    /// whether any conflict still needs the user.
    async fn merge_candidate(
        &self,
        typed: acyclic_fs::MergePlan,
    ) -> crate::Result<(acyclic_fs::UnpublishedMergeCandidate, bool)> {
        let mut unresolved = false;
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
                            if !matches!(resolution, acyclic_fs::MergeResolution::Unresolved) =>
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
                                } else {
                                    acyclic_fs::ConflictSide::Ours
                                },
                            )
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
        Ok((candidate, unresolved))
    }

    /// `plan` re-planned from a fork without any reserved host path that
    /// either side holds.
    async fn without_reserved(
        &self,
        plan: LocalJoinPlan,
        operation: OperationId,
        rebase: bool,
    ) -> crate::Result<LocalJoinPlan> {
        // A mounted child projection authors its host-owned volume aliases.
        // Join from a fork that holds the target's own entries there, so a
        // merge never publishes them into the target's tree.
        let reserved = acyclic_fs::in_heap(|| self.reserved_in(&plan)).await?;
        if reserved.is_empty() {
            return Ok(plan);
        }
        let source =
            acyclic_fs::in_heap(|| self.join_generation(plan.source(), plan.source_head())).await?;
        let target =
            acyclic_fs::in_heap(|| self.join_generation(&self.current, plan.target_head())).await?;
        let filtered = acyclic_fs::in_heap(|| {
            acyclic_fs::git_generation_with_paths_from(
                &source,
                &target,
                &reserved,
                reserved_source_operation(operation),
            )
        })
        .await
        .map_err(Error::from)?;
        let filtered = filtered.workspace();
        let filtered = acyclic_fs::in_heap(|| self.plan_join(&filtered, rebase)).await?;
        if filtered.target_head() != plan.target_head() {
            return Err(Self::error("Git join raced with another workspace writer"));
        }
        Ok(filtered)
    }

    /// Applies `plan`, re-applying once against the exact fork cut when the
    /// lazy cut misread both sides' materializations as additions. Returns
    /// the plan that applied with its outcome.
    async fn apply_on_exact_base(
        &self,
        plan: LocalJoinPlan,
        source: &LocalWorkspace,
        options: ApplyOptions,
    ) -> crate::Result<(LocalJoinPlan, LocalJoinOutcome)> {
        let initial =
            acyclic_fs::in_heap(|| plan.apply(options, BTreeMap::new(), self.permit)).await?;
        Ok(match initial {
            JoinOutcome::Conflicted {
                conflicts,
                truncated,
            } => match acyclic_fs::in_heap(|| {
                self.exact_base(&plan, source, conflicts.clone(), truncated)
            })
            .await?
            {
                Some(base) => {
                    let plan = acyclic_fs::in_heap(|| plan.refine_base(&base))
                        .await
                        .map_err(Error::from)?;
                    let initial =
                        acyclic_fs::in_heap(|| plan.apply(options, BTreeMap::new(), self.permit))
                            .await?;
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
        })
    }

    /// The paths the joined history tracks after `operation` published
    /// `generation` onto the target `target_tree` named.
    async fn joined_tracked_paths(
        &self,
        operation: OperationId,
        target_tree: GitTreeRef,
        generation: &LocalGeneration,
        tracked: &BTreeSet<String>,
    ) -> crate::Result<BTreeSet<String>> {
        // Tracking is a function of the recorded transition, not of this
        // attempt: the target as the transition found it and the generation
        // this operation published under its key. A resumed transition whose
        // join already published re-plans against the advanced target and
        // may see no changes, but it still tracks what the join published.
        let before = acyclic_fs::in_heap(|| self.generation(target_tree)).await?;
        let cancellation = CancellationToken::new();
        let joined = acyclic_fs::in_heap(|| {
            self.current.operation_generation(
                IdempotencyKey::from_bytes(operation.into_bytes()),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
        })
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| Error::from(failure.error))?;
        // The joined source's authored files enter history like any capture:
        // newly published eligible paths become tracked, not only the union of
        // the two branches' previously committed paths.
        acyclic_fs::in_heap(|| {
            acyclic_fs::git_tracked_paths_after(
                &before,
                joined.as_ref().unwrap_or(generation),
                generation,
                &self.repository,
                tracked,
                u32::MAX,
                &cancellation,
            )
        })
        .await
        .map_err(Error::from)
    }

    async fn plan_join(
        &self,
        source: &LocalWorkspace,
        rebase: bool,
    ) -> crate::Result<LocalJoinPlan> {
        let mut builder = source.join_into(&self.current).git_metadata();
        if rebase {
            builder = builder.history(acyclic_fs::JoinHistory::Rebase);
        }
        let validate = || self.validate();
        acyclic_fs::in_heap(|| {
            super::materialize::prepared(
                async {
                    builder
                        .plan(WorkBudget::UNBOUNDED, &CancellationToken::new())
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| Error::from(failure.error))
                },
                &validate,
            )
        })
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
        plan: &LocalJoinPlan,
        source: &LocalWorkspace,
        conflicts: Vec<acyclic_fs::MergeConflict>,
        truncated: bool,
    ) -> crate::Result<Option<LocalGeneration>> {
        let View::Checkout(current) = self.lazy_current else {
            return Ok(None);
        };
        // Each step below resolves paths through the kernel; each is built
        // in a heap frame of its own so this frame holds only pointers.
        let described =
            acyclic_fs::in_heap(|| plan.describe_conflicts(conflicts, truncated)).await?;
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
            let lazy = match acyclic_fs::in_heap(|| {
                current.open_related(workspace.clone(), &mut accounting)
            })
            .await
            {
                Ok(lazy) => lazy,
                // A workspace without a lazy binding has no unresolved source.
                Err(acyclic_fs::LazyWorkspaceError::NotFound) => continue,
                Err(error) => return Err(Error::from(error)),
            };
            let Some(origin) = acyclic_fs::in_heap(|| lazy.state(&mut accounting))
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
            let owner = acyclic_fs::in_heap(|| self.workspace(cut.workspace_id)).await?;
            let owner = acyclic_fs::in_heap(|| current.open_related(owner, &mut accounting))
                .await
                .map_err(Error::from)?;
            // The forked side's own observations pin the base; without them
            // the live source would stand in for content it no longer has.
            return acyclic_fs::in_heap(|| owner.exactify_cut(cut, &paths, &lazy, &mut accounting))
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
        let cancellation = CancellationToken::new();
        acyclic_fs::in_heap(|| {
            workspace.generation(generation, WorkBudget::UNBOUNDED, &cancellation)
        })
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| Error::from(failure.error))
    }

    /// Reserved host paths present in either side of a planned join.
    async fn reserved_in(&self, plan: &LocalJoinPlan) -> crate::Result<Vec<String>> {
        let sides = [
            acyclic_fs::in_heap(|| self.join_generation(plan.source(), plan.source_head())).await?,
            acyclic_fs::in_heap(|| self.join_generation(&self.current, plan.target_head())).await?,
        ];
        let cancellation = CancellationToken::new();
        let mut present = Vec::new();
        for path in self.repository.reserved_paths() {
            let path_name = format!("/{path}");
            for side in &sides {
                // Each probe is a full path resolution; its frame lives on the
                // heap so the join's stack holds only the pointer.
                match acyclic_fs::in_heap(|| {
                    side.stat(&path_name, WorkBudget::UNBOUNDED, &cancellation)
                })
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

/// The path and identity of every described conflict, which a continued
/// merge checks its resolution against.
fn conflict_paths(
    typed: &acyclic_fs::MergePlan,
) -> crate::Result<BTreeMap<String, Option<acyclic_fs::FileId>>> {
    typed
        .conflicts
        .iter()
        .map(|conflict| {
            let path = conflict.path.clone().ok_or_else(|| {
                Error::Conflict("Git join conflict has no resolvable path".into())
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
        .collect()
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
