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
        let mut builder = source.join_into(&self.current).git_metadata();
        if rebase {
            builder = builder.history(acyclic_fs::JoinHistory::Rebase);
        }
        let plan = super::materialize::prepared(
            async {
                builder
                    .plan(WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| Error::from(failure.error))
            },
            &|| self.validate(),
        )
        .await?;
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
        let tracked_paths = existing_tracked_paths(&generation, tracked).await?;
        Ok(GitFilesystemResult::Applied {
            tree: Some(GitTreeRef::exact(self.current.id(), generation.id())),
            tracked_paths: Some(tracked_paths),
        })
    }
}
