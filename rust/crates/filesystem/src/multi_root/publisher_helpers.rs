use super::*;

impl<A, O, R> WorkspaceMultiRootPublisher<A, O, R>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    R: WorkspaceResolver<A, O>,
{
    pub(super) async fn reserve_operation(
        &self,
        target: &Workspace<A, O>,
        operation_id: OperationId,
        conflict: &'static str,
    ) -> Result<PublicationReservation, WorkspaceMultiRootPublisherError<R::Error>> {
        let cancellation = CancellationToken::new();
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let authority_head = target
            .volume
            .fs
            .authority()
            .head(target.authority_id(), &mut authority_work)
            .await
            .map_err(WorkspaceError::engine)?;
        match target
            .volume
            .fs
            .authority()
            .reserve_publication(
                target.authority_id(),
                authority_head,
                operation_id,
                &mut authority_work,
            )
            .await
            .map_err(WorkspaceError::engine)?
        {
            ReservationOutcome::Reserved(reservation)
            | ReservationOutcome::AlreadyReserved(reservation) => Ok(reservation),
            ReservationOutcome::Conflict { .. } => Err(
                WorkspaceMultiRootPublisherError::Candidate(conflict.to_owned()),
            ),
        }
    }

    pub(super) async fn release_operation_reservation(
        &self,
        target: &Workspace<A, O>,
        operation_id: OperationId,
    ) -> Result<(), WorkspaceMultiRootPublisherError<R::Error>> {
        let cancellation = CancellationToken::new();
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let reservation = self
            .reserve_operation(
                target,
                operation_id,
                "publication reservation is owned by another operation",
            )
            .await?;
        target
            .volume
            .fs
            .authority()
            .release_publication(reservation, &mut authority_work)
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(())
    }

    pub(super) async fn pinned_plan(
        &self,
        root: &MultiRootMergeRoot,
        target_override: Option<Workspace<A, O>>,
    ) -> Result<crate::JoinPlan<A, O>, WorkspaceMultiRootPublisherError<R::Error>> {
        let source = self
            .resolver
            .resolve(root.merge_workspace_id.unwrap_or(root.source_workspace_id))
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let target = match target_override {
            Some(target) => target,
            None => self
                .resolver
                .resolve(root.target_workspace_id)
                .await
                .map_err(WorkspaceMultiRootPublisherError::Resolver)?,
        };
        let source_generation = source
            .generation(
                root.source_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let target_generation = target
            .generation(
                root.target_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let plan = source
            .join_into(&target)
            .plan_at(
                &source_generation,
                &target_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        if plan.common_ancestor() != root.base_generation {
            return Err(WorkspaceMultiRootPublisherError::Candidate(
                "common ancestor changed".to_owned(),
            ));
        }
        Ok(plan)
    }

    pub(super) async fn apply_plan(
        &self,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        plan: &crate::JoinPlan<A, O>,
        operation: IdempotencyKey,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
        permit: PublicationPermit,
    ) -> Result<JoinOutcome<A, O>, WorkspaceMultiRootPublisherError<R::Error>> {
        let options = ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key: operation,
        };
        let initial = plan.apply(options, BTreeMap::new(), permit).await?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = initial
        else {
            return Ok(initial);
        };
        let typed = plan
            .describe_conflicts(conflicts.to_vec(), truncated)
            .await?;
        let automatic = child_wins_resolutions(&typed, &root.child_wins_bindings);
        let candidate = if resolutions.is_empty() && !typed.conflicts.is_empty() {
            let remaining = crate::MergePlan {
                conflicts: typed
                    .conflicts
                    .iter()
                    .filter(|conflict| !automatic.contains_key(&conflict.key))
                    .cloned()
                    .collect(),
                ..typed.clone()
            };
            if remaining.conflicts.is_empty() {
                UnpublishedMergeCandidate {
                    plan: typed.clone(),
                    resolutions: automatic,
                }
            } else {
                let Some(registry) = self
                    .root_merge_drivers
                    .get(&root_id)
                    .or(self.merge_drivers.as_ref())
                else {
                    return Err(WorkspaceMultiRootPublisherError::Conflicted {
                        plan: Box::new(typed),
                    });
                };
                let mut cache = self.resolution_cache.lock().map_err(|_| {
                    WorkspaceMultiRootPublisherError::Candidate(
                        "merge resolution cache is unavailable".to_owned(),
                    )
                })?;
                match crate::resolve_merge_plan(remaining, registry, &mut *cache, false) {
                    Ok(mut candidate) => {
                        candidate.plan = typed.clone();
                        candidate.resolutions.extend(automatic);
                        candidate
                    }
                    Err(
                        MergePlanResolutionError::MissingDriver(_)
                        | MergePlanResolutionError::Driver(DriverError::Unsupported),
                    ) => {
                        return Err(WorkspaceMultiRootPublisherError::Conflicted {
                            plan: Box::new(typed),
                        });
                    }
                    Err(error) => {
                        return Err(WorkspaceMultiRootPublisherError::Candidate(
                            error.to_string(),
                        ));
                    }
                }
            }
        } else {
            UnpublishedMergeCandidate {
                plan: typed,
                resolutions: resolutions.clone().into_iter().chain(automatic).collect(),
            }
        };
        if resolutions.is_empty()
            && candidate.resolutions.values().any(|resolution| {
                matches!(resolution, MergeResolution::Text(text)
                    if crate::text_merge::has_conflict_markers(text))
            })
        {
            return Err(WorkspaceMultiRootPublisherError::Conflicted {
                plan: Box::new(candidate.plan.clone()),
            });
        }
        plan.apply_candidate(options, &candidate, permit)
            .await
            .map_err(|error| WorkspaceMultiRootPublisherError::Candidate(error.to_string()))
    }

    pub(super) async fn preview(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
    ) -> Result<bool, WorkspaceMultiRootPublisherError<R::Error>> {
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        if target
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value
            .id()
            != root.target_generation
        {
            return Ok(false);
        }
        // A prior preview may have been deleted after reporting conflicts.
        // Reusing its fork identity would reopen that tombstone on continuation.
        let preview_key = IdempotencyKey::new();
        let preview_name = preview_workspace_name(operation_id, root_id, preview_key)?;
        let target_generation = target
            .generation(
                root.target_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let preview = target
            .fork::<_, _, crate::WorkspaceError>(
                preview_name.as_str(),
                ForkOptions::from_generation(target_generation, preview_key),
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                &mut |_| {
                    std::future::ready(Ok(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    }))
                },
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let result = async {
            let preview_generation = preview
                .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map_err(|failure| failure.error)?
                .value;
            let preview_root = MultiRootMergeRoot {
                target_workspace_id: preview.id(),
                target_generation: preview_generation.id(),
                ..root.clone()
            };
            let plan = self
                .pinned_plan(&preview_root, Some(preview.clone()))
                .await?;
            Ok(matches!(
                self.apply_plan(
                    root_id,
                    root,
                    &plan,
                    publisher_key(operation_id, root_id, b"preview-apply"),
                    resolutions,
                    PublicationPermit::Unrestricted,
                )
                .await?,
                JoinOutcome::Applied(_)
                    | JoinOutcome::AlreadyApplied(_)
                    | JoinOutcome::NoChanges(_)
            ))
        }
        .await;
        let cleanup = preview
            .delete(publisher_key(operation_id, root_id, b"preview-delete"))
            .await?;
        if !matches!(
            cleanup,
            WorkspaceDelete::Deleted | WorkspaceDelete::AlreadyDeleted
        ) {
            return Err(WorkspaceMultiRootPublisherError::PreviewCleanup);
        }
        result
    }
}
