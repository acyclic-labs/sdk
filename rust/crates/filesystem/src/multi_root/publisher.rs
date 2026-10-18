use super::*;

impl<A, O, R> MultiRootPublisher for WorkspaceMultiRootPublisher<A, O, R>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    R: WorkspaceResolver<A, O>,
{
    type Error = WorkspaceMultiRootPublisherError<R::Error>;

    async fn prepare(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
    ) -> Result<MultiRootPrepare, Self::Error> {
        let cancellation = CancellationToken::new();
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        match self.preview(operation_id, root_id, root, resolutions).await {
            Ok(true) => {}
            Ok(false) => return Ok(MultiRootPrepare::Stale),
            Err(WorkspaceMultiRootPublisherError::Conflicted { plan }) => {
                return Ok(MultiRootPrepare::Conflicted(plan));
            }
            Err(error) => return Err(error),
        }
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let authority_head = target
            .volume
            .fs
            .authority()
            .head(target.authority_id(), &mut authority_work)
            .await
            .map_err(WorkspaceError::engine)?;
        let reservation = match target
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
            | ReservationOutcome::AlreadyReserved(reservation) => reservation,
            ReservationOutcome::Conflict { .. } => return Ok(MultiRootPrepare::Stale),
        };
        Ok(MultiRootPrepare::Ready(MultiRootFence {
            token: serde_json::to_vec(&WorkspacePublicationFence {
                target_workspace_id: target.id(),
                reservation,
            })
            .map_err(|error| WorkspaceMultiRootPublisherError::Candidate(error.to_string()))?,
        }))
    }

    async fn release(
        &self,
        operation_id: OperationId,
        _root_id: WorkspaceRootId,
        fence: &MultiRootFence,
    ) -> Result<(), Self::Error> {
        let cancellation = CancellationToken::new();
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let decoded = decode_reservation(operation_id, fence)?;
        let target = self
            .resolver
            .resolve(decoded.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        target
            .volume
            .fs
            .authority()
            .release_publication(decoded.reservation, &mut authority_work)
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(())
    }

    async fn project_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        conflict: Option<&crate::MergePlan>,
    ) -> Result<GenerationId, Self::Error> {
        let cancellation = CancellationToken::new();
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let projection_key = publisher_key(operation_id, root_id, b"conflict-project");
        let reservation_operation = conflict_projection_operation_id(operation_id, root_id);
        if let Some(generation) = target
            .operation_generation(
                projection_key,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value
        {
            self.release_operation_reservation(&target, reservation_operation)
                .await?;
            return Ok(generation.id());
        }
        if target
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value
            .id()
            != root.target_generation
        {
            return Err(WorkspaceMultiRootPublisherError::Candidate(
                "conflict projection target changed".to_owned(),
            ));
        }
        let plan = self.pinned_plan(root, Some(target.clone())).await?;
        let mut resolutions = BTreeMap::new();
        if let Some(conflict) = conflict {
            if conflict.truncated {
                return Err(WorkspaceMultiRootPublisherError::Candidate(
                    "conflict projection plan changed".to_owned(),
                ));
            }
            for view in &conflict.conflicts {
                let resolution = if view.kind == crate::ConflictKind::Text {
                    DefaultTextMergeDriver
                        .resolve(view)
                        .unwrap_or(MergeResolution::Select(ConflictSide::Ours))
                } else {
                    MergeResolution::Select(ConflictSide::Ours)
                };
                resolutions.insert(view.key.clone(), resolution);
            }
        }
        let reservation = self
            .reserve_operation(
                &target,
                reservation_operation,
                "conflict projection reservation changed",
            )
            .await?;
        let outcome = self
            .apply_plan(
                root_id,
                root,
                &plan,
                projection_key,
                &resolutions,
                PublicationPermit::Reservation {
                    operation_id: reservation_operation.into_bytes(),
                    gate_tail: reservation.gate_tail,
                    expected: reservation.expected,
                },
            )
            .await;
        let release = target
            .volume
            .fs
            .authority()
            .release_publication(reservation, &mut authority_work)
            .await
            .map_err(WorkspaceError::engine);
        if let Err(error) = release {
            return Err(error.into());
        }
        let outcome = outcome?;
        match outcome {
            JoinOutcome::Applied(generation) | JoinOutcome::AlreadyApplied(generation) => {
                Ok(generation.generation.id())
            }
            JoinOutcome::NoChanges(generation) => Ok(generation.id()),
            _ => Err(WorkspaceMultiRootPublisherError::Candidate(
                "conflict projection did not produce a generation".to_owned(),
            )),
        }
    }

    async fn abort_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
    ) -> Result<GenerationId, Self::Error> {
        let cancellation = CancellationToken::new();
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let key = publisher_key(operation_id, root_id, b"conflict-abort");
        let reservation_operation =
            conflict_abort_materialization_operation_id(operation_id, root_id);
        if let Some(generation) = target
            .operation_generation(key, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value
        {
            self.release_operation_reservation(&target, reservation_operation)
                .await?;
            return Ok(generation.id());
        }
        let current = target
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        let original = target
            .generation(
                root.target_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let projected = target
            .generation(
                projected_generation,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        let changed = original
            .diff_to(
                &projected,
                u32::MAX,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value
            .0
            .changed_paths(u32::MAX, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value
            .0;
        let paths = changed
            .iter()
            .map(|change| {
                portable_namespace_path(&change.path).ok_or_else(|| {
                    WorkspaceMultiRootPublisherError::Candidate(
                        "conflict abort cannot represent a non-portable path".to_owned(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if paths.is_empty() {
            // The conflicted projection changed no path in the target (every
            // conflicting record was held back), so there is nothing to undo.
            return Ok(current.id());
        }
        let reservation = self
            .reserve_operation(
                &target,
                reservation_operation,
                "conflict abort target changed while being fenced",
            )
            .await?;
        let outcome = target
            .apply_paths_from(
                Some(&projected),
                Some(&original),
                &paths,
                current.id(),
                key,
                PublicationPermit::Reservation {
                    operation_id: reservation_operation.into_bytes(),
                    gate_tail: reservation.gate_tail,
                    expected: reservation.expected,
                },
            )
            .await;
        let release = target
            .volume
            .fs
            .authority()
            .release_publication(reservation, &mut authority_work)
            .await
            .map_err(WorkspaceError::engine);
        if let Err(error) = release {
            return Err(error.into());
        }
        let outcome = outcome?;
        match outcome {
            crate::WorkspacePathApply::Applied(generation)
            | crate::WorkspacePathApply::AlreadyApplied(generation)
            | crate::WorkspacePathApply::NoChanges(generation) => Ok(generation.id()),
            crate::WorkspacePathApply::Conflicted(_)
            | crate::WorkspacePathApply::Stale(_)
            | crate::WorkspacePathApply::Fenced
            | crate::WorkspacePathApply::IdempotencyConflict => {
                Err(WorkspaceMultiRootPublisherError::Candidate(
                    "conflict abort overlaps later workspace changes".to_owned(),
                ))
            }
        }
    }

    async fn declare_conflicts(
        &self,
        _operation_id: OperationId,
        _root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        conflict: &crate::MergePlan,
        paths: &[String],
    ) -> Result<BTreeSet<ConflictKey>, Self::Error> {
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let current = target
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        let normalized = paths
            .iter()
            .map(|path| path.trim_start_matches('/').trim_end_matches('/'))
            .collect::<Vec<_>>();
        let declares_all = normalized
            .iter()
            .any(|candidate| *candidate == "." || candidate.is_empty());
        let mut declared = BTreeSet::new();
        for view in &conflict.conflicts {
            let Some(raw_path) = view.path.as_deref() else {
                if declares_all {
                    declared.insert(view.key.clone());
                }
                continue;
            };
            let path = raw_path.trim_start_matches('/');
            if !normalized.iter().any(|candidate| {
                *candidate == "."
                    || *candidate == path
                    || (!candidate.is_empty()
                        && path
                            .strip_prefix(*candidate)
                            .is_some_and(|suffix| suffix.starts_with('/')))
            }) {
                continue;
            }
            if view.kind == crate::ConflictKind::Text {
                match current
                    .read(
                        &format!("/{path}"),
                        64 * 1024 * 1024,
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
                {
                    Ok(bytes) => {
                        let text = std::str::from_utf8(&bytes).map_err(|_| {
                            WorkspaceMultiRootPublisherError::Candidate(format!(
                                "text conflict {path} is no longer UTF-8"
                            ))
                        })?;
                        if crate::text_merge::has_conflict_markers(text) {
                            return Err(WorkspaceMultiRootPublisherError::Candidate(format!(
                                "text conflict {path} still contains conflict markers"
                            )));
                        }
                    }
                    Err(WorkspaceError::NotFound) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            declared.insert(view.key.clone());
        }
        Ok(declared)
    }

    async fn finish_conflict(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        projected_generation: GenerationId,
    ) -> Result<MultiRootConflictFinish, Self::Error> {
        let target = self
            .resolver
            .resolve(root.target_workspace_id)
            .await
            .map_err(WorkspaceMultiRootPublisherError::Resolver)?;
        let current = target
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        if !generation_descends_from(&target, current.id(), projected_generation).await? {
            return Err(WorkspaceMultiRootPublisherError::Candidate(
                "resolved conflict generation does not descend from its projection".to_owned(),
            ));
        }
        let reservation_operation = conflict_finish_operation_id(operation_id, root_id);
        let reservation = self
            .reserve_operation(
                &target,
                reservation_operation,
                "resolved conflict generation changed while being fenced",
            )
            .await?;
        Ok(MultiRootConflictFinish {
            generation: current.id(),
            fence: MultiRootFence {
                token: serde_json::to_vec(&WorkspacePublicationFence {
                    target_workspace_id: target.id(),
                    reservation,
                })
                .map_err(|error| WorkspaceMultiRootPublisherError::Candidate(error.to_string()))?,
            },
        })
    }

    async fn publish(
        &self,
        operation_id: OperationId,
        root_id: WorkspaceRootId,
        root: &MultiRootMergeRoot,
        resolutions: &BTreeMap<ConflictKey, MergeResolution>,
        fence: &MultiRootFence,
    ) -> Result<MultiRootPublishRoot, Self::Error> {
        let cancellation = CancellationToken::new();
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let decoded = decode_reservation(operation_id, fence)?;
        if decoded.target_workspace_id != root.target_workspace_id {
            return Err(WorkspaceMultiRootPublisherError::InvalidFence);
        }
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
            if let Some(generation) = target
                .operation_generation(
                    publisher_key(operation_id, root_id, b"publish"),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await
                .map_err(|failure| failure.error)?
                .value
            {
                target
                    .volume
                    .fs
                    .authority()
                    .release_publication(decoded.reservation, &mut authority_work)
                    .await
                    .map_err(WorkspaceError::engine)?;
                return Ok(MultiRootPublishRoot::AlreadyPublished(generation.id()));
            }
            return Ok(MultiRootPublishRoot::UnexpectedTarget);
        }
        if decoded.reservation.authority_id != target.authority_id() {
            return Err(WorkspaceMultiRootPublisherError::InvalidFence);
        }
        let plan = self.pinned_plan(root, Some(target.clone())).await?;
        let outcome = match self
            .apply_plan(
                root_id,
                root,
                &plan,
                publisher_key(operation_id, root_id, b"publish"),
                resolutions,
                PublicationPermit::Reservation {
                    operation_id: operation_id.into_bytes(),
                    gate_tail: decoded.reservation.gate_tail,
                    expected: decoded.reservation.expected,
                },
            )
            .await?
        {
            JoinOutcome::Applied(generation) => {
                MultiRootPublishRoot::Published(generation.generation.id())
            }
            JoinOutcome::NoChanges(generation) => MultiRootPublishRoot::Published(generation.id()),
            JoinOutcome::AlreadyApplied(generation) => {
                MultiRootPublishRoot::AlreadyPublished(generation.generation.id())
            }
            JoinOutcome::StaleTarget(_)
            | JoinOutcome::Fenced
            | JoinOutcome::IdempotencyConflict
            | JoinOutcome::Conflicted { .. } => MultiRootPublishRoot::UnexpectedTarget,
        };
        if matches!(
            outcome,
            MultiRootPublishRoot::Published(_) | MultiRootPublishRoot::AlreadyPublished(_)
        ) {
            target
                .volume
                .fs
                .authority()
                .release_publication(decoded.reservation, &mut authority_work)
                .await
                .map_err(WorkspaceError::engine)?;
        }
        Ok(outcome)
    }
}
