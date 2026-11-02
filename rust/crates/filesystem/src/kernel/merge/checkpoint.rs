async fn merge_generation_core<'a, 'accounting, S: AsyncObjectStore>(
    store: &'a S,
    request: MergeGenerationBorrowedRequest<'a>,
    resolutions: ResolutionView<'a>,
    limits: DecodeLimits,
    accounting: &'a mut KernelAccounting<'accounting>,
    cancellation: &'a CancellationToken,
) -> Result<(MergeGenerationOutcome, KernelOwnedAllocation), FsError> {
    let tables = request.tables();
    let (outcome, allocation) =
        merge_file_tables_core(store, tables, resolutions, limits, accounting, cancellation)
            .await?;
    let merged_table = match outcome {
        MergeFileTableOutcome::Prepared(table) => table,
        MergeFileTableOutcome::Conflicted {
            conflicts,
            truncated,
            ..
        } => {
            return Ok((
                MergeGenerationOutcome::Conflicted {
                    conflicts,
                    truncated,
                },
                allocation,
            ));
        }
    };
    let (merged, parent_id) = merged_checkpoint(
        store,
        &request,
        merged_table,
        limits,
        accounting,
        cancellation,
    )
    .await?;
    let parent_count = if request.retain_theirs_parent { 2 } else { 1 };
    let mut parents = Owned::allocate(parent_count, accounting)?;
    let delta = WorkCounters {
        bytes_copied: (parent_count * std::mem::size_of::<GenerationId>()) as u64,
        ..WorkCounters::UNCHARGED
    };
    let copied = accounting.admit(delta).and_then(|()| {
        parents.value.push(parent_id);
        if request.retain_theirs_parent {
            parents
                .value
                .push(GenerationId::from_root(request.theirs_generation));
        }
        accounting.record_completed(delta)
    });
    if let Err(error) = copied {
        parents.retire(accounting)?;
        return Err(error);
    }
    let Owned {
        value: parents,
        allocation,
        ..
    } = parents;
    Ok((
        MergeGenerationOutcome::Prepared {
            generation_root: merged,
            generation_id: GenerationId::from_root(merged),
            root: GenerationRoot {
                volume_id: request.ours.volume_id,
                root_file_id: request.ours.root_file_id,
                file_table: merged_table,
                parents,
                required_features: request.ours.continuation_features(),
            },
        },
        allocation,
    ))
}

async fn merged_checkpoint<S: AsyncObjectStore>(
    store: &S,
    request: &MergeGenerationBorrowedRequest<'_>,
    merged_table: ObjectId,
    limits: DecodeLimits,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(ObjectId, GenerationId), FsError> {
    let (parent, parent_id) = match request.ours_generation {
        Some(object) if object.kind == ObjectKind::GenerationRoot => {
            (object, GenerationId::from_root(object))
        }
        Some(_) => return Err(FsError::InvalidDiff),
        None => {
            let parent = build_checkpoint_with_accounting(
                store,
                CheckpointRequest {
                    base: request.base_generation,
                    file_table: request.ours.file_table,
                    merge_parent: None,
                },
                limits,
                accounting,
                cancellation,
            )
            .await?;
            (parent, GenerationId::from_root(parent))
        }
    };
    let merged = build_checkpoint_with_accounting(
        store,
        CheckpointRequest {
            base: parent,
            file_table: merged_table,
            merge_parent: request
                .retain_theirs_parent
                .then_some(request.theirs_generation),
        },
        limits,
        accounting,
        cancellation,
    )
    .await?;
    Ok((merged, parent_id))
}

impl MergeGenerationBorrowedRequest<'_> {
    /// Retains the same merge's clean decisions and exact unresolved regions.
    /// The checkpoint is a read projection only: no resolution or publication
    /// intent is produced, and an incomplete conflict frontier is refused.
    pub(crate) async fn preview_with_accounting<S: AsyncObjectStore>(
        self,
        store: &S,
        limits: DecodeLimits,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(ObjectId, Owned<Vec<MergeConflict>>), FsError> {
        let (outcome, allocation) = merge_file_tables_core(
            store,
            self.tables(),
            ResolutionView::Preview,
            limits,
            accounting,
            cancellation,
        )
        .await?;
        let (table, conflicts) = match outcome {
            MergeFileTableOutcome::Prepared(table) => {
                (table, Owned::new(Vec::new(), allocation, 0))
            }
            MergeFileTableOutcome::Conflicted {
                conflicts, preview, ..
            } => {
                let bytes = merge_conflict_bytes(&conflicts)?;
                let conflicts = Owned::new(conflicts, allocation, bytes);
                let Some(table) = preview else {
                    conflicts.retire(accounting)?;
                    return Err(FsError::MergeChangeLimit);
                };
                (table, conflicts)
            }
        };
        match merged_checkpoint(store, &self, table, limits, accounting, cancellation).await {
            Ok((checkpoint, _)) => Ok((checkpoint, conflicts)),
            Err(error) => {
                conflicts.retire(accounting)?;
                Err(error)
            }
        }
    }
}
