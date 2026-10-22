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
    let (parent, parent_id) = match request.ours_generation {
        Some(object) if object.kind == ObjectKind::GenerationRoot => {
            (object, GenerationId::new(object.digest))
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
            (parent, GenerationId::new(parent.digest))
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
                .push(GenerationId::new(request.theirs_generation.digest));
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
            generation_id: GenerationId::new(merged.digest),
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
